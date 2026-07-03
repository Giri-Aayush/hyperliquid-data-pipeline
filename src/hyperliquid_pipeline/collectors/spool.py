"""Lossless raw-frame capture spool.

The processing queue between the socket and the callbacks is deliberately
drop-oldest: freshest-wins is right for a signal path, but it punches holes in
the research archive under exactly the bursty conditions that matter most.
The spool is the other half of the design: every raw websocket frame is
written to an hourly JSONL file, stamped with local receive time, BEFORE
parsing and independent of that queue — so the archive is complete even when
the processing path is shedding load, and survives parser bugs (it stores the
wire bytes, not our interpretation of them).

Hot-path cost is one f-string concat and one put_nowait. A dedicated writer
task drains the spool queue in batches and writes via an executor, so a disk
stall never blocks the event loop; the only way to lose a frame is sustained
writer starvation (disk failure), which is counted and is an alarm condition.
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from loguru import logger

from ..config import settings

if TYPE_CHECKING:  # annotation only; runtime import is deferred to avoid a cycle
    from ..storage.object_store import ObjectStore

# Sentinel: distinguishes "use the configured object store" (default) from an
# explicit None ("no store"), so tests never touch a real bucket by accident.
_DEFAULT_STORE = object()

_MS_PER_HOUR = 3_600_000


class RawSpool:
    """Append-only WAL of raw websocket frames, one hourly JSONL file at a time.

    Line format: {"recv_ts_ms": ..., "recv_mono_ns": ..., "raw": <frame>}
    The frame is embedded verbatim (it is already JSON), so writing is pure
    string concatenation — no re-serialization on the hot path.
    """

    def __init__(
        self,
        spool_dir: Optional[Path] = None,
        queue_maxsize: Optional[int] = None,
        flush_interval: Optional[float] = None,
        object_store: Any = _DEFAULT_STORE,
        max_batch_lines: int = 5000,
    ):
        self.spool_dir = Path(spool_dir) if spool_dir else settings.spool_dir
        self.flush_interval = (
            flush_interval if flush_interval is not None else settings.spool_flush_interval
        )
        maxsize = queue_maxsize if queue_maxsize is not None else settings.spool_queue_maxsize
        self._queue: "asyncio.Queue[Tuple[int, str]]" = asyncio.Queue(maxsize=maxsize)
        self._max_batch_lines = max_batch_lines
        self.logger = logger.bind(component="raw_spool")

        if object_store is _DEFAULT_STORE:
            from ..storage.object_store import get_object_store  # deferred: import cycle
            self.object_store = get_object_store()
        else:
            self.object_store = object_store

        self._writer_task: Optional[asyncio.Task] = None
        self._closing = asyncio.Event()
        self._closed = False

        self._current_hour_key: Optional[int] = None
        self._current_path: Optional[Path] = None
        self._current_handle = None

        # Stats
        self.written_lines = 0
        self.write_batches = 0
        self.spool_dropped = 0

    # ------------------------------------------------------------- hot path

    def enqueue(self, raw_frame: str, recv_ts_ms: float, recv_mono_ns: int) -> bool:
        """Queue one raw frame for spooling. Never blocks, never raises.

        Called from the socket read loop. Returns False if the frame could not
        be queued (spool closed, or writer starved and the queue is full).
        """
        if self._closed or self._closing.is_set():
            return False
        line = f'{{"recv_ts_ms":{recv_ts_ms!r},"recv_mono_ns":{recv_mono_ns},"raw":{raw_frame}}}\n'
        hour_key = int(recv_ts_ms // _MS_PER_HOUR)
        try:
            self._queue.put_nowait((hour_key, line))
            return True
        except asyncio.QueueFull:
            self.spool_dropped += 1
            if self.spool_dropped % 10_000 == 1:
                self.logger.error(
                    f"Spool queue full — writer starved (disk failure?). "
                    f"Dropping frames from the LOSSLESS archive (total: {self.spool_dropped})"
                )
            return False

    # ------------------------------------------------------------- lifecycle

    def start(self):
        """Start the writer task. Call from within a running event loop."""
        if self._writer_task is None and not self._closed:
            self._writer_task = asyncio.create_task(self._writer_loop())

    async def close(self):
        """Drain the queue completely, finalize the current file, stop. Idempotent."""
        if self._closed:
            return
        self._closing.set()
        if self._writer_task is not None:
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None
        else:
            # Never started: drain synchronously so nothing queued is lost.
            batch = self._drain_nowait()
            if batch:
                await self._write_batch(batch)
        self._finalize_current()
        self._closed = True

    # ------------------------------------------------------------- writer

    def _drain_nowait(self) -> List[Tuple[int, str]]:
        items: List[Tuple[int, str]] = []
        while len(items) < self._max_batch_lines:
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return items

    async def _writer_loop(self):
        while not (self._closing.is_set() and self._queue.empty()):
            items: List[Tuple[int, str]] = []
            try:
                items.append(await asyncio.wait_for(self._queue.get(), timeout=self.flush_interval))
            except asyncio.TimeoutError:
                continue
            items.extend(self._drain_nowait())
            await self._write_batch(items)

    async def _write_batch(self, items: List[Tuple[int, str]]):
        """Write a batch, split at hour boundaries, via the executor."""
        i = 0
        while i < len(items):
            hour_key = items[i][0]
            j = i
            while j < len(items) and items[j][0] == hour_key:
                j += 1
            chunk = "".join(line for _, line in items[i:j])
            handle = self._handle_for(hour_key)
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, self._write_sync, handle, chunk
                )
                self.written_lines += j - i
                self.write_batches += 1
            except Exception as e:
                self.spool_dropped += j - i
                self.logger.error(f"Spool write failed, {j - i} frames lost: {e}")
            i = j

    @staticmethod
    def _write_sync(handle, chunk: str):
        handle.write(chunk)
        handle.flush()

    # ------------------------------------------------------------- files

    def _path_for(self, hour_key: int) -> Path:
        dt = datetime.fromtimestamp(hour_key * 3600, tz=timezone.utc)
        return self.spool_dir / dt.strftime("%Y%m%d") / f"raw_{dt.strftime('%H')}.jsonl"

    def _handle_for(self, hour_key: int):
        if hour_key != self._current_hour_key:
            self._finalize_current()
            path = self._path_for(hour_key)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._current_handle = open(path, "a")
            self._current_path = path
            self._current_hour_key = hour_key
        return self._current_handle

    def _object_key(self, path: Path) -> str:
        return f"spool/{path.parent.name}/{path.name}"

    def _finalize_current(self):
        """Close the current file and mirror it to the object store, if any."""
        if self._current_handle is None:
            return
        try:
            self._current_handle.close()
        except Exception:
            pass
        if self.object_store and self._current_path is not None:
            try:
                self.object_store.put_file(self._current_path, self._object_key(self._current_path))
            except Exception as e:
                self.logger.error(f"Failed to upload spool file {self._current_path}: {e}")
        self._current_handle = None
        self._current_path = None
        self._current_hour_key = None

    # ------------------------------------------------------------- stats

    def stats(self) -> Dict[str, Any]:
        return {
            "queued": self._queue.qsize(),
            "written_lines": self.written_lines,
            "write_batches": self.write_batches,
            "spool_dropped": self.spool_dropped,
            "current_file": str(self._current_path) if self._current_path else None,
        }
