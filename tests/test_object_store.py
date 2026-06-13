"""Tests for the S3-compatible object store and the historical raw-pull cache.

No network: LocalObjectStore uses a temp dir, the S3 client is stubbed with
botocore's Stubber, and the AWS download in the cache test is mocked.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from hyperliquid_pipeline.config import settings
from hyperliquid_pipeline.storage.object_store import (
    LocalObjectStore,
    S3ObjectStore,
    get_object_store,
)


# --- LocalObjectStore round-trips ---------------------------------------------

def test_local_object_store_roundtrip(tmp_path):
    store = LocalObjectStore(root=tmp_path / "store")

    src = tmp_path / "src.txt"
    src.write_bytes(b"hello")

    assert store.put_file(src, "a/b/c.txt") is True
    assert store.exists("a/b/c.txt") is True
    assert store.exists("a/b/missing.txt") is False

    out = tmp_path / "out.txt"
    assert store.get_file("a/b/c.txt", out) is True
    assert out.read_bytes() == b"hello"

    assert store.get_file("does/not/exist", tmp_path / "x") is False


def test_local_object_store_put_bytes_and_list(tmp_path):
    store = LocalObjectStore(root=tmp_path / "store")

    assert store.put_bytes(b"one", "data/one.bin") is True
    assert store.put_bytes(b"two", "data/two.bin") is True

    keys = set(store.list("data/"))
    assert "data/one.bin" in keys
    assert "data/two.bin" in keys


def test_local_object_store_prefix(tmp_path):
    store = LocalObjectStore(root=tmp_path / "store", prefix="hyperliquid/")
    store.put_bytes(b"x", "raw/foo.lz4")

    # Physically stored under the prefix...
    assert (tmp_path / "store" / "hyperliquid" / "raw" / "foo.lz4").exists()
    # ...but the prefix is invisible to callers.
    assert store.exists("raw/foo.lz4") is True
    assert "raw/foo.lz4" in store.list("raw/")


# --- factory resolution -------------------------------------------------------

def test_get_object_store_disabled_by_default(monkeypatch):
    monkeypatch.setattr(settings, "object_store_backend", "auto")
    monkeypatch.setattr(settings, "object_store_bucket", None)
    assert get_object_store() is None


def test_get_object_store_none(monkeypatch):
    monkeypatch.setattr(settings, "object_store_backend", "none")
    monkeypatch.setattr(settings, "object_store_bucket", "ignored")
    assert get_object_store() is None


def test_get_object_store_local(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "object_store_backend", "local")
    monkeypatch.setattr(settings, "object_store_local_path", tmp_path / "os")
    monkeypatch.setattr(settings, "object_store_prefix", "")
    store = get_object_store()
    assert isinstance(store, LocalObjectStore)


def test_get_object_store_s3_requires_credentials(monkeypatch):
    monkeypatch.setattr(settings, "object_store_backend", "s3")
    monkeypatch.setattr(settings, "object_store_bucket", "my-bucket")
    monkeypatch.setattr(settings, "object_store_access_key_id", None)
    monkeypatch.setattr(settings, "object_store_secret_access_key", None)
    # Missing creds -> disabled, not a crash.
    assert get_object_store() is None


def test_get_object_store_auto_selects_s3_when_bucket_set(monkeypatch):
    monkeypatch.setattr(settings, "object_store_backend", "auto")
    monkeypatch.setattr(settings, "object_store_bucket", "my-bucket")
    monkeypatch.setattr(settings, "object_store_access_key_id", "ak")
    monkeypatch.setattr(settings, "object_store_secret_access_key", "sk")
    monkeypatch.setattr(settings, "object_store_endpoint_url", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setattr(settings, "object_store_region", "auto")
    monkeypatch.setattr(settings, "object_store_prefix", "")
    store = get_object_store()
    assert isinstance(store, S3ObjectStore)


# --- S3ObjectStore against a stubbed client (R2-style endpoint) ---------------

def _make_s3_store():
    return S3ObjectStore(
        bucket="bkt",
        access_key_id="ak",
        secret_access_key="sk",
        endpoint_url="https://acct.r2.cloudflarestorage.com",
        region="auto",
        prefix="pre/",
    )


def test_s3_put_bytes_uses_prefixed_key():
    from botocore.stub import Stubber

    store = _make_s3_store()
    stub = Stubber(store.client)
    stub.add_response(
        "put_object",
        {},
        {"Bucket": "bkt", "Key": "pre/foo.txt", "Body": b"hi"},
    )
    with stub:
        assert store.put_bytes(b"hi", "foo.txt") is True
    stub.assert_no_pending_responses()


def test_s3_exists_true_and_false():
    from botocore.stub import Stubber

    store = _make_s3_store()

    stub = Stubber(store.client)
    stub.add_response("head_object", {"ContentLength": 2}, {"Bucket": "bkt", "Key": "pre/there.txt"})
    with stub:
        assert store.exists("there.txt") is True

    stub2 = Stubber(store.client)
    stub2.add_client_error("head_object", service_error_code="404", http_status_code=404)
    with stub2:
        assert store.exists("missing.txt") is False


# --- historical raw-pull cache (read-through / write-back) --------------------

def test_download_file_caches_and_serves_from_object_store(tmp_path):
    from hyperliquid_pipeline.collectors.historical_collector import (
        HistoricalDataCollector,
        S3Location,
    )

    collector = HistoricalDataCollector(
        object_store=LocalObjectStore(root=tmp_path / "cache")
    )

    # Mock the paid AWS download so it writes a file but counts calls.
    def fake_download(Bucket, Key, Filename, ExtraArgs=None):
        p = Path(Filename)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"rawdata")

    collector.s3_client = MagicMock()
    collector.s3_client.download_file.side_effect = fake_download

    loc = S3Location(
        bucket="hyperliquid-archive",
        key="market_data/20240101/9/trades/BTC.lz4",
    )
    cache_key = "raw/hyperliquid-archive/market_data/20240101/9/trades/BTC.lz4"

    # First pull: AWS is hit once, then the object is cached.
    first = tmp_path / "first.lz4"
    assert collector.download_file(loc, first) is True
    assert collector.s3_client.download_file.call_count == 1
    assert collector.object_store.exists(cache_key) is True

    # Second pull: served from cache, AWS NOT hit again.
    second = tmp_path / "second.lz4"
    assert collector.download_file(loc, second) is True
    assert collector.s3_client.download_file.call_count == 1  # unchanged
    assert second.read_bytes() == b"rawdata"


def test_download_file_ignores_empty_cached_object(tmp_path):
    """A 0-byte cached object must be treated as a miss, not served forever."""
    from hyperliquid_pipeline.collectors.historical_collector import (
        HistoricalDataCollector,
        S3Location,
    )

    store = LocalObjectStore(root=tmp_path / "cache")
    collector = HistoricalDataCollector(object_store=store)

    loc = S3Location(bucket="hyperliquid-archive", key="market_data/20240101/9/trades/BTC.lz4")
    cache_key = "raw/hyperliquid-archive/market_data/20240101/9/trades/BTC.lz4"

    # Poison the cache with an empty object.
    store.put_bytes(b"", cache_key)

    def fake_download(Bucket, Key, Filename, ExtraArgs=None):
        p = Path(Filename)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"realdata")

    collector.s3_client = MagicMock()
    collector.s3_client.download_file.side_effect = fake_download

    out = tmp_path / "out.lz4"
    assert collector.download_file(loc, out) is True
    # Empty cache entry was ignored -> AWS was hit, and good data overwrote it.
    assert collector.s3_client.download_file.call_count == 1
    assert out.read_bytes() == b"realdata"


def test_download_file_does_not_cache_empty_source(tmp_path):
    """An empty source object must not poison the cache via write-back."""
    from hyperliquid_pipeline.collectors.historical_collector import (
        HistoricalDataCollector,
        S3Location,
    )

    store = LocalObjectStore(root=tmp_path / "cache")
    collector = HistoricalDataCollector(object_store=store)

    loc = S3Location(bucket="hyperliquid-archive", key="market_data/20240101/3/trades/BTC.lz4")
    cache_key = "raw/hyperliquid-archive/market_data/20240101/3/trades/BTC.lz4"

    def fake_empty(Bucket, Key, Filename, ExtraArgs=None):
        p = Path(Filename)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")

    collector.s3_client = MagicMock()
    collector.s3_client.download_file.side_effect = fake_empty

    out = tmp_path / "empty.lz4"
    assert collector.download_file(loc, out) is True
    assert store.exists(cache_key) is False  # empty file was not cached


# --- backend aliasing / unknown values ----------------------------------------

def test_get_object_store_r2_alias(monkeypatch):
    monkeypatch.setattr(settings, "object_store_backend", "r2")
    monkeypatch.setattr(settings, "object_store_bucket", "my-bucket")
    monkeypatch.setattr(settings, "object_store_access_key_id", "ak")
    monkeypatch.setattr(settings, "object_store_secret_access_key", "sk")
    monkeypatch.setattr(settings, "object_store_endpoint_url", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setattr(settings, "object_store_region", "auto")
    monkeypatch.setattr(settings, "object_store_prefix", "")
    # "r2" is a friendly alias for the S3-compatible client.
    assert isinstance(get_object_store(), S3ObjectStore)


def test_get_object_store_unknown_backend_disabled(monkeypatch):
    monkeypatch.setattr(settings, "object_store_backend", "bogus")
    monkeypatch.setattr(settings, "object_store_bucket", "my-bucket")
    # Unrecognized backend disables the store (and warns) rather than half-working.
    assert get_object_store() is None


# --- DataLogger object-store upload --------------------------------------------

def _point(symbol, data_type, ts):
    from hyperliquid_pipeline.collectors.realtime_collector import MarketDataPoint
    return MarketDataPoint(timestamp=ts, symbol=symbol, data_type=data_type, data={"x": 1})


def test_datalogger_uploads_namespaced_key_on_close(tmp_path):
    from datetime import datetime, timezone
    from hyperliquid_pipeline.collectors.realtime_collector import DataLogger

    store = LocalObjectStore(root=tmp_path / "bucket")
    dl = DataLogger(output_dir=str(tmp_path / "rt"), object_store=store)
    dl.log_data_point(_point("BTC", "trade", datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)))
    dl.close_all_files()

    # Key is namespaced by output_dir name, not basename alone.
    assert store.exists("realtime/rt/BTC_trade_20240101.jsonl") is True


def test_datalogger_finalizes_previous_day_on_rollover(tmp_path):
    from datetime import datetime, timezone
    from hyperliquid_pipeline.collectors.realtime_collector import DataLogger

    store = LocalObjectStore(root=tmp_path / "bucket")
    dl = DataLogger(output_dir=str(tmp_path / "rt"), object_store=store)

    dl.log_data_point(_point("BTC", "trade", datetime(2024, 1, 1, 23, 59, tzinfo=timezone.utc)))
    # Next day, same stream -> previous day's file is finalized and uploaded now.
    dl.log_data_point(_point("BTC", "trade", datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)))

    assert store.exists("realtime/rt/BTC_trade_20240101.jsonl") is True
    assert store.exists("realtime/rt/BTC_trade_20240102.jsonl") is False  # still open
    dl.close_all_files()
    assert store.exists("realtime/rt/BTC_trade_20240102.jsonl") is True
