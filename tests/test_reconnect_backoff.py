"""Tests for reconnect backoff and the configurable WebSocket endpoint.

The delay must grow exponentially with consecutive failures, stay capped, be
jittered within [0, bound], and reset only once a connection actually delivers
a message — a connect-accept-then-immediately-drop loop must keep backing off.
"""

import asyncio
import random

from hyperliquid_pipeline.collectors import realtime_collector as rc
from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
from hyperliquid_pipeline.config import settings


def test_ws_url_comes_from_settings_and_can_be_overridden():
    assert HyperliquidWebSocketCollector(["BTC"]).ws_url == settings.hyperliquid_ws_url
    custom = "ws://localhost:9999/ws"
    assert HyperliquidWebSocketCollector(["BTC"], ws_url=custom).ws_url == custom


def test_backoff_bound_doubles_and_caps(monkeypatch):
    # Make the jittered draw deterministic: always return the upper bound.
    monkeypatch.setattr(rc.random, "uniform", lambda a, b: b)
    collector = HyperliquidWebSocketCollector(["BTC"])
    base = float(settings.websocket_reconnect_delay)   # 5
    cap = float(settings.websocket_reconnect_max_delay)  # 60
    bounds = [collector._next_reconnect_delay(n) for n in range(0, 7)]
    assert bounds[0] == base   # healthy-session drop: quick retry, bounded by base
    assert bounds[1] == base   # first failure
    assert bounds[2] == base * 2
    assert bounds[3] == base * 4
    assert bounds[4] == base * 8
    assert bounds[5] == cap    # 5 * 2^4 = 80 -> capped
    assert bounds[6] == cap


def test_backoff_jitter_stays_within_bounds():
    random.seed(7)
    collector = HyperliquidWebSocketCollector(["BTC"])
    base = float(settings.websocket_reconnect_delay)
    for _ in range(200):
        d = collector._next_reconnect_delay(2)
        assert 0 <= d <= base * 2


def test_failure_streak_resets_only_after_a_message(monkeypatch):
    """Drive start_with_reconnect with a fake connect(): two silent failures,
    then a message-bearing connection, then one more failure — the recorded
    sleep bounds must show growth, then a reset."""
    monkeypatch.setattr(rc.random, "uniform", lambda a, b: b)

    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])
        delays = []
        connects = 0

        async def fake_connect():
            nonlocal connects
            connects += 1
            if connects == 3:  # third connection is healthy: it delivers data
                collector.message_count += 10
            if connects == 5:
                raise asyncio.CancelledError  # end the loop

        async def fake_sleep(d):
            delays.append(d)

        monkeypatch.setattr(collector, "connect", fake_connect)
        monkeypatch.setattr(rc.asyncio, "sleep", fake_sleep)
        try:
            await collector.start_with_reconnect()
        except asyncio.CancelledError:
            pass

        base = float(settings.websocket_reconnect_delay)
        # failures: 1 -> base, 2 -> base*2, then reset (healthy) -> base, 1 -> base
        assert delays == [base, base * 2, base, base]

    asyncio.run(run())
