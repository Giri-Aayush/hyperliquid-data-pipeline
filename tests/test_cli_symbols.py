"""Tests for the CLI --symbols override.

settings.collect_symbols is a str and settings.symbols_list splits it, so the
CLI override must normalize back to a comma-string. Assigning a list here used
to crash `start --symbols SOL` with AttributeError when the orchestrator read
symbols_list.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from run_pipeline import _normalize_symbols  # noqa: E402
from hyperliquid_pipeline.config import settings  # noqa: E402


def test_normalize_symbols_returns_comma_string():
    assert _normalize_symbols("SOL") == "SOL"
    assert _normalize_symbols("BTC,ETH") == "BTC,ETH"
    assert _normalize_symbols(" btc , eth ") == "btc,eth"
    assert _normalize_symbols("BTC,,ETH,") == "BTC,ETH"  # empty entries dropped


def test_symbols_list_works_after_cli_override():
    original = settings.collect_symbols
    try:
        settings.collect_symbols = _normalize_symbols("SOL")
        assert settings.symbols_list == ["SOL"]

        settings.collect_symbols = _normalize_symbols(" BTC , ETH ")
        assert settings.symbols_list == ["BTC", "ETH"]
    finally:
        settings.collect_symbols = original
