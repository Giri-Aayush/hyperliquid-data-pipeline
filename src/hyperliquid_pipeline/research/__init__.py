"""Signal research on captured market data.

Offline, capture-driven analysis modules: each one reads the DataLogger's
per-stream JSONL (what the live collector writes) and produces an honest
statistical read — never a strategy claim. First signal: order-flow
imbalance (``ofi``).
"""

_OFI_EXPORTS = {
    "BboEvent",
    "aggregate_windows",
    "analyze",
    "decile_table",
    "forward_pairs",
    "forward_triples",
    "load_bbo_events",
    "ofi_series",
    "ols_stats",
}


def __getattr__(name: str):
    """Lazy re-export from the ofi module.

    Importing it eagerly here would double-import it under
    ``python -m hyperliquid_pipeline.research.ofi`` (runpy RuntimeWarning).
    """
    if name in _OFI_EXPORTS:
        import importlib

        return getattr(
            importlib.import_module("hyperliquid_pipeline.research.ofi"), name
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = sorted(_OFI_EXPORTS)
