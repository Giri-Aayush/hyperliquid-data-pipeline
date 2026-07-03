"""Utility modules for the pipeline.

Names from .validation are re-exported lazily (PEP 562): validation imports
MarketDataPoint from the collectors package, so importing it eagerly here
would make any `utils.*` import (e.g. utils.latency from the collector's hot
path) circular.
"""

__all__ = [
    "DataValidator",
    "DataSanitizer",
    "ValidationCallback",
    "ValidationResult",
    "ValidationLevel",
    "DataQualityMetrics",
]


def __getattr__(name):
    if name in __all__:
        from . import validation
        return getattr(validation, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
