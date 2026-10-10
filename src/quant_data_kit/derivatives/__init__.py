"""Explicit, immutable inputs for futures and vanilla-option research.

This format validates declared inputs, not exchange authorization or historical
point-in-time truth. Retrospective imports never acquire a certification label.
"""

from .bundle import DerivativeBundle, load_bundle, write_bundle
from .models import Contract, Quote, decimal, utc

__all__ = ["Contract", "DerivativeBundle", "Quote", "decimal", "load_bundle", "utc", "write_bundle"]
