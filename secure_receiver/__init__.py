"""Bounded-buffer, multi-tenant, crash-recoverable receipt ingest.

The four public layers are:

``budget``      an independently-counted allocation ledger with a hard byte cap
``store``       a tenant-scoped append store with a commit-pointer protocol
``receiver``    the two-producer ingest pipeline (dedupe, validate, publish)
``recovery``    crash injection + replay that only exposes approved batches

See ``docs/DESIGN.md`` for the failure model and the reclaim schedule.
"""

from .budget import Budget, BudgetExhausted, Pool
from .receiver import Receiver
from .store import Store, StoreCorrupt

__all__ = [
    "Budget",
    "BudgetExhausted",
    "Pool",
    "Receiver",
    "Store",
    "StoreCorrupt",
]
