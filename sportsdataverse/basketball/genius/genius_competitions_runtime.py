"""Per-competition genius convenience families, minted from the competition registry.

Spreads ``<prefix>_<suffix>`` callables (one set per registry entry) into this module; the home package
re-exports them. See :mod:`sportsdataverse.basketball._competitions`.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List

from sportsdataverse.basketball._competitions import entries, mint_genius

FAMILIES: Dict[str, Dict[str, Callable[..., Any]]] = {e["key"]: mint_genius(e, __name__) for e in entries("genius")}
for _fam in FAMILIES.values():
    globals().update(_fam)

__all__: List[str] = [name for _fam in FAMILIES.values() for name in _fam]
