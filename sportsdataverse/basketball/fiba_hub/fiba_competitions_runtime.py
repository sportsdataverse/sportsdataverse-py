"""Per-competition fiba convenience families, minted from the competition registry.

Spreads ``<prefix>_<suffix>`` callables (one set per registry entry) into this module; the home package
re-exports them. See :mod:`sportsdataverse.basketball._competitions`.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List

from sportsdataverse.basketball._competitions import entries, mint_fiba

FAMILIES: Dict[str, Dict[str, Callable[..., Any]]] = {e["key"]: mint_fiba(e, __name__) for e in entries("fiba")}
for _fam in FAMILIES.values():
    globals().update(_fam)

__all__: List[str] = [name for _fam in FAMILIES.values() for name in _fam]
