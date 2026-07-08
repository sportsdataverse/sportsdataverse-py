"""Self-contained per-league binder for the PFF Premium Stats flat stem.

Replaces the retired ESPN ``make_league_module`` / ``_bind`` factory (which
``sportsdataverse/_common_espn.py`` no longer provides) for the PFF stem. Given a shim
namespace and a league slug, it installs league-bound copies of the generated
:mod:`sportsdataverse.nfl.pff_core` wrappers:

* wrappers that take a ``league`` parameter (the facet + player views) are bound with
  :func:`functools.partial` so the league is pre-filled and drops out of the caller's
  signature; ``__name__`` / ``__qualname__`` / ``__doc__`` are copied onto the partial for
  IDE introspection (but **not** ``__wrapped__`` -- that would make ``inspect.signature``
  follow back to the unbound core function and re-expose ``league``),
* singletons without a ``league`` parameter (only ``pff_leagues``) are installed unchanged.

The four public shim modules (``sportsdataverse.{nfl,cfb,football.aaf,football.ufl}.pff``)
are one-liners over this helper.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Dict, List

from sportsdataverse.nfl import pff_core

__all__ = ["make_pff_league_module"]

_COPY_ATTRS = ("__module__", "__name__", "__qualname__", "__doc__")


def make_pff_league_module(namespace: Dict[str, Any], league_slug: str) -> List[str]:
    """Install league-bound copies of the ``pff_core`` wrappers into *namespace*.

    Args:
        namespace: The shim module's ``globals()`` dict to populate.
        league_slug: The PFF league slug to pre-bind (``"nfl"`` / ``"ncaa"`` / ``"aaf"`` /
            ``"ufl"``).

    Returns:
        The list of installed wrapper names -- assign it to the shim module's ``__all__``.

    Example:
        In a one-line shim module::

            from sportsdataverse.nfl.pff_league import make_pff_league_module
            __all__ = make_pff_league_module(globals(), "ncaa")
    """
    names: List[str] = []
    for name in pff_core.__all__:
        fn = getattr(pff_core, name)
        if "league" in inspect.signature(fn).parameters:
            bound: Any = functools.partial(fn, league=league_slug)
            for attr in _COPY_ATTRS:
                try:
                    setattr(bound, attr, getattr(fn, attr))
                except (AttributeError, TypeError):  # pragma: no cover - defensive
                    pass
        else:
            bound = fn  # singleton with no league to bind (e.g. pff_leagues)
        namespace[name] = bound
        names.append(name)
    return names
