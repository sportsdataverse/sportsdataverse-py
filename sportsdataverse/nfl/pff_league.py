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


#: PFF league slugs, in the order their aliases are installed. The slug is PFF's
#: own (``ncaa`` for college), not the sdv module name, so the alias matches the
#: value you would pass to ``league=``.
PFF_LEAGUE_SLUGS = ("nfl", "ncaa", "ufl", "aaf")


def make_pff_alias_module(namespace: Dict[str, Any]) -> List[str]:
    """Install league-PREFIXED copies of every PFF wrapper into *namespace*.

    The per-league shims (``nfl/pff.py``, ``cfb/pff.py``, ``football/ufl/pff.py``,
    ``football/aaf/pff.py``) all install the SAME 46 bare names bound to different
    leagues, so they can never be star-imported into one namespace: the last import
    would win and every call would silently hit the wrong league. That is why PFF
    was the one source family unreachable from the top-level package.

    This mints ``pff_<slug>_<rest>`` aliases instead -- ``pff_nfl_facet_blocking_summary``,
    ``pff_ncaa_facet_blocking_summary`` -- matching the ``espn_<league>_*`` /
    ``fox_<league>_*`` convention used everywhere else, so all four leagues coexist.
    Purely additive: the bare names on each shim are untouched.

    Args:
        namespace: The alias module's ``globals()`` dict to populate.

    Returns:
        The installed alias names -- assign to the module's ``__all__``.
    """
    names: List[str] = []
    for slug in PFF_LEAGUE_SLUGS:
        for name in pff_core.__all__:
            fn = getattr(pff_core, name)
            if "league" in inspect.signature(fn).parameters:
                bound: Any = functools.partial(fn, league=slug)
                for attr in _COPY_ATTRS:
                    try:
                        setattr(bound, attr, getattr(fn, attr))
                    except (AttributeError, TypeError):  # pragma: no cover - defensive
                        pass
            else:
                # A singleton with no league to bind (e.g. ``pff_leagues``). Alias it
                # per league anyway so the naming stays uniform; they are the same call.
                bound = fn
            alias = f"pff_{slug}_{name[len('pff_') :]}"
            try:
                bound.__name__ = alias
                bound.__qualname__ = alias
            except (AttributeError, TypeError):  # pragma: no cover - defensive
                pass
            namespace[alias] = bound
            names.append(alias)
    return names


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
