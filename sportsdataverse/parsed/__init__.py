"""sportsdataverse.parsed — DataFrame-by-default mirror of the raw API.

The default ``sportsdataverse.{league}`` modules return raw ``Dict``
payloads — the parser layer is opt-in via ``return_parsed=True`` so
existing callers from 0.0.50 and earlier are unaffected. **This
namespace flips the default**: every wrapper imported from
``sportsdataverse.parsed.{league}`` returns a polars DataFrame by
default (or pandas via ``return_as_pandas=True``).

Usage::

    # Raw-Dict default (existing API, unchanged):
    from sportsdataverse.nba import espn_nba_scoreboard
    raw = espn_nba_scoreboard()                          # → Dict

    # DataFrame default (this module):
    from sportsdataverse.parsed.nba import espn_nba_scoreboard
    df = espn_nba_scoreboard()                           # → polars

    # Override in either direction:
    raw_again = espn_nba_scoreboard(return_parsed=False) # → Dict from parsed.*
    df_again  = espn_nba_scoreboard(return_parsed=True)  # → polars from raw

Available leagues:

* ``sportsdataverse.parsed.nba``
* ``sportsdataverse.parsed.wnba``
* ``sportsdataverse.parsed.mbb``
* ``sportsdataverse.parsed.wbb``
* ``sportsdataverse.parsed.cfb``
* ``sportsdataverse.parsed.nfl``
* ``sportsdataverse.parsed.mlb``
* ``sportsdataverse.parsed.nhl``

Wrappers that don't have a registered parser (e.g. helper / loader
functions, or endpoints whose payload doesn't fit a tidy DataFrame
shape) pass through unchanged from the raw module.
"""

from __future__ import annotations

import inspect
import sys
import types

from sportsdataverse import (
    cfb as _cfb_raw,
    mbb as _mbb_raw,
    mlb as _mlb_raw,
    nba as _nba_raw,
    nfl as _nfl_raw,
    nhl as _nhl_raw,
    wbb as _wbb_raw,
    wnba as _wnba_raw,
)


def _wrap_default_parsed(fn):
    """Wrap an ESPN factory wrapper so ``return_parsed=True`` is the
    new default. Pass-through for callables that don't accept it.

    Preserves ``__name__`` / ``__qualname__`` / ``__doc__`` (with a
    trailing note about the new default) so ``help()`` / IDE
    introspection still works.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn
    if "return_parsed" not in sig.parameters:
        return fn  # no parser registered — pass through unchanged

    def wrapper(*args, **kwargs):
        kwargs.setdefault("return_parsed", True)
        return fn(*args, **kwargs)

    wrapper.__name__ = getattr(fn, "__name__", "wrapper")
    wrapper.__qualname__ = getattr(fn, "__qualname__", wrapper.__name__)
    base_doc = (fn.__doc__ or "").rstrip()
    wrapper.__doc__ = (
        f"{base_doc}\n\n"
        f"**Note:** Imported from ``sportsdataverse.parsed.*`` — "
        f"``return_parsed=True`` is the default. Pass ``return_parsed=False`` "
        f"to recover the raw ``Dict`` behaviour of the underlying wrapper."
    )
    return wrapper


def _build_parsed_module(name: str, raw_module: types.ModuleType) -> types.ModuleType:
    """Build a virtual module that mirrors ``raw_module`` but flips the
    ``return_parsed`` default on every wrapper that supports it.

    Pure callables without ``return_parsed`` in their signature (loaders,
    helpers, non-ESPN wrappers) pass through unchanged. Constants /
    non-callables are also passed through.
    """
    full_name = f"sportsdataverse.parsed.{name}"
    mod = types.ModuleType(full_name)
    mod.__doc__ = (
        f"DataFrame-by-default mirror of :mod:`sportsdataverse.{name}`. "
        f"Every wrapper that supports ``return_parsed=True`` has that as "
        f"its default in this module. See "
        f":mod:`sportsdataverse.parsed` for the full design."
    )
    exposed = []
    for attr_name in dir(raw_module):
        if attr_name.startswith("_"):
            continue
        attr = getattr(raw_module, attr_name)
        if callable(attr):
            setattr(mod, attr_name, _wrap_default_parsed(attr))
        else:
            setattr(mod, attr_name, attr)
        exposed.append(attr_name)
    mod.__all__ = exposed
    return mod


# ---------------------------------------------------------------------------
# Build the 8 per-league sub-modules + register them in sys.modules so
# ``from sportsdataverse.parsed.nba import espn_nba_scoreboard`` works
# without a per-league file on disk.
# ---------------------------------------------------------------------------

_LEAGUE_RAW_MODULES = {
    "nba":  _nba_raw,
    "wnba": _wnba_raw,
    "mbb":  _mbb_raw,
    "wbb":  _wbb_raw,
    "cfb":  _cfb_raw,
    "nfl":  _nfl_raw,
    "mlb":  _mlb_raw,
    "nhl":  _nhl_raw,
}

for _league, _raw in _LEAGUE_RAW_MODULES.items():
    _parsed_mod = _build_parsed_module(_league, _raw)
    globals()[_league] = _parsed_mod
    sys.modules[f"sportsdataverse.parsed.{_league}"] = _parsed_mod

# Expose the league names + the helpers themselves
__all__ = sorted(_LEAGUE_RAW_MODULES) + [
    "_wrap_default_parsed",
    "_build_parsed_module",
]


# ---------------------------------------------------------------------------
# Cleanup module-level imports that shouldn't leak into the namespace
# ---------------------------------------------------------------------------

# These are imported above with underscore aliases so they don't leak —
# but the bare names ``nba``, ``mlb``, etc. above DO leak intentionally,
# pointing at the parsed-default sub-modules.
del _cfb_raw, _mbb_raw, _mlb_raw, _nba_raw, _nfl_raw, _nhl_raw, _wbb_raw, _wnba_raw
del _LEAGUE_RAW_MODULES, _league, _raw, _parsed_mod
