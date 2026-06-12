"""Offline smoke coverage for the generated ``espn_<league>_*`` families.

Each ESPN league module exposes a large ``espn_<lg>_*`` surface generated from one
shared codegen path plus the :mod:`sportsdataverse._common_espn_parsers` layer.
Almost all of it is otherwise touched only by gated live tests, so the long tail
sits at zero offline coverage. This module does two things per league, offline:

1. A **contract** check -- the codegen emits a substantial, fully-callable family.
2. A **representative execution** -- ``espn_<lg>_team_roster`` is driven through its
   real wrapper + parser with the ``_get`` HTTP chokepoint mocked by a captured
   fixture, exercising the shared path once per league.
"""

from __future__ import annotations

import importlib

import polars as pl
import pytest

from tests.conftest import load_fixture

ESPN_LEAGUES = ["nba", "wnba", "mbb", "wbb", "nfl", "cfb", "mlb", "nhl"]


@pytest.mark.parametrize("lg", ESPN_LEAGUES)
def test_espn_family_is_substantial_and_callable(lg):
    """Contract: each league emits a non-trivial, fully-callable ``espn_<lg>_*`` family."""
    mod = importlib.import_module(f"sportsdataverse.{lg}")
    fns = [f for f in dir(mod) if f.startswith(f"espn_{lg}_")]
    assert len(fns) >= 20, f"{lg}: only {len(fns)} espn_{lg}_* functions emitted"
    assert all(callable(getattr(mod, f)) for f in fns), f"{lg}: non-callable in espn family"


@pytest.mark.parametrize("lg", ESPN_LEAGUES)
def test_espn_team_roster_parses_offline(lg, monkeypatch):
    """Drive ``espn_<lg>_team_roster`` through its parser with the HTTP layer mocked.

    Restricted to the codegen ``*_espn_ext`` wrappers (which route through the shared
    ``_get`` chokepoint); a couple of leagues keep a hand-written ``team_roster`` that
    calls ``download`` directly and is covered elsewhere -- those are skipped here.
    """
    mod = importlib.import_module(f"sportsdataverse.{lg}")
    fn = getattr(mod, f"espn_{lg}_team_roster", None)
    fn_mod = getattr(fn, "__module__", "") or ""
    if fn is None or not fn_mod.endswith("_espn_ext"):
        pytest.skip(f"{lg}: espn_{lg}_team_roster is not the codegen _espn_ext wrapper")

    ext = importlib.import_module(fn_mod)
    fixture = load_fixture("espn", f"team_roster_{lg}")
    monkeypatch.setattr(ext, "_get", lambda *a, **k: fixture)

    df = fn(event_id="401", team_id="4")
    assert isinstance(df, pl.DataFrame)

    raw = fn(event_id="401", team_id="4", return_parsed=False)
    assert raw is fixture  # return_parsed=False returns the raw payload unchanged
