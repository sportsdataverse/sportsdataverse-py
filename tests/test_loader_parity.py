"""Smoke tests for NHL/PWHL loader naming-parity aliases and games-manifest loaders.

Verifies fastRhockey (R) parity per sdv-py PR feat/loader-parity-aliases:

NHL aliases (load_nhl_*_box -> load_nhl_*_boxscore/s):
  load_nhl_team_box, load_nhl_player_box, load_nhl_skater_box, load_nhl_goalie_box

PWHL aliases (load_pwhl_*_box -> load_pwhl_*_boxscores, load_pwhl_schedule -> _schedules):
  load_pwhl_team_box, load_pwhl_player_box, load_pwhl_skater_box, load_pwhl_goalie_box,
  load_pwhl_schedule

Games-manifest loaders (no ``seasons`` arg):
  load_nhl_games, load_pwhl_games

Live tests are gated behind ``SDV_PY_LIVE_TESTS=1``.
"""

from __future__ import annotations

import polars as pl
import pytest

import sportsdataverse.nhl as nhl
import sportsdataverse.pwhl as pwhl
from sportsdataverse.nhl import (
    load_nhl_goalie_box,
    load_nhl_goalie_boxscores,
    load_nhl_player_box,
    load_nhl_player_boxscore,
    load_nhl_skater_box,
    load_nhl_skater_boxscores,
    load_nhl_team_box,
    load_nhl_team_boxscore,
)
from sportsdataverse.nhl.nhl_loaders_extra import load_nhl_games
from sportsdataverse.pwhl import (
    load_pwhl_goalie_box,
    load_pwhl_goalie_boxscores,
    load_pwhl_player_box,
    load_pwhl_player_boxscores,
    load_pwhl_schedule,
    load_pwhl_schedules,
    load_pwhl_skater_box,
    load_pwhl_skater_boxscores,
    load_pwhl_team_box,
    load_pwhl_team_boxscores,
)
from sportsdataverse.pwhl.pwhl_loaders_extra import load_pwhl_games
from tests.conftest import skip_if_no_live

# ---------------------------------------------------------------------------
# Import / callability checks — all 11 new names must be importable
# ---------------------------------------------------------------------------

_NHL_ALIASES = [
    "load_nhl_team_box",
    "load_nhl_player_box",
    "load_nhl_skater_box",
    "load_nhl_goalie_box",
    "load_nhl_games",
]

_PWHL_ALIASES = [
    "load_pwhl_team_box",
    "load_pwhl_player_box",
    "load_pwhl_skater_box",
    "load_pwhl_goalie_box",
    "load_pwhl_schedule",
    "load_pwhl_games",
]


@pytest.mark.parametrize("name", _NHL_ALIASES)
def test_nhl_alias_is_importable_and_callable(name):
    fn = getattr(nhl, name)
    assert callable(fn), f"sportsdataverse.nhl.{name} is not callable"


@pytest.mark.parametrize("name", _PWHL_ALIASES)
def test_pwhl_alias_is_importable_and_callable(name):
    fn = getattr(pwhl, name)
    assert callable(fn), f"sportsdataverse.pwhl.{name} is not callable"


def test_all_11_new_names_are_present():
    """Belt-and-suspenders: every new name is in the right namespace."""
    for name in _NHL_ALIASES:
        assert hasattr(nhl, name), f"Missing from sportsdataverse.nhl: {name}"
    for name in _PWHL_ALIASES:
        assert hasattr(pwhl, name), f"Missing from sportsdataverse.pwhl: {name}"


# ---------------------------------------------------------------------------
# __all__ coverage — aliases declared in __all__
# ---------------------------------------------------------------------------


def test_nhl_aliases_in_all():
    from sportsdataverse.nhl import nhl_loaders_extra

    for name in [
        "load_nhl_team_box",
        "load_nhl_player_box",
        "load_nhl_skater_box",
        "load_nhl_goalie_box",
        "load_nhl_games",
    ]:
        assert name in nhl_loaders_extra.__all__, f"{name} missing from nhl_loaders_extra.__all__"


def test_pwhl_aliases_in_all():
    from sportsdataverse.pwhl import pwhl_loaders_extra

    for name in [
        "load_pwhl_team_box",
        "load_pwhl_player_box",
        "load_pwhl_skater_box",
        "load_pwhl_goalie_box",
        "load_pwhl_schedule",
        "load_pwhl_games",
    ]:
        assert name in pwhl_loaders_extra.__all__, f"{name} missing from pwhl_loaders_extra.__all__"


# ---------------------------------------------------------------------------
# Alias forwarding checks (no network needed)
# ---------------------------------------------------------------------------
# Thin-def aliases can't use ``alias is canonical`` identity, so we verify
# each alias delegates to its canonical by checking the function name exposed
# by each alias's __wrapped__ chain or by confirming the alias docstring.


def test_nhl_team_box_docstring_references_canonical():
    assert "load_nhl_team_boxscore" in load_nhl_team_box.__doc__


def test_nhl_player_box_docstring_references_canonical():
    assert "load_nhl_player_boxscore" in load_nhl_player_box.__doc__


def test_nhl_skater_box_docstring_references_canonical():
    assert "load_nhl_skater_boxscores" in load_nhl_skater_box.__doc__


def test_nhl_goalie_box_docstring_references_canonical():
    assert "load_nhl_goalie_boxscores" in load_nhl_goalie_box.__doc__


def test_pwhl_team_box_docstring_references_canonical():
    assert "load_pwhl_team_boxscores" in load_pwhl_team_box.__doc__


def test_pwhl_player_box_docstring_references_canonical():
    assert "load_pwhl_player_boxscores" in load_pwhl_player_box.__doc__


def test_pwhl_skater_box_docstring_references_canonical():
    assert "load_pwhl_skater_boxscores" in load_pwhl_skater_box.__doc__


def test_pwhl_goalie_box_docstring_references_canonical():
    assert "load_pwhl_goalie_boxscores" in load_pwhl_goalie_box.__doc__


def test_pwhl_schedule_docstring_references_canonical():
    assert "load_pwhl_schedules" in load_pwhl_schedule.__doc__


# ---------------------------------------------------------------------------
# Live smoke tests — alias shape parity + manifest loaders
# ---------------------------------------------------------------------------


@skip_if_no_live
def test_load_nhl_team_box_matches_canonical_shape():
    canonical = load_nhl_team_boxscore(seasons=2024)
    alias = load_nhl_team_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape
    assert alias.columns == canonical.columns


@skip_if_no_live
def test_load_nhl_player_box_matches_canonical_shape():
    canonical = load_nhl_player_boxscore(seasons=2024)
    alias = load_nhl_player_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_nhl_skater_box_matches_canonical_shape():
    canonical = load_nhl_skater_boxscores(seasons=2024)
    alias = load_nhl_skater_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_nhl_goalie_box_matches_canonical_shape():
    canonical = load_nhl_goalie_boxscores(seasons=2024)
    alias = load_nhl_goalie_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_pwhl_team_box_matches_canonical_shape():
    canonical = load_pwhl_team_boxscores(seasons=2024)
    alias = load_pwhl_team_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_pwhl_player_box_matches_canonical_shape():
    canonical = load_pwhl_player_boxscores(seasons=2024)
    alias = load_pwhl_player_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_pwhl_skater_box_matches_canonical_shape():
    canonical = load_pwhl_skater_boxscores(seasons=2024)
    alias = load_pwhl_skater_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_pwhl_goalie_box_matches_canonical_shape():
    canonical = load_pwhl_goalie_boxscores(seasons=2024)
    alias = load_pwhl_goalie_box(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_pwhl_schedule_matches_canonical_shape():
    canonical = load_pwhl_schedules(seasons=2024)
    alias = load_pwhl_schedule(seasons=2024)
    assert isinstance(alias, pl.DataFrame)
    assert alias.shape == canonical.shape


@skip_if_no_live
def test_load_nhl_games_returns_nonempty_dataframe():
    df = load_nhl_games()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_nhl_games_pandas_flag():
    import pandas as pd

    df = load_nhl_games(return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@skip_if_no_live
def test_load_pwhl_games_returns_nonempty_dataframe():
    df = load_pwhl_games()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


@skip_if_no_live
def test_load_pwhl_games_pandas_flag():
    import pandas as pd

    df = load_pwhl_games(return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
