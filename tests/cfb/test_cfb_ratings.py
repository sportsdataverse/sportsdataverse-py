"""Unit tests for sportsdataverse.cfb.cfb_ratings.

Structural / contract tests on a small synthetic 2-team league (offline,
deterministic): output schema, the reference-team fill (dropped by the
underlying ridge's ``model.matrix``-style parameterization, re-added here
via the intercept), and the empty-input contract.
"""

from __future__ import annotations

import datetime as dt
import sys

import pandas as pd
import polars as pl
import pytest

# See the NOTE in cfb_ratings.py: `sportsdataverse/cfb/__init__.py` re-exports
# the `cfb_ratings` *function*, shadowing the submodule of the same name in
# the package's own namespace (same pattern as `cfb_adjusted_epa`) -- and per
# that same note, BOTH `from sportsdataverse.cfb import cfb_ratings as ...`
# AND `import sportsdataverse.cfb.cfb_ratings as ...` resolve to the function
# via getattr traversal, not the submodule. A `sys.modules` lookup by the
# fully-qualified string key sidesteps attribute traversal entirely, so
# `monkeypatch.setattr(_cfb_ratings_mod, "load_cfb_pbp", ...)` below patches
# the real submodule's namespace (the one `cfb_ratings()` actually reads from).
from sportsdataverse.cfb.cfb_ratings import cfb_ratings, efficiency_ratings, fei_ratings, special_teams_ratings

_cfb_ratings_mod = sys.modules["sportsdataverse.cfb.cfb_ratings"]

_EXPECTED_COLUMNS = {"team_id", "adj_off_epa", "adj_def_epa", "adj_net", "games"}
_FEI_EXPECTED_COLUMNS = {"team_id", "fei_off", "fei_def", "fei_net"}

# Real per-column dtypes for the 10 `cfb_adjusted_epa._REQUIRED_COLUMNS`.
# Hardcoded rather than an all-Utf8 schema: `_prepare`'s filter compares
# `pass`/`rush` to the int literal 1 and `wp_before` to float bounds, and
# polars type-checks those comparisons even against a zero-row frame -- an
# all-Utf8 empty frame raises ComputeError before ever reaching 0 rows.
_REQUIRED_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "pos_team": pl.Utf8,
    "pos_team_id": pl.Utf8,
    "def_pos_team_id": pl.Utf8,
    "home": pl.Utf8,
    "neutral_site": pl.Boolean,
    "EPA": pl.Float64,
    "pass": pl.Int64,
    "rush": pl.Int64,
    "wp_before": pl.Float64,
}


def _mini_plays() -> pl.DataFrame:
    """Two teams, A's offense deterministically better than B's."""
    rows = []
    for i in range(40):
        rows.append(
            {
                "game_id": f"G{i // 2}",
                "week": 1,
                "pos_team": "A",
                "pos_team_id": "A",
                "def_pos_team_id": "B",
                "home": "A",
                "EPA": 0.30,
                "pass": 1,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Pass Reception",
            }
        )
        rows.append(
            {
                "game_id": f"G{i // 2}",
                "week": 1,
                "pos_team": "B",
                "pos_team_id": "B",
                "def_pos_team_id": "A",
                "home": "A",
                "EPA": -0.30,
                "pass": 0,
                "rush": 1,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Rush",
            }
        )
    return pl.DataFrame(rows)


def test_efficiency_ratings_orders_teams() -> None:
    out = efficiency_ratings(_mini_plays())
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert a["adj_net"] > b["adj_net"]
    assert out.schema["team_id"] == pl.Utf8
    assert set(out.columns) >= _EXPECTED_COLUMNS


def test_efficiency_ratings_reference_team_present_at_baseline() -> None:
    # "A" sorts first among {"A", "B"} so the ridge drops it as the reference
    # level on both offense and defense; efficiency_ratings must re-add it at
    # the intercept (adj_net == 0), not silently omit it.
    out = efficiency_ratings(_mini_plays())
    assert set(out["team_id"].to_list()) == {"A", "B"}
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert a["adj_net"] == 0.0


def test_efficiency_ratings_empty_input_returns_documented_schema() -> None:
    empty = pl.DataFrame(schema=_REQUIRED_SCHEMA)
    out = efficiency_ratings(empty)
    assert out.height == 0
    assert out.schema == {
        "team_id": pl.Utf8,
        "adj_off_epa": pl.Float64,
        "adj_def_epa": pl.Float64,
        "adj_net": pl.Float64,
        "games": pl.Int64,
    }


def _mini_plays_with_st() -> pl.DataFrame:
    """``_mini_plays`` plus special-teams snaps (A better than B) and a no-ST team C."""
    rows = _mini_plays().to_dicts()
    for i in range(10):
        rows.append(
            {
                "game_id": f"GST{i}",
                "week": 1,
                "pos_team": "A",
                "pos_team_id": "A",
                "def_pos_team_id": "B",
                "home": "A",
                "EPA": 0.5,
                "pass": 0,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Kickoff",
            }
        )
        rows.append(
            {
                "game_id": f"GST{i}",
                "week": 1,
                "pos_team": "B",
                "pos_team_id": "B",
                "def_pos_team_id": "A",
                "home": "A",
                "EPA": -0.5,
                "pass": 0,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Punt",
            }
        )
    # Team C: pass/rush snaps only -- never appears on a special-teams play.
    for i in range(4):
        rows.append(
            {
                "game_id": f"GC{i}",
                "week": 1,
                "pos_team": "C",
                "pos_team_id": "C",
                "def_pos_team_id": "A",
                "home": "C",
                "EPA": 0.2,
                "pass": 1,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Pass Reception",
            }
        )
    return pl.DataFrame(rows)


def test_special_teams_ratings_orders_teams_and_fills_no_st_team() -> None:
    out = special_teams_ratings(_mini_plays_with_st())
    assert set(out.columns) == {"team_id", "adj_st_epa"}
    assert out.schema["team_id"] == pl.Utf8
    assert out.schema["adj_st_epa"] == pl.Float64
    assert set(out["team_id"].to_list()) == {"A", "B", "C"}

    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    c = out.filter(pl.col("team_id") == "C").row(0, named=True)
    assert a["adj_st_epa"] > b["adj_st_epa"]
    # C never appears on a special-teams play -> neutral fill (both sides land
    # on the shared intercept, which cancels in the off-minus-def net).
    assert c["adj_st_epa"] == 0.0


def test_special_teams_ratings_all_non_st_input_returns_zero_row_frame() -> None:
    out = special_teams_ratings(_mini_plays())
    assert out.height == 0
    assert out.schema == {"team_id": pl.Utf8, "adj_st_epa": pl.Float64}


def _mini_drives() -> pl.DataFrame:
    """Two teams, one drive per game per team; A's drives sum to +2.0 EPA, B's to -2.0."""
    rows = []
    for i in range(20):
        game_id = f"GD{i}"
        for _play in range(2):
            rows.append(
                {
                    "game_id": game_id,
                    "drive_id": f"{game_id}-A",
                    "pos_team": "A",
                    "pos_team_id": "A",
                    "def_pos_team_id": "B",
                    "home": "A",
                    "EPA": 0.05,
                    "pass": 1,
                    "rush": 0,
                    "wp_before": 0.5,
                    "neutral_site": False,
                    "play_type": "Pass Reception",
                }
            )
            rows.append(
                {
                    "game_id": game_id,
                    "drive_id": f"{game_id}-B",
                    "pos_team": "B",
                    "pos_team_id": "B",
                    "def_pos_team_id": "A",
                    "home": "A",
                    "EPA": -0.05,
                    "pass": 0,
                    "rush": 1,
                    "wp_before": 0.5,
                    "neutral_site": False,
                    "play_type": "Rush",
                }
            )
    return pl.DataFrame(rows)


def test_fei_ratings_orders_teams_by_net() -> None:
    out = fei_ratings(_mini_drives())
    assert set(out.columns) == _FEI_EXPECTED_COLUMNS
    assert out.schema["team_id"] == pl.Utf8
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert a["fei_net"] > b["fei_net"]


def test_fei_ratings_reference_team_present_at_baseline() -> None:
    # "A" sorts first among {"A", "B"} so the ridge drops it as the reference
    # level on both offense and defense; fei_ratings must re-add it at the
    # intercept (fei_net == 0), not silently omit it.
    out = fei_ratings(_mini_drives())
    assert set(out["team_id"].to_list()) == {"A", "B"}
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert a["fei_net"] == 0.0


def test_fei_ratings_empty_input_returns_documented_schema() -> None:
    empty = pl.DataFrame(schema={**_REQUIRED_SCHEMA, "drive_id": pl.Utf8, "play_type": pl.Utf8})
    out = fei_ratings(empty)
    assert out.height == 0
    assert out.schema == {
        "team_id": pl.Utf8,
        "fei_off": pl.Float64,
        "fei_def": pl.Float64,
        "fei_net": pl.Float64,
    }


# ---------------------------------------------------------------------------
# cfb_ratings() -- public as-of-date orchestrator (Task 1.4)
# ---------------------------------------------------------------------------

_SEASON = 2024
_GAME_IDS = [1000 + i for i in range(10)]  # int (Int64) -- deliberately not Utf8
_GAME_DATES = [dt.date(2024, 9, 1) + dt.timedelta(weeks=i) for i in range(10)]
_CUTOFF = _GAME_DATES[5]  # keeps games 0-4, drops games 5-9

_RATINGS_COLUMNS = [
    "season",
    "team_id",
    "adj_off_epa",
    "adj_def_epa",
    "adj_st_epa",
    "adj_net",
    "fei_off",
    "fei_def",
    "fei_net",
    "games",
    "off_rank",
    "def_rank",
    "net_rank",
    "net_z",
]

_RATINGS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_off_epa": pl.Float64,
    "adj_def_epa": pl.Float64,
    "adj_st_epa": pl.Float64,
    "adj_net": pl.Float64,
    "fei_off": pl.Float64,
    "fei_def": pl.Float64,
    "fei_net": pl.Float64,
    "games": pl.Int64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}


def _mini_season_plays() -> pl.DataFrame:
    """10 games, one per calendar week; A's offense deterministically beats B's."""
    rows = []
    for gid in _GAME_IDS:
        game_id = str(gid)
        rows.append(
            {
                "game_id": game_id,
                "pos_team": "A",
                "pos_team_id": "A",
                "def_pos_team_id": "B",
                "home": "A",
                "EPA": 0.30,
                "pass": 1,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Pass Reception",
                "drive_id": f"{game_id}-A",
            }
        )
        rows.append(
            {
                "game_id": game_id,
                "pos_team": "B",
                "pos_team_id": "B",
                "def_pos_team_id": "A",
                "home": "A",
                "EPA": -0.30,
                "pass": 0,
                "rush": 1,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Rush",
                "drive_id": f"{game_id}-B",
            }
        )
        # A couple of special-teams snaps so `special_teams_ratings` exercises
        # its ridge-fit path (not just the all-non-ST zero-row branch).
        rows.append(
            {
                "game_id": game_id,
                "pos_team": "A",
                "pos_team_id": "A",
                "def_pos_team_id": "B",
                "home": "A",
                "EPA": 0.4,
                "pass": 0,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Kickoff",
                "drive_id": f"{game_id}-STA",
            }
        )
        rows.append(
            {
                "game_id": game_id,
                "pos_team": "B",
                "pos_team_id": "B",
                "def_pos_team_id": "A",
                "home": "A",
                "EPA": -0.4,
                "pass": 0,
                "rush": 0,
                "wp_before": 0.5,
                "neutral_site": False,
                "play_type": "Punt",
                "drive_id": f"{game_id}-STB",
            }
        )
    return pl.DataFrame(rows)


def _mini_season_schedule() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": _GAME_IDS,
            "season": [_SEASON] * len(_GAME_IDS),
            "date": _GAME_DATES,
        }
    )


def _patch_loaders(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_cfb_ratings_mod, "load_cfb_pbp", lambda seasons, return_as_pandas=False: _mini_season_plays())
    monkeypatch.setattr(
        _cfb_ratings_mod, "load_cfb_schedule", lambda seasons, return_as_pandas=False: _mini_season_schedule()
    )


def test_cfb_ratings_full_schema_and_ranks(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch)

    out = cfb_ratings(_SEASON)

    assert out.columns == _RATINGS_COLUMNS
    assert out.schema == _RATINGS_SCHEMA
    assert out.schema["team_id"] == pl.Utf8
    assert set(out["team_id"].to_list()) == {"A", "B"}
    assert out["season"].to_list() == [_SEASON, _SEASON]

    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert a["adj_net"] > b["adj_net"]
    assert a["net_rank"] == 1
    assert b["net_rank"] == 2
    # adj_net is off-minus-def only -- special teams must NOT be folded in.
    assert a["adj_net"] == pytest.approx(a["adj_off_epa"] - a["adj_def_epa"])


def test_cfb_ratings_as_of_date_drops_later_games(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch)

    full = cfb_ratings(_SEASON)
    cutoff = cfb_ratings(_SEASON, as_of_date=_CUTOFF)

    full_games = full.filter(pl.col("team_id") == "A").row(0, named=True)["games"]
    cutoff_games = cutoff.filter(pl.col("team_id") == "A").row(0, named=True)["games"]
    assert cutoff_games < full_games


def test_cfb_ratings_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch)

    out = cfb_ratings(_SEASON, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


def test_cfb_ratings_empty_seasons_returns_documented_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_cfb_ratings_mod, "load_cfb_pbp", lambda seasons, return_as_pandas=False: pl.DataFrame())
    monkeypatch.setattr(_cfb_ratings_mod, "load_cfb_schedule", lambda seasons, return_as_pandas=False: pl.DataFrame())

    out = cfb_ratings(_SEASON)
    assert out.height == 0
    assert out.schema == _RATINGS_SCHEMA
