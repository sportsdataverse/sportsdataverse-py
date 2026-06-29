"""Tests for nba_tracking.aggregate_tracking_frames — identity + additivity oracles.

These are INDEPENDENT oracle tests:
- identity: aggregating a single frame reproduces its additive columns per entity.
- additivity: aggregating two frames sums counts and recomputes rates correctly.
"""

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets
from sportsdataverse.nba.nba_tracking import TRACKING_ENTITY_KEYS, aggregate_tracking_frames

FX = pathlib.Path("tests/fixtures/nba_engine/tracking")


def _frame(season_tag: str) -> pl.DataFrame:
    raw = json.loads((FX / f"leaguedashptstats_drives_player_{season_tag}.json").read_text())
    return parse_nba_stats_result_sets(raw)


# ---------------------------------------------------------------------------
# Structural / constant tests
# ---------------------------------------------------------------------------


def test_tracking_entity_keys_has_player_and_team() -> None:
    assert TRACKING_ENTITY_KEYS["Player"] == "player_id"
    assert TRACKING_ENTITY_KEYS["Team"] == "team_id"


# ---------------------------------------------------------------------------
# Never-raise contract
# ---------------------------------------------------------------------------


def test_empty_input_returns_zero_row_frame() -> None:
    result = aggregate_tracking_frames([], entity_key="player_id")
    assert isinstance(result, pl.DataFrame)
    assert result.shape[0] == 0


def test_single_empty_frame_returns_zero_row_frame() -> None:
    f = _frame("2324")
    empty = f.clear()
    result = aggregate_tracking_frames([empty], entity_key="player_id")
    assert result.shape[0] == 0


# ---------------------------------------------------------------------------
# Identity oracle: aggregate([single frame]) == original per entity
# ---------------------------------------------------------------------------


def test_identity_single_frame_unchanged() -> None:
    f = _frame("2324")
    agg = aggregate_tracking_frames([f], entity_key="player_id")

    # same number of unique entities
    assert agg.shape[0] == f["player_id"].n_unique()

    # join to compare per-entity counts
    j = f.join(agg, on="player_id", suffix="_agg")

    for c in [
        "drives",
        "drive_fgm",
        "drive_fga",
        "drive_ftm",
        "drive_fta",
        "drive_pts",
        "drive_passes",
        "drive_ast",
        "drive_tov",
        "drive_pf",
        "gp",
        "w",
        "l",
    ]:
        assert (j[c] == j[f"{c}_agg"]).all(), f"Column {c} mismatch in identity test"

    # recomputed fg_pct == exact integer ratio fgm/fga (ESPN source is pre-rounded to ~3dp)
    jj = j.filter(pl.col("drive_fga") > 0)
    exact_fg = jj["drive_fgm"].cast(pl.Float64) / jj["drive_fga"].cast(pl.Float64)
    diff = (jj["drive_fg_pct_agg"] - exact_fg).abs()
    assert diff.max() < 1e-9, "drive_fg_pct recompute mismatch in identity test"

    # recomputed ft_pct == exact integer ratio ftm/fta
    jj_ft = j.filter(pl.col("drive_fta") > 0)
    exact_ft = jj_ft["drive_ftm"].cast(pl.Float64) / jj_ft["drive_fta"].cast(pl.Float64)
    diff_ft = (jj_ft["drive_ft_pct_agg"] - exact_ft).abs()
    assert diff_ft.max() < 1e-9, "drive_ft_pct recompute mismatch in identity test"

    # non-aggregatable % columns are dropped
    for dropped in ["drive_pts_pct", "drive_passes_pct", "drive_ast_pct", "drive_tov_pct", "drive_pf_pct"]:
        assert dropped not in agg.columns, f"Column {dropped} should be dropped"


def test_recomputed_pct_null_when_zero_attempts() -> None:
    """A zero-attempt entity must get null fg_pct/ft_pct, not 0 or NaN.

    Unconditional: uses a crafted one-row frame with 0 attempts so the
    null-safe-divide path is always exercised regardless of fixture contents.
    """
    crafted = pl.DataFrame(
        {
            "player_id": [9999],
            "player_name": ["Zero Attempts"],
            "team_id": [1610612737],
            "team_abbreviation": ["ATL"],
            "drives": [3],
            "drive_fgm": [0],
            "drive_fga": [0],
            "drive_fg_pct": [None],
            "drive_ftm": [0],
            "drive_fta": [0],
            "drive_ft_pct": [None],
        },
        schema={
            "player_id": pl.Int64,
            "player_name": pl.String,
            "team_id": pl.Int64,
            "team_abbreviation": pl.String,
            "drives": pl.Int64,
            "drive_fgm": pl.Int64,
            "drive_fga": pl.Int64,
            "drive_fg_pct": pl.Float64,
            "drive_ftm": pl.Int64,
            "drive_fta": pl.Int64,
            "drive_ft_pct": pl.Float64,
        },
    )
    agg = aggregate_tracking_frames([crafted], entity_key="player_id")
    row = agg.filter(pl.col("player_id") == 9999).to_dicts()[0]
    assert row["drive_fga"] == 0
    assert row["drive_fg_pct"] is None, "drive_fg_pct must be null when drive_fga == 0"
    assert row["drive_fta"] == 0
    assert row["drive_ft_pct"] is None, "drive_ft_pct must be null when drive_fta == 0"


# ---------------------------------------------------------------------------
# Additivity oracle: aggregate([A, B]) sums counts, recomputes rates correctly
# ---------------------------------------------------------------------------


def test_additivity_two_seasons() -> None:
    a, b = _frame("2223"), _frame("2324")
    agg = aggregate_tracking_frames([a, b], entity_key="player_id")

    # find a player present in BOTH seasons
    common = set(a["player_id"].to_list()) & set(b["player_id"].to_list())
    assert len(common) > 0, "No player in common between the two seasons — fixtures may be wrong"
    pid = sorted(common)[0]

    ra = a.filter(pl.col("player_id") == pid).to_dicts()[0]
    rb = b.filter(pl.col("player_id") == pid).to_dicts()[0]
    rg = agg.filter(pl.col("player_id") == pid).to_dicts()[0]

    # additive counts must be summed exactly
    assert rg["drives"] == ra["drives"] + rb["drives"], "drives not summed correctly"
    assert rg["drive_fgm"] == ra["drive_fgm"] + rb["drive_fgm"], "drive_fgm not summed"
    assert rg["drive_fga"] == ra["drive_fga"] + rb["drive_fga"], "drive_fga not summed"
    assert rg["drive_ftm"] == ra["drive_ftm"] + rb["drive_ftm"], "drive_ftm not summed"
    assert rg["drive_fta"] == ra["drive_fta"] + rb["drive_fta"], "drive_fta not summed"
    assert rg["drive_pts"] == ra["drive_pts"] + rb["drive_pts"], "drive_pts not summed"
    assert rg["gp"] == ra["gp"] + rb["gp"], "gp not summed"
    assert rg["w"] == ra["w"] + rb["w"], "w not summed"
    assert rg["l"] == ra["l"] + rb["l"], "l not summed"

    # fg_pct must be recomputed from SUMMED makes/attempts, not averaged
    fgm = ra["drive_fgm"] + rb["drive_fgm"]
    fga = ra["drive_fga"] + rb["drive_fga"]
    if fga > 0:
        expected_fg_pct = fgm / fga
        assert abs(rg["drive_fg_pct"] - expected_fg_pct) < 1e-9, (
            f"drive_fg_pct should be {expected_fg_pct}, got {rg['drive_fg_pct']}"
        )
    else:
        assert rg["drive_fg_pct"] is None, "drive_fg_pct should be null when 0 attempts"

    ftm = ra["drive_ftm"] + rb["drive_ftm"]
    fta = ra["drive_fta"] + rb["drive_fta"]
    if fta > 0:
        expected_ft_pct = ftm / fta
        assert abs(rg["drive_ft_pct"] - expected_ft_pct) < 1e-9, (
            f"drive_ft_pct should be {expected_ft_pct}, got {rg['drive_ft_pct']}"
        )
    else:
        assert rg["drive_ft_pct"] is None, "drive_ft_pct should be null when 0 attempts"

    # non-aggregatable % columns must not appear
    for dropped in ["drive_pts_pct", "drive_passes_pct", "drive_ast_pct", "drive_tov_pct", "drive_pf_pct"]:
        assert dropped not in rg, f"Column {dropped} should have been dropped"


def test_additivity_player_only_in_one_season() -> None:
    """A player present in only one season should appear with that season's values."""
    a, b = _frame("2223"), _frame("2324")
    agg = aggregate_tracking_frames([a, b], entity_key="player_id")

    ids_a = set(a["player_id"].to_list())
    ids_b = set(b["player_id"].to_list())
    only_in_a = sorted(ids_a - ids_b)
    if not only_in_a:
        pytest.skip("No player exclusive to season a in these fixtures")

    pid = only_in_a[0]
    ra = a.filter(pl.col("player_id") == pid).to_dicts()[0]
    rg = agg.filter(pl.col("player_id") == pid).to_dicts()[0]

    assert rg["drives"] == ra["drives"]
    assert rg["drive_fgm"] == ra["drive_fgm"]
