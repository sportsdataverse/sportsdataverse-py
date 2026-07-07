"""Unit tests for sportsdataverse.cfb.cfb_ratings.

Structural / contract tests on a small synthetic 2-team league (offline,
deterministic): output schema, the reference-team fill (dropped by the
underlying ridge's ``model.matrix``-style parameterization, re-added here
via the intercept), and the empty-input contract.
"""

from __future__ import annotations

import polars as pl

# See the NOTE in cfb_ratings.py: the package `__init__` re-exports the
# `cfb_adjusted_epa` *function*, shadowing the submodule of the same name, so
# the required-columns tuple is imported by name (not via the shadowed
# package attribute) here too.
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings

_EXPECTED_COLUMNS = {"team_id", "adj_off_epa", "adj_def_epa", "adj_net", "games"}

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
