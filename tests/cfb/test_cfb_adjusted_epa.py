"""Unit tests for sportsdataverse.cfb.cfb_adjusted_epa.

Structural / contract tests on a small synthetic league (offline, deterministic).
Full byte-for-value parity against the R ``adjust_epa`` is validated in the
cfbfastR-cfb-data ``team_summaries`` integration suite; here we lock the public
contract: output schema, the net = off - def identity, the valid-games filter,
clean rankings, the pandas option, and the missing-column guard.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb import cfb_adjusted_epa, cfb_adjusted_epa_by_game

_EXPECTED_COLUMNS = {
    "team_id",
    "pos_team",
    "valid_games",
    "adj_off_epa",
    "adj_def_epa",
    "off_strength_faced",
    "def_strength_faced",
    "net_adj_epa",
    "adj_off_epa_rank",
    "adj_def_epa_rank",
    "net_adj_epa_rank",
}


def _synthetic_pbp() -> pl.DataFrame:
    """A deterministic 4-team league: every ordered pairing is a game in which
    both teams take offensive snaps, so each team has plenty of valid games."""
    teams = ["1", "2", "3", "4"]
    names = {t: f"T{t}" for t in teams}
    rng = np.random.default_rng(0)
    rows: list[dict[str, object]] = []
    gid = 0
    for home in teams:
        for away in teams:
            if home == away:
                continue
            gid += 1
            week = (gid - 1) // 2 + 1  # 2 games per week
            for off, dfn in ((home, away), (away, home)):
                for _ in range(20):
                    rows.append(
                        {
                            "game_id": gid,
                            "week": week,
                            "pos_team": names[off],
                            "pos_team_id": off,
                            "def_pos_team_id": dfn,
                            "home": names[home],
                            "neutral_site": False,
                            "EPA": float(rng.normal()),
                            "pass": 1,
                            "rush": 0,
                            "wp_before": 0.5,
                        }
                    )
    return pl.DataFrame(rows)


def test_schema_and_net_identity() -> None:
    df = cfb_adjusted_epa(_synthetic_pbp())
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) == _EXPECTED_COLUMNS
    assert df.height == 4
    net = df["adj_off_epa"] - df["adj_def_epa"]
    assert (net - df["net_adj_epa"]).abs().max() < 1e-9
    assert df["valid_games"].min() >= 2


def test_rankings_are_permutations() -> None:
    df = cfb_adjusted_epa(_synthetic_pbp())
    for rank_col in ("adj_off_epa_rank", "adj_def_epa_rank", "net_adj_epa_rank"):
        assert sorted(df[rank_col].to_list()) == [1.0, 2.0, 3.0, 4.0]


def test_return_as_pandas() -> None:
    import pandas as pd

    out = cfb_adjusted_epa(_synthetic_pbp(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert set(out.columns) == _EXPECTED_COLUMNS


def test_accepts_pandas_input() -> None:
    out = cfb_adjusted_epa(_synthetic_pbp().to_pandas())
    assert isinstance(out, pl.DataFrame)
    assert out.height == 4


def test_missing_required_column_raises() -> None:
    with pytest.raises(KeyError):
        cfb_adjusted_epa(_synthetic_pbp().drop("EPA"))


def test_stable_across_runs() -> None:
    # Stable within float tolerance (polars' threaded group-by reductions can
    # differ by a last-ULP across runs, so exact equality is too strict).
    a = cfb_adjusted_epa(_synthetic_pbp()).sort("team_id")
    b = cfb_adjusted_epa(_synthetic_pbp()).sort("team_id")
    assert a.columns == b.columns
    for col in a.columns:
        if a.schema[col].is_numeric():
            assert (a[col] - b[col]).abs().max() < 1e-9
        else:
            assert a[col].to_list() == b[col].to_list()


_BY_GAME_COLUMNS = {
    "game_id",
    "week",
    "team_id",
    "opponent_id",
    "pos_team",
    "raw_off_epa",
    "adj_off_epa",
    "raw_def_epa",
    "adj_def_epa",
    "off_strength_faced",
    "def_strength_faced",
    "net_adj_epa",
}


def test_by_game_schema_and_grain() -> None:
    df = cfb_adjusted_epa_by_game(_synthetic_pbp())
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) == _BY_GAME_COLUMNS
    # one row per (game, team): 12 games x 2 teams
    assert df.height == 24
    assert df.select(["game_id", "team_id"]).unique().height == 24
    assert df["raw_off_epa"].is_not_null().all()


def test_by_game_is_walk_forward() -> None:
    df = cfb_adjusted_epa_by_game(_synthetic_pbp())
    # week 1 has no prior weeks -> no opponent model -> null adjustments (leak-free)
    wk1 = df.filter(pl.col("week") == 1)
    assert wk1.height > 0
    assert wk1["adj_off_epa"].is_null().all()
    assert wk1["adj_def_epa"].is_null().all()
    # later weeks have a prior fit -> adjusted values present
    assert df.filter(pl.col("week") >= 2)["adj_off_epa"].is_not_null().any()
    # net = off - def where both present
    both = df.filter(pl.col("adj_off_epa").is_not_null() & pl.col("adj_def_epa").is_not_null())
    assert ((both["adj_off_epa"] - both["adj_def_epa"]) - both["net_adj_epa"]).abs().max() < 1e-9


def test_by_game_return_as_pandas() -> None:
    import pandas as pd

    out = cfb_adjusted_epa_by_game(_synthetic_pbp(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert set(out.columns) == _BY_GAME_COLUMNS


def test_by_game_requires_week() -> None:
    with pytest.raises(KeyError):
        cfb_adjusted_epa_by_game(_synthetic_pbp().drop("week"))
