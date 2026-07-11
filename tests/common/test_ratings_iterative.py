"""Unit tests for the shared MBB/NBA iterative fixed point (T7.2).

The per-league byte-for-byte regression against real data lives in each
league's own ``test_*_team_ratings*`` suite, which re-runs unchanged after
the retarget. These tests lock in the pure-function behavior directly,
including the ``baseline=None`` (MBB: data-mean) vs ``baseline=<float>``
(NBA: fitted constant) branch both leagues rely on.
"""

import polars as pl

from sportsdataverse._common.ratings import iterative_opponent_adjust


def _round_robin(base: float = 104.0) -> pl.DataFrame:
    strength = {"A": 10.0, "B": 0.0, "C": -10.0}
    rows = []
    for t in "ABC":
        for o in "ABC":
            if t == o:
                continue
            rows.append((t, o, base + strength[t] - strength[o], base - strength[t] + strength[o], True, False))
    return pl.DataFrame(rows, schema=["team_id", "opp_team_id", "off", "def", "is_home", "neutral_site"], orient="row")


def test_empty_returns_schema() -> None:
    out = iterative_opponent_adjust(
        pl.DataFrame(
            schema={
                "team_id": pl.Utf8,
                "opp_team_id": pl.Utf8,
                "off": pl.Float64,
                "def": pl.Float64,
                "is_home": pl.Boolean,
                "neutral_site": pl.Boolean,
            }
        ),
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off",
        def_col="def",
        home_col="is_home",
        neutral_col="neutral_site",
        hfa=3.0,
    )
    assert out.columns == ["team_id", "adj_off", "adj_def", "adj_net", "raw_off", "raw_def", "games"]
    assert out.height == 0


def test_adj_net_ordering_with_data_derived_baseline() -> None:
    """``baseline=None`` -- MBB's behavior (average computed from the data)."""
    out = iterative_opponent_adjust(
        _round_robin(),
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off",
        def_col="def",
        home_col="is_home",
        neutral_col="neutral_site",
        hfa=3.0,
        baseline=None,
    ).sort("adj_net", descending=True)
    assert out["team_id"].to_list() == ["A", "B", "C"]
    assert out.filter(pl.col("team_id") == "A")["games"].item() == 2


def test_adj_net_ordering_with_fitted_baseline() -> None:
    """``baseline=<float>`` -- NBA's behavior (a fitted external constant)."""
    out = iterative_opponent_adjust(
        _round_robin(),
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off",
        def_col="def",
        home_col="is_home",
        neutral_col="neutral_site",
        hfa=3.0,
        baseline=104.0,
    ).sort("adj_net", descending=True)
    assert out["team_id"].to_list() == ["A", "B", "C"]


def test_neutral_site_zeroes_home_edge() -> None:
    rows = [
        ("A", "B", 105.0, 103.0, True, True),
        ("B", "A", 103.0, 105.0, False, True),
    ]
    game_eff = pl.DataFrame(
        rows, schema=["team_id", "opp_team_id", "off", "def", "is_home", "neutral_site"], orient="row"
    )
    out = iterative_opponent_adjust(
        game_eff,
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off",
        def_col="def",
        home_col="is_home",
        neutral_col="neutral_site",
        hfa=10.0,  # a large hfa would visibly bias a non-neutral game; here it must not.
        baseline=104.0,
    )
    assert out.height == 2
