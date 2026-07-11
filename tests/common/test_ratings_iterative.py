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


def _venue_slate(neutral: bool) -> pl.DataFrame:
    """A multi-game, venue-asymmetric slate where hfa genuinely moves the fit.

    (A single balanced game is degenerate -- the fixed point returns the raw
    values regardless of hfa, so it can't distinguish neutral-zeroing from a
    no-op. This slate has unbalanced venues + a venue-flipped rematch, so the
    hfa term actually bites when ``neutral`` is False -- see the teeth check in
    ``test_neutral_site_zeroes_home_edge``.)
    """
    rows = [
        ("A", "B", 110.0, 95.0, True, neutral),
        ("B", "A", 95.0, 110.0, False, neutral),
        ("A", "C", 108.0, 90.0, True, neutral),
        ("C", "A", 90.0, 108.0, False, neutral),
        ("B", "C", 102.0, 98.0, True, neutral),
        ("C", "B", 98.0, 102.0, False, neutral),
        ("A", "B", 112.0, 96.0, False, neutral),  # rematch, venues flipped
        ("B", "A", 96.0, 112.0, True, neutral),
    ]
    return pl.DataFrame(rows, schema=["team_id", "opp_team_id", "off", "def", "is_home", "neutral_site"], orient="row")


def test_neutral_site_zeroes_home_edge() -> None:
    """On an all-neutral slate the hfa must drop out entirely: the fixed point
    with a large hfa must be identical to the one with hfa=0 -- while the same
    slate marked non-neutral must NOT (proving the equality has teeth)."""
    kw = dict(
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off",
        def_col="def",
        home_col="is_home",
        neutral_col="neutral_site",
        baseline=100.0,
    )
    # Teeth: on a real (non-neutral) slate a large hfa changes the fit.
    assert not iterative_opponent_adjust(_venue_slate(False), hfa=10.0, **kw).equals(
        iterative_opponent_adjust(_venue_slate(False), hfa=0.0, **kw)
    )
    # Property: on an all-neutral slate the hfa is zeroed out -> hfa=10 == hfa=0.
    assert iterative_opponent_adjust(_venue_slate(True), hfa=10.0, **kw).equals(
        iterative_opponent_adjust(_venue_slate(True), hfa=0.0, **kw)
    )
