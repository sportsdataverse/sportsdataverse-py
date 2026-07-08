"""Tests for SoS / Quad résumé / WAB (``mbb_strength_of_schedule``)."""

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule


def _ratings() -> pl.DataFrame:
    # team T plus four opponents with known engine ranks
    return pl.DataFrame(
        {
            "season": [2024] * 5,
            "team_id": ["T", "A", "B", "C", "D"],
            "adj_em": [10.0, 25.0, 15.0, 5.0, -5.0],
            "rank": [40, 10, 50, 170, 300],
        }
    )


def _results() -> pl.DataFrame:
    # T: home W vs A(r10)=Q1, home L vs B(r50)=Q2, away W vs C(r170)=Q3
    # (away q3 bound 240), neutral L vs D(r300)=Q4
    return pl.DataFrame(
        {
            "game_id": ["g1", "g2", "g3", "g4"],
            "season": [2024] * 4,
            "home_team_id": ["T", "T", "C", "T"],
            "away_team_id": ["A", "B", "T", "D"],
            "home_score": [80, 70, 60, 65],
            "away_score": [75, 72, 63, 70],
            "neutral_site": [False, False, False, True],
        }
    )


def test_sos_is_mean_opponent_adj_em():
    out = strength_of_schedule(_results(), _ratings())
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    assert t["sos"] == pytest.approx((25.0 + 15.0 + 5.0 - 5.0) / 4)


def test_quad_assignment_by_venue():
    out = strength_of_schedule(_results(), _ratings())
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    assert (t["quad1_w"], t["quad1_l"]) == (1, 0)  # home vs rank 10
    assert (t["quad2_w"], t["quad2_l"]) == (0, 1)  # home vs rank 50
    assert (t["quad3_w"], t["quad3_l"]) == (1, 0)  # away vs rank 170 (bound 240)
    assert (t["quad4_w"], t["quad4_l"]) == (0, 1)  # neutral vs rank 300


def test_quality_wins_is_q1_plus_q2_wins():
    out = strength_of_schedule(_results(), _ratings())
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    assert t["quality_wins"] == 1


def test_sos_rank_descending():
    out = strength_of_schedule(_results(), _ratings())
    hardest = out.sort("sos", descending=True).row(0, named=True)
    assert hardest["sos_rank"] == 1


def test_output_schema_and_empty_input():
    cols = [
        "season",
        "team_id",
        "sos",
        "sos_rank",
        "wab",
        "quad1_w",
        "quad1_l",
        "quad2_w",
        "quad2_l",
        "quad3_w",
        "quad3_l",
        "quad4_w",
        "quad4_l",
        "quality_wins",
    ]
    out = strength_of_schedule(_results(), _ratings())
    assert out.columns == cols
    empty = strength_of_schedule(_results().head(0), _ratings())
    assert empty.columns == cols
    assert empty.height == 0


def test_join_key_dtype_guard_raises():
    bad = _ratings().with_columns(pl.col("team_id").cast(pl.Categorical))
    with pytest.raises(ValueError, match="dtype"):
        strength_of_schedule(_results(), bad)


def test_wab_positive_when_beating_hard_schedule():
    # T sweeps the same 4-game schedule (incl. the rank-10 team) -> above bubble
    sweep = _results().with_columns(
        pl.Series("home_score", [80, 80, 60, 80]),
        pl.Series("away_score", [75, 72, 80, 70]),
    )
    out = strength_of_schedule(sweep, _ratings())
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    assert t["wab"] > 0


def test_wab_matches_hand_computed_single_game():
    from scipy.stats import norm

    from sportsdataverse.mbb.mbb_prediction_constants import get_constants

    c = get_constants("mens")
    one = _results().head(1)  # T home W vs A (adj_em 25)
    out = strength_of_schedule(one, _ratings())
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    p_bubble = float(norm.cdf((c.em_scale * (c.bubble_adj_em - 25.0) + c.hfa) / c.margin_sd))
    assert t["wab"] == pytest.approx(1.0 - p_bubble)
