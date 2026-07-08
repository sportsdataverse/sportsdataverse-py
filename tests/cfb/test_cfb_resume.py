"""Résumé-metric tests (T2.1 Phase 3).

Task 3.1 covers rating-based strength of schedule, quality wins, and game control
on a hand-computable synthetic schedule; the public ``cfb_resume`` is exercised via
monkeypatched ``cfb_ratings`` / ``load_cfb_schedule`` so no network or loader is hit.
"""

from __future__ import annotations

import sys

import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.cfb.cfb_prediction_constants import get_constants
from sportsdataverse.cfb.cfb_resume import cfb_resume

_mod = sys.modules["sportsdataverse.cfb.cfb_resume"]
_C = get_constants("modern")


def _ratings() -> pl.DataFrame:
    """Team under test T (0.0) + four opponents of known strength (team_id Utf8)."""
    return pl.DataFrame(
        {
            "season": [2023] * 5,
            "team_id": ["T", "A", "B", "C", "D"],
            "adj_net": [0.0, 0.30, 0.10, -0.10, -0.30],
        }
    )


def _schedule() -> pl.DataFrame:
    """T's 4 games: home wins vs A/B, away loss to C, away blowout win at D."""
    return pl.DataFrame(
        {
            "game_id": ["1", "2", "3", "4"],
            "season": [2023] * 4,
            "home_team_id": ["T", "T", "C", "D"],
            "away_team_id": ["A", "B", "T", "T"],
            "home_score": [28, 35, 24, 10],
            "away_score": [21, 14, 17, 42],
            "neutral_site": [False, False, False, False],
        }
    )


def _run(monkeypatch, ratings=None, schedule=None) -> pl.DataFrame:
    monkeypatch.setattr(_mod, "cfb_ratings", lambda *a, **k: ratings if ratings is not None else _ratings())
    monkeypatch.setattr(_mod, "load_cfb_schedule", lambda *a, **k: schedule if schedule is not None else _schedule())
    return cfb_resume(2023)


def test_sos_is_mean_opponent_adj_net(monkeypatch: pytest.MonkeyPatch) -> None:
    """SoS = mean of the four opponents' adj_net (0.30+0.10-0.10-0.30)/4 = 0.0."""
    out = _run(monkeypatch)
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    assert t["sos"] == pytest.approx(0.0)


def test_quality_wins_counts_only_wins_over_strong_opponents(monkeypatch: pytest.MonkeyPatch) -> None:
    """T beat A (0.30) + B (0.10) + D (-0.30); only the two >= threshold (0.0) count."""
    out = _run(monkeypatch)
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    assert t["quality_wins"] == 2


def test_game_control_is_mean_win_expectancy_of_margins(monkeypatch: pytest.MonkeyPatch) -> None:
    """game_control = mean Phi(team_margin / margin_sd) over T's four games."""
    out = _run(monkeypatch)
    t = out.filter(pl.col("team_id") == "T").row(0, named=True)
    md = _C.margin_sd
    expected = float(sum(norm.cdf(m / md) for m in (7.0, 21.0, -7.0, 32.0)) / 4)
    assert t["game_control"] == pytest.approx(expected)
    assert 0.0 < t["game_control"] < 1.0


def test_game_control_rises_with_blowout_margins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Widening every margin can only raise game control."""
    base = _run(monkeypatch).filter(pl.col("team_id") == "T").row(0, named=True)["game_control"]
    blow = _schedule().with_columns(home_score=pl.Series([49, 56, 24, 3]), away_score=pl.Series([0, 0, 17, 49]))
    bigger = _run(monkeypatch, schedule=blow).filter(pl.col("team_id") == "T").row(0, named=True)["game_control"]
    assert bigger > base


def test_sos_rank_and_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Output carries the documented columns + a dense SoS rank."""
    out = _run(monkeypatch)
    assert out.columns == ["season", "team_id", "sos", "sos_rank", "quality_wins", "game_control", "wab"]
    assert out.schema["team_id"] == pl.Utf8
    assert out["sos_rank"].min() == 1


def test_join_key_dtype_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """The schedule normalizes its ids to Utf8; a ratings frame whose team_id is NOT
    Utf8 (should never happen from cfb_ratings) trips the pre-join dtype guard."""
    bad_ratings = pl.DataFrame(
        {"season": [2023] * 5, "team_id": [1, 2, 3, 4, 5], "adj_net": [0.0, 0.3, 0.1, -0.1, -0.3]}
    )
    with pytest.raises(AssertionError):
        _run(monkeypatch, ratings=bad_ratings)


def test_normalizes_real_loader_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real load_cfb_schedule ships home_id/away_id (Int32) + home_points/away_points."""
    real = pl.DataFrame(
        {
            "game_id": [1, 2, 3, 4],
            "season": [2023] * 4,
            "home_id": [100, 100, 300, 400],  # Int -> must be cast + renamed
            "away_id": [200, 500, 100, 100],
            "home_points": [28, 35, 24, 10],
            "away_points": [21, 14, 17, 42],
            "neutral_site": [False, False, False, False],
        }
    )
    ratings = pl.DataFrame(
        {"season": [2023] * 5, "team_id": ["100", "200", "500", "300", "400"], "adj_net": [0.0, 0.3, 0.1, -0.1, -0.3]}
    )
    out = _run(monkeypatch, ratings=ratings, schedule=real)
    assert out.filter(pl.col("team_id") == "100").row(0, named=True)["quality_wins"] == 2


def test_wab_positive_when_beating_strong_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    """T (bubble-average) went 3-1 beating strong A/B -- a bubble team wins fewer of
    those games, so WAB > 0."""
    out = _run(monkeypatch)
    assert out.filter(pl.col("team_id") == "T").row(0, named=True)["wab"] > 0.0


def test_wab_negative_when_losing_easy_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    """A team that loses to weak opponents underperforms the bubble baseline (which
    would beat them), so WAB < 0."""
    ratings = pl.DataFrame({"season": [2023] * 3, "team_id": ["L", "C", "D"], "adj_net": [0.0, -0.10, -0.30]})
    schedule = pl.DataFrame(
        {
            "game_id": ["1", "2"],
            "season": [2023, 2023],
            "home_team_id": ["L", "L"],  # L hosts two weak teams
            "away_team_id": ["C", "D"],
            "home_score": [10, 14],  # and loses both
            "away_score": [17, 21],
            "neutral_site": [False, False],
        }
    )
    out = _run(monkeypatch, ratings=ratings, schedule=schedule)
    assert out.filter(pl.col("team_id") == "L").row(0, named=True)["wab"] < 0.0


def test_wab_in_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """WAB is the last résumé column."""
    out = _run(monkeypatch)
    assert out.columns[-1] == "wab"
    assert out.schema["wab"] == pl.Float64
