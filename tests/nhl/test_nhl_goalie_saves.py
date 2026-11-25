"""``nhl_goalie_saves``: starting-goalie saves projection and interval.

Reads ``tests/fixtures/nhl_prediction/goalie_box_2026.parquet``, the 2025-26 goalie boxscores (a season the
saves constants were NOT fitted on; see the fixtures README).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import nbinom

from sportsdataverse.nhl.nhl_player_props import _SAVES_SCHEMA, nhl_goalie_saves
from sportsdataverse.nhl.nhl_prediction_constants import get_constants

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_prediction" / "goalie_box_2026.parquet"


@pytest.fixture(scope="module")
def box() -> pl.DataFrame:
    return pl.read_parquet(FIXTURE)


@pytest.fixture(scope="module")
def projected(box) -> pl.DataFrame:
    return nhl_goalie_saves(2026, load_goalie_boxscores=lambda _: box)


def _regular_starters(box: pl.DataFrame) -> pl.DataFrame:
    return box.filter((pl.col("starter") == True) & (pl.col("game_id").cast(pl.Utf8).str.slice(4, 2) == "02"))  # noqa: E712


def test_coverage_outside_unit_interval_raises():
    with pytest.raises(ValueError, match="coverage"):
        nhl_goalie_saves(2026, coverage=1.0, load_goalie_boxscores=lambda _: pl.DataFrame())


def test_empty_box_returns_documented_schema():
    out = nhl_goalie_saves(2026, load_goalie_boxscores=lambda _: pl.DataFrame())
    assert out.schema == pl.Schema(_SAVES_SCHEMA)


def test_one_row_per_starter_game_and_interval_brackets_the_mean(box, projected):
    assert projected.height == box.filter(pl.col("starter") == True).height  # noqa: E712
    assert projected.null_count().sum_horizontal().item() == 0
    assert (projected["saves_lo"] <= projected["exp_saves"]).all()
    assert (projected["exp_saves"] <= projected["saves_hi"]).all()


def test_projection_uses_only_earlier_games(box, projected):
    """Rewriting every game from a mid-season date on leaves every projection up to that date unchanged.

    Games ON the date are rewritten too, so a projection that read its own game would move.
    """
    cut = sorted(box["game_date"].unique().to_list())[100]
    later = pl.col("game_date") >= cut
    doctored = box.with_columns(
        pl.when(later).then(pl.col("shots_against") * 2).otherwise(pl.col("shots_against")).alias("shots_against"),
        pl.when(later).then(0).otherwise(pl.col("saves")).alias("saves"),
    )
    again = nhl_goalie_saves(2026, load_goalie_boxscores=lambda _: doctored)
    key = ["game_id", "player_id"]
    cut_date = pl.lit(cut).str.to_date()
    upto = projected.filter(pl.col("game_date") <= cut_date).sort(key)
    assert upto.height > 500
    assert upto.filter(pl.col("game_date") == cut_date).height > 0
    assert upto.equals(again.filter(pl.col("game_date") <= cut_date).sort(key))
    after = projected.filter(pl.col("game_date") > cut_date).sort(key)
    assert not after.equals(again.filter(pl.col("game_date") > cut_date).sort(key))


def test_starters_for_an_unplayed_game_match_the_backtest(box, projected):
    """A game passed as ``starters`` (and absent from the history) projects as the backtest does."""
    game = _regular_starters(box).sort("game_date")["game_id"][800]
    date = box.filter(pl.col("game_id") == game)["game_date"][0]
    history = box.filter(pl.col("game_date") < date)
    starters = (
        _regular_starters(box)
        .filter(pl.col("game_id") == game)
        .select(
            "game_id",
            "game_date",
            pl.col("team_abbrev").alias("team"),
            (pl.col("home_away") == "home").alias("is_home"),
            "player_id",
        )
    )
    future = nhl_goalie_saves(2026, starters=starters, load_goalie_boxscores=lambda _: history)
    assert future.height == 2
    expected = projected.filter(pl.col("game_id") == game).sort("is_home")
    np.testing.assert_allclose(future.sort("is_home")["exp_saves"], expected["exp_saves"], rtol=1e-12)


def test_home_goalie_faces_fewer_shots_on_opening_night(projected):
    """With no games played every team sits at the league seed; home ice alone separates the two goalies."""
    const = get_constants("nhl")
    first = projected.filter(pl.col("game_date") == projected["game_date"].min())
    home = first.filter(pl.col("is_home") == True)["exp_shots_against"]  # noqa: E712
    away = first.filter(pl.col("is_home") == False)["exp_shots_against"]  # noqa: E712
    np.testing.assert_allclose(away.to_numpy() - home.to_numpy(), const.starter_shot_share * const.shots_hfa)
    # and the level is the league seed itself, so avg_shots_against reaches the output
    np.testing.assert_allclose(
        home.to_numpy(), const.starter_shot_share * (const.avg_shots_against - const.shots_hfa / 2)
    )


@pytest.mark.parametrize(("coverage", "observed_implied"), [(0.5, 0.5474), (0.8, 0.8256), (0.9, 0.9150)])
def test_holdout_interval_coverage_and_bias(box, coverage, observed_implied):
    """2025-26 regular-season starters (2,624): each interval covers what the model says it covers.

    Saves are whole numbers, so the equal-tail interval holds a little more than ``coverage`` of the model's
    own distribution: 0.5474 / 0.8256 / 0.9150 on this fixture. Gates: that implied coverage lies in
    ``[coverage, coverage + 0.05]`` (so an interval of the wrong width, or one that ignores ``coverage`` or
    ``shots_nb_size``, fails), realized coverage is within 0.02 of it (2.5 binomial standard errors), and the
    mean projection is within 0.5 saves of the mean realized. Observed realized 0.5461 / 0.8293 / 0.9074 and
    bias +0.087 saves, with 2025-26 as the only history. Never lower a gate in this module to make a refit pass.
    """
    projected = nhl_goalie_saves(2026, coverage=coverage, load_goalie_boxscores=lambda _: box)
    actual = _regular_starters(box).select("game_id", "player_id", pl.col("saves").alias("actual"))
    d = projected.join(actual, on=["game_id", "player_id"])
    assert d.height > 2500
    y = d["actual"].to_numpy()
    lo, hi, m = d["saves_lo"].to_numpy(), d["saves_hi"].to_numpy(), d["exp_saves"].to_numpy()
    r = get_constants("nhl").shots_nb_size
    implied = (nbinom.cdf(hi, r, r / (r + m)) - nbinom.cdf(lo - 1, r, r / (r + m))).mean()
    realized = ((y >= lo) & (y <= hi)).mean()
    assert coverage <= implied <= coverage + 0.05, (implied, observed_implied)
    assert abs(realized - implied) < 0.02, (realized, implied)
    assert abs(m.mean() - y.mean()) < 0.5


def _starters_of(box: pl.DataFrame, game: int) -> pl.DataFrame:
    return (
        _regular_starters(box)
        .filter(pl.col("game_id") == game)
        .select(
            "game_id",
            "game_date",
            pl.col("team_abbrev").alias("team"),
            (pl.col("home_away") == "home").alias("is_home"),
            "player_id",
        )
    )


def test_starters_for_a_game_already_in_the_box_ignore_its_result(box):
    """Re-running a slate after it is played must not feed each game's own saves into its projection."""
    game = _regular_starters(box).sort("game_date")["game_id"][800]
    date = box.filter(pl.col("game_id") == game)["game_date"][0]
    starters = _starters_of(box, game)
    with_game = nhl_goalie_saves(2026, starters=starters, load_goalie_boxscores=lambda _: box)
    without = nhl_goalie_saves(
        2026, starters=starters, load_goalie_boxscores=lambda _: box.filter(pl.col("game_date") != date)
    )
    np.testing.assert_allclose(with_game.sort("is_home")["exp_saves"], without.sort("is_home")["exp_saves"], rtol=1e-12)


def test_starters_must_list_both_teams(box):
    game = _regular_starters(box).sort("game_date")["game_id"][800]
    with pytest.raises(ValueError, match="both teams"):
        nhl_goalie_saves(2026, starters=_starters_of(box, game).head(1), load_goalie_boxscores=lambda _: box)


def test_datetime_game_dates_project_like_strings(box, projected):
    """A loader that returns ``game_date`` as Datetime must not null every projection."""
    as_datetime = box.with_columns(pl.col("game_date").str.to_datetime("%Y-%m-%d"))
    again = nhl_goalie_saves(2026, load_goalie_boxscores=lambda _: as_datetime)
    key = ["game_id", "player_id"]
    assert again.sort(key).equals(projected.sort(key))
