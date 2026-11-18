"""NBA / WNBA pregame projections (GOP Q4) on the real first 21 days of the 2025 WNBA season.

The fixtures are the possession rows and stats games of ``wnba_rated_prior_asof_*_2025``
(tests/fixtures/nba_prediction/README.md).
"""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.rate_gates import NBA_LEVEL
from sportsdataverse._common.team_season_rates import team_season_rates
from sportsdataverse._common.team_strength import GAMES_SCHEMA, RIDGE_LAMBDA, possession_ridge_ratings, team_strength
from sportsdataverse.nba import nba_pregame_projection
from sportsdataverse.nba.nba_game_predict import predict_margin, win_prob_from_margin
from sportsdataverse.nba.nba_rated_prior import rated_home_probs
from sportsdataverse.nba.nba_pregame_projection import (
    PREGAME_PROJECTION_SCHEMA,
    nba_pregame_projections,
    pro_pregame_projections,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"
ROWS = pl.read_parquet(FIX / "wnba_rated_prior_asof_2025.parquet")
GAMES = pl.read_parquet(FIX / "wnba_rated_prior_asof_games_2025.parquet")


def _games(games: pl.DataFrame) -> pl.DataFrame:
    return games.select(
        pl.col("stats_game_id").alias("game_id"),
        "game_date",
        "home_team_id",
        "away_team_id",
        pl.lit(False).alias("neutral_site"),
    )


def _team_strength(rows: pl.DataFrame) -> dict[int, dict]:
    """``nba_team_strength``'s own composition on the rows (wnba: 40-minute games)."""
    ts = team_strength(
        team_season_rates(rows, minutes_per_game=40, level=NBA_LEVEL["10"]),
        possession_ridge_ratings(rows, lam=RIDGE_LAMBDA["10"]),
        pl.DataFrame(schema=GAMES_SCHEMA),
    )
    return {r["team_id"]: r for r in ts.iter_rows(named=True)}


@pytest.fixture(scope="module")
def out() -> pl.DataFrame:
    return pro_pregame_projections(ROWS, _games(GAMES), league_id="10")


def test_each_game_is_the_cards_number_on_team_strength(out: pl.DataFrame) -> None:
    """#434's closed form on the team_strength row the card reads, rebuilt through the day before."""
    rated = out.drop_nulls("rated_home_prob")
    assert rated.height >= 40  # 42 of 49: opening night and four games of a team yet to play are unrated
    for (cutoff,), sub in rated.group_by("as_of_date"):
        ts = _team_strength(ROWS.filter(pl.col("game_date") < cutoff))
        for g in sub.iter_rows(named=True):
            h, a = ts[g["home_team_id"]], ts[g["away_team_id"]]
            assert (g["home_rating"], g["home_rank"], g["home_games"], g["home_pace"]) == (
                h["adj_net_rtg"],
                h["adj_net_rtg_rank"],
                h["games"],
                h["pace"],
            )
            margin = predict_margin(
                h["adj_net_rtg"], a["adj_net_rtg"], home_pace=h["pace"], away_pace=a["pace"], league_id="10"
            )
            assert g["exp_home_margin"] == pytest.approx(margin, abs=1e-12)
            assert g["rated_home_prob"] == pytest.approx(win_prob_from_margin(margin, league_id="10"), abs=1e-12)
            assert g["hfa"] == pytest.approx(1.184)


def test_opening_night_has_no_rating(out: pl.DataFrame) -> None:
    first = out.filter(pl.col("game_date") == GAMES["game_date"].min())
    assert first.height > 0 and first["rated_home_prob"].null_count() == first.height


def test_same_day_possessions_never_rate_a_game() -> None:
    last = GAMES["game_date"].max()
    day = _games(GAMES.filter(pl.col("game_date") == last))
    full = pro_pregame_projections(ROWS, day, league_id="10")
    cut = pro_pregame_projections(ROWS.filter(pl.col("game_date") < last), day, league_id="10")
    assert full["rated_home_prob"].to_list() == cut["rated_home_prob"].to_list()
    assert (full["as_of_date"] == last).all()


def test_an_upcoming_game_is_rated_on_every_game_played() -> None:
    last = ROWS["game_date"].max()
    ahead = _games(GAMES.head(1)).with_columns(pl.lit(last + datetime.timedelta(days=9)).alias("game_date"))
    g = pro_pregame_projections(ROWS, ahead, league_id="10").row(0, named=True)
    assert g["as_of_date"] == last + datetime.timedelta(days=1)
    assert g["home_rating"] == _team_strength(ROWS)[g["home_team_id"]]["adj_net_rtg"]


def test_a_neutral_game_has_no_home_term() -> None:
    g = _games(GAMES.tail(1))
    home = pro_pregame_projections(ROWS, g, league_id="10").row(0, named=True)
    neutral = pro_pregame_projections(ROWS, g.with_columns(pl.lit(True).alias("neutral_site")), league_id="10").row(
        0, named=True
    )
    assert neutral["hfa"] == 0.0 and home["exp_home_margin"] - neutral["exp_home_margin"] == pytest.approx(1.184)


def test_wiring_keys_the_espn_game(monkeypatch: pytest.MonkeyPatch) -> None:
    espn = GAMES.select(
        pl.lit(2025).alias("season"),
        (pl.int_range(pl.len()) + 401_000_000).alias("game_id"),
        "game_date",
        (pl.int_range(pl.len()) + 1).alias("home_id"),
        (pl.int_range(pl.len()) + 100).alias("away_id"),
        pl.lit(False).alias("neutral_site"),
        pl.col("_h").alias("home_display_name"),
        pl.col("_a").alias("away_display_name"),
    )
    from sportsdataverse.wnba import wnba_loaders

    monkeypatch.setattr(wnba_loaders, "load_wnba_schedule", lambda seasons: espn)
    monkeypatch.setattr(nba_pregame_projection, "_possession_rows", lambda league_id, season: (ROWS, GAMES))
    full = nba_pregame_projections(2025, league_id="10")
    assert full.schema == pl.Schema(PREGAME_PROJECTION_SCHEMA)
    assert full.height == GAMES.height and full["stats_game_id"].null_count() == 0
    assert full["home_team_id"].min() == 1  # the ESPN ids, not the stats ids the ratings are keyed on
    with pytest.raises(ValueError, match="league_id"):
        nba_pregame_projections(2025, league_id="20")


def test_wp_prior_columns_are_the_boosters_prior(monkeypatch: pytest.MonkeyPatch) -> None:
    """``wp_prior_*`` is BB-D25's number (``nba_rated_priors``) to the bit, home term included."""
    from sportsdataverse.nba import nba_rated_prior
    from sportsdataverse.wnba import wnba_loaders

    espn = GAMES.select(
        pl.lit(2025).alias("season"),
        (pl.int_range(pl.len()) + 401_000_000).alias("game_id"),
        "game_date",
        (pl.int_range(pl.len()) + 1).alias("home_id"),
        (pl.int_range(pl.len()) + 100).alias("away_id"),
        pl.lit(False).alias("neutral_site"),
        pl.col("_h").alias("home_display_name"),
        pl.col("_a").alias("away_display_name"),
    )
    monkeypatch.setattr(wnba_loaders, "load_wnba_schedule", lambda seasons: espn)
    calls: list[int] = []

    def rows(league_id: str, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
        calls.append(season)
        return ROWS, GAMES  # the fixture is its own prior season

    monkeypatch.setattr(nba_pregame_projection, "_possession_rows", rows)
    monkeypatch.setattr(nba_rated_prior, "_possession_rows", rows)
    out = nba_pregame_projections(2025, league_id="10")
    assert calls == [2025, 2024]  # the season, then the season before for the prior
    d25 = nba_rated_prior.nba_rated_priors(2025, league_id="10")
    both = out.join(d25, on="game_id", validate="1:1")
    assert both.height == GAMES.height and both["wp_prior_home_prob"].null_count() == 0  # opening night too
    assert both["wp_prior_home_prob"].to_list() == both["rated_home_prob_right"].to_list()
    assert both["wp_prior_exp_home_margin"].to_list() == both["exp_home_margin_right"].to_list()
    assert both["wp_prior_home_rating"].to_list() == both["home_adj_net_rtg"].fill_null(0.0).to_list()
    # the page's inputs rebuild the number: rating gap at the model's pace, plus the home term in points
    rebuilt = (pl.col("wp_prior_home_rating") - pl.col("wp_prior_away_rating")) * pl.col(
        "wp_prior_poss"
    ) / 100 + pl.col("wp_prior_hfa")
    gap = out.select((rebuilt - pl.col("wp_prior_exp_home_margin")).abs().max()).item()
    assert gap < 1e-9
    # no neutral game in the fixture, so every game carries the model's as-of home term (it is refit each
    # day, and a few days in it is noisy: -6.5 points on one early date of this fixture)
    assert (out["wp_prior_hfa"] != 0).all()
    # at a neutral site the home term is 0 and the margin is the rating gap alone
    neutral = rated_home_probs(
        ROWS, _games(GAMES.tail(1)).with_columns(pl.lit(True).alias("neutral_site")), league_id="10"
    )
    assert neutral["home_term_pts"].to_list() == [0.0]
    assert out["wp_prior_home_rank"].min() == 1.0
    # no team_strength rating on opening night, but the booster's prior rates it from last season
    first = out.filter(pl.col("game_date") == GAMES["game_date"].min())
    assert first["rated_home_prob"].null_count() == first.height and first["wp_prior_home_prob"].null_count() == 0
