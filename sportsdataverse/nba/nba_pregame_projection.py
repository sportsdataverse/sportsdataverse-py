"""NBA / WNBA pregame projections: every game's rated pregame number, as of its date.

The game page's pregame card prints the producer's closed-form projection
(:func:`~sportsdataverse.nba.nba_game_predict.nba_predict_games`:
``margin = (home adj_net_rtg - away adj_net_rtg) * home_pace * away_pace / avg_pace / 100 + hfa``,
no home term at a neutral site, and ``P(home) = Phi(margin / margin_sd)``, with the fitted
:data:`~sportsdataverse.nba.nba_prediction_constants.LEAGUE_CONSTANTS`) on BB-D19's team strength
(:func:`~sportsdataverse.nba.nba_team_strength.nba_team_strength`). This module publishes that
number per game so the site reads it instead of computing it.

**The rating.** ``team_strength`` rebuilt from the regular-season possessions of games dated
strictly before ``as_of_date``: the possession ridge's ``adj_net_rtg`` with its rank and the
BB-D3 ``pace``. There is no preseason blend (``team_strength`` has none). This is a different
number from :func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`, the in-game booster's
prior, which uses the season sim's game model and preseason prior.

**As of.** ``as_of_date`` is the game's date, or the day after the last regular-season game with
possessions for a game not yet reached, so an upcoming game (and every playoff game) is rated on
the regular season so far. ESPN games are matched to the stats schedule as
:func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors` matches them; an unmatched game
(All-Star games, the WNBA Commissioner's Cup final) has null ratings and prediction.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.rate_gates import NBA_LEVEL
from sportsdataverse._common.team_season_rates import team_season_rates
from sportsdataverse._common.team_strength import GAMES_SCHEMA, RIDGE_LAMBDA, possession_ridge_ratings, team_strength
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_prediction_constants import get_constants
from sportsdataverse.nba.nba_rated_prior import _espn_matched, _possession_rows
from sportsdataverse.nba.nba_team_season_rates import _LEAGUES

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_pregame_projections", "pro_pregame_projections"]

#: :func:`nba_pregame_projections` output, one row per ESPN game.
PREGAME_PROJECTION_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int32(),
    "game_id": pl.Int32(),
    "game_date": pl.Date(),
    "as_of_date": pl.Date(),
    "home_team_id": pl.Int32(),
    "away_team_id": pl.Int32(),
    "neutral_site": pl.Boolean(),
    "stats_game_id": pl.Utf8(),
    "home_rating": pl.Float64(),
    "away_rating": pl.Float64(),
    "home_rank": pl.Float64(),
    "away_rank": pl.Float64(),
    "home_games": pl.Int64(),
    "away_games": pl.Int64(),
    "home_pace": pl.Float64(),
    "away_pace": pl.Float64(),
    "hfa": pl.Float64(),
    "exp_home_margin": pl.Float64(),
    "rated_home_prob": pl.Float64(),
}

_STRENGTH = {"team_id": pl.Int64, "adj_net_rtg": pl.Float64, "adj_net_rtg_rank": pl.Float64}
_STRENGTH |= {"games": pl.Int64, "pace": pl.Float64}


def _strength(rows: pl.DataFrame, league_id: str) -> pl.DataFrame:
    """``team_strength``'s ``team_id, adj_net_rtg, adj_net_rtg_rank, games, pace`` from possession rows."""
    if rows.height == 0:
        return pl.DataFrame(schema=_STRENGTH)
    out = team_strength(
        team_season_rates(rows, minutes_per_game=_LEAGUES[league_id][1], level=NBA_LEVEL[league_id]),
        possession_ridge_ratings(rows, lam=RIDGE_LAMBDA[league_id]),
        pl.DataFrame(schema=GAMES_SCHEMA),  # wins / SOS are not read here
    )
    return out.select(list(_STRENGTH)).filter(pl.col("adj_net_rtg").is_not_null())


def pro_pregame_projections(rows: pl.DataFrame, games: pl.DataFrame, *, league_id: str = "00") -> pl.DataFrame:
    """Rate each game from the regular-season possessions dated strictly before it.

    Args:
        rows: Regular-season possession rows
            (:data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`) plus
            ``game_date``.
        games: One row per game: ``game_id``, ``game_date`` (Date), ``home_team_id`` /
            ``away_team_id`` (stats ids, Int64) and ``neutral_site``; other columns pass through.
        league_id: ``"00"`` NBA or ``"10"`` WNBA (the constants).

    Returns:
        ``games`` plus ``as_of_date``, each side's ``rating`` (``adj_net_rtg``), ``rank``,
        ``games`` and ``pace``, ``hfa`` (points, 0 at a neutral site), ``exp_home_margin`` and
        ``rated_home_prob``; null for a side with no rating yet.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_pregame_projection import pro_pregame_projections
            out = pro_pregame_projections(rows, games)
    """
    last = rows.get_column("game_date").max() if rows.height else None
    cut = (
        pl.min_horizontal("game_date", pl.lit(last + datetime.timedelta(days=1)))
        if isinstance(last, datetime.date)
        else pl.col("game_date")
    )
    games = games.with_columns(cut.alias("as_of_date"))
    c = get_constants(league_id)
    frames = []
    for (cutoff,), sub in games.group_by("as_of_date", maintain_order=True):
        strength = _strength(rows.filter(pl.col("game_date") < cutoff), league_id)
        assert strength.schema["team_id"] == sub.schema["home_team_id"], "team_id dtype differs"
        pred = nba_predict_games(
            sub.select("game_id", "home_team_id", "away_team_id", "neutral_site"),
            strength.select(
                "team_id",
                pl.lit(None, dtype=pl.Float64).alias("adj_off_rtg"),
                pl.lit(None, dtype=pl.Float64).alias("adj_def_rtg"),
                "adj_net_rtg",
                pl.col("pace").alias("adj_pace"),
            ),
            league_id=league_id,
        ).select("game_id", "exp_margin", "home_win_prob")
        out = sub
        for side in ("home", "away"):
            out = out.join(
                strength.select(
                    pl.col("team_id").alias(f"{side}_team_id"),
                    pl.col("adj_net_rtg").alias(f"{side}_rating"),
                    pl.col("adj_net_rtg_rank").alias(f"{side}_rank"),
                    pl.col("games").alias(f"{side}_games"),
                    pl.col("pace").alias(f"{side}_pace"),
                ),
                on=f"{side}_team_id",
                how="left",
            )
        frames.append(out.join(pred, on="game_id", how="left", validate="1:1"))
    if not frames:
        return games.with_columns(
            *(
                pl.lit(None, dtype=dt).alias(f"{side}_{m}")
                for side in ("home", "away")
                for m, dt in (("rating", pl.Float64), ("rank", pl.Float64), ("games", pl.Int64), ("pace", pl.Float64))
            ),
            pl.lit(None, dtype=pl.Float64).alias("hfa"),
            pl.lit(None, dtype=pl.Float64).alias("exp_home_margin"),
            pl.lit(None, dtype=pl.Float64).alias("rated_home_prob"),
        )
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .with_columns(
            pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(c.hfa).alias("hfa"),  # noqa: E712
            pl.col("exp_margin").alias("exp_home_margin"),
            pl.col("home_win_prob").fill_nan(None).alias("rated_home_prob"),
        )
        .drop("exp_margin", "home_win_prob")
        .sort("game_date", maintain_order=True)
    )


@overload
def nba_pregame_projections(
    season: int, *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_pregame_projections(season: int, *, league_id: str = "00", return_as_pandas: Literal[True]) -> pd.DataFrame: ...


def nba_pregame_projections(
    season: int, *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Rated pregame projection for every ESPN game of an NBA / WNBA season, as of each game's date.

    The number the game page's pregame card prints: the closed form on each team's
    ``team_strength`` before the game; see the module docstring for the model and the as-of
    boundary.

    Args:
        season: The season, named by the year it ends (``2026`` is 2025-26); the WNBA season is its
            calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per game of the ESPN schedule release (scheduled games included): ``season``,
        ``game_id``, ``game_date``, ``as_of_date`` (the ratings use regular-season possessions
        dated strictly before it), ``home_team_id`` / ``away_team_id`` (ESPN ids, Int32),
        ``neutral_site``, ``stats_game_id``, each side's ``rating`` (``adj_net_rtg``, per 100),
        ``rank`` (``adj_net_rtg_rank``), ``games`` and ``pace``, ``hfa`` (points, 0 at a
        neutral site), ``exp_home_margin`` (points) and ``rated_home_prob``. The ratings and
        prediction are null for a game with no stats match or a side with no rating yet.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_pregame_projection import nba_pregame_projections
            out = nba_pregame_projections(2026)
            out.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

        WNBA::

            nba_pregame_projections(2025, league_id="10")
    """
    if league_id not in ("00", "10"):
        raise ValueError(f"league_id must be '00' or '10', got {league_id!r}")
    rows, games = _possession_rows(league_id, season)
    matched = _espn_matched(season, league_id, games)
    to_rate = matched.filter(pl.col("stats_game_id").is_not_null()).select(
        "game_id", "game_date", "neutral_site", pl.col("_hs").alias("home_team_id"), pl.col("_as").alias("away_team_id")
    )
    rated = pro_pregame_projections(rows, to_rate, league_id=league_id).drop(
        "game_date", "neutral_site", "home_team_id", "away_team_id"
    )
    last = rows.get_column("game_date").max() if rows.height else None
    out = (
        matched.join(rated, on="game_id", how="left", validate="1:1")
        .with_columns(
            pl.col("as_of_date").fill_null(
                pl.min_horizontal("game_date", pl.lit(last + datetime.timedelta(days=1)))
                if isinstance(last, datetime.date)
                else pl.col("game_date")
            )
        )
        .select(list(PREGAME_PROJECTION_SCHEMA))
        .cast(PREGAME_PROJECTION_SCHEMA)
        .sort("game_date", "game_id")
    )
    return out.to_pandas() if return_as_pandas else out
