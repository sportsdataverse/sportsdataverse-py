"""NBA / WNBA rated pregame prior (BB-D25): home win probability from the as-of team ratings.

The in-game win-probability booster takes a pregame home win probability. Without a betting line
:func:`~sportsdataverse.nba.nba_game_page.home_prior` falls back to home-court advantage alone, the
same number for every game; the WNBA release carries no lines, so every WNBA game got it. This
module rates each game instead, from the season sim's game model (BB-D18,
:func:`~sportsdataverse.nba.nba_season_sim.as_of_game_model`): BB-D19's possession ridge, shrunk
toward last season's regressed ratings (:func:`~sportsdataverse.nba.nba_season_sim.preseason_prior`),
its home term and the as-of possessions per game give the expected home margin
``(net_home - net_away + 2 * hfa) * poss / 100`` (no home term at a neutral site), and
``Phi(margin / margin_sd)`` is the probability. Every constant is the sim's
(:data:`~sportsdataverse.nba.nba_season_sim.SIM_CONSTANTS`,
:data:`~sportsdataverse.nba.nba_season_sim.PRIOR_CONSTANTS`); nothing is fitted here.

**As of the tip.** A game on date ``d`` is rated from the regular-season possessions of games
dated strictly before ``d`` (the boundary is exclusive: a same-day game never rates another). The
holdout that shows this prior beats the home-court fallback is
``tools/models/nba_prediction/wp_model_cards.py``; its numbers are in the booster cards
(``sportsdataverse/nba/models/<lg>_in_game_wp.card.json``).

**ESPN games.** The ratings use the stats.nba.com / stats.wnba.com possessions; the game pages use
ESPN ids. Games are matched on (date, home team name, away team name), which the two sources
share. A matched game takes :mod:`sportsdataverse.nba.nba_neutral_site`'s flag, not ESPN's, which
misses the 2020 bubble and the 2023-24 Cup semifinals; an unmatched game keeps ESPN's. An ESPN game with no stats match gets a null probability and the game
page falls back to home-court advantage: the All-Star games, and the WNBA Commissioner's Cup final,
which the stats schedule does not list. The holdout's match counts are pinned in
``tests/nba/test_nba_wp_model_cards.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl
from scipy.stats import norm

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.nba import nba_neutral_site
from sportsdataverse.nba.nba_season_sim import PreseasonPrior, as_of_game_model, preseason_prior
from sportsdataverse.nba.nba_team_season_rates import _league_loaders, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_rated_priors"]

#: :func:`nba_rated_priors` output, one row per ESPN game.
RATED_PRIOR_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int32(),
    "game_id": pl.Int32(),
    "game_date": pl.Date(),
    "home_team_id": pl.Int32(),
    "away_team_id": pl.Int32(),
    "neutral_site": pl.Boolean(),
    "stats_game_id": pl.Utf8(),
    "home_adj_net_rtg": pl.Float64(),
    "away_adj_net_rtg": pl.Float64(),
    "hfa": pl.Float64(),
    "exp_home_margin": pl.Float64(),
    "rated_home_prob": pl.Float64(),
}

_STATS_GAME_TYPES = ("regular-season", "play-in", "playoffs", "nba-cup")


def rated_home_probs(
    rows: pl.DataFrame, games: pl.DataFrame, *, league_id: str = "00", prior: PreseasonPrior | None = None
) -> pl.DataFrame:
    """Rate each game from the possessions dated strictly before it.

    Args:
        rows: Regular-season possession rows
            (:data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`) plus ``game_date``.
        games: One row per game: ``game_date`` (Date), ``home_team_id`` / ``away_team_id`` (stats
            ids, Int64) and ``neutral_site``; other columns pass through.
        league_id: ``"00"`` NBA or ``"10"`` WNBA (the sim's constants).
        prior: Last season's regressed ratings (:func:`~sportsdataverse.nba.nba_season_sim.preseason_prior`);
            None rates every team average until it has possessions.

    Returns:
        ``games`` plus ``home_adj_net_rtg``, ``away_adj_net_rtg``, ``hfa`` (per 100),
        ``exp_home_margin`` (points) and ``rated_home_prob``. A team with no rating yet is average.
    """
    out = []
    for (day,), sub in games.group_by("game_date", maintain_order=True):
        model = as_of_game_model(rows.filter(pl.col("game_date") < day), league_id=league_id, prior=prior)
        assert model.ratings.schema["team_id"] == sub.schema["home_team_id"], "team_id dtype differs"
        net = model.ratings.select("team_id", "adj_net_rtg")
        rated = sub.join(
            net.rename({"team_id": "home_team_id", "adj_net_rtg": "home_adj_net_rtg"}), on="home_team_id", how="left"
        ).join(
            net.rename({"team_id": "away_team_id", "adj_net_rtg": "away_adj_net_rtg"}), on="away_team_id", how="left"
        )
        home_term = pl.when(pl.col("neutral_site").fill_null(False)).then(0.0).otherwise(2.0 * model.hfa)
        margin = (pl.col("home_adj_net_rtg").fill_null(0.0) - pl.col("away_adj_net_rtg").fill_null(0.0) + home_term) * (
            model.poss / 100.0
        )
        rated = rated.with_columns(pl.lit(model.hfa).alias("hfa"), margin.alias("exp_home_margin"))
        out.append(
            rated.with_columns(
                pl.Series("rated_home_prob", norm.cdf(rated["exp_home_margin"].to_numpy() / model.margin_sd))
            )
        )
    if not out:
        return games.with_columns(
            *(pl.lit(None, dtype=pl.Float64).alias(c) for c in ("home_adj_net_rtg", "away_adj_net_rtg", "hfa")),
            pl.lit(None, dtype=pl.Float64).alias("exp_home_margin"),
            pl.lit(None, dtype=pl.Float64).alias("rated_home_prob"),
        )
    return pl.concat(out).sort("game_date", maintain_order=True)


def _stats_games(schedule: pl.DataFrame) -> pl.DataFrame:
    """The stats schedule's real games (no All-Star or preseason), stats ids Int64, date parsed."""
    return schedule.filter(
        pl.col("season_type").is_in(_STATS_GAME_TYPES) & (pl.col("home_team_id") > 0) & (pl.col("away_team_id") > 0)
    ).select(
        pl.col("game_id").cast(pl.Utf8).alias("stats_game_id"),
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date().alias("game_date"),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
        pl.col("home_team_name").str.to_lowercase().alias("_h"),
        pl.col("away_team_name").str.to_lowercase().alias("_a"),
        "season_type",
    )


def _possession_rows(league_id: str, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(regular-season possession rows with ``game_date``, the stats schedule's games) for a season."""
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    schedule = load_schedule([season - offset])
    games = _stats_games(schedule)
    rows = nba_possession_rate_rows(load_possessions([season - offset]), schedule, league_id=league_id)
    assert rows.schema["game_id"] == games.schema["stats_game_id"], "game_id dtype differs from the schedule's"
    rows = rows.join(
        games.select(pl.col("stats_game_id").alias("game_id"), "game_date"), on="game_id", how="inner", validate="m:1"
    )
    return rows, games


@overload
def nba_rated_priors(
    season: int, *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_rated_priors(season: int, *, league_id: str = "00", return_as_pandas: Literal[True]) -> pd.DataFrame: ...


def nba_rated_priors(
    season: int, *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Rated pregame home win probability for every ESPN game of a season, as of each game's date.

    The in-game win-probability prior when a game has no betting line
    (:func:`~sportsdataverse.nba.nba_game_page.home_prior`'s ``rated_home_prob``); see the module
    docstring for the model and the as-of boundary.

    Args:
        season: The season, named by the year it ends (``2026`` is 2025-26); the WNBA season is its
            calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per game of the ESPN schedule release (scheduled games included): ``season``,
        ``game_id``, ``game_date``, ``home_team_id``, ``away_team_id`` (ESPN ids, Int32, as in the
        pbp release), ``neutral_site``, ``stats_game_id``, the two teams' adjusted net ratings
        before the game (``home_adj_net_rtg``, ``away_adj_net_rtg``, per 100), the home term
        ``hfa`` (per 100, applied to both offenses), ``exp_home_margin`` (points) and
        ``rated_home_prob``. The last six are null for a game with no stats.nba.com match
        (All-Star games and the WNBA Commissioner's Cup final).

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_rated_prior import nba_rated_priors
            priors = nba_rated_priors(2026)
            priors.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

        WNBA::

            nba_rated_priors(2025, league_id="10")
    """
    if league_id not in ("00", "10"):
        raise ValueError(f"league_id must be '00' or '10', got {league_id!r}")
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_schedule as load_espn
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_schedule as load_espn
    rows, games = _possession_rows(league_id, season)
    prior = None
    try:
        prev_rows, prev_games = _possession_rows(league_id, season - 1)
        prior = preseason_prior(
            prev_rows,
            sorted(set(games["home_team_id"].to_list()) | set(games["away_team_id"].to_list())),
            league_id=league_id,
            prev_teams=sorted(set(prev_games["home_team_id"].to_list()) | set(prev_games["away_team_id"].to_list())),
        )
    except SeasonNotFoundError:  # the season before the release floor: average until possessions arrive
        pass
    espn = (
        load_espn([season])
        .select(
            pl.col("season").cast(pl.Int32),
            pl.col("game_id").cast(pl.Int32),
            pl.col("game_date").cast(pl.Date),
            pl.col("home_id").cast(pl.Int32).alias("home_team_id"),
            pl.col("away_id").cast(pl.Int32).alias("away_team_id"),
            pl.col("neutral_site").fill_null(False),
            pl.col("home_display_name").str.to_lowercase().alias("_h"),
            pl.col("away_display_name").str.to_lowercase().alias("_a"),
        )
        .unique("game_id", keep="first", maintain_order=True)
    )  # one row per game, as the pbp release has
    keyed = games.unique(["game_date", "_h", "_a"], keep="none")  # a doubled key would match the wrong game
    stats = keyed.select("game_date", "_h", "_a", "stats_game_id", "home_team_id", "away_team_id", "season_type")
    matched = espn.join(
        stats.rename({"home_team_id": "_hs", "away_team_id": "_as"}), on=["game_date", "_h", "_a"], how="left"
    ).with_columns(  # ESPN's flag is wrong both ways (misses the bubble, flags All-Star games): ours wins
        pl.when(pl.col("stats_game_id").is_not_null())
        .then(nba_neutral_site._neutral_site(espn, league_id, "stats_game_id"))
        .otherwise(pl.col("neutral_site"))
        .alias("neutral_site")
    )
    to_rate = matched.filter(pl.col("stats_game_id").is_not_null()).select(
        "game_id", "game_date", "neutral_site", pl.col("_hs").alias("home_team_id"), pl.col("_as").alias("away_team_id")
    )
    rated = rated_home_probs(rows, to_rate, league_id=league_id, prior=prior).select(
        "game_id", "home_adj_net_rtg", "away_adj_net_rtg", "hfa", "exp_home_margin", "rated_home_prob"
    )
    out = (
        matched.join(rated, on="game_id", how="left", validate="1:1")
        .select(list(RATED_PRIOR_SCHEMA))
        .cast(RATED_PRIOR_SCHEMA)
        .sort("game_date", "game_id")
    )
    return out.to_pandas() if return_as_pandas else out
