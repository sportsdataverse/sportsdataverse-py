"""Ratings-driven season Monte Carlo for college football (T2.1 Phase 4).

Feeds a ratings-based ``compute_results`` closure into the shipped
:func:`sportsdataverse.cfb.cfb_simulations.cfb_simulations` engine (nflseedR-style
season/standings/CFP-bracket machinery) so conference-title / playoff / championship
odds fall out of the same predictors as the pregame model. The engine is reused
whole; this module only supplies the per-game result sampler and (Task 4.2) the
public wrapper.
"""

from __future__ import annotations


import datetime
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.cfb.cfb_loaders import load_cfb_schedule
from sportsdataverse.cfb.cfb_game_predict import slope_for_games
from sportsdataverse.cfb.cfb_prediction_constants import get_constants
from sportsdataverse.cfb.cfb_ratings import cfb_ratings
from sportsdataverse.cfb.cfb_simulations import ComputeResultsFn, cfb_simulations
from sportsdataverse.cfb.cfb_standings import cfb_games_from_schedule

#: Games behind a full-season rating. Season sims run on end-of-season ratings,
#: so they sit in the most-observed (least attenuated) bucket of the curve.
_FULL_SEASON_GAMES = 12

__all__ = ["cfb_season_odds", "make_ratings_compute_results"]

_ODDS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "exp_wins": pl.Float64,
    "conf_title_prob": pl.Float64,
    "playoff_prob": pl.Float64,
    "first_round_bye_prob": pl.Float64,
    "cfp_champ_prob": pl.Float64,
}


def _mask_after(schedule: pl.DataFrame, as_of_date: datetime.date) -> pl.DataFrame:
    """Null the scores of games kicking off on/after ``as_of_date`` (leakage boundary).

    ``cfb_games_from_schedule`` derives ``result`` from ``home_points - away_points``,
    so nulling the points is what makes the engine treat a game as unplayed. The
    boundary is EXCLUSIVE of the future and mirrors :func:`cfb_ratings.cfb_ratings`
    (``date < as_of_date`` is knowable). ``start_date`` is an ISO timestamp string
    (``2023-08-26T18:30:00.000Z``); the leading 10 chars are the date, which answers a
    date-granularity question without a timezone-aware parse. A row with no
    ``start_date`` is left alone -- unknown is not evidence of being in the past.
    """
    if "start_date" not in schedule.columns:
        return schedule
    future = pl.col("start_date").cast(pl.Utf8).str.slice(0, 10).str.to_date(strict=False) >= pl.lit(as_of_date)
    return schedule.with_columns(
        pl.when(future.fill_null(False)).then(None).otherwise(pl.col(c)).alias(c)
        for c in ("home_points", "away_points")
    )


def _fbs_team_ids(schedule: pl.DataFrame, ratings: pl.DataFrame) -> set[str]:
    """The FBS team-id universe: schedule division markers, else ratings membership.

    ``home_division`` / ``away_division`` (values ``fbs`` / ``fcs`` / ``ii`` / ``iii``)
    are the authoritative classification and, unlike ratings membership, do not move
    with ``as_of_date`` -- an early-season boundary leaves a real FBS team with no plays
    yet and therefore no rating, and dropping it would be the same class of bug in
    reverse. The ratings ids are the fallback for schedule shapes predating the division
    columns (:func:`cfb_ratings.cfb_ratings` defaults to ``fbs_only=True``, so its team
    set is the FBS field).
    """
    ids: set[str] = set()
    for side in ("home", "away"):
        div, tid = f"{side}_division", f"{side}_team"
        if div in schedule.columns:
            ids |= set(schedule.filter(pl.col(div).cast(pl.Utf8).str.to_lowercase() == "fbs")[tid].to_list())
    return ids or set(ratings["team_id"].to_list())


def make_ratings_compute_results(ratings: pl.DataFrame, *, era: str = "modern") -> ComputeResultsFn:
    """Build a ``cfb_simulations`` ``compute_results`` closure from fixed ratings.

    The returned closure implements the engine's results contract -- ``(teams, games,
    week_num, *, rng, **kwargs) -> {"teams", "games"}`` -- filling every unplayed
    ``week == week_num`` game's ``result`` with a sampled home margin
    ``round(Normal(exp_margin, margin_sd))``, where ``exp_margin`` is
    :func:`cfb_game_predict.predict_margin` on the two teams' ``adj_net`` (home-field
    applied unless ``neutral``). Unlike the default elo sampler the ratings are
    **fixed**, so ``teams`` passes through unchanged (no elo update). Postseason games
    (``game_type != "REG"``) re-break a sampled tie by win probability.

    Args:
        ratings: A :func:`cfb_ratings.cfb_ratings`-style frame with ``team_id`` and
            ``adj_net``. Teams absent from it are treated as league-average (0.0).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.

    Returns:
        A ``compute_results`` callable suitable for ``cfb_simulations(...,
        compute_results=...)``.

    Example:
        Quick start::

            import numpy as np, polars as pl
            from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
            cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
            teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
            games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                                  "neutral": [0], "result": [None]})
            cr(teams, games, 1, rng=np.random.default_rng(0))["games"]

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the season-simulation engine reused here.
    """
    c = get_constants(era)
    net = dict(zip(ratings["team_id"].to_list(), ratings["adj_net"].to_list()))
    # A season simulation runs on FULL-SEASON ratings, which sit in the
    # best-observed games-played bucket -- so it takes the top of the
    # attenuation curve, not the flat average across all buckets.
    #
    # `net_points_scale` is the average slope over every stage of the season,
    # appropriate when games-played is unknown. Using it here under-disperses
    # the whole simulation: each game drifts toward a coin flip, elite teams
    # never separate, and the 2023 CFP field's mean playoff probability fell
    # from >=0.10 to 0.077. The curve exists to say "trust a well-observed
    # rating more", and a 12-game rating is as well-observed as they get.
    #
    # Selected as the MOST-GAMES bucket, not `max(values())`: those coincide
    # only while the curve is monotone, and the reason wanted here is "this
    # rating is backed by a full season", not "this number is the largest". If
    # the curve is ever reshaped or era-conditioned, taking the max would
    # silently start meaning something else.
    ns = slope_for_games(_FULL_SEASON_GAMES, era=era) if c.slope_by_games else c.net_points_scale
    hfa_pts, md = c.hfa_points, c.margin_sd

    def compute_results(
        teams: pl.DataFrame, games: pl.DataFrame, week_num: int, *, rng: np.random.Generator, **kwargs: object
    ) -> dict[str, pl.DataFrame]:
        g = games.with_columns(pl.col("result").cast(pl.Float64))
        hn = g["home_team"].replace_strict(net, default=0.0, return_dtype=pl.Float64).to_numpy()
        an = g["away_team"].replace_strict(net, default=0.0, return_dtype=pl.Float64).to_numpy()
        neutral = g["neutral"].to_numpy()

        # exp_margin = predict_margin(...), vectorized. HFA is added in POINTS,
        # matching predict_margin -- routing it through the slope (the old
        # `ns * 2*hfa_epa` form) tied the two together so a change to either
        # silently moved the home-field advantage.
        exp = ns * (hn - an) + np.where(neutral == 1, 0.0, hfa_pts)
        n = g.height
        raw = exp + rng.normal(0.0, md, n)
        # round away from zero (matches the engine's _round_out margin convention).
        sim_result = np.where(raw > 0, np.ceil(raw), np.where(raw < 0, np.floor(raw), 0.0))

        if "game_type" in g.columns:
            gt = g["game_type"].to_numpy()
            wp = norm.cdf(exp / md)
            tie = (gt != "REG") & (sim_result == 0.0)
            sim_result = np.where(tie, np.where(rng.uniform(0.0, 1.0, n) < wp, 1.0, -1.0), sim_result)

        g = g.with_columns(pl.Series("_sim_result", sim_result, dtype=pl.Float64)).with_columns(
            pl.when((pl.col("week") == week_num) & pl.col("result").is_null())
            .then(pl.col("_sim_result"))
            .otherwise(pl.col("result"))
            .cast(pl.Float64)
            .alias("result")
        )
        return {"teams": teams, "games": g.select(games.columns)}

    return compute_results


@overload
def cfb_season_odds(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    n_sims: int = ...,
    playoff_seeds: int = ...,
    seed: int = ...,
    era: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_season_odds(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    n_sims: int = ...,
    playoff_seeds: int = ...,
    seed: int = ...,
    era: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_season_odds(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    n_sims: int = 10000,
    playoff_seeds: int = 12,
    seed: int = 0,
    era: str = "modern",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Ratings-driven season Monte Carlo: conference / playoff / championship odds.

    Thin wrapper over :func:`cfb_simulations.cfb_simulations` -- it builds the ratings
    with :func:`cfb_ratings.cfb_ratings`, converts the schedule to the engine format
    with :func:`cfb_standings.cfb_games_from_schedule` (re-keyed on ESPN ``team_id`` so
    the ratings align), and feeds :func:`make_ratings_compute_results` as the sampler.
    All season / standings / bracket machinery is reused; unplayed games are simulated,
    played games (before ``as_of_date``) are kept. Only FBS programs (schedule
    ``division == "fbs"``) enter the simulated universe; non-FBS opponents stay in the
    game set -- their games still count toward FBS records -- but can never reach the
    standings, the playoff field, or the output.

    Args:
        seasons: A single season (an ``int``, or a one-element list). Multiple
            seasons raise ``ValueError`` -- the simulation engine is single-season.
        as_of_date: Leakage boundary applied to BOTH the ratings vintage and the game
            set. Ratings are fit only on plays from games with ``date < as_of_date``
            (:func:`cfb_ratings.cfb_ratings`), and schedule results from ``start_date``
            on/after ``as_of_date`` are masked so those games are simulated instead of
            replayed; masked postseason rows are dropped (the matchup is itself an
            outcome) and regenerated from each sim's own standings. ``None`` uses the
            full season as-is.
        n_sims: Number of simulated seasons.
        playoff_seeds: CFP field size.
        seed: RNG seed for reproducibility.
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per team: ``season``, ``team_id`` (Utf8), ``exp_wins``,
        ``conf_title_prob``, ``playoff_prob``, ``first_round_bye_prob``,
        ``cfp_champ_prob`` (Float64 probabilities in [0, 1]). Zero-row (typed) when
        no ratings/schedule are available.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds
            odds = cfb_season_odds(2023, n_sims=2000)
            odds.sort("cfp_champ_prob", descending=True).head()

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the simulation engine reused here.
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    # Single-season only: the reused cfb_simulations engine runs one week-loop over all
    # games, so multiple seasons would be mixed into one simulated season (and the output
    # `season` column would be null). Fail fast instead of returning wrong results.
    if len(season_list) != 1:
        raise ValueError(
            "cfb_season_odds simulates one season at a time (the cfb_simulations engine "
            f"mixes weeks across seasons); got {len(season_list)} seasons ({season_list}). "
            "Call it once per season."
        )
    # cfb_ratings takes a RatingsConfig, not an era; era feeds the sampler/get_constants.
    ratings = cfb_ratings(seasons, as_of_date=as_of_date)
    schedule = load_cfb_schedule(season_list)
    if ratings.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_ODDS_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    # Re-key the engine on ESPN team_id (Utf8) so it aligns with ratings.team_id -- the
    # schedule ships numeric home_id/away_id alongside the display names.
    schedule = schedule.with_columns(
        pl.col("home_id").cast(pl.Utf8).alias("home_team"),
        pl.col("away_id").cast(pl.Utf8).alias("away_team"),
    )
    # `as_of_date` bounds the GAME SET, not just the ratings vintage. Without this a
    # historical as-of run conditions on the whole season's realized results and
    # "simulates" nothing -- every probability comes back 0.0/1.0 (issue #334). The
    # boundary matches cfb_ratings (`date < as_of_date` is knowable), so a game
    # kicking off ON as_of_date is FUTURE and must be simulated.
    if as_of_date is not None:
        schedule = _mask_after(schedule, as_of_date)
    # Keep only the engine's core columns: cfb_games_from_schedule also emits
    # home_points/away_points (SEC capped-scoring tiebreaker), but cfb_simulations
    # concatenates generated conf-champ/bracket games that lack them -> width mismatch.
    # The ratings sampler doesn't use that tiebreaker rung, so dropping them is safe.
    games = cfb_games_from_schedule(schedule).select(
        "season", "week", "game_type", "home_team", "away_team", "result", "neutral"
    )
    if as_of_date is not None:
        # A postseason MATCHUP is itself an outcome of the season, so an unplayed
        # conf-champ / bowl / CFP row would leak the real bracket into the forecast.
        # Drop them; cfb_simulations regenerates both from each sim's own standings.
        games = games.filter((pl.col("game_type") == "REG") | pl.col("result").is_not_null())
    teams = (
        schedule.select(team=pl.col("home_team"), conference=pl.col("home_conference"))
        .vstack(schedule.select(team=pl.col("away_team"), conference=pl.col("away_conference")))
        .drop_nulls("team")
        .unique(subset=["team"], keep="first")
    )
    # Restrict the SIMULATED UNIVERSE to the FBS field (issue #333). The schedule carries
    # every opponent an FBS team played, and the sampler scores a team absent from
    # `ratings` as league-average -- so 571 FCS/D2/D3/NAIA programs were simulated as
    # median FBS teams and took ~21% of the championship probability. `teams` is the
    # engine's whole standings/seeding/output universe (`sims.join(teams, how="cross")`)
    # while records are computed from `games`, so filtering HERE and not there keeps
    # non-FBS opponents as opponents without letting them reach the playoff field.
    teams = teams.filter(pl.col("team").is_in(list(_fbs_team_ids(schedule, ratings))))
    if teams.is_empty():
        # Empty here means the id namespaces disagreed, not that no team qualified --
        # a silent pass-through would ship a zero-row board as if it were a result.
        raise ValueError(
            "cfb_season_odds: the FBS filter emptied the team set -- schedule team ids do "
            "not intersect the FBS/ratings id namespace. Check the id dtypes."
        )
    assert ratings.schema["team_id"] == teams.schema["team"] == pl.Utf8, (
        f"ratings team_id {ratings.schema['team_id']} != engine team {teams.schema['team']}"
    )

    sim = cfb_simulations(
        games,
        teams,
        compute_results=make_ratings_compute_results(ratings, era=era),
        simulations=n_sims,
        playoff_seeds=playoff_seeds,
        seed=seed,
    )
    overall = sim["overall"]
    assert isinstance(overall, pl.DataFrame)
    season_value = season_list[0] if len(season_list) == 1 else None
    out = overall.select(
        pl.lit(season_value).cast(pl.Int64).alias("season"),
        pl.col("team").alias("team_id"),
        pl.col("wins").alias("exp_wins"),
        pl.col("won_conf").alias("conf_title_prob"),
        pl.col("made_playoff").alias("playoff_prob"),
        pl.col("first_round_bye").alias("first_round_bye_prob"),
        pl.col("won_cfp").alias("cfp_champ_prob"),
    ).sort("cfp_champ_prob", "conf_title_prob", "exp_wins", descending=True)
    return out.to_pandas() if return_as_pandas else out
