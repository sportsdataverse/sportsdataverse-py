"""PWHL categorical-shot_quality xG proxy + best-effort power ratings (T5.3).

**First-of-its-kind, best-effort.** ``load_pwhl_pbp`` -- the actual
HockeyTech-derived PWHL play-by-play sdv-py ships -- carries a categorical
``shot_quality`` column (``"Quality on net"`` / ``"Non quality on net"`` /
``"Quality goal"`` / ``"Non quality goal"``), not a numeric ``xg`` like the
NHL api-web contract the rest of the T5.2/T5.3 spine assumes. This module is
a **separate, additive** path (it does not touch
:mod:`sportsdataverse.pwhl.pwhl_team_ratings` /
:mod:`sportsdataverse.pwhl.pwhl_market`, whose deferred NHL-contract shims
remain unchanged and still correctly report empty until an NHL-shaped PWHL
pbp adapter exists) that:

1. Fits an **empirical goal-rate weight per shot-quality tier** from real
   realized goals (:func:`fit_shot_quality_xg`) -- the xG proxy IS the
   observed scoring rate for that tier, exactly analogous to how
   :func:`sportsdataverse.nhl.nhl_microstat_constants.fit_shot_xg` fits
   on-demand, no bundled artifact.
2. Builds a ``team_game_xg_rates``-shaped per-(game, team) frame
   (:func:`pwhl_team_game_xg_rates`) from PWHL's own columns (``event``,
   ``team_id``, ``power_play`` for an even-strength approximation --
   PWHL has no skater-count/goalie-in columns like the NHL contract).
3. Feeds that frame into the **already-shared, league-agnostic** rating core
   (:func:`sportsdataverse.nhl.nhl_team_ratings.adjust_rate_opponent`) with
   PWHL's ``LEAGUE_CONSTANTS`` (:func:`pwhl_ratings_from_proxy`) to produce
   power ratings, then the equally shared
   :func:`sportsdataverse.nhl.nhl_market.nhl_predict_games` /
   :func:`~sportsdataverse.nhl.nhl_market.win_prob_from_margin` for win
   probability -- no re-implementation of the prediction math, only the
   PWHL-shape ingestion is new.

**Honest limitations (documented, not hidden):**

- ``shot_quality`` tautologically encodes outcome for two of its four values
  (``"Quality goal"`` / ``"Non quality goal"`` ARE goals by construction), so
  the proxy is really a 2-tier ("quality" vs "non_quality") empirical
  goal-rate, not an independent predictive xG model -- it cannot separate
  shot quality from shot outcome the way a coordinate-based model can.
- The even-strength filter uses ``power_play != 1`` (best-effort: PWHL's
  ``power_play``/``short_handed``/``empty_net`` tagging covers well under
  half of shot rows in the captured seasons, so this excludes only the
  confidently-tagged power-play shots rather than requiring full coverage).
- Game dates are read from ``load_pwhl_game_info``'s ``game_date_iso``, NOT
  ``load_pwhl_schedules``'s ``game_date`` (a year-less "Wed, May 8" string
  with no reliable chronological parse) -- see :func:`pwhl_team_game_xg_rates`.
- No external PWHL oracle exists (first-of-its-kind); the gate is a
  realized-outcome Brier/calibration backtest against a naive baseline --
  see ``tests/pwhl/test_pwhl_xg_proxy_oracle.py``.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_ratings_from_proxy
        from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games

        ratings = pwhl_ratings_from_proxy(2025)
        games = ratings.select("team").rename({"team": "home_team"})  # illustrative
        # preds = pwhl_predict_games(games, ratings)

See Also:
    * `fastRhockey`_ -- companion R PWHL/NHL client (raw HockeyTech capture).

.. _fastRhockey: https://fastRhockey.sportsdataverse.org
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Literal, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _as_season_list

_QUALITY_TIERS = ("quality", "non_quality")
_MIN_SHOTS_PER_TIER = 30

GAME_RATES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "date": pl.Date,
    "team": pl.Utf8,
    "opp_team": pl.Utf8,
    "is_home": pl.Boolean,
    "neutral_site": pl.Boolean,
    "xgf": pl.Float64,
    "xga": pl.Float64,
    "gf": pl.Int64,
    "ga": pl.Int64,
}

_RATINGS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "adj_xgf": pl.Float64,
    "adj_xga": pl.Float64,
    "adj_xg_net": pl.Float64,
    "adj_gf": pl.Float64,
    "adj_ga": pl.Float64,
    "games": pl.Int64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}


def _quality_tier_expr() -> pl.Expr:
    """Collapse the 4 raw ``shot_quality`` values into a 2-tier signal (Utf8, nullable)."""
    return (
        pl.when(pl.col("shot_quality").is_in(["Quality on net", "Quality goal"]))
        .then(pl.lit("quality"))
        .when(pl.col("shot_quality").is_in(["Non quality on net", "Non quality goal"]))
        .then(pl.lit("non_quality"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


@dataclass(frozen=True)
class ShotQualityXGModel:
    """A fitted empirical goal-rate-per-quality-tier PWHL xG proxy.

    Args:
        weights: Empirical goal rate (0-1) keyed by tier (:data:`_QUALITY_TIERS`).
        fallback_rate: Overall goal rate used for a tier with too few shots
            to trust its own rate, or for a shot with no resolvable tier.
    """

    weights: dict[str, float]
    fallback_rate: float

    def predict(self, shots: pl.DataFrame) -> pl.Series:
        """Score each row's shot-quality tier to its fitted empirical goal rate.

        Args:
            shots: Frame with a ``shot_quality`` column (PWHL pbp shape).

        Returns:
            A ``pl.Series`` named ``"xg"`` of per-row goal-rate proxy values.

        Example:
            Quick start::

                model.predict(shots_df)
        """
        if shots.height == 0:
            return pl.Series("xg", [], dtype=pl.Float64)
        tier = _quality_tier_expr()
        expr = pl.lit(self.fallback_rate, dtype=pl.Float64)
        for t in _QUALITY_TIERS:
            if t in self.weights:
                expr = pl.when(tier == t).then(pl.lit(self.weights[t])).otherwise(expr)
        return shots.select(expr.alias("xg"))["xg"]


def fit_shot_quality_xg(pbp: pl.DataFrame) -> ShotQualityXGModel:
    """Fit the empirical goal-rate-per-quality-tier PWHL xG proxy on demand.

    No bundled artifact, no first-use download -- mirrors
    :func:`sportsdataverse.nhl.nhl_microstat_constants.fit_shot_xg`'s
    fit-at-call-time contract. Filters to ``event == "shot"`` rows (the
    complete shot-attempt log; goals also appear as a separate, redundant
    ``event == "goal"`` scoring-summary row that this fit does NOT use, to
    avoid double-counting), derives the 2-tier quality signal, and computes
    each tier's realized goal rate. A tier with fewer than
    :data:`_MIN_SHOTS_PER_TIER` shots falls back to the overall rate.

    Args:
        pbp: A frame shaped like ``load_pwhl_pbp`` output (needs ``event``,
            ``shot_quality``, ``goal``).

    Returns:
        A fitted :class:`ShotQualityXGModel`.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_pbp
            from sportsdataverse.pwhl.pwhl_xg_proxy import fit_shot_quality_xg

            model = fit_shot_quality_xg(load_pwhl_pbp(2025))
    """
    if pbp.height == 0 or "event" not in pbp.columns or "shot_quality" not in pbp.columns:
        return ShotQualityXGModel(weights={}, fallback_rate=0.0)

    shots = pbp.filter(pl.col("event") == "shot").with_columns(_quality_tier_expr().alias("_tier"))
    shots = shots.filter(pl.col("_tier").is_not_null())
    if shots.height == 0:
        return ShotQualityXGModel(weights={}, fallback_rate=0.0)

    overall_rate = float(shots["goal"].cast(pl.Float64).mean() or 0.0)
    agg = shots.group_by("_tier").agg(pl.col("goal").cast(pl.Float64).mean().alias("rate"), pl.len().alias("n"))
    weights = {
        row["_tier"]: (row["rate"] if row["n"] >= _MIN_SHOTS_PER_TIER else overall_rate)
        for row in agg.iter_rows(named=True)
    }
    return ShotQualityXGModel(weights=weights, fallback_rate=overall_rate)


def pwhl_team_game_xg_rates(
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    game_info: pl.DataFrame | None = None,
    xg_model: ShotQualityXGModel | None = None,
    even_strength_only: bool = True,
) -> pl.DataFrame:
    """Per-(game, team) xG-proxy-for/against + realized goals from PWHL pbp.

    The PWHL-native counterpart to
    :func:`sportsdataverse.nhl.nhl_team_ratings.team_game_xg_rates`: built for
    ``load_pwhl_pbp``'s HockeyTech shape (no ``xg``, no skater-count/
    goalie-in columns) rather than the NHL api-web contract. ``team``/
    ``opp_team`` are resolved to schedule's clean team-name strings (pbp's
    own ``home_team``/``away_team`` carry a ``"PWHL "`` prefix that schedule's
    do not -- resolved via ``team_id``, never by name-matching across the two
    frames).

    Args:
        pbp: A frame shaped like ``load_pwhl_pbp(season)`` output for ONE
            season (seasons carry different column counts upstream -- see
            ``dev/pwhl_prediction/build_pwhl_xg_fixture.py`` -- so this
            function is single-season; callers loop + concat the output).
        schedule: ``load_pwhl_schedules(season)`` output for the SAME season.
        game_info: Optional ``load_pwhl_game_info(season)`` output, used only
            for its ``game_date_iso`` (schedule's own ``game_date`` has no
            year and cannot be reliably parsed). Without it, ``date`` is null
            and ``as_of_ratings_split`` filtering cannot be applied.
        xg_model: A fitted :class:`ShotQualityXGModel`; fit on ``pbp`` when
            ``None``.
        even_strength_only: Best-effort filter excluding shots with
            ``power_play == 1`` (see the module docstring's coverage caveat).

    Returns:
        One row per (game, team), both home and away.

        |col_name     |type   |
        |:------------|:------|
        |game_id      |String |
        |season       |Int64  |
        |date         |Date   |
        |team         |String |
        |opp_team     |String |
        |is_home      |Boolean|
        |neutral_site |Boolean|
        |xgf          |Float64|
        |xga          |Float64|
        |gf           |Int64  |
        |ga           |Int64  |

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_loaders import (
                load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules,
            )
            from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_team_game_xg_rates

            pbp = load_pwhl_pbp(2025)
            sched = load_pwhl_schedules(2025)
            info = load_pwhl_game_info(2025)
            rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info)
    """
    if pbp.is_empty() or schedule.is_empty():
        return pl.DataFrame(schema=GAME_RATES_SCHEMA)

    model = xg_model if xg_model is not None else fit_shot_quality_xg(pbp)
    shots = pbp.filter(pl.col("event") == "shot")
    if even_strength_only and "power_play" in shots.columns:
        # Best-effort even-strength filter (see module docstring): exclude only
        # confidently-tagged power-play shots. Nulls (the majority, sparse
        # tagging) are KEPT -- `!= 1` alone would drop them (null comparisons
        # are null, and filter() excludes null), so null is filled to 0 first.
        shots = shots.filter(pl.col("power_play").fill_null(0) != 1)
    if shots.height == 0:
        return pl.DataFrame(schema=GAME_RATES_SCHEMA)

    shots = shots.with_columns(
        model.predict(shots).alias("xg"),
        pl.col("goal").cast(pl.Int64).alias("_goal_i"),
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
    )
    per_team = shots.group_by(["game_id", "team_id"]).agg(
        pl.col("xg").sum().alias("xgf"), pl.col("_goal_i").sum().alias("gf")
    )

    sched = schedule.filter(pl.col("game_type") == "regular").select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("season").cast(pl.Int64),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("home_team"),
        pl.col("away_team_id").cast(pl.Int64),
        pl.col("away_team"),
    )
    if sched.is_empty():
        return pl.DataFrame(schema=GAME_RATES_SCHEMA)

    if game_info is not None and not game_info.is_empty():
        dates = game_info.select(
            pl.col("game_id").cast(pl.Int64),
            pl.col("game_date_iso").str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
        ).unique(subset=["game_id"])
        sched = sched.join(dates, on="game_id", how="left")
    else:
        sched = sched.with_columns(pl.lit(None, dtype=pl.Date).alias("date"))

    rows = []
    for is_home, id_col, name_col, opp_id_col, opp_name_col in (
        (True, "home_team_id", "home_team", "away_team_id", "away_team"),
        (False, "away_team_id", "away_team", "home_team_id", "home_team"),
    ):
        side = sched.select(
            "game_id",
            "season",
            "date",
            pl.col(id_col).alias("team_id"),
            pl.col(name_col).alias("team"),
            pl.col(opp_id_col).alias("opp_team_id"),
            pl.col(opp_name_col).alias("opp_team"),
            pl.lit(is_home).alias("is_home"),
            pl.lit(False).alias("neutral_site"),
        )
        side = side.join(per_team, on=["game_id", "team_id"], how="left")
        opp = per_team.rename({"team_id": "opp_team_id", "xgf": "xga", "gf": "ga"})
        side = side.join(opp, on=["game_id", "opp_team_id"], how="left")
        rows.append(side.drop("team_id", "opp_team_id"))

    out = pl.concat(rows, how="vertical_relaxed").with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("xgf").fill_null(0.0),
        pl.col("xga").fill_null(0.0),
        pl.col("gf").fill_null(0).cast(pl.Int64),
        pl.col("ga").fill_null(0).cast(pl.Int64),
    )
    return out.select(
        "game_id", "season", "date", "team", "opp_team", "is_home", "neutral_site", "xgf", "xga", "gf", "ga"
    )


@overload
def pwhl_ratings_from_proxy(
    seasons: Union[int, list[int]],
    *,
    as_of_date: _dt.date | None = ...,
    xg_model: ShotQualityXGModel | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def pwhl_ratings_from_proxy(
    seasons: Union[int, list[int]],
    *,
    as_of_date: _dt.date | None = ...,
    xg_model: ShotQualityXGModel | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def pwhl_ratings_from_proxy(
    seasons: Union[int, list[int]],
    *,
    as_of_date: _dt.date | None = None,
    xg_model: ShotQualityXGModel | None = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Best-effort PWHL power ratings from the categorical-shot_quality xG proxy.

    Loads pbp/schedule/game_info per season (seasons are loaded and processed
    individually -- ``load_pwhl_pbp`` carries a different column count
    season-to-season, so they cannot be concatenated raw), fits (or reuses)
    the shot-quality xG proxy, builds each season's
    :func:`pwhl_team_game_xg_rates`, concatenates, applies the as-of-date
    leakage split if requested, then opponent-adjusts + shrinks via the
    already-shared :func:`sportsdataverse.nhl.nhl_team_ratings.adjust_rate_opponent`
    with PWHL's fitted constants -- the identical rank-derivation the NHL
    core uses.

    Args:
        seasons: An int or iterable of PWHL season end-years (``>= 2024``).
        as_of_date: If given, only games strictly before this date are used
            (the leakage boundary for a predictive backtest).
        xg_model: A pre-fit :class:`ShotQualityXGModel` (e.g. fit once across
            multiple seasons for stability); fit per-season-pooled data when
            ``None``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per (season, team). Same
        shape as :func:`sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`.
        Empty input seasons return a zero-row frame with the documented schema.

        |col_name   |type   |
        |:----------|:------|
        |season     |Int64  |
        |team       |String |
        |adj_xgf    |Float64|
        |adj_xga    |Float64|
        |adj_xg_net |Float64|
        |adj_gf     |Float64|
        |adj_ga     |Float64|
        |games      |Int64  |
        |off_rank   |Int64  |
        |def_rank   |Int64  |
        |net_rank   |Int64  |
        |net_z      |Float64|

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_ratings_from_proxy

            ratings = pwhl_ratings_from_proxy(2025)
            print(ratings.sort("net_rank").head())

        Feed the shared NHL/PWHL market core for win probability::

            from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games
            preds = pwhl_predict_games(games, ratings)
    """
    from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, get_constants
    from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
    from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules

    const = get_constants("pwhl")
    all_rates = []
    for season in _as_season_list(seasons):
        pbp = load_pwhl_pbp(season)
        sched = load_pwhl_schedules(season)
        if pbp.is_empty() or sched.is_empty():
            continue
        info = load_pwhl_game_info(season)
        model = xg_model if xg_model is not None else fit_shot_quality_xg(pbp)
        rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info, xg_model=model)
        if not rates.is_empty():
            all_rates.append(rates)

    if not all_rates:
        return _empty_ratings(return_as_pandas)
    game_rates = pl.concat(all_rates, how="vertical_relaxed")
    if as_of_date is not None:
        game_rates = as_of_ratings_split(game_rates, as_of_date)
    if game_rates.is_empty():
        return _empty_ratings(return_as_pandas)

    out_frames = []
    for season_val in game_rates["season"].unique().sort().to_list():
        season_rates = game_rates.filter(pl.col("season") == season_val)
        xg_adj = adjust_rate_opponent(
            season_rates, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
        )
        avg_goals = const.avg_total_goals / 2.0
        goal_adj = adjust_rate_opponent(
            season_rates, for_col="gf", against_col="ga", hfa=const.hfa, avg=avg_goals, shrink_k=const.shrink_k
        )
        assert xg_adj.schema["team"] == goal_adj.schema["team"]
        season_out = xg_adj.join(
            goal_adj.select("team", pl.col("adj_for").alias("adj_gf"), pl.col("adj_against").alias("adj_ga")),
            on="team",
            how="left",
        ).rename({"adj_for": "adj_xgf", "adj_against": "adj_xga", "adj_net": "adj_xg_net"})
        out_frames.append(season_out)

    out = pl.concat(out_frames, how="vertical_relaxed")
    net_mean = out["adj_xg_net"].mean()
    net_std = out["adj_xg_net"].std()
    out = out.with_columns(
        pl.col("adj_xgf").rank(method="ordinal", descending=True).over("season").cast(pl.Int64).alias("off_rank"),
        pl.col("adj_xga").rank(method="ordinal", descending=False).over("season").cast(pl.Int64).alias("def_rank"),
        pl.col("adj_xg_net").rank(method="ordinal", descending=True).over("season").cast(pl.Int64).alias("net_rank"),
        (((pl.col("adj_xg_net") - net_mean) / net_std) if net_std else pl.lit(0.0)).alias("net_z"),
    ).select(
        "season",
        "team",
        "adj_xgf",
        "adj_xga",
        "adj_xg_net",
        "adj_gf",
        "adj_ga",
        "games",
        "off_rank",
        "def_rank",
        "net_rank",
        "net_z",
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _empty_ratings(return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    out = pl.DataFrame(schema=_RATINGS_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
