"""Empirical-Bayes NFL player-prop projections (model 3 of T4.2).

Projects per-player usage x efficiency from ``load_nfl_player_stats``
week-level rows, shrunk toward position priors by games played
(``rate = (n * player_mean + kappa * prior) / (n + kappa)``), then scaled by
the opponent matchup (from the native ratings) and game script (from the
native expected margin). No market input anywhere in the projection: the
optional propbets ``line`` join feeds ``p_over`` display output only.
"""

from __future__ import annotations


import datetime
from typing import Literal, overload

import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nfl.nfl_loaders import load_nfl_player_stats, load_nfl_schedule
from sportsdataverse.nfl.nfl_market import nfl_predict_games
from sportsdataverse.nfl.nfl_prediction_constants import get_prop_constants
from sportsdataverse.nfl.nfl_ratings import nfl_ratings

__all__ = ["nfl_player_props", "player_usage_efficiency"]

# Matchup: EPA/play allowed above league mean converts ~1:1 into a relative
# volume-of-production swing (opp_adj_def +0.10 -> ~+10% projected output).
# ponytail: single documented constant; promote to a fitted PropConfig field
# if the Task-3.4 backtest shows the matchup term mis-scaled.
_MATCHUP_SCALE = 1.0
# Game script: expected margin (clipped at +/-14) tilts rush volume up and
# pass volume down for the favored side by up to +/-10%.
_GAME_SCRIPT_SCALE = 0.10
_GAME_SCRIPT_CLIP = 14.0

# (output column, per-game usage numerator | None, family kappa attr)
_USAGE_STATS: tuple[tuple[str, str, str], ...] = (
    ("attempts", "attempts", "shrink_pass"),
    ("carries", "carries", "shrink_rush"),
    ("targets", "targets", "shrink_rec"),
)

# (output column, numerator, denominator, prior key, family kappa attr)
_RATE_STATS: tuple[tuple[str, str, str, str, str], ...] = (
    ("ypa", "passing_yards", "attempts", "ypa", "shrink_pass"),
    ("pass_td_rate", "passing_tds", "attempts", "pass_td_rate", "shrink_pass"),
    ("ypc", "rushing_yards", "carries", "ypc", "shrink_rush"),
    ("rush_td_rate", "rushing_tds", "carries", "rush_td_rate", "shrink_rush"),
    ("ypt", "receiving_yards", "targets", "ypt", "shrink_rec"),
    ("rec_td_rate", "receiving_tds", "targets", "rec_td_rate", "shrink_rec"),
)

_USAGE_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "player_id": pl.Utf8,
    "position": pl.Utf8,
    "team_id": pl.Utf8,
    "games": pl.Int64,
    "exp_attempts": pl.Float64,
    "exp_carries": pl.Float64,
    "exp_targets": pl.Float64,
    "ypa": pl.Float64,
    "ypc": pl.Float64,
    "ypt": pl.Float64,
    "pass_td_rate": pl.Float64,
    "rush_td_rate": pl.Float64,
    "rec_td_rate": pl.Float64,
}


def player_usage_efficiency(player_stats: pl.DataFrame, *, as_of_week: int, era: str = "modern") -> pl.DataFrame:
    """Per-player as-of usage + efficiency with empirical-Bayes shrinkage.

    Aggregates one season of week-level player stats over weeks strictly
    before ``as_of_week`` (the leakage boundary), then shrinks every usage
    (per-game attempts / carries / targets) and efficiency (yards + TDs per
    opportunity) stat toward its position prior:
    ``(n * player_value + kappa * prior) / (n + kappa)`` with ``n`` = games
    played and ``kappa`` the stat family's fitted shrinkage.

    Args:
        player_stats: One season of ``load_nfl_player_stats()`` rows
            (columns ``player_id``, ``position``, ``recent_team``, ``week``,
            ``attempts``, ``passing_yards``, ``passing_tds``, ``carries``,
            ``rushing_yards``, ``rushing_tds``, ``targets``,
            ``receiving_yards``, ``receiving_tds``).
        as_of_week: Only weeks ``< as_of_week`` are used.
        era: Constants era key (supplies kappas + position priors).

    Returns:
        pl.DataFrame: One row per ``player_id`` (Utf8) whose position has a
        prior table: ``position`` / ``team_id`` (Utf8, latest team),
        ``games`` (Int64), ``exp_attempts`` / ``exp_carries`` /
        ``exp_targets`` (Float64, shrunk per-game usage), ``ypa`` / ``ypc``
        / ``ypt`` / ``pass_td_rate`` / ``rush_td_rate`` / ``rec_td_rate``
        (Float64, shrunk per-opportunity efficiency). Zero-row,
        correctly-typed on empty input.

    Example:
        Quick start::

            import polars as pl
            import sportsdataverse.nfl as nfl
            stats = nfl.load_nfl_player_stats().filter(pl.col("season") == 2023)
            usage = nfl.player_usage_efficiency(stats, as_of_week=10)
            usage.sort("exp_attempts", descending=True).head()
    """
    cfg = get_prop_constants(era)
    past = player_stats.filter(pl.col("week") < as_of_week)
    if past.height == 0:
        return pl.DataFrame(schema=_USAGE_OUTPUT_SCHEMA)

    agg = (
        past.sort("week")
        .group_by(pl.col("player_id").cast(pl.Utf8))
        .agg(
            pl.col("position").last(),
            pl.col("recent_team").cast(pl.Utf8).last().alias("team_id"),
            pl.col("week").n_unique().cast(pl.Int64).alias("games"),
            *[
                pl.col(c).sum().alias(f"sum_{c}")
                for c in (
                    "attempts",
                    "passing_yards",
                    "passing_tds",
                    "carries",
                    "rushing_yards",
                    "rushing_tds",
                    "targets",
                    "receiving_yards",
                    "receiving_tds",
                )
            ],
        )
        .filter(pl.col("position").is_in(list(cfg.pos_priors.keys())))
    )
    if agg.height == 0:
        return pl.DataFrame(schema=_USAGE_OUTPUT_SCHEMA)

    def _prior(key: str) -> pl.Expr:
        expr = pl.lit(0.0)
        for pos, priors in cfg.pos_priors.items():
            expr = pl.when(pl.col("position") == pos).then(pl.lit(priors.get(key, 0.0))).otherwise(expr)
        return expr.cast(pl.Float64)

    n = pl.col("games").cast(pl.Float64)
    exprs: list[pl.Expr] = []
    for out_col, num, kappa_attr in _USAGE_STATS:
        kappa = float(getattr(cfg, kappa_attr))
        per_game = pl.col(f"sum_{num}") / n
        exprs.append(((n * per_game + kappa * _prior(num)) / (n + kappa)).cast(pl.Float64).alias(f"exp_{out_col}"))
    for out_col, num, den, prior_key, kappa_attr in _RATE_STATS:
        kappa = float(getattr(cfg, kappa_attr))
        raw = (
            pl.when(pl.col(f"sum_{den}") > 0)
            .then(pl.col(f"sum_{num}") / pl.col(f"sum_{den}"))
            .otherwise(_prior(prior_key))
        )
        exprs.append(((n * raw + kappa * _prior(prior_key)) / (n + kappa)).cast(pl.Float64).alias(out_col))

    return agg.with_columns(exprs).select(*_USAGE_OUTPUT_SCHEMA.keys())


_PROPS_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "week": pl.Int64,
    "game_id": pl.Utf8,
    "player_id": pl.Utf8,
    "position": pl.Utf8,
    "team_id": pl.Utf8,
    "opp_team_id": pl.Utf8,
    "stat": pl.Utf8,
    "proj_mean": pl.Float64,
    "proj_sd": pl.Float64,
    "line": pl.Float64,
    "p_over": pl.Float64,
}

# Projected stat per position family: (position list, stat name, usage col, rate col, volume factor).
_STAT_FAMILIES: tuple[tuple[tuple[str, ...], str, str, str, str], ...] = (
    (("QB",), "passing_yards", "exp_attempts", "ypa", "pass"),
    (("RB",), "rushing_yards", "exp_carries", "ypc", "rush"),
    (("WR", "TE"), "receiving_yards", "exp_targets", "ypt", "pass"),
)


def _project_week(
    usage: pl.DataFrame,
    ratings: pl.DataFrame,
    games: pl.DataFrame,
    preds: pl.DataFrame,
    *,
    era: str,
) -> pl.DataFrame:
    """Per-player projections for one week's games (pure frame->frame)."""
    cfg = get_prop_constants(era)
    # Team-perspective rows: (game_id, team_id, opp_team_id, exp_margin_team).
    home = preds.select(
        "game_id",
        pl.col("home_team_id").alias("team_id"),
        pl.col("away_team_id").alias("opp_team_id"),
        pl.col("exp_margin").alias("exp_margin_team"),
    )
    away = preds.select(
        "game_id",
        pl.col("away_team_id").alias("team_id"),
        pl.col("home_team_id").alias("opp_team_id"),
        (-pl.col("exp_margin")).alias("exp_margin_team"),
    )
    sides = pl.concat([home, away])

    mean_def = float(ratings["adj_def_epa"].mean() or 0.0)
    opp_def = ratings.select(pl.col("team_id").alias("opp_team_id"), pl.col("adj_def_epa").alias("opp_adj_def"))
    assert sides.schema["opp_team_id"] == opp_def.schema["opp_team_id"]
    sides = (
        sides.join(opp_def, on="opp_team_id", how="left")
        .with_columns(
            matchup_mult=(1.0 + _MATCHUP_SCALE * (pl.col("opp_adj_def").fill_null(mean_def) - mean_def)),
            script=(pl.col("exp_margin_team").clip(-_GAME_SCRIPT_CLIP, _GAME_SCRIPT_CLIP) / _GAME_SCRIPT_CLIP),
        )
        .with_columns(
            pass_factor=1.0 - _GAME_SCRIPT_SCALE * pl.col("script"),
            rush_factor=1.0 + _GAME_SCRIPT_SCALE * pl.col("script"),
        )
    )

    assert usage.schema["team_id"] == sides.schema["team_id"]
    base = usage.join(sides, on="team_id", how="inner")
    frames = []
    for positions, stat, usage_col, rate_col, family in _STAT_FAMILIES:
        factor = pl.col("pass_factor") if family == "pass" else pl.col("rush_factor")
        frames.append(
            base.filter(pl.col("position").is_in(list(positions))).select(
                "game_id",
                "player_id",
                "position",
                "team_id",
                "opp_team_id",
                pl.lit(stat).alias("stat"),
                (pl.col(usage_col) * pl.col(rate_col) * pl.col("matchup_mult") * factor)
                .cast(pl.Float64)
                .alias("proj_mean"),
                pl.lit(cfg.proj_sds.get(stat)).cast(pl.Float64).alias("proj_sd"),
            )
        )
    return pl.concat(frames)


@overload
def nfl_player_props(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    era: str = ...,
    lines: pl.DataFrame | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def nfl_player_props(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    era: str = ...,
    lines: pl.DataFrame | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def nfl_player_props(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    era: str = "modern",
    lines: pl.DataFrame | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Empirical-Bayes player-prop projections, leakage-safe per week.

    For every game in the requested season(s) (or, with ``as_of_date``, every
    game on/after that date), projects each rostered QB/RB/WR/TE's stat-family
    mean as ``usage x efficiency x matchup x game-script``:

    - usage + efficiency from :func:`player_usage_efficiency` built **as-of
      that game's week** (weeks strictly before it),
    - the matchup multiplier from the opponent's ``adj_def_epa`` in
      :func:`sportsdataverse.nfl.nfl_ratings.nfl_ratings` (as-of the week's
      first game date),
    - game script from the **native** expected margin
      (:func:`sportsdataverse.nfl.nfl_market.nfl_predict_games`) -- the
      market line is never read (binding non-market boundary).

    Args:
        seasons: Season (e.g. ``2023``) or list of seasons.
        as_of_date: When given, only games with ``gameday >= as_of_date`` are
            projected (history before each game's week still feeds the
            projections). ``None`` projects every week of the season(s).
        era: Constants era key.
        lines: Optional market lines to score ``p_over`` against -- columns
            ``game_id`` / ``player_id`` / ``stat`` (Utf8) + ``line``
            (Float64), e.g. built from ``espn_nfl_game_propbets`` (ESPN only
            serves propbets for upcoming games). ``None`` leaves ``line`` /
            ``p_over`` null.
        return_as_pandas: If True, returns a pandas DataFrame.

    Returns:
        One row per (player-game, stat): ``season`` / ``week`` (Int64),
        ``game_id`` / ``player_id`` / ``position`` / ``team_id`` /
        ``opp_team_id`` / ``stat`` (Utf8), ``proj_mean`` / ``proj_sd`` /
        ``line`` / ``p_over`` (Float64; ``p_over = 1 - Phi((line -
        proj_mean) / proj_sd)`` when a line is joined, else null). Stats are
        ``passing_yards`` (QB), ``rushing_yards`` (RB), ``receiving_yards``
        (WR/TE). Zero-row, correctly-typed when there is nothing to project.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_player_props
            props = nfl_player_props(2023)
            props.filter(props["stat"] == "passing_yards").head()

        Upcoming-only, as-of a date::

            import datetime as dt
            props = nfl_player_props(2024, as_of_date=dt.date(2024, 11, 1))
    """
    season_list: list[int] = [seasons] if isinstance(seasons, int) else list(seasons)
    all_stats = load_nfl_player_stats()
    schedule = load_nfl_schedule(season_list)
    if schedule.is_empty() or all_stats.is_empty():
        empty = pl.DataFrame(schema=_PROPS_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    schedule = schedule.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("gameday").cast(pl.Date),
        (pl.col("location") == "Neutral").alias("neutral_site"),
    )
    frames: list[pl.DataFrame] = []
    for season in season_list:
        stats = all_stats.filter(pl.col("season") == season)
        sched = schedule.filter(pl.col("season") == season)
        target = sched if as_of_date is None else sched.filter(pl.col("gameday") >= as_of_date)
        for week in sorted(target["week"].unique().to_list()):
            week_games = target.filter(pl.col("week") == week)
            usage = player_usage_efficiency(stats, as_of_week=int(week), era=era)
            if usage.height == 0:
                continue
            cutoff = week_games["gameday"].min()
            ratings = nfl_ratings(season, as_of_date=cutoff)
            if ratings.height == 0:
                continue
            games = week_games.select(
                "game_id",
                pl.col("home_team").cast(pl.Utf8).alias("home_team_id"),
                pl.col("away_team").cast(pl.Utf8).alias("away_team_id"),
                "neutral_site",
            )
            preds = nfl_predict_games(games, ratings, era=era)
            projected = _project_week(usage, ratings, games, preds, era=era)
            frames.append(
                projected.with_columns(
                    pl.lit(season).cast(pl.Int64).alias("season"),
                    pl.lit(int(week)).cast(pl.Int64).alias("week"),
                )
            )
    if not frames:
        empty = pl.DataFrame(schema=_PROPS_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    out = pl.concat(frames)
    if lines is not None:
        lf = lines.select(
            pl.col("game_id").cast(pl.Utf8),
            pl.col("player_id").cast(pl.Utf8),
            pl.col("stat").cast(pl.Utf8),
            pl.col("line").cast(pl.Float64),
        )
        assert out.schema["player_id"] == lf.schema["player_id"]
        out = out.join(lf, on=["game_id", "player_id", "stat"], how="left")
    else:
        out = out.with_columns(line=pl.lit(None).cast(pl.Float64))
    p_over = 1.0 - norm.cdf((out["line"].to_numpy() - out["proj_mean"].to_numpy()) / out["proj_sd"].to_numpy())
    out = out.with_columns(pl.Series("p_over", p_over).cast(pl.Float64)).with_columns(
        # A missing line must yield a NULL p_over (not NaN from the numpy path).
        p_over=pl.when(pl.col("line").is_null()).then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("p_over"))
    )
    out = out.select(*_PROPS_OUTPUT_SCHEMA.keys())
    return out.to_pandas() if return_as_pandas else out
