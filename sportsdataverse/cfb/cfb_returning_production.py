"""Connelly-style returning production for college football (T2.2 model ④).

Of a team's season S-1 unit production, the weighted fraction attributable to
players still on the season-S roster. Offense weights attributed yardage
(passing + rushing + receiving); defense weights splash-event involvement
(sacks, interceptions, pass breakups, forced fumbles) — the per-play stats
dataset does not carry tackles, so defensive coverage is splash-play based.

Data: the ``cfbfastR-data`` hosted per-play player-stats parquet
(``player_stats/parquet/player_stats_{season}.parquet``) + ``load_cfb_rosters``.
Team identity joins on the normalized team name (crosswalk ``norm_key``).
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_loaders import load_cfb_rosters
from sportsdataverse.cfb.cfb_projection_constants import get_constants
from sportsdataverse._codegen_runtime import _read_release_parquet

__all__ = ["cfb_returning_production"]

_PLAYER_STATS_URL = (
    "https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/"
    "player_stats/parquet/player_stats_{season}.parquet"
)

_RETURNING_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "off_returning": pl.Float64,
    "def_returning": pl.Float64,
    "overall_returning": pl.Float64,
    "n_returning": pl.Int64,
}

# (player-id column, weight expression source, unit, team-name column)
_OFFENSE_EVENTS: list[tuple[str, str]] = [
    ("completion_player_id", "completion_yds"),
    ("rush_player_id", "rush_yds"),
    ("reception_player_id", "reception_yds"),
]
_DEFENSE_EVENTS: list[str] = [
    "sack_player_id",
    "interception_player_id",
    "pass_breakup_player_id",
    "fumble_forced_player_id",
]


def _returning_from_frames(
    prod_prev: pl.DataFrame, roster_curr: pl.DataFrame, *, division: str = "fbs"
) -> pl.DataFrame:
    """Pure-frame core: season S-1 production + season S roster -> returning fractions.

    Args:
        prod_prev: Per-player production for season S-1 — ``season`` (Int64, the
            production season), ``team_id`` (Utf8), ``player_id`` (Utf8), ``unit``
            ("offense" | "defense"), ``prod_weight`` (Float64), ``position`` (Utf8).
        roster_curr: Season-S roster keys — ``season`` (Int64), ``team_id`` (Utf8),
            ``player_id`` (Utf8).
        division: Division slug for the unit weights.

    Returns:
        Per ``(season, team_id)`` (season = S): ``off_returning``, ``def_returning``,
        ``overall_returning`` (Float64 fractions), ``n_returning`` (Int64).
    """
    empty_schema: dict[str, pl.PolarsDataType] = {
        "season": pl.Int64,
        "team_id": pl.Utf8,
        "off_returning": pl.Float64,
        "def_returning": pl.Float64,
        "overall_returning": pl.Float64,
        "n_returning": pl.Int64,
    }
    if prod_prev.height == 0:
        return pl.DataFrame(schema=empty_schema)
    w = get_constants(division).returning_prod_weights
    assert prod_prev.schema["player_id"] == roster_curr.schema["player_id"] == pl.Utf8
    prev = prod_prev.with_columns((pl.col("season") + 1).alias("season"))  # describe next season
    curr_keys = roster_curr.select("season", "team_id", "player_id").with_columns(pl.lit(True).alias("returning"))
    j = prev.join(curr_keys, on=["season", "team_id", "player_id"], how="left").with_columns(
        pl.col("returning").fill_null(False)
    )
    # unit weights deliberately do NOT scale prod_weight here: a constant factor
    # cancels inside the per-unit ret/tot fraction (and a 0 weight would 0/0 it);
    # they only shape how units combine into overall_returning below
    j = j.with_columns(pl.col("prod_weight").alias("wp"))
    agg = (
        j.group_by(["season", "team_id", "unit"])
        .agg(
            (pl.col("wp") * pl.col("returning").cast(pl.Float64)).sum().alias("ret"),
            pl.col("wp").sum().alias("tot"),
            (pl.col("returning").cast(pl.Int64)).sum().alias("n_returning"),
        )
        .with_columns((pl.col("ret") / pl.col("tot")).alias("frac"))
    )
    wide = agg.pivot(values="frac", index=["season", "team_id"], on="unit")
    for unit, out_col in (("offense", "off_returning"), ("defense", "def_returning")):
        wide = (
            wide.rename({unit: out_col})
            if unit in wide.columns
            else wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(out_col))
        )
    n = agg.group_by(["season", "team_id"]).agg(pl.col("n_returning").sum())
    w_off, w_def = w["offense"], w["defense"]
    if w_def == 0.0:
        overall = pl.col("off_returning")
    elif w_off == 0.0:
        overall = pl.col("def_returning")
    else:
        overall = (pl.col("off_returning") * w_off + pl.col("def_returning") * w_def) / (w_off + w_def)
    return (
        wide.join(n, on=["season", "team_id"], how="left")
        .with_columns(overall.alias("overall_returning"))
        .select("season", "team_id", "off_returning", "def_returning", "overall_returning", "n_returning")
    )


def _load_player_stats(season: int) -> pl.DataFrame:
    """One season of the hosted per-play player-stats parquet ({} on 404)."""
    df = _read_release_parquet(_PLAYER_STATS_URL.format(season=season))
    return df if df is not None else pl.DataFrame()


def _production_from_play_stats(stats: pl.DataFrame, season: int) -> pl.DataFrame:
    """Play-level attributed stats -> per-player unit production for one season.

    Offense = attributed yards (passer/rusher/receiver, floored at 0 per event
    sum); defense = splash-event counts (sacks, INTs, PBUs, FFs), credited to
    the defending team (``opponent`` of the possession team).
    """
    frames: list[pl.DataFrame] = []
    for id_col, yds_col in _OFFENSE_EVENTS:
        if id_col not in stats.columns or yds_col not in stats.columns:
            continue
        frames.append(
            stats.select(
                pl.col(id_col).cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
                pl.col("team").cast(pl.Utf8).alias("team"),
                pl.col(yds_col).cast(pl.Float64).alias("prod_weight"),
                pl.lit("offense").alias("unit"),
            ).drop_nulls(["player_id"])
        )
    for id_col in _DEFENSE_EVENTS:
        if id_col not in stats.columns:
            continue
        frames.append(
            stats.select(
                pl.col(id_col).cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
                pl.col("opponent").cast(pl.Utf8).alias("team"),
                pl.lit(1.0).alias("prod_weight"),
                pl.lit("defense").alias("unit"),
            ).drop_nulls(["player_id"])
        )
    if not frames:
        return pl.DataFrame(
            schema={
                "season": pl.Int64,
                "team_id": pl.Utf8,
                "player_id": pl.Utf8,
                "unit": pl.Utf8,
                "prod_weight": pl.Float64,
                "position": pl.Utf8,
            }
        )
    events = pl.concat(frames)
    return (
        events.group_by(["team", "player_id", "unit"])
        .agg(pl.col("prod_weight").sum())
        .with_columns(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            pl.col("prod_weight").clip(lower_bound=0.0),
            pl.col("team")
            .map_elements(_norm_team, return_dtype=pl.Utf8)
            .alias("team_id"),  # normalized team name is this spine's cross-source key
            pl.lit(None, dtype=pl.Utf8).alias("position"),
        )
        .select("season", "team_id", "player_id", "unit", "prod_weight", "position")
    )


def cfb_returning_production(
    seasons: int | list[int], *, division: str = "fbs", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Returning production per team-season (offense / defense / overall).

    For each requested season S, computes the fraction of season S-1 unit
    production attributable to players on the season-S roster (Bill Connelly's
    returning-production concept; unit weights from :func:`get_constants`).

    Args:
        seasons: Target season or list of seasons (production is drawn from S-1).
        division: Division slug for constants lookups.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        Per ``(season, team)``: ``off_returning``, ``def_returning``,
        ``overall_returning`` (Float64 fractions in [0, 1]), ``n_returning``
        (Int64 count of returning contributors). ``team`` is the normalized
        team-name key (crosswalk ``norm_key``). Zero-row (typed) when the
        hosted data is unavailable.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_returning_production
            rp = cfb_returning_production(2023)
            rp.sort("overall_returning", descending=True).head(10)

    See Also:
        * `cfbfastR`_ -- R sister package (hosted data producer).

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    out_frames: list[pl.DataFrame] = []
    for season in season_list:
        stats_prev = _load_player_stats(season - 1)
        if stats_prev.height == 0:
            continue
        prod_prev = _production_from_play_stats(stats_prev, season - 1)
        roster = load_cfb_rosters(season)
        if isinstance(roster, pd.DataFrame):
            roster = pl.from_pandas(roster)
        if roster.height == 0:
            continue
        roster_curr = roster.select(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            pl.col("team").cast(pl.Utf8).map_elements(_norm_team, return_dtype=pl.Utf8).alias("team_id"),
            pl.col("athlete_id").cast(pl.Utf8).alias("player_id"),
        )
        out_frames.append(_returning_from_frames(prod_prev, roster_curr, division=division))
    if not out_frames:
        empty = pl.DataFrame(schema=_RETURNING_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    out = (
        pl.concat(out_frames).rename({"team_id": "team"}).sort("season", "overall_returning", descending=[False, True])
    )
    return out.to_pandas() if return_as_pandas else out
