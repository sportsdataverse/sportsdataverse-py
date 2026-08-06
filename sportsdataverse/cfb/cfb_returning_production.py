"""Connelly-style returning production for college football (T2.2 model ④).

Of a team's season S-1 unit production, the weighted fraction attributable to
players still on the season-S roster. Offense weights attributed yardage
(passing + rushing + receiving); defense weights TACKLE VOLUME plus splash
events (sacks, TFLs, pass breakups).

Data: ``load_cfb_player_box`` (ESPN per-game player box) + ``load_cfb_rosters``.

TWO THINGS CHANGED HERE, both because the prior version measured the wrong
thing quietly:

1. **Defense now counts tackles.** The per-play stats parquet carries no
   tackles, so defensive returning production was splash-only -- a defense
   whose tacklers all returned but whose one sack artist left scored as
   near-empty. The ESPN player box ships ``totalTackles`` / ``soloTackles`` /
   ``tacklesForLoss`` (19,416 non-null rows in 2023), so the volume term is
   available without any new dependency and without CFBD.

2. **Joins are on real ids.** ``team_id`` used to hold a NORMALIZED TEAM NAME
   on both sides; matching by name landed 57.7%. The box ships Int64
   ``team_id`` + ``athlete_id``, so production keys on ids. Rosters carry only
   a team name, so that one side is resolved through the crosswalk
   (``norm_key`` -> ``espn_team_id``) with an explicit match-rate floor -- a
   crosswalk regression fails loudly instead of silently zeroing returning
   production.

NOT SP+ PARITY. Bill Connelly's exact weights are not published, so these are
chosen for interpretability and stated below rather than fitted. ``interceptions``
is deliberately EXCLUDED from the defensive weight: in the ESPN box that column
is interceptions THROWN (2,866 rows, all overlapping passing; zero overlap with
the defensive group), so counting it would credit quarterbacks for turnovers.
"""

from __future__ import annotations

import warnings

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_loaders import load_cfb_player_box, load_cfb_rosters, load_cfb_team_info
from sportsdataverse._codegen_runtime import _read_release_parquet
from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_projection_constants import get_constants

__all__ = ["cfb_returning_production"]

#: Hosted per-play player stats. Used ONLY by the play-stats extractors kept
#: for cfb_draft_projection; returning production itself reads the player box.
_PLAYER_STATS_URL = (
    "https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/"
    "player_stats/parquet/player_stats_{season}.parquet"
)

#: Offensive production = attributed yardage. One row per athlete-game.
_OFFENSE_BOX_COLS: tuple[str, ...] = ("passingYards", "rushingYards", "receivingYards")

#: Defensive production weights. Tackles are the VOLUME term (they stand in for
#: defensive snaps played); the rest are splash events layered on top. Chosen
#: for interpretability and declared here so they are visible and tunable --
#: they are NOT Connelly's published weights, which do not exist publicly.
#:
#: `interceptions` is absent ON PURPOSE: in the ESPN box that column is INTs
#: THROWN (verified 2023 -- 2,866 rows, every one overlapping passing, zero
#: overlapping the defensive group), so including it credits QBs for turnovers.
#: `soloTackles` is absent because it is a subset of `totalTackles`.
_DEFENSE_BOX_WEIGHTS: dict[str, float] = {
    "totalTackles": 1.0,
    "sacks": 2.0,
    "tacklesForLoss": 1.0,
    "passesDefended": 1.0,
}

#: Below this share of teams carrying defensive box stats, `def_returning` is
#: not a league-wide metric and a warning is emitted. ESPN's defensive box
#: coverage is season-dependent and only recently near-complete (teams with
#: tackles / teams): 2016 34%, 2019 17%, 2021 55%, 2022 35%, 2023 65%, 2024 97%.
_MIN_DEF_COVERAGE = 0.60

#: Minimum share of roster rows that must resolve to a team id. Measured on
#: 2023: the crosswalk covers essentially every FBS roster; a drop below this
#: means the crosswalk or the roster team naming regressed, and returning
#: production would silently collapse toward zero.
_MIN_ROSTER_MATCH = 0.80

_RETURNING_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
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


def _num(col: str) -> pl.Expr:
    """ESPN box stats ship as strings; cast to Float64, non-numeric -> null.

    `strict=False` matters: the box uses "--" for a stat group a player did not
    record, and a strict cast would raise on it.
    """
    return pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)


def _load_box(season: int) -> pl.DataFrame:
    """One season of the ESPN player box (empty frame when unavailable)."""
    box = load_cfb_player_box([season])
    if isinstance(box, pd.DataFrame):
        box = pl.from_pandas(box)
    return box if box is not None else pl.DataFrame()


def _production_from_box(box: pl.DataFrame, season: int) -> pl.DataFrame:
    """Player box -> per-player unit production for one season, keyed on REAL ids.

    Offense is attributed yardage; defense is tackle volume plus splash events
    (see :data:`_DEFENSE_BOX_WEIGHTS`). Both units carry the box's own Int64
    ``team_id`` / ``athlete_id``, cast to Utf8 from the INTEGER (never from a
    float, which would stringify as "23.0" and match nothing).
    """
    schema: dict[str, pl.PolarsDataType] = {
        "season": pl.Int64,
        "team_id": pl.Utf8,
        "player_id": pl.Utf8,
        "player_name": pl.Utf8,
        "unit": pl.Utf8,
        "prod_weight": pl.Float64,
        "position": pl.Utf8,
    }
    if box.height == 0 or "athlete_id" not in box.columns or "team_id" not in box.columns:
        return pl.DataFrame(schema=schema)

    keys = [
        pl.col("team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        pl.col("athlete_id").cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
        (pl.col("athlete_name").cast(pl.Utf8) if "athlete_name" in box.columns else pl.lit(None, dtype=pl.Utf8)).alias(
            "player_name"
        ),
    ]

    frames: list[pl.DataFrame] = []
    off_cols = [c for c in _OFFENSE_BOX_COLS if c in box.columns]
    if off_cols:
        weight = pl.sum_horizontal([_num(c).fill_null(0.0) for c in off_cols])
        frames.append(box.select(*keys, weight.alias("prod_weight"), pl.lit("offense").alias("unit")))
    def_cols = {c: w for c, w in _DEFENSE_BOX_WEIGHTS.items() if c in box.columns}
    if def_cols:
        weight = pl.sum_horizontal([_num(c).fill_null(0.0) * w for c, w in def_cols.items()])
        frames.append(box.select(*keys, weight.alias("prod_weight"), pl.lit("defense").alias("unit")))
    if not frames:
        return pl.DataFrame(schema=schema)

    events = pl.concat(frames).drop_nulls(["player_id", "team_id"])
    return (
        events.group_by(["team_id", "player_id", "unit"])
        .agg(pl.col("prod_weight").sum(), pl.col("player_name").drop_nulls().first())
        .filter(pl.col("prod_weight") > 0)  # a 0-weight player contributes nothing either way
        .with_columns(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("position"),
        )
        .select("season", "team_id", "player_id", "player_name", "unit", "prod_weight", "position")
    )


def _roster_keys(season: int) -> pl.DataFrame:
    """Season-S roster as ``(season, team_id, player_id)`` with REAL team ids.

    Rosters ship a team NAME and no team id, so this is the one place a name is
    still involved. It resolves against ``load_cfb_team_info``'s ``school``
    column, NOT the teams crosswalk: the crosswalk keys on school+mascot
    ("western kentucky hilltoppers") while rosters carry school only ("Western
    Kentucky"), which matched 0.0% of 2023 rows. `team_info` carries school and
    `team_id` side by side, so the join is direct.

    Matching folds case (CLAUDE.md: names join case-insensitively unless case is
    load-bearing) and falls back through ESPN's alternate names.

    The match rate is ASSERTED rather than assumed -- an unresolved roster makes
    every player look departed, which reads as "this team returns nobody"
    instead of as a failure.

    Raises:
        ValueError: If fewer than :data:`_MIN_ROSTER_MATCH` of roster rows
            resolve to a team id.
    """
    roster = load_cfb_rosters(season)
    if isinstance(roster, pd.DataFrame):
        roster = pl.from_pandas(roster)
    if roster is None or roster.height == 0:
        return pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "player_id": pl.Utf8})

    info = load_cfb_team_info(season)
    if isinstance(info, pd.DataFrame):
        info = pl.from_pandas(info)

    name_cols = [c for c in ("school", "alt_name1", "alt_name2", "alt_name3") if c in info.columns]
    lookups = [
        info.select(
            pl.col(c).cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("key"),
            pl.col("team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        ).drop_nulls()
        for c in name_cols
    ]
    # school first, then alternates; unique(keep="first") preserves that priority
    lookup = pl.concat(lookups).unique(subset=["key"], keep="first", maintain_order=True)

    keyed = roster.select(
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.col("team").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("key"),
        pl.col("athlete_id").cast(pl.Utf8).alias("player_id"),
    ).drop_nulls(["player_id"])

    joined = keyed.join(lookup, on="key", how="left")
    matched = joined.filter(pl.col("team_id").is_not_null())
    rate = matched.height / joined.height if joined.height else 0.0
    if rate < _MIN_ROSTER_MATCH:
        unmatched = joined.filter(pl.col("team_id").is_null())["key"].unique(maintain_order=True).head(8).to_list()
        raise ValueError(
            f"cfb_returning_production: only {rate:.1%} of {season} roster rows resolved to a "
            f"team id (floor {_MIN_ROSTER_MATCH:.0%}). Returning production would collapse "
            f"toward zero without erroring. Unmatched examples: {unmatched}"
        )
    return matched.select("season", "team_id", "player_id")


# ---------------------------------------------------------------------------
# Play-stats extractors: RETAINED FOR cfb_draft_projection, which wants
# per-play attributed production at the player level. Returning production
# itself no longer uses these -- it reads the ESPN player box, which carries
# tackles and real ids (see the module docstring). Repointing the draft model
# at the box would silently move ITS outputs, which is a separate decision.
# ---------------------------------------------------------------------------


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
        name_col = id_col.removesuffix("_id")
        frames.append(
            stats.select(
                pl.col(id_col).cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
                (pl.col(name_col).cast(pl.Utf8) if name_col in stats.columns else pl.lit(None, dtype=pl.Utf8)).alias(
                    "player_name"
                ),
                pl.col("team").cast(pl.Utf8).alias("team"),
                pl.col(yds_col).cast(pl.Float64).alias("prod_weight"),
                pl.lit("offense").alias("unit"),
            ).drop_nulls(["player_id"])
        )
    for id_col in _DEFENSE_EVENTS:
        if id_col not in stats.columns:
            continue
        name_col = id_col.removesuffix("_id")
        frames.append(
            stats.select(
                pl.col(id_col).cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
                (pl.col(name_col).cast(pl.Utf8) if name_col in stats.columns else pl.lit(None, dtype=pl.Utf8)).alias(
                    "player_name"
                ),
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
        .agg(pl.col("prod_weight").sum(), pl.col("player_name").drop_nulls().first())
        .with_columns(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            pl.col("prod_weight").clip(lower_bound=0.0),
            pl.col("team")
            .map_elements(_norm_team, return_dtype=pl.Utf8)
            .alias("team_id"),  # normalized team name is this spine's cross-source key
            pl.lit(None, dtype=pl.Utf8).alias("position"),
        )
        .select("season", "team_id", "player_id", "player_name", "unit", "prod_weight", "position")
    )


def _warn_thin_defense(frame: pl.DataFrame, season: int) -> None:
    """Warn when `def_returning` is too sparse to read as a league-wide metric.

    ESPN's defensive player box is only recently near-complete, so for older
    seasons most teams have no tackle rows at all and `def_returning` is null
    for them. That is a COVERAGE fact, not a defensive collapse, and silently
    returning a column that exists for a third of the league invites exactly
    the wrong reading.
    """
    if frame.height == 0 or "def_returning" not in frame.columns:
        return
    covered = frame["def_returning"].drop_nulls().len()
    share = covered / frame.height
    if share < _MIN_DEF_COVERAGE:
        warnings.warn(
            f"cfb_returning_production {season}: def_returning is present for only "
            f"{covered}/{frame.height} teams ({share:.0%}) because ESPN's defensive box "
            "is sparsely populated that season. Treat it as partial, not league-wide; "
            "overall_returning is unaffected while the defense weight is 0.",
            UserWarning,
            stacklevel=3,
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
        Per ``(season, team_id)``: ``off_returning``, ``def_returning``,
        ``overall_returning`` (Float64 fractions in [0, 1]), ``n_returning``
        (Int64 count of returning contributors). ``team_id`` is the ESPN team
        id as Utf8 -- BREAKING vs the previous release, which emitted a
        normalized team NAME under ``team`` and joined at 57.7%. Zero-row
        (typed) when the box data is unavailable.

    Raises:
        ValueError: If the season-S roster cannot be resolved to team ids above
            the crosswalk match floor.

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
        box_prev = _load_box(season - 1)
        if box_prev.height == 0:
            continue
        prod_prev = _production_from_box(box_prev, season - 1)
        if prod_prev.height == 0:
            continue
        roster_curr = _roster_keys(season)
        if roster_curr.height == 0:
            continue
        frame = _returning_from_frames(prod_prev, roster_curr, division=division)
        _warn_thin_defense(frame, season)
        out_frames.append(frame)
    if not out_frames:
        empty = pl.DataFrame(schema=_RETURNING_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    out = pl.concat(out_frames).sort("season", "overall_returning", descending=[False, True])
    return out.to_pandas() if return_as_pandas else out
