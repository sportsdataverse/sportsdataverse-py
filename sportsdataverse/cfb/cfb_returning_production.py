"""Connelly-style returning production for college football (T2.2 model ④).

Of a team's season S-1 unit production, the weighted fraction attributable to
players still on the season-S roster. Offense weights attributed yardage
(passing + rushing + receiving); defense weights TACKLE VOLUME plus splash
events (sacks, TFLs, pass breakups).

Data: ``load_cfb_player_box`` (ESPN per-game player box, offense) +
``load_cfb_play_participants`` / ``load_cfb_game_rosters`` (defense, production
seasons 2014+) or ``load_cfb_pbp`` splash ids (defense, 2004-2013) +
``load_cfb_rosters``. ``def_basis`` names the defensive source per row.

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

NOT SP+ PARITY. Bill Connelly's exact weights are not published. The per-event
weights below are chosen for interpretability; the offense/defense unit weights
that combine them into ``overall_returning`` are fitted (see
``cfb_projection_constants``). ``interceptions``
is deliberately EXCLUDED from the defensive weight: in the ESPN box that column
is interceptions THROWN (2,866 rows, all overlapping passing; zero overlap with
the defensive group), so counting it would credit quarterbacks for turnovers.
"""

from __future__ import annotations

import warnings

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_loaders import (
    load_cfb_game_rosters,
    load_cfb_pbp,
    load_cfb_play_participants,
    load_cfb_player_box,
    load_cfb_rosters,
    load_cfb_rosters_cfbd,
    load_cfb_team_info,
)
from sportsdataverse._codegen_runtime import _read_release_parquet
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_projection_constants import get_constants

__all__ = ["cfb_returning_production"]

#: Hosted per-play player stats. Used ONLY by the play-stats extractors kept
#: for cfb_draft_projection; returning production itself reads the player box.
_PLAYER_STATS_URL = (
    "https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/"
    "player_stats/parquet/player_stats_{season}.parquet"
)

#: Season-2003 offensive production, the one season ESPN's player box cannot
#: supply. ESPN starts in 2004 (0 athlete rows across sampled 2003 games; team
#: rosters likewise empty), so returning production for 2004 had no S-1 side and
#: raised SeasonNotFoundError. CFBD's play-by-play DOES start in 2003 and its
#: play text is name-tagged by team, so the offensive half is recoverable; the
#: producer parses it and hosts the result, because THIS package is keyless by
#: design and must not start requiring a CFBD API key to compute a season.
#:
#: Offense only, deliberately. 2003 play text carries no tacklers at all -- the
#: volume term of `_DEFENSE_BOX_WEIGHTS` is unrecoverable -- so 2004 carries a
#: null `def_returning` and an offense-only `overall_returning`
#: (`overall_basis == "offense"`).
_PRODUCTION_2003_URL = (
    "https://raw.githubusercontent.com/sportsdataverse/cfbfastR-cfb-data/main/data/cfb_production_2003.parquet"
)

#: The first season ESPN's player box can describe. Below it, `_load_box` has
#: nothing and the S-1 production side must come from `_PRODUCTION_2003_URL`.
_ESPN_BOX_FLOOR = 2004

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

#: First season ESPN play participants exist. From here defensive production is
#: built from participants (tacklers, assists, sackers, pass defenders), scored
#: the way the box counts the same plays and credited through game rosters --
#: 92-100% of teams every season (100% of FBS), where ESPN's defensive player box
#: covers 0% (2014-15) to 17-65% (2016-23). Validated against the box where both
#: are near-complete: player-level r = 0.944 (2024) / 0.980 (2025), team totals
#: 0.910 / 0.967; participants total 81% / 92% of the box's weight.
_PARTICIPANTS_FLOOR = 2014

#: Share of play-participant rows that must join a pbp play before a season's
#: defense is treated as complete. Measured 98.0% (2016), 99.4% (2020), 98.4% (2024):
#: roughly 17-19 participant games a season have no pbp.
_MIN_PLAY_MATCH = 0.95

#: Pre-participant defensive splash events on the play-by-play, keyed on the
#: defending team id: (player-id column, weight). No tacklers exist before 2014,
#: so this is a SPLASH-ONLY measure (no tackle volume) and is flagged via
#: `def_basis`. The interception here is the DEFENDER's (unlike the box column,
#: which is interceptions thrown).
_DEFENSE_PBP_SPLASH: tuple[tuple[str, float], ...] = (
    ("sack_player_id", 2.0),
    ("interception_player_id", 1.0),
    ("pass_breakup_player_id", 1.0),
    ("fumble_forced_player_id", 1.0),
)

#: Below this share of teams carrying a `def_returning`, it is not a league-wide
#: metric and a warning is emitted. Measured teams covered: 67-94% for 2005-2014
#: (pbp splash), 92-100% from 2015 (participants). A season falling below it
#: means a defensive source release is missing and the box fallback took over.
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
    #: Where S-1 defensive production came from: "participants" (2014+ play
    #: participants: tackle volume + splash), "pbp_splash" (2004-2013: splash
    #: events only, no tackle volume), or null when there is none.
    "def_basis": pl.Utf8,
    #: How `overall_returning` was combined for this team: "offense+defense"
    #: (fitted unit weights) or "offense" (no defensive value for the team).
    "overall_basis": pl.Utf8,
    #: True only where the S-1 production came from parsed play text rather than
    #: the ESPN box (2004 alone). Back-tested against 2005, where both sources
    #: exist, the play-text route tracks the box route at r=0.73 with a -0.085
    #: bias: it ranks teams well but reads systematically LOW, so a consumer
    #: comparing 2004 against its neighbours on level must filter on this.
    "is_estimated": pl.Boolean,
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


def _combine_units(weights: dict[str, float]) -> tuple[pl.Expr, pl.Expr]:
    """``(overall_returning, overall_basis)`` expressions over ``off_returning`` / ``def_returning``.

    A team with no defensive value falls back to offense rather than going null,
    and says so in ``overall_basis`` -- never a silently different number. The
    projection backtest gates score this same expression.
    """
    w_off, w_def = weights["offense"], weights["defense"]
    has_def = pl.col("def_returning").is_not_null()
    if w_def == 0.0:
        return pl.col("off_returning"), pl.lit("offense")
    if w_off == 0.0:
        return pl.col("def_returning"), pl.when(has_def).then(pl.lit("defense")).otherwise(pl.lit(None, dtype=pl.Utf8))
    weighted = (pl.col("off_returning") * w_off + pl.col("def_returning") * w_def) / (w_off + w_def)
    return (
        pl.when(has_def).then(weighted).otherwise(pl.col("off_returning")),
        pl.when(has_def).then(pl.lit("offense+defense")).otherwise(pl.lit("offense")),
    )


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
        "overall_basis": pl.Utf8,
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
    overall, basis = _combine_units(w)
    return (
        wide.join(n, on=["season", "team_id"], how="left")
        .with_columns(overall.alias("overall_returning"), basis.alias("overall_basis"))
        .select(
            "season", "team_id", "off_returning", "def_returning", "overall_returning", "n_returning", "overall_basis"
        )
    )


def _num(col: str) -> pl.Expr:
    """ESPN box stats ship as strings; cast to Float64, non-numeric -> null.

    `strict=False` matters: the box uses "--" for a stat group a player did not
    record, and a strict cast would raise on it.
    """
    return pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)


def _load_box(season: int) -> pl.DataFrame:
    """One season of the ESPN player box (empty frame when unavailable)."""
    try:
        box = load_cfb_player_box([season])
    except SeasonNotFoundError:
        # "empty frame when unavailable" is what this promised and what every
        # caller assumes; below the ESPN floor the loader RAISES instead, which
        # is how cfb_returning_production(2004) came to abort a whole build.
        return pl.DataFrame()
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


_PRODUCTION_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "unit": pl.Utf8,
    "prod_weight": pl.Float64,
    "position": pl.Utf8,
}


def _as_polars(frame: object) -> pl.DataFrame:
    if isinstance(frame, pd.DataFrame):
        return pl.from_pandas(frame)
    return frame if isinstance(frame, pl.DataFrame) else pl.DataFrame()


def _defense_rows(events: pl.DataFrame, season: int) -> pl.DataFrame:
    """(team_id, player_id, prod_weight) events -> the production schema, unit = defense."""
    if events.height == 0:
        return pl.DataFrame(schema=_PRODUCTION_SCHEMA)
    return (
        events.drop_nulls(["team_id", "player_id"])
        .group_by(["team_id", "player_id"])
        .agg(pl.col("prod_weight").sum())
        .filter(pl.col("prod_weight") > 0)
        .with_columns(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("player_name"),
            pl.lit("defense").alias("unit"),
            pl.lit(None, dtype=pl.Utf8).alias("position"),
        )
        .select(list(_PRODUCTION_SCHEMA))
    )


def _defense_from_participants(season: int) -> pl.DataFrame:
    """Season defensive production from ESPN play participants (2014+).

    Scored the way ESPN's box counts the same plays, with the box weights of
    :data:`_DEFENSE_BOX_WEIGHTS`. Measured on the 2025 box, a sack is also a
    tackle and a tackle for loss, and a shared tackle for loss is split. So, per
    play:

    * every distinct tackler, assister or sacker: a tackle, 1.0;
    * each sacker: 2.0 / n sackers (the box counts a half sack);
    * on a play that lost yardage or had a sack, the same distinct players share
      one tackle for loss, 1.0 / n;
    * each pass defender: 1.0.

    Forced fumbles are not scored: the box weights carry none. (The pbp-splash
    era counts them, because without tackles it has little else.)

    Each defender is credited to the team their game roster lists them on for
    that game, because the defending team is wrong on punts and turnover returns,
    where the tacklers play for the team in possession. A defender missing from
    the roster falls back to the play's defending team. The play is joined on
    ``(game_id, play_id)``, both cast to Int64 first because the pbp release ships
    the play id as Utf8 in some seasons (2014) and Int64 in others. A join below
    :data:`_MIN_PLAY_MATCH` warns, since a partial release would otherwise read
    as a real drop in production.
    """
    from sportsdataverse.football.usage_box import _decode_list_cell

    try:
        parts = _as_polars(load_cfb_play_participants([season]))
        pbp = _as_polars(load_cfb_pbp([season]))
    except SeasonNotFoundError:
        return pl.DataFrame(schema=_PRODUCTION_SCHEMA)
    need_parts = {"game_id", "play_id"}
    need_pbp = {"game_id", "id", "def_pos_team_id", "statYardage"}
    if parts.height == 0 or pbp.height == 0 or not need_parts <= set(parts.columns) or not need_pbp <= set(pbp.columns):
        return pl.DataFrame(schema=_PRODUCTION_SCHEMA)
    plays = pbp.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("id").cast(pl.Int64).alias("play_id"),
        pl.col("def_pos_team_id").cast(pl.Int64).cast(pl.Utf8).alias("def_team_id"),
        pl.col("statYardage").cast(pl.Float64).alias("yds"),
    )
    lists = []
    for col in ("tackler_player_ids", "assisted_by_player_ids", "sacked_by_player_ids", "pass_defender_player_ids"):
        if col not in parts.columns:
            lists.append(pl.lit([], dtype=pl.List(pl.Utf8)).alias(col))
        elif parts.schema[col] == pl.Utf8:
            lists.append(pl.col(col).map_elements(_decode_list_cell, return_dtype=pl.List(pl.Utf8)).alias(col))
        else:
            lists.append(pl.col(col).cast(pl.List(pl.Utf8), strict=False).alias(col))
    parts = parts.select(pl.col("game_id").cast(pl.Int64), pl.col("play_id").cast(pl.Int64), *lists)
    assert parts.schema["play_id"] == plays.schema["play_id"] == pl.Int64
    joined = parts.join(plays, on=["game_id", "play_id"], how="inner")
    match = joined.height / parts.height
    if match < _MIN_PLAY_MATCH:
        warnings.warn(
            f"cfb_returning_production: only {match:.1%} of season-{season} play participants join a pbp play "
            f"(floor {_MIN_PLAY_MATCH:.0%}); defensive production is partial.",
            UserWarning,
            stacklevel=3,
        )

    empty = pl.lit([], dtype=pl.List(pl.Utf8))
    plays_ = joined.with_columns(
        pl.col("sacked_by_player_ids").fill_null(empty).alias("sackers"),
        pl.concat_list(
            pl.col("tackler_player_ids").fill_null(empty),
            pl.col("assisted_by_player_ids").fill_null(empty),
            pl.col("sacked_by_player_ids").fill_null(empty),
        )
        .list.unique()
        .alias("tacklers"),
    ).with_columns(
        pl.col("sackers").list.len().alias("n_sack"),
        pl.col("tacklers").list.len().alias("n_tack"),
        ((pl.col("yds") < 0) | (pl.col("sackers").list.len() > 0)).fill_null(False).alias("is_tfl"),
    )

    def credit(frame: pl.DataFrame, ids: str, weight: pl.Expr) -> pl.DataFrame:
        return (
            frame.filter(pl.col(ids).list.len() > 0)
            .with_columns(weight.alias("prod_weight"))
            .select("game_id", "def_team_id", pl.col(ids).alias("player_id"), "prod_weight")
            .explode("player_id", empty_as_null=False)
        )

    w = _DEFENSE_BOX_WEIGHTS
    events = pl.concat(
        [
            credit(plays_, "tacklers", pl.lit(w["totalTackles"])),
            credit(plays_, "sackers", w["sacks"] / pl.col("n_sack")),
            credit(plays_.filter(pl.col("is_tfl") == True), "tacklers", w["tacklesForLoss"] / pl.col("n_tack")),
            credit(
                joined.with_columns(pl.col("pass_defender_player_ids").fill_null(empty)),
                "pass_defender_player_ids",
                pl.lit(w["passesDefended"]),
            ),
        ],
        how="vertical_relaxed",
    )
    rosters = _game_roster_teams(season)
    if rosters.height:
        assert events.schema["player_id"] == rosters.schema["player_id"] == pl.Utf8
        events = events.join(rosters, on=["game_id", "player_id"], how="left")
    else:
        events = events.with_columns(pl.lit(None, dtype=pl.Utf8).alias("roster_team_id"))
    events = events.select(pl.coalesce("roster_team_id", "def_team_id").alias("team_id"), "player_id", "prod_weight")
    return _defense_rows(events, season)


def _game_roster_teams(season: int) -> pl.DataFrame:
    """``(game_id, player_id) -> roster_team_id`` from the season's game rosters (empty when unavailable).

    A player listed for both teams in one game (a handful per season) is
    dropped, so the play's defending team decides instead.
    """
    schema = {"game_id": pl.Int64, "player_id": pl.Utf8, "roster_team_id": pl.Utf8}
    try:
        gr = _as_polars(load_cfb_game_rosters([season]))
    except SeasonNotFoundError:
        return pl.DataFrame(schema=schema)
    if gr.height == 0 or not {"game_id", "athlete_id", "team_id"} <= set(gr.columns):
        return pl.DataFrame(schema=schema)
    keys = (
        gr.select(
            pl.col("game_id").cast(pl.Int64),
            pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
            pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("roster_team_id"),
        )
        .drop_nulls()
        .unique()
    )
    return keys.filter(pl.len().over("game_id", "player_id") == 1)


def _defense_from_pbp_splash(season: int) -> pl.DataFrame:
    """Season defensive production from play-by-play splash ids (2004-2013; no tackles)."""
    try:
        pbp = _as_polars(load_cfb_pbp([season]))
    except SeasonNotFoundError:
        return pl.DataFrame(schema=_PRODUCTION_SCHEMA)
    if pbp.height == 0 or "def_pos_team_id" not in pbp.columns:
        return pl.DataFrame(schema=_PRODUCTION_SCHEMA)
    team = pl.col("def_pos_team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id")
    events = [
        pbp.select(
            team,
            pl.col(col).cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
            pl.lit(w).alias("prod_weight"),
        )
        for col, w in _DEFENSE_PBP_SPLASH
        if col in pbp.columns
    ]
    return _defense_rows(pl.concat(events) if events else pl.DataFrame(), season)


_EMPTY_KEYS = {"season": pl.Int64, "team_id": pl.Utf8, "player_id": pl.Utf8}


def _roster_keys(season: int) -> pl.DataFrame:
    """Season-S roster as ``(season, team_id, player_id)`` with REAL team ids.

    Two sources are UNIONED, because neither is complete on its own:

    * **ESPN** (``espn_cfb_rosters``, what :func:`load_cfb_rosters` serves since
      #399): built from game rosters, so it carries ``team_id`` directly but
      only lists players who have dressed -- for the season in progress that is
      a handful of teams (2026 week 1: 4 teams, 476 rows), which made every
      returning-production fraction collapse toward zero.
    * **CFBD** (``cfbfastR-data`` rosters, :func:`load_cfb_rosters_cfbd`): the
      preseason roster for every team, keyed by school name and ESPN athlete
      id. Resolved against ``load_cfb_team_info``'s ``school`` column, NOT the
      teams crosswalk -- the crosswalk keys on school+mascot ("western kentucky
      hilltoppers") while this roster carries school only ("Western Kentucky"),
      which matched 0.0% of 2023 rows. Matching folds case and falls back
      through ESPN's alternate names. Its match rate is ASSERTED: an unresolved
      roster makes every player look departed, which reads as "this team
      returns nobody" instead of as a failure.

    A player is "on the roster" if either source lists them (Connelly's
    definition is roster membership, not appearances). Rows with a null id are
    dropped; the union is de-duplicated on ``(team_id, player_id)``.

    Raises:
        ValueError: If the CFBD roster is present but fewer than
            :data:`_MIN_ROSTER_MATCH` of its rows resolve to a team id.
    """
    parts = [_espn_roster_keys(season), _cfbd_roster_keys(season)]
    parts = [p for p in parts if p.height]
    if not parts:
        return pl.DataFrame(schema=_EMPTY_KEYS)
    return pl.concat(parts).unique(subset=["team_id", "player_id"], maintain_order=True)


def _espn_roster_keys(season: int) -> pl.DataFrame:
    """ESPN game-roster keys: ``team_id`` is carried directly, nothing to resolve."""
    roster = load_cfb_rosters(season)
    if isinstance(roster, pd.DataFrame):
        roster = pl.from_pandas(roster)
    if roster is None or roster.height == 0 or "team_id" not in roster.columns:
        return pl.DataFrame(schema=_EMPTY_KEYS)
    return roster.select(
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.col("team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        pl.col("athlete_id").cast(pl.Utf8).alias("player_id"),
    ).drop_nulls(["team_id", "player_id"])


def _cfbd_roster_keys(season: int) -> pl.DataFrame:
    """CFBD preseason-roster keys, school name resolved to the ESPN team id."""
    roster = load_cfb_rosters_cfbd(season)
    if isinstance(roster, pd.DataFrame):
        roster = pl.from_pandas(roster)
    if roster is None or roster.height == 0 or "team" not in roster.columns:
        return pl.DataFrame(schema=_EMPTY_KEYS)

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


def _load_production_2003() -> pl.DataFrame:
    """Hosted season-2003 offensive production ({} when unreachable).

    Already in `_production_from_box`'s output shape and keyed on ESPN athlete
    ids, so it drops straight into `_returning_from_frames`. A 2003 producer who
    is NOT on the 2004 roster carries a synthetic ``cfbd2003:`` id: they belong
    in the DENOMINATOR -- production that did not return -- and must never
    collide with a real athlete id.
    """
    df = _read_release_parquet(_PRODUCTION_2003_URL)
    return df if df is not None else pl.DataFrame()


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

    A null `def_returning` is a COVERAGE fact, not a defensive collapse, and
    silently returning a column that exists for a third of the league invites
    exactly the wrong reading. Those teams' `overall_returning` is offense only
    (`overall_basis == "offense"`).
    """
    if frame.height == 0 or "def_returning" not in frame.columns:
        return
    covered = frame["def_returning"].drop_nulls().len()
    share = covered / frame.height
    if share < _MIN_DEF_COVERAGE:
        warnings.warn(
            f"cfb_returning_production {season}: def_returning is present for only "
            f"{covered}/{frame.height} teams ({share:.0%}). Treat it as partial, not "
            "league-wide; overall_returning for the other teams is offense only "
            "(overall_basis == 'offense').",
            UserWarning,
            stacklevel=3,
        )


def _with_defense_source(prod_prev: pl.DataFrame, season: int) -> tuple[pl.DataFrame, str | None]:
    """Replace the box's defensive production with the season's coverage-complete source.

    2014+ reads play participants, 2004-2013 play-by-play splash ids. When that
    source yields nothing (a missing release), the box's own defense is kept and
    labelled ``"box"`` rather than silently dropping defense.
    """
    if season >= _PARTICIPANTS_FLOOR:
        defense, basis = _defense_from_participants(season), "participants"
    else:
        defense, basis = _defense_from_pbp_splash(season), "pbp_splash"
    if defense.height:
        return pl.concat([prod_prev.filter(pl.col("unit") == "offense"), defense.select(prod_prev.columns)]), basis
    box_defense = prod_prev.filter(pl.col("unit") == "defense").height > 0
    return prod_prev, ("box" if box_defense else None)


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
        (Int64 count of returning contributors), ``def_basis``,
        ``overall_basis`` (Utf8, below), ``is_estimated`` (Boolean).
        ``team_id`` is the ESPN team id as Utf8 -- BREAKING vs the previous
        release, which emitted a normalized team NAME under ``team`` and joined
        at 57.7%. Zero-row (typed) when the box data is unavailable.

        ``is_estimated`` is True for **2004 only**, where season-2003 production
        is parsed from CFBD play text because ESPN's player box starts in 2004.
        Back-tested on 2005, where both sources exist, that route tracks the box
        route at r=0.73 (MAE 0.11) but reads about 0.085 LOW. It ranks teams
        well; it is not on the same level as its neighbours, so filter on this
        flag before comparing 2004 against another season. 2004 also carries a
        null ``def_returning`` -- 2003 play text has no defensive ids.

        ``def_basis`` names the defensive measure: ``"participants"`` when the
        production season is 2014+ (tackles, assists, tackles for loss, shared
        sacks, passes defended -- 92-100% of teams), ``"pbp_splash"`` for
        2004-2013 (sacks, interceptions, pass breakups, forced fumbles; no
        tackle volume, so not on the same scale), ``"box"`` when that source's
        release is missing, null with ``def_returning``. ``overall_basis`` is
        ``"offense+defense"``, or ``"offense"`` for a team with no defensive
        value, whose overall then equals ``off_returning``.

    Raises:
        ValueError: If the season-S CFBD roster cannot be resolved to team ids
            above the crosswalk match floor (the ESPN roster carries ``team_id``
            and needs no resolution; the two are unioned, see
            :func:`_roster_keys`).

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
        estimated = False
        def_basis: str | None = None
        box_prev = _load_box(season - 1)
        if box_prev.height:
            prod_prev = _production_from_box(box_prev, season - 1)
            prod_prev, def_basis = _with_defense_source(prod_prev, season - 1)
        elif season - 1 == _ESPN_BOX_FLOOR - 1:
            # The one season the box cannot describe. Parsed 2003 play text is a
            # proxy, not the same measurement -- hence the flag on every row.
            prod_prev = _load_production_2003()
            estimated = prod_prev.height > 0
        else:
            continue
        if prod_prev.height == 0:
            continue
        roster_curr = _roster_keys(season)
        if roster_curr.height == 0:
            continue
        frame = _returning_from_frames(prod_prev, roster_curr, division=division)
        _warn_thin_defense(frame, season)
        out_frames.append(
            frame.with_columns(
                pl.when(pl.col("def_returning").is_not_null())
                .then(pl.lit(def_basis, dtype=pl.Utf8))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("def_basis"),
                pl.lit(estimated).alias("is_estimated"),
            ).select(list(_RETURNING_SCHEMA))
        )
    if not out_frames:
        empty = pl.DataFrame(schema=_RETURNING_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    out = pl.concat(out_frames).sort("season", "overall_returning", descending=[False, True])
    return out.to_pandas() if return_as_pandas else out
