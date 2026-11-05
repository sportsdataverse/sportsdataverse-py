"""NBA offensive and defensive play-style archetypes for players and teams (BB-D20).

Four clusterings, each fit once on training seasons and applied as-of by
:mod:`sportsdataverse._common.play_style` (season-relative z-scores, frozen KMeans centers):

* **Offense, player** (2015-16+): the Synergy play-type mix (share of a player's play-type
  possessions in isolation, pick-and-roll ball handler, roll man, post-up, spot-up, handoff,
  off-screen, cut, transition and putbacks), his shot profile (three-point share of attempts,
  unassisted share of makes, paint and mid-range share of points) and tracking (drives, touches,
  seconds and dribbles per touch, catch-and-shoot and pull-up share of attempts, post and paint
  touches, potential assists).
* **Offense, team** (2015-16+): the team's Synergy mix (isolation, pick-and-roll ball handler,
  post-up, spot-up, off-screen, transition), its three-point share of attempts and mid-range share
  of points, and the possession start types: the mean length of a possession that starts after a
  defensive rebound and after a live-ball turnover.
* **Defense, player** (2017-18+): matchups (share of matchup time on guards, forwards and centers),
  hustle (deflections, contested twos and threes, charges drawn, defensive box-outs and loose
  balls), tracking defense (rim attempts defended, blocks, steals, defensive rebounds) and the
  Synergy play types he defended.
* **Defense, team** (2015-16+): the opponent shot profile (three-point and free-throw rate, paint
  and fast-break share of points, turnover and offensive rebound rate) and hustle (deflections per
  opponent possession, contested share of opponent threes).

The team sets are short on purpose: with 30 teams a season, team styles form a continuum and the
long player vectors split teams on noise. Team archetypes are the least stable of the four (see the
artifacts' ``stability``); read a team chip with its ``second_archetype`` and ``margin``.

Everything is read from the ``nba_stats_synergy``, ``nba_stats_hustle``, ``nba_stats_matchups``,
``nba_stats_leaguedash`` and ``nba_stats_possessions`` releases, regular season only. Every
feature is a rate or a share, so it does not depend on games played. A team's Synergy mix is the
sum of its players' Synergy possessions (the release carries Synergy per player only).

Seasons are named by the year they end (``2026`` is 2025-26). The bundled artifacts
(``nba/models/nba_play_style_*.json``) are refit by ``tools/models/play_style/fit_play_style.py``,
which records the training and holdout seasons and the stability report.
"""

from __future__ import annotations

import json
from functools import cache
from importlib.resources import files
from typing import Final

import pandas as pd
import polars as pl

from sportsdataverse._common.play_style import ASSIGN_SCHEMA, assign_play_style

__all__ = [
    "NBA_PLAY_STYLE_FEATURES",
    "NBA_PLAY_STYLE_FIRST_SEASON",
    "NBA_PLAY_STYLE_PHRASES",
    "nba_play_style_features",
    "nba_player_play_styles",
    "nba_team_play_styles",
]

#: The first season (end year) each clustering covers: Synergy and hustle start in 2015-16,
#: the matchup roll-up in 2017-18.
NBA_PLAY_STYLE_FIRST_SEASON: Final[dict[tuple[str, str], int]] = {
    ("offense", "player"): 2016,
    ("offense", "team"): 2016,
    ("defense", "player"): 2018,
    ("defense", "team"): 2016,
}

#: Player qualifier: season minutes at least this share of the season's most minutes. A share
#: rather than a count, so an in-season cut qualifies the same rotation players a full season does.
PLAYER_MIN_SHARE: Final[float] = 0.25

_OFF_TYPES: Final[tuple[str, ...]] = (
    "isolation",
    "prballhandler",
    "prrollman",
    "postup",
    "spotup",
    "handoff",
    "offscreen",
    "cut",
    "transition",
    "offrebound",
)
_DEF_TYPES: Final[tuple[str, ...]] = (
    "isolation",
    "prballhandler",
    "prrollman",
    "postup",
    "spotup",
    "handoff",
    "offscreen",
)

#: The clustering features of each (side, entity), in artifact order. The team sets are short on
#: purpose: with 30 teams a season, the full player-style vectors split teams on noise (the
#: holdout ARI of an 18-feature team fit was 0.2-0.3; see the fitter's sweep).
NBA_PLAY_STYLE_FEATURES: Final[dict[tuple[str, str], tuple[str, ...]]] = {
    ("offense", "player"): (
        *(f"pt_{t}" for t in _OFF_TYPES),
        "fga3_share",
        "uast_share",
        "paint_pts_share",
        "mid_pts_share",
        "drives_p36",
        "touches_p36",
        "sec_per_touch",
        "drib_per_touch",
        "catch_shoot_share",
        "pull_up_share",
        "post_touches_p36",
        "paint_touches_p36",
        "potential_ast_p36",
    ),
    ("offense", "team"): (
        "pt_isolation",
        "pt_prballhandler",
        "pt_postup",
        "pt_spotup",
        "pt_offscreen",
        "pt_transition",
        "fga3_share",
        "mid_pts_share",
        "sec_after_miss",
        "sec_after_live_tov",
    ),
    ("defense", "player"): (
        "match_g_share",
        "match_f_share",
        "match_c_share",
        "deflections_p36",
        "contested_2pt_p36",
        "contested_3pt_p36",
        "charges_p36",
        "def_boxouts_p36",
        "def_loose_balls_p36",
        "rim_fga_p36",
        "blk_p36",
        "stl_p36",
        "dreb_p36",
        *(f"dpt_{t}" for t in _DEF_TYPES),
    ),
    ("defense", "team"): (
        "opp_fg3a_rate",
        "opp_fta_rate",
        "opp_tov_rate",
        "opp_paint_share",
        "opp_fb_share",
        "opp_oreb_rate",
        "deflections_per_poss",
        "contested_3pt_rate",
    ),
}


_PHRASES: Final[dict[str, dict[str, str]]] = {
    "offense": {
        "pt_isolation": "isolation scorer",
        "pt_prballhandler": "pick-and-roll handler",
        "pt_prrollman": "roll man",
        "pt_postup": "post-up scorer",
        "pt_spotup": "spot-up shooter",
        "pt_handoff": "handoff",
        "pt_offscreen": "off-screen shooter",
        "pt_cut": "cutter",
        "pt_transition": "transition runner",
        "pt_offrebound": "putback",
        "fga3_share": "three-point volume",
        "uast_share": "self-creator",
        "paint_pts_share": "paint scorer",
        "mid_pts_share": "mid-range",
        "drives_p36": "driver",
        "touches_p36": "high-touch hub",
        "sec_per_touch": "primary ball handler",
        "drib_per_touch": "dribble creator",
        "catch_shoot_share": "catch-and-shoot",
        "pull_up_share": "pull-up shooter",
        "post_touches_p36": "post-up big",
        "paint_touches_p36": "rim runner",
        "potential_ast_p36": "playmaker",
        "sec_after_miss": "half court after misses",
        "sec_after_live_tov": "half court after steals",
    },
    "defense": {
        "match_g_share": "guard defender",
        "match_f_share": "wing defender",
        "match_c_share": "big-man defender",
        "deflections_p36": "deflection hunter",
        "contested_2pt_p36": "interior contester",
        "contested_3pt_p36": "perimeter contester",
        "charges_p36": "charge taker",
        "def_boxouts_p36": "box-out",
        "def_loose_balls_p36": "loose-ball",
        "rim_fga_p36": "rim protector",
        "blk_p36": "shot blocker",
        "stl_p36": "ball hawk",
        "dreb_p36": "defensive rebounder",
        "dpt_isolation": "isolation stopper",
        "dpt_prballhandler": "pick-and-roll defender",
        "dpt_prrollman": "roll-man defender",
        "dpt_postup": "post defender",
        "dpt_spotup": "help-side defender",
        "dpt_handoff": "handoff defender",
        "dpt_offscreen": "screen chaser",
        "opp_fg3a_rate": "allows threes",
        "opp_fta_rate": "fouls",
        "opp_tov_rate": "forces turnovers",
        "opp_paint_share": "allows paint points",
        "opp_fb_share": "allows transition",
        "opp_oreb_rate": "allows offensive boards",
        "deflections_per_poss": "deflections",
        "contested_3pt_rate": "contests threes",
    },
}


_TEAM_OFFENSE_PHRASES: Final[dict[str, str]] = {
    "pt_isolation": "isolation-heavy",
    "pt_prballhandler": "pick-and-roll offense",
    "pt_postup": "post-up offense",
    "pt_spotup": "spot-up offense",
    "pt_offscreen": "off-screen motion",
    "pt_transition": "transition offense",
    "fga3_share": "three-point heavy",
    "mid_pts_share": "mid-range heavy",
}

#: The label phrase of each feature, by (side, entity): an archetype is named after the feature its
#: center is highest on (:func:`sportsdataverse._common.play_style.label_centers`).
NBA_PLAY_STYLE_PHRASES: Final[dict[tuple[str, str], dict[str, str]]] = {
    ("offense", "player"): _PHRASES["offense"],
    ("offense", "team"): _PHRASES["offense"] | _TEAM_OFFENSE_PHRASES,
    ("defense", "player"): _PHRASES["defense"],
    ("defense", "team"): _PHRASES["defense"],
}


def _ratio(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    """``num / den`` as Float64, null where ``den`` is not positive."""
    return pl.when(den > 0).then(num.cast(pl.Float64) / den.cast(pl.Float64))


def _p36(col: str, minutes: str = "min") -> pl.Expr:
    return (36.0 * _ratio(pl.col(col), pl.col(minutes))).alias(f"{col}_p36")


def _regular(df: pl.DataFrame) -> pl.DataFrame:
    """Regular-season rows of a ``leaguedash`` family (``"Regular Season"``)."""
    return df.filter(pl.col("season_type") == "Regular Season") if df.height else df


def synergy_mix(synergy: pl.DataFrame, *, by: str, grouping: str) -> pl.DataFrame:
    """Each entity's share of its Synergy possessions by play type (``pt_*`` or ``dpt_*``).

    stats.nba.com publishes a player's play type only above a volume floor, so the published rows
    of a player do not add up to his possessions. Each row's ``poss / poss_pct`` recovers the
    stint's total (``poss_pct`` is the play type's share of all his possessions with that team),
    and a share is ``poss`` over that total, as stats.nba.com computes it; an unpublished play
    type counts 0. A player traded mid-season sums his stints, and a team sums its players'.

    Args:
        synergy: ``load_nba_stats_synergy`` totals rows (one season type) with ``season``,
            ``player_id``, ``team_id``, ``type_grouping``, ``play_type``, ``poss`` and ``poss_pct``.
        by: ``"player_id"`` or ``"team_id"``.
        grouping: ``"Offensive"`` or ``"Defensive"``.

    Returns:
        One row per ``(by, season)``: ``syn_poss`` (the stints' total possessions) and one share
        column per play type.
    """
    types, prefix = (_OFF_TYPES, "pt_") if grouping == "Offensive" else (_DEF_TYPES, "dpt_")
    rows = synergy.filter(pl.col("type_grouping") == grouping).with_columns(
        pl.col("play_type").str.to_lowercase(), pl.col("poss").cast(pl.Float64)
    )
    stint = ["player_id", "team_id", "season"]
    implied = pl.col("poss") / pl.col("poss_pct").cast(pl.Float64)
    totals = rows.group_by(stint).agg(
        pl.coalesce(implied.filter(pl.col("poss_pct") > 0).median(), pl.col("poss").sum()).alias("syn_poss")
    )
    total = totals.group_by(by, "season").agg(pl.col("syn_poss").sum())
    wide = (
        rows.filter(pl.col("play_type").is_in(types))
        .group_by(by, "season", "play_type")
        .agg(pl.col("poss").sum())
        .pivot(on="play_type", index=[by, "season"], values="poss")
    )
    for t in types:
        if t not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(t))
    return total.join(wide, on=[by, "season"], how="left").select(
        by,
        "season",
        "syn_poss",
        *[_ratio(pl.col(t).fill_null(0.0), pl.col("syn_poss")).alias(f"{prefix}{t}") for t in types],
    )


def _qualify(df: pl.DataFrame) -> pl.DataFrame:
    """Player-seasons with at least :data:`PLAYER_MIN_SHARE` of the season's most minutes."""
    return df.filter(pl.col("min") >= PLAYER_MIN_SHARE * pl.col("min").max().over("season"))


def offense_player_features(
    synergy: pl.DataFrame, scoring: pl.DataFrame, tracking: dict[str, pl.DataFrame]
) -> pl.DataFrame:
    """Offensive play-style features per qualifying player-season.

    Args:
        synergy: Offensive Synergy totals rows (regular season).
        scoring: ``leaguedash`` ``player_stats_scoring`` rows.
        tracking: ``leaguedash`` ``player_tracking_*`` rows keyed by family suffix:
            ``drives``, ``possessions``, ``catchshoot``, ``pullupshot``, ``passing``.

    Returns:
        One row per qualifying ``(player_id, season)``: ``player_id`` (Int64), ``player_name``,
        ``team_id`` (Int64, the leaguedash team), ``season``, ``min`` and the offense-player
        features of :data:`NBA_PLAY_STYLE_FEATURES`.
    """
    key = ["player_id", "season"]
    base = _regular(tracking["drives"]).select(*key, "player_name", "team_id", "min", _p36("drives"), "drive_fga")
    sc = _regular(scoring).select(
        *key,
        "fga",
        pl.col("pct_fga_3_pt").alias("fga3_share"),
        pl.col("pct_uast_fgm").alias("uast_share"),
        pl.col("pct_pts_paint").alias("paint_pts_share"),
        pl.col("pct_pts_2_pt_mr").alias("mid_pts_share"),
    )
    po = _regular(tracking["possessions"]).select(
        *key,
        (36.0 * _ratio(pl.col("touches"), pl.col("min"))).alias("touches_p36"),
        pl.col("avg_sec_per_touch").alias("sec_per_touch"),
        pl.col("avg_drib_per_touch").alias("drib_per_touch"),
        (36.0 * _ratio(pl.col("post_touches"), pl.col("min"))).alias("post_touches_p36"),
        (36.0 * _ratio(pl.col("paint_touches"), pl.col("min"))).alias("paint_touches_p36"),
    )
    cs = _regular(tracking["catchshoot"]).select(*key, "catch_shoot_fga")
    pu = _regular(tracking["pullupshot"]).select(*key, "pull_up_fga")
    pa = _regular(tracking["passing"]).select(
        *key, (36.0 * _ratio(pl.col("potential_ast"), pl.col("min"))).alias("potential_ast_p36")
    )
    out = base
    for part in (sc, po, cs, pu, pa, synergy_mix(synergy, by="player_id", grouping="Offensive")):
        out = out.join(part, on=key, how="left")
    out = out.with_columns(
        _ratio(pl.col("catch_shoot_fga"), pl.col("fga")).alias("catch_shoot_share"),
        _ratio(pl.col("pull_up_fga"), pl.col("fga")).alias("pull_up_share"),
    ).filter(pl.col("syn_poss") > 0)
    return _qualify(out).select(*key, "player_name", "team_id", "min", *NBA_PLAY_STYLE_FEATURES[("offense", "player")])


def _possession_tempo(possessions: pl.DataFrame) -> pl.DataFrame:
    """Mean possession length (seconds) after a defensive rebound and after a steal, per offense team-season.

    A possession runs from the previous possession's end (the rebound, steal or make that gave
    the ball over) to its own end; the release's ``start_seconds_remaining`` is the time of the
    possession's first own event, not the change of possession, so the length is the previous
    ``end_seconds_remaining`` in the game and period minus this one. A period's first possession
    has none and is skipped. Regular-season games only.
    """
    starts = {"OffMissedShot": "sec_after_miss", "OffLiveBallTurnover": "sec_after_live_tov"}
    regular = pl.col("game_id").cast(pl.Utf8).str.zfill(10).str.starts_with("002")
    p = possessions.filter(regular).sort("game_id", "period", "possession_number")
    p = p.with_columns(
        (pl.col("end_seconds_remaining").shift(1).over("game_id", "period") - pl.col("end_seconds_remaining")).alias(
            "_len"
        )
    ).filter(pl.col("possession_start_type").is_in(list(starts)) & pl.col("_len").is_not_null())
    if "count_as_possession" in p.columns:
        p = p.filter(pl.col("count_as_possession") == True)  # noqa: E712
    return (
        p.with_columns(pl.col("offense_team_id").cast(pl.Int64).alias("team_id"))
        .group_by("team_id", "season", "possession_start_type")
        .agg(pl.col("_len").mean())
        .pivot(on="possession_start_type", index=["team_id", "season"], values="_len")
        .rename(starts, strict=False)
    )


def offense_team_features(synergy: pl.DataFrame, team_scoring: pl.DataFrame, possessions: pl.DataFrame) -> pl.DataFrame:
    """Offensive play-style features per team-season.

    Args:
        synergy: Offensive Synergy totals rows (regular season, player level).
        team_scoring: ``leaguedash`` ``team_stats_scoring`` rows.
        possessions: ``nba_stats_possessions`` rows with ``season`` set to the end year.

    Returns:
        One row per ``(team_id, season)``: ``team_id`` (Int64), ``team_name``, ``season`` and the
        offense-team features of :data:`NBA_PLAY_STYLE_FEATURES`.
    """
    key = ["team_id", "season"]
    sc = _regular(team_scoring).select(
        *key,
        "team_name",
        pl.col("pct_fga_3_pt").alias("fga3_share"),
        pl.col("pct_pts_2_pt_mr").alias("mid_pts_share"),
    )
    out = sc.join(synergy_mix(synergy, by="team_id", grouping="Offensive"), on=key, how="inner")
    out = out.join(_possession_tempo(possessions), on=key, how="left")
    return out.select(*key, "team_name", *NBA_PLAY_STYLE_FEATURES[("offense", "team")])


def defense_player_features(
    synergy: pl.DataFrame, rollup: pl.DataFrame, hustle: pl.DataFrame, tracking_defense: pl.DataFrame
) -> pl.DataFrame:
    """Defensive play-style features per qualifying player-season.

    Args:
        synergy: Defensive Synergy totals rows (regular season).
        rollup: ``load_nba_stats_matchups_rollup`` regular-season totals rows.
        hustle: ``load_nba_stats_hustle_player`` regular-season totals rows.
        tracking_defense: ``leaguedash`` ``player_tracking_defense`` rows.

    Returns:
        One row per qualifying ``(player_id, season)``: ``player_id`` (Int64), ``player_name``,
        ``team_id``, ``season``, ``min`` and the defense-player features of
        :data:`NBA_PLAY_STYLE_FEATURES`. Players without matchup or hustle rows drop out.
    """
    key = ["player_id", "season"]
    td = _regular(tracking_defense).select(
        *key,
        "player_name",
        "team_id",
        "min",
        (36.0 * _ratio(pl.col("def_rim_fga"), pl.col("min"))).alias("rim_fga_p36"),
        (36.0 * _ratio(pl.col("blk"), pl.col("min"))).alias("blk_p36"),
        (36.0 * _ratio(pl.col("stl"), pl.col("min"))).alias("stl_p36"),
        (36.0 * _ratio(pl.col("dreb"), pl.col("min"))).alias("dreb_p36"),
    )
    hu_cols = {
        "deflections": "deflections_p36",
        "contested_shots_2_pt": "contested_2pt_p36",
        "contested_shots_3_pt": "contested_3pt_p36",
        "charges_drawn": "charges_p36",
        "def_boxouts": "def_boxouts_p36",
        "def_loose_balls_recovered": "def_loose_balls_p36",
    }
    hu = hustle.group_by(*key).agg(pl.col("min").sum().alias("_hmin"), *[pl.col(c).sum() for c in hu_cols])
    hu = hu.select(*key, *[(36.0 * _ratio(pl.col(c), pl.col("_hmin"))).alias(a) for c, a in hu_cols.items()])
    ru = (
        rollup.filter(pl.col("position").is_in(["G", "F", "C"]))
        .group_by(pl.col("def_player_id").alias("player_id"), "season", "position")
        .agg(pl.col("percent_of_time").mean())
        .pivot(on="position", index=key, values="percent_of_time")
    )
    for p in ("G", "F", "C"):
        if p not in ru.columns:
            ru = ru.with_columns(pl.lit(None, dtype=pl.Float64).alias(p))
    ru = ru.select(*key, *[pl.col(p).fill_null(0.0).alias(f"match_{p.lower()}_share") for p in ("G", "F", "C")])
    syn = synergy_mix(synergy, by="player_id", grouping="Defensive").drop("syn_poss")
    out = td.join(hu, on=key, how="inner").join(ru, on=key, how="inner").join(syn, on=key, how="left")
    out = out.with_columns(pl.col(f"dpt_{t}").fill_null(0.0) for t in _DEF_TYPES)
    return _qualify(out).select(*key, "player_name", "team_id", "min", *NBA_PLAY_STYLE_FEATURES[("defense", "player")])


def defense_team_features(
    team_opponent: pl.DataFrame, team_defense: pl.DataFrame, team_hustle: pl.DataFrame
) -> pl.DataFrame:
    """Defensive play-style features per team-season: the opponent shot profile and hustle.

    Args:
        team_opponent: ``leaguedash`` ``team_stats_opponent`` rows.
        team_defense: ``leaguedash`` ``team_stats_defense`` rows.
        team_hustle: ``load_nba_stats_hustle_team`` regular-season totals rows.

    Returns:
        One row per ``(team_id, season)``: ``team_id`` (Int64), ``team_name``, ``season`` and the
        defense-team features of :data:`NBA_PLAY_STYLE_FEATURES`.
    """
    key = ["team_id", "season"]
    opp_poss = pl.col("opp_fga") + 0.44 * pl.col("opp_fta") + pl.col("opp_tov") - pl.col("opp_oreb")
    op = _regular(team_opponent).select(
        *key,
        "team_name",
        "opp_fg3_a",
        "opp_oreb",
        opp_poss.alias("_opp_poss"),
        _ratio(pl.col("opp_fg3_a"), pl.col("opp_fga")).alias("opp_fg3a_rate"),
        _ratio(pl.col("opp_fta"), pl.col("opp_fga")).alias("opp_fta_rate"),
        _ratio(pl.col("opp_tov"), pl.col("opp_fga") + 0.44 * pl.col("opp_fta") + pl.col("opp_tov")).alias(
            "opp_tov_rate"
        ),
        pl.col("opp_pts"),
    )
    de = _regular(team_defense).select(*key, "dreb", "opp_pts_paint", "opp_pts_fb")
    hu = team_hustle.select(*key, "deflections", "contested_shots_3_pt")
    out = op.join(de, on=key, how="inner").join(hu, on=key, how="inner")
    out = out.with_columns(
        _ratio(pl.col("opp_oreb"), pl.col("opp_oreb") + pl.col("dreb")).alias("opp_oreb_rate"),
        _ratio(pl.col("opp_pts_paint"), pl.col("opp_pts")).alias("opp_paint_share"),
        _ratio(pl.col("opp_pts_fb"), pl.col("opp_pts")).alias("opp_fb_share"),
        _ratio(pl.col("deflections"), pl.col("_opp_poss")).alias("deflections_per_poss"),
        _ratio(pl.col("contested_shots_3_pt"), pl.col("opp_fg3_a")).alias("contested_3pt_rate"),
    )
    return out.select(*key, "team_name", *NBA_PLAY_STYLE_FEATURES[("defense", "team")])


def _seasons(seasons: int | list[int], side: str, entity: str) -> list[int]:
    if (side, entity) not in NBA_PLAY_STYLE_FIRST_SEASON:
        raise ValueError(f"side must be 'offense' or 'defense' and entity 'player' or 'team', got {(side, entity)}")
    first = NBA_PLAY_STYLE_FIRST_SEASON[(side, entity)]
    wanted = [seasons] if isinstance(seasons, int) else list(seasons)
    return [int(s) for s in wanted if int(s) >= first]


def _load_season(season: int, side: str, entity: str) -> pl.DataFrame:
    """Load one season's releases (``season`` = end year) and build its features."""
    from sportsdataverse.nba import nba_loaders_extra as X

    start = season - 1  # the multi-asset nba_stats loaders take the start year
    rs = {"season_type": "regular-season", "per_mode": "totals"}
    dash = lambda fam: X.load_nba_stats_leaguedash(fam, season)  # noqa: E731
    if (side, entity) == ("defense", "team"):
        hustle = X.load_nba_stats_hustle_team(start, **rs)
        if hustle.is_empty():
            return pl.DataFrame()
        return defense_team_features(dash("team_stats_opponent"), dash("team_stats_defense"), hustle)
    grouping = "offensive" if side == "offense" else "defensive"
    syn = X.load_nba_stats_synergy(start, type_grouping=grouping, **rs)
    if syn.is_empty():
        return pl.DataFrame()
    if (side, entity) == ("offense", "player"):
        fams = ("drives", "possessions", "catchshoot", "pullupshot", "passing")
        trk = {f: dash(f"player_tracking_{f}") for f in fams}
        return offense_player_features(syn, dash("player_stats_scoring"), trk)
    if (side, entity) == ("offense", "team"):
        from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions

        poss = load_nba_stats_possessions(start).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
        return offense_team_features(syn, dash("team_stats_scoring"), poss)
    rollup = X.load_nba_stats_matchups_rollup(start, **rs)
    hustle = X.load_nba_stats_hustle_player(start, **rs)
    if rollup.is_empty() or hustle.is_empty():
        return pl.DataFrame()
    return defense_player_features(syn, rollup, hustle, dash("player_tracking_defense"))


def nba_play_style_features(
    seasons: int | list[int], *, side: str = "offense", entity: str = "player", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The play-style clustering features, one row per qualifying player- or team-season.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            Seasons before :data:`NBA_PLAY_STYLE_FIRST_SEASON` are skipped.
        side: ``"offense"`` or ``"defense"``.
        entity: ``"player"`` or ``"team"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The ids, ``season`` (and ``min`` for players) and the raw (unstandardized) features of
        :data:`NBA_PLAY_STYLE_FEATURES` for ``(side, entity)``. A season no release covers yet
        is skipped.

    Raises:
        ValueError: If ``side`` or ``entity`` is not one of the values above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_play_style import nba_play_style_features
            feats = nba_play_style_features(2025, side="defense", entity="team")
    """
    frames = [f for s in _seasons(seasons, side, entity) if (f := _load_season(s, side, entity)).height]
    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return out.to_pandas() if return_as_pandas else out


@cache
def _artifact(side: str, entity: str) -> dict:
    path = files("sportsdataverse.nba") / "models" / f"nba_play_style_{side}_{entity}.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


_PREFIX = {"offense": "off_", "defense": "def_"}


def _styles(
    seasons: int | list[int], entity: str, ids: dict[str, pl.DataType], names: dict[str, pl.DataType]
) -> pl.DataFrame:
    """Both sides' assignments for ``entity``, full-joined on the ``ids``, then the ``names`` columns."""
    key = list(ids)
    parts, meta = [], []
    for side, prefix in _PREFIX.items():
        feats = nba_play_style_features(seasons, side=side, entity=entity)
        if feats.is_empty():
            parts.append(pl.DataFrame(schema=ids | {f"{prefix}{c}": t for c, t in ASSIGN_SCHEMA.items()}))
            continue
        assigned = assign_play_style(feats, _artifact(side, entity), key)
        parts.append(assigned.rename({c: f"{prefix}{c}" for c in ASSIGN_SCHEMA}))
        meta.append(feats.select(*key, *names))
    out = parts[0].join(parts[1], on=key, how="full", coalesce=True)
    named = pl.concat(meta).unique(subset=key, keep="first") if meta else pl.DataFrame(schema=ids | names)
    out = out.join(named, on=key, how="left")
    return out.select(*key, *names, *[c for c in out.columns if c not in key and c not in names]).sort(key)


def nba_player_play_styles(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Offensive and defensive play-style archetypes per player-season (BB-D20).

    The method is in :mod:`sportsdataverse._common.play_style` and the features in
    :mod:`sportsdataverse.nba.nba_play_style`. A player qualifies on a side with at least a quarter
    of the season's most minutes (:data:`PLAYER_MIN_SHARE`).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            Offense covers 2016+, defense 2018+; an uncovered side is null.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(player_id, season)`` qualifying on either side: ``player_id`` (Int64),
        ``season``, ``player_name``, ``team_id`` (Int64), ``min`` (season minutes) and for
        ``off_`` and ``def_`` the ``archetype``, ``cluster``, ``dist_to_center``,
        ``second_archetype`` and ``margin``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_play_style import nba_player_play_styles
            styles = nba_player_play_styles(2025)
            styles.group_by("off_archetype").len()
    """
    ids = {"player_id": pl.Int64(), "season": pl.Int64()}
    out = _styles(seasons, "player", ids, {"player_name": pl.Utf8(), "team_id": pl.Int64(), "min": pl.Float64()})
    return out.to_pandas() if return_as_pandas else out


def nba_team_play_styles(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Offensive and defensive play-style archetypes per team-season (BB-D20).

    The method is in :mod:`sportsdataverse._common.play_style` and the features in
    :mod:`sportsdataverse.nba.nba_play_style`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            2016+.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(team_id, season)``: ``team_id`` (Int64), ``season``, ``team_name`` and for
        ``off_`` and ``def_`` the ``archetype``, ``cluster``, ``dist_to_center``,
        ``second_archetype`` and ``margin``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_play_style import nba_team_play_styles
            teams = nba_team_play_styles(2025)
    """
    out = _styles(seasons, "team", {"team_id": pl.Int64(), "season": pl.Int64()}, {"team_name": pl.Utf8()})
    return out.to_pandas() if return_as_pandas else out
