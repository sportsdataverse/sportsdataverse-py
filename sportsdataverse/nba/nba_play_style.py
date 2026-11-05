"""NBA offensive and defensive play-style archetypes for players and teams (BB-D20).

Three clusterings, each fit once on training seasons and applied as-of by
:mod:`sportsdataverse._common.play_style` (season-relative z-scores, frozen KMeans centers):

* **Offense, player** (2015-16+): the Synergy play-type mix (share of a player's play-type
  possessions in isolation, pick-and-roll ball handler, roll man, post-up, spot-up, handoff,
  off-screen, cut, transition and putbacks), his shot profile (three-point share of attempts,
  unassisted share of makes, paint and mid-range share of points) and tracking (drives, touches,
  seconds and dribbles per touch, catch-and-shoot and pull-up share of attempts, post and paint
  touches, potential assists).
* **Defense, player** (2017-18+): matchups (share of matchup time on guards, forwards and centers),
  hustle (deflections, contested twos and threes, charges drawn, defensive box-outs and loose
  balls), tracking defense (rim attempts defended, blocks, steals, defensive rebounds) and the
  Synergy play types he defended.
* **Defense, team** (2015-16+): the opponent shot profile (three-point and free-throw rate, paint
  and fast-break share of points, turnover and offensive rebound rate) and hustle (deflections per
  opponent possession, contested share of opponent threes); the fitter also weighed adding the
  opponents' assisted and second-chance shares, charges and the Synergy mix allowed
  (:data:`NBA_TEAM_FEATURE_CANDIDATES`) and kept the short set on the training seasons.

There is no team offense archetype. Clustering a team's Synergy mix, shot profile and possession
start types (the mean length of a possession after a defensive rebound and after a steal) did not
beat a no-structure null on the holdout seasons: with ``k`` chosen on the training seasons, the
frozen-vs-refit ARI was 0.13 against 0.12 for the same pipeline on features shuffled within
season. With 30 teams a season, team offenses form a continuum. The team defense model is the
weakest of the three that ship (see the artifacts' ``stability``); read a team chip with its
``second_archetype`` and ``margin``.

Everything is read from the ``nba_stats_synergy``, ``nba_stats_hustle``, ``nba_stats_matchups``,
and ``nba_stats_leaguedash`` releases, regular season only. Every
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
    "NBA_TEAM_FEATURE_CANDIDATES",
    "nba_play_style_features",
    "nba_player_play_styles",
    "nba_team_play_styles",
]

#: The first season (end year) each clustering covers: Synergy and hustle start in 2015-16,
#: the matchup roll-up in 2017-18.
NBA_PLAY_STYLE_FIRST_SEASON: Final[dict[tuple[str, str], int]] = {
    ("offense", "player"): 2016,
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

_DEFENSE_TEAM_SHORT: Final[tuple[str, ...]] = (
    "opp_fg3a_rate",
    "opp_fta_rate",
    "opp_tov_rate",
    "opp_paint_share",
    "opp_fb_share",
    "opp_oreb_rate",
    "deflections_per_poss",
    "contested_3pt_rate",
)

#: The clustering features of each (side, entity), in artifact order. The team defense set is the
#: ``short`` candidate the training-only sweep chose over ``long``. Disclosure: that short set was
#: first drawn up after a look at holdout ARI (in this item's first fit, before selection moved to
#: the training seasons), so the team defense holdout numbers have been seen once and lean optimistic.
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
    ("defense", "team"): _DEFENSE_TEAM_SHORT,
}


_PHRASES: Final[dict[str, dict[str, str]]] = {
    "offense": {
        "pt_isolation": "isolation scorer",
        "pt_prballhandler": "pick-and-roll handler",
        "pt_prrollman": "roll man",
        "pt_postup": "post-up scorer",
        "pt_spotup": "spot-up shooter",
        "pt_handoff": "off-ball shooter",
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


_TEAM_DEFENSE_PHRASES: Final[dict[str, str]] = {
    "opp_ast_share": "forces passing",
    "opp_2nd_share": "allows second chances",
    "charges_per_poss": "takes charges",
    "dpt_isolation": "faces isolations",
    "dpt_prballhandler": "faces pick-and-roll",
    "dpt_prrollman": "faces roll men",
    "dpt_postup": "faces post-ups",
    "dpt_spotup": "faces spot-ups",
    "dpt_handoff": "faces handoffs",
    "dpt_offscreen": "faces off-screen action",
}

#: The team feature sets the fitter chooses between on training seasons only
#: (:func:`sportsdataverse._common.play_style.selection_score`): ``long`` is every team feature the
#: builders emit, ``short`` the style core. :data:`NBA_PLAY_STYLE_FEATURES` holds the chosen one.
NBA_TEAM_FEATURE_CANDIDATES: Final[dict[tuple[str, str], dict[str, tuple[str, ...]]]] = {
    ("defense", "team"): {
        "short": _DEFENSE_TEAM_SHORT,
        "long": (
            *_DEFENSE_TEAM_SHORT,
            "opp_ast_share",
            "opp_2nd_share",
            "charges_per_poss",
            *(f"dpt_{t}" for t in _DEF_TYPES),
        ),
    },
}

#: The label phrase of each feature, by (side, entity): an archetype is named after the feature its
#: center is highest on (:func:`sportsdataverse._common.play_style.label_centers`).
NBA_PLAY_STYLE_PHRASES: Final[dict[tuple[str, str], dict[str, str]]] = {
    ("offense", "player"): _PHRASES["offense"],
    ("defense", "player"): _PHRASES["defense"],
    ("defense", "team"): _PHRASES["defense"] | _TEAM_DEFENSE_PHRASES,
}


def _ratio(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    """``num / den`` as Float64, null where ``den`` is not positive."""
    return pl.when(den > 0).then(num.cast(pl.Float64) / den.cast(pl.Float64))


def _p36(col: str, minutes: str = "min") -> pl.Expr:
    return (36.0 * _ratio(pl.col(col), pl.col(minutes))).alias(f"{col}_p36")


def _regular(df: pl.DataFrame) -> pl.DataFrame:
    """Regular-season rows of a ``leaguedash`` family (``"Regular Season"``), keys as Int64."""
    return _keys(df.filter(pl.col("season_type") == "Regular Season") if df.height else df)


_KEYS = ("player_id", "team_id", "season")


def _keys(df: pl.DataFrame) -> pl.DataFrame:
    """Cast the id and season keys to Int64: the releases carry ``season`` as Int32 or Int64."""
    return df.with_columns(pl.col(c).cast(pl.Int64) for c in _KEYS if c in df.columns)


def _join(left: pl.DataFrame, right: pl.DataFrame, key: list[str], how: str = "left") -> pl.DataFrame:
    """``left.join(right)`` after asserting the key dtypes agree."""
    for k in key:
        if left.schema[k] != right.schema[k]:
            raise TypeError(f"join key {k}: {left.schema[k]} vs {right.schema[k]}")
    return left.join(right, on=key, how=how)


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
    rows = (
        _keys(synergy)
        .filter(pl.col("type_grouping") == grouping)
        .with_columns(pl.col("play_type").str.to_lowercase(), pl.col("poss").cast(pl.Float64))
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
    return _join(total, wide, [by, "season"]).select(
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
        out = _join(out, part, key)
    out = out.with_columns(
        _ratio(pl.col("catch_shoot_fga"), pl.col("fga")).alias("catch_shoot_share"),
        _ratio(pl.col("pull_up_fga"), pl.col("fga")).alias("pull_up_share"),
    ).filter(pl.col("syn_poss") > 0)
    return _qualify(out).select(*key, "player_name", "team_id", "min", *NBA_PLAY_STYLE_FEATURES[("offense", "player")])


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
        :data:`NBA_PLAY_STYLE_FEATURES`. Players without matchup, hustle or defensive Synergy rows
        drop out.
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
    hu = _keys(hustle).group_by(*key).agg(pl.col("min").sum().alias("_hmin"), *[pl.col(c).sum() for c in hu_cols])
    hu = hu.select(*key, *[(36.0 * _ratio(pl.col(c), pl.col("_hmin"))).alias(a) for c, a in hu_cols.items()])
    ru = (
        _keys(rollup.rename({"def_player_id": "player_id"}))
        .filter(pl.col("position").is_in(["G", "F", "C"]))
        .group_by("player_id", "season", "position")
        .agg(pl.col("percent_of_time").mean())
        .pivot(on="position", index=key, values="percent_of_time")
    )
    for p in ("G", "F", "C"):
        if p not in ru.columns:
            ru = ru.with_columns(pl.lit(None, dtype=pl.Float64).alias(p))
    ru = ru.select(*key, *[pl.col(p).fill_null(0.0).alias(f"match_{p.lower()}_share") for p in ("G", "F", "C")])
    syn = synergy_mix(synergy, by="player_id", grouping="Defensive")
    out = _join(_join(_join(td, hu, key, "inner"), ru, key, "inner"), syn, key)
    # a qualifier with no defensive Synergy rows has no play-type mix: drop him, as offense does
    return (
        _qualify(out)
        .filter(pl.col("syn_poss") > 0)
        .select(*key, "player_name", "team_id", "min", *NBA_PLAY_STYLE_FEATURES[("defense", "player")])
    )


def defense_team_features(
    synergy: pl.DataFrame, team_opponent: pl.DataFrame, team_defense: pl.DataFrame, team_hustle: pl.DataFrame
) -> pl.DataFrame:
    """Defensive play-style features per team-season: the opponent shot profile, hustle and the Synergy mix allowed.

    Args:
        synergy: Defensive Synergy totals rows (regular season, player level).
        team_opponent: ``leaguedash`` ``team_stats_opponent`` rows.
        team_defense: ``leaguedash`` ``team_stats_defense`` rows.
        team_hustle: ``load_nba_stats_hustle_team`` regular-season totals rows.

    Returns:
        One row per ``(team_id, season)``: ``team_id`` (Int64), ``team_name``, ``season`` and
        every candidate defense-team feature (:data:`NBA_TEAM_FEATURE_CANDIDATES` ``long``).
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
        _ratio(pl.col("opp_ast"), pl.col("opp_fgm")).alias("opp_ast_share"),
        _ratio(pl.col("opp_tov"), pl.col("opp_fga") + 0.44 * pl.col("opp_fta") + pl.col("opp_tov")).alias(
            "opp_tov_rate"
        ),
        pl.col("opp_pts"),
    )
    de = _regular(team_defense).select(*key, "dreb", "opp_pts_paint", "opp_pts_fb", "opp_pts_2_nd_chance")
    hu = _keys(team_hustle).select(*key, "deflections", "contested_shots_3_pt", "charges_drawn")
    syn = synergy_mix(synergy, by="team_id", grouping="Defensive").drop("syn_poss")
    out = _join(_join(_join(op, de, key, "inner"), hu, key, "inner"), syn, key)
    out = out.with_columns(
        _ratio(pl.col("opp_oreb"), pl.col("opp_oreb") + pl.col("dreb")).alias("opp_oreb_rate"),
        _ratio(pl.col("opp_pts_paint"), pl.col("opp_pts")).alias("opp_paint_share"),
        _ratio(pl.col("opp_pts_fb"), pl.col("opp_pts")).alias("opp_fb_share"),
        _ratio(pl.col("opp_pts_2_nd_chance"), pl.col("opp_pts")).alias("opp_2nd_share"),
        _ratio(pl.col("deflections"), pl.col("_opp_poss")).alias("deflections_per_poss"),
        _ratio(pl.col("contested_shots_3_pt"), pl.col("opp_fg3_a")).alias("contested_3pt_rate"),
        _ratio(pl.col("charges_drawn"), pl.col("_opp_poss")).alias("charges_per_poss"),
        *[pl.col(f"dpt_{t}").fill_null(0.0) for t in _DEF_TYPES],
    )
    return out.select(*key, "team_name", *NBA_TEAM_FEATURE_CANDIDATES[("defense", "team")]["long"])


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
    grouping = "offensive" if side == "offense" else "defensive"
    syn = X.load_nba_stats_synergy(start, type_grouping=grouping, **rs)
    if syn.is_empty():
        return pl.DataFrame()
    if (side, entity) == ("defense", "team"):
        hustle = X.load_nba_stats_hustle_team(start, **rs)
        if hustle.is_empty():
            return pl.DataFrame()
        return defense_team_features(syn, dash("team_stats_opponent"), dash("team_stats_defense"), hustle)
    if (side, entity) == ("offense", "player"):
        fams = ("drives", "possessions", "catchshoot", "pullupshot", "passing")
        trk = {f: dash(f"player_tracking_{f}") for f in fams}
        return offense_player_features(syn, dash("player_stats_scoring"), trk)
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
        :data:`NBA_PLAY_STYLE_FEATURES` for ``(side, entity)`` (every candidate feature for the team
        defense). A season no release covers yet is skipped; with none left the frame has those
        columns and zero rows. The returns table below is the default ``side="offense",
        entity="player"`` frame.

    Raises:
        ValueError: If ``side`` or ``entity`` is not one of the values above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_play_style import nba_play_style_features
            feats = nba_play_style_features(2025, side="defense", entity="team")
    """
    frames = [f for s in _seasons(seasons, side, entity) if (f := _load_season(s, side, entity)).height]
    out = pl.concat(frames, how="diagonal_relaxed") if frames else _empty_features(side, entity)
    return out.to_pandas() if return_as_pandas else out


def _empty_features(side: str, entity: str) -> pl.DataFrame:
    """The zero-row frame of :func:`nba_play_style_features` for ``(side, entity)``."""
    ids: dict[str, pl.DataType] = (
        {
            "player_id": pl.Int64(),
            "season": pl.Int64(),
            "player_name": pl.Utf8(),
            "team_id": pl.Int64(),
            "min": pl.Float64(),
        }
        if entity == "player"
        else {"team_id": pl.Int64(), "season": pl.Int64(), "team_name": pl.Utf8()}
    )
    cols = NBA_TEAM_FEATURE_CANDIDATES.get((side, entity), {}).get("long", NBA_PLAY_STYLE_FEATURES[(side, entity)])
    return pl.DataFrame(schema=ids | dict.fromkeys(cols, pl.Float64()))


@cache
def _artifact(side: str, entity: str) -> dict:
    path = files("sportsdataverse.nba") / "models" / f"nba_play_style_{side}_{entity}.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


_PREFIX = {"offense": "off_", "defense": "def_"}


def _styles(
    seasons: int | list[int], entity: str, ids: dict[str, pl.DataType], names: dict[str, pl.DataType]
) -> pl.DataFrame:
    """Each modelled side's assignments for ``entity``, full-joined on the ``ids``, then the ``names`` columns."""
    key = list(ids)
    parts, meta = [], []
    for side, prefix in _PREFIX.items():
        if (side, entity) not in NBA_PLAY_STYLE_FIRST_SEASON:
            continue
        feats = nba_play_style_features(seasons, side=side, entity=entity)
        if feats.is_empty():
            parts.append(pl.DataFrame(schema=ids | {f"{prefix}{c}": t for c, t in ASSIGN_SCHEMA.items()}))
            continue
        assigned = assign_play_style(feats, _artifact(side, entity), key)
        parts.append(assigned.rename({c: f"{prefix}{c}" for c in ASSIGN_SCHEMA}))
        meta.append(feats.select(*key, *names))
    out = parts[0]
    for part in parts[1:]:
        out = out.join(part, on=key, how="full", coalesce=True)
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
    """Defensive play-style archetypes per team-season (BB-D20); team offense has none (see the module).

    The method is in :mod:`sportsdataverse._common.play_style` and the features in
    :mod:`sportsdataverse.nba.nba_play_style`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            2016+.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(team_id, season)``: ``team_id`` (Int64), ``season``, ``team_name`` and the
        ``def_archetype``, ``def_cluster``, ``def_dist_to_center``, ``def_second_archetype`` and
        ``def_margin``.

    Raises:
        ValueError: If a feature is null for a whole season (a release stopped carrying it).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_play_style import nba_team_play_styles
            teams = nba_team_play_styles(2025)
    """
    out = _styles(seasons, "team", {"team_id": pl.Int64(), "season": pl.Int64()}, {"team_name": pl.Utf8()})
    return out.to_pandas() if return_as_pandas else out
