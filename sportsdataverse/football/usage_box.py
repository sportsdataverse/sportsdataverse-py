"""Usage and situational box: player, position-group, tackle, team and drive-script splits.

League-agnostic (CFB and NFL processors emit the same play frame). One call,
:func:`create_usage_box`, turns a game's processed plays (plus the wide
per-play participants frame when available) into six sections that both
``advBoxScore`` producers attach and both ``espn_{league}_adv_*`` dataset
families release; season leaderboards are the same rows summed over games
(:func:`aggregate_usage_box`).

Sections (each a list of dicts, one row per key):

``player_usage`` -- one row per (pos_team, player) for every rusher /
receiver: rushes, targets, receptions, touches (rushes + receptions),
opportunities (rushes + targets), yards, first downs, touchdowns,
``fd_or_td`` plays and the **first down + touchdown rate** (per opportunity),
explosive plays and rate, **target share** and **first-down share** of the
team, red-zone (``rz_play``, inside the 20) and scoring-opportunity
(``scoring_opp``, inside the 40) touches / targets / touchdowns, and
third-down conversions vs expected (the league's distance curve).

``position_group_usage`` -- the same usage columns summed per
(pos_team, position group) when the participants carry positions.

``tackles`` -- one row per (def_pos_team, tackler): tackles, assists,
**tackle share** = (tackles + 0.5 assists) / team total, position group.
Needs the participants frame (``tackler_player_ids`` /
``assisted_by_player_ids``); empty without it.

``position_group_tackles`` -- tackles summed per (def_pos_team, group).

``team_usage`` -- one row per pos_team: targets, rushes, first downs,
**third downs converted over expected**, and the **red-zone** and
**scoring-opportunity efficiencies** (trips, plays, TD rate, points per
trip, success rate, EPA per play, conversion rate).

``drive_scripting`` -- one row per (pos_team, script) where ``scripted`` is
the team's first two drives of each half and ``non_scripted`` the rest:
drives, plays, EPA per play, success rate, yards per play, points per drive,
TD rate, scoring-opportunity rate.

``st_kickers`` -- one row per (kicking team, kicker): kickoffs (yards,
touchbacks, onside, out of bounds), the coverage allowed on them (returns,
return yards, return TDs, EPA from the kicking side), field goals by range
(0-39, 40-49, 50+), long, blocked, and extra points.

``st_punters`` -- one row per (punting team, punter): punts, gross and net
average (net = gross - return yards - 20 per touchback), long, inside the 20,
touchbacks, fair catches, downed, out of bounds, blocked, returns allowed.

``st_returners`` -- one row per (return team, returner): kick and punt
returns, yards, average, long, touchdowns, EPA.

``st_blocks`` -- one row per (defending team, player): punts and field goals
blocked.

``st_team`` -- one row per team: its own kicking, punting and returns plus
the coverage against it (returns, yards and TDs allowed on kickoffs and
punts) and blocks made (``*_blocks_by``) and suffered.

Conventions: only ``scrimmage_play`` rows that were not nullified by a
penalty count for the usage sections (special teams rows that stood count
for the ``st_*`` sections); drive points come from ``drive.result`` (TD 7, FG 3) so a
field goal counts even though its row is a special-teams play; "converted"
on third down is a first down or a touchdown.

Example:
    From a processed game::

        from sportsdataverse.nfl import NFLPlayProcess
        from sportsdataverse.football.usage_box import create_usage_box
        proc = NFLPlayProcess(gameId=401772510)
        proc.espn_nfl_pbp()
        out = proc.run_processing_pipeline()
        box = create_usage_box(proc.plays_frame(), proc.participants, league="nfl")
        box["team_usage"][0]["third_down_over_expected"]
"""

from __future__ import annotations

import ast
import json
from importlib.resources import files
from typing import Any, Optional

import numpy as np
import polars as pl

from sportsdataverse.football.positions import position_group

__all__ = [
    "SECTIONS",
    "aggregate_usage_box",
    "create_usage_box",
    "fit_third_down_curve",
    "load_third_down_curve",
]

#: section names, in output order
SECTIONS = (
    "player_usage",
    "position_group_usage",
    "tackles",
    "position_group_tackles",
    "team_usage",
    "drive_scripting",
    "st_kickers",
    "st_punters",
    "st_returners",
    "st_blocks",
    "st_team",
)

#: yards-to-go grid of the bundled third-down curves (the last row is 25+)
_TD_MAX_DISTANCE = 25
_SCRIPTED_DRIVES_PER_HALF = 2

_DRIVE_PTS = (
    pl.when(pl.col("drive.result") == "TD").then(7.0).when(pl.col("drive.result") == "FG").then(3.0).otherwise(0.0)
)


# --------------------------------------------------------------------------
# third-down expected conversion
# --------------------------------------------------------------------------
def fit_third_down_curve(plays: pl.DataFrame) -> pl.DataFrame:
    """Fit P(convert | yards to go) on third down from released plays.

    Conversion is a first down or a touchdown on a third-down scrimmage play
    that stood (no nullifying penalty). Rates by distance are smoothed with a
    count-weighted non-increasing isotonic regression and reported on the
    1..25 grid (25 = 25 or more).

    Args:
        plays: plays in the released ``espn_{league}_pbp`` shape (any
            number of seasons). Needs ``down`` / ``distance`` (or
            ``start.down`` / ``start.distance``), ``scrimmage_play``,
            ``first_down_created``, ``touchdown``.

    Returns:
        ``distance: Int64 (1..25), rate: Float64, n: Int64``.

    Example:
        Refresh the NFL curve from local season parquet::

            from sportsdataverse.football.usage_box import fit_third_down_curve
            curve = fit_third_down_curve(pl.concat(frames, how="diagonal_relaxed"))
            curve.write_parquet("nfl_third_down_conversion.parquet")
    """
    from scipy.optimize import isotonic_regression

    schema = {"distance": pl.Int64(), "rate": pl.Float64(), "n": pl.Int64()}
    if plays.height == 0:
        return pl.DataFrame(schema=schema)
    third = _third_downs(_standing_scrimmage(plays))
    if third.height == 0:
        return pl.DataFrame(schema=schema)
    g = (
        third.group_by("td_distance")
        .agg(rate=pl.col("converted").cast(pl.Float64).mean(), n=pl.len())
        .sort("td_distance")
    )
    x = g["td_distance"].to_numpy()
    iso = isotonic_regression(g["rate"].to_numpy(), weights=g["n"].to_numpy(), increasing=False).x
    grid = np.arange(1, _TD_MAX_DISTANCE + 1)
    rate = np.interp(grid, x, iso)
    n = np.interp(grid, x, g["n"].to_numpy()).round().astype("int64")
    return pl.DataFrame({"distance": grid, "rate": rate, "n": n}, schema=schema)


def load_third_down_curve(league: str) -> pl.DataFrame:
    """Load the bundled third-down conversion curve for ``league`` (``"cfb"`` / ``"nfl"``).

    Args:
        league: ``"cfb"`` or ``"nfl"``.

    Returns:
        ``distance: Int64 (1..25), rate: Float64, n: Int64``.

    Example:
        Quick start::

            from sportsdataverse.football.usage_box import load_third_down_curve
            load_third_down_curve("nfl").filter(pl.col("distance") == 3)
    """
    path = files(f"sportsdataverse.{league}") / "models" / f"{league}_third_down_conversion.parquet"
    with path.open("rb") as f:
        return pl.read_parquet(f)


# --------------------------------------------------------------------------
# frame preparation
# --------------------------------------------------------------------------
def _col(frame: pl.DataFrame, *names: str) -> Optional[str]:
    for n in names:
        if n in frame.columns:
            return n
    return None


def _standing_scrimmage(plays: pl.DataFrame) -> pl.DataFrame:
    """Scrimmage plays that counted (no nullifying penalty), typed for the aggregations."""
    if "scrimmage_play" not in plays.columns:
        return plays.clear()
    df = plays.filter(pl.col("scrimmage_play") == True)  # noqa: E712
    if "penalty_no_play" in df.columns:
        df = df.filter(pl.col("penalty_no_play").fill_null(False) == False)  # noqa: E712
    down = _col(df, "start.down", "down")
    dist = _col(df, "start.distance", "distance")
    exprs: list[pl.Expr] = []
    exprs.append((pl.col(down).cast(pl.Int64, strict=False) if down else pl.lit(None, dtype=pl.Int64)).alias("td_down"))
    exprs.append(
        (pl.col(dist).cast(pl.Float64, strict=False) if dist else pl.lit(None, dtype=pl.Float64))
        .clip(1, _TD_MAX_DISTANCE)
        .round(0)
        .cast(pl.Int64)
        .alias("td_distance")
    )
    for c, dt in (
        ("rush", pl.Boolean),
        ("pass", pl.Boolean),
        ("target", pl.Boolean),
        ("completion", pl.Boolean),
        ("first_down_created", pl.Boolean),
        ("touchdown", pl.Boolean),
        ("EPA_explosive", pl.Boolean),
        ("EPA_success", pl.Boolean),
        ("rz_play", pl.Boolean),
        ("scoring_opp", pl.Boolean),
    ):
        exprs.append(
            (pl.col(c).cast(dt, strict=False).fill_null(False) if c in df.columns else pl.lit(False)).alias(f"u_{c}")
        )
    for c in ("EPA", "statYardage", "yds_rushed", "yds_receiving"):
        exprs.append(
            (pl.col(c).cast(pl.Float64, strict=False) if c in df.columns else pl.lit(None, dtype=pl.Float64)).alias(
                f"u_{c}"
            )
        )
    df = df.with_columns(exprs)
    return df.with_columns(
        converted=(pl.col("u_first_down_created") | pl.col("u_touchdown")),
        fd_or_td=(pl.col("u_first_down_created") | pl.col("u_touchdown")),
    )


def _third_downs(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter((pl.col("td_down") == 3) & pl.col("td_distance").is_not_null())


def _with_expected(df: pl.DataFrame, curve: Optional[pl.DataFrame]) -> pl.DataFrame:
    """Attach ``expected`` (the curve's rate at the play's yards to go) to third downs."""
    if curve is None or curve.height == 0:
        return df.with_columns(expected=pl.lit(None, dtype=pl.Float64))
    c = curve.select(pl.col("distance").cast(pl.Int64).alias("td_distance"), pl.col("rate").alias("expected"))
    return df.join(c, on="td_distance", how="left")


def _team_col(df: pl.DataFrame, side: str) -> str:
    return "pos_team" if side == "off" else "def_pos_team"


# --------------------------------------------------------------------------
# sections
# --------------------------------------------------------------------------
def _player_rows(df: pl.DataFrame, curve: Optional[pl.DataFrame], positions: dict[str, Any]) -> pl.DataFrame:
    """Long (play, player, role) rows for rushers and receivers."""
    parts = []
    if "rusher_player_id" in df.columns or "rusher_player_name" in df.columns:
        r = df.filter(pl.col("u_rush") == True).with_columns(  # noqa: E712
            player_id=pl.col("rusher_player_id").cast(pl.Utf8)
            if "rusher_player_id" in df.columns
            else pl.lit(None, dtype=pl.Utf8),
            player_name=pl.col("rusher_player_name").cast(pl.Utf8)
            if "rusher_player_name" in df.columns
            else pl.lit(None, dtype=pl.Utf8),
            role=pl.lit("rush"),
            yards=pl.coalesce(pl.col("u_yds_rushed"), pl.col("u_statYardage")),
        )
        parts.append(r)
    if "receiver_player_id" in df.columns or "receiver_player_name" in df.columns:
        t = df.filter(pl.col("u_target") == True).with_columns(  # noqa: E712
            player_id=pl.col("receiver_player_id").cast(pl.Utf8)
            if "receiver_player_id" in df.columns
            else pl.lit(None, dtype=pl.Utf8),
            player_name=pl.col("receiver_player_name").cast(pl.Utf8)
            if "receiver_player_name" in df.columns
            else pl.lit(None, dtype=pl.Utf8),
            role=pl.lit("target"),
            yards=pl.when(pl.col("u_completion"))
            .then(pl.coalesce(pl.col("u_yds_receiving"), pl.col("u_statYardage")))
            .otherwise(0.0),
        )
        parts.append(t)
    if not parts:
        return pl.DataFrame()
    long = pl.concat(parts, how="diagonal_relaxed")
    long = long.filter(pl.col("player_id").is_not_null() | pl.col("player_name").is_not_null())
    long = long.with_columns(
        player_key=pl.coalesce(pl.col("player_id"), pl.col("player_name")),
        is_third=(pl.col("td_down") == 3),
    )
    long = _with_expected(long, curve)
    return long


def _usage_agg(long: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    is_rush = pl.col("role") == "rush"
    is_target = pl.col("role") == "target"
    is_rec = is_target & pl.col("u_completion")
    touch = is_rush | is_rec
    third = pl.col("is_third")
    return long.group_by(keys, maintain_order=True).agg(
        rushes=is_rush.sum(),
        targets=is_target.sum(),
        receptions=is_rec.sum(),
        touches=touch.sum(),
        opportunities=pl.len(),
        rush_yards=pl.col("yards").filter(is_rush).sum(),
        receiving_yards=pl.col("yards").filter(is_rec).sum(),
        first_downs=pl.col("u_first_down_created").sum(),
        touchdowns=pl.col("u_touchdown").sum(),
        fd_or_td=pl.col("fd_or_td").sum(),
        explosive_plays=pl.col("u_EPA_explosive").sum(),
        successful_plays=pl.col("u_EPA_success").sum(),
        epa=pl.col("u_EPA").sum(),
        rz_rushes=(is_rush & pl.col("u_rz_play")).sum(),
        rz_targets=(is_target & pl.col("u_rz_play")).sum(),
        rz_touches=(touch & pl.col("u_rz_play")).sum(),
        rz_touchdowns=(pl.col("u_rz_play") & pl.col("u_touchdown")).sum(),
        so_rushes=(is_rush & pl.col("u_scoring_opp")).sum(),
        so_targets=(is_target & pl.col("u_scoring_opp")).sum(),
        so_touches=(touch & pl.col("u_scoring_opp")).sum(),
        so_touchdowns=(pl.col("u_scoring_opp") & pl.col("u_touchdown")).sum(),
        third_down_opportunities=third.sum(),
        third_down_conversions=(third & pl.col("converted")).sum(),
        third_down_expected=pl.col("expected").filter(third).sum(),
    )


def _usage_rates(df: pl.DataFrame, team_totals: Optional[pl.DataFrame]) -> pl.DataFrame:
    def _rate(num: str, den: str) -> pl.Expr:
        return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)

    df = df.with_columns(
        fd_td_rate=_rate("fd_or_td", "opportunities"),
        explosive_rate=_rate("explosive_plays", "opportunities"),
        success_rate=_rate("successful_plays", "opportunities"),
        epa_per_opportunity=_rate("epa", "opportunities"),
        rz_touchdown_rate=_rate("rz_touchdowns", "rz_touches"),
        so_touchdown_rate=_rate("so_touchdowns", "so_touches"),
        third_down_rate=_rate("third_down_conversions", "third_down_opportunities"),
        third_down_over_expected=pl.col("third_down_conversions") - pl.col("third_down_expected"),
    )
    if team_totals is not None:
        df = df.join(team_totals, on="pos_team", how="left").with_columns(
            target_share=_rate("targets", "team_targets"),
            first_down_share=_rate("first_downs", "team_first_downs"),
            touch_share=_rate("touches", "team_touches"),
        )
    return df


def _team_totals(df: pl.DataFrame) -> pl.DataFrame:
    """Team denominators for the shares, from EVERY standing scrimmage play (an
    unattributed target still counts against the team), so shares sum to at most one."""
    return df.group_by("pos_team").agg(
        team_targets=pl.col("u_target").sum(),
        team_first_downs=pl.col("u_first_down_created").sum(),
        team_touches=(pl.col("u_rush") | (pl.col("u_target") & pl.col("u_completion"))).sum(),
    )


def _decode_list_cell(v: Any) -> Optional[list[str]]:
    """A list cell as stored in a final.json: JSON, a Python repr, or already a list."""
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return [str(x) for x in v if x is not None]
    if isinstance(v, str):
        text = v.strip()
        if not text:
            return []
        try:
            out = json.loads(text)
        except ValueError:
            try:
                out = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                return []
        return [str(x) for x in out if x is not None] if isinstance(out, (list, tuple)) else []
    return []


def _tackle_rows(participants: pl.DataFrame, plays: pl.DataFrame, positions: dict[str, Any]) -> pl.DataFrame:
    """Long (play, tackler, kind) rows from the participants' tackler / assist lists."""
    need = {"play_id"}
    if participants is None or participants.height == 0 or not need <= set(participants.columns):
        return pl.DataFrame()
    if "id" not in plays.columns or "def_pos_team" not in plays.columns:
        return pl.DataFrame()
    # list columns arrive as List(Utf8) live, but as stringified cells from a
    # committed final.json (JSON, or a Python repr with single quotes)
    list_cols = [
        c
        for c in ("tackler_player_ids", "assisted_by_player_ids", "tackler_player_names", "assisted_by_player_names")
        if c in participants.columns
    ]
    decoded = participants.with_columns(
        [
            (
                pl.col(c).map_elements(_decode_list_cell, return_dtype=pl.List(pl.Utf8))
                if participants.schema[c] == pl.Utf8
                else pl.col(c).cast(pl.List(pl.Utf8), strict=False)
            ).alias(c)
            for c in list_cols
        ]
    )
    base = plays.select(
        pl.col("id").cast(pl.Int64, strict=False).alias("play_id"),
        pl.col("def_pos_team"),
        pl.col("pos_team"),
    ).join(decoded.with_columns(pl.col("play_id").cast(pl.Int64, strict=False)), on="play_id", how="inner")
    parts = []
    for col, kind in (("tackler_player_ids", "tackle"), ("assisted_by_player_ids", "assist")):
        if col not in base.columns:
            continue
        names_col = col.replace("_ids", "_names")
        rows = base.select(
            "play_id",
            "def_pos_team",
            pl.col(col).alias("ids"),
            (pl.col(names_col) if names_col in base.columns else pl.lit([], dtype=pl.List(pl.Utf8))).alias("names"),
        ).filter(pl.col("ids").list.len() > 0)
        if rows.height == 0:
            continue
        # ids and names are parallel lists (built from the same participant rows)
        same_len = rows.filter(pl.col("ids").list.len() == pl.col("names").list.len())
        rows = (
            same_len.explode(["ids", "names"])
            if same_len.height == rows.height
            else rows.explode("ids").with_columns(names=pl.lit(None, dtype=pl.Utf8))
        )
        rows = rows.with_columns(
            player_id=pl.col("ids").cast(pl.Utf8),
            player_name=pl.col("names").cast(pl.Utf8),
            kind=pl.lit(kind),
        ).drop("ids", "names")
        parts.append(rows)
    if not parts:
        return pl.DataFrame()
    long = pl.concat(parts, how="diagonal_relaxed").filter(pl.col("player_id").is_not_null())
    long = long.with_columns(
        position_group=pl.col("player_id").map_elements(lambda pid: positions.get(pid), return_dtype=pl.Utf8)
    )
    return long


def _tackle_agg(long: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    g = long.group_by(keys, maintain_order=True).agg(
        tackles=(pl.col("kind") == "tackle").sum(),
        assists=(pl.col("kind") == "assist").sum(),
    )
    g = g.with_columns(tackle_points=pl.col("tackles") + 0.5 * pl.col("assists"))
    team = g.group_by("def_pos_team").agg(team_tackle_points=pl.col("tackle_points").sum())
    return g.join(team, on="def_pos_team", how="left").with_columns(
        tackle_share=pl.when(pl.col("team_tackle_points") > 0)
        .then(pl.col("tackle_points") / pl.col("team_tackle_points"))
        .otherwise(None)
    )


def _drive_frame(df: pl.DataFrame) -> pl.DataFrame:
    """One row per (pos_team, drive) with plays, EPA, success, yards, points, script."""
    if "drive.id" not in df.columns or "pos_team" not in df.columns:
        return pl.DataFrame()
    d = df.filter(pl.col("drive.id").is_not_null())
    if d.height == 0:
        return pl.DataFrame()
    half = (
        pl.col("half").cast(pl.Int64, strict=False)
        if "half" in d.columns
        else (pl.when(pl.col("period").cast(pl.Int64, strict=False) <= 2).then(1).otherwise(2))
    )
    order = _col(d, "game_play_number", "sequenceNumber", "id")
    drv = (
        d.with_columns(half=half, _ord=pl.col(order).cast(pl.Int64, strict=False) if order else pl.int_range(pl.len()))
        .group_by(["pos_team", "drive.id"], maintain_order=True)
        .agg(
            half=pl.col("half").min(),
            first_play=pl.col("_ord").min(),
            plays=pl.len(),
            epa=pl.col("u_EPA").sum(),
            successes=pl.col("u_EPA_success").sum(),
            yards=pl.col("u_statYardage").sum(),
            touchdown=pl.col("u_touchdown").any(),
            rz=pl.col("u_rz_play").any(),
            scoring_opp=pl.col("u_scoring_opp").any(),
            drive_pts=_DRIVE_PTS.first() if "drive.result" in d.columns else pl.lit(0.0),
        )
        .sort(["pos_team", "half", "first_play"])
    )
    drv = drv.with_columns(
        drive_index=pl.int_range(pl.len()).over(["pos_team", "half"]),
    ).with_columns(
        script=pl.when(pl.col("drive_index") < _SCRIPTED_DRIVES_PER_HALF)
        .then(pl.lit("scripted"))
        .otherwise(pl.lit("non_scripted"))
    )
    return drv


def _drive_eff(drv: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    g = drv.group_by(keys, maintain_order=True).agg(
        drives=pl.len(),
        plays=pl.col("plays").sum(),
        epa=pl.col("epa").sum(),
        successes=pl.col("successes").sum(),
        yards=pl.col("yards").sum(),
        points=pl.col("drive_pts").sum(),
        touchdowns=pl.col("touchdown").sum(),
        scoring_opps=pl.col("scoring_opp").sum(),
    )

    def _rate(num: str, den: str) -> pl.Expr:
        return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)

    return g.with_columns(
        epa_per_play=_rate("epa", "plays"),
        success_rate=_rate("successes", "plays"),
        yards_per_play=_rate("yards", "plays"),
        points_per_drive=_rate("points", "drives"),
        touchdown_rate=_rate("touchdowns", "drives"),
        scoring_opp_rate=_rate("scoring_opps", "drives"),
    )


def _zone_eff(df: pl.DataFrame, drv: pl.DataFrame, flag: str, prefix: str) -> pl.DataFrame:
    """Per pos_team efficiency inside a zone: trips (drives touching it), plays, TDs, points."""
    drive_flag = {"rz_play": "rz", "scoring_opp": "scoring_opp"}[flag]
    zone_plays = df.filter(pl.col(f"u_{flag}") == True)  # noqa: E712
    per_play = zone_plays.group_by("pos_team").agg(
        **{
            f"{prefix}_plays": pl.len(),
            f"{prefix}_successes": pl.col("u_EPA_success").sum(),
            f"{prefix}_epa": pl.col("u_EPA").sum(),
            f"{prefix}_touchdowns": pl.col("u_touchdown").sum(),
            f"{prefix}_targets": pl.col("u_target").sum(),
            f"{prefix}_rushes": pl.col("u_rush").sum(),
        }
    )
    trips = (
        drv.filter(pl.col(drive_flag) == True)  # noqa: E712
        .group_by("pos_team")
        .agg(**{f"{prefix}_trips": pl.len(), f"{prefix}_points": pl.col("drive_pts").sum()})
        if drv.height
        else pl.DataFrame(schema={"pos_team": per_play.schema.get("pos_team", pl.Int64)})
    )
    out = per_play.join(trips, on="pos_team", how="full", coalesce=True)

    def _rate(num: str, den: str) -> pl.Expr:
        return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)

    for c in (f"{prefix}_trips", f"{prefix}_points"):
        if c not in out.columns:
            out = out.with_columns(pl.lit(0).alias(c))
    return out.with_columns(
        **{
            f"{prefix}_touchdown_rate": _rate(f"{prefix}_touchdowns", f"{prefix}_trips"),
            f"{prefix}_points_per_trip": _rate(f"{prefix}_points", f"{prefix}_trips"),
            f"{prefix}_success_rate": _rate(f"{prefix}_successes", f"{prefix}_plays"),
            f"{prefix}_epa_per_play": _rate(f"{prefix}_epa", f"{prefix}_plays"),
        }
    )


def _team_rows(df: pl.DataFrame, drv: pl.DataFrame, curve: Optional[pl.DataFrame]) -> pl.DataFrame:
    if "pos_team" not in df.columns or df.height == 0:
        return pl.DataFrame()
    third = _with_expected(_third_downs(df), curve)
    base = df.group_by("pos_team").agg(
        plays=pl.len(),
        rushes=pl.col("u_rush").sum(),
        targets=pl.col("u_target").sum(),
        completions=pl.col("u_completion").sum(),
        first_downs=pl.col("u_first_down_created").sum(),
        touchdowns=pl.col("u_touchdown").sum(),
        explosive_plays=pl.col("u_EPA_explosive").sum(),
        successful_plays=pl.col("u_EPA_success").sum(),
        epa=pl.col("u_EPA").sum(),
    )
    t3 = third.group_by("pos_team").agg(
        third_down_opportunities=pl.len(),
        third_down_conversions=pl.col("converted").sum(),
        third_down_expected=pl.col("expected").sum(),
    )
    out = base.join(t3, on="pos_team", how="left")
    out = out.join(_zone_eff(df, drv, "rz_play", "rz"), on="pos_team", how="left")
    out = out.join(_zone_eff(df, drv, "scoring_opp", "so"), on="pos_team", how="left")

    def _rate(num: str, den: str) -> pl.Expr:
        return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)

    return out.with_columns(
        success_rate=_rate("successful_plays", "plays"),
        explosive_rate=_rate("explosive_plays", "plays"),
        epa_per_play=_rate("epa", "plays"),
        third_down_rate=_rate("third_down_conversions", "third_down_opportunities"),
        third_down_over_expected=pl.col("third_down_conversions") - pl.col("third_down_expected"),
    )


# --------------------------------------------------------------------------
# special teams
# --------------------------------------------------------------------------
_ST_FLAGS = (
    "kickoff_play",
    "kickoff_tb",
    "kickoff_onside",
    "kickoff_oob",
    "kickoff_fair_catch",
    "punt",
    "punt_tb",
    "punt_fair_catch",
    "punt_downed",
    "punt_oob",
    "punt_blocked",
    "fg_attempt",
    "fg_made",
    "xp_attempt",
    "xp_made",
    "touchdown",
    "penalty_no_play",
)
_ST_NUMS = (
    "yds_kickoff",
    "yds_kickoff_return",
    "yds_punted",
    "yds_punt_return",
    "yds_fg",
    "EPA",
    "start.yardsToEndzone",
)
_ST_IDS = (
    "kickoff_player",
    "kickoff_return_player",
    "punter_player",
    "punt_return_player",
    "fg_kicker_player",
    "xp_kicker_player",
    "fg_block_player",
    "punt_block_player",
)
_ST_TEAMS = (
    "pos_team",
    "def_pos_team",
    "kicking_team",
    "return_team",
    "kick_return_team",
    "punt_return_team",
    "punt_team",
    "fg_team",
)


def _st_frame(plays: pl.DataFrame) -> pl.DataFrame:
    """Special-teams rows typed for the aggregations (kickoffs, punts, FGs, XPs that stood)."""
    if plays.height == 0:
        return plays.clear()
    exprs: list[pl.Expr] = []
    for c in _ST_FLAGS:
        exprs.append(
            (pl.col(c).cast(pl.Boolean, strict=False).fill_null(False) if c in plays.columns else pl.lit(False)).alias(
                f"s_{c}"
            )
        )
    for c in _ST_NUMS:
        exprs.append(
            (pl.col(c).cast(pl.Float64, strict=False) if c in plays.columns else pl.lit(None, dtype=pl.Float64)).alias(
                f"s_{c}"
            )
        )
    for c in _ST_IDS:
        for suffix in ("_id", "_name"):
            col = f"{c}{suffix}"
            exprs.append(
                (
                    pl.col(col).cast(pl.Utf8, strict=False) if col in plays.columns else pl.lit(None, dtype=pl.Utf8)
                ).alias(f"s_{col}")
            )
    for c in _ST_TEAMS:
        exprs.append(
            (pl.col(c).cast(pl.Int64, strict=False) if c in plays.columns else pl.lit(None, dtype=pl.Int64)).alias(
                f"s_{c}"
            )
        )
    df = plays.with_columns(exprs).filter(pl.col("s_penalty_no_play") == False)  # noqa: E712
    return df.filter(
        pl.col("s_kickoff_play") | pl.col("s_punt") | pl.col("s_fg_attempt") | pl.col("s_xp_attempt")
    ).with_columns(
        # kickoffs: pos_team is the RECEIVING team; the kicker's EPA is the negation
        kick_team=pl.coalesce(pl.col("s_kicking_team"), pl.col("s_def_pos_team")),
        kick_ret_team=pl.coalesce(pl.col("s_kick_return_team"), pl.col("s_return_team"), pl.col("s_pos_team")),
        punt_ret_team=pl.coalesce(pl.col("s_punt_return_team"), pl.col("s_def_pos_team")),
        fg_kick_team=pl.coalesce(pl.col("s_fg_team"), pl.col("s_pos_team")),
        kick_epa=-pl.col("s_EPA"),
        punt_landing=(pl.col("s_start.yardsToEndzone") - pl.col("s_yds_punted")),
        kick_returned=(
            pl.col("s_kickoff_play")
            & ~pl.col("s_kickoff_tb")
            & ~pl.col("s_kickoff_oob")
            & ~pl.col("s_kickoff_onside")
            & ~pl.col("s_kickoff_fair_catch")
            & pl.col("s_kickoff_return_player_name").is_not_null()
        ),
        punt_returned=(
            pl.col("s_punt")
            & ~pl.col("s_punt_tb")
            & ~pl.col("s_punt_fair_catch")
            & ~pl.col("s_punt_downed")
            & ~pl.col("s_punt_oob")
            & ~pl.col("s_punt_blocked")
            & pl.col("s_punt_return_player_name").is_not_null()
        ),
    )


def _st_rate(num: str, den: str) -> pl.Expr:
    return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)


def _merge_players(parts: list[pl.DataFrame], team_col: str) -> pl.DataFrame:
    """Full-join per-player frames on (team, player_key), coalescing id and name."""
    parts = [f for f in parts if f is not None and f.height]
    if not parts:
        return pl.DataFrame()
    out = parts[0]
    for f in parts[1:]:
        out = (
            out.join(f, on=[team_col, "player_key"], how="full", coalesce=True, suffix="_r")
            .with_columns(
                player_id=pl.coalesce(pl.col("player_id"), pl.col("player_id_r")),
                player_name=pl.coalesce(pl.col("player_name"), pl.col("player_name_r")),
            )
            .drop("player_id_r", "player_name_r")
        )
    return out


def _fill_counts(df: pl.DataFrame, skip: tuple[str, ...]) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col(c).fill_null(0)
            for c in df.columns
            if c not in skip and not c.endswith("_long") and df.schema[c].is_numeric()
        ]
    )


def _st_kicker_rows(st: pl.DataFrame) -> pl.DataFrame:
    """Per (kicking team, kicker): kickoffs + coverage allowed, field goals by range, extra points."""
    ko = st.filter(pl.col("s_kickoff_play")).with_columns(
        pos_team=pl.col("kick_team"),
        player_key=pl.coalesce(pl.col("s_kickoff_player_id"), pl.col("s_kickoff_player_name")),
    )
    ko_agg = (
        ko.filter(pl.col("player_key").is_not_null())
        .group_by(["pos_team", "player_key"])
        .agg(
            player_id=pl.col("s_kickoff_player_id").drop_nulls().first(),
            player_name=pl.col("s_kickoff_player_name").drop_nulls().first(),
            kickoffs=pl.len(),
            kickoff_yards=pl.col("s_yds_kickoff").sum(),
            kickoff_touchbacks=pl.col("s_kickoff_tb").sum(),
            kickoff_onside=pl.col("s_kickoff_onside").sum(),
            kickoff_out_of_bounds=pl.col("s_kickoff_oob").sum(),
            kickoff_returns_allowed=pl.col("kick_returned").sum(),
            kickoff_return_yards_allowed=pl.col("s_yds_kickoff_return").filter(pl.col("kick_returned")).sum(),
            kickoff_return_tds_allowed=(pl.col("kick_returned") & pl.col("s_touchdown")).sum(),
            kickoff_epa=pl.col("kick_epa").sum(),
        )
    )
    fg = st.filter(pl.col("s_fg_attempt")).with_columns(
        pos_team=pl.col("fg_kick_team"),
        player_key=pl.coalesce(pl.col("s_fg_kicker_player_id"), pl.col("s_fg_kicker_player_name")),
        blocked=pl.col("s_fg_block_player_name").is_not_null(),
    )
    d = pl.col("s_yds_fg")
    fg_agg = (
        fg.filter(pl.col("player_key").is_not_null())
        .group_by(["pos_team", "player_key"])
        .agg(
            player_id=pl.col("s_fg_kicker_player_id").drop_nulls().first(),
            player_name=pl.col("s_fg_kicker_player_name").drop_nulls().first(),
            fg_attempts=pl.len(),
            fg_made=pl.col("s_fg_made").sum(),
            fg_long=d.filter(pl.col("s_fg_made")).max(),
            fg_blocked=pl.col("blocked").sum(),
            fg_0_39_attempts=(d < 40).sum(),
            fg_0_39_made=((d < 40) & pl.col("s_fg_made")).sum(),
            fg_40_49_attempts=((d >= 40) & (d < 50)).sum(),
            fg_40_49_made=((d >= 40) & (d < 50) & pl.col("s_fg_made")).sum(),
            fg_50_plus_attempts=(d >= 50).sum(),
            fg_50_plus_made=((d >= 50) & pl.col("s_fg_made")).sum(),
            fg_epa=pl.col("s_EPA").sum(),
        )
    )
    xp = st.filter(pl.col("s_xp_attempt")).with_columns(
        pos_team=pl.col("s_pos_team"),
        player_key=pl.coalesce(pl.col("s_xp_kicker_player_id"), pl.col("s_xp_kicker_player_name")),
    )
    xp_agg = (
        xp.filter(pl.col("player_key").is_not_null())
        .group_by(["pos_team", "player_key"])
        .agg(
            player_id=pl.col("s_xp_kicker_player_id").drop_nulls().first(),
            player_name=pl.col("s_xp_kicker_player_name").drop_nulls().first(),
            xp_attempts=pl.len(),
            xp_made=pl.col("s_xp_made").sum(),
        )
    )
    out = _merge_players([ko_agg, fg_agg, xp_agg], "pos_team")
    if out.height == 0:
        return out
    out = _fill_counts(out, ("pos_team", "player_key"))
    for c in (
        "kickoffs",
        "kickoff_yards",
        "kickoff_touchbacks",
        "kickoff_returns_allowed",
        "kickoff_return_yards_allowed",
        "fg_attempts",
        "fg_made",
        "xp_attempts",
        "xp_made",
    ):
        if c not in out.columns:
            out = out.with_columns(pl.lit(0).alias(c))
    return out.with_columns(
        kickoff_avg=_st_rate("kickoff_yards", "kickoffs"),
        kickoff_touchback_rate=_st_rate("kickoff_touchbacks", "kickoffs"),
        kickoff_return_avg_allowed=_st_rate("kickoff_return_yards_allowed", "kickoff_returns_allowed"),
        fg_pct=_st_rate("fg_made", "fg_attempts"),
        xp_pct=_st_rate("xp_made", "xp_attempts"),
    ).drop("player_key")


def _st_punter_rows(st: pl.DataFrame) -> pl.DataFrame:
    pu = st.filter(pl.col("s_punt")).with_columns(
        pos_team=pl.coalesce(pl.col("s_punt_team"), pl.col("s_pos_team")),
        player_key=pl.coalesce(pl.col("s_punter_player_id"), pl.col("s_punter_player_name")),
        inside_20=(~pl.col("s_punt_tb") & (pl.col("punt_landing") <= 20) & (pl.col("punt_landing") >= 0)),
    )
    if pu.height == 0:
        return pl.DataFrame()
    out = (
        pu.filter(pl.col("player_key").is_not_null())
        .group_by(["pos_team", "player_key"])
        .agg(
            player_id=pl.col("s_punter_player_id").drop_nulls().first(),
            player_name=pl.col("s_punter_player_name").drop_nulls().first(),
            punts=pl.len(),
            punt_yards=pl.col("s_yds_punted").sum(),
            punt_long=pl.col("s_yds_punted").max(),
            punt_touchbacks=pl.col("s_punt_tb").sum(),
            punt_inside_20=pl.col("inside_20").sum(),
            punt_fair_catches=pl.col("s_punt_fair_catch").sum(),
            punt_downed=pl.col("s_punt_downed").sum(),
            punt_out_of_bounds=pl.col("s_punt_oob").sum(),
            punt_blocked=pl.col("s_punt_blocked").sum(),
            punt_returns_allowed=pl.col("punt_returned").sum(),
            punt_return_yards_allowed=pl.col("s_yds_punt_return").filter(pl.col("punt_returned")).sum(),
            punt_return_tds_allowed=(pl.col("punt_returned") & pl.col("s_touchdown")).sum(),
            punt_epa=pl.col("s_EPA").sum(),
        )
    )
    if out.height == 0:
        return out
    out = _fill_counts(out, ("pos_team", "player_key"))
    return (
        out.with_columns(
            punt_avg=_st_rate("punt_yards", "punts"),
            punt_net_yards=pl.col("punt_yards") - pl.col("punt_return_yards_allowed") - 20 * pl.col("punt_touchbacks"),
        )
        .with_columns(
            punt_net_avg=_st_rate("punt_net_yards", "punts"),
            punt_inside_20_rate=_st_rate("punt_inside_20", "punts"),
            punt_return_avg_allowed=_st_rate("punt_return_yards_allowed", "punt_returns_allowed"),
        )
        .drop("player_key")
    )


def _st_returner_rows(st: pl.DataFrame) -> pl.DataFrame:
    kr = st.filter(pl.col("kick_returned")).with_columns(
        pos_team=pl.col("kick_ret_team"),
        player_key=pl.coalesce(pl.col("s_kickoff_return_player_id"), pl.col("s_kickoff_return_player_name")),
    )
    kr_agg = (
        kr.filter(pl.col("player_key").is_not_null())
        .group_by(["pos_team", "player_key"])
        .agg(
            player_id=pl.col("s_kickoff_return_player_id").drop_nulls().first(),
            player_name=pl.col("s_kickoff_return_player_name").drop_nulls().first(),
            kick_returns=pl.len(),
            kick_return_yards=pl.col("s_yds_kickoff_return").sum(),
            kick_return_long=pl.col("s_yds_kickoff_return").max(),
            kick_return_tds=pl.col("s_touchdown").sum(),
            kick_return_epa=pl.col("s_EPA").sum(),  # pos_team is the return team on a kickoff
        )
    )
    pr = st.filter(pl.col("punt_returned")).with_columns(
        pos_team=pl.col("punt_ret_team"),
        player_key=pl.coalesce(pl.col("s_punt_return_player_id"), pl.col("s_punt_return_player_name")),
    )
    pr_agg = (
        pr.filter(pl.col("player_key").is_not_null())
        .group_by(["pos_team", "player_key"])
        .agg(
            player_id=pl.col("s_punt_return_player_id").drop_nulls().first(),
            player_name=pl.col("s_punt_return_player_name").drop_nulls().first(),
            punt_returns=pl.len(),
            punt_return_yards=pl.col("s_yds_punt_return").sum(),
            punt_return_long=pl.col("s_yds_punt_return").max(),
            punt_return_tds=pl.col("s_touchdown").sum(),
            punt_return_epa=-pl.col("s_EPA").sum(),  # pos_team is the punting team on a punt
        )
    )
    out = _merge_players([kr_agg, pr_agg], "pos_team")
    if out.height == 0:
        return out
    out = _fill_counts(out, ("pos_team", "player_key"))
    for c in ("kick_returns", "kick_return_yards", "punt_returns", "punt_return_yards"):
        if c not in out.columns:
            out = out.with_columns(pl.lit(0).alias(c))
    return out.with_columns(
        kick_return_avg=_st_rate("kick_return_yards", "kick_returns"),
        punt_return_avg=_st_rate("punt_return_yards", "punt_returns"),
    ).drop("player_key")


def _st_block_rows(st: pl.DataFrame) -> pl.DataFrame:
    """Per (defending team, player): punts and field goals blocked."""
    pb = st.filter(pl.col("s_punt") & pl.col("s_punt_block_player_name").is_not_null()).select(
        def_pos_team=pl.col("punt_ret_team"),
        player_id=pl.col("s_punt_block_player_id"),
        player_name=pl.col("s_punt_block_player_name"),
        punt_blocks=pl.lit(1),
        fg_blocks=pl.lit(0),
    )
    fb = st.filter(pl.col("s_fg_attempt") & pl.col("s_fg_block_player_name").is_not_null()).select(
        def_pos_team=pl.col("s_def_pos_team"),
        player_id=pl.col("s_fg_block_player_id"),
        player_name=pl.col("s_fg_block_player_name"),
        punt_blocks=pl.lit(0),
        fg_blocks=pl.lit(1),
    )
    rows = pl.concat([pb, fb], how="diagonal_relaxed")
    if rows.height == 0:
        return pl.DataFrame()
    return (
        rows.with_columns(player_key=pl.coalesce(pl.col("player_id"), pl.col("player_name")))
        .group_by(["def_pos_team", "player_key"])
        .agg(
            player_id=pl.col("player_id").drop_nulls().first(),
            player_name=pl.col("player_name").drop_nulls().first(),
            punt_blocks=pl.col("punt_blocks").sum(),
            fg_blocks=pl.col("fg_blocks").sum(),
        )
        .with_columns(blocks=pl.col("punt_blocks") + pl.col("fg_blocks"))
        .drop("player_key")
    )


def _st_team_rows(st: pl.DataFrame) -> pl.DataFrame:
    """Per team: own kicking / punting / returns and the coverage and blocks against."""
    ko = st.filter(pl.col("s_kickoff_play"))
    kick = ko.group_by(pl.col("kick_team").alias("pos_team")).agg(
        kickoffs=pl.len(),
        kickoff_touchbacks=pl.col("s_kickoff_tb").sum(),
        kickoff_returns_allowed=pl.col("kick_returned").sum(),
        kickoff_return_yards_allowed=pl.col("s_yds_kickoff_return").filter(pl.col("kick_returned")).sum(),
        kickoff_return_tds_allowed=(pl.col("kick_returned") & pl.col("s_touchdown")).sum(),
        kickoff_epa=pl.col("kick_epa").sum(),
    )
    kret = (
        ko.filter(pl.col("kick_returned"))
        .group_by(pl.col("kick_ret_team").alias("pos_team"))
        .agg(
            kick_returns=pl.len(),
            kick_return_yards=pl.col("s_yds_kickoff_return").sum(),
            kick_return_tds=pl.col("s_touchdown").sum(),
            kick_return_epa=pl.col("s_EPA").sum(),
        )
    )
    pu = st.filter(pl.col("s_punt"))
    punt = pu.group_by(pl.coalesce(pl.col("s_punt_team"), pl.col("s_pos_team")).alias("pos_team")).agg(
        punts=pl.len(),
        punt_yards=pl.col("s_yds_punted").sum(),
        punt_touchbacks=pl.col("s_punt_tb").sum(),
        punts_blocked=pl.col("s_punt_blocked").sum(),
        punt_returns_allowed=pl.col("punt_returned").sum(),
        punt_return_yards_allowed=pl.col("s_yds_punt_return").filter(pl.col("punt_returned")).sum(),
        punt_return_tds_allowed=(pl.col("punt_returned") & pl.col("s_touchdown")).sum(),
        punt_epa=pl.col("s_EPA").sum(),
    )
    pret = (
        pu.filter(pl.col("punt_returned"))
        .group_by(pl.col("punt_ret_team").alias("pos_team"))
        .agg(
            punt_returns=pl.len(),
            punt_return_yards=pl.col("s_yds_punt_return").sum(),
            punt_return_tds=pl.col("s_touchdown").sum(),
            punt_return_epa=-pl.col("s_EPA").sum(),
        )
    )
    fg = st.filter(pl.col("s_fg_attempt"))
    fgs = fg.group_by(pl.col("fg_kick_team").alias("pos_team")).agg(
        fg_attempts=pl.len(),
        fg_made=pl.col("s_fg_made").sum(),
        fgs_blocked=pl.col("s_fg_block_player_name").is_not_null().sum(),
        fg_epa=pl.col("s_EPA").sum(),
    )
    blocks = pl.concat(
        [
            pu.filter(pl.col("s_punt_blocked")).select(
                pl.col("punt_ret_team").alias("pos_team"), punt_blocks_by=pl.lit(1), fg_blocks_by=pl.lit(0)
            ),
            fg.filter(pl.col("s_fg_block_player_name").is_not_null()).select(
                pl.col("s_def_pos_team").alias("pos_team"), punt_blocks_by=pl.lit(0), fg_blocks_by=pl.lit(1)
            ),
        ],
        how="diagonal_relaxed",
    )
    blocks_agg = (
        blocks.group_by("pos_team").agg(pl.col("punt_blocks_by").sum(), pl.col("fg_blocks_by").sum())
        if blocks.height
        else None
    )
    out = None
    for f in (kick, kret, punt, pret, fgs, blocks_agg):
        if f is None or f.height == 0:
            continue
        out = f if out is None else out.join(f, on="pos_team", how="full", coalesce=True)
    if out is None:
        return pl.DataFrame()
    out = _fill_counts(out.filter(pl.col("pos_team").is_not_null()), ("pos_team",))
    for c in (
        "kickoffs",
        "kickoff_touchbacks",
        "kickoff_returns_allowed",
        "kickoff_return_yards_allowed",
        "punts",
        "punt_yards",
        "punt_touchbacks",
        "punt_returns_allowed",
        "punt_return_yards_allowed",
        "kick_returns",
        "kick_return_yards",
        "punt_returns",
        "punt_return_yards",
        "fg_attempts",
        "fg_made",
    ):
        if c not in out.columns:
            out = out.with_columns(pl.lit(0).alias(c))
    return out.with_columns(
        punt_net_yards=pl.col("punt_yards") - pl.col("punt_return_yards_allowed") - 20 * pl.col("punt_touchbacks")
    ).with_columns(
        kickoff_touchback_rate=_st_rate("kickoff_touchbacks", "kickoffs"),
        kickoff_return_avg_allowed=_st_rate("kickoff_return_yards_allowed", "kickoff_returns_allowed"),
        punt_net_avg=_st_rate("punt_net_yards", "punts"),
        punt_return_avg_allowed=_st_rate("punt_return_yards_allowed", "punt_returns_allowed"),
        kick_return_avg=_st_rate("kick_return_yards", "kick_returns"),
        punt_return_avg=_st_rate("punt_return_yards", "punt_returns"),
        fg_pct=_st_rate("fg_made", "fg_attempts"),
    )


_ST_SECTIONS = ("st_kickers", "st_punters", "st_returners", "st_blocks", "st_team")


def _special_teams(plays: pl.DataFrame) -> dict[str, pl.DataFrame]:
    st = _st_frame(plays) if isinstance(plays, pl.DataFrame) else pl.DataFrame()
    if st.height == 0:
        return {k: pl.DataFrame() for k in _ST_SECTIONS}
    return {
        "st_kickers": _st_kicker_rows(st),
        "st_punters": _st_punter_rows(st),
        "st_returners": _st_returner_rows(st),
        "st_blocks": _st_block_rows(st),
        "st_team": _st_team_rows(st),
    }


def _position_map(participants: Optional[pl.DataFrame]) -> dict[str, Any]:
    """athlete id -> position group, from every ``{type}_position_id`` column."""
    if participants is None or participants.height == 0:
        return {}
    out: dict[str, Any] = {}
    for col in participants.columns:
        if not col.endswith("_position_id"):
            continue
        id_col = col.replace("_position_id", "_player_id")
        if id_col not in participants.columns:
            continue
        for pid, pos in participants.select(id_col, col).drop_nulls().iter_rows():
            grp = position_group(pos)
            if grp and str(pid) not in out:
                out[str(pid)] = grp
    return out


def _attach_group(df: pl.DataFrame, positions: dict[str, Any]) -> pl.DataFrame:
    if df.height == 0 or "player_id" not in df.columns:
        return df
    return df.with_columns(
        position_group=pl.col("player_id").map_elements(lambda pid: positions.get(pid), return_dtype=pl.Utf8)
    )


def _to_rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    return df.to_dicts() if df is not None and df.height else []


def create_usage_box(
    plays: pl.DataFrame,
    participants: Optional[pl.DataFrame] = None,
    *,
    league: str = "cfb",
    third_down_curve: Optional[pl.DataFrame] = None,
) -> dict[str, list[dict[str, Any]]]:
    """Build the six usage / situational sections for one game.

    Args:
        plays: the processed plays frame (``NFLPlayProcess`` / ``CFBPlayProcess``).
        participants: the wide per-play participants frame (tackles and
            position groups come from it); ``None`` yields empty tackle
            sections and no position groups.
        league: ``"cfb"`` or ``"nfl"`` -- selects the bundled third-down curve.
        third_down_curve: override the bundled curve (``distance``, ``rate``).

    Returns:
        ``{section: [row, ...]}`` for every name in :data:`SECTIONS`; a
        section with nothing to report is an empty list.

    Example:
        Quick start::

            from sportsdataverse.football.usage_box import create_usage_box
            box = create_usage_box(plays, participants, league="cfb")
            sorted(box["player_usage"], key=lambda r: -r["target_share"] or 0)[:5]
    """
    empty: dict[str, list[dict[str, Any]]] = {s: [] for s in SECTIONS}
    if not isinstance(plays, pl.DataFrame) or plays.height == 0:
        return empty
    curve = third_down_curve
    if curve is None:
        try:
            curve = load_third_down_curve(league)
        except (FileNotFoundError, OSError, ValueError):
            curve = None
    df = _standing_scrimmage(plays)
    if df.height == 0 or "pos_team" not in df.columns:
        empty.update({k: _to_rows(v) for k, v in _special_teams(plays).items()})
        return empty
    positions = _position_map(participants)

    long = _player_rows(df, curve, positions)
    if long.height:
        totals = _team_totals(df)
        player = _usage_rates(_usage_agg(long, ["pos_team", "player_key"]), totals)
        names = long.group_by("player_key").agg(
            player_id=pl.col("player_id").drop_nulls().first(),
            player_name=pl.col("player_name").drop_nulls().first(),
        )
        player = player.join(names, on="player_key", how="left").drop("player_key")
        player = _attach_group(player, positions)
        if "position_group" in player.columns:
            grouped_src = long.with_columns(
                position_group=pl.col("player_id").map_elements(lambda pid: positions.get(pid), return_dtype=pl.Utf8)
            ).filter(pl.col("position_group").is_not_null())
            group = (
                _usage_rates(_usage_agg(grouped_src, ["pos_team", "position_group"]), totals)
                if grouped_src.height
                else pl.DataFrame()
            )
        else:
            group = pl.DataFrame()
    else:
        player, group = pl.DataFrame(), pl.DataFrame()

    tackles_long = _tackle_rows(participants, plays, positions) if participants is not None else pl.DataFrame()
    if tackles_long.height:
        tackles = _tackle_agg(tackles_long, ["def_pos_team", "player_id"])
        names = tackles_long.group_by("player_id").agg(
            player_name=pl.col("player_name").drop_nulls().first(),
            position_group=pl.col("position_group").drop_nulls().first(),
        )
        tackles = tackles.join(names, on="player_id", how="left")
        grouped = tackles_long.filter(pl.col("position_group").is_not_null())
        group_tackles = _tackle_agg(grouped, ["def_pos_team", "position_group"]) if grouped.height else pl.DataFrame()
    else:
        tackles, group_tackles = pl.DataFrame(), pl.DataFrame()

    drv = _drive_frame(df)
    team = _team_rows(df, drv, curve)
    scripting = _drive_eff(drv, ["pos_team", "script"]) if drv.height else pl.DataFrame()
    st = _special_teams(plays)

    out = {
        "player_usage": _to_rows(player),
        "position_group_usage": _to_rows(group),
        "tackles": _to_rows(tackles),
        "position_group_tackles": _to_rows(group_tackles),
        "team_usage": _to_rows(team),
        "drive_scripting": _to_rows(scripting),
    }
    out.update({k: _to_rows(v) for k, v in st.items()})
    return out


# --------------------------------------------------------------------------
# season aggregation (leaderboards)
# --------------------------------------------------------------------------
_SUM_KEYS = {
    "player_usage": ["pos_team", "player_id", "player_name", "position_group"],
    "position_group_usage": ["pos_team", "position_group"],
    "tackles": ["def_pos_team", "player_id", "player_name", "position_group"],
    "position_group_tackles": ["def_pos_team", "position_group"],
    "team_usage": ["pos_team"],
    "drive_scripting": ["pos_team", "script"],
    "st_kickers": ["pos_team", "player_id", "player_name"],
    "st_punters": ["pos_team", "player_id", "player_name"],
    "st_returners": ["pos_team", "player_id", "player_name"],
    "st_blocks": ["def_pos_team", "player_id", "player_name"],
    "st_team": ["pos_team"],
}
_RATE_COLS = {
    "fd_td_rate",
    "explosive_rate",
    "success_rate",
    "epa_per_opportunity",
    "rz_touchdown_rate",
    "so_touchdown_rate",
    "third_down_rate",
    "third_down_over_expected",
    "target_share",
    "first_down_share",
    "touch_share",
    "tackle_share",
    "epa_per_play",
    "yards_per_play",
    "points_per_drive",
    "touchdown_rate",
    "scoring_opp_rate",
    "rz_touchdown_rate",
    "rz_points_per_trip",
    "rz_success_rate",
    "rz_epa_per_play",
    "so_touchdown_rate",
    "so_points_per_trip",
    "so_success_rate",
    "so_epa_per_play",
    "kickoff_avg",
    "kickoff_touchback_rate",
    "kickoff_return_avg_allowed",
    "fg_pct",
    "xp_pct",
    "punt_avg",
    "punt_net_avg",
    "punt_inside_20_rate",
    "punt_return_avg_allowed",
    "kick_return_avg",
    "punt_return_avg",
    "punt_net_yards",
}


def aggregate_usage_box(section: str, frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Sum per-game section rows over games and recompute every rate.

    The season leaderboard for a section is its per-game rows (with a
    ``season`` / ``game_id`` stamp) summed on the section's identity keys,
    rates recomputed from the sums so a leaderboard never averages averages.

    Args:
        section: one of :data:`SECTIONS`.
        frames: per-game frames of that section (the released
            ``espn_{league}_adv_*`` rows, or ``pl.from_dicts(box[section])``).

    Returns:
        One row per identity key with the summed counts and recomputed rates;
        an empty frame when nothing was passed.

    Example:
        Season target-share leaderboard::

            from sportsdataverse.football.usage_box import aggregate_usage_box
            lb = aggregate_usage_box("player_usage", season_frames)
            lb.sort("target_share", descending=True).head(10)
    """
    keep = [f for f in frames if f is not None and f.height]
    if not keep:
        return pl.DataFrame()
    df = pl.concat(keep, how="diagonal_relaxed")
    keys = [k for k in _SUM_KEYS[section] if k in df.columns]
    if "season" in df.columns:
        keys = ["season", *keys]
    drop = {"game_id", "week", "nflverse_game_id", "season_type", *_RATE_COLS, "team_tackle_points"}
    numeric = [c for c in df.columns if c not in keys and c not in drop and df.schema[c].is_numeric()]
    longs = [c for c in numeric if c.endswith("_long")]  # a season long is a max, not a sum
    sums = [c for c in numeric if c not in longs]
    g = df.group_by(keys, maintain_order=True).agg(
        [pl.col(c).sum() for c in sums] + [pl.col(c).max() for c in longs] + [pl.len().alias("games")]
    )

    def _rate(num: str, den: str) -> pl.Expr:
        return pl.when(pl.col(den) > 0).then(pl.col(num) / pl.col(den)).otherwise(None)

    have = set(g.columns)
    exprs = []
    if {"fd_or_td", "opportunities"} <= have:
        exprs += [
            _rate("fd_or_td", "opportunities").alias("fd_td_rate"),
            _rate("explosive_plays", "opportunities").alias("explosive_rate"),
            _rate("successful_plays", "opportunities").alias("success_rate"),
            _rate("epa", "opportunities").alias("epa_per_opportunity"),
            _rate("rz_touchdowns", "rz_touches").alias("rz_touchdown_rate"),
            _rate("so_touchdowns", "so_touches").alias("so_touchdown_rate"),
            _rate("third_down_conversions", "third_down_opportunities").alias("third_down_rate"),
            (pl.col("third_down_conversions") - pl.col("third_down_expected")).alias("third_down_over_expected"),
        ]
        if {"team_targets", "team_first_downs", "team_touches"} <= have:
            exprs += [
                _rate("targets", "team_targets").alias("target_share"),
                _rate("first_downs", "team_first_downs").alias("first_down_share"),
                _rate("touches", "team_touches").alias("touch_share"),
            ]
    if {"tackles", "assists"} <= have:
        g = g.with_columns(tackle_points=pl.col("tackles") + 0.5 * pl.col("assists"))
        team_key = [k for k in keys if k != "player_id" and k != "player_name" and k != "position_group"]
        team = g.group_by(team_key).agg(team_tackle_points=pl.col("tackle_points").sum())
        g = g.join(team, on=team_key, how="left")
        exprs.append(_rate("tackle_points", "team_tackle_points").alias("tackle_share"))
    if {"drives", "plays", "points"} <= have:
        exprs += [
            _rate("epa", "plays").alias("epa_per_play"),
            _rate("successes", "plays").alias("success_rate"),
            _rate("yards", "plays").alias("yards_per_play"),
            _rate("points", "drives").alias("points_per_drive"),
            _rate("touchdowns", "drives").alias("touchdown_rate"),
            _rate("scoring_opps", "drives").alias("scoring_opp_rate"),
        ]
    if "kickoffs" in have or "fg_attempts" in have:
        for num, den, name in (
            ("kickoff_yards", "kickoffs", "kickoff_avg"),
            ("kickoff_touchbacks", "kickoffs", "kickoff_touchback_rate"),
            ("kickoff_return_yards_allowed", "kickoff_returns_allowed", "kickoff_return_avg_allowed"),
            ("fg_made", "fg_attempts", "fg_pct"),
            ("xp_made", "xp_attempts", "xp_pct"),
        ):
            if {num, den} <= have:
                exprs.append(_rate(num, den).alias(name))
    if {"punts", "punt_yards", "punt_return_yards_allowed", "punt_touchbacks"} <= have:
        g = g.with_columns(
            punt_net_yards=pl.col("punt_yards") - pl.col("punt_return_yards_allowed") - 20 * pl.col("punt_touchbacks")
        )
        have = set(g.columns)
        for num, den, name in (
            ("punt_yards", "punts", "punt_avg"),
            ("punt_net_yards", "punts", "punt_net_avg"),
            ("punt_inside_20", "punts", "punt_inside_20_rate"),
            ("punt_return_yards_allowed", "punt_returns_allowed", "punt_return_avg_allowed"),
        ):
            if {num, den} <= have:
                exprs.append(_rate(num, den).alias(name))
    for num, den, name in (
        ("kick_return_yards", "kick_returns", "kick_return_avg"),
        ("punt_return_yards", "punt_returns", "punt_return_avg"),
    ):
        if {num, den} <= have:
            exprs.append(_rate(num, den).alias(name))
    if {"plays", "successful_plays", "third_down_opportunities"} <= have and "drives" not in have:
        exprs += [
            _rate("successful_plays", "plays").alias("success_rate"),
            _rate("explosive_plays", "plays").alias("explosive_rate"),
            _rate("epa", "plays").alias("epa_per_play"),
            _rate("third_down_conversions", "third_down_opportunities").alias("third_down_rate"),
            (pl.col("third_down_conversions") - pl.col("third_down_expected")).alias("third_down_over_expected"),
        ]
        for pfx in ("rz", "so"):
            if f"{pfx}_trips" in have:
                exprs += [
                    _rate(f"{pfx}_touchdowns", f"{pfx}_trips").alias(f"{pfx}_touchdown_rate"),
                    _rate(f"{pfx}_points", f"{pfx}_trips").alias(f"{pfx}_points_per_trip"),
                    _rate(f"{pfx}_successes", f"{pfx}_plays").alias(f"{pfx}_success_rate"),
                    _rate(f"{pfx}_epa", f"{pfx}_plays").alias(f"{pfx}_epa_per_play"),
                ]
    return g.with_columns(exprs) if exprs else g
