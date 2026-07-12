"""ESPN WBB play-by-play producer -- polars port of ``wehoop:::helper_espn_wbb_pbp``.

Source: ``wehoop/R/espn_wbb_data.R`` lines 2763-3160 (wehoop 3.0.0). Takes one
game's stored ``final.json`` payload (the wehoop-wbb-raw scraper persists the
*processed* summary: plays are flat dicts with dotted/camelCase keys and the
engineered clock/spread/lead-lag features already baked in) and returns the
per-game plays frame published to the ``espn_womens_college_basketball_pbp``
release. NOT the same lineage as ``wbb_pbp.helper_wbb_pbp`` (the interactive
``espn_wbb_pbp`` pipeline) -- this is the release-parity producer.

The R-released parquet is the parity oracle: dtypes and column order mirror it
exactly (R integer == Int32), with ONE deliberate dtype improvement -- the play
``id`` is emitted Int64, not R's precision-losing Float64 (see ``_INT64_COLS``).
The four ``coordinate_*`` columns the creation script appends as season-level
NA fallback when a season ships no coordinates are added here per-game
(identical union outcome, no season-level special case needed).

Documented deviations from R (both on compat paths current payloads skip):
the R away-first ``id_vars`` branch plucks the DARK logo for ``homeTeamLogo``
(``logos[[2]]``, a copy-paste bug) and hard-errors on one-logo teams -- this
port normalizes both teams to ``logos[0]``/``logos[1]`` with null fallbacks;
and the nested-``participants`` compat unnest emits no ``play_id`` companion
column and tolerates 0-3 participants where R assumes exactly 2.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.wbb.wbb_team_box import _game_datetime, _to_int

# Released-parquet dtype contract (union of the published seasons' schemas).
# Columns not listed pass through with their inferred dtype.
_INT32_COLS: tuple[str, ...] = (
    "game_play_number",
    "sequence_number",
    "type_id",
    "away_score",
    "home_score",
    "period_number",
    "score_value",
    "team_id",
    "athlete_id_1",
    "athlete_id_2",
    "athlete_id_3",
    "game_id",
    "season",
    "season_type",
    "home_team_id",
    "away_team_id",
    "qtr",
    "clock_minutes",
    "clock_seconds",
    "half",
    "game_half",
    "lead_qtr",
    "lead_half",
    "start_quarter_seconds_remaining",
    "start_half_seconds_remaining",
    "start_game_seconds_remaining",
    "end_quarter_seconds_remaining",
    "end_half_seconds_remaining",
    "end_game_seconds_remaining",
    "period",
    "lag_qtr",
    "lag_half",
    "points_attempted",
)
_FLOAT64_COLS: tuple[str, ...] = (
    "game_spread",
    "home_team_spread",
    "coordinate_x_raw",
    "coordinate_y_raw",
    "coordinate_x",
    "coordinate_y",
)
# DELIBERATE divergence from the R releases: R/jsonlite has no int64, so the
# published `id` is Float64 and LOSES PRECISION above 2^53 -- adjacent play ids
# (~4e17) round to the same double and collide. The stored payload carries the
# id as a true integer, so the Python producer emits Int64 and keeps it exact.
_INT64_COLS: tuple[str, ...] = ("id",)

_COORD_COLS: tuple[str, ...] = (
    "coordinate_x_raw",
    "coordinate_y_raw",
    "coordinate_x",
    "coordinate_y",
)

__all__ = ["helper_wbb_play_by_play"]


def _clean_name(name: str) -> str:
    """janitor::clean_names parity for the stored dotted/camelCase play keys."""
    return underscore(name.replace(".", "_"))


def _home_team_id(competitors: list[dict[str, Any]]) -> int | None:
    """The home competitor's team id (R: homeAway1-branched pluck chain)."""
    for c in competitors:
        if c.get("homeAway") == "home":
            return _to_int((c.get("team") or {}).get("id"))
    return None


def _id_vars(competitors: list[dict[str, Any]]) -> dict[str, Any]:
    """R id_vars fallback block -- only bound when plays lack homeTeamName.

    The wehoop-wbb-raw payloads always carry the team columns, so this is a
    compat path for pre-processed payload shapes.
    """
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") != "home"), {})
    out: dict[str, Any] = {}
    for prefix, c in (("home", home), ("away", away)):
        team = c.get("team") or {}
        logos = team.get("logos") or []
        records = c.get("record") or []
        out[f"{prefix}TeamId"] = _to_int(team.get("id"))
        out[f"{prefix}TeamMascot"] = team.get("name")
        out[f"{prefix}TeamName"] = team.get("location")
        out[f"{prefix}TeamAbbrev"] = team.get("abbreviation")
        out[f"{prefix}TeamLogo"] = (logos[0] or {}).get("href") if logos else None
        out[f"{prefix}TeamLogoDark"] = (logos[1] or {}).get("href") if len(logos) > 1 else None
        out[f"{prefix}TeamFullName"] = team.get("displayName")
        out[f"{prefix}TeamColor"] = team.get("color")
        out[f"{prefix}TeamAlternateColor"] = team.get("alternateColor")
        out[f"{prefix}TeamScore"] = _to_int(c.get("score"))
        out[f"{prefix}TeamWinner"] = c.get("winner")
        out[f"{prefix}TeamRecord"] = (records[0] or {}).get("summary") if records else None
    return out


def helper_wbb_play_by_play(final: dict) -> pl.DataFrame:
    """Parse one game's stored payload into the released play-by-play frame.

    Faithful polars port of ``wehoop:::helper_espn_wbb_pbp``
    (``wehoop/R/espn_wbb_data.R:2763``). Returns one row per play whose
    column set, order, and dtypes match the R-released
    ``espn_womens_college_basketball_pbp`` parquet for that game's season.

    Args:
        final: One game's stored payload (the ``final.json`` the
            ``wehoop-wbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per play. Empty (zero-column) frame when the
        payload has ``playByPlaySource == "none"`` or 10 or fewer plays
        (R guard) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_play_by_play
            final = json.load(open("401700473.json", encoding="utf-8"))
            df = helper_wbb_play_by_play(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("shooting_play") == True).height

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    header = final.get("header") or {}
    competitions = header.get("competitions") or []
    if not competitions:
        return pl.DataFrame()
    comp = competitions[0]
    plays_raw = final.get("plays") or []
    if comp.get("playByPlaySource") == "none" or len(plays_raw) <= 10:
        return pl.DataFrame()
    competitors = comp.get("competitors") or []

    game_datetime = _game_datetime(comp["date"])
    season = header.get("season") or {}
    home_id = _home_team_id(competitors)

    # Compat: old nested participants[] -> dotted keys (R unnest_wider branch).
    plays: list[dict[str, Any]] = []
    for p in plays_raw:
        parts = p.get("participants")
        if isinstance(parts, list):
            p = {k: v for k, v in p.items() if k != "participants"}
            for i, part in enumerate(parts[:3]):
                p[f"participants.{i}.athlete.id"] = ((part or {}).get("athlete") or {}).get("id")
        plays.append(p)

    # First-seen key order across all plays (R: fromJSON record-union order).
    order: dict[str, None] = {}
    for p in plays:
        for k in p:
            order.setdefault(k, None)
    # R: select(-any_of(c("athlete.id", "athlete_id")))
    for drop in ("athlete.id", "athlete_id"):
        order.pop(drop, None)
    df = pl.DataFrame({k: [p.get(k) for p in plays] for k in order}, strict=False)

    # Coordinate transform (R lines 3058-3090): FT plays pinned to (25, 13.75),
    # then home plays flipped about center court; raw values kept in place and
    # the transformed pair appended.
    has_coords = "coordinate.x" in df.columns and "coordinate.y" in df.columns
    if has_coords:
        ft = pl.col("type.text").str.contains("Free Throw")
        df = df.with_columns(
            pl.when(ft).then(pl.lit(25.0)).otherwise(pl.col("coordinate.x").cast(pl.Float64)).alias("coordinate.x"),
            pl.when(ft).then(pl.lit(13.75)).otherwise(pl.col("coordinate.y").cast(pl.Float64)).alias("coordinate.y"),
        )
        is_home = pl.col("team.id").cast(pl.Int64, strict=False) == home_id
        df = df.with_columns(
            pl.when(is_home)
            .then(-1 * (pl.col("coordinate.y") - 41.75))
            .otherwise(pl.col("coordinate.y") - 41.75)
            .alias("coordinate_x_transformed"),
            pl.when(is_home)
            .then(-1 * (pl.col("coordinate.x") - 25))
            .otherwise(pl.col("coordinate.x") - 25)
            .alias("coordinate_y_transformed"),
        ).rename(
            {
                "coordinate.x": "coordinate.x.raw",
                "coordinate.y": "coordinate.y.raw",
                "coordinate_x_transformed": "coordinate.x",
                "coordinate_y_transformed": "coordinate.y",
            }
        )

    # R id_vars fallback: bound only when plays lack the team columns.
    if "homeTeamName" not in df.columns:
        df = df.with_columns([pl.lit(v).alias(k) for k, v in _id_vars(competitors).items()])

    # janitor::clean_names + game/season columns (overwrite in place; the two
    # date columns are new and append) + participants -> athlete_id renames.
    df = df.rename({c: _clean_name(c) for c in df.columns})
    df = df.with_columns(
        pl.lit(_to_int(header.get("id"))).alias("game_id"),
        pl.lit(season.get("year")).alias("season"),
        pl.lit(season.get("type")).alias("season_type"),
        pl.lit(game_datetime.date()).alias("game_date"),
        pl.lit(game_datetime).cast(pl.Datetime("us", "America/New_York")).alias("game_date_time"),
    )
    df = df.rename(
        {
            old: new
            for old, new in (
                ("participants_0_athlete_id", "athlete_id_1"),
                ("participants_1_athlete_id", "athlete_id_2"),
                ("participants_2_athlete_id", "athlete_id_3"),
            )
            if old in df.columns
        }
    )

    # Creation-script parity: seasons without coordinates ship the four
    # columns as all-null Float64 (appended after the date columns).
    if not has_coords:
        df = df.with_columns([pl.lit(None, dtype=pl.Float64).alias(c) for c in _COORD_COLS])

    # Released dtype contract (except `id` -- see _INT64_COLS).
    return df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in _INT32_COLS if c in df.columns]
        + [pl.col(c).cast(pl.Int64, strict=False) for c in _INT64_COLS if c in df.columns]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in _FLOAT64_COLS if c in df.columns]
    )
