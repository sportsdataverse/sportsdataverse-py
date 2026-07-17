"""ESPN WBB team-box producer -- polars port of ``wehoop:::helper_espn_wbb_team_box``.

Source: ``wehoop/R/espn_wbb_data.R`` lines 3167-3474 (wehoop 3.0.0). Takes one
game's ESPN summary (``final.json``) payload and returns the tidy per-team box
frame published to the ``espn_womens_college_basketball_team_boxscores``
release. The R-released parquet is the byte-parity oracle: dtypes mirror it
exactly (R ``as.integer`` == Int32; the four narrative stats ESPN ships as
prose -- fast_break_points, largest_lead, points_in_paint, turnover_points --
stay String, matching R).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from sportsdataverse.dl_utils import underscore

_EASTERN = ZoneInfo("America/New_York")

# R: tidyr::separate("<made>-<attempted>", sep = "-") -- split in place.
_SPLIT_STATS: dict[str, tuple[str, str]] = {
    "fieldGoalsMade-fieldGoalsAttempted": ("fieldGoalsMade", "fieldGoalsAttempted"),
    "freeThrowsMade-freeThrowsAttempted": ("freeThrowsMade", "freeThrowsAttempted"),
    "threePointFieldGoalsMade-threePointFieldGoalsAttempted": (
        "threePointFieldGoalsMade",
        "threePointFieldGoalsAttempted",
    ),
}

# (ESPN team key, snake suffix) in the R column-assignment order.
_TEAM_META: tuple[tuple[str, str], ...] = (
    ("uid", "uid"),
    ("slug", "slug"),
    ("location", "location"),
    ("name", "name"),
    ("abbreviation", "abbreviation"),
    ("displayName", "display_name"),
    ("shortDisplayName", "short_display_name"),
    ("color", "color"),
    ("alternateColor", "alternate_color"),
    ("logo", "logo"),
)

# R: dplyr::across(any_of(...), as.integer) -- cast only when present.
_INT32_STATS: tuple[str, ...] = (
    "assists",
    "blocks",
    "defensive_rebounds",
    "field_goals_made",
    "field_goals_attempted",
    "flagrant_fouls",
    "fouls",
    "free_throws_made",
    "free_throws_attempted",
    "offensive_rebounds",
    "steals",
    "team_turnovers",
    "technical_fouls",
    "three_point_field_goals_made",
    "three_point_field_goals_attempted",
    "total_rebounds",
    "total_technical_fouls",
    "total_turnovers",
    "turnovers",
)
_INT32_META: tuple[str, ...] = (
    "game_id",
    "season",
    "season_type",
    "team_id",
    "team_score",
    "opponent_team_id",
    "opponent_team_score",
)
_FLOAT_STATS: tuple[str, ...] = (
    "field_goal_pct",
    "free_throw_pct",
    "three_point_field_goal_pct",
)

__all__ = ["helper_wbb_team_box"]


def _to_int(val: Any) -> int | None:
    """R ``as.integer`` semantics: numeric strings truncate ("12.5" -> 12),
    parse failure -> NA (None), never raise."""
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None


def _game_datetime(date_str: str) -> datetime:
    """R: strip trailing Z, ``ymd_hm`` (UTC), ``with_tz("America/New_York")``."""
    raw = date_str[:-1] if date_str.endswith("Z") else date_str
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"unparseable competition date: {date_str!r}")
    return parsed.replace(tzinfo=timezone.utc).astimezone(_EASTERN)


def _team_row(
    idx: int,
    teams: list[dict[str, Any]],
    competitors: list[dict[str, Any]],
    game_cols: dict[str, Any],
) -> dict[str, Any]:
    """One team's row in the R output column order (meta, stats, opponent)."""
    team: dict[str, Any] = teams[idx].get("team") or {}
    opp: dict[str, Any] = teams[1 - idx].get("team") or {}
    team_id = _to_int(team.get("id"))
    # R ifelse chain: the index-aligned competitor when ids match, else the other.
    # An unparseable id on either side is NA in R -> ifelse(NA, ...) -> NA fields.
    aligned, fallback = competitors[idx], competitors[1 - idx]
    aligned_id = _to_int(aligned.get("id"))
    if team_id is None or aligned_id is None:
        picked, unpicked = {}, {}
    elif aligned_id == team_id:
        picked, unpicked = aligned, fallback
    else:
        picked, unpicked = fallback, aligned

    row: dict[str, Any] = dict(game_cols)
    row["team_id"] = team_id
    for espn_key, suffix in _TEAM_META:
        row[f"team_{suffix}"] = team.get(espn_key)
    row["team_home_away"] = picked.get("homeAway")
    row["team_score"] = _to_int(picked.get("score"))
    row["team_winner"] = picked.get("winner")

    # R tidyr::spread orders stat columns alphabetically by their ESPN name.
    wide = {s["name"]: s.get("displayValue") for s in (teams[idx].get("statistics") or []) if s.get("name")}
    for name in sorted(wide):
        val = wide[name]
        if name in _SPLIT_STATS:
            made_key, att_key = _SPLIT_STATS[name]
            made, dash, attempted = (val or "").partition("-")
            row[underscore(made_key)] = made if val is not None else None
            row[underscore(att_key)] = attempted if dash else None
        else:
            row[underscore(name)] = val

    row["opponent_team_id"] = _to_int(opp.get("id"))
    for espn_key, suffix in _TEAM_META:
        row[f"opponent_team_{suffix}"] = opp.get(espn_key)
    row["opponent_team_score"] = _to_int(unpicked.get("score"))
    return row


def helper_wbb_team_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released team-box frame.

    Faithful polars port of ``wehoop:::helper_espn_wbb_team_box``
    (``wehoop/R/espn_wbb_data.R:3167``). Returns two rows (one per team) whose
    column set, order, and dtypes match the R-released
    ``espn_womens_college_basketball_team_boxscores`` parquet.

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``wehoop-wbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: Two team rows. Empty (zero-column) frame when the
        payload has no available boxscore -- the schema is payload-driven
        (stat columns come from ESPN's per-game statistics list), so no
        fixed empty schema is imposed; season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_team_box
            final = json.load(open("401700473.json", encoding="utf-8"))
            df = helper_wbb_team_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("team_display_name", "team_score", "team_winner")

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    header = final.get("header") or {}
    competitions = header.get("competitions") or []
    if not competitions:
        return pl.DataFrame()
    comp = competitions[0]
    # ESPN's header `boxscoreAvailable` flag is unreliable for archival games
    # (pre-2014 WBB payloads carry full team statistics while the flag says
    # false), so availability is derived from the payload itself -- the
    # teams/statistics checks below are the real gate. This deliberately
    # diverges from the R helper's original flag gate (fixed the same way in
    # wehoop); matching it would drop ~97% of extractable 2006-2013 boxscores.
    teams = (final.get("boxscore") or {}).get("teams") or []
    competitors = comp.get("competitors") or []
    if len(teams) < 2 or len(competitors) < 2 or not teams[0].get("statistics"):
        return pl.DataFrame()
    # Degenerate payloads the R producer hard-errors on (tryCatch -> game skipped):
    # an empty second statistics list, duplicate stat names (spread duplicate-id
    # error; a dict would silently last-win), and `winner` absent from BOTH
    # competitors (jsonlite drops the column -> ifelse length-zero error).
    if not teams[1].get("statistics"):
        return pl.DataFrame()
    for t in teams[:2]:
        names = [s.get("name") for s in t.get("statistics") or [] if s.get("name")]
        if len(names) != len(set(names)):
            return pl.DataFrame()
    if all("winner" not in c for c in competitors[:2]):
        return pl.DataFrame()

    game_date_time = _game_datetime(comp["date"])
    season = header.get("season") or {}
    game_cols: dict[str, Any] = {
        "game_id": _to_int(header.get("id")),
        "season": season.get("year"),
        "season_type": season.get("type"),
        "game_date": game_date_time.date(),
        "game_date_time": game_date_time,
    }

    rows = [_team_row(0, teams, competitors, game_cols), _team_row(1, teams, competitors, game_cols)]
    # R bind_rows column union: row-1 order first, row-2-only columns appended.
    cols = list(rows[0])
    cols += [c for c in rows[1] if c not in rows[0]]
    # R tidyr::separate errors when a combined "M-A" stat is missing from the
    # bound frame entirely (absent for BOTH teams) -> tryCatch -> game skipped.
    required = [underscore(k) for pair in _SPLIT_STATS.values() for k in pair]
    if not all(c in cols for c in required):
        return pl.DataFrame()
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in cols})

    int_cols = [c for c in (*_INT32_META, *_INT32_STATS) if c in df.columns]
    float_cols = [c for c in _FLOAT_STATS if c in df.columns]
    return df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in int_cols]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in float_cols]
        + [pl.col("game_date_time").cast(pl.Datetime("us", "America/New_York"))]
    )
