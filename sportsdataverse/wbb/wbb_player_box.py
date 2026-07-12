"""ESPN WBB player-box producer -- polars port of ``wehoop:::helper_espn_wbb_player_box``.

Source: ``wehoop/R/espn_wbb_data.R`` lines 3481-3883 (wehoop 3.0.0). Takes one
game's ESPN summary (``final.json``) payload and returns the tidy per-athlete
box frame published to the ``espn_womens_college_basketball_player_boxscores``
release. The R-released parquet is the parity oracle; the helper ends in a
canonical ``dplyr::select`` so column order is deterministic (payload-
independent), and dtypes mirror the release (Int32 counts, Float64 minutes).
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.wbb.wbb_team_box import _SPLIT_STATS, _game_datetime, _to_int

# R: dplyr::across(any_of(...), as.integer) on the pivoted stat columns.
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
    "rebounds",
    "total_technical_fouls",
    "total_turnovers",
    "turnovers",
    "points",
)
# R: dplyr::across(any_of(...), as.numeric); pcts are computed then dropped by
# the final select, so only minutes survives to the release.
_FLOAT_STATS: tuple[str, ...] = (
    "minutes",
    "field_goal_pct",
    "free_throw_pct",
    "three_point_field_goal_pct",
)
_INT32_META: tuple[str, ...] = (
    "game_id",
    "season",
    "season_type",
    "athlete_id",
    "team_id",
    "team_score",
    "opponent_team_id",
    "opponent_team_score",
)

# The canonical final dplyr::select(any_of(...)) order (R lines 3807-3865).
_FINAL_ORDER: tuple[str, ...] = (
    "game_id",
    "season",
    "season_type",
    "game_date",
    "game_date_time",
    "athlete_id",
    "athlete_display_name",
    "team_id",
    "team_name",
    "team_location",
    "team_short_display_name",
    "minutes",
    "field_goals_made",
    "field_goals_attempted",
    "three_point_field_goals_made",
    "three_point_field_goals_attempted",
    "free_throws_made",
    "free_throws_attempted",
    "offensive_rebounds",
    "defensive_rebounds",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "fouls",
    "points",
    "starter",
    "ejected",
    "did_not_play",
    "reason",
    "active",
    "athlete_jersey",
    "athlete_short_name",
    "athlete_headshot_href",
    "athlete_position_name",
    "athlete_position_abbreviation",
    "team_display_name",
    "team_uid",
    "team_slug",
    "team_logo",
    "team_abbreviation",
    "team_color",
    "team_alternate_color",
    "home_away",
    "team_winner",
    "team_score",
    "opponent_team_id",
    "opponent_team_name",
    "opponent_team_location",
    "opponent_team_display_name",
    "opponent_team_abbreviation",
    "opponent_team_logo",
    "opponent_team_color",
    "opponent_team_alternate_color",
    "opponent_team_score",
)

__all__ = ["helper_wbb_player_box"]


def _stat_dict(stat_cols: list[str], stats: list[Any]) -> dict[str, Any]:
    """Pivot one athlete's stats vector to snake columns, splitting "M-A" pairs."""
    out: dict[str, Any] = {}
    for name, val in zip(stat_cols, stats):
        if name in _SPLIT_STATS:
            made_key, att_key = _SPLIT_STATS[name]
            made, dash, attempted = (str(val) if val is not None else "").partition("-")
            out[underscore(made_key)] = made if val is not None else None
            out[underscore(att_key)] = attempted if dash else None
        else:
            out[underscore(name)] = val
    return out


def helper_wbb_player_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released player-box frame.

    Faithful polars port of ``wehoop:::helper_espn_wbb_player_box``
    (``wehoop/R/espn_wbb_data.R:3481``). Returns one row per athlete (DNP
    athletes included with null stats), sorted away-then-home, whose column
    set, order, and dtypes match the R-released
    ``espn_womens_college_basketball_player_boxscores`` parquet.

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``wehoop-wbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per athlete. Empty (zero-column) frame when the
        boxscore is unavailable or fails R's validity probes (the outcome the
        R producer's tryCatch-skip yields) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_player_box
            final = json.load(open("401700473.json", encoding="utf-8"))
            df = helper_wbb_player_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points")

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    header = final.get("header") or {}
    competitions = header.get("competitions") or []
    if not competitions:
        return pl.DataFrame()
    comp = competitions[0]
    if comp.get("boxscoreAvailable") != True:  # noqa: E712 -- R: boxScoreAvailable == TRUE
        return pl.DataFrame()
    team_blocks = (final.get("boxscore") or {}).get("players") or []
    competitors = comp.get("competitors") or []
    if len(team_blocks) < 2 or len(competitors) < 2:
        return pl.DataFrame()

    # R validity probes (lines 3509-3533): both teams' athletes are real
    # frames, the first athlete's stats vector is non-trivial, and its 7th
    # entry (rebounds) parses numeric. Failure -> producer skips the game.
    try:
        stats_block_0 = (team_blocks[0].get("statistics") or [])[0]
        stats_block_1 = (team_blocks[1].get("statistics") or [])[0]
        athletes_0 = stats_block_0.get("athletes") or []
        athletes_1 = stats_block_1.get("athletes") or []
        if not athletes_0 or not athletes_1:
            return pl.DataFrame()
        first_stats = athletes_0[0].get("stats") or []
        if len(first_stats) <= 1:
            return pl.DataFrame()
        float(first_stats[6])
    except (IndexError, TypeError, ValueError):
        return pl.DataFrame()

    stat_cols: list[str] = list(stats_block_0.get("keys") or [])
    game_datetime = _game_datetime(comp["date"])
    season = header.get("season") or {}
    game_cols: dict[str, Any] = {
        "game_id": _to_int(header.get("id")),
        "season": season.get("year"),
        "season_type": season.get("type"),
        "game_date": game_datetime.date(),
        "game_date_time": game_datetime,
    }

    c0, c1 = competitors[0], competitors[1]
    c0_id = _to_int(c0.get("id"))

    def _side_cols(team_id: int | None) -> dict[str, Any]:
        # R ifelse chain on team_id == homeAway1_team.id; NA id -> NA fields.
        if team_id is None or c0_id is None:
            mine: dict[str, Any] = {}
            opp: dict[str, Any] = {}
        elif team_id == c0_id:
            mine, opp = c0, c1
        else:
            mine, opp = c1, c0
        opp_team = opp.get("team") or {}
        opp_logos = opp_team.get("logos") or []
        return {
            "home_away": mine.get("homeAway"),
            "team_winner": mine.get("winner"),
            "team_score": _to_int(mine.get("score")),
            "opponent_team_id": _to_int(opp.get("id")),
            "opponent_team_name": opp_team.get("name"),
            "opponent_team_location": opp_team.get("location"),
            "opponent_team_display_name": opp_team.get("displayName"),
            "opponent_team_abbreviation": opp_team.get("abbreviation"),
            "opponent_team_logo": (opp_logos[0] or {}).get("href") if opp_logos else None,
            "opponent_team_color": opp_team.get("color"),
            "opponent_team_alternate_color": opp_team.get("alternateColor"),
            "opponent_team_score": _to_int(opp.get("score")),
        }

    def _meta_row(entry: dict[str, Any], team: dict[str, Any]) -> dict[str, Any]:
        ath = entry.get("athlete") or {}
        team_id = _to_int(team.get("id"))
        row: dict[str, Any] = dict(game_cols)
        row.update(
            {
                "athlete_id": _to_int(ath.get("id")),
                "athlete_display_name": ath.get("displayName"),
                "team_id": team_id,
                "team_name": team.get("name"),
                "team_location": team.get("location"),
                "team_short_display_name": team.get("shortDisplayName"),
                "starter": entry.get("starter"),
                "ejected": entry.get("ejected"),
                "did_not_play": entry.get("didNotPlay"),
                "active": entry.get("active"),
                "athlete_jersey": ath.get("jersey"),
                "athlete_short_name": ath.get("shortName"),
                "athlete_headshot_href": (ath.get("headshot") or {}).get("href"),
                "athlete_position_name": (ath.get("position") or {}).get("name"),
                "athlete_position_abbreviation": (ath.get("position") or {}).get("abbreviation"),
                "team_display_name": team.get("displayName"),
                "team_uid": team.get("uid"),
                "team_slug": team.get("slug"),
                "team_logo": team.get("logo"),
                "team_abbreviation": team.get("abbreviation"),
                "team_color": team.get("color"),
                "team_alternate_color": team.get("alternateColor"),
            }
        )
        if "reason" in entry:
            row["reason"] = entry.get("reason")
        row.update(_side_cols(team_id))
        return row

    # R: unnest keeps team-1 athletes then team-2; stats matrix is built from
    # the non-empty stats vectors and positionally bound to the !didNotPlay
    # rows (rbind drops zero-length vectors). A count mismatch is the case
    # where R's bind_cols errors -> tryCatch skips the game.
    entries = [
        (entry, block.get("team") or {})
        for block in team_blocks[:2]
        for entry in ((block.get("statistics") or [{}])[0].get("athletes") or [])
    ]
    stats_vectors = [e.get("stats") for e, _ in entries if e.get("stats")]
    played = [(e, t) for e, t in entries if e.get("didNotPlay") == False]  # noqa: E712
    dnp = [(e, t) for e, t in entries if e.get("didNotPlay") == True]  # noqa: E712
    # R producer contract: a stats/athletes count mismatch (bind_cols error), a
    # ragged stats vector (rbind recycling warning), or missing keys (colnames
    # error) all tryCatch-skip the game.
    if not stat_cols or len(stats_vectors) != len(played) or any(len(s) != len(stat_cols) for s in stats_vectors):
        return pl.DataFrame()

    rows: list[dict[str, Any]] = []
    for (entry, team), stats in zip(played, stats_vectors):
        row = _meta_row(entry, team)
        row.update(_stat_dict(stat_cols, stats))
        rows.append(row)
    rows.extend(_meta_row(entry, team) for entry, team in dnp)
    if not rows:
        return pl.DataFrame()

    present = {k for r in rows for k in r}
    cols = [c for c in _FINAL_ORDER if c in present]
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in cols}, strict=False)
    df = df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in (*_INT32_META, *_INT32_STATS) if c in df.columns]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in _FLOAT_STATS if c in df.columns]
        + [pl.col("game_date_time").cast(pl.Datetime("us", "America/New_York"))]
    )
    # R: dplyr::arrange(home_away) -- stable, NA last.
    return df.sort("home_away", maintain_order=True, nulls_last=True)
