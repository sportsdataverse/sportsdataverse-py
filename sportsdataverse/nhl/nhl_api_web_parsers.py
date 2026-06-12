"""sportsdataverse.nhl.nhl_api_web_parsers — polars parsers for the modern
NHL game-feed API at ``api-web.nhle.com/v1/``.

**Documentation**:

* NHL api-web parser deep-dive: https://py.sportsdataverse.org/docs/nhl/api-web
* Parsers overview: https://py.sportsdataverse.org/docs/parsers/
* Reusable patterns: https://py.sportsdataverse.org/docs/architecture/building-blocks

Each ``nhl_web_*`` wrapper in :mod:`sportsdataverse.nhl.nhl_api_web` ships
a different payload shape — game-center deep dives carry per-team player
arrays, schedules nest day → games, leaderboards key by stat-category, and
the right-rail endpoint exposes 8+ independent sub-frames. This module
mirrors the design of :mod:`sportsdataverse._common_espn_parsers`:

* Every parser returns ``polars.DataFrame`` by default; pass
  ``return_as_pandas=True`` for pandas.
* Empty / malformed payloads return a zero-row frame instead of raising.
* Output columns are snake-cased via
  :func:`sportsdataverse.dl_utils.underscore`.
* List-valued cells are stringified so polars accepts the frame.

Parsers fall into three groups:

1. **Game-center**:  :func:`parse_nhl_web_pbp`,
   :func:`parse_nhl_web_boxscore`, :func:`parse_nhl_web_landing`,
   :func:`parse_nhl_web_right_rail` (dispatcher returning 6 sub-frames).
2. **Schedule / score**: :func:`parse_nhl_web_schedule`,
   :func:`parse_nhl_web_score`, :func:`parse_nhl_web_scoreboard`,
   :func:`parse_nhl_web_club_schedule`.
3. **Team / player / standings / leaders / draft**:
   :func:`parse_nhl_web_standings`, :func:`parse_nhl_web_standings_season`,
   :func:`parse_nhl_web_club_stats` (dispatcher returning skaters +
   goalies), :func:`parse_nhl_web_roster` (merges 3 position groups),
   :func:`parse_nhl_web_player_landing`,
   :func:`parse_nhl_web_player_game_log`, :func:`parse_nhl_web_leaders`,
   :func:`parse_nhl_web_draft_picks`.
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

# ---------------------------------------------------------------------------
# Helpers (mirror _common_espn_parsers / mlb_api_parsers conventions)
# ---------------------------------------------------------------------------


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    return df


def _to_output(df: pd.DataFrame, return_as_pandas: bool):
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _empty_frame(return_as_pandas: bool = False):
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


def _flatten_rows(items, return_as_pandas: bool):
    """``pd.json_normalize`` a list of dicts → tidy frame. Zero-row frame
    on empty input. Stringifies list-valued cells for polars ingestion."""
    if not isinstance(items, list) or not items:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(items, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def _single_row(payload_dict: Dict, return_as_pandas: bool):
    """Flatten a dict to a single-row frame; zero-row on empty/non-dict."""
    if not isinstance(payload_dict, dict) or not payload_dict:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(payload_dict, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ---------------------------------------------------------------------------
# 1. Game-center
# ---------------------------------------------------------------------------


def parse_nhl_web_pbp(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_pbp()`` into one row per play.

    Walks ``payload["plays"]`` (~330 plays per game) and flattens each
    play's nested ``periodDescriptor`` / ``details`` sub-dicts. The PBP
    feed identifies plays by ``eventId`` + ``sortOrder`` and keys event
    types via ``typeCode`` / ``typeDescKey``.
    """
    plays = (payload or {}).get("plays")
    return _flatten_rows(plays, return_as_pandas)


def parse_nhl_web_boxscore(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_boxscore()`` into one row per (team × player).

    Boxscore ships ``playerByGameStats: {awayTeam: {forwards, defense,
    goalies}, homeTeam: {forwards, defense, goalies}}``. This parser
    walks all six (team × position-group) buckets and tags each row
    with ``home_away`` ("home" / "away") and ``position_group``
    ("forwards" / "defense" / "goalies") so the output is one tidy
    long-form frame.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    by_team = payload.get("playerByGameStats") or {}
    rows: List[dict] = []
    for side in ("awayTeam", "homeTeam"):
        team_block = by_team.get(side) or {}
        ha = "away" if side == "awayTeam" else "home"
        for pos_group in ("forwards", "defense", "goalies"):
            for player in team_block.get(pos_group) or []:
                row = {"home_away": ha, "position_group": pos_group}
                for k, v in (player or {}).items():
                    row[k] = v
                rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


def parse_nhl_web_landing(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_landing()`` into a single-row game profile.

    The landing endpoint ships the game header (id, date, venue, teams,
    periodDescriptor, gameState, clock, plus a ``summary`` sub-dict with
    scoring / threeStars / penalties — those are stringified to keep the
    output one row per call).
    """
    return _single_row(payload, return_as_pandas)


def parse_nhl_web_right_rail(
    payload: Dict,
    section: str = None,
    return_as_pandas: bool = False,
):
    """Parse ``nhl_web_right_rail()`` — dispatcher with 6 sub-frames.

    The right-rail endpoint ships game-context sub-frames typically
    rendered alongside the box-score on NHL.com:

    * ``season_series``    — list of head-to-head games (~7 rows)
    * ``shots_by_period``  — per-period shot totals (3 rows)
    * ``team_game_stats``  — per-category team-vs-team stat comparison
                             (~10 rows; one row per category)
    * ``game_info``        — single-row game-info dict (referees,
                             linesmen, awayTeam, homeTeam fields)
    * ``linescore_by_period`` — per-period score breakdown
    * ``season_series_wins`` — single-row aggregate of series wins

    With ``section=None`` (default), returns a dict of all 6 sub-frames
    keyed by section name. With ``section="<name>"``, returns just that
    one frame.
    """
    sub_parsers = {
        "season_series": lambda p: _flatten_rows(p.get("seasonSeries"), return_as_pandas),
        "shots_by_period": lambda p: _flatten_rows(p.get("shotsByPeriod"), return_as_pandas),
        "team_game_stats": lambda p: _flatten_rows(p.get("teamGameStats"), return_as_pandas),
        "game_info": lambda p: _single_row(p.get("gameInfo"), return_as_pandas),
        "linescore_by_period": lambda p: _flatten_rows(((p.get("linescore") or {}).get("byPeriod")), return_as_pandas),
        "season_series_wins": lambda p: _single_row(p.get("seasonSeriesWins"), return_as_pandas),
    }
    payload = payload if isinstance(payload, dict) else {}
    if section is not None:
        if section not in sub_parsers:
            raise ValueError(
                f"Unknown right_rail section {section!r}. "
                f"Choose one of {sorted(sub_parsers)} or pass "
                f"section=None for the full dict.",
            )
        return sub_parsers[section](payload)
    return {name: fn(payload) for name, fn in sub_parsers.items()}


# ---------------------------------------------------------------------------
# 2. Schedule / score / scoreboard
# ---------------------------------------------------------------------------


def parse_nhl_web_schedule(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_schedule()`` into one row per scheduled game.

    Input: ``{gameWeek: [{date, dayAbbrev, numberOfGames, games: [...]},
    ...], ...}``. Walks every ``gameWeek[].games[]`` and prefixes the
    day's ``date`` onto each game row.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    week = payload.get("gameWeek") or []
    rows = []
    for day in week:
        date_str = (day or {}).get("date")
        for game in day.get("games") or []:
            row = {"schedule_date": date_str}
            for k, v in (game or {}).items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


def parse_nhl_web_score(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_score()`` into one row per game for the date.

    Shape: ``{currentDate, games: [...], gameWeek: [...]}``.
    Returns the ``games`` array flattened.
    """
    return _flatten_rows((payload or {}).get("games"), return_as_pandas)


def parse_nhl_web_scoreboard(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_scoreboard()`` into one row per game across days.

    Shape: ``{focusedDate, gamesByDate: [{date, games: [...]}, ...]}``.
    Walks every ``gamesByDate[].games[]`` and prefixes the day's
    ``date`` as ``scoreboard_date``.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    by_date = payload.get("gamesByDate") or []
    rows = []
    for day in by_date:
        date_str = (day or {}).get("date")
        for game in day.get("games") or []:
            row = {"scoreboard_date": date_str}
            for k, v in (game or {}).items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


def parse_nhl_web_club_schedule(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_club_schedule_season()`` / ``_month`` / ``_week``
    into one row per game.

    All three club-schedule endpoints share the ``{games: [...]}``
    payload shape plus a few context fields (``currentSeason``,
    ``previousSeason``, ``clubTimezone``). The context fields are
    prefixed onto each row as ``club_*`` columns.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    games = payload.get("games") or []
    ctx = {
        "club_previous_season": payload.get("previousSeason"),
        "club_current_season": payload.get("currentSeason"),
        "club_next_season": payload.get("nextSeason"),
        "club_timezone": payload.get("clubTimezone"),
    }
    rows = []
    for game in games:
        row = dict(ctx)
        for k, v in (game or {}).items():
            row[k] = v
        rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


# ---------------------------------------------------------------------------
# 3. Standings
# ---------------------------------------------------------------------------


def parse_nhl_web_standings(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_standings()`` into one row per team."""
    return _flatten_rows((payload or {}).get("standings"), return_as_pandas)


def parse_nhl_web_standings_season(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_standings_season()`` into one row per season."""
    return _flatten_rows((payload or {}).get("seasons"), return_as_pandas)


# ---------------------------------------------------------------------------
# 4. Team / player surfaces
# ---------------------------------------------------------------------------


def parse_nhl_web_club_stats(
    payload: Dict,
    section: str = None,
    return_as_pandas: bool = False,
):
    """Parse ``nhl_web_club_stats()`` — dispatcher with skaters + goalies.

    Returns a dict ``{skaters: <frame>, goalies: <frame>}`` by default,
    or a single frame when ``section="skaters"`` / ``"goalies"``.
    """
    if not isinstance(payload, dict):
        if section is not None:
            return _empty_frame(return_as_pandas)
        return {"skaters": _empty_frame(return_as_pandas), "goalies": _empty_frame(return_as_pandas)}
    sub_parsers = {
        "skaters": lambda: _flatten_rows(payload.get("skaters"), return_as_pandas),
        "goalies": lambda: _flatten_rows(payload.get("goalies"), return_as_pandas),
    }
    if section is not None:
        if section not in sub_parsers:
            raise ValueError(
                f"Unknown club_stats section {section!r}. Choose 'skaters' or 'goalies' or pass section=None.",
            )
        return sub_parsers[section]()
    return {name: fn() for name, fn in sub_parsers.items()}


def parse_nhl_web_roster(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_roster()`` into one row per player.

    Shape: ``{forwards: [...], defensemen: [...], goalies: [...]}``.
    Merges all three position groups with a ``position_group`` column
    so the output is one long-form frame instead of three.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows = []
    for pos_group in ("forwards", "defensemen", "goalies"):
        for player in payload.get(pos_group) or []:
            row = {"position_group": pos_group}
            for k, v in (player or {}).items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


def parse_nhl_web_player_landing(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_player_landing()`` into a single-row player
    profile. Nested ``featuredStats`` / ``careerTotals`` / ``last5Games``
    sub-frames are stringified — call them out separately if needed.
    """
    return _single_row(payload, return_as_pandas)


def parse_nhl_web_player_game_log(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_player_game_log()`` into one row per game.

    Walks ``payload["gameLog"]`` (~76 games per season for a regular
    skater) and flattens.
    """
    return _flatten_rows((payload or {}).get("gameLog"), return_as_pandas)


# ---------------------------------------------------------------------------
# 5. Leaders + draft
# ---------------------------------------------------------------------------


def parse_nhl_web_leaders(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_skater_leaders()`` / ``nhl_web_goalie_leaders()``
    into one row per (category × player).

    The leaders payloads are keyed by stat category at the top level —
    e.g. ``{points: [<10 player rows>], goals: [<10 player rows>], ...}``
    for skaters; ``{wins: [...], savePctg: [...]}`` for goalies. This
    parser walks every top-level list-valued key, tags each row with
    the ``category`` it came from, and concatenates.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows = []
    for category, players in payload.items():
        if not isinstance(players, list):
            continue
        for player in players:
            if not isinstance(player, dict):
                continue
            row = {"category": category}
            for k, v in player.items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


def parse_nhl_web_draft_picks(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_draft_picks()`` into one row per pick."""
    return _flatten_rows((payload or {}).get("picks"), return_as_pandas)


def parse_nhl_web_player_spotlight(payload, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_player_spotlight()`` into one row per featured player.

    The ``/v1/player-spotlight`` endpoint returns a *bare top-level JSON
    array* of currently-spotlighted players, so this parser flattens it directly.
    """
    if not isinstance(payload, list):
        return _empty_frame(return_as_pandas)
    return _flatten_rows(payload, return_as_pandas)


def parse_nhl_web_draft_rankings(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_draft_rankings()`` / ``_now()`` into one row per prospect.

    Input: ``{draftYear, categoryId, categoryKey, rankings: [...]}``. Each
    ``rankings[]`` row is flattened and prefixed with the draft-year / category
    context so a row carries both the prospect and which board it came from.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    base = {
        "draft_year": payload.get("draftYear"),
        "category_id": payload.get("categoryId"),
        "category_key": payload.get("categoryKey"),
    }
    rows = []
    for prospect in payload.get("rankings") or []:
        row = dict(base)
        for k, v in (prospect or {}).items():
            row[k] = v
        rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


def parse_nhl_web_playoff_series(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``nhl_web_playoff_series()`` into one row per series game.

    Input: ``{round, seriesLetter, topSeedTeam, bottomSeedTeam, games: [...]}``.
    Emits one row per game prefixed with series context (round, series letter,
    top/bottom seed team ids + abbrevs).
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    top = payload.get("topSeedTeam") or {}
    bottom = payload.get("bottomSeedTeam") or {}
    base = {
        "round": payload.get("round"),
        "series_letter": payload.get("seriesLetter"),
        "top_seed_team_id": top.get("id"),
        "top_seed_team_abbrev": top.get("abbrev"),
        "bottom_seed_team_id": bottom.get("id"),
        "bottom_seed_team_abbrev": bottom.get("abbrev"),
    }
    rows = []
    for game in payload.get("games") or []:
        row = dict(base)
        for k, v in (game or {}).items():
            row[k] = v
        rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


# Maps an nhl_web_* wrapper name to its parser. Dispatchers
# (``parse_nhl_web_right_rail``, ``parse_nhl_web_club_stats``) return a
# dict by default — callers wanting a single frame pass section=.
NHL_API_WEB_ENDPOINT_PARSERS = {
    # Game-center
    "nhl_web_pbp": parse_nhl_web_pbp,
    "nhl_boxscore": parse_nhl_web_boxscore,
    "nhl_landing": parse_nhl_web_landing,
    "nhl_right_rail": parse_nhl_web_right_rail,
    # Schedule / score / scoreboard
    "nhl_web_schedule": parse_nhl_web_schedule,
    "nhl_score": parse_nhl_web_score,
    "nhl_scoreboard": parse_nhl_web_scoreboard,
    "nhl_schedule_calendar": parse_nhl_web_schedule,
    "nhl_club_schedule_season": parse_nhl_web_club_schedule,
    "nhl_club_schedule_month": parse_nhl_web_club_schedule,
    "nhl_club_schedule_week": parse_nhl_web_club_schedule,
    # Standings
    "nhl_standings": parse_nhl_web_standings,
    "nhl_standings_season": parse_nhl_web_standings_season,
    # Team / player
    "nhl_club_stats": parse_nhl_web_club_stats,
    "nhl_club_stats_season": parse_nhl_web_club_stats,
    "nhl_roster": parse_nhl_web_roster,
    "nhl_roster_season": parse_nhl_web_roster,
    "nhl_player_landing": parse_nhl_web_player_landing,
    "nhl_player_game_log": parse_nhl_web_player_game_log,
    # Leaders
    "nhl_skater_leaders": parse_nhl_web_leaders,
    "nhl_goalie_leaders": parse_nhl_web_leaders,
    # Draft
    "nhl_draft_picks": parse_nhl_web_draft_picks,
    "nhl_draft_picks_now": parse_nhl_web_draft_picks,
    "nhl_draft_tracker_picks_now": parse_nhl_web_draft_picks,
    "nhl_draft_rankings": parse_nhl_web_draft_rankings,
    "nhl_draft_rankings_now": parse_nhl_web_draft_rankings,
    "nhl_player_spotlight": parse_nhl_web_player_spotlight,
    "nhl_playoff_series": parse_nhl_web_playoff_series,
}


def parser_for_nhl_api_web(fn_name: str):
    """Return the registered parser for an ``nhl_web_*`` wrapper.

    Returns ``None`` for endpoints without a registered parser (e.g.
    ``playoff_series``, ``player_spotlight``, ``draft_rankings``) since
    their payloads are too idiosyncratic for a useful generic fallback.
    Callers should null-check the result.

    Args:
        fn_name: The ``__name__`` of any ``nhl_web_*`` wrapper.

    Returns:
        Parser callable, or ``None`` if unregistered.
    """
    return NHL_API_WEB_ENDPOINT_PARSERS.get(fn_name)
