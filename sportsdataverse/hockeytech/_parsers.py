"""Pure HockeyTech parsers: parsed JSON (dict/list) -> snake_cased polars frame.

No network here -- tests drive these from captured fixtures. Every parser
tolerates empty/None payloads by returning a zero-row frame.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case and deduplicate by keeping the first occurrence."""
    seen: dict[str, int] = {}
    new_cols: list[str] = []
    for c in df.columns:
        snake = underscore(str(c)).replace(".", "_").replace("__", "_")
        if snake in seen:
            # suffix subsequent duplicates so polars doesn't raise on non-unique columns
            seen[snake] += 1
            snake = f"{snake}_{seen[snake]}"
        else:
            seen[snake] = 0
        new_cols.append(snake)
    df.columns = new_cols
    return df


def _to_frame(records: List[Dict[str, Any]], return_as_pandas: bool) -> Any:
    pdf = pd.json_normalize(records or [], sep="_")
    pdf = _snake_columns(pdf)
    for col in [c for c in pdf.columns if pd.api.types.is_object_dtype(pdf[c].dtype)]:
        # The feed sometimes ships one field as 0 in one row and "1" in the next (gamebygame
        # ``plus_minus``); pyarrow rejects such a column, so a mixed scalar column becomes strings.
        kinds = {type(v) for v in pdf[col] if isinstance(v, (int, float, str, bool))}
        if len(kinds) > 1 and str in kinds:
            pdf[col] = pdf[col].map(lambda v: str(v) if isinstance(v, (int, float, bool)) else v)
    if return_as_pandas:
        return pdf
    return pl.from_pandas(pdf) if len(pdf) else pl.DataFrame()


def _sitekit(payload: Any, key: str) -> Any:
    return ((payload or {}).get("SiteKit", {}) or {}).get(key)


def _derive_season_year(name: str) -> Optional[int]:
    """End year of a season name; the first rule that matches wins.

    1. ``YYYY-YYYY`` / ``YYYY-YY`` (``-`` or ``/``, spaces allowed): "2025-2026", "2025/26",
       "2026 - 27" -> 2026, 2026, 2027. A two-digit tail takes the start's century, +100 when
       that falls below the start ("1999-00" -> 2000).
    2. ``YY-ZZ`` with ZZ = YY + 1: "26-27 Regular Season" -> 2027.
    3. The first standalone 4-digit token: a year in 1950..(this year + 2) is itself; else a
       compact span ``YYZZ`` with ZZ = YY + 1 is its end year ("CCHL 2425 Special Events" -> 2025).

    A two-digit end year is 20ZZ, or 19ZZ when 20ZZ is past this year + 2. A result outside
    1950..(this year + 2), or a name none of the rules match ("19 Tie Break"), is None.
    """
    latest = date.today().year + 2

    def two_digit_end(zz: int) -> int:
        return 2000 + zz if 2000 + zz <= latest else 1900 + zz

    name = name or ""
    yr = None
    m = re.search(r"(\d{4})\s*[-/]\s*(\d{4}|\d{2})(?!\d)", name)
    short = re.search(r"(?<!\d)(\d{2})\s*[-/]\s*(\d{2})(?!\d)", name)
    token = re.search(r"(?<!\d)(\d{4})(?!\d)", name)
    if m:
        start, tail = int(m.group(1)), m.group(2)
        yr = int(tail) if len(tail) == 4 else (start // 100) * 100 + int(tail)
        yr = yr + 100 if yr < start else yr
    elif short and (int(short.group(1)) + 1) % 100 == int(short.group(2)):
        yr = two_digit_end(int(short.group(2)))
    elif token:
        t = int(token.group(1))
        if 1950 <= t <= latest:
            yr = t
        elif (t // 100 + 1) % 100 == t % 100:
            yr = two_digit_end(t % 100)
    return yr if yr is not None and 1950 <= yr <= latest else None


def _game_type_label(name: str) -> str:
    """Game type of a season name; the first case-insensitive match wins.

    ``pre[- ]?season`` -> preseason ("2025-26 Preseason Exhibition" stays a preseason),
    ``playoff|post`` -> playoffs, ``exhibition`` -> exhibition, anything else regular.
    """
    n = (name or "").lower()
    if re.search(r"pre[- ]?season", n):
        return "preseason"
    if re.search(r"playoff|post", n):
        return "playoffs"
    if "exhibition" in n:
        return "exhibition"
    return "regular"


# A name spanning two years ("2025-26", "2025/2026", "2026 - 27", "26-27"). Shared with
# resolve_season_id's ranking; no lookaround, so polars can run it too.
TWO_YEAR_NAME_RE = r"\d{2}\s*[-/]\s*\d{2}"


_SCOREBAR_RENAME = {
    "ID": "game_id",
    "GameDateISO8601": "game_date",
    "GameStatusStringLong": "game_status",
    "HomeLongName": "home_team",
    "HomeID": "home_team_id",
    "HomeGoals": "home_score",
    "VisitorLongName": "away_team",
    "VisitorID": "away_team_id",
    "VisitorGoals": "away_score",
    "venue_name": "venue",
    "SeasonID": "season_id",
    "game_type": "game_type",
}

# The same columns from ``modulekit/schedule``, the season-scoped view.
_SCHEDULE_RENAME = {
    "game_id": "game_id",
    "GameDateISO8601": "game_date",
    "game_status": "game_status",
    "home_team_name": "home_team",
    "home_team": "home_team_id",
    "home_goal_count": "home_score",
    "visiting_team_name": "away_team",
    "visiting_team": "away_team_id",
    "visiting_goal_count": "away_score",
    "venue_name": "venue",
    "season_id": "season_id",
    "game_type": "game_type",
}


def parse_schedule(payload: Any, return_as_pandas: bool = False, season_id: Optional[int] = None) -> Any:
    """Parse a HockeyTech ``modulekit/schedule`` or ``modulekit/scorebar`` JSON payload.

    One row per game. ``schedule`` (``SiteKit.Schedule``) is the season-scoped view the
    ``<league>_schedule`` functions call; a ``scorebar`` payload (``SiteKit.Scorebar``)
    maps onto the same columns. ``season_id`` keeps only that season's games: ``scorebar``
    ignores the ``season_id`` it is sent and returns every season in its date window.
    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True`` for a
    :class:`pandas.DataFrame`. An empty/None payload returns a zero-row frame of the same
    type, never raises.
    """
    games, rename = _sitekit(payload, "Schedule"), _SCHEDULE_RENAME
    if not games:
        games, rename = _sitekit(payload, "Scorebar") or [], _SCOREBAR_RENAME
    rows = [{new: g.get(old) for old, new in rename.items()} for g in games]
    if season_id is not None:
        rows = [r for r in rows if str(r["season_id"]) == str(season_id)]
    return _to_frame(rows, return_as_pandas)


def parse_standings(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/standings`` JSON payload into a flat frame.

    The payload is a top-level LIST of section dicts, each containing a
    ``sections`` list with ``data`` items whose ``row`` key holds the per-team
    stats dict. Returns a :class:`polars.DataFrame` by default; pass
    ``return_as_pandas=True`` for a :class:`pandas.DataFrame`. An empty/None
    payload returns a zero-row frame of the same type, never raises.
    """
    rows: List[Dict[str, Any]] = []
    sections = payload if isinstance(payload, list) else (payload or {}).get("sections", [])
    for sec in sections or []:
        if not isinstance(sec, dict):
            continue
        for blk in sec.get("sections", [sec]):
            for item in blk.get("data", []) or []:
                r = item.get("row") if isinstance(item, dict) else None
                if isinstance(r, dict):
                    rows.append(r)
    df = _to_frame(rows, False)
    # Rename raw keys -> required column names:
    # - "rank" -> "team_rank"  (standings position integer already present)
    # - "name" -> "team"       (long team name)
    # NOTE: "regulation_wins" is intentionally NOT renamed to "wins".
    # The PWHL uses a 3-2-1-0 points system; "wins" must be TOTAL wins
    # (regulation + non-regulation), not just regulation wins.
    ren = {
        "rank": "team_rank",
        "name": "team",
    }
    df = df.rename({k: v for k, v in ren.items() if k in df.columns})
    # Compute total wins = regulation_wins + non_reg_wins (both are integer strings)
    if "regulation_wins" in df.columns and "non_reg_wins" in df.columns:
        df = df.with_columns(
            (
                pl.col("regulation_wins").cast(pl.Int64, strict=False)
                + pl.col("non_reg_wins").cast(pl.Int64, strict=False)
            ).alias("wins")
        )
    return df.to_pandas() if return_as_pandas else df


def parse_teams(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/teamsbyseason`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.
    """
    raw = _sitekit(payload, "Teamsbyseason") or []
    rows: List[Dict[str, Any]] = []
    for t in raw:
        rows.append(
            {
                "team_name": t.get("name"),
                "team_id": t.get("id"),
                "team_code": t.get("code"),
                "team_nickname": t.get("nickname"),
                "team_label": t.get("city"),
                "division": t.get("division_id") or t.get("division"),
                "team_logo": t.get("team_logo_url") or t.get("logo"),
            }
        )
    return _to_frame(rows, return_as_pandas)


def parse_roster(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/roster`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.

    List-valued fields (e.g. ``draftinfo``) are dropped before normalization
    because ``pd.json_normalize`` cannot flatten bare lists.
    """
    raw = _sitekit(payload, "Roster") or []
    rows: List[Dict[str, Any]] = []
    for player in raw:
        if not isinstance(player, dict):
            # Intentionally skip non-dict entries (e.g. the coaching-staff sub-list);
            # the result frame contains players only.
            continue
        rows.append({k: v for k, v in player.items() if not isinstance(v, list)})
    return _to_frame(rows, return_as_pandas)


def _player(d: Any) -> dict:
    """Extract a flat player dict from a raw player sub-object (or None)."""
    d = d or {}
    return {
        "id": d.get("id"),
        "first": d.get("firstName"),
        "last": d.get("lastName"),
        "pos": d.get("position"),
    }


def _str_or_none(v: Any) -> Optional[str]:
    """Return str(v) if v is not None/empty, else None."""
    if v is None:
        return None
    s = str(v)
    return s if s else None


def _parse_pbp_a(events: List[Any], game_id: Any = None) -> List[Dict[str, Any]]:
    """Convert a list of raw HockeyTech dialect-a event dicts to flat row dicts."""
    rows: List[Dict[str, Any]] = []
    for e in events:
        if not isinstance(e, dict):
            continue
        ev = e.get("event")
        d = e.get("details") or {}

        # period is always a dict {"id": "1", ...} in dialect a
        period_raw = d.get("period")
        if isinstance(period_raw, dict):
            period = period_raw.get("id")
        else:
            period = period_raw

        # Normalize team_id to string to avoid mixed int/str column dtype
        raw_team = d.get("team_id") or d.get("shooterTeamId") or d.get("teamId")
        base: Dict[str, Any] = {
            "game_id": game_id,
            "event": ev,
            "team_id": _str_or_none(raw_team),
            "period_of_game": period,
            "time_of_period": d.get("time"),
            "x_coord": d.get("xLocation"),
            "y_coord": d.get("yLocation"),
            # defaults — overridden below per event type
            "player_id": None,
            "player_name_first": None,
            "player_name_last": None,
            "player_position": None,
            "goal": None,
            "is_goal_twin": False,
            "goalie_id": None,
            "goalie_first": None,
            "goalie_last": None,
        }

        if ev in ("shot", "blocked_shot"):
            sh = _player(d.get("shooter"))
            gl = _player(d.get("goalie"))
            base.update(
                {
                    "player_id": sh["id"],
                    "player_name_first": sh["first"],
                    "player_name_last": sh["last"],
                    "player_position": sh["pos"],
                    "player_team_id": d.get("shooterTeamId"),
                    "event_type": d.get("shotType"),
                    "shot_quality": d.get("shotQuality"),
                    "goal": bool(d.get("isGoal")) if ev == "shot" else False,
                    # The feed emits BOTH a shot row (isGoal=true) and a goal row
                    # for every goal; flag the shot twin so callers can dedupe.
                    "is_goal_twin": bool(d.get("isGoal")) if ev == "shot" else False,
                    "goalie_id": gl["id"],
                    "goalie_first": gl["first"],
                    "goalie_last": gl["last"],
                }
            )
        elif ev == "goal":
            sc = _player(d.get("scoredBy"))
            assists = d.get("assists") or []
            props = d.get("properties") or {}
            team_id = _str_or_none((d.get("team") or {}).get("id")) or base["team_id"]
            base.update(
                {
                    "player_id": sc["id"],
                    "player_name_first": sc["first"],
                    "player_name_last": sc["last"],
                    "player_position": sc["pos"],
                    "team_id": team_id,
                    "goal": True,
                    "empty_net": props.get("isEmptyNet"),
                    "game_winner": props.get("isGameWinningGoal"),
                    "penalty_shot": props.get("isPenaltyShot"),
                    "insurance": props.get("isInsuranceGoal"),
                    "short_handed": props.get("isShortHanded"),
                    "power_play": props.get("isPowerPlay"),
                }
            )
            # Fix 3: assist player names AND positions (player_two = primary, player_three = secondary)
            assist_names = ["two", "three"]
            for i, a in enumerate(assists[:2]):
                pa = _player(a)
                base[f"player_{assist_names[i]}_id"] = pa["id"]
                base[f"player_{assist_names[i]}_name_first"] = pa["first"]
                base[f"player_{assist_names[i]}_name_last"] = pa["last"]
                base[f"player_{assist_names[i]}_position"] = pa["pos"]
            # Fix 1 & 2: word ordinals, include _position for each plus/minus slot, cap at 5
            _ORDINALS = ["one", "two", "three", "four", "five"]
            for sign, key in (("plus", "plus_players"), ("minus", "minus_players")):
                for j, p in enumerate((d.get(key) or [])[:5]):
                    ord_name = _ORDINALS[j]
                    pp = _player(p)
                    base[f"{sign}_player_{ord_name}_id"] = pp["id"]
                    base[f"{sign}_player_{ord_name}_first"] = pp["first"]
                    base[f"{sign}_player_{ord_name}_last"] = pp["last"]
                    base[f"{sign}_player_{ord_name}_position"] = pp["pos"]
        elif ev == "faceoff":
            hp = _player(d.get("homePlayer"))
            base.update(
                {
                    "player_id": hp["id"],
                    "player_name_first": hp["first"],
                    "player_name_last": hp["last"],
                    "player_position": hp["pos"],
                    "home_win": d.get("homeWin"),
                }
            )
        elif ev == "hit":
            pl_info = _player(d.get("player"))
            base.update(
                {
                    "player_id": pl_info["id"],
                    "player_name_first": pl_info["first"],
                    "player_name_last": pl_info["last"],
                    "player_position": pl_info["pos"],
                    "team_id": _str_or_none(d.get("teamId")) or base["team_id"],
                }
            )
        elif ev == "penalty":
            # Fix 5: match fastRhockey -- servedBy is primary (player_id), takenBy is secondary (player_two_*)
            sb = _player(d.get("servedBy"))
            tb = _player(d.get("takenBy"))
            against = d.get("againstTeam") or {}
            base.update(
                {
                    "player_id": sb["id"],
                    "player_name_first": sb["first"],
                    "player_name_last": sb["last"],
                    "player_position": sb["pos"],
                    "player_two_id": tb["id"],
                    "player_two_name_first": tb["first"],
                    "player_two_name_last": tb["last"],
                    "player_two_position": tb["pos"],
                    "team_id": _str_or_none(against.get("id")),
                    "penalty_length": d.get("minutes"),
                    "event_type": d.get("description"),
                    "power_play": "1" if d.get("isPowerPlay") else "0",
                }
            )
        elif ev == "goalie_change":
            gc = _player(d.get("goalieComingIn"))
            base.update(
                {
                    "goalie_id": gc["id"],
                    "goalie_first": gc["first"],
                    "goalie_last": gc["last"],
                }
            )

        rows.append(base)
    return rows


def _parse_pbp_b(events: List[Any], game_id: Any = None) -> List[Dict[str, Any]]:
    """Convert a list of raw HockeyTech dialect-b event dicts to flat row dicts.

    Dialect b covers OHL, WHL, and QMJHL.  Despite the league registry marking
    these as ``"hockeytech_b"``, live API inspection (2026-06-09) confirmed that
    these leagues emit the **same** nested ``{"event": ..., "details": {...}}``
    structure as dialect-a (PWHL/AHL) -- just served via their own
    ``client_code`` / ``api_key`` / host.

    Event-type coverage vs dialect a:

    - ``goalie_change`` -- identical keys (``goalieComingIn``, ``goalieGoingOut``,
      ``team_id``, ``period``, ``time``).
    - ``faceoff`` -- identical keys (``homePlayer``, ``visitingPlayer``,
      ``xLocation``, ``yLocation``, ``homeWin``).
    - ``shot`` -- identical keys (``shooter``, ``goalie``, ``shooterTeamId``,
      ``isGoal``, ``shotType``, ``shotQuality``, ``xLocation``, ``yLocation``).
    - ``penalty`` -- identical keys (``takenBy``, ``servedBy``, ``againstTeam``,
      ``minutes``, ``description``, ``isPowerPlay``); additionally carries
      ``isBench`` (junior leagues).
    - ``goal`` -- identical keys (``scoredBy``, ``assists``, ``properties``,
      ``plus_players``, ``minus_players``, ``xLocation``, ``yLocation``).
    - ``hit`` -- identical keys (``player``, ``teamId``); may appear in some
      junior games.
    - ``blocked_shot`` -- identical keys; may appear in some junior games.
    - ``timeout`` -- junior-specific event with keys ``teamId``,
      ``timeoutLength``, ``callingCoach``; kept with raw event name
      ``"timeout"`` (no dialect-a equivalent).

    All events are processed through :func:`_parse_pbp_a` since the wire format
    is identical.  The ``"timeout"`` event type is handled by the default
    (identity) branch in ``_parse_pbp_a``, which emits a row with
    ``event="timeout"`` and ``player_id=None`` -- consistent with how
    ``_parse_pbp_a`` handles unrecognised event types.
    """
    return _parse_pbp_a(events, game_id=game_id)


def parse_pbp(
    payload: Any,
    pbp_style: str = "hockeytech_a",
    game_id: Any = None,
    return_as_pandas: bool = False,
) -> Any:
    """Parse a HockeyTech play-by-play payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default (one row per event);
    pass ``return_as_pandas=True`` for a :class:`pandas.DataFrame`.
    An empty/None payload returns a zero-row frame, never raises.

    Goal double-rowing: the HockeyTech feed emits TWO rows for (nearly) every
    goal — the ``goal`` event itself plus a twin ``shot`` event for the same
    play (the feed's ``isGoal`` flag). The shot rows alone match the official
    boxscore shots-on-goal totals (goal twins included), so neither row is
    dropped; instead the twin shot row carries ``is_goal_twin = True``. For a
    deduplicated event stream (e.g. an xG training corpus), drop rows with
    ``is_goal_twin == True`` and keep the ``goal`` rows; for boxscore-consistent
    SOG counts, count the ``shot`` rows as-is.

    Args:
        payload: A list of event dicts as returned by the HockeyTech API
            ``gameCenterPlayByPlay`` endpoint.  Both dialect-a and
            dialect-b payloads are lists of ``{"event": ..., "details":
            {...}}`` dicts.
        pbp_style: Dialect flag.  ``"hockeytech_a"`` (PWHL/AHL) and
            ``"hockeytech_b"`` (OHL/WHL/QMJHL) are both supported.
            Dialect b uses the same nested wire format as dialect a; the
            distinction exists at the league-registry level (different
            client codes / API keys / hosts) but not in the JSON schema.
        game_id: Optional game identifier echoed onto every row as
            ``game_id``.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of a :class:`polars.DataFrame`.
    """
    events: List[Any] = payload if isinstance(payload, list) else []
    if pbp_style == "hockeytech_b":
        rows = _parse_pbp_b(events, game_id=game_id)
    else:
        rows = _parse_pbp_a(events, game_id=game_id)
    return _to_frame(rows, return_as_pandas)


def mmss_to_seconds(value: Any) -> Optional[int]:
    """Convert a ``'MM:SS'`` clock string to total seconds.

    Returns ``None`` for ``None`` or empty string inputs.  Countdown-clock
    values (e.g. ``'03:16'``) are converted exactly as supplied -- the caller
    is responsible for any inversion logic.

    Examples:
        Quick start::

            mmss_to_seconds("03:16")
            196
            mmss_to_seconds("00:00")
            0
            mmss_to_seconds(None) is None
            True
    """
    if value in (None, ""):
        return None
    try:
        m, s = str(value).split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return None


def parse_shifts(payload: Any, game_id: Any = None, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/gameshifts`` JSON payload into a flat frame.

    One row per player-shift stint.  Returns a :class:`polars.DataFrame` by
    default; pass ``return_as_pandas=True`` for a :class:`pandas.DataFrame`.
    An empty/None payload returns a zero-row frame of the same type, never
    raises.

    The shift clock is a **countdown** within the period (``start_time >
    end_time``).  ``start_s`` and ``end_s`` are the raw countdown values
    converted to seconds -- so ``start_s >= end_s`` for every row.

    Args:
        payload: The parsed JSON dict from the HockeyTech ``gameshifts``
            endpoint (keyed under ``SiteKit.Gameshifts``).
        game_id: Optional game identifier echoed onto every row as ``game_id``.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of a :class:`polars.DataFrame`.
    """
    gs = _sitekit(payload, "Gameshifts") or {}
    rows: List[Dict[str, Any]] = []
    for side in ("home", "visitor"):
        for player in gs.get(side, []) or []:
            for sh in player.get("shifts", []) or []:
                rows.append(
                    {
                        "game_id": game_id,
                        "player_id": player.get("player_id"),
                        "first_name": player.get("first_name"),
                        "last_name": player.get("last_name"),
                        "jersey_number": player.get("jersey_number"),
                        "home": int(player.get("home", 1 if side == "home" else 0)),
                        "period": int(sh.get("period")) if sh.get("period") else None,
                        "start_time": sh.get("start_time"),
                        "end_time": sh.get("end_time"),
                        "length": sh.get("length"),
                        "start_s": mmss_to_seconds(sh.get("start_time")),
                        "end_s": mmss_to_seconds(sh.get("end_time")),
                        "goal_on_shift": int(sh.get("goal_on_shift", 0) or 0),
                        "penalty_on_shift": int(sh.get("penalty_on_shift", 0) or 0),
                    }
                )
    return _to_frame(rows, return_as_pandas)


def parse_player_stats(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/player`` (seasonstats) JSON payload.

    ``SiteKit.Player`` is a dict with keys ``regular``, ``exhibition``, and
    ``playoff``, each holding a list of per-season stat rows.  All sub-lists
    are concatenated; a ``stat_type`` column is added to identify the source
    list.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.
    """
    player = _sitekit(payload, "Player") or {}
    rows: List[Dict[str, Any]] = []
    for stat_type in ("regular", "exhibition", "playoff"):
        sub = player.get(stat_type) if isinstance(player, dict) else None
        for season in sub or []:
            if not isinstance(season, dict):
                continue
            # Coerce all scalar values to str to avoid mixed int/str dtype errors
            # when the "Total" summary rows use numeric types for fields that are
            # otherwise string-valued (e.g. max_start_date, veteran_status).
            row: Dict[str, Any] = {
                k: (str(v) if v is not None and not isinstance(v, (list, dict)) else v) for k, v in season.items()
            }
            row["stat_type"] = stat_type
            rows.append(row)
    return _to_frame(rows, return_as_pandas)


def parse_leaders(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech leaders payload into a flat frame.

    Handles two payload shapes:

    1. ``SiteKit.Statviewtype`` -- a list of flat player dicts (the standard
       ``modulekit/statviewtype`` endpoint used by fastRhockey).
    2. A top-level ``skaters``/``goalies`` dict of stat-category objects, each
       carrying a ``results`` list (as seen in the captured fixture
       ``pwhl_leaders_5.json``).

    Returns a :class:`polars.DataFrame` by default. An empty/None payload or
    a fixture with empty result lists returns a zero-row frame without raising.
    """
    rows: List[Dict[str, Any]] = []

    # Shape 1: SiteKit.Statviewtype (standard modulekit endpoint)
    stat_view = _sitekit(payload, "Statviewtype")
    if stat_view is not None:
        for p in stat_view or []:
            if isinstance(p, dict):
                rows.append(p)
        return _to_frame(rows, return_as_pandas)

    # Shape 2: top-level skaters/goalies categories (leadersExtended-style)
    top = payload if isinstance(payload, dict) else {}
    for pos_key in ("skaters", "goalies"):
        pos_data = top.get(pos_key)
        if not isinstance(pos_data, dict):
            continue
        for cat_key, cat_val in pos_data.items():
            if not isinstance(cat_val, dict):
                continue
            results = cat_val.get("results") or []
            for entry in results:
                if not isinstance(entry, dict):
                    continue
                # Entry may be flat OR wrap player under a "player" key
                player = entry.get("player", entry) if isinstance(entry, dict) else {}
                if isinstance(player, dict):
                    rows.append(player)

    return _to_frame(rows, return_as_pandas)


def _shots_by_period_to_records(sbp: Any) -> List[Dict[str, Any]]:
    """Normalise ``shotsByPeriod`` into a list of flat row dicts.

    The PWHL ``gc/gamesummary`` endpoint returns::

        {"visitor": {"1": 11, "2": 11, "3": 9},
         "home":    {"1": 10, "2": 13, "3": 12}}

    This helper converts that to one row per (side, period) pair.  If
    ``sbp`` is already a list (old/alternate dialect), it is returned
    unchanged.  None or an unsupported type yields an empty list.
    """
    if sbp is None:
        return []
    if isinstance(sbp, list):
        return sbp
    if isinstance(sbp, dict):
        rows: List[Dict[str, Any]] = []
        for side in ("visitor", "home"):
            per_period = sbp.get(side)
            if not isinstance(per_period, dict):
                continue
            for period, shots in per_period.items():
                rows.append({"side": side, "period": period, "shots": shots})
        return rows
    return []


def parse_game_summary(payload: Any, game_id: Any = None) -> Dict[str, Any]:
    """Parse a HockeyTech ``gc/gamesummary`` JSON payload.

    Returns a dict with five :class:`polars.DataFrame` values:

    - ``game`` -- one-row header (date, status, venue, attendance, scores).
    - ``goals`` -- scoring summary (one row per goal).
    - ``penalties`` -- penalty summary (one row per penalty).
    - ``shots_by_period`` -- shots breakdown by period (one row per
      side/period combination).
    - ``three_stars`` -- post-game three-star selections (falls back to
      ``mvps`` when ``threeStars`` is empty).

    The live PWHL ``gc/gamesummary`` response nests all data under
    ``GC.Gamesummary``.  The ``visitor`` and ``home`` keys are flat team
    dicts; ``totalGoals`` carries the final score; ``shotsByPeriod`` is a
    ``{visitor: {period: shots}, home: {period: shots}}`` dict rather than
    a list.  When ``threeStars`` is empty the ``mvps`` list is used instead
    so that post-game star selections are always populated for finished games.

    Older/alternate dialects may place data directly under ``GC`` with
    ``homeTeam``/``visitingTeam`` sub-keys; both layouts are handled.

    An empty/None payload returns the five-key dict with zero-row frames for all
    subframes and a one-row ``game`` frame whose ``game_id`` is the supplied arg.
    """
    gc_root: Dict[str, Any] = (payload or {}).get("GC", {}) or {}

    # Prefer GC.Gamesummary (live PWHL path); fall back to direct GC keys
    # (alternate dialect where homeTeam/visitingTeam live directly under GC).
    if gc_root.get("Gamesummary") is not None:
        summary: Dict[str, Any] = gc_root.get("Gamesummary", {}) or {}
    elif gc_root.get("details") is not None or gc_root.get("homeTeam") is not None:
        summary = gc_root
    else:
        summary = gc_root.get("Gamesummary", {}) or {}

    # ---- game header --------------------------------------------------------
    # Live PWHL layout: date/status/venue at top level of Gamesummary;
    # alternate dialect wraps them in a ``details`` sub-dict.
    details = summary.get("details") or {}

    # Team info: live path -> flat ``home``/``visitor`` dicts;
    # alternate path -> ``homeTeam``/``visitingTeam`` with ``info``/``stats``.
    home_raw = summary.get("home") or summary.get("homeTeam") or {}
    away_raw = summary.get("visitor") or summary.get("visitingTeam") or {}
    home_info = home_raw.get("info", home_raw)
    away_info = away_raw.get("info", away_raw)
    home_stats = home_raw.get("stats", {})
    away_stats = away_raw.get("stats", {})

    total_goals = summary.get("totalGoals") or {}

    game_row = {
        "game_id": game_id,
        "date": summary.get("game_date") or details.get("date") or summary.get("date_played"),
        "status": summary.get("status_value") or details.get("status") or summary.get("status"),
        "venue": summary.get("venue") or details.get("venue"),
        "attendance": summary.get("attendance") or details.get("attendance"),
        "home_team": home_info.get("name"),
        "home_team_id": home_info.get("id") or home_info.get("team_id"),
        "home_score": home_stats.get("goals") or total_goals.get("home"),
        "away_team": away_info.get("name"),
        "away_team_id": away_info.get("id") or away_info.get("team_id"),
        "away_score": away_stats.get("goals") or total_goals.get("visitor"),
    }

    # ---- subframes ----------------------------------------------------------
    goals_raw = list(summary.get("goals", []) or [])
    penalties_raw = list(summary.get("penalties", []) or [])

    # shotsByPeriod can be a dict {side: {period: shots}} or a plain list
    sbp_raw = _shots_by_period_to_records(summary.get("shotsByPeriod") or summary.get("shots_by_period"))

    # threeStars is empty for some games; fall back to mvps (same concept)
    stars_raw = list(summary.get("threeStars") or summary.get("three_stars") or summary.get("mvps") or [])

    return {
        "game": _to_frame([game_row], False),
        "goals": _to_frame(goals_raw, False),
        "penalties": _to_frame(penalties_raw, False),
        "shots_by_period": _to_frame(sbp_raw, False),
        "three_stars": _to_frame(stars_raw, False),
    }


# ---------------------------------------------------------------------------
# Flat SiteKit extractors (long-tail parsers)
# ---------------------------------------------------------------------------


def _flat_sitekit_parser(key: str, rename: Optional[Dict[str, str]] = None, records: Optional[str] = None):
    """Factory that creates a flat SiteKit extractor for the given ``key``.

    The returned parser reads ``SiteKit.<key>`` -- a list of dicts, or, for views that wrap
    it, the list under ``SiteKit.<key>.<records>`` (``transactions``: ``{"transactions":
    [...], "num_results": N}``; ``player`` gamebygame: ``{"games": [...], ...}``). Any other
    dict is one record. Optionally applies column renames, and delegates to ``_to_frame``. An
    empty/None payload returns a zero-row frame without raising.
    """

    def _parser(payload: Any, return_as_pandas: bool = False) -> Any:
        raw = _sitekit(payload, key) or []
        if isinstance(raw, dict):
            raw = (raw.get(records) or []) if records else [raw]
        if rename and isinstance(raw, list):
            raw = [{rename.get(k, k): v for k, v in r.items()} for r in raw if isinstance(r, dict)]
        return _to_frame(list(raw), return_as_pandas)

    _parser.__name__ = f"parse_{key.lower()}"
    _parser.__doc__ = (
        f"Parse a HockeyTech payload whose data lives under ``SiteKit.{key}``.\n\n"
        "Returns a :class:`polars.DataFrame` by default. "
        "An empty/None payload returns a zero-row frame, never raises."
    )
    return _parser


parse_player_info = _flat_sitekit_parser("Player")
"""Parse ``SiteKit.Player`` as a flat frame (single-player info view)."""

parse_player_game_log = _flat_sitekit_parser("Player", records="games")
"""Parse ``SiteKit.Player.games`` (the gamebygame view) as one row per game."""

parse_player_search = _flat_sitekit_parser("Searchplayers")
"""Parse ``SiteKit.Searchplayers`` into a flat frame (player search results)."""

parse_streaks = _flat_sitekit_parser("Streaks")
"""Parse ``SiteKit.Streaks`` into a flat frame (player/team streaks)."""

parse_transactions = _flat_sitekit_parser("Transactions", records="transactions")
"""Parse ``SiteKit.Transactions`` into a flat frame (roster transactions)."""


def parse_playoff_bracket(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse ``SiteKit.Brackets`` into one row per playoff series.

    The view nests series under rounds (``{"rounds": [{"round": ..., "matchups": [...]}],
    "teams": {...}}``); each series row carries its round's ``round_name`` /
    ``round_type_name``. An empty/None payload returns a zero-row frame, never raises.
    """
    brackets = _sitekit(payload, "Brackets") or {}
    rounds = brackets.get("rounds") or [] if isinstance(brackets, dict) else []
    rows = [
        {**{k: v for k, v in rnd.items() if k != "matchups"}, **series}
        for rnd in rounds
        if isinstance(rnd, dict)
        for series in rnd.get("matchups") or []
        if isinstance(series, dict)
    ]
    return _to_frame(rows, return_as_pandas)


parse_scorebar = _flat_sitekit_parser("Scorebar")
"""Parse ``SiteKit.Scorebar`` into a flat frame (live scorebar).

NOTE: :func:`parse_schedule` maps the same rows onto the canonical schedule columns
(``_SCOREBAR_RENAME``).
"""

parse_stats = _flat_sitekit_parser("Statviewtype")
"""Parse ``SiteKit.Statviewtype`` into a flat frame (stat view / leaders).

NOTE: for the leaders-specific column contract see :func:`parse_leaders`.
"""

parse_game_info = _flat_sitekit_parser("Gameinfo")
"""Parse ``SiteKit.Gameinfo`` into a flat frame (single-game metadata).

NOTE: needs a captured fixture for full column parity (Task A1.8 follow-up).
"""

parse_player_box = _flat_sitekit_parser("Playerbox")
"""Parse ``SiteKit.Playerbox`` into a flat frame (player box score).

NOTE: needs a captured fixture for full column parity (Task A1.8 follow-up).
"""


def parse_seasons(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/seasons`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.
    """
    raw = _sitekit(payload, "Seasons") or []
    rows = []
    for s in raw:
        name = s.get("season_name")
        yr, label = _derive_season_year(name), _game_type_label(name)
        # A one-year preseason or exhibition name gives the camp's calendar year: "2026
        # Pre-season" starts 2026-08-11 and opens 2026-27, so it is 2027, like "2026-27 MHL
        # Exhibition Season". Shifted only when the name spans no two years and the row starts
        # in the year the name gives (all 20 feeds, 2026-10-05). PWHL's "2024 Preseason"
        # started 2023-11-01 and stays 2024, the season it opened.
        if (
            label in ("preseason", "exhibition")
            and yr is not None
            and not re.search(TWO_YEAR_NAME_RE, name or "")
            and str(s.get("start_date") or "")[:4] == str(yr)
        ):
            yr += 1
        rows.append(
            {
                "season_id": int(s.get("season_id")) if s.get("season_id") else None,
                "season_name": name,
                "season_short": s.get("shortname"),
                "career": s.get("career", "0"),
                "playoff": s.get("playoff", "0"),
                "start_date": s.get("start_date"),
                "end_date": s.get("end_date"),
                "season_yr": yr,
                "game_type_label": label,
            }
        )
    df = _to_frame(rows, return_as_pandas)
    if not len(df):
        return df
    # pandas turns an int column holding a None into float64: cast back at the boundary
    if return_as_pandas:
        df["season_yr"] = df["season_yr"].astype("Int64")
        return df
    return df.with_columns(pl.col("season_yr").cast(pl.Int64))
