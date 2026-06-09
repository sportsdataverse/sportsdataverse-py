"""Pure HockeyTech parsers: parsed JSON (dict/list) -> snake_cased polars frame.

No network here -- tests drive these from captured fixtures. Every parser
tolerates empty/None payloads by returning a zero-row frame.
"""

from __future__ import annotations

import re
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
    if return_as_pandas:
        return pdf
    return pl.from_pandas(pdf) if len(pdf) else pl.DataFrame()


def _sitekit(payload: Any, key: str) -> Any:
    return ((payload or {}).get("SiteKit", {}) or {}).get(key)


def _derive_season_year(name: str) -> Optional[int]:
    m = re.search(r"(\d{4})-(\d{2})", name or "")
    if m:
        return int(m.group(1)[:2]) * 100 + int(m.group(2))
    m2 = re.search(r"(\d{4})", name or "")
    return int(m2.group(1)) if m2 else None


def _game_type_label(name: str) -> str:
    n = (name or "").lower()
    if re.search(r"pre[- ]?season", n):
        return "preseason"
    if re.search(r"playoff|post", n):
        return "playoffs"
    return "regular"


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
}


def parse_schedule(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/scorebar`` JSON payload into a flat frame.

    One row per game. Returns a :class:`polars.DataFrame` by default; pass
    ``return_as_pandas=True`` for a :class:`pandas.DataFrame`. An empty/None
    payload returns a zero-row frame of the same type, never raises.
    """
    games = _sitekit(payload, "Scorebar") or []
    rows = []
    for g in games:
        row = {new: g.get(old) for old, new in _SCOREBAR_RENAME.items()}
        row["game_type"] = g.get("game_type")
        rows.append(row)
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

    Args:
        payload: A list of event dicts (dialect a) as returned by the
            HockeyTech API ``getGamePlayByPlay`` endpoint.
        pbp_style: Dialect flag.  Only ``"hockeytech_a"`` (PWHL/AHL) is
            implemented here.  ``"hockeytech_b"`` (OHL/WHL/QMJHL) is
            added in task A3.1 and raises :exc:`NotImplementedError`
            until then.
        game_id: Optional game identifier echoed onto every row as
            ``game_id``.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of a :class:`polars.DataFrame`.
    """
    if pbp_style == "hockeytech_b":
        # dialect b (OHL/WHL/QMJHL) is implemented in task A3.1
        raise NotImplementedError("pbp dialect 'hockeytech_b' is implemented in task A3.1")
    events: List[Any] = payload if isinstance(payload, list) else []
    rows = _parse_pbp_a(events, game_id=game_id)
    return _to_frame(rows, return_as_pandas)


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
        rows.append(
            {
                "season_id": int(s.get("season_id")) if s.get("season_id") else None,
                "season_name": name,
                "season_short": s.get("shortname"),
                "career": s.get("career", "0"),
                "playoff": s.get("playoff", "0"),
                "start_date": s.get("start_date"),
                "end_date": s.get("end_date"),
                "season_yr": _derive_season_year(name),
                "game_type_label": _game_type_label(name),
            }
        )
    return _to_frame(rows, return_as_pandas)
