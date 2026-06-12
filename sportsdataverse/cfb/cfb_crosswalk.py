"""Cross-provider CFB identity crosswalks (ESPN / Fox Sports / Yahoo / Odds API).

Each data provider assigns its **own** primary keys to the same real-world
entity. Ohio State is ESPN team ``194``, Fox team ``25``, and Yahoo team
``ncaaf.t.194``-style dotted id; a single game is ESPN event ``401752687``,
a Fox Bifrost event id, and a Yahoo dotted ``ncaaf.g.<date-id>``. Joining data
across providers therefore requires a *crosswalk* — a translation table that
maps each provider's id/name/abbreviation onto a shared key.

The shared key here is an aggressively normalized **team name** (location +
nickname), because it is the one field every provider exposes consistently:

============  ===============================  ==================================
Provider      Full-name field                  Example
============  ===============================  ==================================
ESPN          ``team_display_name``            ``"Ohio State Buckeyes"``
Fox           teamnav ``entityLink.title``     ``"OHIO STATE BUCKEYES"``
Yahoo         scoreboard ``full_name``         ``"Ohio State Buckeyes"``
The Odds API  ``home_team`` / ``away_team``    ``"Ohio State Buckeyes"``
============  ===============================  ==================================

All four normalize to ``"ohio state buckeyes"``. A small, *additive* alias table
(:data:`_TEAM_ALIASES`) unifies the genuine spelling divergences (Ole Miss vs
Mississippi, UConn vs Connecticut, "Miami (FL)" vs "Miami", ...). The alias
table can only ever *merge* two spellings onto one canonical key — it never
splits — so extending it is safe.

Design: the **matching logic is pure** (``_merge_*`` functions take normalized
record lists and return merged record lists), and the **network adapters are
thin** (``_*_dir`` / ``_*_games`` functions fetch a provider and project to the
uniform mini-schema). This keeps the join logic unit-testable offline without
hitting any live API.

Public surface:

* :func:`cfb_teams_crosswalk` — ESPN x Fox x Yahoo team-id crosswalk.
* :func:`cfb_schedule_crosswalk` — ESPN x Yahoo game-id crosswalk for a week.
* :func:`cfb_rosters_crosswalk` — ESPN x Fox player-id crosswalk for a team.
* :func:`cfb_odds_events_crosswalk` — Odds API event-id ↔ ESPN game-id.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd
    from polars._typing import PolarsDataType

from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_team_roster
from sportsdataverse.cfb.cfb_fox_ext import fox_cfb_schedule, fox_cfb_team_roster, fox_cfb_teams
from sportsdataverse.cfb.cfb_schedule import espn_cfb_calendar, espn_cfb_schedule, most_recent_cfb_season
from sportsdataverse.cfb.cfb_teams import espn_cfb_teams
from sportsdataverse.cfb.cfb_yahoo_ext import yahoo_cfb_scoreboard, yahoo_cfb_teams

__all__ = [
    "cfb_teams_crosswalk",
    "cfb_schedule_crosswalk",
    "cfb_rosters_crosswalk",
    "cfb_odds_events_crosswalk",
]

DataFrameT = Union[pl.DataFrame, "pd.DataFrame"]

# ---------------------------------------------------------------------------
# Normalization layer (pure)
# ---------------------------------------------------------------------------

# Variant normalized full-name -> canonical normalized full-name. Every entry
# merges two spellings of the SAME team onto one key; because distinct teams
# never share a value here, this table can only unify (never mis-merge). Extend
# it whenever a real cross-provider spelling gap is found.
_TEAM_ALIASES: Dict[str, str] = {
    "ole miss rebels": "mississippi rebels",
    "miami fl hurricanes": "miami hurricanes",
    "miami florida hurricanes": "miami hurricanes",
    "uconn huskies": "connecticut huskies",
    "umass minutemen": "massachusetts minutemen",
    "southern miss golden eagles": "southern mississippi golden eagles",
    "app state mountaineers": "appalachian state mountaineers",
    "ul monroe warhawks": "louisiana monroe warhawks",
    "louisiana ragin cajuns": "louisiana lafayette ragin cajuns",
    "sam houston bearkats": "sam houston state bearkats",
    "fiu panthers": "florida international panthers",
    "usf bulls": "south florida bulls",
    "north carolina state wolfpack": "nc state wolfpack",  # Fox spells it out; ESPN/Yahoo use "NC State"
    # FCS / lower-division divergences: Yahoo & Fox carry the formal school name,
    # ESPN abbreviates (drops "State"/"University", "St."->"Saint", etc.).
    # Each key is the Yahoo/Fox spelling; each value is ESPN's exact spelling
    # (verified against the live espn_cfb_teams() directory).
    "grambling state tigers": "grambling tigers",
    "nicholls state colonels": "nicholls colonels",
    "southeastern louisiana lions": "se louisiana lions",
    "tennessee martin skyhawks": "ut martin skyhawks",
    "delaware fightin blue hens": "delaware blue hens",
    "central connecticut state blue devils": "central connecticut blue devils",
    "southern university jaguars": "southern jaguars",
    "university at albany great danes": "ualbany great danes",
    "liu sharks": "long island university sharks",
    "st thomas tommies": "st thomas minnesota tommies",
    "st francis pa red flash": "saint francis red flash",
    "ave maria gyrenes": "ave maria university gyrenes",
    "central state oh marauders": "central state marauders",
}


def _ascii_fold(text: str) -> str:
    """Drop accents/diacritics so ``San José`` matches ``San Jose``."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def _norm_team(name: Optional[str]) -> str:
    """Normalize a team name into the shared crosswalk key.

    Lowercases, ASCII-folds, strips all punctuation (so ``"Texas A&M"`` ->
    ``"texas a m"`` and ``"Miami (FL)"`` -> ``"miami fl"``), collapses
    whitespace, then applies the :data:`_TEAM_ALIASES` override.

    Args:
        name: Any provider's team name (display name, full name, location).

    Returns:
        The canonical normalized key, or ``""`` for empty input.
    """
    if not name:
        return ""
    folded = _ascii_fold(str(name)).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", folded)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return _TEAM_ALIASES.get(cleaned, cleaned)


def _norm_person(name: Optional[str]) -> str:
    """Normalize a player name for cross-provider roster matching.

    Handles the ``"Last, First"`` form (some feeds invert), ASCII-folds, drops
    punctuation (so ``"C.J. Stroud"`` -> ``"c j stroud"``), and collapses
    whitespace.

    Args:
        name: A player's displayed name from any provider.

    Returns:
        The normalized name key, or ``""`` for empty input.
    """
    if not name:
        return ""
    text = str(name).strip()
    if "," in text:  # "Stroud, C.J." -> "C.J. Stroud"
        last, _, first = text.partition(",")
        text = f"{first.strip()} {last.strip()}"
    folded = _ascii_fold(text).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", folded)
    return re.sub(r"\s+", " ", cleaned).strip()


def _norm_jersey(value: Any) -> str:
    """Normalize a jersey number (drop non-digits + leading zeros)."""
    if value is None:
        return ""
    digits = re.sub(r"\D", "", str(value))
    return digits.lstrip("0") or ("0" if digits else "")


def _matchup_key(home_norm: str, away_norm: str) -> str:
    """An order-independent key for a game: the sorted team-pair."""
    pair = sorted(k for k in (home_norm, away_norm) if k)
    return "|".join(pair)


def _yahoo_date(start_time: Optional[str]) -> Optional[str]:
    """Parse a Yahoo ``start_time`` (RFC-2822) into an ISO ``YYYY-MM-DD``.

    Uses :func:`email.utils.parsedate_to_datetime` rather than ``strptime`` with
    ``%a``/``%b`` directives, which are locale-dependent and would silently fail
    on a non-English system.
    """
    if not start_time:
        return None
    try:
        return parsedate_to_datetime(str(start_time)).date().isoformat()
    except (ValueError, TypeError):
        return None


def _iso_date(value: Optional[str]) -> Optional[str]:
    """Take the ``YYYY-MM-DD`` prefix of an ISO timestamp (or ``None``)."""
    if not value:
        return None
    return str(value)[:10] or None


def _matched_sources(flags: Sequence[tuple[str, bool]]) -> str:
    """Render a ``"espn+fox+yahoo"``-style provenance tag from presence flags."""
    return "+".join(name for name, present in flags if present)


def _pick(row: Mapping[str, Any], *candidates: str) -> Any:
    """Return the first present, non-null value among ``candidates`` keys."""
    for key in candidates:
        if key in row and row[key] is not None:
            return row[key]
    return None


# ---------------------------------------------------------------------------
# Network adapters -> uniform normalized record lists (thin)
# ---------------------------------------------------------------------------


def _rows(df: DataFrameT) -> List[Dict[str, Any]]:
    """Coerce a polars/pandas frame (or ``None``) to a list of row dicts."""
    if df is None:
        return []
    if isinstance(df, pl.DataFrame):
        return df.to_dicts()
    return df.to_dict(orient="records")  # pandas fallback


def _espn_team_dir(**kwargs: Any) -> List[Dict[str, Any]]:
    df = espn_cfb_teams(**kwargs)
    out: List[Dict[str, Any]] = []
    for r in _rows(df):
        name = _pick(r, "team_display_name", "team_name", "team_location")
        out.append(
            {
                "norm_key": _norm_team(name),
                "team_id": _pick(r, "team_id", "team_uid"),
                "name": name,
                "abbreviation": _pick(r, "team_abbreviation"),
            }
        )
    return out


def _fox_team_dir(**kwargs: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in _rows(fox_cfb_teams(**kwargs)):
        name = _pick(r, "name")
        out.append(
            {
                "norm_key": _norm_team(name),
                "team_id": _pick(r, "fox_team_id"),
                "name": name,
                "abbreviation": _pick(r, "abbreviation"),
            }
        )
    return out


def _yahoo_team_dir(season: int, week: int, **kwargs: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in _rows(yahoo_cfb_teams(season, week, **kwargs)):
        name = _pick(r, "full_name", "display_name")
        out.append(
            {
                "norm_key": _norm_team(name),
                "team_id": _pick(r, "team_id"),
                "name": name,
                "abbreviation": _pick(r, "abbreviation"),
            }
        )
    return out


def _project_espn(df: DataFrameT) -> List[Dict[str, Any]]:
    """ESPN schedule frame -> uniform game records."""
    out: List[Dict[str, Any]] = []
    for r in _rows(df):
        home = _pick(r, "home_display_name", "home_name", "home_location")
        away = _pick(r, "away_display_name", "away_name", "away_location")
        out.append(
            {
                "matchup_key": _matchup_key(_norm_team(home), _norm_team(away)),
                "game_id": _pick(r, "game_id", "id"),
                "date": _iso_date(_pick(r, "start_date", "date")),
                "home_team": home,
                "away_team": away,
            }
        )
    return out


def _espn_games(season: int, week: Optional[int], season_type: int, **kwargs: Any) -> List[Dict[str, Any]]:
    # week=None is intentional: espn_cfb_schedule returns ESPN's current/default
    # slate when no week is supplied (used by the odds crosswalk).
    return _project_espn(espn_cfb_schedule(dates=season, week=week, season_type=season_type, **kwargs))


def _espn_season_games(season: int, **kwargs: Any) -> List[Dict[str, Any]]:
    """Every ESPN game for a season -- regular weeks + bowls + CFP.

    Driven by the ESPN calendar so the exact (week, season_type) slots are used:
    regular weeks (season_type 2), bowls (season_type 3, week 1), and the CFP
    (season_type 3, week 999). season_type 4 (all-star) is skipped. Each weekly
    pull is best-effort -- an empty/failed week contributes nothing rather than
    aborting the season.
    """
    out: List[Dict[str, Any]] = []
    seen: set[Any] = set()
    slots: List[tuple[Any, Any]] = []
    try:
        cal = espn_cfb_calendar(season=season, **kwargs)
        slots = [(r.get("week"), r.get("season_type")) for r in _rows(cal)]
    except Exception:
        slots = []
    if not slots:  # calendar unavailable -> sensible default coverage
        slots = [(str(w), "2") for w in range(1, 17)] + [("1", "3"), ("999", "3")]
    for week, stype in slots:
        if str(stype) not in ("2", "3") or week is None:
            continue
        try:
            rows = _project_espn(espn_cfb_schedule(dates=season, week=int(week), season_type=int(stype), **kwargs))
        except Exception:
            continue
        for row in rows:
            gid = row.get("game_id")
            if gid is not None and gid in seen:
                continue
            if gid is not None:
                seen.add(gid)
            out.append(row)
    return out


def _yahoo_games(season: int, week: int, **kwargs: Any) -> List[Dict[str, Any]]:
    raw = yahoo_cfb_scoreboard(season, week, return_parsed=False, **kwargs)
    scoreboard = (raw.get("service") or {}).get("scoreboard") or {}
    teams_map = scoreboard.get("teams") or {}
    id_to_name = {tid: (team.get("full_name") or team.get("display_name")) for tid, team in teams_map.items()}
    out: List[Dict[str, Any]] = []
    for gid, game in (scoreboard.get("games") or {}).items():
        home = id_to_name.get(game.get("home_team_id"))
        away = id_to_name.get(game.get("away_team_id"))
        home_norm, away_norm = _norm_team(home), _norm_team(away)
        out.append(
            {
                "matchup_key": _matchup_key(home_norm, away_norm),
                "game_id": gid,
                "global_game_id": game.get("global_gameid"),
                "date": _yahoo_date(game.get("start_time")),
                "home_team": home,
                "away_team": away,
            }
        )
    return out


def _project_fox(df: DataFrameT) -> List[Dict[str, Any]]:
    """Fox schedule frame -> uniform game records."""
    out: List[Dict[str, Any]] = []
    for r in _rows(df):
        home = _pick(r, "home_team")
        away = _pick(r, "away_team")
        out.append(
            {
                "matchup_key": _matchup_key(_norm_team(home), _norm_team(away)),
                "game_id": _pick(r, "game_id"),
                "date": _iso_date(_pick(r, "date")),
                "home_team": home,
                "away_team": away,
            }
        )
    return out


def _fox_games(season: int, week: int, **kwargs: Any) -> List[Dict[str, Any]]:
    # Fetch just the regular-season week segment ("{season}-{week}-1") -- one HTTP
    # call -- and match its games onto ESPN's week by team. Fox is best-effort: a
    # Fox outage must never break the ESPN<->Yahoo core, hence the deliberate
    # broad guard returning an empty list.
    try:
        return _project_fox(fox_cfb_schedule(segment_id=f"{season}-{week}-1", **kwargs))
    except Exception:
        return []


def _fox_season_games(season: int, **kwargs: Any) -> List[Dict[str, Any]]:
    # Fox's full season (regular weeks + conf championships + bowls + every CFP
    # round), best-effort. Fox postseason matchups can be projections in the
    # offseason -- those simply fail to match and fall through as null Fox ids.
    try:
        return _project_fox(fox_cfb_schedule(season, **kwargs))
    except Exception:
        return []


def _yahoo_season_games(season: int, **kwargs: Any) -> List[Dict[str, Any]]:
    # Yahoo scoreboard is per-week; loop the regular season + postseason weeks,
    # swallowing the occasional per-week parser error so one bad week can't sink
    # the whole season. Dedup by game id across weeks.
    out: List[Dict[str, Any]] = []
    seen: set[Any] = set()
    for week in range(1, 21):
        try:
            rows = _yahoo_games(season, week, **kwargs)
        except Exception:
            continue
        for row in rows:
            gid = row.get("game_id")
            if gid is not None and gid in seen:
                continue
            if gid is not None:
                seen.add(gid)
            out.append(row)
    return out


def _odds_events(sport: str, **kwargs: Any) -> List[Dict[str, Any]]:
    from sportsdataverse.odds import toa_sports_events

    out: List[Dict[str, Any]] = []
    for r in _rows(toa_sports_events(sport=sport, **kwargs)):
        home = _pick(r, "home_team")
        away = _pick(r, "away_team")
        home_norm, away_norm = _norm_team(home), _norm_team(away)
        out.append(
            {
                "matchup_key": _matchup_key(home_norm, away_norm),
                "event_id": _pick(r, "id"),
                "commence_time": _pick(r, "commence_time"),
                "date": _iso_date(_pick(r, "commence_time")),
                "home_team": home,
                "away_team": away,
            }
        )
    return out


def _espn_roster(team_id: Union[int, str], **kwargs: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in _rows(espn_cfb_team_roster(team_id, **kwargs)):
        name = _pick(r, "full_name", "display_name")
        jersey = _pick(r, "jersey")
        out.append(
            {
                "person_key": _norm_person(name),
                "jersey_key": _norm_jersey(jersey),
                "athlete_id": _pick(r, "id", "athlete_id"),
                "name": name,
                "jersey": jersey,
                "position": _pick(r, "position_abbreviation", "position_name"),
            }
        )
    return out


def _fox_roster(team_id: Union[int, str], **kwargs: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in _rows(fox_cfb_team_roster(team_id, **kwargs)):
        name = _pick(r, "player")
        jersey = _pick(r, "no_", "no.", "no", "number", "jersey")
        out.append(
            {
                "person_key": _norm_person(name),
                "jersey_key": _norm_jersey(jersey),
                "athlete_id": _pick(r, "athlete_id"),
                "name": name,
                "jersey": jersey,
                "position": _pick(r, "pos", "pos.", "position", "position_group"),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Pure merge builders (no network) -> list[dict]
# ---------------------------------------------------------------------------


def _index_by(records: Sequence[Mapping[str, Any]], key: str) -> Dict[str, Mapping[str, Any]]:
    """First-wins index of ``records`` by a non-empty string ``key`` field."""
    index: Dict[str, Mapping[str, Any]] = {}
    for rec in records:
        value = rec.get(key)
        if value and value not in index:
            index[value] = rec
    return index


def _merge_teams(
    espn: Sequence[Mapping[str, Any]],
    fox: Sequence[Mapping[str, Any]],
    yahoo: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Full-outer-join the three team directories on ``norm_key`` (pure)."""
    fox_by = _index_by(fox, "norm_key")
    yahoo_by = _index_by(yahoo, "norm_key")
    espn_keys = {r["norm_key"] for r in espn if r.get("norm_key")}
    fox_keys = {r["norm_key"] for r in fox if r.get("norm_key")}

    def row(
        e: Optional[Mapping[str, Any]], f: Optional[Mapping[str, Any]], y: Optional[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        anchor = e or f or y or {}
        return {
            "norm_key": anchor.get("norm_key"),
            "espn_team_id": e.get("team_id") if e else None,
            "espn_team": e.get("name") if e else None,
            "espn_abbreviation": e.get("abbreviation") if e else None,
            "fox_team_id": f.get("team_id") if f else None,
            "fox_team": f.get("name") if f else None,
            "fox_abbreviation": f.get("abbreviation") if f else None,
            "yahoo_team_id": y.get("team_id") if y else None,
            "yahoo_team": y.get("name") if y else None,
            "yahoo_abbreviation": y.get("abbreviation") if y else None,
            "matched_sources": _matched_sources(
                [("espn", e is not None), ("fox", f is not None), ("yahoo", y is not None)]
            ),
        }

    out: List[Dict[str, Any]] = []
    for e in espn:  # ESPN-anchored rows first
        key = e.get("norm_key")
        out.append(row(e, fox_by.get(key) if key else None, yahoo_by.get(key) if key else None))
    for f in fox:  # Fox teams with no ESPN match
        key = f.get("norm_key")
        if key and key not in espn_keys:
            out.append(row(None, f, yahoo_by.get(key)))
    for y in yahoo:  # Yahoo teams with no ESPN or Fox match
        key = y.get("norm_key")
        if key and key not in espn_keys and key not in fox_keys:
            out.append(row(None, None, y))
    return out


def _schedule_row(
    e: Optional[Mapping[str, Any]],
    f: Optional[Mapping[str, Any]],
    y: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """One ESPN/Fox/Yahoo schedule-crosswalk row from the matched game records."""
    anchor = e or y or f or {}
    return {
        "matchup_key": anchor.get("matchup_key"),
        "espn_game_id": e.get("game_id") if e else None,
        "fox_game_id": f.get("game_id") if f else None,
        "yahoo_game_id": y.get("game_id") if y else None,
        "yahoo_global_game_id": y.get("global_game_id") if y else None,
        "home_team": _pick(e or {}, "home_team") or _pick(f or {}, "home_team") or _pick(y or {}, "home_team"),
        "away_team": _pick(e or {}, "away_team") or _pick(f or {}, "away_team") or _pick(y or {}, "away_team"),
        "espn_date": e.get("date") if e else None,
        "fox_date": f.get("date") if f else None,
        "yahoo_date": y.get("date") if y else None,
        "matched_sources": _matched_sources(
            [("espn", e is not None), ("fox", f is not None), ("yahoo", y is not None)]
        ),
    }


def _merge_schedule(
    espn: Sequence[Mapping[str, Any]],
    fox: Sequence[Mapping[str, Any]],
    yahoo: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Join ESPN + Fox + Yahoo games on ``matchup_key``, anchored on ESPN (pure).

    For a single week: ESPN's slate is the spine; Fox and Yahoo games are mapped
    onto it by team matchup. Yahoo games with no ESPN match are appended (Yahoo's
    feed is week-scoped, so they are real same-week games). Fox games with no
    ESPN match are **not** appended — a Fox segment spans a whole phase, so a
    fox-only row would be a different week's game, not a gap in this one.
    """
    fox_by = _index_by(fox, "matchup_key")
    yahoo_by = _index_by(yahoo, "matchup_key")
    espn_keys = {r["matchup_key"] for r in espn if r.get("matchup_key")}

    out: List[Dict[str, Any]] = []
    for e in espn:
        key = e.get("matchup_key")
        out.append(_schedule_row(e, fox_by.get(key) if key else None, yahoo_by.get(key) if key else None))
    for y in yahoo:  # same-week Yahoo games ESPN didn't list
        key = y.get("matchup_key")
        if key and key not in espn_keys:
            out.append(_schedule_row(None, fox_by.get(key) if key else None, y))
    return out


def _date_dist(a: Optional[str], b: Optional[str]) -> int:
    """Absolute day distance between two ISO ``YYYY-MM-DD`` dates (large if N/A)."""
    if not a or not b:
        return 10**6
    try:
        return abs((date.fromisoformat(a[:10]) - date.fromisoformat(b[:10])).days)
    except (ValueError, TypeError):
        return 10**6


def _index_multi(records: Sequence[Mapping[str, Any]], key: str) -> Dict[str, List[Mapping[str, Any]]]:
    """Group records into ``{key: [records...]}`` (keeps rematches separable)."""
    index: Dict[str, List[Mapping[str, Any]]] = {}
    for rec in records:
        value = rec.get(key)
        if value:
            index.setdefault(value, []).append(rec)
    return index


def _pick_match(candidates: List[Mapping[str, Any]], when: Optional[str]) -> Optional[Mapping[str, Any]]:
    """From same-matchup candidates, pick the one nearest ``when`` (date-aware).

    A team pair can meet more than once a season (regular game, conference
    championship, CFP rematch), so date disambiguates: an exact-date match wins,
    else the closest by date, else the only/first candidate.
    """
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    exact = [c for c in candidates if c.get("date") == when]
    if exact:
        return exact[0]
    return min(candidates, key=lambda c: _date_dist(c.get("date"), when))


def _merge_schedule_full(
    espn: Sequence[Mapping[str, Any]],
    fox: Sequence[Mapping[str, Any]],
    yahoo: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Whole-season ESPN x Fox x Yahoo join, anchored on ESPN, matched by team +
    date (pure).

    Unlike the per-week merge, a matchup can recur across the season, so Fox and
    Yahoo are indexed as lists per ``matchup_key`` and the date-nearest game is
    chosen. Yahoo games ESPN never lists (e.g. non-FBS) are appended; Fox-only
    games are dropped. Missing matches fall through as null ids — the graceful
    degradation that lets regular season, conf championships, bowls, and CFP all
    flow through one call even when a provider lacks a game.
    """
    fox_idx = _index_multi(fox, "matchup_key")
    yahoo_idx = _index_multi(yahoo, "matchup_key")
    espn_keys = {r["matchup_key"] for r in espn if r.get("matchup_key")}

    out: List[Dict[str, Any]] = []
    for e in espn:
        key, when = e.get("matchup_key"), e.get("date")
        f = _pick_match(fox_idx.get(key, []), when) if key else None
        y = _pick_match(yahoo_idx.get(key, []), when) if key else None
        out.append(_schedule_row(e, f, y))
    for y in yahoo:  # Yahoo games ESPN never listed (broader division coverage)
        key = y.get("matchup_key")
        if key and key not in espn_keys:
            f = _pick_match(fox_idx.get(key, []), y.get("date"))
            out.append(_schedule_row(None, f, y))
    return out


def _merge_odds(
    events: Sequence[Mapping[str, Any]],
    games: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Join Odds API events to ESPN games on ``matchup_key`` (pure)."""
    games_by = _index_by(games, "matchup_key")
    out: List[Dict[str, Any]] = []
    for ev in events:
        key = ev.get("matchup_key")
        g = games_by.get(key) if key else None
        out.append(
            {
                "matchup_key": key,
                "odds_event_id": ev.get("event_id"),
                "espn_game_id": g.get("game_id") if g else None,
                "home_team": ev.get("home_team"),
                "away_team": ev.get("away_team"),
                "commence_time": ev.get("commence_time"),
                "espn_date": g.get("date") if g else None,
                "matched_sources": _matched_sources([("odds", True), ("espn", g is not None)]),
            }
        )
    return out


def _merge_rosters(
    espn: Sequence[Mapping[str, Any]],
    fox: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Full-outer-join ESPN + Fox rosters on normalized name (pure).

    Matches on ``person_key`` (normalized name); when the same name appears on
    both sides with conflicting jerseys the row is still emitted but tagged
    ``match_method="name_jersey_conflict"`` so the caller can review it.
    """
    fox_by = _index_by(fox, "person_key")
    espn_keys = {r["person_key"] for r in espn if r.get("person_key")}

    def method(e: Optional[Mapping[str, Any]], f: Optional[Mapping[str, Any]]) -> str:
        if e is None or f is None:
            return "unmatched"
        ej, fj = e.get("jersey_key"), f.get("jersey_key")
        if ej and fj and ej != fj:
            return "name_jersey_conflict"
        return "name_jersey" if ej and fj and ej == fj else "name"

    def row(e: Optional[Mapping[str, Any]], f: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        anchor = e or f or {}
        return {
            "person_key": anchor.get("person_key"),
            "espn_athlete_id": e.get("athlete_id") if e else None,
            "fox_athlete_id": f.get("athlete_id") if f else None,
            "name": (e or {}).get("name") or (f or {}).get("name"),
            "espn_jersey": e.get("jersey") if e else None,
            "fox_jersey": f.get("jersey") if f else None,
            "espn_position": e.get("position") if e else None,
            "fox_position": f.get("position") if f else None,
            "match_method": method(e, f),
            "matched_sources": _matched_sources([("espn", e is not None), ("fox", f is not None)]),
        }

    out: List[Dict[str, Any]] = []
    for e in espn:
        key = e.get("person_key")
        out.append(row(e, fox_by.get(key) if key else None))
    for f in fox:
        key = f.get("person_key")
        if key and key not in espn_keys:
            out.append(row(None, f))
    return out


# ---------------------------------------------------------------------------
# Output schemas + materialization
# ---------------------------------------------------------------------------

_TEAMS_SCHEMA: Dict[str, "PolarsDataType"] = {
    "norm_key": pl.Utf8,
    "espn_team_id": pl.Int64,
    "espn_team": pl.Utf8,
    "espn_abbreviation": pl.Utf8,
    "fox_team_id": pl.Utf8,
    "fox_team": pl.Utf8,
    "fox_abbreviation": pl.Utf8,
    "yahoo_team_id": pl.Utf8,
    "yahoo_team": pl.Utf8,
    "yahoo_abbreviation": pl.Utf8,
    "matched_sources": pl.Utf8,
}

_SCHEDULE_SCHEMA: Dict[str, "PolarsDataType"] = {
    "matchup_key": pl.Utf8,
    "espn_game_id": pl.Int64,
    "fox_game_id": pl.Utf8,
    "yahoo_game_id": pl.Utf8,
    "yahoo_global_game_id": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "espn_date": pl.Utf8,
    "fox_date": pl.Utf8,
    "yahoo_date": pl.Utf8,
    "matched_sources": pl.Utf8,
}

_ODDS_SCHEMA: Dict[str, "PolarsDataType"] = {
    "matchup_key": pl.Utf8,
    "odds_event_id": pl.Utf8,
    "espn_game_id": pl.Int64,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "commence_time": pl.Utf8,
    "espn_date": pl.Utf8,
    "matched_sources": pl.Utf8,
}

_ROSTER_SCHEMA: Dict[str, "PolarsDataType"] = {
    "person_key": pl.Utf8,
    "espn_athlete_id": pl.Int64,
    "fox_athlete_id": pl.Utf8,
    "name": pl.Utf8,
    "espn_jersey": pl.Utf8,
    "fox_jersey": pl.Utf8,
    "espn_position": pl.Utf8,
    "fox_position": pl.Utf8,
    "match_method": pl.Utf8,
    "matched_sources": pl.Utf8,
}


def _materialize(
    rows: Sequence[Mapping[str, Any]],
    schema: Mapping[str, "PolarsDataType"],
    return_as_pandas: bool,
) -> DataFrameT:
    """Build a typed polars frame (pandas when requested) from merged rows.

    Values are coerced toward the declared dtype where it is loss-free (ids that
    arrive as ints/strings), and an explicit ``schema`` means even a zero-row
    result carries the documented column set.
    """
    int_cols = {name for name, dtype in schema.items() if dtype == pl.Int64}
    coerced: List[Dict[str, Any]] = []
    for row in rows:
        rec = {col: row.get(col) for col in schema}
        for col in int_cols:
            rec[col] = _as_int(rec[col])
        coerced.append(rec)
    df = pl.DataFrame(coerced, schema=dict(schema), orient="row")
    if return_as_pandas:
        return df.to_pandas()
    return df


def _as_int(value: Any) -> Optional[int]:
    """Best-effort int coercion for id columns; ``None`` on failure."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def cfb_teams_crosswalk(
    *,
    season: Optional[int] = None,
    week: int = 1,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> DataFrameT:
    """Build the ESPN x Fox x Yahoo CFB team-id crosswalk.

    Fetches all three provider team directories, normalizes each team name to a
    shared key, and full-outer-joins them so every row carries each provider's
    id, name, and abbreviation (``None`` where a provider has no match). The
    ``matched_sources`` column records which providers contributed.

    Args:
        season: Season year used only to fetch Yahoo's embedded team directory
            (Yahoo has no standalone teams endpoint). Defaults to the most
            recent CFB season.
        week: Schedule week used for the Yahoo scoreboard fetch. Defaults to
            ``1``. The embedded directory is the full league list regardless.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to the underlying provider HTTP getters.

    Returns:
        A polars DataFrame (pandas when ``return_as_pandas=True``) with columns
        ``norm_key``, ``espn_team_id``, ``espn_team``, ``espn_abbreviation``,
        ``fox_team_id``, ``fox_team``, ``fox_abbreviation``, ``yahoo_team_id``,
        ``yahoo_team``, ``yahoo_abbreviation``, ``matched_sources``.

    Example:
        Translate an ESPN team id to its Fox + Yahoo ids::

            from sportsdataverse.cfb import cfb_teams_crosswalk
            xwalk = cfb_teams_crosswalk(season=2024)
            row = xwalk.filter(pl.col("espn_team_id") == 194)  # Ohio State

        Find teams only one provider knows about::

            import polars as pl
            gaps = cfb_teams_crosswalk(season=2024).filter(
                pl.col("matched_sources") != "espn+fox+yahoo"
            )

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB data
    """
    if season is None:
        season = most_recent_cfb_season()
    rows = _merge_teams(
        _espn_team_dir(**kwargs),
        _fox_team_dir(**kwargs),
        _yahoo_team_dir(season, week, **kwargs),
    )
    return _materialize(rows, _TEAMS_SCHEMA, return_as_pandas)


def cfb_schedule_crosswalk(
    season: int,
    week: Optional[int] = None,
    *,
    season_type: int = 2,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> DataFrameT:
    """Build the ESPN x Fox x Yahoo CFB game-id crosswalk.

    Each ESPN game is keyed by its order-independent team matchup, and the Fox
    and Yahoo games are mapped onto it, so each row pairs the ESPN ``event`` id
    with the Fox Bifrost event id and the Yahoo dotted game id. Where a provider
    has no game, its columns are ``None`` and ``matched_sources`` records who
    contributed — so regular season, conference championships, bowls, and the
    CFP all flow through the same call, degrading gracefully when a source lacks
    a game.

    Two modes:

    * **Full season** (``week`` omitted): pulls every ESPN game (regular weeks +
      bowls + CFP), Fox's full season, and Yahoo's full season, and matches on
      team **+ date** (date disambiguates rematches — a regular-season game vs a
      conference-championship or CFP rematch of the same teams).
    * **Single week** (``week`` given): just that week's slate, matched on team.

    Each provider leg is best-effort: a Fox outage, a Yahoo per-week parser
    hiccup, or Fox's offseason-projected CFP matchups simply leave that
    provider's columns null rather than failing the call.

    Args:
        season: Season year (e.g. ``2024``).
        week: Schedule week number for single-week mode; omit (``None``) for the
            whole season.
        season_type: ESPN season type for single-week mode — ``2`` regular,
            ``3`` post-season (``week=1`` bowls, ``week=999`` CFP). Ignored in
            full-season mode. Defaults to ``2``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to the underlying provider HTTP getters.

    Returns:
        A polars DataFrame (pandas when ``return_as_pandas=True``) with columns
        ``matchup_key``, ``espn_game_id``, ``fox_game_id``, ``yahoo_game_id``,
        ``yahoo_global_game_id``, ``home_team``, ``away_team``, ``espn_date``,
        ``fox_date``, ``yahoo_date``, ``matched_sources``.

    Example:
        Crosswalk the whole season (regular + bowls + CFP)::

            from sportsdataverse.cfb import cfb_schedule_crosswalk
            full = cfb_schedule_crosswalk(2024)
            all_three = full.filter(pl.col("matched_sources") == "espn+fox+yahoo")

        Or just one week::

            wk5 = cfb_schedule_crosswalk(2024, 5)

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB schedules
    """
    if week is None:  # whole season: match by team + date
        rows = _merge_schedule_full(
            _espn_season_games(season, **kwargs),
            _fox_season_games(season, **kwargs),
            _yahoo_season_games(season, **kwargs),
        )
    else:  # single week: match by team
        rows = _merge_schedule(
            _espn_games(season, week, season_type, **kwargs),
            _fox_games(season, week, **kwargs),
            _yahoo_games(season, week, **kwargs),
        )
    return _materialize(rows, _SCHEDULE_SCHEMA, return_as_pandas)


def cfb_rosters_crosswalk(
    espn_team_id: Union[int, str],
    fox_team_id: Union[int, str],
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> DataFrameT:
    """Build the ESPN x Fox player-id crosswalk for one team.

    Fetches both providers' rosters for the given team, matches players on
    normalized name (with jersey as a confidence signal), and returns each
    player's ESPN and Fox athlete ids side by side. Use
    :func:`cfb_teams_crosswalk` first to translate an ESPN team id into the
    matching Fox team id. Yahoo is excluded — it ships no roster endpoint.

    Args:
        espn_team_id: ESPN team id (e.g. ``194`` for Ohio State).
        fox_team_id: Fox Bifrost team id (e.g. ``25`` for Ohio State).
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to the underlying provider HTTP getters.

    Returns:
        A polars DataFrame (pandas when ``return_as_pandas=True``) with columns
        ``person_key``, ``espn_athlete_id``, ``fox_athlete_id``, ``name``,
        ``espn_jersey``, ``fox_jersey``, ``espn_position``, ``fox_position``,
        ``match_method``, ``matched_sources``. ``match_method`` is one of
        ``name_jersey`` (name + jersey agree), ``name`` (name only),
        ``name_jersey_conflict`` (name matches but jerseys differ — review),
        or ``unmatched``.

    Example:
        Crosswalk Ohio State's roster across ESPN and Fox::

            from sportsdataverse.cfb import cfb_rosters_crosswalk
            xwalk = cfb_rosters_crosswalk(espn_team_id=194, fox_team_id=25)
            matched = xwalk.filter(pl.col("matched_sources") == "espn+fox")

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB rosters
    """
    rows = _merge_rosters(
        _espn_roster(espn_team_id, **kwargs),
        _fox_roster(fox_team_id, **kwargs),
    )
    return _materialize(rows, _ROSTER_SCHEMA, return_as_pandas)


def cfb_odds_events_crosswalk(
    season: Optional[int] = None,
    week: Optional[int] = None,
    *,
    sport: str = "americanfootball_ncaaf",
    api_key: Optional[str] = None,
    season_type: int = 2,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> DataFrameT:
    """Match The Odds API CFB events to ESPN game ids.

    Pulls the upcoming/live events for ``sport`` from The Odds API and the ESPN
    scoreboard for ``(season, week)``, then joins them on the order-independent
    team matchup so each odds event id maps to its ESPN ``event`` id. Because
    The Odds API only lists near-term events, this is most useful for the
    current/upcoming week.

    Args:
        season: ESPN season year for the schedule side. Defaults to the most
            recent CFB season.
        week: ESPN schedule week. When ``None``, ESPN returns its default
            (current) slate.
        sport: The Odds API sport key. Defaults to ``"americanfootball_ncaaf"``.
        api_key: The Odds API key; falls back to the ``ODDS_API_KEY`` env var.
        season_type: ESPN season type (``2`` regular, ``3`` post-season).
            Defaults to ``2``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to the underlying provider HTTP getters.

    Returns:
        A polars DataFrame (pandas when ``return_as_pandas=True``), one row per
        odds event, with columns ``matchup_key``, ``odds_event_id``,
        ``espn_game_id``, ``home_team``, ``away_team``, ``commence_time``,
        ``espn_date``, ``matched_sources``.

    Example:
        Map Odds API events to ESPN game ids for week 5::

            from sportsdataverse.cfb import cfb_odds_events_crosswalk
            xwalk = cfb_odds_events_crosswalk(season=2024, week=5)
            matched = xwalk.filter(pl.col("espn_game_id").is_not_null())

        See Also:
            * `The Odds API <https://the-odds-api.com>`_ -- odds event source
    """
    if season is None:
        season = most_recent_cfb_season()
    rows = _merge_odds(
        _odds_events(sport, api_key=api_key, **kwargs),
        _espn_games(season, week, season_type, **kwargs),
    )
    return _materialize(rows, _ODDS_SCHEMA, return_as_pandas)
