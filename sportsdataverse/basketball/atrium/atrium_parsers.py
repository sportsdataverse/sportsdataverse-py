"""Parsers for the generated ``atrium`` wrappers (Sportradar Atrium / Synergy embed API).

The embed API (``embed-api.eui.connect.sportradar.com/v1/embed/{websiteId}/{type}``) serves the
view model of the Sportradar "Connect" widget that league sites embed. One host serves every site;
``website_id`` picks the site (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador,
22 Panama, 236 Austria, 326 Montenegro: :data:`SITES`). Every body is ``{data, links, meta}``.

Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_cased columns, and a **typed** zero-row frame (the documented
schema, :data:`SCHEMAS`) for an empty or malformed body, so two empty frames and an empty frame
onto a populated one all stack.

**Ids.** Every id is a UUID v1 and a ``String`` column; the key spaces are shared across sites (a
team or person keeps its UUID on every ``website_id``). ``fixture_id`` is one game;
``entity_id`` is a team (one id per club across seasons); ``person_id`` is a player;
``season_id`` is one competition in one year (LNB mints a new ``competition_id`` per edition);
``event_id`` is one play-by-play event, shared with the shot chart. ``website_id`` (``Int64``) is
read from the body's own ``links.self``, which the runtime checks against the request.

**Season.** ``year`` is what the site calls the season: the START year on site 12 (LNB), the
calendar year on the others. ``season_end_year`` is the sdv key (the END year):
:func:`atrium_end_year`. Sites outside :data:`SITES` and site 236 (no season list) map to null.

**Columns are configured per site.** LNB box scores have ``blocks_received`` and ``plus_minus``
but no field-goal totals; LNBP has ``field_goals_*`` and ``fouls_drawn``. Each parser's schema is
the union of the keys seen in committed full bodies (null where a site omits one); a key the
schema does not list is appended with an inferred dtype instead of being dropped.

**Dtypes** follow the key, not the sample: ``*Percentage``, ``*PerGame`` and ``winPercentage``
are ``Float64`` (the API sends ``50.0`` and ``50`` alike), ``minutes``-style durations stay
ISO 8601 strings (``PT7M23S``; ``seconds_*`` columns carry the parsed value), every other
statistic is ``Int64``. Percentages are 0-100 except ``win_percentage``, a 0-1 fraction.
Scores are strings in ``competitors[].score`` and integers in ``scores`` / ``teamScore``; both
become ``Int64``.

**Times.** ``startTimeUTC`` / ``startTimeLocal`` carry no offset: UTC and the venue's local time
(``timezone``). They become naive ``Datetime`` columns. Play-by-play ``clock`` is the time
REMAINING in the period (``PT10M0S`` at the start); ``clock_seconds`` parses it. Overtime
periods are ``period_id`` 11, 12, ... (not 5).

**Statistic tables** (``player_stats`` and friends) are positional arrays aligned to
``headers[]``. The tables are returned in long form (one row per subject and statistic) because
each site configures its own statistic set. The player stat tables carry names only; the widget
link beside each name encodes the person (and team) id, which the parser decodes into
``person_id`` / ``entity_id`` (null when a link is absent or undecodable).
"""

from __future__ import annotations

import base64
import math
import json
import re
import zlib
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

Frame = Union[pl.DataFrame, pd.DataFrame]

__all__ = [
    "atrium_end_year",
    "parse_atrium_box_score",
    "parse_atrium_fixture",
    "parse_atrium_fixture_periods",
    "parse_atrium_fixtures",
    "parse_atrium_fixtures_ribbon",
    "parse_atrium_leaders",
    "parse_atrium_pbp",
    "parse_atrium_player",
    "parse_atrium_player_game_log",
    "parse_atrium_player_seasons",
    "parse_atrium_players",
    "parse_atrium_seasons",
    "parse_atrium_shots",
    "parse_atrium_standings",
    "parse_atrium_stat_tables",
    "parse_atrium_team",
    "parse_atrium_team_box_score",
    "parse_atrium_teams",
]

#: websiteId -> (site, what ``data.seasons.seasons[].year`` means). ``"start"``: the START year
#: (END = year + 1); ``"calendar"``: the calendar year (= END); ``None``: the site lists no seasons.
SITES: Dict[int, Tuple[str, Optional[str]]] = {
    12: ("LNB (France)", "start"),
    14: ("LNBP (Mexico)", "calendar"),
    3: ("NBL1 (Australia)", "calendar"),
    13: ("Liga Mayor de Baloncesto (El Salvador)", "calendar"),
    22: ("LPB / LPBF (Panama)", "calendar"),
    236: ("Basketball Superliga (Austria)", None),
    326: ("Montenegro (Memorial Ljubo Jovanovic)", "calendar"),
}


def atrium_end_year(website_id: Any, year: Any) -> Optional[int]:
    """The sdv season key (END year) of a site's ``year``, or ``None`` when it cannot be mapped.

    Args:
        website_id: the site (see :data:`SITES`).
        year: the ``year`` of a ``data.seasons.seasons[]`` row (START year on site 12, the
            calendar year elsewhere).

    Returns:
        ``year + 1`` for site 12 (2025 -> 2026 = 2025-26), ``year`` for the calendar-year sites,
        ``None`` for an unregistered site, site 236 (no season list) or a non-integer year.

    Raises:
        None: a value that cannot be mapped returns ``None``.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_end_year
            atrium_end_year(12, 2025)   # 2026
            atrium_end_year(14, 2026)   # 2026
    """
    kind = SITES.get(website_id, (None, None))[1] if isinstance(website_id, int) else None
    if kind is None or isinstance(year, bool) or not isinstance(year, int):
        return None
    return year + 1 if kind == "start" else year


# --- documented schemas ---------------------------------------------------------------------------

S, N, F, B = pl.String, pl.Int64, pl.Float64, pl.Boolean
DT, D = pl.Datetime("us"), pl.Date


def _stat_dtype(key: str) -> Any:
    """Dtype of a statistic key: durations / result strings ``String``, rates ``Float64``, the rest ``Int64``."""
    if key in ("minutes", "timeInLead", "minutesPerGame") or key.startswith("resultString"):
        return S
    if key.endswith(("Percentage", "PerGame")) or key == "offensiveRating":
        return F
    return N


def _stats(keys: Iterable[str], prefix: str = "") -> Dict[str, Any]:
    """``{snake column: dtype}`` for statistic keys, plus ``seconds_*`` beside every ISO duration."""
    out: Dict[str, Any] = {}
    for k in keys:
        out[prefix + _col(k)] = _stat_dtype(k)
        if k in ("minutes", "timeInLead"):
            out[prefix + ("seconds_played" if k == "minutes" else "time_in_lead_seconds")] = N
    return out


def _col(key: str) -> str:
    return re.sub(r"\W+", "_", underscore(key)).strip("_").lower()


#: Statistic keys of a box score (union of sites 12 and 14).
BOX_KEYS = (
    "minutes points rebounds reboundsOffensive reboundsDefensive assists steals blocks blocksReceived turnovers "
    "foulsTotal foulsDrawn plusMinus efficiency fieldGoalsMade fieldGoalsAttempted fieldGoalsPercentage "
    "pointsTwoMade pointsTwoAttempted pointsTwoPercentage pointsThreeMade pointsThreeAttempted "
    "pointsThreePercentage freeThrowsMade freeThrowsAttempted freeThrowsPercentage pointsFastBreak"
).split()
_EXTRA_KEYS = "foulsTotal rebounds reboundsDefensive reboundsOffensive turnovers".split()
_TOTAL_KEYS = (
    "biggestLead biggestScoringRun fieldGoalsPercentage freeThrowsPercentage leadChanges pointsFastBreak "
    "pointsFromBench pointsFromTurnover timeInLead turnovers"
).split()
_STANDING_KEYS = (
    "wins losses played standingPoints winPercentage scoredFor scoredAgainst pointDifference resultStringHome "
    "scoredForHome scoredAgainstHome pointDifferenceHome resultStringAway scoredForAway scoredAgainstAway "
    "pointDifferenceAway streak streakHome streakAway"
).split()
_AVG_KEYS = (
    "minutesPerGame pointsPerGame reboundsPerGame reboundsOffensivePerGame reboundsDefensivePerGame assistsPerGame "
    "stealsPerGame blocksPerGame turnoversPerGame foulsPersonalPerGame dunksPerGame efficiency"
).split()
_LOG_KEYS = BOX_KEYS + ["dunks", "foulsPersonal"]

_SIDE_COLS = ("entity_id", "code", "name", "score", "result_status")
_SIDE_TYPES = (S, S, S, N, S)


def _sides(prefixes: Iterable[str] = ("home", "away"), cols: Iterable[str] = _SIDE_COLS) -> Dict[str, Any]:
    types = dict(zip(_SIDE_COLS, _SIDE_TYPES))
    return {f"{p}_{c}": types[c] for p in prefixes for c in cols}


_EVENT = {
    "website_id": N, "fixture_id": S, "season_id": S, "sequence": N, "event_id": S, "period_id": N, "clock": S,
    "clock_seconds": N, "entity_id": S, "side": S, "person_id": S, "person_name": S, "bib": S, "event_type": S,
    "event_sub_type": S, "description": S, "success": B, "success_label": S, "x": F, "y": F, "home_score": N,
    "away_score": N,
}  # fmt: skip

#: The documented columns (name -> dtype, in order) of every parser's frame.
SCHEMAS: Dict[str, Dict[str, Any]] = {
    "seasons": {
        "website_id": N, "season_id": S, "competition_id": S, "competition_name": S, "season_name": S,
        "abbreviation_local": S, "year": N, "season_end_year": N, "is_default_season": B,
    },
    "fixtures": {
        "website_id": N, "season_id": S, "competition_id": S, "competition_name": S, "season_end_year": N,
        "fixture_id": S, "fixture_type": S, "name": S, "round": S, "round_code": S, "pool": S, "series": S,
        "stage_code": S, "status": S, "status_label": S, "start_time_utc": DT, "start_time_local": DT,
        "timezone": S, "times_unconfirmed": B, "venue": S, "venue_id": S, "attendance": N,
        **{f"{p}_{c}": t for p in ("home", "away") for c, t in (
            ("entity_id", S), ("code", S), ("name", S), ("score", N), ("result_place", N), ("result_status", S),
            ("draw", B), ("shootout_attempts", S), ("secondary_score", S))},
    },
    "fixtures_ribbon": {
        "website_id": N, "season_id": S, "season_name": S, "fixture_id": S, "series": S, "stage_name": S,
        "status": S, "status_label": S, "is_final": B, "is_live": B, "times_unconfirmed": B,
        "start_time_utc": DT, "start_time_local": DT,
        **{f"{p}_{c}": t for p in ("home", "away") for c, t in (
            ("entity_id", S), ("name", S), ("score", N), ("result_status", S), ("is_leading", B))},
    },
    "fixture": {
        "website_id": N, "fixture_id": S, "season_id": S, "competition_name": S, "name": S, "fixture_type": S,
        "age_group": S, "status": S, "status_label": S, "start_time_utc": DT, "start_time_local": DT,
        "timezone": S, "venue": S, "venue_id": S, "attendance": N, "external_id": S, "updated": DT,
        "live_data_available": B, "live_video_available": B, "discipline": S, "series": S,
        "number_of_periods": N, "period_length": N, "overtime_allowed": B, "overtime_length": N, "shot_clock": N,
        "max_fouls_personal": N, "fouls_before_bonus": N, "maximum_players_on_court": N, "maximum_roster_size": N,
        **_sides(), "home_captain_person_id": S, "away_captain_person_id": S,
    },
    "fixture_periods": {
        "website_id": N, "fixture_id": S, "season_id": S, "period_id": N, "label": S, "is_overtime": B,
        "is_shootout": B, "started": B, "ended": B, "duration_minutes": N, "elapsed_minutes_before_period": N,
        "home_entity_id": S, "away_entity_id": S, "home_points": N, "away_points": N,
    },
    "box_score": {
        "website_id": N, "fixture_id": S, "season_id": S, "entity_id": S, "side": S, "person_id": S,
        "person_name": S, "bib": S, "position": S, "starter": B, "active": B, "participated": B,
        "did_not_play_reason": S, **_stats(BOX_KEYS),
    },
    "team_box_score": {
        "website_id": N, "fixture_id": S, "season_id": S, "entity_id": S, "side": S, **_stats(BOX_KEYS),
        **_stats(_EXTRA_KEYS, "extra_"), **_stats(_TOTAL_KEYS, "total_"),
    },
    "pbp": _EVENT,
    "shots": _EVENT,
    "standings": {
        "website_id": N, "season_id": S, "season_end_year": N, "group": S, "position": N, "entity_id": S,
        "team_name": S, "is_current_team": B, "logo_url": S, **_stats(_STANDING_KEYS),
    },
    "teams": {"website_id": N, "season_id": S, "entity_id": S, "name": S, "image_url": S},
    "team": {
        "website_id": N, "entity_id": S, "name": S, "color": S, "logo_url": S,
        **{f"{k}_{c}": t for k in ("previous", "next") for c, t in (
            ("fixture_id", S), ("round", S), ("status", S), ("start_time_utc", DT), ("start_time_local", DT),
            ("venue", S), ("attendance", N), ("home_entity_id", S), ("home_name", S), ("home_score", N),
            ("away_entity_id", S), ("away_name", S), ("away_score", N))},
    },
    "players": {
        "website_id": N, "season_id": S, "person_id": S, "name": S, "bib": S, "entity_id": S, "team_name": S,
        "position_code": S, "position_name": S, "nationality": S, "date_of_birth": D, "age": N, "height": N,
        "status": S, "image_url": S,
    },
    "player": {
        "website_id": N, "person_id": S, "name": S, "bib": S, "team_id": S, "team_name": S, "image_url": S,
        "date_of_birth": D, "age": N, "nationality": S, **_stats(["pointsPerGame", "reboundsOffensivePerGame",
                                                                   "reboundsDefensivePerGame"]),
    },
    "player_seasons": {
        "website_id": N, "person_id": S, "season_name": S, "season_year": N, "season_end_year": N, "team_id": S,
        "team_name": S, **_stats(_AVG_KEYS),
    },
    "player_game_log": {
        "website_id": N, "person_id": S, "season_id": S, "date": DT, "opponent_entity_id": S, "opponent_name": S,
        "result": S, "result_label": S, "score": S, **_stats(_LOG_KEYS),
    },
    "leaders": {
        "website_id": N, "season_id": S, "stat_key": S, "stat_name": S, "stat_label": S, "rank": N,
        "entity_id": S, "entity_name": S, "person_id": S, "person_name": S, "value": F, "value_text": S,
    },
    "stat_tables": {
        "website_id": N, "season_id": S, "table_key": S, "table_label": S, "person_id": S, "person_name": S,
        "entity_id": S, "entity_name": S, "stat_key": S, "stat_name": S, "stat_label": S, "value": F,
        "value_text": S,
    },
}  # fmt: skip


# --- helpers --------------------------------------------------------------------------------------

_SELF = re.compile(r"/v1/embed/(\d+)/([a-z_]+)(?:[?#]|$)")
_ISO = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")
_INT = re.compile(r"^-?\d+$")
_MAX_STATE_BYTES = 64 * 1024


def _dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def _data(raw: Any) -> Dict[str, Any]:
    return _dict(_dict(raw).get("data"))


def _site(raw: Any) -> Optional[int]:
    """``website_id`` from the body's ``links.self`` (``.../v1/embed/{site}/{type}``)."""
    m = _SELF.search(str(_dict(_dict(raw).get("links")).get("self") or ""))
    return int(m.group(1)) if m else None


def _id(v: Any) -> Optional[str]:
    """An id as ``String``: a non-empty string as is, an integer by its digits, anything else null."""
    if isinstance(v, str):
        return v or None
    if isinstance(v, int) and not isinstance(v, bool):
        return str(v)
    return None


def _text(v: Any) -> Optional[str]:
    if v is None or v == "":
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, float):  # 123.0 -> "123" (never "123.0"); NaN / inf are not values
        if not math.isfinite(v):
            return None
        return str(int(v)) if v.is_integer() else str(v)
    return json.dumps(v, sort_keys=True, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)


def _num(v: Any) -> Any:
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _int(v: Any) -> Optional[int]:
    """An integer from an int or a digit string (scores arrive as ``"87"``); anything else null."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return int(v) if isinstance(v, str) and _INT.match(v.strip()) else None


def _seconds(v: Any) -> Optional[int]:
    """ISO 8601 duration ``PT{h}H{m}M{s}S`` -> whole seconds (null when not a duration)."""
    m = _ISO.match(v) if isinstance(v, str) else None
    if not m or not any(m.groups()):
        return None
    h, mi, s = m.groups()
    return int(h or 0) * 3600 + int(mi or 0) * 60 + int(float(s or 0))


def _dt(v: Any) -> Optional[datetime]:
    """A naive datetime from ``YYYY-MM-DDTHH:MM:SS`` (an offset, if any, is converted to UTC)."""
    if not isinstance(v, str):
        return None
    try:
        out = datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return out.astimezone(timezone.utc).replace(tzinfo=None) if out.tzinfo else out


def _date(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(v.strip()[:10]) if isinstance(v, str) else None
    except ValueError:
        return None


def _series(name: str, values: List[Any], dtype: Any) -> pl.Series:
    """A column of ``dtype``; a value it cannot hold widens the column (Float64, then String), never nulls it."""
    if dtype is None:
        try:
            return pl.Series(name, values)
        except Exception:
            return pl.Series(name, [_text(v) for v in values], dtype=S)
    for t in (dtype, F, S) if dtype == N else (dtype, S):
        try:
            return pl.Series(name, values if t == dtype else [_text(v) if t == S else v for v in values], dtype=t)
        except Exception:
            continue
    return pl.Series(name, [_text(v) for v in values], dtype=S)


def _frame(rows: List[Dict[str, Any]], schema: Dict[str, Any], return_as_pandas: bool) -> Frame:
    """Rows -> the documented columns (null-padded, typed) followed by any column the schema lacks."""
    extra: List[str] = []
    seen = set(schema)
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                extra.append(k)
    cols = [_series(c, [r.get(c) for r in rows], t) for c, t in schema.items()]
    cols += [_series(c, [r.get(c) for r in rows], None) for c in extra]
    df = pl.DataFrame(cols) if cols else pl.DataFrame()
    return df.to_pandas() if return_as_pandas else df


def _statcols(stats: Any, prefix: str = "") -> Dict[str, Any]:
    """A ``{statKey: value}`` map -> snake columns, with ``seconds_*`` beside every ISO duration."""
    out: Dict[str, Any] = {}
    for k, v in _dict(stats).items():
        out[prefix + _col(k)] = v
        if k in ("minutes", "timeInLead"):
            out[prefix + ("seconds_played" if k == "minutes" else "time_in_lead_seconds")] = _seconds(v)
    return out


def _positional(headers: Any, values: Any) -> Dict[str, Any]:
    """Zip a positional ``statistics`` array with ``headers[].key`` into a ``{statKey: value}`` map."""
    keys = [_dict(h).get("key") for h in _list(headers)]
    return {k: v for k, v in zip(keys, _list(values)) if isinstance(k, str)}


def _link_ids(link: Any) -> Dict[str, Any]:
    """Decode a widget link (``queryParams["~w"]`` or ``"&~w=p~<token>"``) into its state object.

    The token is ``base64url(zlib(JSON))`` of ``{s: seasonId, p: personId, e: entityId, f: fixtureId}``
    and the zlib stream has no final flush. Anything undecodable is ``{}``.
    """
    tok = _dict(_dict(link).get("queryParams")).get("~w") if isinstance(link, dict) else link
    if not isinstance(tok, str) or "~" not in tok:
        return {}
    tok = tok.split("~w=")[-1].split("~", 1)[-1]
    try:
        d = zlib.decompressobj()
        out = d.decompress(base64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4)), _MAX_STATE_BYTES)
        obj = json.loads(out)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _kv(items: Any) -> Dict[str, Any]:
    """``[{key, value}, ...]`` -> ``{snake key: value}`` (items without a string key are skipped)."""
    out: Dict[str, Any] = {}
    for item in _list(items):
        item = _dict(item)
        key = item.get("key")
        if isinstance(key, str):
            out[_col(key)] = item.get("value")
    return out


def _pick_sides(competitors: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """``(home, away)`` competitor objects: by ``isHome``, else by list order."""
    comps = [c for c in _list(competitors) if isinstance(c, dict)]
    home = next((c for c in comps if c.get("isHome") is True), None)
    away = next((c for c in comps if c.get("isHome") is False and c is not home), None)
    rest = [c for c in comps if c is not home and c is not away]
    if home is None:
        home = rest.pop(0) if rest else None
    if away is None:
        away = rest.pop(0) if rest else None
    return _dict(home), _dict(away)


def _side_cols(prefix: str, c: Dict[str, Any]) -> Dict[str, Any]:
    return {
        f"{prefix}_entity_id": _id(c.get("entityId")),
        f"{prefix}_code": _text(c.get("code")),
        f"{prefix}_name": _text(c.get("name")),
        f"{prefix}_score": _int(c.get("score")),
        f"{prefix}_result_status": _text(c.get("resultStatus")),
    }


def _season_index(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """``{season_id: {competition_id, competition_name, season_name, year, abbreviation_local}}`` from ``data.seasons``."""
    seasons = _dict(data.get("seasons"))
    comps = _dict(seasons.get("competitions"))
    out: Dict[str, Dict[str, Any]] = {}
    for s in _list(seasons.get("seasons")):
        s = _dict(s)
        sid = _id(s.get("seasonId"))
        if sid is None:
            continue
        cid = _id(s.get("competitionId"))
        out[sid] = {
            "competition_id": cid,
            "competition_name": _text(_dict(comps.get(cid or "")).get("name")),
            "season_name": _text(s.get("nameLocal")),
            "abbreviation_local": _text(s.get("abbreviationLocal")),
            "year": _int(s.get("year")),
        }
    return out


def _fixture_id(data: Dict[str, Any]) -> Optional[str]:
    return _id(_dict(data.get("fixture")).get("fixtureId"))


def _season_id(data: Dict[str, Any]) -> Optional[str]:
    return _id(data.get("seasonId")) or _id(_dict(data.get("fixture")).get("seasonId"))


# --- seasons and fixtures -------------------------------------------------------------------------


def parse_atrium_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """The season list of a site (``data.seasons`` of a ``fixtures`` body) -> one row per season.

    A season is one competition in one year. ``year`` is the site's own key (START year on
    site 12, calendar year elsewhere); ``season_end_year`` is the sdv END year
    (:func:`atrium_end_year`). Site 236 lists no seasons, so its frame is empty.

    Args:
        raw: a ``fixtures`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(season_id)``; ``is_default_season`` marks the season the body is about.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_fixtures, parse_atrium_seasons
            seasons = parse_atrium_seasons(atrium_fixtures(website_id=12, return_parsed=False))
    """
    data, site = _data(raw), _site(raw)
    default = _id(data.get("seasonId"))
    rows = [
        {
            "website_id": site, "season_id": sid, **{k: v for k, v in s.items()},
            "season_end_year": atrium_end_year(site, s["year"]), "is_default_season": sid == default,
        }
        for sid, s in _season_index(data).items()
    ]  # fmt: skip
    return _frame(rows, SCHEMAS["seasons"], return_as_pandas)


def parse_atrium_fixtures(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixtures`` body -> one row per game, home and away side by side.

    ``season_id`` is the season the page is about (the site default when none was asked);
    ``competition_*`` and ``season_end_year`` come from the body's season list. Scores are
    ``Int64`` (the API sends strings); ``start_time_utc`` / ``start_time_local`` are naive
    datetimes (UTC and the venue's ``timezone``).

    Args:
        raw: a ``fixtures`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``fixture_id``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_fixtures
            games = atrium_fixtures(website_id=14)
            games.select("fixture_id", "home_name", "home_score", "away_name", "away_score")
    """
    data, site = _data(raw), _site(raw)
    sid = _id(data.get("seasonId"))
    info = _season_index(data).get(sid or "", {})
    rows = []
    for f in _list(data.get("fixtures")):
        f = _dict(f)
        home, away = _pick_sides(f.get("competitors"))
        st = _dict(f.get("status"))
        row = {
            "website_id": site, "season_id": sid, "competition_id": info.get("competition_id"),
            "competition_name": info.get("competition_name"),
            "season_end_year": atrium_end_year(site, info.get("year")),
            "fixture_id": _id(f.get("fixtureId")), "fixture_type": _text(f.get("fixtureType")),
            "name": _text(f.get("name")), "round": _text(f.get("round")), "round_code": _text(f.get("roundCode")),
            "pool": _text(f.get("pool")), "series": _text(f.get("series")), "stage_code": _text(f.get("stageCode")),
            "status": _text(st.get("value")), "status_label": _text(st.get("label")),
            "start_time_utc": _dt(f.get("startTimeUTC")), "start_time_local": _dt(f.get("startTimeLocal")),
            "timezone": _text(f.get("timezone")), "times_unconfirmed": f.get("timesUnconfirmed"),
            "venue": _text(f.get("venue")), "venue_id": _id(f.get("venueId")), "attendance": _int(f.get("attendance")),
        }  # fmt: skip
        for p, c in (("home", home), ("away", away)):
            row.update(_side_cols(p, c))
            row.update(
                {
                    f"{p}_result_place": _int(c.get("resultPlace")), f"{p}_draw": c.get("draw"),
                    f"{p}_shootout_attempts": _text(c.get("shootOutAttempts")),
                    f"{p}_secondary_score": _text(c.get("secondaryScore")),
                }
            )  # fmt: skip
        rows.append(row)
    return _frame(rows, SCHEMAS["fixtures"], return_as_pandas)


def parse_atrium_fixtures_ribbon(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixtures_ribbon`` body (recent and upcoming games across a site's competitions) -> one row per game.

    The ribbon is empty on site 12 off-season. Its scores are integers; ``start_time_local``
    is the ribbon's ``fixture.date`` (a local time, no offset; unverified).

    Args:
        raw: a ``fixtures_ribbon`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``fixture_id``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_fixtures_ribbon
            atrium_fixtures_ribbon(website_id=14)
    """
    data, site = _data(raw), _site(raw)
    rows = []
    for item in _list(data.get("fixtures")):
        item = _dict(item)
        f, st = _dict(item.get("fixture")), _dict(_dict(item.get("fixture")).get("status"))
        home, away = _pick_sides(item.get("competitors"))
        row = {
            "website_id": site, "season_id": _id(f.get("seasonId")), "season_name": _text(f.get("season")),
            "fixture_id": _id(f.get("fixtureId")), "series": _text(f.get("series")),
            "stage_name": _text(f.get("stageName")), "status": _text(st.get("value")),
            "status_label": _text(st.get("label")), "is_final": f.get("isFinal"), "is_live": f.get("isLive"),
            "times_unconfirmed": f.get("timesUnconfirmed"), "start_time_utc": _dt(f.get("startTimeUTC")),
            "start_time_local": _dt(f.get("date")),
        }  # fmt: skip
        for p, c in (("home", home), ("away", away)):
            row.update(
                {
                    f"{p}_entity_id": _id(c.get("entityId")), f"{p}_name": _text(c.get("name")),
                    f"{p}_score": _int(c.get("score")), f"{p}_result_status": _text(c.get("resultStatus")),
                    f"{p}_is_leading": c.get("isLeading"),
                }
            )  # fmt: skip
        rows.append(row)
    return _frame(rows, SCHEMAS["fixtures_ribbon"], return_as_pandas)


# --- one game (fixture_detail) --------------------------------------------------------------------


def parse_atrium_fixture(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixture_detail`` body -> one row: the game header, its rules and the captains.

    The rules (``number_of_periods``, ``period_length`` in minutes, ``overtime_length``,
    ``shot_clock`` in seconds, ...) come from the game's ``profile``. ``*_captain_person_id`` is
    the person id of each team's captain. ``attendance`` is null for games without one.

    Args:
        raw: a ``fixture_detail`` body (any ``sub``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per game (``fixture_id``).

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_fixture
            atrium_fixture(website_id=14, fixture_id="0dc631bd-c34c-11f1-8d16-4d5a21afb79d")
    """
    data = _data(raw)
    f = _dict(data.get("fixture"))
    if not f:
        return _frame([], SCHEMAS["fixture"], return_as_pandas)
    prof = _dict(f.get("profile"))
    home, away = _pick_sides(f.get("competitors"))
    caps = _dict(f.get("captains"))
    row = {
        "website_id": _site(raw), "fixture_id": _id(f.get("fixtureId")), "season_id": _season_id(data),
        "competition_name": _text(f.get("competitionName")), "name": _text(f.get("name")),
        "fixture_type": _text(f.get("fixtureType")), "age_group": _text(f.get("ageGroup")),
        "status": _text(f.get("status")), "status_label": _text(f.get("statusLabel")),
        "start_time_utc": _dt(f.get("startTimeUTC")), "start_time_local": _dt(f.get("startTimeLocal")),
        "timezone": _text(f.get("timezone")), "venue": _text(f.get("venue")), "venue_id": _id(f.get("venueId")),
        "attendance": _int(f.get("attendance")), "external_id": _text(f.get("externalId")),
        "updated": _dt(f.get("updated")), "live_data_available": f.get("liveDataAvailable"),
        "live_video_available": f.get("liveVideoAvailable"), "discipline": _text(f.get("discipline")),
        "series": _text(f.get("series")), "number_of_periods": _int(prof.get("numberOfPeriods")),
        "period_length": _int(prof.get("periodLength")), "overtime_allowed": prof.get("overtimeAllowed"),
        "overtime_length": _int(prof.get("overtimeLength")), "shot_clock": _int(prof.get("shotClock")),
        "max_fouls_personal": _int(prof.get("maxFoulsPersonal")),
        "fouls_before_bonus": _int(prof.get("foulsBeforeBonus")),
        "maximum_players_on_court": _int(prof.get("maximumPlayersOnCourt")),
        "maximum_roster_size": _int(prof.get("maximumRosterSize")),
        **_side_cols("home", home), **_side_cols("away", away),
        "home_captain_person_id": _id(caps.get(_id(home.get("entityId")) or "")),
        "away_captain_person_id": _id(caps.get(_id(away.get("entityId")) or "")),
    }  # fmt: skip
    return _frame([row], SCHEMAS["fixture"], return_as_pandas)


def parse_atrium_fixture_periods(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixture_detail`` body -> one row per period with the points each team scored in it.

    ``home_points`` / ``away_points`` are the points scored IN that period (``teamScore``), not
    the running score. Overtime periods are ``period_id`` 11, 12, ... (not 5).

    Args:
        raw: a ``fixture_detail`` body (any ``sub``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(fixture_id, period_id)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_fixture_periods
            atrium_fixture_periods(website_id=14, fixture_id="0dc631bd-c34c-11f1-8d16-4d5a21afb79d")
    """
    data, site = _data(raw), _site(raw)
    home, away = _pick_sides(_dict(data.get("fixture")).get("competitors"))
    hid, aid = _id(home.get("entityId")), _id(away.get("entityId"))
    rows = []
    for p in _list(data.get("periodData")):
        p = _dict(p)
        score = _dict(p.get("teamScore"))
        rows.append(
            {
                "website_id": site,
                "fixture_id": _fixture_id(data),
                "season_id": _season_id(data),
                "period_id": _int(p.get("periodId")),
                "label": _text(_dict(p.get("labels")).get("shortLabel")),
                "is_overtime": p.get("isOvertime"),
                "is_shootout": p.get("isShootout"),
                "started": p.get("started"),
                "ended": p.get("ended"),
                "duration_minutes": _int(p.get("durationMinutes")),
                "elapsed_minutes_before_period": _int(p.get("elapsedMinutesBeforePeriod")),
                "home_entity_id": hid,
                "away_entity_id": aid,
                "home_points": _int(score.get(hid or "")),
                "away_points": _int(score.get(aid or "")),
            }  # fmt: skip
        )
    return _frame(rows, SCHEMAS["fixture_periods"], return_as_pandas)


def parse_atrium_box_score(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixture_detail`` (``sub=statistics``) body -> one row per rostered player.

    A player who did not play has ``participated`` false and null statistics. The statistic set is
    configured per site, so columns a site omits are null (LNB has ``blocks_received`` and
    ``plus_minus`` but no field-goal totals; LNBP the reverse). ``minutes`` is the ISO duration as
    sent and ``seconds_played`` its value. Percentages are 0-100. ``side`` is ``home`` or ``away``.

    Args:
        raw: a ``fixture_detail`` body with ``sub=statistics``.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(fixture_id, person_id)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_box_score
            box = atrium_box_score(website_id=14, fixture_id="0dc631bd-c34c-11f1-8d16-4d5a21afb79d")
            box.filter(pl.col("participated") == True).select("person_name", "points")
    """
    data = _data(raw)
    base = _dict(_dict(_dict(data.get("statistics")).get("data")).get("base"))
    rows = []
    for side in ("home", "away"):
        for group in _list(_dict(base.get(side)).get("persons")):
            for r in _list(_dict(group).get("rows")):
                r = _dict(r)
                pos = r.get("position")
                rows.append(
                    {
                        "website_id": _site(raw),
                        "fixture_id": _fixture_id(data),
                        "season_id": _season_id(data),
                        "entity_id": _id(r.get("entityId")),
                        "side": side,
                        "person_id": _id(r.get("personId")),
                        "person_name": _text(r.get("personName")),
                        "bib": _text(r.get("bib")),
                        "position": _text(_dict(pos).get("code") if isinstance(pos, dict) else pos),
                        "starter": r.get("starter"),
                        "active": r.get("active"),
                        "participated": r.get("participated"),
                        "did_not_play_reason": _text(r.get("didNotPlayReason")),
                        **_statcols(r.get("statistics")),
                    }  # fmt: skip
                )
    return _frame(rows, SCHEMAS["box_score"], return_as_pandas)


def parse_atrium_team_box_score(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixture_detail`` (``sub=statistics``) body -> two rows, one per team.

    The team line is the API's ``entity`` block (minutes are the sum over players, ``PT200M``);
    ``extra_*`` are the team-level counts not credited to a player (team rebounds and turnovers)
    and ``total_*`` the game-level aggregates (``total_biggest_lead``, ``total_lead_changes``,
    ``total_time_in_lead`` ...). Columns a site omits are null.

    Args:
        raw: a ``fixture_detail`` body with ``sub=statistics``.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(fixture_id, entity_id)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_team_box_score
            atrium_team_box_score(website_id=12, fixture_id="5423072b-6715-11f0-bd8b-9ffadebc148f")
    """
    data = _data(raw)
    base = _dict(_dict(_dict(data.get("statistics")).get("data")).get("base"))
    comps = dict(zip(("home", "away"), _pick_sides(_dict(data.get("fixture")).get("competitors"))))
    rows = []
    for side in ("home", "away"):
        blk = _dict(base.get(side))
        if not blk:
            continue
        first: Any = next((r for g in _list(blk.get("persons")) for r in _list(_dict(g).get("rows"))), {})
        rows.append(
            {
                "website_id": _site(raw),
                "fixture_id": _fixture_id(data),
                "season_id": _season_id(data),
                "entity_id": _id(comps[side].get("entityId")) or _id(_dict(first).get("entityId")),
                "side": side,
                **_statcols(blk.get("entity")),
                **_statcols(blk.get("extra"), "extra_"),
                **_statcols(blk.get("totalEntityStats"), "total_"),
            }  # fmt: skip
        )
    return _frame(rows, SCHEMAS["team_box_score"], return_as_pandas)


def _events(raw: Any, key: str, schema: str, return_as_pandas: bool) -> Frame:
    data, site = _data(raw), _site(raw)
    home, away = _pick_sides(_dict(data.get("fixture")).get("competitors"))
    hid, aid = _id(home.get("entityId")), _id(away.get("entityId"))
    rows, n = [], 0
    for p in _list(data.get("periodData")):
        for e in _list(_dict(p).get(key)):
            e = _dict(e)
            n += 1
            ent, sc = _id(e.get("entityId")), _dict(e.get("scores"))
            rows.append(
                {
                    "website_id": site,
                    "fixture_id": _fixture_id(data),
                    "season_id": _season_id(data),
                    "sequence": n,
                    "event_id": _id(e.get("eventId")),
                    "period_id": _int(e.get("periodId")),
                    "clock": _text(e.get("clock")),
                    "clock_seconds": _seconds(e.get("clock")),
                    "entity_id": ent,
                    "side": "home" if ent == hid and ent else "away" if ent == aid and ent else None,
                    "person_id": _id(e.get("personId")),
                    "person_name": _text(e.get("name")),
                    "bib": _text(e.get("bib")),
                    "event_type": _text(e.get("eventType")),
                    "event_sub_type": _text(e.get("eventSubType")),
                    "description": _text(e.get("desc")),
                    "success": e.get("success"),
                    "success_label": _text(e.get("successString")),
                    "x": _num(e.get("x")),
                    "y": _num(e.get("y")),
                    "home_score": _int(sc.get(hid or "")),
                    "away_score": _int(sc.get(aid or "")),
                }  # fmt: skip
            )
    return _frame(rows, SCHEMAS[schema], return_as_pandas)


def parse_atrium_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixture_detail`` (``sub=pbp``) body -> one row per play-by-play event, in game order.

    ``sequence`` is the 1-based position in the body (periods in order, events chronological
    within a period). ``clock`` is the time REMAINING in the period as sent (``PT10M0S`` at the
    start) and ``clock_seconds`` its value. ``home_score`` / ``away_score`` are the running score
    after the event. ``x`` / ``y`` (shots only) are percent of the court (0-100): the home team
    attacks the low-x basket for the whole game; which sideline is y = 0 is unverified. Team
    events (time-outs, team rebounds) have no ``person_id``. Overtime is ``period_id`` 11+.

    Args:
        raw: a ``fixture_detail`` body with ``sub=pbp``.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``event_id``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_pbp
            pbp = atrium_pbp(website_id=14, fixture_id="0dc631bd-c34c-11f1-8d16-4d5a21afb79d")
            pbp.filter(pl.col("event_type") == "3pt").select("clock", "person_name", "success")
    """
    return _events(raw, "events", "pbp", return_as_pandas)


def parse_atrium_shots(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``fixture_detail`` (``sub=shot_chart``) body -> one row per 2pt / 3pt attempt with its location.

    The shot chart holds field-goal attempts only (no free throws); ``event_id`` joins the same
    event in :func:`parse_atrium_pbp`. ``x`` / ``y`` are percent of the court (0-100, origin at a
    corner, x along the 28 m length). LNBP and NBL1 clamp rim attempts to x = 5 / 95. Columns are
    those of the play-by-play frame.

    Args:
        raw: a ``fixture_detail`` body with ``sub=shot_chart``.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per shot (``event_id``).

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_shots
            shots = atrium_shots(website_id=14, fixture_id="0dc631bd-c34c-11f1-8d16-4d5a21afb79d")
            shots.group_by("person_name").agg(pl.col("success").mean())
    """
    return _events(raw, "shots", "shots", return_as_pandas)


# --- tables of a season ---------------------------------------------------------------------------


def parse_atrium_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``standings`` or ``standings_fragment`` body -> one row per team and group.

    The columns are configured per site: LNB has home / away splits and result strings, LNBP
    ``played`` and ``standing_points``, the compact fragment only ``wins`` / ``losses`` /
    ``played``. ``win_percentage`` is a 0-1 fraction. ``group`` is the table's label (empty for a
    single table).

    Args:
        raw: a ``standings`` or ``standings_fragment`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(season_id, group, entity_id)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_standings
            atrium_standings(website_id=12, season=2026, competition_id="3f4064bb-51ad-11f0-aaaf-2923c944b404")
    """
    data, site = _data(raw), _site(raw)
    sid = _id(data.get("seasonId"))
    year = _season_index(data).get(sid or "", {}).get("year")
    rows = []
    for g in _list(data.get("standings")):
        g = _dict(g)
        for r in _list(g.get("rows")):
            r = _dict(r)
            t = _dict(r.get("team"))
            rows.append(
                {
                    "website_id": site,
                    "season_id": sid,
                    "season_end_year": atrium_end_year(site, year),
                    "group": _text(g.get("label")),
                    "position": _int(r.get("position")),
                    "entity_id": _id(t.get("entityId")),
                    "team_name": _text(t.get("name")),
                    "is_current_team": t.get("isCurrentTeam"),
                    "logo_url": _text(t.get("logoURL")),
                    **{_col(k): v for k, v in _dict(r.get("results")).items()},
                }  # fmt: skip
            )
    return _frame(rows, SCHEMAS["standings"], return_as_pandas)


def parse_atrium_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """An ``entities`` body -> one row per team of the season.

    Args:
        raw: an ``entities`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(season_id, entity_id)``; ``entity_id`` is stable across seasons.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_teams
            atrium_teams(website_id=12, season=2026, competition_id="3f4064bb-51ad-11f0-aaaf-2923c944b404")
    """
    data, site = _data(raw), _site(raw)
    rows = [
        {
            "website_id": site,
            "season_id": _id(data.get("seasonId")),
            "entity_id": _id(_dict(t).get("entityId")),
            "name": _text(_dict(t).get("name")),
            "image_url": _text(_dict(t).get("imageURL")),
        }  # fmt: skip
        for t in _list(data.get("teams"))
    ]
    return _frame(rows, SCHEMAS["teams"], return_as_pandas)


def parse_atrium_team(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """An ``entity_detail`` (team page, summary tab) body -> one row: the team and its last and next game.

    ``previous_*`` is the last played game and ``next_*`` the next scheduled one (null when the
    page shows none). ``entity_id`` is decoded from the page's tab links (the body does not
    echo it elsewhere); ``color`` is a hex string with its ``#``.

    Args:
        raw: an ``entity_detail`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row for the team.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_team
            atrium_team(website_id=12, entity_id="f43b1b0f-55bc-11f0-84d9-01499d2f0a07")
    """
    data = _data(raw)
    team = _dict(data.get("team"))
    if not team:
        return _frame([], SCHEMAS["team"], return_as_pandas)
    banner = _dict(team.get("banner"))
    tabs = _list(data.get("tabs"))
    feat = _dict(_dict(team.get("summary")).get("featuredFixtures"))
    row: Dict[str, Any] = {
        "website_id": _site(raw), "entity_id": _id(_link_ids(_dict(tabs[0]).get("link")).get("e")) if tabs else None,
        "name": _text(banner.get("name")), "color": _text(banner.get("color")), "logo_url": _text(banner.get("logoURL")),
    }  # fmt: skip
    for k in ("previous", "next"):
        f = _dict(feat.get(k))
        comp = _dict(f.get("competitors"))
        h, a = _dict(comp.get("home")), _dict(comp.get("away"))
        row.update(
            {
                f"{k}_fixture_id": _id(f.get("fixtureId")),
                f"{k}_round": _text(f.get("round")),
                f"{k}_status": _text(f.get("status")),
                f"{k}_start_time_utc": _dt(f.get("startTimeUTC")),
                f"{k}_start_time_local": _dt(f.get("startTimeLocal")),
                f"{k}_venue": _text(_dict(f.get("venue")).get("name")),
                f"{k}_attendance": _int(f.get("attendance")),
                f"{k}_home_entity_id": _id(h.get("entityId")),
                f"{k}_home_name": _text(h.get("name")),
                f"{k}_home_score": _int(h.get("score")),
                f"{k}_away_entity_id": _id(a.get("entityId")),
                f"{k}_away_name": _text(a.get("name")),
                f"{k}_away_score": _int(a.get("score")),
            }  # fmt: skip
        )
    return _frame([row], SCHEMAS["team"], return_as_pandas)


def parse_atrium_players(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``persons`` body -> one row per player of the season, with bio columns.

    ``height`` is an integer where the site gives one (centimetres; unit unverified) and null
    otherwise. ``position_code`` / ``position_name`` follow the site locale (the name is
    translated, the code is not). ``person_id`` is stable across seasons and sites.

    Args:
        raw: a ``persons`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(season_id, person_id)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_players
            atrium_players(website_id=12, season=2026, competition_id="3f4064bb-51ad-11f0-aaaf-2923c944b404")
    """
    data, site = _data(raw), _site(raw)
    rows = []
    for p in _list(_dict(data.get("persons")).get("rows")):
        p = _dict(p)
        pos = _dict(p.get("position"))
        rows.append(
            {
                "website_id": site,
                "season_id": _id(data.get("seasonId")),
                "person_id": _id(p.get("id")),
                "name": _text(p.get("name")),
                "bib": _text(p.get("bib")),
                "entity_id": _id(p.get("entityId")),
                "team_name": _text(p.get("teamName")),
                "position_code": _text(pos.get("code")),
                "position_name": _text(pos.get("name")),
                "nationality": _text(p.get("nationality")),
                "date_of_birth": _date(p.get("Date of birth")),
                "age": _int(p.get("age")),
                "height": _int(p.get("height")),
                "status": _text(p.get("status")),
                "image_url": _text(p.get("imageURL")),
            }  # fmt: skip
        )
    return _frame(rows, SCHEMAS["players"], return_as_pandas)


# --- one player (person_detail) -------------------------------------------------------------------


def parse_atrium_player(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``person_detail`` body (no ``sub``) -> one row: the player's bio and season averages.

    The averages (``points_per_game`` ...) are those the site shows on the player card, for the
    season asked. Info items the schema does not list (height, position, ...) are appended as
    text columns named after their key.

    Args:
        raw: a ``person_detail`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row for the player (``person_id``).

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_player
            atrium_player(website_id=12, person_id="4458f165-f865-11f0-8ebf-539c931c1654")
    """
    p = _dict(_data(raw).get("person"))
    if not p:
        return _frame([], SCHEMAS["player"], return_as_pandas)
    info, avgs = _kv(p.get("info")), _kv(p.get("averages"))
    info["date_of_birth"] = _date(info.pop("date_of_birth", None))
    info["age"] = _int(info.get("age"))
    row = {
        "website_id": _site(raw), "person_id": _id(p.get("id")), "name": _text(p.get("name")),
        "bib": _text(p.get("bib")), "team_id": _id(p.get("teamId")), "team_name": _text(p.get("teamName")),
        "image_url": _text(p.get("imageURL")), **info, **avgs,
    }  # fmt: skip
    return _frame([row], SCHEMAS["player"], return_as_pandas)


def parse_atrium_player_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``person_detail`` body (no ``sub``) -> one row per competition-season the player has a line in.

    ``season_year`` is the site's own year and ``season_end_year`` the sdv END year. The
    statistics are the per-game averages the site configures (``Float64`` rates, ISO
    ``minutes_per_game``); a column a site omits is null.

    Args:
        raw: a ``person_detail`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(person_id, season_name, team_id)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_player_seasons
            atrium_player_seasons(website_id=12, person_id="4458f165-f865-11f0-8ebf-539c931c1654")
    """
    data, site = _data(raw), _site(raw)
    stats = _dict(data.get("statistics"))
    rows = []
    for r in _list(stats.get("rows")):
        r = _dict(r)
        t = _dict(r.get("team"))
        rows.append(
            {
                "website_id": site,
                "person_id": _id(_dict(data.get("person")).get("id")),
                "season_name": _text(r.get("seasonName")),
                "season_year": _int(r.get("seasonYear")),
                "season_end_year": atrium_end_year(site, _int(r.get("seasonYear"))),
                "team_id": _id(t.get("id")),
                "team_name": _text(t.get("name")),
                **{_col(k): v for k, v in _positional(stats.get("headers"), r.get("statistics")).items()},
            }  # fmt: skip
        )
    return _frame(rows, SCHEMAS["player_seasons"], return_as_pandas)


def parse_atrium_player_game_log(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``person_detail`` (``sub=log``) body -> one row per game the player appeared in.

    ``opponent_entity_id`` / ``opponent_name`` is the other team. ``score`` is the site's text
    (``"88 - 91"``); which side is listed first is unverified, so it is not split. ``date`` is the
    local start time (no offset). The log carries no fixture id: join on date and opponent.

    Args:
        raw: a ``person_detail`` body with ``sub=log``.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per game.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_player_game_log
            atrium_player_game_log(website_id=12, person_id="4458f165-f865-11f0-8ebf-539c931c1654")
    """
    data, site = _data(raw), _site(raw)
    log = _dict(data.get("log"))
    rows = []
    for r in _list(log.get("rows")):
        r = _dict(r)
        res = _dict(r.get("result"))
        rows.append(
            {
                "website_id": site,
                "person_id": _id(_dict(data.get("person")).get("id")),
                "season_id": _id(log.get("activeSeason")),
                "date": _dt(r.get("date")),
                "opponent_entity_id": _id(r.get("entityId")),
                "opponent_name": _text(r.get("name")),
                "result": _text(res.get("value")),
                "result_label": _text(res.get("label")),
                "score": _text(r.get("score")),
                **{_col(k): v for k, v in _positional(log.get("headers"), r.get("statistics")).items()},
            }  # fmt: skip
        )
    return _frame(rows, SCHEMAS["player_game_log"], return_as_pandas)


# --- leaders and statistic tables -----------------------------------------------------------------


def _value(v: Any) -> Tuple[Any, Optional[str]]:
    """``(numeric, text)``: a number goes to ``value``, anything else (ISO durations) to ``value_text``."""
    return (float(v), None) if isinstance(v, (int, float)) and not isinstance(v, bool) else (None, _text(v))


def parse_atrium_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``statistics_leaders`` body -> one row per leader per statistic (top 5 of each).

    ``rank`` is the 1-based position within the statistic. ``value`` is numeric; a duration
    statistic (``minutesPerGame``) lands in ``value_text`` as an ISO string. The
    leaders carry their ``person_id`` and ``entity_id`` directly.

    Args:
        raw: a ``statistics_leaders`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(season_id, stat_key, rank)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_leaders
            atrium_leaders(website_id=12, season=2026, competition_id="3f4064bb-51ad-11f0-aaaf-2923c944b404")
    """
    data, site = _data(raw), _site(raw)
    rows = []
    for table in _list(data.get("statistics")):
        table = _dict(table)
        h = _dict(table.get("header"))
        for i, d in enumerate(_list(table.get("data")), start=1):
            d = _dict(d)
            ent, per = _dict(d.get("entity")), _dict(d.get("person"))
            val, txt = _value(d.get("value"))
            rows.append(
                {
                    "website_id": site,
                    "season_id": _id(data.get("seasonId")),
                    "stat_key": _text(h.get("key")),
                    "stat_name": _text(h.get("name")),
                    "stat_label": _text(h.get("code")),
                    "rank": i,
                    "entity_id": _id(ent.get("entityId")),
                    "entity_name": _text(ent.get("name")),
                    "person_id": _id(per.get("personId")),
                    "person_name": _text(per.get("name")),
                    "value": val,
                    "value_text": txt,
                }  # fmt: skip
            )
    return _frame(rows, SCHEMAS["leaders"], return_as_pandas)


def parse_atrium_stat_tables(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """A ``statistics_persons`` / ``statistics_entities`` / ``statistics_competition_*`` body in long form.

    Each body holds several tables (``averages``, ``shootingstatistics``, ``totals``, ...) whose
    statistic set the site configures, so the rows are one per subject, table and statistic:
    ``stat_key`` (``pointsPerGame``) with its numeric ``value`` or, for ISO durations, ``value_text``.
    Pivot on ``stat_key`` for a wide table. The player tables carry names only; ``person_id`` and
    ``entity_id`` are decoded from the widget link beside each name (null when it is missing, as
    in the ``statistics_competition_*`` bodies). Percentages are 0-100.

    Args:
        raw: a statistics body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``(table_key, person_id or entity_name, stat_key)``.

    Raises:
        None: a malformed body returns a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_player_stats
            long = atrium_player_stats(website_id=12, season=2026, competition_id="3f4064bb-51ad-11f0-aaaf-2923c944b404")
            long.filter(pl.col("stat_key") == "pointsPerGame").sort("value", descending=True)
    """
    data, site = _data(raw), _site(raw)
    rows = []
    for table in _list(data.get("statistics")):
        table = _dict(table)
        label = _dict(table.get("label"))
        heads = [_dict(h) for h in _list(table.get("headers"))]
        for subj in _list(table.get("statistics")):
            subj = _dict(subj)
            per, ent = _dict(subj.get("person")), _dict(subj.get("entity"))
            pid = _id(per.get("personId")) or _id(_link_ids(per.get("link")).get("p"))
            eid = _id(ent.get("entityId")) or _id(_link_ids(ent.get("link")).get("e"))
            for h, v in zip(heads, _list(subj.get("statistics"))):
                val, txt = _value(v)
                rows.append(
                    {
                        "website_id": site,
                        "season_id": _id(data.get("seasonId")),
                        "table_key": _text(label.get("key")),
                        "table_label": _text(label.get("text")),
                        "person_id": pid,
                        "person_name": _text(per.get("name")),
                        "entity_id": eid,
                        "entity_name": _text(ent.get("name")),
                        "stat_key": _text(h.get("key")),
                        "stat_name": _text(h.get("name")),
                        "stat_label": _text(h.get("code")),
                        "value": val,
                        "value_text": txt,
                    }  # fmt: skip
                )
    return _frame(rows, SCHEMAS["stat_tables"], return_as_pandas)
