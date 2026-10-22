"""Parsers for the generated ``wjbl`` wrappers (W League, Women's Japan Basketball League, ``api.wjbl.01core.app/api``).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns, and a zero-row frame (never an exception) for an empty or malformed body. The zero-row frame
carries the parser's documented returns schema (:data:`SCHEMAS`, derived from the FULL bodies in
``tests/fixtures/wjbl/full``), so two empty frames, or an empty one and a populated one, stack. Every column has one
dtype for good: ids and flags ``Int64``, rates ``Float64``, everything else ``String``.

**Input.** ``wjbl_runtime._get`` stamps the request under ``_request`` and wraps a bare list as ``{"items": [...]}``;
the parsers also accept the bare list or object. One body can feed several frames (the game header feeds ``wjbl_game``
and ``wjbl_period_scores``, the box score ``wjbl_box_score`` and ``wjbl_team_box_score``, the standings grid
``wjbl_team_standings`` and ``wjbl_head_to_head``, the roster ``wjbl_roster`` and ``wjbl_staff``), so fetch it once with
``return_parsed=False`` and parse it as many ways as needed.

**Row rule.** Wire names are kept (``home_season_team_name``, ``player_id1``); a nested object flattens into
``<parent>_<child>`` columns. Maps keyed by position or id (``seasons``, ``teams``, ``periods``) become rows. Strings
with a counter or a unit become numbers (``'6本'`` -> 6, ``'23勝5負'`` -> 23 wins and 5 losses, ``'40.92%'`` -> 40.92,
``'3-4-7'`` -> 3, 4 and 7). The wire spells two keys wrong (``home_trun_over``, ``home_max_differenct``); the frames
spell them ``home_turnovers`` and ``home_max_difference``.

**Personal data.** Contact details are never output: keys naming a contact (``contact``, ``contact_url``) and the
player's blood type (``player_blood``) are dropped, and phone numbers and e-mail addresses inside any free text are
replaced by ``[redacted-phone]`` and ``[redacted-email]``. Player bio fields and referee names are public and stay.

**Season.** The sdv season is the END year of a league edition (``'25-26 Wリーグ'`` -> 2026, ``'99-00'`` -> 2000);
``season`` is null for an all-star game, cup or camp. ``season_id`` is a phase inside an edition, not a year.

**Id key spaces.** Every id column is ``Int64``:

* ``league_id``: a competition edition (every W League season, all-star game, cup and camp; 45 = 2025-26).
* ``season_id``: a phase inside one ``league_id`` (169 = 2025-26 regular season Premier division, 170 = Future
  division, 176 = playoff final). ``season_team_id`` is one team in one phase.
* ``play_schedule_id``: a game, ONE global sequence across every edition (4243 = 2013-14 final, 7987 = 2025-26 final).
* ``team_id``: the franchise, stable across seasons and sponsor renames (13 = Toyota Antelopes). All-star sides have
  their own ids. ``player_id``: the person, stable across seasons and teams (the ``pi=`` of the site's player page);
  in the play-by-play ``player_id1`` / ``player_id2`` are the first and second player of the action.
* ``coach_id``: the head coach.

**Sides.** ``team_side`` is ``home`` or ``away``; in the play-by-play ``home_away`` is 1 for home and 2 for away.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.wjbl.wjbl_runtime import season_of_title
from sportsdataverse.errors import _data_only

__all__ = [
    "parse_wjbl_box_score",
    "parse_wjbl_game",
    "parse_wjbl_game_info",
    "parse_wjbl_game_months",
    "parse_wjbl_head_to_head",
    "parse_wjbl_leagues",
    "parse_wjbl_pbp",
    "parse_wjbl_period_scores",
    "parse_wjbl_player",
    "parse_wjbl_player_period_leaders",
    "parse_wjbl_roster",
    "parse_wjbl_schedule",
    "parse_wjbl_staff",
    "parse_wjbl_standing_seasons",
    "parse_wjbl_team_box_score",
    "parse_wjbl_team_period_stats",
    "parse_wjbl_team_ranking",
    "parse_wjbl_team_standings",
    "parse_wjbl_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

_REQUEST = "_request"
_PHONE = re.compile(
    r"(?<![0-9０-９])[(（]?[0０][0-9０-９]{1,4}[)）]?[-－‐ー−― 　]?[0-9０-９]{1,4}[-－‐ー−― 　]?[0-9０-９]{3,4}(?![0-9０-９])"
)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_CONTACT_KEY = re.compile(r"contact")
_NUM = re.compile(r"-?\d+(?:\.\d+)?")


# --- schemas ------------------------------------------------------------------------------------------------------


def _spec(text: str) -> Dict[str, Any]:
    """``'a:i b:f c:s'`` -> ``{'a': Int64, 'b': Float64, 'c': String}`` (order kept)."""
    kinds = {"i": pl.Int64, "f": pl.Float64, "s": pl.String}
    return {name: kinds[code] for name, code in (tok.split(":") for tok in text.split())}


_SIDE_KEYS = (
    "team_id:i season_team_name:s season_team_short_name:s team_logo_image:s team_entry_flg:i season_team_area:s "
    "free_text:s winner_flg:i score:i score_p1:i score_p2:i score_p3:i score_p4:i score_ot1:i score_ot2:i "
    "score_ot3:i score_ot4:i"
)
_BOX = "pts:i p2m:i p2a:i p3m:i p3a:i ftm:i fta:i f:i off:i def:i tot:i ast:i stl:i blk:i to:i"
_SIDE_STATS = (
    "{s}_ptr:f {s}_pt2r:f {s}_pt3r:f {s}_ftr:f {s}_rebound_off:i {s}_rebound_def:i {s}_rebound_tot:i "
    "{s}_assist:i {s}_turnovers:i {s}_max_difference:i"
)
_LEADERS = " ".join(
    f"{s}_{k}_{t}:s" for s in ("home", "away") for k in ("point", "assist", "rebound") for t in ("player", "value")
)
_TEAM_ROW = "season_team_name:s season_team_short_name:s team_entry_flg:i position:i season_team_area:s"
_SCHED_SIDE = (
    "{s}_team_id:i {s}_season_team_name:s {s}_season_team_short_name:s {s}_team_logo_image:s {s}_team_free_text:s "
    "{s}_team_score:i {s}_team_entry_flg:i"
)
_INFO_SIDE = "{s}_team_id:i {s}_season_team_name:s {s}_season_team_short_name:s {s}_team_logo_image:s {s}_free_text:s"
_ROSTER_KEYS = "league_id:i league_title:s season:i season_id:i season_full_name:s season_team_id:i team_id:i"

#: ``{parser name: {column: dtype}}``: the documented returns schema of each parser.
SCHEMAS: Dict[str, Dict[str, Any]] = {
    "parse_wjbl_leagues": _spec("league_id:i league_title:s season:i"),
    "parse_wjbl_game_months": _spec("league_id:i season:i game_month:s game_month_jpn:s"),
    "parse_wjbl_schedule": _spec(
        "league_id:i league_title:s season:i season_id:i season_title:s season_full_name:s play_schedule_id:i "
        "game_year:i game_date:s game_day:s game_date_week_day_name:s game_start_time:s started_at:s opening_time:s "
        "venue:s venue_address:s game_icon:i ticket_url:s vod_link:s live_link:s game_status:s box_flg:i pbp_flg:i "
        "cancel_flg:i delay_flg:i ot_flg:i " + _SCHED_SIDE.format(s="home") + " " + _SCHED_SIDE.format(s="away")
    ),
    "parse_wjbl_game": _spec(
        "play_schedule_id:i game_status:s "
        + " ".join(f"{s}_{tok}" for s in ("home", "away") for tok in _SIDE_KEYS.split())
    ),
    "parse_wjbl_period_scores": _spec("play_schedule_id:i team_side:s team_id:i period:i period_label:s score:i"),
    "parse_wjbl_game_info": _spec(
        "play_schedule_id:i league_id:i league_title:s season:i season_id:i season_title:s season_full_name:s "
        "game_date:s game_day:s game_start_time:s started_at:s opening_time:s venue:s game_icon:i venue_address:s "
        "venue_others:s allow_shoes_on:i allow_shoes_on_others:s ticket_info:s ticket_url:s event_info:s live_link:s "
        "vod_link:s bs_url:s cs_url:s ground_url:s other_url:s cancel_flg:i delay_flg:i game_status:s box_flg:i "
        "pbp_flg:i referee_name:s sub_referee_name1:s sub_referee_name2:s comment:s report:s "
        + _INFO_SIDE.format(s="home")
        + " "
        + _INFO_SIDE.format(s="away")
    ),
    "parse_wjbl_box_score": _spec(
        "play_schedule_id:i team_side:s team_id:i player_id:i player_name:s player_number:s gs:i "
        + _BOX
        + " min:s min_seconds:i"
    ),
    "parse_wjbl_team_box_score": _spec(
        "play_schedule_id:i team_side:s team_id:i row_type:s " + _BOX + " min:s min_seconds:i p2p:f p3p:f ftp:f fgp:f"
    ),
    "parse_wjbl_pbp": _spec(
        "play_schedule_id:i no:i period:i rest_time:s seconds_remaining:i team_id:i team_name:s team_side:s "
        "player_number1:s player_number2:s player_name1:s player_name2:s player_id1:i player_id2:i play_text:s "
        "score:s home_score:i away_score:i end_period_flg:i end_game_flg:i action1:s action2:s action3:s home_away:i"
    ),
    "parse_wjbl_team_period_stats": _spec(
        "play_schedule_id:i home_team_id:i away_team_id:i period_category:i period_label:s "
        + _SIDE_STATS.format(s="home")
        + " "
        + _SIDE_STATS.format(s="away")
        + " lead_count:i tie_count:i"
    ),
    "parse_wjbl_player_period_leaders": _spec(
        "play_schedule_id:i home_team_id:i away_team_id:i period_category:i period_label:s " + _LEADERS
    ),
    "parse_wjbl_standing_seasons": _spec(
        "season_id:i season_full_name:s season:i season_league_name:s team_standings_flg:i "
        "season_tournament_text_flg:i season_tournament_text:s"
    ),
    "parse_wjbl_team_standings": _spec(
        "season_id:i season_title:s league_name:s season_league_name:s season_league_flg:i update_date:s team_id:i "
        + _TEAM_ROW
        + " win_lost:s wins:i losses:i"
    ),
    "parse_wjbl_head_to_head": _spec("season_id:i team_id:i opponent_team_id:i game_number:i win:i losg:i result:s"),
    "parse_wjbl_team_ranking": _spec(
        "league_id:i league_title:s season:i season_id:i season_title:s season_full_name:s season_league_name:s "
        "season_league_flg:i season_league_img:s season_tournament_text:s update_info:s team_id:i "
        + _TEAM_ROW
        + " win:i lost:i winning_rate:f ppg:f opp:f fg:f opp_fg:f p3pg:f rpg:f apg:f spg:f bpg:f tpg:f gpd:f "
        "win_point:s unsatisfied:s"
    ),
    "parse_wjbl_teams": _spec("team_id:i team_name:s"),
    "parse_wjbl_roster": _spec(
        _ROSTER_KEYS
        + " coach_id:i coach_name:s team_comment:s player_id:i player_name:s player_number:s captain_flg:i "
        "sub_captain_flg:i transfer_flg:i position:s rookie:i early_entry:i picture:s foreign_national_flg:i "
        "naturalized_flg:i"
    ),
    "parse_wjbl_staff": _spec(_ROSTER_KEYS + " staff_name:s position:s url:s"),
    "parse_wjbl_player": _spec(
        "player_id:i player_name:s player_name_en:s player_furigana:s team_id:i season_team_name:s "
        "season_team_short_name:s season_team_player_number:s team_logo_image:s team_url:s player_birthday:s "
        "player_nickname:s player_position:s player_home_town:s player_height:s player_weight:s player_height_cm:i "
        "player_weight_kg:i player_affiliation_history:s player_independent_flg:i player_national_team:s "
        "player_awards:s player_coach_history:s player_coach_record:s player_picture:s player_wjbl_flg:i "
        "player_rookie:i player_early_entry:i player_foreign_national_flg:i player_naturalized_flg:i player_appeal:s "
        "player_championships:s player_url:s player_facebook_url:s player_twitter_url:s player_instagram_url:s"
    ),
}


# --- helpers ------------------------------------------------------------------------------------------------------


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _items(raw: Any) -> List[Mapping[str, Any]]:
    """Rows of a list route: ``items`` of the wrapped body, or a bare list."""
    return _list(raw) if isinstance(raw, list) else _list(_obj(raw).get("items"))


def _request(raw: Any) -> Mapping[str, Any]:
    return _obj(_obj(raw).get(_REQUEST))


def _int(value: Any) -> Optional[int]:
    """An int from an int, an integral float or an integer string; never a bool; else ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str) and re.fullmatch(r"\s*-?\d+\s*", value):
        return int(value)
    return None


def _float(value: Any) -> Optional[float]:
    """A float from a number or a numeric string (a trailing ``%`` is dropped); else ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*%?\s*", value)
        return float(m.group(1)) if m else None
    return None


def _clean(value: Any) -> Optional[str]:
    """A string with phone numbers and e-mail addresses replaced; ``None`` stays ``None``."""
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    return _EMAIL.sub("[redacted-email]", _PHONE.sub("[redacted-phone]", text))


def _text(value: Any) -> Optional[str]:
    return None if value is None else value if isinstance(value, str) else str(value)


#: Free-text columns, where a phone number or an e-mail address can sit; every other String column is kept as sent.
_FREE_TEXT = frozenset(
    {
        "ticket_info", "event_info", "comment", "report", "venue_others", "allow_shoes_on_others", "team_comment",
        "player_affiliation_history", "player_national_team", "player_awards", "player_coach_history",
        "player_coach_record", "player_appeal", "player_championships", "player_nickname", "player_home_town",
    }
)  # fmt: skip
_CASTS: Dict[Any, Callable[[Any], Any]] = {pl.Int64: _int, pl.Float64: _float, pl.String: _text}


def _finish(rows: Iterable[Mapping[str, Any]], parser: str, return_as_pandas: bool) -> Frame:
    """Build the frame of ``parser`` from row dicts: every column cast to its documented dtype, order kept."""
    schema = SCHEMAS[parser]
    rows = list(rows)
    data = {
        name: [(_clean if name in _FREE_TEXT else _CASTS[dtype])(r.get(name)) for r in rows]
        for name, dtype in schema.items()
    }
    df = pl.DataFrame(data, schema=schema)
    return df.to_pandas() if return_as_pandas else df


def _flat(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Nested objects as ``<parent>_<child>``; lists are dropped (each route names the lists it unrolls)."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        if key == _REQUEST or isinstance(value, list):
            continue
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(_flat(value, name + "_"))
        else:
            out[name] = value
    return out


def _keyed(value: Any) -> List[Mapping[str, Any]]:
    """The dict values of a map keyed by a position or an id, in numeric key order (non-numeric keys last)."""
    if not isinstance(value, dict):
        return []
    order = sorted(value, key=lambda k: (0, int(k)) if str(k).isdigit() else (1, 0))
    return [value[k] for k in order if isinstance(value[k], dict)]


def _seconds(clock: Any) -> Optional[int]:
    """Seconds in a ``m:ss`` / ``mmm:ss`` clock string, else ``None``."""
    m = re.fullmatch(r"\s*(\d+):(\d{2})\s*", clock) if isinstance(clock, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _counter(value: Any) -> Optional[int]:
    """The integer in a counted string (``'6本'``, ``'0回'``), else ``None``."""
    if isinstance(value, str):
        m = re.fullmatch(r"\s*(\d+)\s*\D{0,2}\s*", value)
        return int(m.group(1)) if m else None
    return _int(value)


def _period_label(category: Optional[int]) -> Optional[str]:
    """Q1-Q4, OT1-OT10 (5-14), H1 / H2 (15 / 16), OT (17, all overtimes) and GAME (18) for a period category."""
    if category is None:
        return None
    if 1 <= category <= 4:
        return f"Q{category}"
    if 5 <= category <= 14:
        return f"OT{category - 4}"
    return {15: "H1", 16: "H2", 17: "OT", 18: "GAME"}.get(category)


def _game_day(value: Any, year: Any = None) -> Optional[str]:
    """ISO date from ``('2026', '04.04')`` or from a Japanese date ``'2026年4月4日(土)'``; else ``None``."""
    if isinstance(value, str):
        m = re.match(r"\s*(\d{4})年(\d{1,2})月(\d{1,2})日", value)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = re.fullmatch(r"\s*(\d{2})\.(\d{2})\s*", value)
        if m and _int(year) is not None:
            return f"{int(year):04d}-{m.group(1)}-{m.group(2)}"
    return None


def _side_rows(raw: Any) -> List[tuple]:
    body = _obj(raw)
    return [(side, _obj(body.get(side))) for side in ("home", "away")]


# --- competitions, schedules --------------------------------------------------------------------------------------


@_data_only
def parse_wjbl_leagues(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/leagues`` -> one row per competition edition (every W League season, all-star game, cup and camp).

    Args:
        raw: the ``/leagues`` body (``items`` list, or the bare list).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per edition: ``league_id`` (the opaque edition id), ``league_title`` (Japanese, ``'25-26 Wリーグ'``) and
        ``season`` (the sdv END year, null for an all-star game, cup or camp). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_leagues
            parse_wjbl_leagues([{"league_id": 45, "league_title": "25-26 Wリーグ"}])
    """
    rows = [{**r, "season": season_of_title(r.get("league_title"))} for r in _items(raw)]
    return _finish(rows, "parse_wjbl_leagues", return_as_pandas)


def parse_wjbl_game_months(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/play-schedule-months`` -> one row per month with games in a league edition, newest first.

    Args:
        raw: the ``/play-schedule-months`` body (``league_id`` as a string and ``game_months``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per month: ``league_id`` (cast to an integer; the API echoes a string), ``season`` (the END year
        the request asked for), ``game_month`` (``YYYY-MM``, the ``game_month`` argument of ``wjbl_schedule``) and
        ``game_month_jpn``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_game_months
            parse_wjbl_game_months({"league_id": "45", "game_months": [{"game_month": "2026-04", "game_month_jpn": "2026年4月"}]})
    """
    body = _obj(raw)
    league = _int(body.get("league_id"))
    season = _request(raw).get("season")
    rows = [{**m, "league_id": league, "season": season} for m in _list(body.get("game_months"))]
    return _finish(rows, "parse_wjbl_game_months", return_as_pandas)


def parse_wjbl_schedule(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/play-schedules`` -> one row per game of ONE month of a league edition, both teams and the score.

    Args:
        raw: the ``/play-schedules`` body (``seasons`` keyed by phase, each with ``play_schedules``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per game: the edition (``league_id``, ``league_title``, ``season``), the phase (``season_id``,
        ``season_title``, ``season_full_name``), ``play_schedule_id``, the date (``game_year``, ``game_date``
        ``'MM.DD'``, ``game_day`` ISO), the times (JST), the venue, the broadcast links, ``game_status``
        (``fixed`` = final), the content flags and ``home_*`` / ``away_*`` team id, names and score. Zero rows for an
        empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_schedule
            parse_wjbl_schedule({"league_id": 45, "league_title": "25-26 Wリーグ", "seasons": {"176": {"season_id": 176, "play_schedules": [{"play_schedule_id": 7987, "game_year": "2026", "game_date": "04.04"}]}}})
    """
    body = _obj(raw)
    head = {
        "league_id": body.get("league_id"),
        "league_title": body.get("league_title"),
        "season": season_of_title(body.get("league_title")),
    }
    rows = []
    for phase in _keyed(body.get("seasons")):
        for game in _list(phase.get("play_schedules")):
            row = {**head, **{k: phase.get(k) for k in ("season_id", "season_title", "season_full_name")}, **game}
            row["game_day"] = _game_day(game.get("game_date"), game.get("game_year"))
            rows.append(row)
    return _finish(rows, "parse_wjbl_schedule", return_as_pandas)


def parse_wjbl_game(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/play-schedule/detail`` -> the game header, one row: both teams and the score by period.

    Args:
        raw: the ``/play-schedule/detail`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: ``play_schedule_id``, ``game_status`` and, for ``home_`` and ``away_``, the team id and names, the
        result flag ``winner_flg`` (1 won, -1 lost), the final ``score``, ``score_p1`` to ``score_p4`` (quarters) and
        ``score_ot1`` to ``score_ot4`` (null when not played). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_game
            parse_wjbl_game({"play_schedule_id": 7987, "game_status": "fixed", "home": {"team_id": 13, "score": 66}, "away": {"team_id": 12, "score": 75}})
    """
    body = _obj(raw)
    if not body or body.get("play_schedule_id") is None:
        return _finish([], "parse_wjbl_game", return_as_pandas)
    row: Dict[str, Any] = {k: body.get(k) for k in ("play_schedule_id", "game_status")}
    for side, block in _side_rows(raw):
        row.update({f"{side}_{k}": v for k, v in block.items()})
    return _finish([row], "parse_wjbl_game", return_as_pandas)


def parse_wjbl_period_scores(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/play-schedule/detail`` -> points of each side in each period played, from the game header.

    Args:
        raw: the ``/play-schedule/detail`` body (the same body as ``wjbl_game``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per side and period played: ``play_schedule_id``, ``team_side`` (``home`` / ``away``), ``team_id``,
        ``period`` (1-4 quarters, 5-8 the first to fourth overtime), ``period_label`` (``Q1`` ... ``OT1``) and
        ``score`` (points in that period). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_period_scores
            parse_wjbl_period_scores({"play_schedule_id": 7987, "home": {"team_id": 13, "score_p1": 19}, "away": {"team_id": 12, "score_p1": 22}})
    """
    body = _obj(raw)
    keys = [(f"score_p{i}", i, f"Q{i}") for i in range(1, 5)] + [(f"score_ot{i}", 4 + i, f"OT{i}") for i in range(1, 5)]
    rows = []
    for side, block in _side_rows(raw):
        for key, period, label in keys:
            if _int(block.get(key)) is not None:
                rows.append(
                    {
                        "play_schedule_id": body.get("play_schedule_id"),
                        "team_side": side,
                        "team_id": block.get("team_id"),
                        "period": period,
                        "period_label": label,
                        "score": block.get(key),
                    }
                )
    return _finish(rows, "parse_wjbl_period_scores", return_as_pandas)


def parse_wjbl_game_info(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/play-schedule`` -> the game information, one row: venue, times, officials, links and the game report.

    Args:
        raw: the ``/play-schedule`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the edition and phase, the date (``game_date`` as sent, Japanese, and ``game_day`` ISO), the times
        (JST), the venue, the ticket, stream and other links, the cancel / delay flags, ``game_status``, the content
        flags, the referee names, the free-text ``comment`` and ``report`` and the ``home_`` / ``away_`` team names.
        The organiser's contact details are never output and phone numbers or e-mail addresses inside free text are
        replaced. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_game_info
            parse_wjbl_game_info({"play_schedule_id": 7987, "league_title": "25-26 Wリーグ", "game_date": "2026年4月4日(土)", "home": {"team_id": 13}, "away": {"team_id": 12}})
    """
    body = _obj(raw)
    if not body or body.get("play_schedule_id") is None:
        return _finish([], "parse_wjbl_game_info", return_as_pandas)
    row: Dict[str, Any] = {k: v for k, v in body.items() if not _CONTACT_KEY.search(k) and not isinstance(v, dict)}
    row["season"] = season_of_title(body.get("league_title"))
    row["game_day"] = _game_day(body.get("game_date"))
    for side, block in _side_rows(raw):
        row.update({f"{side}_{k}": v for k, v in block.items()})
    return _finish([row], "parse_wjbl_game_info", return_as_pandas)


# --- box score ----------------------------------------------------------------------------------------------------


def parse_wjbl_box_score(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/boxscore`` -> one row per player of both teams (2013-14 on).

    Args:
        raw: the ``/boxscore`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per player: ``play_schedule_id``, ``team_side``, ``team_id``, ``player_id`` (the person id, joins
        ``wjbl_player`` and the play-by-play ``player_id1`` / ``player_id2``), ``player_name``, ``player_number``
        (a string), ``gs`` (1 starter), points, 2-point, 3-point and free-throw made / attempted, fouls, rebounds
        (offensive, defensive, total), assists, steals, blocks, turnovers, ``min`` (``'mm:ss'`` as sent) and
        ``min_seconds``. The team line and the totals are ``wjbl_team_box_score``. Zero rows for an empty or malformed
        body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_box_score
            parse_wjbl_box_score({"play_schedule_id": 7987, "home": {"team_id": 13, "players": [{"player_id": 9799, "pts": 5, "min": "22:19"}]}})
    """
    pid = _obj(raw).get("play_schedule_id")
    rows = []
    for side, block in _side_rows(raw):
        for p in _list(block.get("players")):
            rows.append(
                {
                    **p,
                    "play_schedule_id": pid,
                    "team_side": side,
                    "team_id": block.get("team_id"),
                    "min_seconds": _seconds(p.get("min")),
                }
            )
    return _finish(rows, "parse_wjbl_box_score", return_as_pandas)


def parse_wjbl_team_box_score(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/boxscore`` -> the team line and the totals of each side, with the shooting percentages.

    Args:
        raw: the ``/boxscore`` body (the same body as ``wjbl_box_score``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        Two rows per side: ``row_type`` ``team`` (the stats credited to the team, not a player: team rebounds and
        turnovers; points and shooting are 0) and ``total`` (the sum of the players and the team line), with the same
        stat columns as ``wjbl_box_score``. The ``total`` row also carries ``p2p``, ``p3p``, ``ftp`` and ``fgp``
        (percentages 0-100, from strings on the wire; null on the ``team`` row). Zero rows for an empty or malformed
        body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_team_box_score
            parse_wjbl_team_box_score({"play_schedule_id": 7987, "home": {"team_id": 13, "total": {"pts": 66, "min": "200:00"}, "percentage": {"fgp": "39.3"}}})
    """
    pid = _obj(raw).get("play_schedule_id")
    rows = []
    for side, block in _side_rows(raw):
        for kind in ("team", "total"):
            line = _obj(block.get(kind))
            if not line:
                continue
            pct = _obj(block.get("percentage")) if kind == "total" else {}
            rows.append(
                {
                    **line,
                    **{k: pct.get(k) for k in ("p2p", "p3p", "ftp", "fgp")},
                    "play_schedule_id": pid,
                    "team_side": side,
                    "team_id": block.get("team_id"),
                    "row_type": kind,
                    "min_seconds": _seconds(line.get("min")),
                }
            )
    return _finish(rows, "parse_wjbl_team_box_score", return_as_pandas)


# --- play-by-play and period stats --------------------------------------------------------------------------------


def parse_wjbl_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/pbp/play-by-play`` -> one row per play of a final game, in game order (2018-19 on).

    Args:
        raw: the ``/pbp/play-by-play`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per play: ``play_schedule_id``, ``no`` (the sequence number), ``period`` (1-4 quarters, 5-14
        overtimes), ``rest_time`` (clock remaining ``'m:ss'``) and ``seconds_remaining``, the acting team
        (``team_id``, ``team_name``, ``team_side`` and ``home_away`` 1 home / 2 away), the first and second player
        (number, name and ``player_id``; on a substitution the first goes out and the second comes in), ``play_text``
        (Japanese display text), ``score`` (``'HOME-AWAY'`` after a scoring play) with ``home_score`` and
        ``away_score``, the end flags and the three Japanese action fields (``action1`` is the action, see the column
        descriptions). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_pbp
            parse_wjbl_pbp({"play_schedule_id": 7987, "play_by_plays": [{"no": 12, "period": 1, "rest_time": "9:38", "score": "2-0", "home_away": 1}]})
    """
    body = _obj(raw)
    rows = []
    for p in _list(body.get("play_by_plays")):
        m = re.fullmatch(r"\s*(\d+)-(\d+)\s*", p.get("score") or "") if isinstance(p.get("score"), str) else None
        rows.append(
            {
                **p,
                "play_schedule_id": body.get("play_schedule_id"),
                "seconds_remaining": _seconds(p.get("rest_time")),
                "team_side": {1: "home", 2: "away"}.get(_int(p.get("home_away")) or 0),
                "home_score": int(m.group(1)) if m else None,
                "away_score": int(m.group(2)) if m else None,
            }
        )
    return _finish(rows, "parse_wjbl_pbp", return_as_pandas)


def _period_rows(raw: Any) -> List[Mapping[str, Any]]:
    body = _obj(raw)
    base = {k: body.get(k) for k in ("play_schedule_id", "home_team_id", "away_team_id")}
    rows: List[Mapping[str, Any]] = []
    for p in _keyed(body.get("periods")):
        category = _int(p.get("period_category", p.get("periodCategory")))
        rows.append({**base, **p, "period_category": category, "period_label": _period_label(category)})
    return rows


def parse_wjbl_team_period_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/pbp/team-stats`` -> team stats per period category, one row each, both teams side by side (2018-19 on).

    Args:
        raw: the ``/pbp/team-stats`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``period_category`` (1-4 quarters, 5-14 overtimes, 15 / 16 halves, 17 all overtime, 18 the whole
        game; ``period_label`` names it): ``home_`` and ``away_`` field-goal, 2-point, 3-point and free-throw
        percentages (``ptr``, ``pt2r``, ``pt3r``, ``ftr``, 0-100), rebounds (``rebound_off``, ``rebound_def``,
        ``rebound_tot``, from ``'3-4-7'``), assists, turnovers (from ``'6本'``) and largest lead
        (``max_difference``), plus ``lead_count`` and ``tie_count`` (from ``'0回'``). Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_team_period_stats
            parse_wjbl_team_period_stats({"play_schedule_id": 7987, "periods": {"1": {"period_category": 1, "home_ptr": 61.5, "home_rebound": "3-4-7", "home_trun_over": "6本"}}})
    """
    rows = []
    for p in _period_rows(raw):
        row = dict(p)
        for side in ("home", "away"):
            parts = (
                re.fullmatch(r"\s*(\d+)-(\d+)-(\d+)\s*", p.get(f"{side}_rebound") or "")
                if isinstance(p.get(f"{side}_rebound"), str)
                else None
            )
            for name, idx in (("off", 1), ("def", 2), ("tot", 3)):
                row[f"{side}_rebound_{name}"] = int(parts.group(idx)) if parts else None
            row[f"{side}_turnovers"] = _counter(p.get(f"{side}_trun_over", p.get(f"{side}_turnovers")))
            row[f"{side}_max_difference"] = p.get(f"{side}_max_differenct", p.get(f"{side}_max_difference"))
        row["lead_count"] = _counter(p.get("lead_count"))
        row["tie_count"] = _counter(p.get("tie_count"))
        rows.append(row)
    return _finish(rows, "parse_wjbl_team_period_stats", return_as_pandas)


def parse_wjbl_player_period_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/pbp/player-stats`` -> the leading players of each team per period category, one row each (2018-19 on).

    Args:
        raw: the ``/pbp/player-stats`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ``period_category`` (see ``wjbl_team_period_stats``): for ``home_`` and ``away_`` the leading
        scorer, passer and rebounder as ``*_player`` (``'#8 高田 真希'``, tied players joined by ``' , '``) and the
        matching ``*_value`` (``'6点'`` points, ``'4本'`` assists, ``'1-0-1'`` offensive-defensive-total rebounds, tied
        values joined the same way). The strings are kept as sent. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_player_period_leaders
            parse_wjbl_player_period_leaders({"play_schedule_id": 7987, "periods": {"1": {"periodCategory": 1, "home_point_player": "#30 三浦 舞華", "home_point_value": "5点"}}})
    """
    return _finish(_period_rows(raw), "parse_wjbl_player_period_leaders", return_as_pandas)


# --- standings, teams, players ------------------------------------------------------------------------------------


def parse_wjbl_standing_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/seasons/standings`` -> one row per season phase (``season_id``) with its standings flags.

    Args:
        raw: the ``/seasons/standings`` body (``items`` list of editions, each with ``seasons`` keyed by phase).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per phase: ``season_id`` (the argument of ``wjbl_team_standings`` and ``wjbl_team_ranking``),
        ``season_full_name`` of its edition, ``season`` (the sdv END year from that name, null for non-league
        editions), ``season_league_name`` (the division or stage), ``team_standings_flg`` (1 when the phase has a
        results grid) and the tournament text flags. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_standing_seasons
            parse_wjbl_standing_seasons([{"season_full_name": "25-26 Wリーグ", "seasons": {"169": {"season_id": 169, "team_standings_flg": 1}}}])
    """
    rows = []
    for edition in _items(raw):
        name = edition.get("season_full_name")
        end = (
            season_of_title(name.split(" ", 2)[0] + " " + name.split(" ", 2)[1])
            if isinstance(name, str) and len(name.split(" ", 2)) > 1
            else None
        )
        for phase in _keyed(edition.get("seasons")):
            rows.append({**phase, "season_full_name": name, "season": end})
    return _finish(rows, "parse_wjbl_standing_seasons", return_as_pandas)


def parse_wjbl_team_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/team-standings`` -> the league table of each requested phase, one row per team.

    Args:
        raw: the ``/team-standings`` body (``items`` list, one per phase, each with ``teams`` keyed by rank).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team and phase: the phase (``season_id``, ``season_title``, ``league_name``,
        ``season_league_name``, ``update_date``), ``team_id``, names, ``position`` (rank), ``win_lost`` (``'23勝5負'``
        as sent) with ``wins`` and ``losses``. The head-to-head grid is ``wjbl_head_to_head``. Zero rows for an empty
        or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_team_standings
            parse_wjbl_team_standings([{"season_id": 169, "teams": {"1": {"team_id": 13, "position": "1", "win_lost": "23勝5負"}}}])
    """
    rows = []
    for phase in _items(raw):
        for t in _keyed(phase.get("teams")):
            m = re.search(r"(\d+)勝\s*(\d+)負", t.get("win_lost") or "") if isinstance(t.get("win_lost"), str) else None
            rows.append(
                {**phase, **t, "wins": int(m.group(1)) if m else None, "losses": int(m.group(2)) if m else None}
            )
    return _finish(rows, "parse_wjbl_team_standings", return_as_pandas)


def parse_wjbl_head_to_head(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/team-standings`` -> the head-to-head results grid (hoshitori-hyo), one row per game played.

    Args:
        raw: the ``/team-standings`` body (the same body as ``wjbl_team_standings``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per game of one team against one opponent, so each game appears twice (once per side): ``season_id``,
        ``team_id`` (the row team), ``opponent_team_id``, ``game_number`` (1-based order within the pair), ``win``
        (the row team's points, a string on the wire, whatever the result), ``losg`` (the opponent's points; the wire
        key is spelled so) and ``result`` (``'○ 63-56'`` won, ``'● 66-67'`` lost). Pairs that have not met have no
        rows. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_head_to_head
            parse_wjbl_head_to_head([{"season_id": 169, "teams": {"1": {"team_id": 13, "vs_teams": {"2": {"team_id": 12, "scores": [{"win": "63", "losg": "56", "result": "○ 63-56"}]}}}}}])
    """
    rows = []
    for phase in _items(raw):
        for t in _keyed(phase.get("teams")):
            for opp in _keyed(t.get("vs_teams")):
                for n, g in enumerate(_list(opp.get("scores")), start=1):
                    rows.append(
                        {
                            **g,
                            "season_id": phase.get("season_id"),
                            "team_id": t.get("team_id"),
                            "opponent_team_id": opp.get("team_id"),
                            "game_number": n,
                        }
                    )
    return _finish(rows, "parse_wjbl_head_to_head", return_as_pandas)


def parse_wjbl_team_ranking(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/team-ranking`` -> the league table with team averages of each requested phase, one row per team.

    Args:
        raw: the ``/team-ranking`` body (``league_id``, ``league_title`` and ``seasons`` keyed by phase).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team and phase: the edition (``league_id``, ``league_title``, ``season``), the phase, ``team_id``,
        names, ``position``, ``win``, ``lost``, ``winning_rate`` and per-game averages: points ``ppg``, opponent
        points ``opp``, field-goal percentage ``fg`` and ``opp_fg`` (0-100, from ``'40.92%'``), three-pointers made
        ``p3pg``, rebounds ``rpg``, assists ``apg``, steals ``spg``, blocks ``bpg``, turnovers ``tpg`` and ``gpd``.
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_team_ranking
            parse_wjbl_team_ranking({"league_id": 45, "league_title": "25-26 Wリーグ", "seasons": {"169": {"season_id": 169, "teams": {"1": {"team_id": 13, "ppg": "63.11", "fg": "40.92%"}}}}})
    """
    body = _obj(raw)
    head = {
        "league_id": body.get("league_id"),
        "league_title": body.get("league_title"),
        "season": season_of_title(body.get("league_title")),
    }
    rows = []
    for phase in _keyed(body.get("seasons")):
        for t in _keyed(phase.get("teams")):
            rows.append({**head, **phase, **t})
    return _finish(rows, "parse_wjbl_team_ranking", return_as_pandas)


@_data_only
def parse_wjbl_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/teams/base`` -> every franchise (``team_id``) the site knows, all-star sides included.

    Args:
        raw: the ``/teams/base`` body (``items`` list, or the bare list).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per franchise: ``team_id`` (stable across seasons and sponsor renames) and ``team_name`` (Japanese).
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_teams
            parse_wjbl_teams([{"team_id": 30, "team_name": "富士通レッドウェーブ"}])
    """
    return _finish(_items(raw), "parse_wjbl_teams", return_as_pandas)


def _roster_head(raw: Any) -> Dict[str, Any]:
    body = _obj(raw)
    head = {
        k: body.get(k)
        for k in ("league_id", "league_title", "season_id", "season_full_name", "season_team_id", "team_id")
    }
    head["season"] = season_of_title(body.get("league_title"))
    return head


def parse_wjbl_roster(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/team/roster`` -> the players of a team in a season phase, one row each.

    Args:
        raw: the ``/team/roster`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per player: the edition and phase (``league_id``, ``season``, ``season_id``), ``season_team_id`` (the
        team in that phase; not ``team_id``), ``team_id``, the head coach (``coach_id``, ``coach_name``), the team's
        free-text ``team_comment``, then ``player_id``, ``player_name``, ``player_number`` (a string), the captain,
        transfer, rookie, foreign-national and naturalised flags and ``position`` (empty in practice). Zero rows for an
        empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_roster
            parse_wjbl_roster({"league_id": 45, "league_title": "25-26 Wリーグ", "team_id": 13, "players": [{"player_id": 14380, "player_name": "古木 梨子"}]})
    """
    body = _obj(raw)
    head = {
        **_roster_head(raw),
        "coach_id": body.get("coach_id"),
        "coach_name": body.get("coach_name"),
        "team_comment": body.get("comment"),
    }
    return _finish([{**head, **p} for p in _list(body.get("players"))], "parse_wjbl_roster", return_as_pandas)


def parse_wjbl_staff(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/team/roster`` -> the staff of a team in a season phase (general manager, assistants, support), one row each.

    Args:
        raw: the ``/team/roster`` body (the same body as ``wjbl_roster``).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per staff member: the edition, phase and team ids as in ``wjbl_roster``, ``staff_name``, ``position``
        (the Japanese role) and ``url``. The head coach is on the ``wjbl_roster`` rows. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_staff
            parse_wjbl_staff({"league_id": 45, "team_id": 13, "staffs": [{"staff_name": "清野 英二", "position": "ゼネラルマネージャー"}]})
    """
    head = _roster_head(raw)
    rows = [{**head, **s} for s in _list(_obj(raw).get("staffs"))]
    return _finish(rows, "parse_wjbl_staff", return_as_pandas)


def parse_wjbl_player(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/player`` -> the player profile, one row: current team, bio and career notes.

    Args:
        raw: the ``/player`` body.
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: ``player_id``, names (Japanese, English, furigana), the current or last team, birthday
        (``'YYYY/MM/DD'`` as sent), position, home prefecture, height and weight as sent with ``player_height_cm`` and
        ``player_weight_kg``, affiliation history, national-team and award notes, flags and profile links. The blood
        type is never output. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.wjbl import parse_wjbl_player
            parse_wjbl_player({"player_id": 9799, "player_name": "岡本 美優", "player_height": "179cm", "player_weight": "77kg"})
    """
    body = _obj(raw)
    if body.get("player_id") is None:
        return _finish([], "parse_wjbl_player", return_as_pandas)
    row = {k: v for k, v in body.items() if k != "player_blood" and not isinstance(v, (dict, list))}
    for src, dst in (("player_height", "player_height_cm"), ("player_weight", "player_weight_kg")):
        m = _NUM.search(row.get(src) or "") if isinstance(row.get(src), str) else None
        row[dst] = int(float(m.group(0))) if m else None
    return _finish([row], "parse_wjbl_player", return_as_pandas)
