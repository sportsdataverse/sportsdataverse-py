"""Parsers for the generated ``nationalleague`` wrappers (``www.nationalleague.ch/api``).

The National League app API is keyless JSON with four body shapes:

* **Row lists** -- ``/games``, ``/games/current``, ``/games/team/{teamId}`` (one row per game),
  ``/player``, ``/player/topscorer``, ``/player/team/{teamId}`` (one row per player) and
  ``/teams`` (one row per team). A list becomes one row per item.
* **One object** -- ``/player/{playerId}`` (one player row) and ``/teams/{teamId}/info`` (the
  club's CMS record). An object becomes one row; nested objects flatten to ``parent_child``
  columns.
* **Bracket trees** -- ``/games/playoffs`` and ``/games/playouts``: rounds -> series -> games.
  :func:`parse_nationalleague_bracket` gives one row per game with its ``round`` and the
  series' ``series_*`` columns.
* **The game page** -- ``/games/{gameId}``: ``overview``, ``result``, ``actions`` (events by
  period), ``lineupHome`` / ``lineupAway``, ``playerStatsHome`` / ``playerStatsAway``,
  ``teamStatsHome`` / ``teamStatsAway`` and ``shotsHome`` / ``shotsAway``. Six shapers each
  read one part: overview, pbp, lineups, player_box, team_stats and shots.

Every frame follows the package parser contract: polars by default, pandas via
``return_as_pandas=True``, snake_cased columns, and a zero-row frame with the documented
schema (never an exception) on an empty or malformed payload. Each shape carries its
documented schema (:data:`_SCHEMAS`, derived from full, untruncated 2026 bodies), so a
column's dtype never depends on which rows a body happens to hold: a rate that is ``90`` in
one row and ``86.36`` in the next is ``Float64`` everywhere. A column the API adds later is
kept, typed from its values. List cells (``tv_partner``, ``referees``, ``streak``) are JSON
strings.

Ids and joins:

* ``player_id`` (``playerId``; the shot ``pid``) is the SIHF licence number, ``Int64``. It
  joins the ``sihf`` family's ``player_id`` (``Int64`` there too) and every SIHF
  ``*LicenceNr``: this API is the way to put ids on SIHF season stats, whose tables carry
  names only.
* ``game_id`` is the SIHF game id ``<season(4)><competition(4)><sequence(6)>``, ``Int64``,
  the same id as the ``sihf`` family's ``game_id``; its first four digits are the season's
  END year.
* ``team_id``, ``home_team_id``, ``away_team_id`` are SIHF team ids, ``Int64`` (the API
  sends digit strings, and an integer in ``actions[].teamId``).
* ``cms_node_id`` (``/teams/{teamId}/info`` ``id``) is an Umbraco CMS node, not a team id.
* Every other ``*_id`` (Vimeo video ids, CMS media ids) is ``Int64`` too.

``fo`` means two things upstream. In the game box score and team game totals it is faceoffs
taken (``faceoffs`` here, with ``faceoffs_percentage``); in the season player and team rows
it is penalties taken (``penalties`` here; the season rows count faceoffs in ``face_offs``
/ ``fot``).

The club CMS record carries a backend key (``fields.apiKey``). It is dropped from every
body before parsing (:func:`_scrub`), and the runtime drops it from the raw body too.

robots.txt on www.nationalleague.ch disallows ``/api``; the owner approved building on it
on 2026-10-12 for polite, incremental use (see ``nationalleague_runtime``).
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_nationalleague",
    "parse_nationalleague_bracket",
    "parse_nationalleague_game_overview",
    "parse_nationalleague_games",
    "parse_nationalleague_lineups",
    "parse_nationalleague_pbp",
    "parse_nationalleague_player_box",
    "parse_nationalleague_player_stats",
    "parse_nationalleague_shots",
    "parse_nationalleague_standings",
    "parse_nationalleague_team_info",
    "parse_nationalleague_team_stats",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

_CODES: Dict[str, pl.DataType] = {"s": pl.String(), "i": pl.Int64(), "f": pl.Float64(), "b": pl.Boolean()}

# Rates the API may send as an integer when the value is whole (90 for 90.0); always Float64.
_RATE = re.compile(r"(percentage|per_game|average|efficiency|utilization|^xg)")

_API_KEY = re.compile(r"(?i)^api[_-]?key$")
_API_KEY_IN_URL = re.compile(r"(?i)(api[_-]?key=)[^&#\s\"']+")


def _schema(spec: str) -> Dict[str, pl.DataType]:
    """``"game_id:i date:s"`` -> ``{"game_id": Int64, "date": String}`` (codes s, i, f, b)."""
    return {name: _CODES[code] for name, code in (tok.rsplit(":", 1) for tok in spec.split())}


def _is_id(name: str) -> bool:
    return name == "id" or name.endswith("_id")


def _scrub(obj: Any) -> Any:
    """``obj`` without any api-key entry (any case or separator) and with ``apiKey=`` URL values redacted."""
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items() if not (isinstance(k, str) and _API_KEY.match(k))}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    if isinstance(obj, str):
        return _API_KEY_IN_URL.sub(r"\1<redacted>", obj)
    return obj


def _flatten(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """One level of ``{k: v}`` per leaf: nested objects become ``parent_child`` snake_case keys."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        name = f"{prefix}_{underscore(str(key))}" if prefix else underscore(str(key))
        if isinstance(value, dict) and value:
            out.update(_flatten(value, name))
        else:
            out[name] = value
    return out


def _infer(name: str, values: Sequence[Any]) -> pl.DataType:
    """Dtype for a column the schema does not list (a field the API added later)."""
    present = [v for v in values if v is not None]
    if _is_id(name):
        return pl.Int64()
    if present and all(isinstance(v, bool) for v in present):
        return pl.Boolean()
    if present and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in present):
        if _RATE.search(name) or any(isinstance(v, float) and not v.is_integer() for v in present):
            return pl.Float64()
        return pl.Int64()
    return pl.String()


def _coerce(value: Any, dtype: pl.DataType) -> Any:
    """``value`` as a Python value of ``dtype``; anything that does not fit becomes None."""
    if value is None:
        return None
    if dtype == pl.String:
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)
    if dtype == pl.Boolean:
        return value if isinstance(value, bool) else None
    if isinstance(value, bool):
        return None
    if dtype == pl.Int64:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else None
        if isinstance(value, str):
            text = value.strip()
            return int(text) if re.fullmatch(r"-?\d+", text) else None
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _frame(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, pl.DataType],
    return_as_pandas: bool = False,
) -> Frame:
    """Rows (already flat, snake_cased) -> a frame with ``schema``'s columns first, extras after."""
    rows = [r for r in rows if isinstance(r, Mapping)]
    names = list(schema)
    for row in rows:
        names += [k for k in row if k not in schema and k not in names]
    series = []
    for name in names:
        values = [row.get(name) for row in rows]
        dtype = schema.get(name) or _infer(name, values)
        cells = [_coerce(v, dtype) for v in values]
        if not _is_id(name) and any(c is None and v is not None and v != "" for c, v in zip(cells, values)):
            # A value the documented dtype cannot hold (2.5 in an Int64 column): never drop it.
            # An id stays Int64 whatever arrives (a join key never flips dtype); a misfit is null.
            dtype = _infer(name, values) if dtype != _infer(name, values) else pl.String()
            cells = [_coerce(v, dtype) for v in values]
        series.append(pl.Series(name, cells, dtype=dtype))
    df = pl.DataFrame(series)
    return df.to_pandas() if return_as_pandas else df


def _rows(raw: Any) -> List[Dict[str, Any]]:
    """The row rule: a list -> its object items; a non-empty object -> one row; else none."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict) and r]
    if isinstance(raw, dict) and raw:
        return [raw]
    return []


def _rename(row: Dict[str, Any], names: Mapping[str, str]) -> Dict[str, Any]:
    return {names.get(k, k): v for k, v in row.items()}


def _flat_rows(
    raw: Any, renames: Optional[Mapping[str, str]] = None, *, lists_only: bool = False
) -> List[Dict[str, Any]]:
    """Flattened, renamed rows of ``raw``; ``lists_only`` refuses an object (a list route's malformed body)."""
    if lists_only and not isinstance(raw, list):
        return []
    flat = (_rename(_flatten(r), renames or {}) for r in _rows(_scrub(raw)))
    return [r for r in flat if any(v is not None for v in r.values())]  # {"data": null} -> no row


# ---------------------------------------------------------------------------
# Documented schemas (from full 2026 bodies; see tests/fixtures/nationalleague/full/)
# ---------------------------------------------------------------------------

_GAME_ROW = """
game_id:i home_team_short_name:s home_team_name:s away_team_short_name:s away_team_name:s home_team_id:i
away_team_id:i vimeo_video_id:i date:s status:s base_status:s home_team_result:i away_team_result:i
is_overtime:b is_shootout:b highlight_vimeo_video_id:i has_highlight:b is_current:b is_current_for_team:b
tv_partner:s spectators:i has_my_sports:b referees:s is_exhibition:b home_team_coach:s away_team_coach:s
game_time:i show_game_time:b arena:s
"""
_SERIES = """
series_group:i series_home_team_short_name:s series_away_team_short_name:s series_home_team_id:i
series_away_team_id:i series_home_team_result:i series_away_team_result:i series_best_of:i series_title:s
round:s
"""
_PLAYER_ROW = """
player_id:i first_name:s last_name:s team_id:i team_short_name:s team_name:s number:s position:s
topscorer_points:i topscorer_point_average:f topscorer_market_value:i topscorer_market_value_trend:i
topscorer_amount:i is_topscorer:b nationality:s birth:s height:i weight:i hand:s lcrid:i noc:i gp:i g:i a1:i
a2:i penalties:i pim:i geq:i gpp:i gpk:i gwg:i ps:i pg:i pl:i mi:i toi:i toi_eq:i toipp:i toi_pk:i sog:i shm:i
shp:i shb:i bks:i fow:i fol:i shi:i gwgk:i gw:i gl:i fk:i ga:i sa:i svs:i so:i mip:i assists:i points:i
goal_efficiency:f save_percentage:f face_offs_percentage:f face_offs:i is_captain:b first_goalkeeper:b
ga_per_game:f points_per_game:f svs_per_game:f bks_per_game:f sog_per_game:f shm_per_game:f shp_per_game:f
fow_per_game:f fol_per_game:f toi_eq_per_game:f toi_pp_per_game:f toi_pk_per_game:f
face_offs_per_game_percentage:f toi_average:f plus_minus:i tgs:i sep:i toi_percentage:f gwit:i glit:i sag:i
sago:i sam:i sap:i sab:i saslot:i sagoslot:i aeq:i app:i apk:i ha:i fo2:i fo5:i fo10:i gm:i mp:i pm:i pwg:i
sgo:i fowoz:i fownz:i fowdz:i foloz:i folnz:i foldz:i psg:i pssvs:i got:i ot_rank:i ot_g:i ot_a1:i ot_a2:i
ot_toi:i ot_a:i ot_p:i psgwg:i sogslot:i pswg:i toishi:f shigp:f saslotgp:f samgp:f sapgp:f svs_pslot:f
sa_per_game:f otw:i otl:i fopoz:f fopnz:f fopdz:f fot:i fogp:f psm:i ps_percentage:f gw_shutout:i
sogslot_per_game:f psag:i pssvs_percentage:f show_rank:b chf:i team_stats:s
"""
_TEAM_ROW = """
rank:i team_id:i name:s short_name:s streak:s gp:i gw:i gwot:i gwpe:i gl:i glot:i glpe:i po:i g:i geq:i gpp:i
gpk:i ga:i gaeq:i gapp:i gapk:i go:i ppo:i pks:i sog:i penalties:i pim:i fow:i fol:i shm:i shp:i shb:i bks:i
sa:i sam:i sap:i sab:i sort_order:i website:s position_change:s is_live:b current_result:i
current_opponent_result:i pos:i hg:i gsow:i gfd:i gff:i gfi:i gasow:i pp:i bp:i fo2:i fo5:i fo10:i gm:i mp:i
ps:i psg:i psm:i psa:i psga:i pssvs:i spec:i cap:i fowoz:i fownz:i fowdz:i foloz:i folnz:i foldz:i sgo:i sslot:i
sag:i sago:i saslot:i qualification_status:s is_qualified_chl:b goal_efficiency:f ppg_efficiency:f ppggp:f
ppopgp:f ppgp:f gpg:f gagp:f gappgp:f geqgp:f gaeqgp:f bks_per_game:f pim_per_game:f ps_percentage:f
attendance:i capacity:i utilization:f soggp:f sslotgp:f shmgp:f shpgp:f gpkgp:f pk_efficiency:f pksgp:f gapkgp:f
bpgp:f fot:i face_offs_percentage:f fogp:f fowgp:f folgp:f fopoz:f fopnz:f fopdz:f sagp:f saslotgp:f gffd:i
gffp:f gfdp:f gfip:f pssvs_percentage:f pgp:f gwit:i gwo:i glit:i glo:i
"""
_TEAM_INFO = """
name:s cms_node_id:i url_segment:s type:s content_type:s url:s fields_grid:s fields_logo_name:s fields_logo_id:i
fields_logo_url_segment:s fields_logo_type:s fields_logo_content_type:s fields_logo_url:s
fields_logo_fields_umbraco_file_src:s fields_logo_fields_umbraco_file_focal_point:s
fields_logo_fields_umbraco_file_crops:s fields_logo_fields_umbraco_width:i fields_logo_fields_umbraco_height:i
fields_logo_fields_umbraco_bytes:i fields_logo_fields_umbraco_extension:s team_id:i fields_website:s
fields_ticket_url:s fields_game_ticket_url:s fields_overriden_name:s fields_players:s fields_api_name:s
"""
_OVERVIEW = """
game_id:i home_team_short_name:s home_team_name:s away_team_short_name:s away_team_name:s home_team_id:i
away_team_id:i vimeo_video_id:i date:s status:s base_status:s home_team_result:i away_team_result:i
is_overtime:b is_shootout:b highlight_vimeo_video_id:i has_highlight:b is_current:b is_current_for_team:b
tv_partner:s spectators:i has_my_sports:b referees:s is_exhibition:b home_team_coach:s away_team_coach:s
game_time:i show_game_time:b arena:s home_team_first_result:i away_team_first_result:i home_team_second_result:i
away_team_second_result:i home_team_third_result:i away_team_third_result:i home_team_overtime_result:i
away_team_overtime_result:i home_team_overtime1_result:i away_team_overtime1_result:i
home_team_overtime2_result:i away_team_overtime2_result:i home_team_overtime3_result:i
away_team_overtime3_result:i home_team_shootout_result:i away_team_shootout_result:i home_aggregated_score:s
away_aggregated_score:s
"""
_PBP = """
game_id:i event_index:i team_side:s game_seconds:i team_id:i home_team:b time:s video_time:i video_vimeo_id:i
action:s foul_type:i foul_minutes:i player_first_name:s player_last_name:s player_number:s player_total:i
period:i overtime:i goal_situation:s home_team_result:i away_team_result:i assist1_first_name:s
assist1_last_name:s assist1_number:s assist1_total:i assist2_first_name:s assist2_last_name:s assist2_number:s
assist2_total:i situation:s revoked_goal_type:i revoked_goal_sub_type:i review_successful:s review_goal_counts:b
"""
_LINEUPS = """
game_id:i team_side:s team_id:i unit:s player_id:i position:s line:i first_name:s last_name:s number:s
is_topscorer:b is_captain:b first_goalkeeper:b
"""
_PLAYER_BOX = """
game_id:i team_side:s team_id:i player_id:i number:s first_name:s last_name:s short_name:s name:s position:s
fow:i fol:i sog:i shm:i shp:i shb:i bks:i toi:i toi_eq:i toi_pp:i toi_pk:i pim:i g:i a1:i a2:i pl:i mi:i mip:i
ga:i svs:i sa:i gaa:f save_percentage:f faceoffs:i faceoffs_percentage:f a:i pts:i pl_mi:i
"""
_TEAM_STATS = """
game_id:i team_side:s team_id:i g:i ga:i sa:i sog1:i sog2:i sog3:i sog_ot:i sog_ot1:i sog_ot2:i sog_ot3:i shm:i
shp:i shb:i bks:i fow_d:i fow_n:i fow_o:i fol_n:i fol_o:i fol_d:i ppt:i ppo:i ppg:i ppga:i pkt:i pks:i pkg:i
pkga:i faceoffs:i pim:i sog:i sog_percentage:f pp_percentage:f pk_percentage:f fow:i fol:i faceoffs_percentage:f
"""
_SHOTS = """
game_id:i team_side:s team_id:i player_id:i player_number:s player_first_name:s player_last_name:s period:i
time:i strength:s shot_type:s pos_x:f pos_y:f pos_x_percentage:f pos_y_percentage:f video_time:i xg:f xg_sum:f
unix:i unix_start:i
"""

_SCHEMAS: Dict[str, Dict[str, pl.DataType]] = {
    "games": _schema(_GAME_ROW),
    "bracket": _schema(_SERIES + " " + _GAME_ROW),
    "player_stats": _schema(_PLAYER_ROW),
    "standings": _schema(_TEAM_ROW),
    "team_info": _schema(_TEAM_INFO),
    "game_overview": _schema(_OVERVIEW),
    "pbp": _schema(_PBP),
    "lineups": _schema(_LINEUPS),
    "player_box": _schema(_PLAYER_BOX),
    "team_stats": _schema(_TEAM_STATS),
    "shots": _schema(_SHOTS),
}

# ``fo`` is penalties in season rows and faceoffs in game rows (module docstring).
_SEASON_RENAMES = {"fo": "penalties"}
_GAME_BOX_RENAMES = {"fo": "faceoffs", "fo_percentage": "faceoffs_percentage"}


# ---------------------------------------------------------------------------
# Row-shaped routes
# ---------------------------------------------------------------------------


def parse_nationalleague(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse any National League body by the row rule, typing every column from its values.

    A list becomes one row per object item, a non-empty object one row (nested objects flatten
    to ``parent_child`` columns), anything else a zero-row frame. The route-specific parsers
    below add the documented schema; use this one for a route they do not cover.

    Args:
        raw (list | dict | None): a JSON body from ``www.nationalleague.ch/api``.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per item, snake_cased columns; ``*_id`` columns
        are ``Int64``.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import parse_nationalleague
            parse_nationalleague([{"gameId": "20261105000325", "arena": "zondacrypto-Arena"}])

    See Also:
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _frame(_flat_rows(raw), {}, return_as_pandas)


def parse_nationalleague_games(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a games list (``/games``, ``/games/current``, ``/games/team/{teamId}``): one row per game.

    Args:
        raw (list | None): the games list body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per game -- ``game_id``, the teams
        (``home_team_id`` / ``away_team_id`` and names), ``date`` (ISO 8601, UTC), ``status``,
        the score, ``spectators`` (``Int64``), ``referees`` / ``tv_partner`` (JSON lists) and
        ``is_exhibition``. Zero rows with the same columns for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_schedule
            nl_schedule(season=2026)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_schedule` - the route this works on.
        * :func:`sportsdataverse.hockey.nl.nl_current_games` - the route this works on.
        * :func:`sportsdataverse.hockey.nl.nl_team_schedule` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _frame(_flat_rows(raw, lists_only=True), _SCHEMAS["games"], return_as_pandas)


def parse_nationalleague_bracket(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a playoff / playout tree (rounds -> series -> games): one row per game.

    ``round`` is the round key (``final``, ``semifinal``, ``quarterfinal``, ``playIn``,
    ``playoutFinal``, ``leagueQualifications``); ``series_group`` numbers the series within it,
    and the ``series_*`` columns carry the series score (games won), ``series_best_of`` and
    ``series_title``. A round with no series yet contributes no rows.

    Args:
        raw (list | None): the ``/games/playoffs`` or ``/games/playouts`` body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): the series columns, then the same game columns as
        :func:`parse_nationalleague_games`. Zero rows with the same columns for an empty or
        malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_playoffs
            nl_playoffs(season=2026)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_playoffs` - the route this works on.
        * :func:`sportsdataverse.hockey.nl.nl_playouts` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows: List[Dict[str, Any]] = []
    for rnd in _rows(_scrub(raw)) if isinstance(raw, list) else []:
        for series in _list(rnd.get("games")):
            if not isinstance(series, dict):
                continue
            head = {f"series_{k}": v for k, v in _flatten({k: v for k, v in series.items() if k != "games"}).items()}
            head["round"] = rnd.get("round")
            for game in _list(series.get("games")):
                if isinstance(game, dict):
                    rows.append({**head, **_flatten(game)})
    return _frame(rows, _SCHEMAS["bracket"], return_as_pandas)


def parse_nationalleague_player_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse season player rows (``/player``, ``/player/topscorer``, ``/player/team/{teamId}``, ``/player/{playerId}``).

    One row per player, keyed by ``player_id`` (the SIHF licence number, ``Int64``), with the
    team, bio and every season counter and rate. ``penalties`` is the upstream ``fo``
    (penalties taken: ``fo2`` + ``fo5`` + ``fo10`` + ``gm`` + ``mp``); faceoffs are
    ``face_offs`` / ``fot``. Unplayed stats are 0, not null; percentages without a
    denominator are -1. Time-on-ice columns are seconds; ``mip`` is hundredths of a minute.

    Args:
        raw (list | dict | None): the player list, or the one-player object.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per player; rates are ``Float64``. Zero rows with
        the same columns for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_player_stats
            nl_player_stats(season=2026, phase=4940)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_player_stats` - the route this works on.
        * :func:`sportsdataverse.hockey.nl.nl_top_scorers` - the route this works on.
        * :func:`sportsdataverse.hockey.nl.nl_team_player_stats` - the route this works on.
        * :func:`sportsdataverse.hockey.nl.nl_player_season` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _frame(_flat_rows(raw, _SEASON_RENAMES), _SCHEMAS["player_stats"], return_as_pandas)


def parse_nationalleague_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/teams``: the standings, one row per team with its season stats.

    ``penalties`` is the upstream ``fo`` (penalties taken); faceoffs are ``fow`` / ``fol`` /
    ``fot``. ``gw`` / ``gl`` count every win / loss, overtime and shootout included
    (``gwot`` / ``gwpe`` / ``glot`` / ``glpe`` split them out).

    Args:
        raw (list | None): the ``/teams`` body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per team, ``team_id`` ``Int64``, rates
        ``Float64``, ``streak`` a JSON list. Zero rows with the same columns for an empty or
        malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_standings
            nl_standings(season=2026, phase=4940)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_standings` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _frame(_flat_rows(raw, _SEASON_RENAMES, lists_only=True), _SCHEMAS["standings"], return_as_pandas)


def parse_nationalleague_team_info(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/teams/{teamId}/info``: the club's CMS record as one row.

    ``team_id`` is ``fields.teamID`` (the SIHF team id); the record's own ``id`` is an Umbraco
    CMS node and lands in ``cms_node_id``. The club backend key (``fields.apiKey``) is dropped
    and never returned.

    Args:
        raw (dict | None): the CMS record body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row; nested ``fields`` flatten to ``fields_*`` columns.
        Zero rows with the same columns for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_team_info
            nl_team_info(team_id=101144)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_team_info` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    renames = {"id": "cms_node_id", "fields_team_id": "team_id"}
    return _frame(_flat_rows(raw, renames), _SCHEMAS["team_info"], return_as_pandas)


# ---------------------------------------------------------------------------
# The game page (/games/{gameId})
# ---------------------------------------------------------------------------


def _game(raw: Any) -> Dict[str, Any]:
    return _scrub(raw) if isinstance(raw, dict) else {}


def _obj(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _sides(game: Mapping[str, Any]) -> Dict[str, Any]:
    """``{"game_id", "home", "away"}``: the game id and each side's team id, from ``overview``."""
    ov = _obj(game.get("overview"))
    return {"game_id": ov.get("gameId"), "home": ov.get("homeTeamId"), "away": ov.get("awayTeamId")}


def _roster(game: Mapping[str, Any]) -> Dict[tuple, Dict[str, Any]]:
    """``{(side, sweater number): lineup player}`` and ``{(side, player id): ...}`` from the lineups."""
    out: Dict[tuple, Dict[str, Any]] = {}
    for side in ("home", "away"):
        for unit in _list(game.get(f"lineup{side.title()}")):
            for p in _list(_obj(unit).get("players")):
                if isinstance(p, dict):
                    out[(side, "number", str(p.get("number")))] = p
                    out[(side, "player", str(p.get("playerId")))] = p
    return out


def _game_frame(raw: Any, key: str, build: Callable[[Dict[str, Any]], List[Dict[str, Any]]], ret: bool) -> Frame:
    game = _game(raw)
    return _frame(build(game) if game else [], _SCHEMAS[key], ret)


def parse_nationalleague_game_overview(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{gameId}`` into the game header: one row.

    The ``overview`` (teams, date, status, final score, coaches, referees, arena, attendance)
    plus the per-period goals from ``result`` (``home_team_first_result`` ...
    ``away_team_shootout_result``, and the aggregate score of a two-legged tie).

    Args:
        raw (dict | None): the game page body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row. Zero rows with the same columns for an empty or
        malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_game_overview
            nl_game_overview(game_id=20261105000325)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_game_overview` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """

    def build(game: Dict[str, Any]) -> List[Dict[str, Any]]:
        ov = _obj(game.get("overview"))
        if not ov:
            return []
        row = _flatten(ov)
        for k, v in _flatten(_obj(game.get("result"))).items():
            row.setdefault(k, v)
        return [row]

    return _game_frame(raw, "game_overview", build, return_as_pandas)


def _clock_seconds(mmss: Any) -> Optional[int]:
    """``"822"`` (8:22 elapsed, MMSS without a separator) -> 502."""
    text = str(mmss or "").strip()
    if not text.isdigit():
        return None
    return int(text[:-2] or 0) * 60 + int(text[-2:])


def parse_nationalleague_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{gameId}`` ``actions`` into the event timeline: one row per event.

    ``action`` is ``goal``, ``foul`` (a penalty: ``foul_type`` code, ``foul_minutes``), ``in`` /
    ``out`` (a goaltender enters / leaves) or ``revokedGoal``. ``period`` is the period number,
    ``time`` the elapsed game clock as upstream's MMSS string (``"822"`` = 8:22) and
    ``game_seconds`` the same in seconds. Goals carry the assists, ``situation`` tags (``EQ``,
    ``PP1``, ``GWG``, comma-separated) and the score after the goal. Events carry names and
    sweater numbers, not player ids: join to :func:`parse_nationalleague_lineups` on
    (``team_side``, ``player_number``).

    Args:
        raw (dict | None): the game page body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per event, ``event_index`` in timeline order;
        ``team_id`` ``Int64``. Zero rows with the same columns for a game without events, an
        empty or a malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_pbp
            nl_pbp(game_id=20261105000325)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_pbp` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """

    def build(game: Dict[str, Any]) -> List[Dict[str, Any]]:
        sides = _sides(game)
        rows: List[Dict[str, Any]] = []
        for period in _list(game.get("actions")):
            for event in _list(_obj(period).get("actions")):
                if not isinstance(event, dict):
                    continue
                row = _rename(_flatten(event), {"third": "period"})
                home = event.get("homeTeam")
                rows.append(
                    {
                        "game_id": sides["game_id"],
                        "event_index": len(rows) + 1,
                        "team_side": None if home is None else ("home" if home else "away"),
                        "game_seconds": _clock_seconds(event.get("time")),
                        **row,
                    }
                )
        return rows

    return _game_frame(raw, "pbp", build, return_as_pandas)


def parse_nationalleague_lineups(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{gameId}`` ``lineupHome`` / ``lineupAway``: one row per dressed player.

    ``unit`` is the lineup group (``goalkeeper``, then forward/defence lines ``1``-``4``),
    ``line`` the player's line number, ``player_id`` the SIHF licence number (``Int64``).

    Args:
        raw (dict | None): the game page body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per player with ``game_id``, ``team_side``,
        ``team_id``. Zero rows with the same columns for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_lineups
            nl_lineups(game_id=20261105000325)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_lineups` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """

    def build(game: Dict[str, Any]) -> List[Dict[str, Any]]:
        sides = _sides(game)
        rows: List[Dict[str, Any]] = []
        for side in ("home", "away"):
            for unit in _list(game.get(f"lineup{side.title()}")):
                for p in _list(_obj(unit).get("players")):
                    if isinstance(p, dict):
                        head = {"game_id": sides["game_id"], "team_side": side, "team_id": sides[side]}
                        rows.append({**head, "unit": _obj(unit).get("line"), **_flatten(p)})
        return rows

    return _game_frame(raw, "lineups", build, return_as_pandas)


def parse_nationalleague_player_box(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{gameId}`` ``playerStatsHome`` / ``playerStatsAway``: the player box score.

    Upstream rows carry a sweater number and name, no id; ``player_id`` is filled from the
    game's lineup by (``team_side``, ``number``) and is null for a number the lineup lacks.
    ``faceoffs`` is the upstream ``fo`` (faceoffs taken; ``fow`` / ``fol`` won / lost) and
    ``faceoffs_percentage`` its win rate (-1 without faceoffs). Time on ice is seconds;
    goaltender ``mip`` is hundredths of a minute.

    Args:
        raw (dict | None): the game page body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per player with ``game_id``, ``team_side``,
        ``team_id``, ``player_id``. Zero rows with the same columns for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_player_box
            nl_player_box(game_id=20261105000325)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_player_box` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """

    def build(game: Dict[str, Any]) -> List[Dict[str, Any]]:
        sides, roster = _sides(game), _roster(game)
        rows: List[Dict[str, Any]] = []
        for side in ("home", "away"):
            for p in _list(game.get(f"playerStats{side.title()}")):
                if isinstance(p, dict):
                    player = roster.get((side, "number", str(p.get("number"))), {})
                    head = {"game_id": sides["game_id"], "team_side": side, "team_id": sides[side]}
                    rows.append(
                        {**head, "player_id": player.get("playerId"), **_rename(_flatten(p), _GAME_BOX_RENAMES)}
                    )
        return rows

    return _game_frame(raw, "player_box", build, return_as_pandas)


def parse_nationalleague_team_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{gameId}`` ``teamStatsHome`` / ``teamStatsAway``: two rows, home then away.

    Shots by period (``sog1`` ... ``sog_ot3``), missed / post / blocked shots, faceoffs by zone,
    power play and penalty kill (``ppt`` / ``pkt`` seconds, opportunities, goals) and the rates.
    ``faceoffs`` is the upstream ``fo`` (faceoffs taken).

    Args:
        raw (dict | None): the game page body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): two rows with ``game_id``, ``team_side``, ``team_id``.
        Zero rows with the same columns for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_team_stats
            nl_team_stats(game_id=20261105000325)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_team_stats` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """

    def build(game: Dict[str, Any]) -> List[Dict[str, Any]]:
        sides = _sides(game)
        rows = []
        for side in ("home", "away"):
            stats = _obj(game.get(f"teamStats{side.title()}"))
            if stats:
                head = {"game_id": sides["game_id"], "team_side": side, "team_id": sides[side]}
                rows.append({**head, **_rename(_flatten(stats), _GAME_BOX_RENAMES)})
        return rows

    return _game_frame(raw, "team_stats", build, return_as_pandas)


def parse_nationalleague_shots(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{gameId}`` ``shotsHome`` / ``shotsAway``: every shot attempt with location and xG.

    ``shot_type`` is SOG (saved), MISS, POST, BLOCK or GOAL; ``strength`` EQ / PP / PK;
    ``period`` the period number; ``time`` elapsed game seconds. Coordinates are metres on a
    30 x 60 m rink, both running along its length: ``pos_x`` 0-30 across the ice and ``pos_y``
    0-60 from the end boards of the attacked end; shots are mirrored to one attacking end
    (inferred), so ``pos_y`` above 30 is a shot from beyond centre. ``pos_x_percentage`` is
    ``pos_x / 30``; ``pos_y_percentage`` is ``pos_y / 24`` (the app's zone drawing; above 1 for
    long shots). ``xg`` is expected goals, 0-1: non-zero from the 2023 season (one sampled game
    per season), always 0 on a BLOCK; ``xg_sum`` the team's running total. NL games only: a
    Swiss League game has no shots. ``player_id`` (upstream ``pid``) is the shooter's licence
    number; name and number come from the game's lineup.

    Args:
        raw (dict | None): the game page body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame (or pandas): one row per attempt, ordered by ``time``, with
        ``game_id``, ``team_side``, ``team_id``. Zero rows with the same columns for a game
        without shots, an empty or a malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.nl import nl_shots
            nl_shots(game_id=20261105000325)

    See Also:
        * :func:`sportsdataverse.hockey.nl.nl_shots` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    renames = {"pid": "player_id", "sec": "period", "sit": "strength", "type": "shot_type"}

    def build(game: Dict[str, Any]) -> List[Dict[str, Any]]:
        sides, roster = _sides(game), _roster(game)
        rows: List[Dict[str, Any]] = []
        for side in ("home", "away"):
            for shot in _list(game.get(f"shots{side.title()}")):
                if isinstance(shot, dict):
                    who = roster.get((side, "player", str(shot.get("pid"))), {})
                    row = _rename(_flatten(shot), renames)
                    rows.append(
                        {
                            "game_id": sides["game_id"],
                            "team_side": side,
                            "team_id": sides[side],
                            "player_id": row.pop("player_id", None),
                            "player_number": who.get("number"),
                            "player_first_name": who.get("firstName"),
                            "player_last_name": who.get("lastName"),
                            **row,
                        }
                    )
        rows.sort(key=lambda r: r["time"] if isinstance(r.get("time"), int) else -1)
        return rows

    return _game_frame(raw, "shots", build, return_as_pandas)
