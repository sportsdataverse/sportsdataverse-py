"""Parsers for the generated ``kzs`` wrappers (KZS, the Basketball Federation of Slovenia, ``api.kzs.si/api/v1/public``).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns (``dl_utils.underscore``), and a zero-row frame (never an exception) for an empty or malformed
body. The zero-row frame carries the parser's documented returns schema (``kzs_dtypes.DTYPES``, generated with the
schemas), so two empty frames, or an empty one and a populated one, stack.

**Input.** ``kzs_runtime._get`` unwraps the ``{"status": "OK", "data": ...}`` envelope and stamps the request under
``_request`` (``competition_id``, ``match_id``, ``team_id``, ``player_id``, ``season_id``, ``season``,
``stats_category``); a bare list (``playingDays``) arrives as ``{"items": [...]}``. The parsers also accept the bare
``data`` object. One body can feed several frames (the match header feeds ``kzs_match`` and ``kzs_period_scores``, the
box score ``kzs_box_score``, ``kzs_team_box_score`` and ``kzs_score_changes``, a competition ``kzs_competition`` and
``kzs_phases``), so fetch it once with ``return_parsed=False`` and parse it as many ways as needed.

**Row rule.** Each route has its own shaper; inside it, nested objects flatten into ``<parent>_<child>`` columns and
lists become JSON strings. Personal and bulky sub-trees are dropped, not flattened: contact details (the API serves
officials' and coaches' emails, phones and home towns publicly), club postal addresses and personnel, the
commissioner's report text, delegation changes, move requests, conclusions and streams. A wire ``id`` is renamed for
what it identifies (``competition_id``, ``match_id``, ``team_id``, ``player_id`` ...).

**Season.** The sdv season is the END year (2026/2027 = 2027). KZS season ids are opaque (27 = 2026/2027) and a season
``name`` is ``START/END``; season-scoped frames carry ``season`` (END year, from the row's own ``season.name`` or
``seasonId``, else from the request the runtime stamps) next to the opaque ``season_id``.

**Id key spaces.** Every id column is ``Int64``:

* ``competition_id``: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608).
  ``phase_id`` and ``group_id`` live inside one competition.
* ``match_id``: the federation key of a game (global). ``genius_match_id`` is the Genius LiveStats id parsed from
  ``fiba_live_stats_url`` (absent before 2015/16; join ``genius`` org ``KZS``).
* Three team keys, never to be joined to one another: ``first_team_id`` / ``second_team_id`` on a match and
  ``match_team_id`` in the play-by-play are the **match-team** (one per match and side); ``team_id`` is one club team
  in one season (``/teams/{id}``, standings, box score, leaderboards); ``club_id`` spans seasons.
  ``team_application_id`` is the team's season registration.
* Two player keys: ``player_id`` is the player registry id (box score, play-by-play, leaderboards, ``/players``);
  ``person_id`` is the person behind it; ``match_team_player_id`` is the per-match entry and ``roster_entry_id`` the
  roster entry. ``identification_number`` is the KZS registration number, another key space.
* ``first_team`` / ``team_side`` ``home`` is the organiser: ``firstTeam`` and ``score1`` on the wire.

**Numbers as strings.** ``jersey_number`` and ``shirt_number`` (strings on the wire), the box score's year of birth
(a string) and the play-by-play's ``score1`` / ``score2`` (numeric strings) are kept as sent, except ``score1`` /
``score2`` of the play-by-play, which are cast to ``Int64``. Percentages are 0-100 and minutes are decimal minutes.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.basketball.kzs.kzs_dtypes import DTYPES
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_kzs_box_score",
    "parse_kzs_competition",
    "parse_kzs_competitions",
    "parse_kzs_match",
    "parse_kzs_matches",
    "parse_kzs_pbp",
    "parse_kzs_pbp_players",
    "parse_kzs_period_scores",
    "parse_kzs_phases",
    "parse_kzs_playing_days",
    "parse_kzs_player",
    "parse_kzs_player_leaderboard",
    "parse_kzs_player_stats",
    "parse_kzs_roster",
    "parse_kzs_rounds",
    "parse_kzs_score_changes",
    "parse_kzs_seasons",
    "parse_kzs_standings",
    "parse_kzs_team",
    "parse_kzs_team_box_score",
    "parse_kzs_team_general",
    "parse_kzs_team_leaderboard",
    "parse_kzs_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``kzs_runtime.REQUEST_KEY``).
_REQUEST = "_request"
_ID_COL = re.compile(r"(^|_)id$")
_DTYPE_CODES = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
#: Wire keys never flattened: contact data and bulky free text / lists (see the module docstring).
_DROP = frozenset(
    {
        "addressesPostOfficeName",
        "email",
        "scoresheetEmail",
        "phone",
        "phoneNumber",
        "mobile",
        "iban",
        "swiftCode",
        "bankName",
        "taxNumber",
        "birthDate",
        "dateOfBirth",
        "coachesIdNumber",
        "commissionerReportHtml",
        "delegationChanges",
        "moveRequests",
        "conclusions",
        "streams",
        "liveStreams",
        "personnel",
        "club",
        "organiserTeam",
        "competitions",
        "scores",
    }
)
_GENIUS = re.compile(r"/u/[A-Za-z0-9_-]+/(\d+)/?")


# --- helpers -----------------------------------------------------------------------------------------------------


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _int(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _request(raw: Any) -> Mapping[str, Any]:
    return _obj(_obj(raw).get(_REQUEST))


def _json(value: Any) -> Optional[str]:
    return json.dumps(value, ensure_ascii=False) if value is not None else None


def _flat(obj: Mapping[str, Any], prefix: str = "", drop: frozenset = _DROP) -> Dict[str, Any]:
    """One flat row: nested objects as ``<parent>_<child>``, lists as JSON strings, ``drop`` keys left out."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        if key == _REQUEST or key in drop:
            continue
        name = prefix + underscore(str(key))
        if isinstance(value, list):
            out[name] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, dict):
            out.update(_flat(value, name + "_", drop))
        else:
            out[name] = value
    return out


def _named(obj: Mapping[str, Any], id_name: str, drop: frozenset = _DROP) -> Dict[str, Any]:
    """Flatten ``obj`` and rename its wire ``id`` to ``id_name`` (first column)."""
    row = _flat(obj, drop=drop)
    return {id_name: row.pop("id", None), **row}


def _end_year(season_id: Optional[int] = None, name: Any = None) -> Optional[int]:
    """The END-year season: the second half of a ``START/END`` name, else from the opaque season id."""
    if isinstance(name, str):
        m = re.match(r"^\s*(\d{4})\s*/\s*(\d{4})\s*$", name)
        if m:
            return int(m.group(2))
    if season_id is not None:
        if 1 <= season_id <= 30:
            return 2000 + season_id
        if 31 <= season_id <= 39:
            return 1961 + season_id
    return None


def _dtype_for(col: str, schema: Optional[Mapping[str, str]] = None) -> Any:
    """A column's dtype without a value to go by: the returns schema, else Int64 for an id, else String."""
    code = (schema or {}).get(col)
    if code:
        return _DTYPE_CODES[code]
    return pl.Int64 if _ID_COL.search(col) else pl.String


def _typed(df: pl.DataFrame) -> pl.DataFrame:
    """Ids -> Int64 (lossless only); an all-null column -> a concrete dtype."""
    df = _cast_ids_int64(df, [c for c in df.columns if _ID_COL.search(c)])
    casts = [pl.col(c).cast(_dtype_for(c)) for c, d in df.schema.items() if d == pl.Null]
    return df.with_columns(casts) if casts else df


def _frame(rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    rows = [r for r in rows if r]
    if not rows:
        return pl.DataFrame()
    try:
        df = pl.from_dicts(rows, infer_schema_length=None, strict=False)
    except Exception:  # noqa: BLE001 -- a malformed body parses to nothing, never raises
        return pl.DataFrame()
    return _typed(df)


def _finish(df: pl.DataFrame, columns: Optional[Sequence[str]], return_as_pandas: bool, parser: str) -> Frame:
    """Apply the returns-schema dtypes and the documented columns.

    An all-null column (and a whole-number rate) takes the schema dtype; an empty frame keeps the documented
    columns and a missing column is added as nulls, all typed like a populated frame.
    """
    schema = DTYPES.get(parser, {})
    if schema and df.width:
        casts = []
        for col, dtype in df.schema.items():
            want = _dtype_for(col, schema) if col in schema else dtype
            if dtype != want and (df[col].null_count() == df.height or (want == pl.Float64 and dtype.is_numeric())):
                casts.append(pl.col(col).cast(want))
        df = df.with_columns(casts) if casts else df
    columns = list(columns) if columns else list(schema)
    if columns:
        if df.width == 0:
            df = pl.DataFrame(schema={c: _dtype_for(c, schema) for c in columns})
        else:
            missing = [pl.lit(None, dtype=_dtype_for(c, schema)).alias(c) for c in columns if c not in df.columns]
            if missing:
                df = df.with_columns(missing)
            # the documented order first, so a populated frame stacks onto an empty one
            df = df.select([*columns, *[c for c in df.columns if c not in columns]])
    return df.to_pandas() if return_as_pandas else df


def _items(raw: Any) -> List[Mapping[str, Any]]:
    """Rows of a list route: ``items`` of the data object, or a bare list."""
    if isinstance(raw, list):
        return _list(raw)
    return _list(_obj(raw).get("items"))


# --- seasons, competitions ----------------------------------------------------------------------------------------


def parse_kzs_seasons(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/seasons/`` -> one row per season (39: 1991/1992 to 2029/2030).

    Args:
        raw: the ``/seasons/`` data (``items`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per season: ``season_id`` (opaque, 27 = 2026/2027), ``name`` (``START/END``), ``is_active`` (the
        current season only) and ``season`` (the sdv END year). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_seasons
            parse_kzs_seasons({"items": [{"id": 27, "name": "2026/2027", "isActive": True, "active": True}]})
    """
    rows = []
    for s in _items(raw):
        row = _named(s, "season_id")
        rows.append({**row, "season": _end_year(_int(s.get("id")), s.get("name"))})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_seasons")


def _competition_row(c: Mapping[str, Any]) -> Dict[str, Any]:
    row = _named(c, "competition_id", _DROP | {"phases", "socialMedias"})
    sid = _int(_obj(c.get("season")).get("id"))
    row["season"] = _end_year(sid, _obj(c.get("season")).get("name"))
    return row


def parse_kzs_competitions(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/competitions/`` -> one row per competition (every league, cup, youth and 3x3 competition).

    Args:
        raw: the ``/competitions/`` data (``items`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per competition: ``competition_id`` (per season), ``name`` (Slovenian, sponsor-dependent),
        ``gender``, ``category``, ``rank``, ``type`` (absent on two competitions), the opaque ``season_id`` and the
        sdv ``season`` (END year). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_competitions
            parse_kzs_competitions({"items": [{"id": 579, "name": "Liga OTP banka", "season": {"id": 26, "name": "2025/2026"}}]})
    """
    return _finish(
        _frame(_competition_row(c) for c in _items(raw)), columns, return_as_pandas, "parse_kzs_competitions"
    )


def parse_kzs_competition(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/competitions/{id}`` -> the competition header, one row (its phases are ``kzs_phases``).

    Args:
        raw: the ``/competitions/{id}`` data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``competition_id``, ``name``, ``gender``, ``category``, ``rank``, ``type``, the season and the
        competition's social media as a JSON string. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_competition
            parse_kzs_competition({"id": 579, "name": "Liga OTP banka", "season": {"id": 26, "name": "2025/2026"}})
    """
    body = _header(raw)
    if not body:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_kzs_competition")
    row = _competition_row(body)
    row["social_medias"] = _json(body.get("socialMedias"))
    return _finish(_frame([row]), columns, return_as_pandas, "parse_kzs_competition")


def parse_kzs_phases(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/competitions/{id}`` -> one row per phase of the competition (regular season, play-offs, finals ...).

    Args:
        raw: the ``/competitions/{id}`` data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per phase: ``competition_id``, ``season``, ``phase_id`` (inside one competition), ``name``,
        ``start_date``, ``end_date`` (UTC midnight strings), ``type``, ``wins_needed`` (series phases only) and the
        phase's groups as a JSON string. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_phases
            parse_kzs_phases({"id": 579, "season": {"id": 26, "name": "2025/2026"}, "phases": [{"id": 5811, "name": "Redni del"}]})
    """
    body = _obj(raw)
    season = _end_year(_int(_obj(body.get("season")).get("id")), _obj(body.get("season")).get("name"))
    rows = []
    for p in _list(body.get("phases")):
        row = _named(p, "phase_id")
        rows.append({"competition_id": _int(body.get("id")), "season": season, **row})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_phases")


def parse_kzs_rounds(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/competitions/{id}/rounds`` -> the number of rounds of each phase.

    Args:
        raw: the rounds data object (``phases`` list), with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per phase: ``competition_id`` (the request's), ``phase_id`` and ``rounds``. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_rounds
            parse_kzs_rounds({"phases": [{"id": 5811, "rounds": 27}], "_request": {"competition_id": 579}})
    """
    cid = _int(_request(raw).get("competition_id"))
    rows = [{"competition_id": cid, **_named(p, "phase_id")} for p in _list(_obj(raw).get("phases"))]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_rounds")


def _leaderboard(raw: Any, parser: str, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    ctx = _request(raw)
    rows = []
    for r in _items(raw):
        rows.append(
            {
                "competition_id": _int(ctx.get("competition_id")),
                "stats_category": ctx.get("stats_category"),
                **_flat(r),
            }
        )
    return _finish(_frame(rows), columns, as_pandas, parser)


def parse_kzs_player_leaderboard(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/competitions/{id}/playerStats`` -> a player leaderboard for one stat category.

    Args:
        raw: the leaderboard data (``items`` list), with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: ``competition_id`` and ``stats_category`` (the request's: the rows echo neither),
        ``player_id``, the player's club and team (``club_id``, ``team_id``), ``games_played`` and the three
        figures of the category: ``sum`` (season total), ``average`` (per game) and ``max`` (single-game best).
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_player_leaderboard
            parse_kzs_player_leaderboard({"items": [{"playerId": 1, "teamId": 2, "sum": 10.0, "average": 5.0, "max": 7.0}]})
    """
    return _leaderboard(raw, "parse_kzs_player_leaderboard", columns, return_as_pandas)


def parse_kzs_team_leaderboard(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/competitions/{id}/teamStats`` -> a team leaderboard for one stat category.

    Args:
        raw: the leaderboard data (``items`` list), with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team: ``competition_id`` and ``stats_category`` (the request's), ``team_id``, ``club_id``,
        ``games_played`` and ``sum`` / ``average`` / ``max`` of the category. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_team_leaderboard
            parse_kzs_team_leaderboard({"items": [{"teamId": 2, "gamesPlayed": 27, "sum": 2547.0, "average": 94.3}]})
    """
    return _leaderboard(raw, "parse_kzs_team_leaderboard", columns, return_as_pandas)


# --- matches ------------------------------------------------------------------------------------------------------


def _competition_entry(match: Mapping[str, Any], cid: Optional[int]) -> Dict[str, Any]:
    """The match's competition entry (the requested competition when it lists several) as flat columns."""
    entries = _list(match.get("competitions"))
    entry = next(
        (e for e in entries if cid is not None and e.get("competitionId") == cid), entries[0] if entries else {}
    )
    out = {
        "competition_id": _int(entry.get("competitionId")),
        "competition_name": entry.get("competitionName"),
        "competition_type": entry.get("competitionType"),
        "competition_phase_id": _int(entry.get("competitionPhaseId", _obj(entry.get("competitionPhase")).get("id"))),
        "competition_phase_name": entry.get("competitionPhaseName", _obj(entry.get("competitionPhase")).get("name")),
        "transferred": entry.get("transferred"),
        "season_id": _int(entry.get("seasonId")),
    }
    out["season"] = _end_year(out["season_id"])
    return out


def _match_row(m: Mapping[str, Any], cid: Optional[int]) -> Dict[str, Any]:
    row = _named(m, "match_id")
    url = row.get("fiba_live_stats_url")
    found = _GENIUS.search(url) if isinstance(url, str) else None
    head = {"match_id": row.pop("match_id"), **_competition_entry(m, cid)}
    organiser = _int(_obj(m.get("organiserTeam")).get("id"))
    return {
        **head,
        "genius_match_id": int(found.group(1)) if found else None,
        "organiser_team_id": organiser,
        **row,
    }


def parse_kzs_matches(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/matches/`` -> one row per match of a competition (optionally inside a from/to window).

    Args:
        raw: the ``/matches/`` data (``items`` list), with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per match: ``match_id`` (global), the competition entry (``competition_id``, ``competition_name``,
        ``competition_phase_id`` ...), ``season`` (END year), ``genius_match_id`` (parsed from
        ``fiba_live_stats_url``; null before 2015/16), the match-team ids (``first_team_id``, ``second_team_id``) and
        team ids (``first_team_team_id``, ``second_team_team_id``) of the home (first) and away (second) side,
        scores, ``status`` (``FINISHED`` ...) and the officials (names only). Delegation changes, move requests,
        conclusions and streams are left out. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_matches
            parse_kzs_matches({"items": [{"id": 101846, "firstTeamId": 1, "secondTeamId": 2, "status": "FINISHED"}]})
    """
    cid = _int(_request(raw).get("competition_id"))
    return _finish(_frame(_match_row(m, cid) for m in _items(raw)), columns, return_as_pandas, "parse_kzs_matches")


def _header(raw: Any, key: str = "id") -> Mapping[str, Any]:
    """A single-object body, or ``{}`` when it lacks ``key`` (an empty body, a request stamp alone, a list route)."""
    body = _obj(raw)
    return body if key in body else {}


def parse_kzs_match(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/matches/{id}`` -> the match header, one row.

    Args:
        raw: the match header data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``match_id``, the competition entry and ``season``, ``genius_match_id``, ``status``, the two sides
        (``first_team_*`` is the home/organiser side: match-team ``first_team_id``, ``first_team_team_id`` and
        its ``best_performer_*`` line), the venue, ``attendance``, the officials' names, ``bests`` (JSON) and the
        final scores (quarter scores are ``kzs_period_scores``). The commissioner report, delegation changes, move
        requests and conclusions are left out. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_match
            parse_kzs_match({"id": 101839, "status": "FINISHED", "firstTeam": {"id": 1, "teamId": 2}})
    """
    body = _header(raw)
    if not body:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_kzs_match")
    return _finish(_frame([_match_row(body, None)]), columns, return_as_pandas, "parse_kzs_match")


def parse_kzs_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/matches/{id}`` -> the score by match part (quarters and overtimes), one row each.

    Args:
        raw: the match header data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per match part: ``match_id``, ``score_id``, ``match_part`` (``QUARTER1`` ... ), and the points
        scored in that part by the home (``first_team_score``) and away (``second_team_score``) side.
        Zero rows for an empty or malformed body or a match without part scores.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_period_scores
            parse_kzs_period_scores({"id": 101839, "scores": [{"id": 1, "matchPart": "QUARTER1", "firstTeamScore": 25, "secondTeamScore": 24}]})
    """
    body = _header(raw)
    rows = [{"match_id": _int(body.get("id")), **_named(s, "score_id")} for s in _list(body.get("scores"))]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_period_scores")


# --- play-by-play -------------------------------------------------------------------------------------------------


def _pbp_sides(body: Mapping[str, Any]) -> Dict[int, str]:
    """``{match_team_id: home|away}`` from the players the feed lists for each side."""
    first = {p.get("id") for p in _list(body.get("firstTeamPlayers"))}
    second = {p.get("id") for p in _list(body.get("secondTeamPlayers"))}
    sides: Dict[int, str] = {}
    for a in _list(body.get("actions")):
        team, player = _int(a.get("matchTeamId")), a.get("playerId")
        if team is None or team in sides:
            continue
        if player in first:
            sides[team] = "home"
        elif player in second:
            sides[team] = "away"
    return sides


def parse_kzs_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/matches/{id}/playByPlay`` -> one row per action, in game order (Genius Sports actions relayed by KZS).

    Args:
        raw: the play-by-play data object (``actions``, ``firstTeamPlayers``, ``secondTeamPlayers``), with the
            runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per action: ``match_id`` (the request's: the body has none), ``action_number`` (1-based order in the
        feed), ``team_side`` (``home`` / ``away``, from the players the feed lists for each side; null for a clock
        row without a team), ``match_team_id`` (per match: not a ``team_id``), ``player_id`` (registry id),
        ``match_team_player_id``, ``action_type``, ``sub_type``, ``qualifiers`` (``;``-separated), ``clock``,
        ``period``, ``score1`` / ``score2`` (home / away points after the action, cast from numeric strings),
        ``x`` / ``y`` (Genius court coordinates, percent; shots carry them, most other rows 0), ``side`` (the basket
        end, LEFT / RIGHT; absent on non-shot rows and on some 2020/21 shots), ``area`` and ``success``. The feed
        is not strictly ordered: the score on a non-scoring row can lag the true score and the closing rows can carry
        ``period`` 1 (both seen in game 101839); trust the scoring rows and the final row. Zero rows
        for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_pbp
            parse_kzs_pbp({"actions": [{"actionType": "2pt", "playerId": 7, "matchTeamId": 9, "x": 12.5, "y": 40.0}], "firstTeamPlayers": [{"id": 7}]})
    """
    body = _obj(raw)
    mid = _int(_request(raw).get("match_id"))
    sides = _pbp_sides(body)
    rows = []
    for n, a in enumerate(_list(body.get("actions")), start=1):
        row = _flat(a)
        for key in ("score1", "score2"):
            val = row.get(key)
            row[key] = int(val) if isinstance(val, str) and val.strip().isdigit() else _int(val)
        rows.append(
            {
                "match_id": mid,
                "action_number": n,
                "team_side": sides.get(_int(a.get("matchTeamId")) or -1),
                **row,
            }
        )
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_pbp")


def parse_kzs_pbp_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/matches/{id}/playByPlay`` -> the players the feed lists for each side.

    Args:
        raw: the play-by-play data object, with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per listed player: ``match_id`` (the request's), ``team_side`` (``home`` for ``firstTeamPlayers``),
        ``player_id`` (registry id), ``first_name`` and ``last_name``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_pbp_players
            parse_kzs_pbp_players({"firstTeamPlayers": [{"id": 7, "firstName": "A", "lastName": "B"}]})
    """
    body = _obj(raw)
    mid = _int(_request(raw).get("match_id"))
    rows = []
    for side, key in (("home", "firstTeamPlayers"), ("away", "secondTeamPlayers")):
        for p in _list(body.get(key)):
            rows.append({"match_id": mid, "team_side": side, **_named(p, "player_id")})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_pbp_players")


# --- box score ----------------------------------------------------------------------------------------------------

_SIDES = (("home", "firstTeam"), ("away", "secondTeam"))


def _box_ctx(body: Mapping[str, Any], side: str, block: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "match_id": _int(body.get("matchId")),
        "team_side": side,
        "team_id": _int(block.get("teamId")),
        "team_name": block.get("teamName"),
    }


def parse_kzs_box_score(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/matches/{id}/stats`` -> one row per player of both teams.

    Args:
        raw: the box score data object (``firstTeam`` / ``secondTeam`` blocks with ``playerStats``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: ``match_id``, ``team_side`` (``home`` = ``firstTeam``), ``team_id`` (one club team in one
        season), ``player_id`` (registry id), ``jersey_number`` (string), ``minutes`` (decimal), ``played``,
        ``starting_lineup``, ``captain`` and the counting stats (``two_pm``, ``three_pa``, ``f_tm`` ... the wire
        names snake-cased). Percentages are 0-100. The advanced fields (``offensive_rating``, ``usage_percentage`` ...)
        are 0 in every observed game: the feed does not compute them. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_box_score
            parse_kzs_box_score({"matchId": 101839, "firstTeam": {"teamId": 1, "playerStats": [{"playerId": 7, "points": 12}]}})
    """
    body = _obj(raw)
    rows = []
    for side, key in _SIDES:
        block = _obj(body.get(key))
        for p in _list(block.get("playerStats")):
            rows.append({**_box_ctx(body, side, block), **_flat(p)})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_box_score")


def parse_kzs_team_box_score(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/matches/{id}/stats`` -> the team totals, one row per side.

    Args:
        raw: the box score data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per side: ``match_id``, ``team_side``, ``team_id``, ``team_name`` and the team's totals (the same
        stat columns as ``kzs_box_score`` plus the team-only ones: ``rebounds_team``, ``turnovers_team``,
        ``bench_points``, ``biggest_lead`` ...). In 2015/16 the team ``minutes`` is 0 and ``efficiency`` -1 (not
        computed in that era). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_team_box_score
            parse_kzs_team_box_score({"matchId": 101839, "firstTeam": {"teamId": 1, "teamStats": {"points": 80}}})
    """
    body = _obj(raw)
    rows = []
    for side, key in _SIDES:
        block = _obj(body.get(key))
        if isinstance(block.get("teamStats"), dict):
            rows.append({**_box_ctx(body, side, block), **_flat(block["teamStats"])})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_team_box_score")


def parse_kzs_score_changes(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/matches/{id}/stats`` -> the score at the end of each minute of the game.

    Args:
        raw: the box score data object (``scoreChanges``, ``totalMinutes``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per minute: ``match_id``, ``minute``, ``score1`` / ``score2`` (home / away points) and
        ``total_minutes`` of the game. For games without live stats (2007/08) every score is 0. Zero rows for an
        empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_score_changes
            parse_kzs_score_changes({"matchId": 1, "totalMinutes": 40, "scoreChanges": [{"minute": 1, "score1": 2, "score2": 0}]})
    """
    body = _obj(raw)
    rows = [
        {"match_id": _int(body.get("matchId")), **_flat(s), "total_minutes": _int(body.get("totalMinutes"))}
        for s in _list(body.get("scoreChanges"))
    ]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_score_changes")


# --- standings, calendar ------------------------------------------------------------------------------------------


def parse_kzs_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/matches/standings`` -> the table of each phase (and group) of a competition.

    Args:
        raw: the standings data object (``competition`` and ``phases``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team and phase (and group, where a phase has groups: ``group_id`` / ``group_name``, else
        null): ``competition_id``, ``phase_id``, ``phase_name``, ``phase_type``, ``position`` (1-based row order
        within the table), ``team_id`` (one club team in one season), ``team_name``, ``club_code``, ``games_played``,
        ``won``, ``lost``, ``draws``, ``points`` (league points), ``pts_plus`` / ``pts_minus`` /
        ``pts_difference``, the home / away records and ``tie_break_position``. The nested ``team`` object repeats
        ``team_id``, ``team_name`` and ``club_code`` and is not flattened. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_standings
            parse_kzs_standings({"competition": {"id": 579}, "phases": [{"competitionPhase": {"id": 5811}, "standings": [{"teamId": 1, "won": 3}]}]})
    """
    body = _obj(raw)
    comp = _obj(body.get("competition"))
    rows = []
    for ph in _list(body.get("phases")):
        phase = _obj(ph.get("competitionPhase"))
        head = {
            "competition_id": _int(comp.get("id")),
            "phase_id": _int(phase.get("id")),
            "phase_name": phase.get("name"),
            "phase_type": phase.get("type"),
        }
        blocks: List[Tuple[Dict[str, Any], List[Mapping[str, Any]]]] = [
            ({"group_id": None, "group_name": None}, _list(ph.get("standings")))
        ]
        for g in _list(ph.get("groups")):
            grp = _obj(g.get("competitionPhaseGroup"))
            blocks.append(({"group_id": _int(grp.get("id")), "group_name": grp.get("name")}, _list(g.get("standings"))))
        for group, table in blocks:
            for n, r in enumerate(table, start=1):
                rows.append({**head, **group, "position": n, **_flat(r, drop=_DROP | {"team"})})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_standings")


def parse_kzs_playing_days(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/matches/{id}/playingDays`` -> the match days of a competition and their game counts.

    Args:
        raw: the playing days (a list, or ``{"items": [...]}`` with the runtime's ``_request`` stamp).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per day: ``competition_id`` (the request's), ``date`` (``YYYY-MM-DD``) and ``no_of_games``. Zero rows
        for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_playing_days
            parse_kzs_playing_days({"items": [{"date": "2025-09-26", "noOfGames": 3}], "_request": {"competition_id": 579}})
    """
    cid = _int(_request(raw).get("competition_id"))
    rows = [{"competition_id": cid, **_flat(d)} for d in _items(raw)]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_playing_days")


# --- teams, rosters, players --------------------------------------------------------------------------------------


def _team_row(t: Mapping[str, Any]) -> Dict[str, Any]:
    app = _obj(t.get("teamApplication"))
    row = {
        "team_id": _int(t.get("id")),
        "team_application_id": _int(app.get("id")),
        "club_id": _int(_obj(app.get("club")).get("id")),
        "club_member": _obj(app.get("club")).get("member"),
        **_flat({k: v for k, v in app.items() if k not in ("id", "club")}),
        **_flat({k: v for k, v in t.items() if k not in ("id", "teamApplication")}),
    }
    return row


def parse_kzs_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/teams/`` -> the teams entered in a competition.

    Args:
        raw: the ``/teams/`` data (``items`` list), with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team: ``competition_id`` (the request's), ``team_id`` (one club team in one season),
        ``team_application_id`` (its season registration), ``club_id`` (spans seasons), ``name``, ``code``,
        ``short_name``, ``category``, ``rank``, ``gender``, the home hall, jersey colours and ``logo_uuid``. The club's
        addresses, personnel and halls are left out. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_teams
            parse_kzs_teams({"items": [{"id": 17551, "teamApplication": {"id": 45242, "name": "Ilirija", "club": {"id": 9}}}]})
    """
    cid = _int(_request(raw).get("competition_id"))
    rows = [{"competition_id": cid, **_team_row(t)} for t in _items(raw)]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_teams")


def parse_kzs_team(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/teams/{id}`` -> a team profile, one row.

    Args:
        raw: the team data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: the ``kzs_teams`` columns, ``season`` (END year of the team's season) and ``competitions`` (the
        competitions the team entered, a JSON string). The club personnel (coaches, with contact details) are left
        out. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_team
            parse_kzs_team({"id": 17551, "teamApplication": {"id": 45242, "season": {"id": 26, "name": "2025/2026"}}})
    """
    body = _header(raw)
    if not body:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_kzs_team")
    app = _obj(body.get("teamApplication"))
    season = _obj(app.get("season"))
    row = _team_row(body)
    row = {
        **row,
        "season": _end_year(_int(season.get("id")), season.get("name")),
        "competitions": _json(body.get("competitions")),
    }
    return _finish(_frame([row]), columns, return_as_pandas, "parse_kzs_team")


def parse_kzs_team_general(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/teams/{id}/general`` -> a team's record, form and best players in one competition.

    Args:
        raw: the team summary data object, with the runtime's ``_request`` stamp. The body echoes neither the team
            nor the competition, so both ids come from the request.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``team_id`` and ``competition_id`` (the request's), ``won``, ``lost``, ``last_games`` (a JSON list
        of results), and the leader in points, rebounds and assists with the average (``best_points_player_*``,
        ``best_average_points`` ...). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_team_general
            parse_kzs_team_general({"won": 20, "lost": 7, "lastGames": ["W", "L"], "_request": {"team_id": 17551}})
    """
    body = _header(raw, "won")
    if not body:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_kzs_team_general")
    ctx = _request(raw)
    row = {"team_id": _int(ctx.get("team_id")), "competition_id": _int(ctx.get("competition_id")), **_flat(body)}
    return _finish(_frame([row]), columns, return_as_pandas, "parse_kzs_team_general")


def _player_row(p: Mapping[str, Any], id_name: str = "player_id") -> Dict[str, Any]:
    person, nat = _obj(p.get("person")), _obj(p.get("nationality"))
    return {
        id_name: _int(p.get("id")),
        "identification_number": _int(p.get("identificationNumber")),
        "person_id": _int(person.get("id")),
        "first_name": person.get("firstName"),
        "last_name": person.get("lastName"),
        "year_of_birth": _int(person.get("yearOfBirth")),
        "nationality_id": _int(nat.get("id")),
        "nationality_alpha2_code": nat.get("alpha2Code"),
        "nationality_alpha3_code": nat.get("alpha3Code"),
        "nationality_name": nat.get("name"),
        "nationality_eng_name": nat.get("engName"),
        "homegrown": p.get("homegrown"),
        "photo_uuid": p.get("photoUuid"),
        "positions": _json(p.get("positions")),
    }


def parse_kzs_roster(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/teamPlayers/`` -> a team's roster in one competition.

    Args:
        raw: the roster data (``items`` list), with the runtime's ``_request`` stamp. The rows carry no team or
            competition id, so both come from the request.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per rostered player: ``team_id`` and ``competition_id`` (the request's), ``roster_entry_id``,
        ``player_id`` (registry id), ``person_id``, ``identification_number`` (KZS registration number),
        ``first_name``, ``last_name``, ``year_of_birth``, nationality, ``homegrown``, ``photo_uuid``, ``positions``
        (JSON) and ``jersey_number`` (string). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_roster
            parse_kzs_roster({"items": [{"id": 5, "player": {"id": 106285, "person": {"id": 109521}}, "jerseyNumber": "7"}]})
    """
    ctx = _request(raw)
    rows = []
    for r in _items(raw):
        rows.append(
            {
                "team_id": _int(ctx.get("team_id")),
                "competition_id": _int(ctx.get("competition_id")),
                "roster_entry_id": _int(r.get("id")),
                **_player_row(_obj(r.get("player"))),
                "jersey_number": r.get("jerseyNumber"),
            }
        )
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_roster")


def parse_kzs_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/players/{id}`` -> a player profile, one row.

    Args:
        raw: the player data object.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``player_id`` (registry id), ``person_id`` (the person behind it), ``identification_number``,
        ``first_name``, ``last_name``, ``year_of_birth`` (integer), nationality, ``homegrown``, ``photo_uuid`` and
        ``positions`` (JSON). Birth dates are not served. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_player
            parse_kzs_player({"id": 106285, "person": {"id": 109521, "firstName": "A", "lastName": "B", "yearOfBirth": 1999}})
    """
    body = _header(raw)
    if not body:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_kzs_player")
    return _finish(_frame([_player_row(body)]), columns, return_as_pandas, "parse_kzs_player")


def parse_kzs_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/players/{id}/stats`` -> a player's totals and averages per competition of one season.

    Args:
        raw: the player stats data object (``competitionStats``), with the runtime's ``_request`` stamp.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per competition: ``player_id``, ``season_id`` (opaque) and ``season`` (END year) from the request,
        ``competition_id``, the totals (``pts``, ``m2`` / ``a2`` made / attempted, ``ass`` ...), the per-game
        averages (``avgpts`` ...) and the competition's name, rank, category and gender. The wire names are kept
        (snake-cased): ``blag`` / ``blfv`` are blocks against / in favour, ``pfcm`` / ``pfrv`` fouls committed /
        received. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kzs import parse_kzs_player_stats
            parse_kzs_player_stats({"competitionStats": [{"competitionId": 579, "pts": 100}], "_request": {"player_id": 106285, "season_id": 26}})
    """
    ctx = _request(raw)
    sid = _int(ctx.get("season_id"))
    rows = [
        {
            "player_id": _int(ctx.get("player_id")),
            "season_id": sid,
            "season": _int(ctx.get("season")) or _end_year(sid),
            **_flat(c),
        }
        for c in _list(_obj(raw).get("competitionStats"))
    ]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_kzs_player_stats")
