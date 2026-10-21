"""Parsers for the KHL webcaster mobile API (``khl_webcaster``: ``GET https://khl.api.webcaster.pro/api/khl_mobile/*.json``).

The webcaster API is the KHL TV app's backend. Its bodies are plain JSON:

* a list of one-key wrappers (``events_v2`` ``[{"event": {...}}]``, ``teams_v2`` ``[{"team": ...}]``,
  ``players_v2`` ``[{"player": ...}]``) -> one row per wrapped object;
* a one-key page object (``event_v2`` ``{"event": {...}}``, ``team_v2`` ``{"team": {...}}``) -> one row;
* a bare list of rows (``players_v2_light``) -> one row each;
* ``data.json`` -> :func:`parse_khl_webcaster_stages` (the season table, one row per stage);
  ``leaders_v2`` -> :func:`parse_khl_webcaster_leaders` (one row per category per player);
  ``tables_v2`` -> :func:`parse_khl_webcaster_tables` (one row per season per team, from
  ``tables.championship``: ``stages[].regular`` repeats the CURRENT season under every season).

Nested objects are flattened to ``<parent>_<key>`` columns (``team_a_khl_id``); list cells (an
event's goals, penalties, per-player match stats) are kept as JSON text.

**Id key spaces.** The webcaster ids (``id``, ``stage_id``, ``team_a_id``, ...) are their OWN id
space and never equal a khl.ru id. Join to the khl.ru family (``khl_rest``) only through the
``*khl_id`` columns: ``stage.khl_id`` = tournament id, ``event.khl_id`` (and ``match_id``) = game id,
``team.khl_id`` = CLUB id (never a team-season id), ``player.khl_id`` = player id. Every id column is
``Int64``, cast losslessly at the boundary.

Contract: polars by default, pandas with ``return_as_pandas=True``; empty or malformed input returns a
zero-row frame (with the ``columns`` schema when the generated wrapper passes it), never an
exception; snake_case columns.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl

from sportsdataverse.errors import _data_only
from sportsdataverse.hockey.khl.khl_rest_parsers import (
    Columns,
    Frame,
    _columns,
    _conform,
    _generic,
    _int,
    _list,
    _obj,
    _out,
    _stat_cast,
    _typed,
)

__all__ = [
    "parse_khl_webcaster",
    "parse_khl_webcaster_leaders",
    "parse_khl_webcaster_stages",
    "parse_khl_webcaster_tables",
]


def _unwrap(item: Any) -> Any:
    """``{"event": {...}}`` -> ``{...}``: a one-key dict wrapping one object."""
    if isinstance(item, dict) and len(item) == 1:
        (inner,) = item.values()
        if isinstance(inner, dict):
            return inner
    return item


@_data_only
def parse_khl_webcaster(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """Generic webcaster body -> rows (see the module docstring for the shapes).

    Args:
        raw: a webcaster body (``events_v2``, ``event_v2``, ``teams_v2``, ``team_v2``, ``players_v2``,
            ``players_v2_light``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row per object (zero rows when unusable, or for an empty page: ``events_v2`` answers
        ``[]`` past the last page).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_webcaster_events
            from sportsdataverse.hockey.khl.khl_webcaster_parsers import parse_khl_webcaster
            raw = khl_webcaster_events(season=2026, return_parsed=False)
            df = parse_khl_webcaster(raw)

        As pandas::

            df_pd = parse_khl_webcaster(raw, return_as_pandas=True)

        Next step::

            df.select("id", "khl_id", "team_a_name", "team_b_name")

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_event` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_events` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_players` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_players_light` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    if isinstance(raw, list):
        rows = [_unwrap(item) for item in raw]
    elif isinstance(raw, dict) and raw:
        rows = [_unwrap(raw)]
    else:
        rows = []
    return _out(_conform(_generic(rows, columns), columns), return_as_pandas)


_STAGES_SCHEMA = {
    "season": pl.Int64,
    "type": pl.Utf8,
    "tournament_id": pl.Int64,
    "stage_id": pl.Int64,
    "season_label": pl.Utf8,
    "title": pl.Utf8,
    "display_rule": pl.Utf8,
    "percent_points_scored": pl.Boolean,
}


def _end_year(label: Any) -> Optional[int]:
    """``"2025/2026"`` -> 2026 (the season key is the END year)."""
    return _int(str(label or "").rsplit("/", 1)[-1])


def parse_khl_webcaster_stages(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``data.json`` -> the season table: one row per stage of ``stages_v2``.

    ``season`` is the END year of the stage's ``"YYYY/YYYY"`` label (2026 = 2025/26), ``tournament_id``
    the khl.ru tournament id (``khl_id``, the ``tnId`` every khl.ru call takes) and ``stage_id`` the
    webcaster stage id (``id``, what the webcaster calls take). ``type`` is ``regular`` or ``playoff``.
    There is no arithmetic rule between the two: 2025/26 is 1369 / 1370 but 2009/10 is 167 / 168 and
    2008/09 is 160 / 165, so a season is always looked up here, never computed. Newest first, as sent.

    Args:
        raw: the ``data.json`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The stages with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_webcaster_stages
            from sportsdataverse.hockey.khl.khl_webcaster_parsers import parse_khl_webcaster_stages
            raw = khl_webcaster_stages(return_parsed=False)
            df = parse_khl_webcaster_stages(raw)

        As pandas::

            df_pd = parse_khl_webcaster_stages(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("season") == 2026)

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_stages` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    stages = _list(_obj(raw).get("stages_v2"))
    rows: List[Dict[str, Any]] = [
        {
            "season": _end_year(_obj(s).get("season")),
            "type": _obj(s).get("type"),
            "tournament_id": _obj(s).get("khl_id"),
            "stage_id": _obj(s).get("id"),
            "season_label": _obj(s).get("season"),
            "title": _obj(s).get("title"),
            "display_rule": _obj(s).get("display_rule"),
            "percent_points_scored": _obj(s).get("percent_points_scored"),
        }
        for s in stages
    ]
    return _out(_typed(rows, _STAGES_SCHEMA), return_as_pandas)


def parse_khl_webcaster_leaders(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``leaders_v2`` -> one row per player per leader category.

    The body is ``[{"name": <category>, "players": [...]}]``; ``category`` names the list and ``rank``
    is the player's 1-based place in it.

    Args:
        raw: the ``leaders_v2`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        The leaders (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_webcaster_leaders
            from sportsdataverse.hockey.khl.khl_webcaster_parsers import parse_khl_webcaster_leaders
            raw = khl_webcaster_leaders(season=2026, return_parsed=False)
            df = parse_khl_webcaster_leaders(raw)

        As pandas::

            df_pd = parse_khl_webcaster_leaders(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("rank") == 1)

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_leaders` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    columns = _columns(columns, "khl_webcaster_leaders")
    rows = [
        {"category": _obj(cat).get("name"), "rank": i, **_obj(p)}
        for cat in _list(raw)
        for i, p in enumerate(_list(_obj(cat).get("players")), start=1)
    ]
    return _out(_conform(_generic(rows, columns), columns), return_as_pandas)


_TABLE_TEXT = frozenset(
    {"image", "tv_image", "name", "location", "division", "division_key", "conference", "conference_key"}
)


def parse_khl_webcaster_tables(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``tables_v2`` -> one row per season per team of the championship (league-wide) table.

    Read from ``tables.championship``: the ``stages[].regular`` lists repeat the CURRENT season under
    every season. ``season`` is the END year of the ``"YYYY/YYYY"`` label; the stat columns arrive as
    numeric strings and are typed (``pts_pct`` ``Float64``). ``id`` is the webcaster team id and
    ``khl_id`` the khl.ru club id.

    Args:
        raw: the ``tables_v2`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        The tables (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_webcaster_tables
            from sportsdataverse.hockey.khl.khl_webcaster_parsers import parse_khl_webcaster_tables
            raw = khl_webcaster_tables(return_parsed=False)
            df = parse_khl_webcaster_tables(raw)

        As pandas::

            df_pd = parse_khl_webcaster_tables(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("season") == 2026).select("rank", "name", "pts")

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_webcaster_tables` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    columns = _columns(columns, "khl_webcaster_tables")
    rows = [
        {
            "season": _end_year(_obj(block).get("season")),
            "season_label": _obj(block).get("season"),
            "rank": i,
            **_obj(team),
        }
        for block in _list(raw)
        for i, team in enumerate(_list(_obj(_obj(block).get("tables")).get("championship")), start=1)
    ]
    df = _generic(rows, columns)
    skip = {"season", "season_label", "rank", "id", "khl_id", *_TABLE_TEXT}
    return _out(_conform(_stat_cast(df, [c for c in df.columns if c not in skip]), columns), return_as_pandas)
