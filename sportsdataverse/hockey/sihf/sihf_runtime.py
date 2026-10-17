"""Runtime for the generated SIHF wrappers (``data.sihf.ch/statistic/api/cms``).

The Swiss Ice Hockey Federation's statistics API is keyless JSON. Four contracts sit on
top of the shared getter, all measured live (2026-10-08):

* **The league is a scope, not a filter.** ``/table`` takes ``searchQuery=1//<league>``
  (``1`` = the championship tournament, the last segment = the league). Sent as a
  ``League`` filter instead, the league works for past seasons but the CURRENT season
  answers ``400 {"error": "MissingLeagueId"}``. The wrappers send ``League`` like any
  other parameter; :func:`_get` moves it into ``searchQuery``.
* **The other filters are one composite pair.** ``filterBy=Season,Phase,Date`` +
  ``filterQuery=2026/4940/<range>`` (names and values in the same order). :func:`_get`
  folds the ``Season`` / ``Phase`` / ``Date`` / ``ContentType`` parameters that are set
  into the pair. Never send
  ``Phase=all``: ``standing`` answers it with the regular season and ``player`` /
  ``goalkeeper`` with Playoff Overall (one phase each), while ``results`` answers every
  game. No Phase filter is the documented way to get the whole season.
* **An empty Phase asks for the phase list.** The ``Phase`` echo lists its ``entries``
  (the phase ids of that league, season and table) only when ``Phase`` is in
  ``filterBy``. Sent empty (``filterQuery=2026/``), SIHF picks its own default phase and
  still lists every one; ``sihf_phases`` sends that. An empty Phase is not checked
  against the echo (SIHF chooses the phase), but its ``entries`` list must be there.
* **Results need a season-wide date range.** Without a ``Date`` filter the current
  season's ``results`` table answers only today's game day (one row). ``results``
  therefore always sends ``Date=01.07.<season-1>-30.06.<season>``.
* **The standings need ``ContentType``.** ``standing`` without a ``ContentType`` filter
  answers the AWAY-games table (26 of the 52 National League games in 2025-26, measured
  2026-10-12 against the season's ``results``) and does not echo the filter. ``ContentType=All``
  answers the full table and echoes ``All``; ``Home`` / ``Away`` give the split tables. The
  ``sihf_standings`` wrapper always sends it. A dropped or different ``ContentType`` echo
  raises :class:`~sportsdataverse.errors.AssetFetchError`: every season has all three tables,
  so it is a broken answer, never "no data".
* **An unknown value is never an error upstream.** An unknown ``Season`` / ``Phase`` is
  answered with another one (``Season=1890`` serves the current season) and only the
  body's ``filters[].selected`` echo shows it; an unknown league answers a single
  ``["Keine Tabelle gefunden!"]`` row. One case echoes the value back: ``results`` with
  ``Phase=0`` echoes ``selected: "0"`` and serves the Playout Final's games, so a phase also
  has to be one of the ids the body's own ``Phase`` filter lists in ``entries``.
  :func:`_get` raises :class:`~sportsdataverse.errors.NoDataError` for all of these, so a
  typo can never return another season's or phase's rows labelled as the requested one. A
  table body with no filter echo at all, an echo that leaves out a requested filter, or a
  phase echo without its ``entries`` list, cannot be verified and raises
  :class:`~sportsdataverse.errors.AssetFetchError`.

A table body also announces its row count in ``pages`` (``0`` on ``standing``); a body whose
rows disagree with it raises :class:`~sportsdataverse.errors.AssetFetchError` rather than
passing a truncated table off as the whole one.

A ``filters[].selected`` of ``null`` is no echo at all: it raises ``AssetFetchError`` like a
dropped filter, never ``NoDataError``.

**A game body must be the game asked for.** ``/gameoverview`` answers ``{"gameId": ...}``; a
body whose ``gameId`` is not the requested ``searchQuery`` (a stub, a redirect or another game)
raises :class:`~sportsdataverse.errors.AssetFetchError`, so it can never be parsed into a
frame labelled as the requested game.

**The error shapes drift within a day** (measured 2026-10-12). An unknown table ``alias``
answered a 200 ``{"stub": true, "data": [], ...}`` at ~07:45 EDT and an empty HTTP 501 at
09:35 EDT; a menu alias (``team``) answers a 200 ``{"action": "Redirect", ...}``. All of them
raise ``AssetFetchError``, whichever shape arrives. A ``/table`` without ``searchQuery`` answered
an empty 501 (``AssetFetchError``) and later 400 ``{"error": "InvalidSearchQuery"}``
(``ValueError``); the wrappers always send it.

Everything else is the package error vocabulary from the shared getter: 404 (the
``gameoverview`` ``{"error": "GameNotFound"}``) -> ``NoDataError``, 400
(``MissingSearchQuery``) -> ``ValueError``, any other non-2xx, an empty or non-JSON 200 or a
dead connection -> ``AssetFetchError``. Every request, each retry included, goes through
:data:`sportsdataverse.hockey.euro_hockey_runtime.SESSION`, so every attempt waits at least one
second after the last one to the host (:func:`~sportsdataverse.hockey.euro_hockey_runtime._polite_wait`).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _eh

__all__: List[str] = []

#: The filters folded into ``filterBy`` / ``filterQuery``, in this order.
FILTER_KEYS: Tuple[str, ...] = ("Season", "Phase", "Date", "ContentType")


def _season_date_range(season: Any) -> str:
    """``01.07.<season-1>-30.06.<season>``: every date of a season keyed by its END year."""
    end = int(season)
    return f"01.07.{end - 1}-30.06.{end}"


def _build_query(params: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Tuple[str, str]]]:
    """The query to send and the ``(name, value)`` filters folded into it (see the module docstring)."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    league = clean.pop("League", None)
    if league is not None:
        clean["searchQuery"] = f"{clean.get('searchQuery', '1//')}{league}"
    if clean.get("alias") == "results" and "Season" in clean and "Date" not in clean:
        clean["Date"] = _season_date_range(clean["Season"])
    filters = [(k, str(clean.pop(k))) for k in FILTER_KEYS if k in clean]
    if filters:
        clean["filterBy"] = ",".join(k for k, _ in filters)
        clean["filterQuery"] = "/".join(v for _, v in filters)
    return clean, filters


def _check_table(url: str, raw: Any, filters: List[Tuple[str, str]]) -> None:
    """Raise unless the table body serves exactly what was asked (see the module docstring)."""
    if not filters:
        return
    echoed = raw.get("filters") if isinstance(raw, dict) else None
    if not isinstance(echoed, list):
        raise AssetFetchError(f"{_rt._where(url)}: the body carries no filter echo, so the answer cannot be verified")
    by_alias = {str(f.get("alias")): f for f in echoed if isinstance(f, dict)}
    for name, value in filters:
        if name == "Phase" and value == "":
            if not isinstance((by_alias.get(name) or {}).get("entries"), list):
                raise AssetFetchError(f"{_rt._where(url)}: the body carries no Phase entries to list")
            continue
        # ``selected: null`` echoes nothing, exactly like a dropped filter.
        selected = by_alias[name].get("selected") if name in by_alias else None
        got = None if selected is None else str(selected)
        if got != value:
            answered = f"answered {name}={got}" if got is not None else f"dropped the {name} filter"
            if name == "ContentType" or got is None:
                # A filter missing from the echo was ignored, not answered: unverifiable, like a body
                # with no echo at all. And every season has an All / Home / Away table, so a swapped
                # ContentType is SIHF breaking its own contract: a failed fetch, never an empty season.
                raise AssetFetchError(
                    f"{_rt._where(url)}: SIHF {answered} for {name}={value}, so the table cannot be verified"
                )
            raise NoDataError(
                f"{_rt._where(url)}: SIHF has no data for {name}={value} with the other filters given; "
                f"it {answered} instead"
            )
        if name == "Phase":
            entries = by_alias[name].get("entries")
            if not isinstance(entries, list):
                raise AssetFetchError(
                    f"{_rt._where(url)}: the Phase echo lists no entries, so Phase={value} cannot be verified"
                )
            if value not in {str(e.get("alias")) for e in entries if isinstance(e, dict)}:
                raise NoDataError(
                    f"{_rt._where(url)}: SIHF lists no Phase={value} for this table; it echoed the id back "
                    "but serves another phase's rows"
                )
    rows = raw.get("data")
    if isinstance(rows, list) and rows and all(isinstance(r, list) and len(r) == 1 for r in rows):
        raise NoDataError(f"{_rt._where(url)}: SIHF answered {rows[0][0]!r} (no table for that league and season)")


def _check_complete(url: str, raw: Any) -> None:
    """Raise unless a table body holds every row it announces.

    ``pages`` is the table's total ROW count, not a page count: measured 2026-10-12 it equals
    ``len(data)`` on full bodies (``results`` 414 / 414, ``goalkeeper`` 35 / 35 with its
    league-average row), and is ``0`` on ``standing``, which has no paging. So a positive
    ``pages`` that disagrees with the rows served means a truncated answer.
    """
    if not isinstance(raw, dict):
        return
    pages, rows = raw.get("pages"), raw.get("data")
    if isinstance(pages, int) and not isinstance(pages, bool) and pages > 0 and isinstance(rows, list):
        if len(rows) != pages:
            raise AssetFetchError(
                f"{_rt._where(url)}: the table announces {pages} rows but serves {len(rows)}; "
                "a partial table is not returned as a complete one"
            )


def _check_body(url: str, raw: Any, query: Dict[str, Any]) -> None:
    """Raise on a 200 that is not the requested answer: a stub, a redirect, or another game."""
    if isinstance(raw, dict) and raw.get("stub") is True:
        raise AssetFetchError(f"{_rt._where(url)}: SIHF answered a stub body (an unknown alias)")
    if isinstance(raw, dict) and raw.get("action") == "Redirect":
        target = (raw.get("redirectTo") or {}).get("alias") if isinstance(raw.get("redirectTo"), dict) else None
        raise AssetFetchError(f"{_rt._where(url)}: SIHF answered a redirect to alias {target!r}, not a table")
    if url.rstrip("/").endswith("/gameoverview"):
        got = raw.get("gameId") if isinstance(raw, dict) else None
        if str(got) != str(query.get("searchQuery")):
            raise AssetFetchError(
                f"{_rt._where(url)}: asked for game {query.get('searchQuery')}, SIHF answered gameId={got!r}"
            )


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a SIHF route as JSON: build the scoped query, fetch, then verify a table answer.

    Raises:
        NoDataError: the host answered 404, answered another season / phase than the one
            requested, does not list the requested phase, or has no table for the league.
        ValueError: the host answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx after retries, a connection failure after
            retries, a 2xx whose body is empty or not JSON, a table body without its
            filter echo, an echo that leaves out a requested filter (or a phase echo without
            its ``entries``), a ``ContentType`` echo that differs from the request, or a table
            body with
            fewer or more rows than its ``pages`` row count announces; a stub or redirect body; a
            ``gameoverview`` body whose ``gameId`` is not the requested game.
    """
    query, filters = _build_query(params)
    raw = _eh._get(url, query, **kwargs)
    _check_body(url, raw, query)
    _check_table(url, raw, filters)
    _check_complete(url, raw)
    return raw
