"""Dispatch entry for the basketball game page (private, experimental).

``_process_game(league, espn_id, source=...)`` is the call Game on Paper's basketball route
makes instead of fetching ``espn_<league>_pbp(raw=True)`` itself. It resolves a source
adapter, validates the adapted summary against :mod:`~sportsdataverse.basketball.sources.contract`,
runs the unmodified ``helper_<league>_pbp`` parser and
:func:`~sportsdataverse.nba.nba_game_page.game_page_frames` on it, and returns the parsed
dict and the game-page frames with provenance stamped in. Mirrors
:mod:`sportsdataverse.football.sources.dispatch`.

Game on Paper's ``?source=`` allow-list IS :data:`SOURCE_ORDER`: a source listed there is
selectable, and an alternate is listed only once every game of a real captured corpus passes its
cross-source parity gates (BB-D15, :mod:`~sportsdataverse.basketball.sources.parity`). The
cdn.nba.com / cdn.wnba.com live adapter (BB-D13, ``source="live"``,
:mod:`sportsdataverse.nba.live_pbp.to_espn_summary`) passed them on 27 games and follows ESPN for
NBA and WNBA, so it is selectable there and is also the failover when ESPN cannot serve a game
(``source="espn"`` falls through to it). MBB and WBB list ESPN alone.
"""

from __future__ import annotations

import importlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

# ponytail: sport-neutral, shared so a caller catches one AllSourcesFailed for every league
from sportsdataverse.basketball.sources.contract import KNOWN_LOSSY, LEAGUES, ContractReport, _validate_summary
from sportsdataverse.football.sources.dispatch import AllSourcesFailed, Attempt, SourceUnavailable
from sportsdataverse.nba.nba_game_page import game_page_frames, league_pbp_functions

#: Failover order per league: ESPN first and the terminal fallback. An alternate joins only once
#: BB-D15's parity gates pass (:data:`~sportsdataverse.basketball.sources.parity.GATES`); listing one
#: here makes it selectable in Game on Paper. "live" passed them on the 27-game corpus (2026-11-03).
SOURCE_ORDER: dict[str, tuple[str, ...]] = {
    **dict.fromkeys(LEAGUES, ("espn",)),
    "nba": ("espn", "live"),
    "wnba": ("espn", "live"),
}


@dataclass
class SourceContext:
    """What an adapter receives besides ``(league, espn_id)``.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | payload | Any | the source's native payload for offline runs (ESPN: the ``raw=True`` dict); None = fetch |
    """

    payload: Any = None


@dataclass
class AdaptedGame:
    """What an adapter returns: the ESPN-shaped summary plus provenance.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | summary | dict | the ``espn_<league>_pbp(raw=True)``-shaped dict ``helper_<league>_pbp`` parses |
        | native_ids | dict | the source's own game/team ids the adapter used |
        | notes | list[str] | adapter-side degradations worth surfacing |
    """

    summary: dict
    native_ids: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass
class ProcessedGame:
    """Return value of :func:`_process_game`.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | final | dict | ``helper_<league>_pbp``'s parsed dict (what Game on Paper's ``build_payload`` reads), plus a ``source`` provenance key |
        | frames | dict | :func:`~sportsdataverse.nba.nba_game_page.game_page_frames` on ``final`` |
        | provenance | dict | ``final["source"]``: requested vs served source, attempts, contract report, lossy columns |
        | health | dict[str, str] | per-source outcome this call: ``"ok"``, ``"not implemented"``, or the error |
    """

    final: dict
    frames: dict
    provenance: dict
    health: dict[str, str]


Adapter = Callable[[str, int, SourceContext], AdaptedGame]

_ADAPTERS: dict[tuple[str, str], Adapter] = {}

#: Adapter modules imported on first use, as in football's dispatch: the adapter lives next to its
#: own parser and imports this module for ``AdaptedGame`` / ``_register`` without a cycle.
_ADAPTER_MODULES: dict[tuple[str, str], str] = {
    ("nba", "live"): "sportsdataverse.nba.live_pbp.to_espn_summary",
    ("wnba", "live"): "sportsdataverse.nba.live_pbp.to_espn_summary",
}

#: Why an adapter module failed to import, so a broken adapter falls through with its real cause
#: in ``attempts`` instead of reading as ``"not implemented"``.
_ADAPTER_IMPORT_ERRORS: dict[tuple[str, str], str] = {}


def _adapter_for(league: str, source: str) -> Adapter | None:
    """The registered adapter for ``(league, source)``, importing its module on first use."""
    key = (league, source)
    if key not in _ADAPTERS and key in _ADAPTER_MODULES:
        try:
            importlib.import_module(_ADAPTER_MODULES[key])
        except Exception as exc:  # noqa: BLE001 -- a broken adapter falls through, but says so
            _ADAPTER_IMPORT_ERRORS[key] = f"import failed: {type(exc).__name__}: {exc}"
            return None
    return _ADAPTERS.get(key)


def _register(league: str, source: str) -> Callable[[Adapter], Adapter]:
    """Register an adapter for ``(league, source)``; adapters are plain callables."""

    def deco(fn: Adapter) -> Adapter:
        _ADAPTERS[(league, source)] = fn
        return fn

    return deco


def _espn_adapter(league: str, espn_id: int, ctx: SourceContext) -> AdaptedGame:
    """ESPN is its own adapter: the ``raw=True`` dict passes through, or is fetched (one call)."""
    summary = ctx.payload
    if summary is None:
        fetch, _ = league_pbp_functions(league)
        try:
            summary = fetch(espn_id, raw=True)
        except Exception as exc:  # network / JSON / NoDataError -> the next source
            raise SourceUnavailable(f"espn summary fetch failed: {type(exc).__name__}: {exc}") from exc
    return AdaptedGame(summary=summary, native_ids={"espn_event_id": str(espn_id)})


for _league in LEAGUES:
    _register(_league, "espn")(_espn_adapter)


def _fallthrough_order(league: str, source: str, fallthrough: bool = True) -> tuple[str, ...]:
    """Sources to try, in order: the requested one, the rest of :data:`SOURCE_ORDER`, ESPN last."""
    order = SOURCE_ORDER.get(league)
    if order is None:
        raise ValueError(f"league must be one of {LEAGUES}, got {league!r}")
    if source not in order:
        raise ValueError(f"unknown source {source!r} for {league}; known: {order}")
    if not fallthrough:
        return (source,)
    rest = tuple(s for s in order if s not in (source, "espn"))
    return (source, *rest) + (("espn",) if source != "espn" else ())


def _process_game(
    league: str,
    espn_id: int,
    *,
    source: str = "espn",
    fallthrough: bool = True,
    payloads: Mapping[str, Any] | None = None,
) -> ProcessedGame:
    """Build one game's page through the requested source, falling through the order on failure.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        espn_id: the ESPN event id, the only game id Game on Paper knows.
        source: source to try first (a member of ``SOURCE_ORDER[league]``).
        fallthrough: when True, a failing source (fetch error, contract failure, parser
            exception) hands over to the next one; ESPN is the terminal fallback.
        payloads: native payloads by source for offline runs, e.g. ``{"espn": raw_summary}``.
            A source with no entry fetches.

    Returns:
        :class:`ProcessedGame`. ``final["source"]`` carries the provenance dict:

        | key | type | description |
        |---|---|---|
        | requested | str | the ``source`` argument |
        | served | str | the source that actually produced the game |
        | fallback | bool | True when ``served != requested`` |
        | attempts | list[dict] | every source tried, in order: ``{source, ok, error, seconds}`` |
        | playByPlaySource | str | ``header.competitions[0].playByPlaySource`` of the adapted summary |
        | native_ids | dict | the producing adapter's native ids |
        | contract | dict | ``ContractReport.summary()``, including the contract ``version`` |
        | lossy_columns | list[str] | :data:`~sportsdataverse.basketball.sources.contract.KNOWN_LOSSY` for ``(league, source)`` |
        | notes | list[str] | adapter notes |

    Raises:
        AllSourcesFailed: no source produced a game (``.attempts`` has the per-source errors).
        ValueError: unknown league or source.

    Example:
        Quick start::

            from sportsdataverse.basketball.sources.dispatch import _process_game
            game = _process_game("wnba", 401726992)
            game.frames["pbp"].select("text", "home_win_prob").head()
            game.provenance["served"], game.provenance["contract"]["ok"]
    """
    order = _fallthrough_order(league, source, fallthrough)
    payloads = payloads or {}
    attempts: list[Attempt] = []
    for src in order:
        t0 = time.perf_counter()
        adapter = _adapter_for(league, src)
        if adapter is None:
            attempts.append(Attempt(src, False, _ADAPTER_IMPORT_ERRORS.get((league, src), "not implemented")))
            continue
        try:
            adapted, report, final, frames = _run_source(league, espn_id, adapter, payloads.get(src))
        except _SourceFailed as exc:
            attempts.append(Attempt(src, False, str(exc), time.perf_counter() - t0))
            continue
        attempts.append(Attempt(src, True, None, time.perf_counter() - t0))
        provenance = {
            "requested": source,
            "served": src,
            "fallback": src != source,
            "attempts": [a.__dict__ for a in attempts],
            "playByPlaySource": _pbp_source(adapted.summary),
            "native_ids": adapted.native_ids,
            "contract": report.summary(),
            "lossy_columns": list(KNOWN_LOSSY.get((league, src), ())),
            "notes": adapted.notes,
        }
        final["source"] = provenance
        return ProcessedGame(
            final=final,
            frames=frames,
            provenance=provenance,
            health={a.source: ("ok" if a.ok else a.error or "failed") for a in attempts},
        )
    raise AllSourcesFailed(league, espn_id, attempts)


class _SourceFailed(Exception):
    """One source's leg of :func:`_process_game` failed; the message is the ``Attempt.error``."""


def _run_source(
    league: str, espn_id: int, adapter: Adapter, payload: Any
) -> tuple[AdaptedGame, ContractReport, dict, dict]:
    """One source's leg of :func:`_process_game`: adapt, check the contract and header id, parse, frame.

    Shared with :mod:`~sportsdataverse.basketball.sources.parity`, which runs a source whether or not it
    is in :data:`SOURCE_ORDER` through exactly the path a served game takes.

    Raises:
        _SourceFailed: the adapter, the contract, the header id or the parser failed.
    """
    try:
        adapted = adapter(league, espn_id, SourceContext(payload=payload))
        if not isinstance(adapted, AdaptedGame):
            raise SourceUnavailable(f"adapter returned {type(adapted).__name__}, expected AdaptedGame")
    except Exception as exc:  # noqa: BLE001 -- any adapter failure hands over to the next source
        raise _SourceFailed(f"{type(exc).__name__}: {exc}") from exc
    report = _validate_summary(adapted.summary, league)
    if not report.ok:
        raise _SourceFailed(f"contract: missing={report.missing} invalid={report.invalid}")
    header_id = adapted.summary["header"].get("id")
    if str(header_id) != str(espn_id):
        # an injected payload for another event would be served under this game's id
        raise _SourceFailed(f"header.id {header_id!r} is not the requested game {espn_id}")
    _, parse = league_pbp_functions(league)
    try:
        final = parse(espn_id, adapted.summary)
        frames = game_page_frames(final, league=league)
    except Exception as exc:  # noqa: BLE001 -- a parser failure on one source is not the game's end
        raise _SourceFailed(f"parser: {type(exc).__name__}: {exc}") from exc
    return adapted, report, final, frames


def _pbp_source(summary: dict) -> Any:
    competitions = (summary.get("header") or {}).get("competitions") or [{}]
    return competitions[0].get("playByPlaySource")
