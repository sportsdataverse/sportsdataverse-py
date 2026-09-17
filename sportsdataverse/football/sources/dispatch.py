"""Dispatch entry for the football play processors (private, experimental).

``_process_game(league, espn_id, source=...)`` is the one call Game on Paper makes instead
of the ``(ProcessorClass, "espn_<league>_pbp")`` pair in its ``_PROCESSORS`` registry. It
resolves a source adapter, validates the adapted summary against the contract, runs the
unmodified processor on it, and returns the processed dict with provenance stamped in.

Only the ESPN adapter is registered today. Every other source in :data:`SOURCE_ORDER` is
a named slot that raises :class:`SourceUnavailable` until its adapter lands (Stage 2
items 2+), so the fall-through path is exercised now and the adapters plug in later
without touching this module or GOP.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from sportsdataverse.football.sources.contract import KNOWN_LOSSY, LEAGUES, ContractReport, _validate_summary

#: Canonical failover order per league (scorecards 2026-09-16). ESPN first while it is the
#: primary; the remaining order is the recommended alternate ranking.
SOURCE_ORDER: dict[str, tuple[str, ...]] = {
    "nfl": ("espn", "shield", "cbs", "yahoo", "fox"),
    "cfb": ("espn", "cbs", "yahoo", "ncaa", "fox"),
}


class SourceUnavailable(Exception):
    """An adapter could not produce a summary (fetch failed, id unmapped, not implemented)."""


class AllSourcesFailed(Exception):
    """Every source in the fall-through order failed; ``attempts`` says why."""

    def __init__(self, league: str, espn_id: int, attempts: list[Attempt]):
        self.league, self.espn_id, self.attempts = league, espn_id, attempts
        detail = "; ".join(f"{a.source}: {a.error}" for a in attempts)
        super().__init__(f"{league} {espn_id}: no source produced a game ({detail})")


@dataclass
class SourceContext:
    """What an adapter receives besides ``(league, espn_id)``.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | idmap_row | dict or None | the game's id-map row (``idmap._lookup``): native game + team ids |
        | payload | Any | the source's native payload, injected for offline runs (ESPN: the summary dict); None = fetch |
        | participants | polars.DataFrame or None | pre-shaped ``participants=`` frame to hand the processor |
        | odds_override | dict or None | ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` for sources without odds |
    """

    idmap_row: dict | None = None
    payload: Any = None
    participants: pl.DataFrame | None = None
    odds_override: dict | None = None


@dataclass
class AdaptedGame:
    """What an adapter returns: the ESPN-shaped summary plus the processor's injection kwargs.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | summary | dict | ESPN-summary-shaped dict for ``espn_{nfl,cfb}_pbp(summary=)`` |
        | participants | polars.DataFrame or None | optional ``participants=`` frame (ESPN athlete-id space) |
        | odds_override | dict or None | optional closing line for the processor's odds cascade |
        | native_ids | dict | the source's own game/team ids the adapter used, for provenance |
        | notes | list[str] | adapter-side degradations worth surfacing (e.g. "open drive synthesized") |
    """

    summary: dict
    participants: pl.DataFrame | None = None
    odds_override: dict | None = None
    native_ids: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass
class Attempt:
    """One source tried during dispatch."""

    source: str
    ok: bool
    error: str | None = None
    seconds: float = 0.0


@dataclass
class ProcessedGame:
    """Return value of :func:`_process_game`.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | game | dict | the processor's ``run_processing_pipeline()`` dict, plus a ``source`` provenance key |
        | processor | NFLPlayProcess or CFBPlayProcess | the instance (GOP needs ``plays_frame`` / ``create_box_score`` for spans) |
        | plays_frame | polars.DataFrame | ``processor.plays_frame`` |
        | provenance | dict | ``game["source"]``: requested/actual source, attempts, contract report, lossy columns |
        | health | dict[str, str] | per-source outcome this call: ``"ok"``, ``"not implemented"``, or the error |
    """

    game: dict
    processor: Any
    plays_frame: pl.DataFrame
    provenance: dict
    health: dict[str, str]


Adapter = Callable[[str, int, SourceContext], AdaptedGame]

_ADAPTERS: dict[tuple[str, str], Adapter] = {}


def _register(league: str, source: str) -> Callable[[Adapter], Adapter]:
    """Register an adapter for ``(league, source)``; adapters are plain callables."""

    def deco(fn: Adapter) -> Adapter:
        _ADAPTERS[(league, source)] = fn
        return fn

    return deco


def _processor_class(league: str):
    # local imports: nfl_pbp / cfb_pbp are heavy (models load at import)
    if league == "nfl":
        from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

        return NFLPlayProcess, "espn_nfl_pbp"
    if league == "cfb":
        from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

        return CFBPlayProcess, "espn_cfb_pbp"
    raise ValueError(f"league must be one of {LEAGUES}, got {league!r}")


def _espn_adapter(league: str, espn_id: int, ctx: SourceContext) -> AdaptedGame:
    """ESPN is its own adapter: the summary passes through (or is fetched with ``raw=True``, one call)."""
    if ctx.payload is not None:
        summary = ctx.payload
    else:
        cls, fetch = _processor_class(league)
        try:
            summary = getattr(cls(gameId=espn_id, raw=True), fetch)()
        except Exception as exc:  # network / JSON / NoDataError -> the next source
            raise SourceUnavailable(f"espn summary fetch failed: {type(exc).__name__}: {exc}") from exc
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=ctx.odds_override,
        native_ids={"espn_event_id": str(espn_id)},
    )


for _league in LEAGUES:
    _register(_league, "espn")(_espn_adapter)


def _fallthrough_order(league: str, source: str, fallthrough: bool = True) -> tuple[str, ...]:
    """Sources to try, in order: the requested one, then the rest of :data:`SOURCE_ORDER`.

    ESPN is always the terminal fallback when an alternate was requested (it fails fast in
    an ESPN outage and costs nothing otherwise).
    """
    order = SOURCE_ORDER.get(league)
    if order is None:
        raise ValueError(f"league must be one of {LEAGUES}, got {league!r}")
    if source not in order:
        raise ValueError(f"unknown source {source!r} for {league}; known: {order}")
    if not fallthrough:
        return (source,)
    rest = tuple(s for s in order if s not in (source, "espn"))
    return (source, *rest) + (("espn",) if source != "espn" else ())


def _run_processor(league: str, espn_id: int, adapted: AdaptedGame):
    cls, fetch = _processor_class(league)
    kwargs: dict[str, Any] = {"gameId": int(espn_id)}
    if adapted.participants is not None:
        kwargs["participants"] = adapted.participants
    else:
        kwargs["join_participants"] = False
    if adapted.odds_override is not None:
        kwargs["odds_override"] = adapted.odds_override
    proc = cls(**kwargs)
    getattr(proc, fetch)(summary=adapted.summary)  # summary= => _offline: no participants/roster/odds calls
    game = proc.run_processing_pipeline()
    return proc, game


def _process_game(
    league: str,
    espn_id: int,
    *,
    source: str = "espn",
    fallthrough: bool = True,
    payloads: Mapping[str, Any] | None = None,
    participants: pl.DataFrame | None = None,
    odds_override: dict | None = None,
    idmap_row: dict | None = None,
) -> ProcessedGame:
    """Process one game through the requested source, falling through the ordered list on failure.

    Args:
        league: ``"nfl"`` or ``"cfb"``.
        espn_id: the ESPN event id -- the only game id GOP knows; alternate ids come from ``idmap_row``.
        source: source to try first (a key of :data:`SOURCE_ORDER`).
        fallthrough: when True, a failing source (fetch error, unmapped id, contract failure,
            processor exception) hands over to the next one; ESPN is the terminal fallback.
        payloads: native payloads by source for offline runs, e.g. ``{"espn": summary_dict}``.
            A source with no entry fetches.
        participants: optional pre-shaped ``participants=`` frame (ESPN athlete-id space).
        odds_override: optional closing line for sources that carry no odds.
        idmap_row: the game's pre-kickoff id-map row (``idmap._lookup``); never built at request time.

    Returns:
        :class:`ProcessedGame`. ``game["source"]`` carries the provenance dict:

        | key | type | description |
        |---|---|---|
        | requested | str | the ``source`` argument |
        | source | str | the source that produced the game |
        | fallback | bool | True when ``source != requested`` |
        | attempts | list[dict] | every source tried: ``{source, ok, error, seconds}`` |
        | playByPlaySource | str | ``header.competitions[0].playByPlaySource`` of the adapted summary |
        | native_ids | dict | the producing adapter's native ids |
        | contract | dict | ``ContractReport.summary()`` for the adapted summary |
        | lossy_columns | list[str] | :data:`KNOWN_LOSSY` for ``(league, source)`` |
        | notes | list[str] | adapter notes |

    Raises:
        AllSourcesFailed: no source produced a game (``.attempts`` has the per-source errors).
        ValueError: unknown league or source.
    """
    order = _fallthrough_order(league, source, fallthrough)
    payloads = payloads or {}
    attempts: list[Attempt] = []
    for src in order:
        t0 = time.perf_counter()
        adapter = _ADAPTERS.get((league, src))
        if adapter is None:
            attempts.append(Attempt(src, False, "not implemented"))
            continue
        ctx = SourceContext(
            idmap_row=idmap_row,
            payload=payloads.get(src),
            participants=participants,
            odds_override=odds_override,
        )
        try:
            adapted = adapter(league, espn_id, ctx)
        except Exception as exc:
            attempts.append(Attempt(src, False, f"{type(exc).__name__}: {exc}", time.perf_counter() - t0))
            continue
        report: ContractReport = _validate_summary(adapted.summary, league)
        if not report.ok:
            attempts.append(
                Attempt(
                    src, False, f"contract: missing={report.missing} invalid={report.invalid}", time.perf_counter() - t0
                )
            )
            continue
        try:
            proc, game = _run_processor(league, espn_id, adapted)
        except Exception as exc:
            attempts.append(Attempt(src, False, f"processor: {type(exc).__name__}: {exc}", time.perf_counter() - t0))
            continue
        attempts.append(Attempt(src, True, None, time.perf_counter() - t0))
        provenance = {
            "requested": source,
            "source": src,
            "fallback": src != source,
            "attempts": [a.__dict__ for a in attempts],
            "playByPlaySource": (adapted.summary.get("header", {}).get("competitions") or [{}])[0].get(
                "playByPlaySource"
            ),
            "native_ids": adapted.native_ids,
            "contract": report.summary(),
            "lossy_columns": list(KNOWN_LOSSY.get((league, src), ())),
            "notes": adapted.notes,
        }
        game["source"] = provenance
        return ProcessedGame(
            game=game,
            processor=proc,
            plays_frame=proc.plays_frame,
            provenance=provenance,
            health={a.source: ("ok" if a.ok else a.error or "failed") for a in attempts},
        )
    raise AllSourcesFailed(league, espn_id, attempts)
