"""Dispatch entry for the football play processors (private, experimental).

``_process_game(league, espn_id, source=...)`` is the one call Game on Paper makes instead
of the ``(ProcessorClass, "espn_<league>_pbp")`` pair in its ``_PROCESSORS`` registry. It
resolves a source adapter, validates the adapted summary against the contract, runs the
unmodified processor on it, and returns the processed dict with provenance stamped in.

ESPN, NFL Shield, NFL CBS, NFL Yahoo, CFB Yahoo, CFB CBS and CFB NCAA are registered today. Every other source in
:data:`SOURCE_ORDER` is
a named slot that is skipped with ``"not implemented"`` in ``provenance["attempts"]`` until
its adapter lands (Stage 2 items 2+), so the fall-through path is exercised now and the
adapters plug in later without touching this module or GOP. A registered adapter hands over
to the next source by raising anything (:class:`SourceUnavailable` is the conventional
choice) or by returning something other than an :class:`AdaptedGame`.
"""

from __future__ import annotations

import importlib
import os
import time
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from sportsdataverse.football.sources.contract import KNOWN_LOSSY, LEAGUES, ContractReport, _validate_summary
from sportsdataverse.football.sources.idmap import _odds_override_from_row

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
        | provenance | dict | ``game["source"]``: requested vs served source, attempts, contract report, odds, lossy columns |
        | health | dict[str, str] | per-source outcome this call: ``"ok"``, ``"not implemented"``, or the error |
    """

    game: dict
    processor: Any
    plays_frame: pl.DataFrame
    provenance: dict
    health: dict[str, str]


Adapter = Callable[[str, int, SourceContext], AdaptedGame]

_ADAPTERS: dict[tuple[str, str], Adapter] = {}

#: Adapter modules imported on first use. Keeping the import lazy is what lets an adapter
#: live next to its own parser (and import this module for ``AdaptedGame`` / ``_register``)
#: without a cycle, and keeps the heavy Shield parser off the ESPN-only path.
_ADAPTER_MODULES: dict[tuple[str, str], str] = {
    ("nfl", "shield"): "sportsdataverse.nfl.shield_pbp.to_espn_summary",
    ("nfl", "cbs"): "sportsdataverse.nfl.cbs_pbp.to_espn_summary",
    ("cfb", "cbs"): "sportsdataverse.cfb.cbs_pbp.to_espn_summary",
    ("cfb", "yahoo"): "sportsdataverse.cfb.yahoo_pbp.to_espn_summary",
    ("nfl", "yahoo"): "sportsdataverse.nfl.yahoo_pbp.to_espn_summary",
    ("cfb", "ncaa"): "sportsdataverse.cfb.ncaa_pbp.to_espn_summary",
}


#: Why an adapter module failed to import, by ``(league, source)``. A broken adapter must fall
#: through rather than break dispatch, but it must not then be indistinguishable from an
#: unregistered slot: without this, an ``ImportError`` from a renamed symbol served every NFL
#: game from ESPN with ``attempts=[{"source": "shield", "error": "not implemented"}]`` and no
#: trace of the real cause anywhere.
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


#: Data API base URL for the request-time id-map lookup, and the offline id-map directory.
#: Both optional: an unset env var just means that step of the cascade is skipped.
IDMAP_BASE_URL_ENV = "SDV_DATA_API_URL"
IDMAP_DIR_ENV = "SDV_IDMAP_DIR"


def _resolve_idmap_row(league: str, espn_id: int) -> tuple[dict | None, str]:
    """Find the game's id-map row when the caller did not pass one.

    Game on Paper calls ``_process_game(league, espn_id, source=...)`` with the ESPN event id
    and nothing else, so an alternate source would otherwise reach its adapter with
    ``idmap_row=None`` and fail on an unmapped id every time. The cascade is resolved **once
    per call**, here, so every adapter gets the same row:

    1. the Data API route (:func:`...idmap._fetch_idmap_row`), when ``SDV_DATA_API_URL`` is set;
    2. an offline id-map parquet pair (:func:`...idmap._lookup`), when ``SDV_IDMAP_DIR`` is set;
    3. nothing -- the adapter is then free to resolve what it can from a schedule, and must
       raise :class:`SourceUnavailable` rather than invent an id.

    Neither lookup may raise: a 404, a timeout or a missing asset is a warning and a miss, and
    dispatch falls through to ESPN. Returns ``(row, how)``; ``how`` is stamped in provenance.
    """
    base_url = os.environ.get(IDMAP_BASE_URL_ENV)
    if base_url:
        try:
            from sportsdataverse.football.sources.idmap import _fetch_idmap_row

            row = _fetch_idmap_row(league, espn_id, base_url=base_url)
            if row:
                return row, "data_api"
        except Exception as exc:  # noqa: BLE001 -- the route is optional and may not exist yet
            warnings.warn(
                f"idmap: Data API lookup for {league} {espn_id} failed ({type(exc).__name__}: {exc}); falling back",
                RuntimeWarning,
                stacklevel=2,
            )
    directory = os.environ.get(IDMAP_DIR_ENV)
    if directory:
        try:
            from sportsdataverse.football.sources.idmap import _load_idmap, _lookup

            games, teams = _load_idmap(directory)
            row = _lookup(games, espn_id, teams)
            if row:
                return row, "offline_parquet"
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"idmap: offline lookup for {league} {espn_id} in {directory} failed "
                f"({type(exc).__name__}: {exc}); falling back",
                RuntimeWarning,
                stacklevel=2,
            )
    return None, "unresolved"


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

    **Decision (user, 2026-09-17): ESPN is always the terminal fallback**, even when an alternate
    was explicitly requested -- it fails fast in an ESPN outage and costs nothing otherwise.
    Shadow-mode comparisons learn that the requested source failed from
    ``provenance["requested"] != provenance["served"]`` plus the per-source ``attempts`` errors.
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
        idmap_row: the game's pre-kickoff id-map row (``idmap._fetch_idmap_row`` from the Data API,
            or ``idmap._lookup`` offline); never built at request time. Its stored closing line becomes
            ``odds_override`` when none was passed. **Optional**: left None (Game on Paper's call
            shape) it is resolved once by :func:`_resolve_idmap_row` for every non-ESPN source.

    Returns:
        :class:`ProcessedGame`. ``game["source"]`` carries the provenance dict:

        | key | type | description |
        |---|---|---|
        | requested | str | the ``source`` argument |
        | served | str | the source that actually produced the game |
        | fallback | bool | True when ``served != requested`` |
        | attempts | list[dict] | every source tried, in order: ``{source, ok, error, seconds}`` |
        | playByPlaySource | str | ``header.competitions[0].playByPlaySource`` of the adapted summary |
        | native_ids | dict | the producing adapter's native ids |
        | idmap | dict | how the id-map row was obtained: ``caller`` / ``data_api`` / ``offline_parquet`` / ``unresolved`` |
        | contract | dict | ``ContractReport.summary()`` for the adapted summary |
        | odds | dict | ``{source, default, from_idmap}`` -- the processor's ``odds_source`` and whether the 2.5 / 55.5 default was used |
        | lossy_columns | list[str] | :data:`KNOWN_LOSSY` for ``(league, source)`` |
        | notes | list[str] | adapter notes |

    Raises:
        AllSourcesFailed: no source produced a game (``.attempts`` has the per-source errors).
        ValueError: unknown league or source.
    """
    order = _fallthrough_order(league, source, fallthrough)
    payloads = payloads or {}
    attempts: list[Attempt] = []
    # GOP passes the ESPN event id and nothing else, so the id map is resolved here rather than
    # per adapter -- but only once, and only when a non-ESPN source is actually *reached*. The
    # default order puts ESPN first and it serves almost always, so resolving up front billed
    # every ESPN page view for a Data API round trip (and a RuntimeWarning on a hiccup) for a
    # row nothing would read.
    idmap_source = "caller" if idmap_row is not None else "not needed"
    idmap_resolved = idmap_row is not None
    for src in order:
        if src != "espn" and not idmap_resolved:
            idmap_row, idmap_source = _resolve_idmap_row(league, espn_id)
            idmap_resolved = True
        t0 = time.perf_counter()
        adapter = _adapter_for(league, src)
        if adapter is None:
            attempts.append(Attempt(src, False, _ADAPTER_IMPORT_ERRORS.get((league, src), "not implemented")))
            continue
        ctx = SourceContext(
            idmap_row=idmap_row,
            payload=payloads.get(src),
            participants=participants,
            odds_override=odds_override,
        )
        try:
            adapted = adapter(league, espn_id, ctx)
            if not isinstance(adapted, AdaptedGame):
                raise SourceUnavailable(f"adapter returned {type(adapted).__name__}, expected AdaptedGame")
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
        if adapted.odds_override is None:
            # odds on failover: the stored closing line (nightly job, beside the id map) first,
            # then the source's own odds (adapter-supplied), else the processor's 2.5 / 55.5 default
            adapted.odds_override = _odds_override_from_row(idmap_row)
        try:
            proc, game = _run_processor(league, espn_id, adapted)
        except Exception as exc:
            attempts.append(Attempt(src, False, f"processor: {type(exc).__name__}: {exc}", time.perf_counter() - t0))
            continue
        attempts.append(Attempt(src, True, None, time.perf_counter() - t0))
        odds_source = getattr(proc, "odds_source", None)
        provenance = {
            "requested": source,
            "served": src,
            "fallback": src != source,
            "attempts": [a.__dict__ for a in attempts],
            "playByPlaySource": (adapted.summary.get("header", {}).get("competitions") or [{}])[0].get(
                "playByPlaySource"
            ),
            "native_ids": adapted.native_ids,
            "idmap": {"source": idmap_source, "resolved": idmap_row is not None},
            "contract": report.summary(),
            "odds": {
                # "injected" = stored closing line (or adapter odds); "default" = the 2.5 / 55.5 fallback
                "source": odds_source,
                "default": odds_source == "default",
                "from_idmap": adapted.odds_override is not None
                and idmap_row is not None
                and adapted.odds_override == _odds_override_from_row(idmap_row),
            },
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
