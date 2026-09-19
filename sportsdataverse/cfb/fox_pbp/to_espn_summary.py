"""Fox Bifrost college football -> an ESPN-summary-shaped dict ``CFBPlayProcess`` consumes unchanged.

Everything that decides a number lives in :mod:`sportsdataverse.football.fox_common`, which
both Fox adapters share; this module is the CFB half: ESPN's college play-type vocabulary, the
id resolver, and the dispatch registration for ``source="fox"``.

This is a **rewrite**, not a wrapper around the shipped :mod:`sportsdataverse.cfb.cfb_pbp_fox`.
That module reads Fox's absolute field coordinates as yards-to-goal and takes possession from
``play.image.altText`` (the team *credited* with the play), which mirrors the field on every
away-offense play: 50.0% / 53.7% of spots right and ``EP_start`` r 0.16 / 0.53 on the two 2026
games measured in ``2026-09-16-cfb-alt-sources/B_fox.md`` §7. Nothing here imports it.

Fox is the **last** CFB source in ``...sources.dispatch.SOURCE_ORDER``: its play-by-play covers
**2022 onward** (2021 early-season only) and **no FCS-hosted game in any era**, and its text is
a Stats-Perform grammar, so receiver / kickoff / punt names degrade
(``...contract.KNOWN_LOSSY[("cfb", "fox")]``). What it buys is a feed independent of ESPN, plus
a closing line on the game payload.

Everything here is ``_``-prefixed and nothing is re-exported from ``sportsdataverse.cfb``, so
no codegen or reference-doc regeneration is involved.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

import polars as pl

from sportsdataverse.cfb.yahoo_pbp.to_espn_summary import ESPN_PLAY_TYPES as _YAHOO_CFB_PLAY_TYPES
from sportsdataverse.football.fox_common import (
    _fox_event_data,
    _fox_event_odds,
    _fox_odds_override,
    _fox_to_espn_summary,
    _fox_team_id,
    _has_pbp,
)

#: ESPN CFB ``type.id`` -> ``(type.text, type.abbreviation)``. The table the Yahoo CFB adapter
#: enumerated from real ESPN college summaries, plus the four special-teams types Fox can emit
#: that a Yahoo payload never distinguishes (a blocked punt, its touchdown, a muffed punt and
#: the officials' timeout). Shared rather than re-derived so two CFB adapters cannot drift.
ESPN_PLAY_TYPES: Dict[str, Tuple[str, Optional[str]]] = {
    **_YAHOO_CFB_PLAY_TYPES,
    "17": ("Blocked Punt", "BP"),
    "30": ("Muffed Punt Recovery (Opponent)", None),
    "37": ("Blocked Punt Touchdown", "TD"),
    "74": ("Official Timeout", "Off TO"),
    "75": ("Two-minute warning", "2Min Warn"),
    "79": ("End of Regulation", "ER"),
}

#: ``cfb_crosswalk`` frames already read, keyed by the season tuple. Successful reads only:
#: caching a miss would make one transient release-asset failure permanent for the life of a
#: Game on Paper worker, and this runs on its request path.
_CROSSWALK_CACHE: Dict[Tuple[int, ...], pl.DataFrame] = {}


def _crosswalk_fox_id(espn_id: Any, seasons: Tuple[int, ...]) -> Optional[str]:
    """``fox_game_id`` for an ESPN event id from the published ``cfb_crosswalk`` asset, or None.

    Fox's CFB ids are opaque monotone integers with no computable form, so unlike Yahoo's there
    is no formula to fall back on: it is the stored map or nothing. Never raises -- an
    unreachable release asset is a miss, and the game falls through to the next source.

    Note that this asset is itself **built on ESPN's schedule**, so it does not survive the
    outage it exists for; the durable leg is the id map's own ``fox_event_id`` column, which
    the nightly job fills from this same crosswalk before kickoff.
    """
    if seasons not in _CROSSWALK_CACHE:
        try:
            from sportsdataverse.cfb import load_cfb_schedule_crosswalk

            frame = load_cfb_schedule_crosswalk(list(seasons))
        except Exception:  # noqa: BLE001 -- an unreachable asset is a miss, never a raise
            return None
        if (
            not isinstance(frame, pl.DataFrame)
            or frame.is_empty()
            or not {"espn_game_id", "fox_game_id"} <= set(frame.columns)
        ):
            return None
        _CROSSWALK_CACHE[seasons] = frame
    hit = _CROSSWALK_CACHE[seasons].filter(pl.col("espn_game_id").cast(pl.Utf8) == str(espn_id))
    if hit.is_empty():
        return None
    return hit.row(0, named=True).get("fox_game_id") or None


def _current_cfb_seasons() -> Tuple[int, ...]:
    """``(previous, current)`` CFB seasons -- the window the crosswalk asset is read over."""
    from sportsdataverse.cfb.cfb_schedule import most_recent_cfb_season

    season = int(most_recent_cfb_season())
    return (season - 1, season)


def _resolve_fox_event_id(espn_id: Any, idmap_row: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], str]:
    """``(fox_event_id, provenance)`` for one ESPN event id; ``(None, ...)`` means hand over."""
    stored = (idmap_row or {}).get("fox_event_id")
    if stored:
        return str(stored), "idmap"
    seasons = (int(idmap_row["season"]),) if (idmap_row or {}).get("season") else _current_cfb_seasons()
    from_crosswalk = _crosswalk_fox_id(espn_id, seasons)
    if from_crosswalk:
        return str(from_crosswalk), "cfb_schedule_crosswalk"
    return None, "unresolved"


def _fox_cfb_to_espn_summary(
    fox: Mapping[str, Any], idmap_row: Mapping[str, Any], *, odds: Optional[Mapping[str, Any]] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one Fox CFB game onto an ESPN-summary-shaped dict (see ``fox_common``)."""
    return _fox_to_espn_summary(
        fox,
        idmap_row,
        league="cfb",
        play_types=ESPN_PLAY_TYPES,
        espn_uid_league="23",
        odds=odds,
    )


def _fox_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="fox"`` (CFB). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (:class:`...dispatch.SourceUnavailable`) when the Fox event
    id cannot be resolved without inventing one, when the id map states no ESPN team ids, when
    the fetch fails, and -- the case three of the five CFB sources answer with **HTTP 200** --
    when the payload carries no ``pbp`` at all: **every FCS-hosted game in every era**, and
    every game before Fox's 2022 play floor.
    """
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row

    row = dict(ctx.idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))
    payload, odds_payload = ctx.payload, None
    if isinstance(payload, Mapping) and "data" in payload and "header" not in payload:
        payload, odds_payload = payload.get("data"), payload.get("odds")
    fox_event_id, resolved_by = None, "payload"
    if payload is None:
        fox_event_id, resolved_by = _resolve_fox_event_id(espn_id, ctx.idmap_row)
        if not fox_event_id:
            raise SourceUnavailable(
                f"cfb {espn_id}: no fox_event_id in the id map or the crosswalk (Fox ids are not computable)"
            )
        try:
            payload = _fox_event_data("cfb", fox_event_id)
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"fox event data fetch failed: {type(exc).__name__}: {exc}") from exc
    if not isinstance(payload, Mapping) or not payload.get("header"):
        raise SourceUnavailable(f"cfb {espn_id}: fox returned no event payload (request failed or bad id)")
    if not _has_pbp(payload):
        raise SourceUnavailable(
            f"cfb {espn_id}: fox event {fox_event_id or payload.get('header', {}).get('id')} carries no "
            "play-by-play (HTTP 200 with no pbp key: FCS-hosted, or before Fox's 2022 play floor)"
        )
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id")):
        raise SourceUnavailable(f"cfb {espn_id}: id-map row carries no ESPN team ids")

    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row)
    if odds is None:
        odds = _fox_odds(payload, odds_payload, fox_event_id)
    summary, notes = _fox_cfb_to_espn_summary(payload, row, odds=odds)
    served = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    if not any(d.get("plays") for d in served):
        raise SourceUnavailable(f"cfb {espn_id}: fox drive chart carries no plays yet")
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=odds,
        native_ids={
            "espn_event_id": str(espn_id),
            "fox_event_id": fox_event_id or (payload.get("header") or {}).get("id"),
            "fox_id_resolved_by": resolved_by,
            "fox_home_team_id": _fox_team_id((payload.get("header") or {}).get("rightTeam")),
            "fox_away_team_id": _fox_team_id((payload.get("header") or {}).get("leftTeam")),
        },
        notes=notes,
    )


def _fox_odds(payload: Mapping[str, Any], odds_payload: Any, fox_event_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Fox's closing six-pack as ``odds_override``, fetched only when nothing else stated one."""
    home = ((payload.get("header") or {}).get("rightTeam") or {}).get("imageAltText")
    if odds_payload is None:
        if fox_event_id is None:
            return None
        try:
            odds_payload = _fox_event_odds("cfb", fox_event_id)
        except Exception:  # noqa: BLE001 -- odds are a bonus; the processor has a default
            return None
    return _fox_odds_override(odds_payload, home)


def _register_fox() -> None:
    """Register :func:`_fox_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("cfb", "fox")(_fox_adapter)


_register_fox()
