"""Fox Bifrost NFL -> an ESPN-summary-shaped dict ``NFLPlayProcess`` consumes unchanged.

Everything that decides a number lives in :mod:`sportsdataverse.football.fox_common`, which
both Fox adapters share; this module is the NFL half: ESPN's NFL play-type vocabulary and the
dispatch registration for ``source="fox"``. The id resolver and its ESPN <-> Fox franchise
table live in :mod:`sportsdataverse.nfl.fox_pbp.game_id`.

Fox is the **last** NFL source in ``...sources.dispatch.SOURCE_ORDER``: its play-by-play only
reaches the **2024 season** and its text is not GSIS, so rusher / receiver / sacker names and
air yards are lost (``...contract.KNOWN_LOSSY[("nfl", "fox")]``). What it buys is a feed that
shares an upstream with neither GSIS nor ESPN, and a closing line on the game payload.

Everything here is ``_``-prefixed and nothing is re-exported from ``sportsdataverse.nfl``, so
no codegen or reference-doc regeneration is involved -- the same shape as the Shield and CBS
adapters.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from sportsdataverse._codegen_runtime import _id_text
from sportsdataverse.football.fox_common import (
    _fox_event_data,
    _fox_event_odds,
    _fox_odds_override,
    _fox_to_espn_summary,
    _fox_team_id,
    _has_pbp,
)
from sportsdataverse.nfl.fox_pbp.game_id import _resolve_fox_game_id
from sportsdataverse.nfl.shield_pbp.to_espn_summary import _ESPN_TEAMS

#: ESPN NFL ``type.id`` -> ``(type.text, type.abbreviation)``; the same table the Shield and
#: CBS adapters emit, re-exported here so the Fox projection never invents a type string.
from sportsdataverse.nfl.shield_pbp.to_espn_summary import ESPN_PLAY_TYPES  # noqa: E402  (re-export)

#: Club codes Fox's Stats-Perform **play text** spells differently from its own header, mapped
#: to the header code. The processor charges timeouts ("Timeout #1 by CLE.") and attributes
#: penalties ("PENALTY on JAC-R.Hainsey") by matching the emitted abbreviation against the
#: text, so an unmapped code silently costs every timeout and every penalty team in the game.
_FOX_TEXT_ALIASES: Dict[str, str] = {"JAC": "JAX", "WAS": "WSH", "LA": "LAR"}

_TEAM_META: Dict[str, Dict[str, Any]] = {
    espn_id: {"abbreviation": abbr, "color": color, "alternateColor": alternate}
    for espn_id, (abbr, color, alternate) in _ESPN_TEAMS.items()
}


def _resolve_fox_event_id(idmap_row: Mapping[str, Any], *, transport: Any = None) -> Tuple[Optional[str], str]:
    """``(fox_event_id, provenance)`` for one game; ``(None, ...)`` means hand over.

    Cascade, strongest first:

    1. the id map's stored ``fox_event_id`` -- a fact, not a search;
    2. Fox's own week scoreboard (``nfl/league/scores-segment/{season}-{week}-{type}``), joined
       on the **Fox team-id pair** from :data:`...game_id._FOX_TEAM_BY_ESPN`. This is the leg that has to
       work while ESPN is unreachable, and it does: the segment is Fox's own schedule.
    3. nothing. An id is **never** invented -- a game Fox does not list falls through to the
       next source rather than fetching a wrong one.
    """
    row = idmap_row or {}
    # through _id_text: a NaN id-map cell is truthy and would be fetched as "nan" (#930)
    stored = _id_text(row.get("fox_event_id"))
    if stored:
        return stored, "idmap"
    return _resolve_fox_game_id(
        row.get("season"),
        row.get("season_type"),
        row.get("week"),
        _id_text(row.get("home_espn_team_id")),
        _id_text(row.get("away_espn_team_id")),
        transport=transport,
    )


def _fox_nfl_to_espn_summary(
    fox: Mapping[str, Any], idmap_row: Mapping[str, Any], *, odds: Optional[Mapping[str, Any]] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one Fox NFL game onto an ESPN-summary-shaped dict (see ``fox_common``)."""
    return _fox_to_espn_summary(
        fox,
        idmap_row,
        league="nfl",
        play_types=ESPN_PLAY_TYPES,
        espn_uid_league="28",
        team_meta=_TEAM_META,
        text_aliases=_FOX_TEXT_ALIASES,
        odds=odds,
    )


def _fox_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="fox"`` (NFL). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (:class:`...dispatch.SourceUnavailable`) when the Fox event
    id cannot be resolved without inventing one, when the id map states no ESPN team ids, when
    the fetch fails, when the payload carries no ``pbp`` at all -- the case Fox answers with
    **HTTP 200**: every NFL game before the **2024** season -- and when the adapted drive chart
    carries no play yet.

    Args:
        league: ``"nfl"``; the dispatcher passes it, this adapter does not branch on it.
        espn_id: The ESPN event id being served.
        ctx: The dispatcher's :class:`...dispatch.SourceContext` (``payload``, ``idmap_row``,
            ``participants``, ``odds_override``).

    Returns:
        :class:`...dispatch.AdaptedGame` -- the ESPN-shaped summary plus ``native_ids`` and the
        adapter notes.

    Raises:
        SourceUnavailable: no Fox event id could be resolved without inventing one; the fetch
            failed; the payload carries no ``header`` (an empty or headerless injected payload, or
            a bad id); the payload carries no ``pbp``; the id-map row states no ESPN team
            ids; or the adapted drive chart carries no plays yet.
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
        # Resolution runs only on the fetch path: an injected payload already IS the game, and
        # resolving anyway would make an offline replay reach Fox's scoreboard.
        fox_event_id, resolved_by = _resolve_fox_event_id(ctx.idmap_row or {})
        if not fox_event_id:
            raise SourceUnavailable(f"nfl {espn_id}: no fox_event_id in the id map and none on Fox's week scoreboard")
        try:
            payload = _fox_event_data("nfl", fox_event_id)
        except Exception as exc:  # network / JSON -> the next source
            raise SourceUnavailable(f"fox event data fetch failed: {type(exc).__name__}: {exc}") from exc
    if not isinstance(payload, Mapping) or not payload.get("header"):
        # the getter raises on a failed request, so this is an empty or headerless payload --
        # deliberately a different message from "Fox does not cover this game", below.
        raise SourceUnavailable(f"nfl {espn_id}: fox returned no event payload (request failed or bad id)")
    if not _has_pbp(payload):
        raise SourceUnavailable(
            f"nfl {espn_id}: fox event {fox_event_id or payload.get('header', {}).get('id')} carries no "
            "play-by-play (HTTP 200 with no plays: before Fox's 2024 NFL play floor, "
            "or a listed game Fox has not served yet)"
        )
    # ids through _id_text first: a NaN id is truthy and pandas.NA raises on truthiness
    if not (_id_text(row.get("home_espn_team_id")) and _id_text(row.get("away_espn_team_id"))):
        raise SourceUnavailable(f"nfl {espn_id}: id-map row carries no ESPN team ids")

    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row)
    if odds is None:
        odds = _fox_odds(payload, odds_payload, fox_event_id)
    summary, notes = _fox_nfl_to_espn_summary(payload, row, odds=odds)
    served = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    if not any(d.get("plays") for d in served):
        # checked over BOTH groupings: a live game's only drive is the open one, which lives
        # under ``current`` -- reading ``previous`` alone refused every opening drive in #540.
        raise SourceUnavailable(f"nfl {espn_id}: fox drive chart carries no plays yet")
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
    """Fox's closing six-pack as ``odds_override``, fetched only when nothing else stated one.

    The stored closing line beside the id map wins; this is the fall-back that keeps a Fox
    failover off the processor's 2.5 / 55.5 default. Never raises and never fetches on the
    injected-payload path.
    """
    home = ((payload.get("header") or {}).get("rightTeam") or {}).get("imageAltText")
    if odds_payload is None:
        if fox_event_id is None:
            return None
        try:
            odds_payload = _fox_event_odds("nfl", fox_event_id)
        except Exception:  # noqa: BLE001 -- odds are a bonus; the processor has a default
            return None
    return _fox_odds_override(odds_payload, home)


def _register_fox() -> None:
    """Register :func:`_fox_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("nfl", "fox")(_fox_adapter)


_register_fox()
