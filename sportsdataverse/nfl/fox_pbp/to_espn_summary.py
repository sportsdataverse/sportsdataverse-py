"""Fox Bifrost NFL -> an ESPN-summary-shaped dict ``NFLPlayProcess`` consumes unchanged.

Everything that decides a number lives in :mod:`sportsdataverse.football.fox_common`, which
both Fox adapters share; this module is the NFL half: ESPN's NFL play-type vocabulary, the
ESPN <-> Fox franchise table the id resolver joins on, and the dispatch registration for
``source="fox"``.

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

from sportsdataverse.football.fox_common import (
    _fox_event_data,
    _fox_event_odds,
    _fox_odds_override,
    _fox_to_espn_summary,
    _fox_team_id,
    _has_pbp,
)
from sportsdataverse.nfl.shield_pbp.to_espn_summary import _ESPN_TEAMS

#: ESPN NFL ``type.id`` -> ``(type.text, type.abbreviation)``; the same table the Shield and
#: CBS adapters emit, re-exported here so the Fox projection never invents a type string.
from sportsdataverse.nfl.shield_pbp.to_espn_summary import ESPN_PLAY_TYPES  # noqa: E402  (re-export)

#: ESPN franchise id -> Fox Bifrost team id. Fox's ids are its own (Cleveland is 7 on Fox and
#: 5 on ESPN), and the id map's ``fox_team_id`` column is still null for the NFL because there
#: is no offline producer for it, so the resolver joins the week's Fox scoreboard on this.
#: Derived -- not invented -- by joining the captured Fox ``scores-segment`` rows to nfl-raw's
#: ``crosswalk/teams.json`` on the **full team name**, which the source study proved 16/16 on
#: two 2026 weeks; Fox abbreviations are unusable for the join (``WAS``/``WSH``, ``JAC``/``JAX``).
# ponytail: a static 32-row snapshot keyed by ESPN's franchise id, which survives relocations.
# If it ever needs to move, fill the id map's ``TEAM_SCHEMA.fox_team_id`` column instead.
_FOX_TEAM_BY_ESPN: Dict[str, str] = {
    "1": "25",  # ATL
    "2": "1",  # BUF
    "3": "20",  # CHI
    "4": "6",  # CIN
    "5": "7",  # CLE
    "6": "15",  # DAL
    "7": "10",  # DEN
    "8": "21",  # DET
    "9": "22",  # GB
    "10": "8",  # TEN
    "11": "2",  # IND
    "12": "11",  # KC
    "13": "12",  # LV
    "14": "26",  # LAR
    "15": "3",  # MIA
    "16": "23",  # MIN
    "17": "4",  # NE
    "18": "27",  # NO
    "19": "16",  # NYG
    "20": "5",  # NYJ
    "21": "17",  # PHI
    "22": "18",  # ARI
    "23": "9",  # PIT
    "24": "13",  # LAC
    "25": "28",  # SF
    "26": "14",  # SEA
    "27": "24",  # TB
    "28": "19",  # WSH
    "29": "32",  # CAR
    "30": "31",  # JAX
    "33": "33",  # BAL
    "34": "34",  # HOU
}

#: Club codes Fox's Stats-Perform **play text** spells differently from its own header, mapped
#: to the header code. The processor charges timeouts ("Timeout #1 by CLE.") and attributes
#: penalties ("PENALTY on JAC-R.Hainsey") by matching the emitted abbreviation against the
#: text, so an unmapped code silently costs every timeout and every penalty team in the game.
_FOX_TEXT_ALIASES: Dict[str, str] = {"JAC": "JAX", "WAS": "WSH", "LA": "LAR"}

#: ESPN ``season_type`` -> Fox segment type digit (1 REG, 2 POST, 3 PRE).
_SEGMENT_TYPE = {1: 3, 2: 1, 3: 2}
#: ESPN postseason week -> Fox postseason week. ESPN numbers the Pro Bowl 4 and the Super Bowl
#: 5; Fox has no Pro Bowl and numbers the Super Bowl 4.
_POST_WEEK = {1: 1, 2: 2, 3: 3, 4: None, 5: 4}

_TEAM_META: Dict[str, Dict[str, Any]] = {
    espn_id: {"abbreviation": abbr, "color": color, "alternateColor": alternate}
    for espn_id, (abbr, color, alternate) in _ESPN_TEAMS.items()
}


def _segment_ids(season: Any, season_type: Any, week: Any) -> List[str]:
    """Fox ``scores-segment`` ids to try for one ESPN (season, season_type, week), best first.

    A Fox segment id is ``{season}-{week}-{type}``. Regular season and preseason are a single
    candidate; the postseason gets the rest of its weeks as fall-backs, because the segment is
    only a *search space* -- the game is identified by its Fox team-id pair, so a wrong segment
    simply yields no match rather than a wrong game.
    """
    try:
        season, season_type, week = int(season), int(season_type), int(week)
    except (TypeError, ValueError):
        return []
    digit = _SEGMENT_TYPE.get(season_type)
    if digit is None:
        return []
    if season_type != 3:
        return [f"{season}-{week}-{digit}"]
    first = _POST_WEEK.get(week)
    candidates = ([first] if first else []) + [w for w in (1, 2, 3, 4) if w != first]
    return [f"{season}-{w}-{digit}" for w in candidates]


def _resolve_fox_event_id(idmap_row: Mapping[str, Any], *, transport: Any = None) -> Tuple[Optional[str], str]:
    """``(fox_event_id, provenance)`` for one game; ``(None, ...)`` means hand over.

    Cascade, strongest first:

    1. the id map's stored ``fox_event_id`` -- a fact, not a search;
    2. Fox's own week scoreboard (``nfl/league/scores-segment/{season}-{week}-{type}``), joined
       on the **Fox team-id pair** from :data:`_FOX_TEAM_BY_ESPN`. This is the leg that has to
       work while ESPN is unreachable, and it does: the segment is Fox's own schedule.
    3. nothing. An id is **never** invented -- a game Fox does not list falls through to the
       next source rather than fetching a wrong one.
    """
    stored = (idmap_row or {}).get("fox_event_id")
    if stored:
        return str(stored), "idmap"
    home = _FOX_TEAM_BY_ESPN.get(str((idmap_row or {}).get("home_espn_team_id") or ""))
    away = _FOX_TEAM_BY_ESPN.get(str((idmap_row or {}).get("away_espn_team_id") or ""))
    if not (home and away):
        return None, "unresolved"
    from sportsdataverse._fox_layout import fox_get, parse_segment_events

    get = transport or fox_get
    for segment in _segment_ids(
        (idmap_row or {}).get("season"), (idmap_row or {}).get("season_type"), (idmap_row or {}).get("week")
    ):
        try:
            raw = get(f"nfl/league/scores-segment/{segment}")
        except Exception:  # noqa: BLE001 -- a 404 or a timeout is a miss on this segment, not a raise
            continue
        for event in parse_segment_events(raw, segment):
            if str(event.get("home_team_id") or "") == home and str(event.get("away_team_id") or "") == away:
                if event.get("game_id"):
                    return str(event["game_id"]), f"scores_segment:{segment}"
    return None, "unresolved"


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
    the fetch fails, and -- the case Fox answers with **HTTP 200** -- when the payload carries
    no ``pbp`` at all: every NFL game before the **2024** season.
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
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"fox event data fetch failed: {type(exc).__name__}: {exc}") from exc
    if not isinstance(payload, Mapping) or not payload.get("header"):
        # the shared ``_get`` returns {} on any failure, so this is "the request failed" --
        # deliberately a different message from "Fox does not cover this game", below.
        raise SourceUnavailable(f"nfl {espn_id}: fox returned no event payload (request failed or bad id)")
    if not _has_pbp(payload):
        raise SourceUnavailable(
            f"nfl {espn_id}: fox event {fox_event_id or payload.get('header', {}).get('id')} carries no "
            "play-by-play (HTTP 200 with no pbp key: before Fox's 2024 NFL play floor)"
        )
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id")):
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
