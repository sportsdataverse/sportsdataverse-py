"""Possession-boundary rule engine (pbpstats ``stats_nba`` semantics port).

Each rule function ports exactly one pbpstats property and carries a
``# pbpstats: file:lines`` citation. See the Phase B design spec and the
grounding dossier for the semantics inventory.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

#: Unknown subType strings seen at rule-decision time (conservative fallback taken).
UNKNOWN_SUBTYPE_COUNTER: Counter = Counter()


def _norm(s: object) -> str:
    """Casefolded, stripped string of a possibly-None value."""
    return str(s or "").strip().casefold()


def resolve_event_team(row: dict, home_id: int, away_id: int) -> int:
    """Row ``team_id`` if truthy, else location ``h``/``v`` mapping, else 0."""
    team = row.get("team_id") or 0
    if team:
        return int(team)
    loc = row.get("location") or ""
    if loc == "h":
        return home_id
    if loc == "v":
        return away_id
    return 0


@dataclass
class EventContext:
    """Pre-pass index over enhanced-pbp rows for co-clock rule lookups.

    Mirrors pbpstats ``get_all_events_at_current_time``
    (pbpstats: resources/enhanced_pbp/enhanced_pbp_item.py:52-69).
    """

    rows: list = field(default_factory=list)
    at_clock: dict = field(default_factory=dict)

    def co_clock(self, i: int) -> list:
        """Indices of all rows sharing (period, seconds_remaining) with row i."""
        row = self.rows[i]
        return self.at_clock.get(
            (int(row.get("period") or 0), float(row.get("seconds_remaining") or 0.0)),
            [i],
        )


def build_event_context(rows: list) -> EventContext:
    """Build the co-clock index in one pass over the row dicts."""
    at_clock: dict = {}
    for i, row in enumerate(rows):
        key = (int(row.get("period") or 0), float(row.get("seconds_remaining") or 0.0))
        at_clock.setdefault(key, []).append(i)
    return EventContext(rows=rows, at_clock=at_clock)


def is_no_turnover(row: dict) -> bool:
    """A turnover placeholder row that is not a real turnover.

    # pbpstats: resources/enhanced_pbp/turnover.py:16-18 (abstract);
    # live/turnover.py:15-17 (``not hasattr(self, "sub_type")``). playbyplayv3
    # carries subType strings for every real turnover (season-catalog-verified);
    # an empty/missing subType is the conservative placeholder signal.
    """
    return (row.get("event_type") or "") == "turnover" and not _norm(row.get("sub_type"))


def _rebound_missed_shot_index(ctx: EventContext, i: int) -> int:
    """Index of the missed shot / missed FT this rebound follows, else -1.

    # pbpstats: resources/enhanced_pbp/rebound.py:16-28 (``missed_shot`` walks
    # previous events for a Missed FieldGoal or missed FreeThrow).
    """
    rows = ctx.rows
    for j in range(i - 1, -1, -1):
        et = rows[j].get("event_type") or ""
        if et == "missed_shot":
            return j
        if et == "free_throw":
            sh = (rows[j].get("score_home") or "").strip()
            sa = (rows[j].get("score_away") or "").strip()
            if not (sh or sa):  # missed FT (no score string -- same signal as _ft_made)
                return j
            return -1
        if et in ("made_shot", "turnover", "rebound", "jump_ball", "period"):
            return -1
    return -1


_TURNOVER_COINCIDENT_SUBTYPES = frozenset(("shot clock turnover", "kicked ball violation"))


def is_real_rebound(ctx: EventContext, i: int) -> bool:
    """The 5 placeholder exclusions of pbpstats ``is_real_rebound``, v3-reconstructed.

    # pbpstats: resources/enhanced_pbp/rebound.py:30-133. v3's rebound subType
    # ("Unknown"/"Normal Rebound") carries no ``player1_id == 0`` placeholder
    # signal that the stats.nba.com v2-era ``is_placeholder``/``event_action_type``
    # fields exposed, so each exclusion below is reconstructed from
    # clock/event adjacency instead of a field-value check:
    #  1. non-final-FT-miss placeholder (rebound.py:81-90, is_non_live_ft_placeholder)
    #     -- reconstructed via _rebound_missed_shot_index + _is_last_ft instead of
    #     ``missed_shot.is_end_ft``.
    #  2. turnover-coincident placeholder (rebound.py:65-79, is_turnover_placeholder)
    #     -- reconstructed via co_clock + is_no_turnover instead of
    #     ``player1_id == 0`` + ``is_shot_clock_violation``/``is_kicked_ball``.
    #  3. buzzer-beater-at-0.0s placeholder (rebound.py:92-113, is_buzzer_beater_placeholder)
    #  4. buzzer-beater-at-shot-time placeholder (rebound.py:115-133,
    #     is_buzzer_beater_rebound_at_shot_time)
    #     -- 3+4 reconstructed together below via seconds_remaining <= 3 and the
    #     next non-rebound row being a period boundary (v3 has no Replay event
    #     type to skip over, so ``next_event`` is simply the next row).
    # ``is_placeholder`` itself (rebound.py:56-63, ``player1_id == 0``) has no
    # exact v3 analogue and is intentionally NOT ported as a standalone
    # exclusion -- its two concrete manifestations (non-final-FT rebounds and
    # turnover-coincident rebounds) are each covered by exclusions 1 and 2
    # above; a bare ``player1_id == 0`` check would also flag ordinary team
    # rebounds that ARE real (e.g. a team rebound off a live-ball missed FG),
    # which pbpstats itself only excludes via the other four properties.
    """
    rows = ctx.rows
    row = rows[i]
    if (row.get("event_type") or "") != "rebound":
        return False
    # 1. rebound after a missed NON-final FT -> placeholder (play continues to next FT)
    j = _rebound_missed_shot_index(ctx, i)
    if j >= 0 and (rows[j].get("event_type") or "") == "free_throw":
        # local import: avoids cycle with nba_possessions
        from sportsdataverse.nba.nba_possessions import _is_last_ft

        if not _is_last_ft(rows[j].get("sub_type") or ""):
            return False
    # 2. coincident with a shot-clock / kicked-ball turnover at the same clock
    for k in ctx.co_clock(i):
        if k == i:
            continue
        if (rows[k].get("event_type") or "") == "turnover" and not is_no_turnover(rows[k]):
            if _norm(rows[k].get("sub_type")) in _TURNOVER_COINCIDENT_SUBTYPES:
                return False
    # 3+4. buzzer-beater placeholders: rebound at 0.0s, or at the same clock as a
    # <=3s missed shot, when the next non-rebound row is a period boundary.
    secs = float(row.get("seconds_remaining") or 0.0)
    if secs <= 3.0:
        nxt = i + 1
        while nxt < len(rows) and (rows[nxt].get("event_type") or "") == "rebound":
            nxt += 1
        next_is_period_end = nxt >= len(rows) or (rows[nxt].get("event_type") or "") == "period"
        if next_is_period_end and (secs == 0.0 or (j >= 0 and float(rows[j].get("seconds_remaining") or 0.0) == secs)):
            return False
    return True
