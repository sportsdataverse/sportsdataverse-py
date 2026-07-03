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
