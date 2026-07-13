"""Stage-2 rate-minting: raw-count LineupEventStats -> 254-field LineupStatSet bucket.

Faithful port of hoop-explorer's ``commonLineupAggregations.ts`` (the Elasticsearch
aggregation that mints per-lineup rate fields). Stage 1 (classification) is
``mbb_ncaa_lineup_enrich``; stage 3 (on/off re-weighting) is ``mbb_lineup_stats``.
See dev/bigballr_port/lineup_aggregation_design.md.
"""

from __future__ import annotations

from typing import Any, Optional

from .mbb_ncaa_models import LineupEvent, ShotClockStats

LineupStatSet = dict[str, Any]

# ponytail: __all__ omitted -- lineup_stats_bucket/lineup_stats_buckets (the
# public entry points) land in a later task; this task ships only the
# underscore-prefixed accessors + the LineupStatSet alias.


def _leaf(stat: Optional[ShotClockStats], suffix: str) -> float:
    # ponytail: ES leaf selector, commonLineupAggregations.ts:171-180 suffix map.
    if stat is None:
        return 0.0
    attr = suffix.lstrip(".")  # ".total" -> "total"
    val = getattr(stat, attr, None)
    return float(val) if val is not None else 0.0


def _bucket_key(ev: LineupEvent) -> str:
    return "_".join(sorted(p.code for p in ev.players))


def _players_array(ev: LineupEvent) -> dict:
    return {"hits": {"hits": [{"_source": {"players": [{"code": p.code, "id": p.id} for p in ev.players]}}]}}
