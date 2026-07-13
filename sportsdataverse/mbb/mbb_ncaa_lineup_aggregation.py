"""Stage-2 rate-minting: raw-count LineupEventStats -> 254-field LineupStatSet bucket.

Faithful port of hoop-explorer's ``commonLineupAggregations.ts`` (the Elasticsearch
aggregation that mints per-lineup rate fields). Stage 1 (classification) is
``mbb_ncaa_lineup_enrich``; stage 3 (on/off re-weighting) is ``mbb_lineup_stats``.
See dev/bigballr_port/lineup_aggregation_design.md.
"""

from __future__ import annotations

from typing import Any, Optional

from .mbb_ncaa_models import LineupEvent, LineupEventStats, ShotClockStats

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


# (name, attribute-path-tuple)  — ponytail: commonShotAggs/commonMiscAggs, .ts:31-78
_SHOT_SRC = {  # emitted-stem -> (fg-attr) ; each expands to _attempts/_made/_ast
    "2prim": "fg_rim",
    "2pmid": "fg_mid",
    "2p": "fg_2p",
    "3p": "fg_3p",
}
_MISC_SRC = {  # emitted-stem -> attr-path-on-stats
    "fga": ("fg", "attempts"),
    "fgm": ("fg", "made"),
    "ftm": ("ft", "made"),
    "fta": ("ft", "attempts"),
    "to": ("to",),
    "assist": ("assist",),
    "orb": ("orb",),
    "drb": ("drb",),
    "blk": ("blk",),
    "stl": ("stl",),
    "foul": ("foul",),
}
_ASSIST_SRC = {"ast_rim": "ast_rim", "ast_mid": "ast_mid", "ast_3p": "ast_3p"}


def _sum_fields(s: LineupEventStats, dst: str, prefix: str, suffix: str) -> dict[str, float]:
    out: dict[str, float] = {}

    def put(stem: str, val: float) -> None:
        out[f"total_{dst}_{prefix}{stem}"] = val

    # shots: attempts/made/ast for each shot type
    for stem, fg_attr in _SHOT_SRC.items():
        fg = getattr(s, fg_attr)
        put(f"{stem}_attempts", _leaf(fg.attempts, suffix))
        put(f"{stem}_made", _leaf(fg.made, suffix))
        put(f"{stem}_ast", _leaf(fg.ast, suffix))
    # misc scalars-by-clock
    for stem, path in _MISC_SRC.items():
        node: Any = s
        for a in path:
            node = getattr(node, a)
        put(stem, _leaf(node, suffix))
    # assist counts
    for stem, ai_attr in _ASSIST_SRC.items():
        ai = getattr(s, ai_attr)
        put(stem, _leaf(ai.counts if ai is not None else None, suffix))
    # pts/poss are scalar totals (prefix "" here; scramble_/trans_ handled in Task 6)
    if prefix == "":
        put("pts", float(s.pts))
        put("poss", float(s.num_possessions))
    return out
