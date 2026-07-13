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


# (shotType stems used by both the shot-rate and eFG tables) — T3, commonAverageAggs.ts:291-397
_SHOT_RATE_TYPES = ("2p", "3p", "2prim", "2pmid")
_ASSIST_DIST_TYPES = ("rim", "mid", "3p")


def _rate(
    out: dict[str, dict[str, float]],
    totals: dict[str, float],
    dst: str,
    name: str,
    top: str,
    bottom: str,
    factor: float = 1.0,
) -> None:
    # ponytail: guarded rate formula, commonAverageAggs.ts:81-99 -- (num>0)?factor*num/den:0.
    # `out` takes an explicit param (vs. a closure) so Task 5's scramble_/trans_ loop can
    # reuse this directly instead of re-deriving a nested closure per prefix.
    num = totals.get(f"total_{dst}_{top}", 0.0)
    den = totals.get(f"total_{dst}_{bottom}", 0.0)
    # ponytail: guard den==0 too (num>0 alone can still divide by zero when the
    # denominator field is absent/zero, e.g. a partial totals dict in tests).
    out[f"{dst}_{name}"] = {"value": factor * num / den if num > 0 and den > 0 else 0.0}


def _rate_table(prefix: str) -> list[tuple[str, float, str, str]]:
    # T3 rate tuples (name, factor, top, bottom); top/bottom are total_* stems sans "total_{dst}_".
    p = prefix
    table: list[tuple[str, float, str, str]] = []
    for st in _SHOT_RATE_TYPES:
        table.append((f"{p}{st}", 1.0, f"{p}{st}_made", f"{p}{st}_attempts"))
        table.append((f"{p}{st}_ast", 1.0, f"{p}{st}_ast", f"{p}{st}_made"))
    table.extend(
        [
            (f"{p}ft", 1.0, f"{p}ftm", f"{p}fta"),
            (f"{p}ftr", 1.0, f"{p}fta", f"{p}fga"),
            (f"{p}2primr", 1.0, f"{p}2prim_attempts", f"{p}fga"),
            (f"{p}2pmidr", 1.0, f"{p}2pmid_attempts", f"{p}fga"),
            (f"{p}3pr", 1.0, f"{p}3p_attempts", f"{p}fga"),
            (f"{p}assist", 1.0, f"{p}assist", f"{p}fgm"),
            (f"{p}ppp", 100.0, f"{p}pts", f"{p}poss"),
            (f"{p}to", 1.0, f"{p}to", f"{p}poss"),
        ]
    )
    return table


def _efg(totals: dict[str, float], dst: str, prefix: str) -> dict[str, dict[str, float]]:
    # ponytail: eFG special-case, commonAverageAggs.ts:429-437 -- threes weighted 1.5x.
    fga = totals.get(f"total_{dst}_{prefix}fga", 0.0)
    made2 = totals.get(f"total_{dst}_{prefix}2p_made", 0.0)
    made3 = totals.get(f"total_{dst}_{prefix}3p_made", 0.0)
    value = (1.0 * made2 + 1.5 * made3) / fga if fga > 0 else 0.0
    return {f"{dst}_{prefix}efg": {"value": value}}


def _orb(totals: dict[str, float], oppo_totals: dict[str, float], dst: str) -> dict[str, dict[str, float]]:
    # ponytail: orb rate special-case, commonAverageAggs.ts:414-422 -- cross-side drb
    # (off's orb rate is denominated against the opponent's drb, not off's own drb).
    oppo_dst = "def" if dst == "off" else "off"
    var_orb = totals.get(f"total_{dst}_orb", 0.0)
    var_drb = oppo_totals.get(f"total_{oppo_dst}_drb", 0.0)
    value = var_orb / (var_orb + var_drb) if var_orb > 0 else 0.0
    return {f"{dst}_orb": {"value": value}}


def _rate_fields(
    totals: dict[str, float],
    dst: str,
    prefix: str,
    oppo_totals: dict[str, float],
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for name, factor, top, bottom in _rate_table(prefix):
        _rate(out, totals, dst, name, top, bottom, factor)
    out.update(_efg(totals, dst, prefix))
    # orb + assist-distribution rates are cross-side/base-only per T3 -- prefix "" only.
    if prefix == "":
        out.update(_orb(totals, oppo_totals, dst))
        for st in _ASSIST_DIST_TYPES:
            _rate(out, totals, dst, f"ast_{st}", f"ast_{st}", "assist")
    return out
