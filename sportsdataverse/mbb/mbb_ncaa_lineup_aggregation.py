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

__all__ = ["LineupStatSet", "lineup_stats_bucket"]

# ponytail: lineup_stats_buckets (the list-form public entry point) lands in
# a later task (group-by producer); this task ships the single-lineup
# assembler `lineup_stats_bucket`.


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


_PREFIXES = ("", "scramble_", "trans_")


def _all_rate_fields(
    totals_by_prefix: dict[str, dict[str, float]],
    dst: str,
    oppo_totals_by_prefix: dict[str, dict[str, float]],
) -> dict[str, dict[str, float]]:
    # ponytail: prefix loop over base/scramble_/trans_ families, commonLineupAggregations.ts:181-282.
    # orb + assist-dist gating already lives in `_rate_fields` (prefix == "" only); no special-casing here.
    out: dict[str, dict[str, float]] = {}
    for prefix in _PREFIXES:
        out.update(_rate_fields(totals_by_prefix[prefix], dst, prefix, oppo_totals_by_prefix[prefix]))
    return out


_PLAY_TYPE_PREFIXES = ("scramble_", "trans_")  # ponytail: `_.drop(typePrefixes)` == everything but "".


def _play_type_pts_poss(
    off_totals_by_prefix: dict[str, dict[str, float]],
    def_totals_by_prefix: dict[str, dict[str, float]],
) -> dict[str, float]:
    """``total_{dst}_{scramble_,trans_}pts`` / ``poss`` -- commonLineupAggregations.ts:342-380.

    The base (prefix "") pts/poss totals are plain scalar sums already emitted by
    ``_sum_fields`` (see its ``if prefix == ""`` branch); TS only bucket-scripts pts/poss
    for the *other* typePrefixes (``_.drop(typePrefixes)`` == ``scramble_``/``trans_``),
    hence this function's narrower scope.

    Takes both sides' per-prefix totals (not one dst at a time, unlike ``_rate_fields``)
    because the ``poss`` script's rebound_pct term cross-references the *opposite* dst's
    BASE (prefix "") ``drb`` total (``total_${oppoDstPrefix}_drb``, ts:368) -- same
    cross-side convention as this module's ``_orb`` helper.

    Args:
        off_totals_by_prefix: ``{"": {...}, "scramble_": {...}, "trans_": {...}}`` totals
            for ``dst="off"`` (as returned by ``_sum_fields`` per prefix).
        def_totals_by_prefix: same shape for ``dst="def"``.

    Returns:
        Flat ``total_{off,def}_{scramble_,trans_}{pts,poss}`` dict (8 keys).
    """
    out: dict[str, float] = {}
    sides = {"off": (off_totals_by_prefix, def_totals_by_prefix), "def": (def_totals_by_prefix, off_totals_by_prefix)}
    for dst, (totals_by_prefix, oppo_totals_by_prefix) in sides.items():
        oppo_dst = "def" if dst == "off" else "off"
        # ponytail: var_orb/var_drb are always the BASE (prefix "") totals, ts:367-368 --
        # not re-derived per scramble_/trans_ typePrefix.
        var_orb = totals_by_prefix[""].get(f"total_{dst}_orb", 0.0)
        var_drb = oppo_totals_by_prefix[""].get(f"total_{oppo_dst}_drb", 0.0)
        rebound_pct = var_orb / (var_orb + var_drb) if var_orb > 0 else 0.0
        for prefix in _PLAY_TYPE_PREFIXES:
            t = totals_by_prefix.get(prefix, {})
            made3p = t.get(f"total_{dst}_{prefix}3p_made", 0.0)
            made2p = t.get(f"total_{dst}_{prefix}2p_made", 0.0)
            ftm = t.get(f"total_{dst}_{prefix}ftm", 0.0)
            out[f"total_{dst}_{prefix}pts"] = 3.0 * made3p + 2.0 * made2p + ftm

            fga = t.get(f"total_{dst}_{prefix}fga", 0.0)
            fgm = t.get(f"total_{dst}_{prefix}fgm", 0.0)
            fta = t.get(f"total_{dst}_{prefix}fta", 0.0)
            to = t.get(f"total_{dst}_{prefix}to", 0.0)
            fg_missed = fga - fgm  # ts:371 `def fgM = params.fga - params.fgm;`
            out[f"total_{dst}_{prefix}poss"] = fgm + (1.0 - rebound_pct) * fg_missed + 0.475 * fta + to
    return out


def _adj_fields(
    pts: float,
    poss: float,
    dst: str,
    opponent_baselines: Optional[dict[str, float]],
    avg_eff: float = 100.0,
) -> dict[str, dict[str, float]]:
    """``{dst}_adj_ppp`` / ``{dst}_adj_opp`` -- commonLineupAggregations.ts:443-502.

    ``opponent_baselines is None`` is the only branch this port implements: TS's
    ``properAdjEffCalc`` (hardcoded ``true``, ts:169) weighted_avg/SOS branch needs a
    per-opponent baseline lookup (``calculateAdjEff``'s ES ``weighted_avg`` over
    opponent efficiency) this port has no data source for yet.

    ponytail: the returned value is a *faithful degenerate case* of TS's own
    bucket_script fallback (ts:503-522: ``(var_adj_opp > 0) ? var_ppp*avgEff/var_adj_opp
    : avgEff``), not an invented formula -- with no SOS data, ``adj_opp := avg_eff``
    (can't compute the real weighted-avg), so
    ``adj_ppp = var_ppp*avgEff/avgEff == var_ppp`` (raw ppp). Confirmed against the
    analyzer's own adj_ppp *read*-site coalesce (not the query builder):
    ``cbb-on-off-analyzer/src/utils/stats/LineupUtils.ts:1161``
    ``const off_adj_ppp = lineup.off_adj_ppp?.value || avgEff;`` -- the consumer already
    falls back to ``avgEff`` whenever the stored value is falsy/zero, so returning raw
    ppp here (or ``0.0`` under the zero-poss guard) composes correctly with that
    read-site; it does not contradict it.

    Args:
        pts: Total points for this dst/prefix bucket.
        poss: Total possessions for this dst/prefix bucket.
        dst: ``"off"`` or ``"def"``.
        opponent_baselines: SOS baseline lookup; only ``None`` (no baselines) is
            implemented.
        avg_eff: League-average efficiency (points per 100 possessions) used as the
            fallback constant. Defaults to ``100.0``.

    Returns:
        ``{f"{dst}_adj_ppp": {"value": ...}, f"{dst}_adj_opp": {"value": ...}}``.

    Raises:
        NotImplementedError: If ``opponent_baselines`` is not ``None`` -- the real
            SOS-adjusted branch isn't ported yet; raising avoids silently returning the
            degenerate fallback to a caller that expects real adjustment.
    """
    if opponent_baselines is not None:
        raise NotImplementedError("_adj_fields: opponent_baselines SOS branch (TS properAdjEffCalc) not yet ported")
    ppp = 100.0 * pts / poss if poss > 0 else 0.0
    return {
        f"{dst}_adj_ppp": {"value": ppp},
        f"{dst}_adj_opp": {"value": avg_eff},
    }


_T1_SUFFIXES = {"": ".total", "scramble_": ".orb", "trans_": ".early"}  # ts:31-78 shot-bucket suffix map.


def lineup_stats_bucket(
    ev: LineupEvent,
    *,
    avg_eff: float = 100.0,
    opponent_baselines: Optional[dict[str, float]] = None,
    doc_count: int = 1,
) -> LineupStatSet:
    """Assemble one lineup's full 254-field ``{value}`` bucket.

    ``lineup_stats_bucket`` is the Python entry point for stage 2 of the port (see the
    module docstring) -- the faithful composition of this module's factories in the order
    ``commonLineupAggregations.ts`` (572-line ES aggregation) issues them: ``sum`` (
    :func:`_sum_fields`) -> merge the play-type ``pts``/``poss`` bucket_script (
    :func:`_play_type_pts_poss`) -> mint every other rate bucket_script (
    :func:`_all_rate_fields`) -> the SOS-adjusted-efficiency bucket_script (
    :func:`_adj_fields`).

    Args:
        ev: One already-summed lineup event (``team_stats``/``opponent_stats`` populated by
            stage 1, :func:`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`).
        avg_eff: League-average efficiency passed through to :func:`_adj_fields`.
        opponent_baselines: SOS baseline lookup passed through to :func:`_adj_fields`; only
            ``None`` (no baselines) is implemented.
        doc_count: The ES ``doc_count`` for this bucket (number of raw events folded in).

    Returns:
        The full bucket: every ``total_*``/rate/adj field wrapped in ``{"value": <float>}``,
        plus the structural keys ``key``, ``players_array``, ``doc_count`` (bare, unwrapped).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_aggregation import lineup_stats_bucket

            bucket = lineup_stats_bucket(enriched_event, doc_count=7)
            bucket["off_ppp"]["value"]

    See Also:
        * `cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_ --
          ``src/utils/es-queries/commonLineupAggregations.ts``, this function's TS source.
    """
    off_totals_by_prefix = {
        prefix: _sum_fields(ev.team_stats, dst="off", prefix=prefix, suffix=suffix)
        for prefix, suffix in _T1_SUFFIXES.items()
    }
    def_totals_by_prefix = {
        prefix: _sum_fields(ev.opponent_stats, dst="def", prefix=prefix, suffix=suffix)
        for prefix, suffix in _T1_SUFFIXES.items()
    }

    # Fold the play-type pts/poss bucket_script in BEFORE minting rates -- _all_rate_fields
    # reads total_{dst}_{scramble_,trans_}{pts,poss} out of these same dicts.
    play_type = _play_type_pts_poss(off_totals_by_prefix, def_totals_by_prefix)
    for dst, totals_by_prefix in (("off", off_totals_by_prefix), ("def", def_totals_by_prefix)):
        for prefix in _PLAY_TYPE_PREFIXES:
            for stem in ("pts", "poss"):
                key = f"total_{dst}_{prefix}{stem}"
                totals_by_prefix[prefix][key] = play_type[key]

    off_rates = _all_rate_fields(off_totals_by_prefix, "off", def_totals_by_prefix)
    def_rates = _all_rate_fields(def_totals_by_prefix, "def", off_totals_by_prefix)

    off_adj = _adj_fields(
        pts=off_totals_by_prefix[""]["total_off_pts"],
        poss=off_totals_by_prefix[""]["total_off_poss"],
        dst="off",
        opponent_baselines=opponent_baselines,
        avg_eff=avg_eff,
    )
    def_adj = _adj_fields(
        pts=def_totals_by_prefix[""]["total_def_pts"],
        poss=def_totals_by_prefix[""]["total_def_poss"],
        dst="def",
        opponent_baselines=opponent_baselines,
        avg_eff=avg_eff,
    )

    bucket: LineupStatSet = {
        "key": _bucket_key(ev),
        "players_array": _players_array(ev),
        "doc_count": doc_count,
    }
    for totals_by_prefix in (off_totals_by_prefix, def_totals_by_prefix):
        for totals in totals_by_prefix.values():
            for stem, value in totals.items():
                bucket[stem] = {"value": value}
    bucket.update(off_rates)
    bucket.update(def_rates)
    bucket.update(off_adj)
    bucket.update(def_adj)
    return bucket
