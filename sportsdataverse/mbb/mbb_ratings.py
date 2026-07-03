"""Individual offensive rating (Dean-Oliver ORtg + Adj Rtg+ productivity).

Faithful port of hoop-explorer's ``RatingUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/RatingUtils.ts``, 2260 LOC). This task (Phase 2, Task 2.2)
ports :func:`build_o_rtg` -- the ``buildORtg`` static method
(``RatingUtils.ts:398``) -- which derives an individual player's offensive
rating (points produced per 100 individual possessions, adapted from
`basketball-reference.com's NBA box-score method
<https://www.basketball-reference.com/about/ratings.html>`_) plus the
"Adj Rtg+" (SoS + usage adjusted efficiency above replacement) used
downstream as a RAPM prior (Phase 3).

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``RatingUtils.ts`` from
`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
(the hoop-explorer.com SPA), which is licensed under the Apache License,
Version 2.0 (the upstream repo's ``LICENSE`` file; full text at
`<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section 4's
redistribution-of-derivative-works obligations, sportsdataverse-py (itself
MIT-licensed) retains the upstream copyright notice for this derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full
third-party attribution entry (upstream URL, license, and exactly what was
derived), and ``tests/fixtures/hoop_explorer/README.md`` for the vendored
jest-oracle fixture provenance (same upstream repo, same commit, test-only --
not shipped in the distributed wheel).

**Signature note -- brief vs. TS (TS governs).** The Phase-2 plan's brief
sketch for this task (``.superpowers/sdd/hoop-explorer-port/task-2.2-brief.md``)
proposed a ``build_o_rtg(player, roster_stats_by_code, team_stats,
avg_efficiency, *, adjust_for_luck=False, override_adjusted=False)`` stub
with a 2-tuple return. That stub does not match ``buildORtg``'s actual
6-positional-arg / 5-tuple contract (no ``adjust_for_luck`` parameter exists
anywhere in ``RatingUtils.ts``; ``calc_diags``/``override_adjusted`` are
required booleans, not defaulted keyword-only flags). Per the task's own
governing rule ("where brief and TS disagree, TS governs") and per
Task 2.1's surprise #1 (getting the ``(calc_diags, override_adjusted)`` flag
pair wrong only silently diverges the 2nd/3rd oracle assertions, not the
1st), this module mirrors ``buildORtg``'s actual TS signature verbatim,
snake-cased:

``build_o_rtg(stat_set, roster_stats_by_code, extra_team_stat_info,
avg_efficiency, calc_diags, override_adjusted) -> (o_rtg, adj_o_rtg,
raw_o_rtg, raw_adj_o_rtg, o_rtg_diags)`` -- a 5-tuple, matching
``RatingUtils.ts:398-411``'s own destructured return shape.

**Diagnostics dict key naming.** ``o_rtg_diags`` (the ``ORtgDiagnostics``
return slot) keeps its field names **exactly as declared in the TS
``ORtgDiagnostics`` type** (``RatingUtils.ts:12-131``, e.g. ``rawFga``,
``SD_at_Usage``, ``adjORtgPlus``) rather than snake-casing them. This is a
deliberate data-contract decision, not an oversight: those keys are the
oracle projection validated bit-for-bit against
``tests/fixtures/hoop_explorer/rating_utils_snap.json`` (the
``"RatingUtils - buildORtg 1"`` snapshot entry) and
``sample-data/sampleOrtgDiagnostics.ts`` -- introducing a translation layer
between the Python dict and the vendored oracle fixture would itself be a
source of transcription bugs, and every other Phase-1/2 pass-through
``LineupStatSet``-shaped dict in this port (e.g. ``off_ppp``,
``total_off_fga``) is likewise kept verbatim rather than renamed. Only the
*function/parameter* surface (``build_o_rtg``, ``roster_stats_by_code``,
``stat_set``, ...) is snake_case, per the project's Python API convention.

Ported behavior (``RatingUtils.ts`` anchors):

- ``REPLACEMENT_LEVEL`` = ``0.92`` (``RatingUtils.ts:321``,
  ``RatingUtils.Replacement_Level``) and
  ``RETAIN_POSS_WITH_REBOUND_RATE`` = ``1.07`` (``RatingUtils.ts:324``,
  ``RatingUtils.retainPossWithReboundRate``) are copied verbatim as module
  constants.
- :func:`_build_off_overrides` -- private port of ``buildOffOverrides``
  (``RatingUtils.ts:329-393``). Computes the manual-shooting-override deltas
  (``off_3p``/``off_2p``/``off_ft``/``off_to`` value vs. ``old_value``) and
  projects them onto the raw made/attempt/points/turnover fields
  ``build_o_rtg`` consumes when ``override_adjusted=True``. Kept private
  (leading underscore) for this task -- Task 2.3 owns deciding whether to
  promote it to a public ``build_off_overrides`` (the upstream jest test
  covers it directly via inline ``toEqual`` literals, not a
  ``rating_utils_snap.json`` entry, so it isn't this task's oracle gate) --
  but the math is ported verbatim now since ``build_o_rtg`` cannot compute
  its 2nd/3rd progressive-override oracle assertions without it.
- :func:`_build_productivity` -- private port of ``buildProductivity``
  (``RatingUtils.ts:963-990``, Dean Oliver's "PUE" with diagnostics). Turns
  ``(ORtg, o_adj, usage, avgEfficiency)`` into ``{Adj_ORtg, Adj_ORtgPlus,
  Usage_Bonus, SoS_Bonus}``. Per Task 2.1's report, this function has **no
  direct jest test** -- it is only exercised indirectly through
  ``buildORtg`` (this task) and ``adjustOffRatingStats`` (Task 2.3). Kept
  private and structured as a standalone helper (not inlined into
  :func:`build_o_rtg`) specifically so Task 2.3's ``build_d_rtg`` /
  ``adjust_off_rating_stats`` can import and reuse it without duplicating
  the formula -- Task 2.3 owns whether it graduates to a public
  ``build_productivity``.
- :func:`build_o_rtg` -- the full ``buildORtg`` possession-chain port
  (``RatingUtils.ts:398-960``): points-produced decomposition (FG/AST/FT/ORB
  parts, with both the "classic" and the new per-shot-location assisted-eFG
  algorithms retained side by side, matching upstream), possession
  decomposition (``ScPoss``/``FGxPoss``/``FTxPoss``/``TotPoss``), the
  Dean-Oliver ``ORtg`` = ``100 * PProd / TotPoss``, usage, SD-based
  diagnostics (``SD_at_Usage`` etc., upstream-flagged "not used any more"
  but preserved for parity), and the recursive un-overridden
  raw-value pass when ``override_adjusted=True``.

**Known unguarded-division landmines (preserved for fidelity).** Several TS
expressions divide without a ``|| 1`` safety net and rely on JS's
``x / 0 -> Infinity`` / ``0 / 0 -> NaN`` semantics rather than raising:

1. ``Team_Prob_Hit_1plus_FT = 1 - (1 - Team_FTM / Team_FTA) ** 2``
   (``RatingUtils.ts:627``, no guard on ``Team_FTA``).
2. ``_build_off_overrides``'s
   ``extra_tos = (adj_new_to_pct * curr_poss - old_tos) / (1 - adj_new_to_pct)``
   (``RatingUtils.ts:347``, unguarded when ``adj_new_to_pct == 1``, though
   the cap to ``0.9`` just above makes the zero denominator unreachable in
   practice -- the *expression* itself carries no guard).
3. ``o_adj = avgEfficiency / Def_SOS`` (``RatingUtils.ts:789``, no guard on
   ``Def_SOS``). ``Def_SOS`` falls back to ``avg_efficiency`` when
   ``def_adj_opp`` is absent or falsy (the ``|| avgEfficiency`` coalesce at
   ``RatingUtils.ts:463`` -- note a ``{"value": 0}`` is *also* falsy and
   folds to the fallback), so the zero denominator is reachable only when
   the caller passes ``avg_efficiency == 0`` itself -- but the expression
   carries no guard.

Python raises ``ZeroDivisionError`` for a literal ``x / 0.0`` where JS would
silently produce ``inf``/``nan`` and keep going -- this module does **not**
add a defensive ``or 1`` to these spots (that would be an unfaithful
deviation from the TS), so e.g. a caller feeding a stat set with
``team_total_off_fta == 0`` -- including the degenerate empty-``{}`` stat
set, which (matching TS ``!statSet`` truthiness: ``{}`` is truthy in JS)
does NOT short-circuit and instead falls through into landmine 1 -- will
see an exception where the JS original degrades to NaN propagation (for the
``{}`` case the upstream NaN is contained to the ``teamProbFtHitOnePlus``
diagnostic; every downstream use is behind a ``Team_FTA > 0`` guard). None
of this task's oracle fixtures exercise these edge cases.

Deferred to Task 2.3 (``.superpowers/sdd/hoop-explorer-port/PLAN-phase2.md``):
``build_d_rtg`` (``buildDRtg``, ``RatingUtils.ts:1252``), ``build_net_points``
(``buildNetPoints``, ``RatingUtils.ts:1036``), ``adjust_off_rating_stats``
(``adjustOffRatingStats``, ``RatingUtils.ts:993``), and the on-ball-defense
adjustment family (``injectUncatOnBallDefenseStats``,
``buildOnBallDefenseAdjustmentsPhase1``,
``injectOnBallDefenseAdjustmentsPhase2``). ``build_off_overrides`` may be
promoted to public by that task if it decides to test it directly.
"""

from __future__ import annotations

from typing import Any, Callable

from sportsdataverse.mbb.mbb_lineup_stats import LineupStatSet, _field_val, _num

#: ``ORtgDiagnostics`` (``RatingUtils.ts:12-131``) -- kept as a plain dict
#: alias (field names verbatim from TS, see module docstring).
ORtgDiagnostics = dict[str, Any]

#: The % of average efficiency that represents replacement level.
#: Verbatim from ``RatingUtils.ts:321`` (``RatingUtils.Replacement_Level``).
REPLACEMENT_LEVEL: float = 0.92

#: Arbitrary/guess constant from the original (NBA box scores) paper: the
#: possession is retained ~7% of the time without an off. rebound. Verbatim
#: from ``RatingUtils.ts:324`` (``RatingUtils.retainPossWithReboundRate``).
RETAIN_POSS_WITH_REBOUND_RATE: float = 1.07

#: Shot-location keys in upstream iteration order (``RatingUtils.ts:432``).
_SHOT_LOCS: tuple[str, str, str] = ("2prim", "2pmid", "3p")

#: ``shotLocToLoc`` (``RatingUtils.ts:433-437``) -- maps a shot-location key
#: to the suffix used by the ``total_off_ast_*`` / ``off_ast_*_target``
#: aggregate field names.
_SHOT_LOC_TO_LOC: dict[str, str] = {"3p": "3p", "2prim": "rim", "2pmid": "mid"}

#: Points awarded per made shot at each ``_SHOT_LOCS`` index
#: (``RatingUtils.ts:438``, ``shotBonus``).
_SHOT_BONUS: tuple[int, int, int] = (2, 2, 3)


def _sum_by(values: list[float], fn: Callable[[float, int], float]) -> float:
    """Index-aware sum, port of the ``buildORtg``-local ``sumBy`` closure
    (``RatingUtils.ts:416-418``, a version of ``_.sumBy`` that also passes
    the index).
    """
    return sum(fn(x, ii) for ii, x in enumerate(values))


def _override_diff(field: Any) -> float:
    """Port of ``OverrideUtils.diff`` (``OverrideUtils.ts:460-462``): the
    delta from the raw value to a manual override, or ``0.0`` when the
    field carries no ``old_value`` (``_.isNil(val?.old_value)``).
    """
    if not isinstance(field, dict) or field.get("old_value") is None:
        return 0.0
    val = field.get("value")
    return (val if val else 0.0) - field["old_value"]


def _build_off_overrides(stat_set: LineupStatSet) -> dict[str, dict[str, float]]:
    """Private port of ``RatingUtils.buildOffOverrides`` (``RatingUtils.ts:329-393``).

    Builds the raw-field overrides implied by a player's manual shooting/TO%
    overrides (``off_3p``/``off_2p``/``off_ft``/``off_to`` ``value`` vs.
    ``old_value``), projected onto the fields :func:`build_o_rtg` reads via
    its internal ``stat_get`` when ``override_adjusted=True``. See the
    module docstring for why this stays private for this task.

    Args:
        stat_set: The player's ``LineupStatSet`` (ES-aggregation-shaped
            per-player doc).

    Returns:
        A dict of 11 ``{"value": float}`` overrides:
        ``total_off_fgm``/``total_off_2p_made``/``total_off_3p_made``/
        ``total_off_ftm``/``total_off_to``/``off_poss``/
        ``team_total_off_pts``/``team_total_off_fgm``/
        ``team_total_off_3p_made``/``team_total_off_ftm``/
        ``team_total_off_to``.
    """
    three_p_tries = _num(stat_set, "total_off_3p_attempts", 0.0)
    two_p_tries = _num(stat_set, "total_off_2p_attempts", 0.0)
    free_throw_tries = _num(stat_set, "total_off_fta", 0.0)

    extra_3p_makes = _override_diff(stat_set.get("off_3p")) * three_p_tries
    extra_2p_makes = _override_diff(stat_set.get("off_2p")) * two_p_tries
    extra_fg_makes = extra_3p_makes + extra_2p_makes
    extra_ft_makes = _override_diff(stat_set.get("off_ft")) * free_throw_tries

    # TOs are more complicated -- see RatingUtils.ts:339-347 for the algebra:
    # (old_tos + tos_diff) / (currPoss + tos_diff) = new_to% =>
    # tos_diff = (new_to% * currPoss - old_tos) / (1 - new_to%)
    new_to_pct = _num(stat_set, "off_to", 0.0)
    adj_new_to_pct = 0.9 if new_to_pct > 0.9 else new_to_pct  # avoid stupidly high TO%
    old_tos = _num(stat_set, "total_off_to", 0.0)
    curr_poss = _num(stat_set, "off_poss", 0.0)
    extra_tos = (adj_new_to_pct * curr_poss - old_tos) / (1 - adj_new_to_pct)

    return {
        "total_off_fgm": {"value": _num(stat_set, "total_off_fgm", 0.0) + extra_fg_makes},
        "total_off_2p_made": {"value": _num(stat_set, "total_off_2p_made", 0.0) + extra_2p_makes},
        "total_off_3p_made": {"value": _num(stat_set, "total_off_3p_made", 0.0) + extra_3p_makes},
        "total_off_ftm": {"value": _num(stat_set, "total_off_ftm", 0.0) + extra_ft_makes},
        "total_off_to": {"value": _num(stat_set, "total_off_to", 0.0) + extra_tos},
        "off_poss": {"value": _num(stat_set, "off_poss", 0.0) + extra_tos},
        "team_total_off_pts": {
            "value": (
                _num(stat_set, "team_total_off_pts", 0.0) + 3 * extra_3p_makes + 2 * extra_2p_makes + extra_ft_makes
            )
        },
        "team_total_off_fgm": {"value": _num(stat_set, "team_total_off_fgm", 0.0) + extra_fg_makes},
        "team_total_off_3p_made": {"value": _num(stat_set, "team_total_off_3p_made", 0.0) + extra_3p_makes},
        "team_total_off_ftm": {"value": _num(stat_set, "team_total_off_ftm", 0.0) + extra_ft_makes},
        "team_total_off_to": {"value": _num(stat_set, "team_total_off_to", 0.0) + extra_tos},
    }


def _build_productivity(
    o_rtg: float,
    o_adj: float,
    usage: float,
    avg_efficiency: float,
) -> dict[str, float]:
    """Private port of ``RatingUtils.buildProductivity`` (``RatingUtils.ts:963-990``).

    Converts ``ORtg`` and a few other numbers into "productivity" using Dean
    Oliver's PUE ("Player Usage Efficiency") formulation, SoS-adjusted via
    ``o_adj = avgEfficiency / Def_SOS``. **RAPM prior source (Phase 3):**
    ``Adj_ORtgPlus`` is the value RAPM uses as an individual-offense prior --
    see ``PLAN-phase2.md``'s self-review notes.

    Args:
        o_rtg: The player's (possibly override-adjusted) ``ORtg``.
        o_adj: ``avg_efficiency / Def_SOS`` -- the strength-of-schedule
            adjustment factor.
        usage: ``100 * TotPoss / (Team_Poss or 1)`` -- the player's
            possession-usage percentage.
        avg_efficiency: The league/context average efficiency (``100`` in
            every vendored jest call).

    Returns:
        ``{"Adj_ORtg": float, "Adj_ORtgPlus": float, "Usage_Bonus": float,
        "SoS_Bonus": float}`` -- keys kept TS-verbatim (see module
        docstring's naming-convention note).
    """
    adj_o_rtg = o_rtg * o_adj
    unadjusted_productivity = 0.01 * usage * (o_rtg - avg_efficiency)
    raw_productivity = (
        0.01 * usage * (o_rtg - REPLACEMENT_LEVEL * avg_efficiency) - 0.2 * (1 - REPLACEMENT_LEVEL) * avg_efficiency
    )
    # Adjusted for both SoS and usage:
    adj_o_rtg_plus = (
        0.01 * usage * (adj_o_rtg - REPLACEMENT_LEVEL * avg_efficiency) - 0.2 * (1 - REPLACEMENT_LEVEL) * avg_efficiency
    )

    usage_bonus = 5 * (raw_productivity - unadjusted_productivity)
    sos_bonus = adj_o_rtg_plus - raw_productivity
    return {
        "Adj_ORtg": adj_o_rtg,
        "Adj_ORtgPlus": adj_o_rtg_plus,
        "Usage_Bonus": usage_bonus,
        "SoS_Bonus": sos_bonus,
    }


def build_o_rtg(
    stat_set: LineupStatSet | None,
    roster_stats_by_code: dict[str, LineupStatSet] | None,
    extra_team_stat_info: LineupStatSet,
    avg_efficiency: float,
    calc_diags: bool,
    override_adjusted: bool,
) -> tuple[
    dict[str, float] | None,
    dict[str, float] | None,
    dict[str, float] | None,
    dict[str, float] | None,
    ORtgDiagnostics | None,
]:
    """Individual offensive rating (Dean-Oliver ORtg) + diagnostics.

    Faithful port of ``RatingUtils.buildORtg`` (``RatingUtils.ts:398-960``).
    See the module docstring for the signature-vs-brief note (this mirrors
    the TS 6-positional-arg / 5-tuple contract verbatim, snake_cased) and
    the diagnostics-dict key-naming convention (TS-verbatim, not
    snake_cased).

    Args:
        stat_set: The player's ``LineupStatSet`` (ES-aggregation-shaped
            per-player doc, "IndivStatSet" upstream). ``None`` returns an
            all-``None`` 5-tuple (``RatingUtils.ts:412-413``'s
            ``if (!statSet)`` -- null/undefined only). An **empty dict does
            NOT short-circuit** (``{}`` is truthy in JS and falls through to
            compute upstream); in this port it falls into unguarded-division
            landmine 1 and raises ``ZeroDivisionError`` where the TS
            degrades to a NaN-laced degenerate result -- see the module
            docstring's landmine list.
        roster_stats_by_code: ``{player_code: LineupStatSet}`` for every
            player on the roster -- used for the approximate team-ORB
            apportionment and the per-shot-location assisted-eFG fallback.
            ``None`` is treated as ``{}`` (every vendored jest call passes
            a literal ``{}``).
        extra_team_stat_info: ``{"total_off_to": {...}, "sum_total_off_to":
            {...}}`` -- team-level TOV bookkeeping used to compute
            "unblamed" team turnovers apportioned by ``off_team_poss_pct``.
        avg_efficiency: League/context average efficiency (``100`` in every
            vendored jest call).
        calc_diags: When ``True``, populate the 5th tuple slot
            (``ORtgDiagnostics``); otherwise it is ``None``.
        override_adjusted: When ``True``, apply :func:`_build_off_overrides`
            to the raw made/attempt/turnover fields before computing, and
            additionally recurse once (with ``calc_diags=False,
            override_adjusted=False``) to compute the un-overridden "raw"
            values for the 3rd/4th tuple slots.

    Returns:
        A 5-tuple ``(o_rtg, adj_o_rtg, raw_o_rtg, raw_adj_o_rtg,
        o_rtg_diags)``:

        - ``o_rtg``: ``{"value": ORtg}`` when ``TotPoss > 0``, else ``None``.
        - ``adj_o_rtg``: ``{"value": Adj_ORtgPlus}`` when ``TotPoss > 0``,
          else ``None``.
        - ``raw_o_rtg``: when ``calc_diags or override_adjusted``, the
          un-overridden ``ORtg`` (``None`` if ``override_adjusted=False``,
          since no un-overridden pass was computed); otherwise a special
          internal-recursion value ``{"value": usage}``
          (``RatingUtils.ts:835``'s "if called internally return usage
          here" case).
        - ``raw_adj_o_rtg``: the un-overridden ``adj_o_rtg`` (``None`` when
          ``override_adjusted=False``).
        - ``o_rtg_diags``: the full ``ORtgDiagnostics`` dict (``None``
          unless ``calc_diags=True``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ratings import build_o_rtg

            o_rtg, adj_o_rtg, _, _, diags = build_o_rtg(
                player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
                100.0, True, False,
            )
            print(o_rtg["value"], diags["oRtg"])

        Override-adjusted (manual shooting-% overrides applied)::

            o_rtg2, adj_o_rtg2, raw_o_rtg2, raw_adj_o_rtg2, _ = build_o_rtg(
                player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
                100.0, False, True,
            )

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    # TS `if (!statSet)` (RatingUtils.ts:412) -- only null/undefined are
    # falsy for an object-typed arg; `{}` is truthy in JS and falls through:
    if stat_set is None:
        return (None, None, None, None, None)

    roster = roster_stats_by_code or {}

    overrides: dict[str, dict[str, float]] = _build_off_overrides(stat_set) if override_adjusted else {}

    def stat_get(key: str) -> float:
        """Port of the ``buildORtg``-local ``statGet`` closure
        (``RatingUtils.ts:423-429``): prefer the override projection, fall
        back to the raw stat-set field, then ``|| 0``.
        """
        override_field = overrides.get(key)
        if override_field is not None:
            raw = override_field.get("value")
        else:
            field = stat_set.get(key)
            raw = field.get("value") if isinstance(field, dict) else None
        return raw if raw else 0.0

    unblamed_tovs = max(
        _num(extra_team_stat_info, "total_off_to", 0.0) - _num(extra_team_stat_info, "sum_total_off_to", 0.0),
        0.0,
    )
    unblamed_tovs_player = 0.2 * unblamed_tovs * stat_get("off_team_poss_pct")

    fga = _num(stat_set, "total_off_fga", 0.0)
    fgm = stat_get("total_off_fgm")
    ftm = stat_get("total_off_ftm")
    fta = _num(stat_set, "total_off_fta", 0.0)
    ast = _num(stat_set, "total_off_assist", 0.0)
    tov = stat_get("total_off_to") + unblamed_tovs_player
    orb = _num(stat_set, "total_off_orb", 0.0)
    fg2pm = stat_get("total_off_2p_made")
    fg3pm = stat_get("total_off_3p_made")
    off_poss = stat_get("off_poss")
    def_sos = _num(stat_set, "def_adj_opp", avg_efficiency)

    made = [stat_get(f"total_off_{loc}_made") for loc in _SHOT_LOCS]
    attempts_denom = [stat_get(f"total_off_{loc}_attempts") or 1.0 for loc in _SHOT_LOCS]  # ||1: used as denom
    real_attempts = [stat_get(f"total_off_{loc}_attempts") for loc in _SHOT_LOCS]
    assisted_pct = [stat_get(f"off_{loc}_ast") for loc in _SHOT_LOCS]
    assists_totals = [stat_get(f"total_off_ast_{_SHOT_LOC_TO_LOC[loc]}") for loc in _SHOT_LOCS]
    assists: list[tuple[str, dict[str, float]]] = []
    for loc in _SHOT_LOCS:
        target_field = stat_set.get(f"off_ast_{_SHOT_LOC_TO_LOC[loc]}_target")
        player_map = target_field.get("value") if isinstance(target_field, dict) else None
        assists.append((loc, player_map or {}))

    team_ast = _num(stat_set, "team_total_off_assist", 0.0)
    team_fgm = stat_get("team_total_off_fgm")
    team_fga = _num(stat_set, "team_total_off_fga", 0.0)
    team_ftm = stat_get("team_total_off_ftm")
    team_fta = _num(stat_set, "team_total_off_fta", 0.0)
    team_pts = stat_get("team_total_off_pts")
    team_tov = stat_get("team_total_off_to")
    team_3pm = stat_get("team_total_off_3p_made")
    team_poss = stat_get("team_total_off_poss")

    # TODO (upstream): regress this to bigger samples (RatingUtils.ts:495).
    team_orb = _num(stat_set, "team_total_off_orb", 0.0)
    opponent_drb = _num(stat_set, "oppo_total_def_drb", 0.0)
    sum_players_orb = sum(_num(p, "total_off_orb", 0.0) for p in roster.values())
    global_orb = sum(_num(p, "team_total_off_orb", 0.0) for p in roster.values()) / 5.0
    roster_orb = team_orb * (sum_players_orb / (global_orb or 1.0))

    # Useful base derived stats:
    pts_from_fg = 2 * fg2pm + 3 * fg3pm
    e_fg = pts_from_fg / (2 * fga) if fga > 0 else 0.0
    team_pts_from_fg = team_pts - team_ftm
    others_fga = team_fga - fga
    others_fgm = team_fgm - fgm
    others_ast = team_ast - ast
    others_efg = (team_pts_from_fg - pts_from_fg) / (2 * others_fga) if others_fga > 0 else 0.0

    # don't use q_ast_classic except for display (inaccurate at scale, upstream comment)
    q_ast_classic = 1.14 * (others_ast / team_fgm) if team_fgm > 0 else 0.0
    q_ast = (_sum_by(made, lambda player_made, ii: assisted_pct[ii] * player_made) / fgm) if fgm > 0 else 0.0

    team_assist_contrib_classic = 0.5 * e_fg * q_ast
    fg_part_classic = fgm * (1 - team_assist_contrib_classic)
    ast_part_classic = 0.5 * ((team_pts_from_fg - pts_from_fg) / (2 * others_fga)) * ast if others_fga > 0 else 0.0

    def cap_three_pt_assist_penalty(efg: float, shot_type: int) -> float:
        """Port of the ``buildORtg``-local ``capThreePtAssistPenalty``
        closure (``RatingUtils.ts:559-569``). ``shot_type == 2`` is the 3P
        shot-location index (``_SHOT_LOCS[2] == "3p"``).
        """
        if shot_type == 2:
            # shouldn't penalize 3P shooters for being good! in fact we'll
            # give them a buff if they are really good (capped at 45%):
            elite_3p_buff = min(efg - 0.38 * 1.5, 0.07 * 1.5) if efg > 0.38 * 1.5 else 0.0
            return min(efg, 0.345 * 1.5) - elite_3p_buff
        return efg

    fgm_minus_assist_penalty: list[float] = []
    for ii, player_made in enumerate(made):
        # (0.5*eFG)*(assisted FGs=FG*assisted)
        player_efg = 0.5 * _SHOT_BONUS[ii] * (player_made / attempts_denom[ii])
        fgm_minus_assist_penalty.append(
            player_made * (1 - 0.5 * cap_three_pt_assist_penalty(player_efg, ii) * assisted_pct[ii])
        )
    fg_part = sum(fgm_minus_assist_penalty)
    # Back-calculate the classic-calc equivalents, for approximate diags display only:
    team_assist_contrib = 1 - fg_part / fgm if fgm > 0 else 0.0
    team_assisted_efg = 2 * (team_assist_contrib / q_ast) if q_ast > 0 else 0.0

    capped_efg_by_shot_type: list[float] = []
    for ii, (shot_loc, player_map) in enumerate(assists):
        total_efg_count = assists_totals[ii] or 1.0
        efg_part1 = 0.5 * _SHOT_BONUS[ii]
        total = 0.0
        for player_code, count in player_map.items():
            roster_player = roster.get(player_code)
            field = roster_player.get(f"off_{shot_loc}") if roster_player else None
            # (if we can't find the player, fall back to team eFG for all phases)
            player_efg = efg_part1 * _field_val(field, "value", others_efg / efg_part1)
            total += cap_three_pt_assist_penalty(player_efg, ii) * count
        capped_efg_by_shot_type.append(total / total_efg_count)
    ast_part = [0.5 * capped_efg_by_shot_type[ii] * assists_totals[ii] for ii in range(3)]

    # We have the actual number of possessions, so we can do better than the legacy 0.475:
    actual_ft_poss = max(team_poss - (team_tov + team_fga - team_orb), 1.0 if fta > 0 else 0.0)
    actual_fta_to_poss = actual_ft_poss / (team_fta or 1.0)

    prob_miss_both_ft = (1 - ftm / fta) ** 2 if fta > 0 else 0.0
    ft_part = (1 - prob_miss_both_ft) * actual_fta_to_poss * fta if fta > 0 else 0.0

    # NOTE: unguarded division (matches upstream) -- see module docstring.
    team_prob_hit_1plus_ft = 1 - (1 - team_ftm / team_fta) ** 2
    team_scoring_poss = team_fgm + team_prob_hit_1plus_ft * team_fta * actual_fta_to_poss if team_fta > 0 else 0.0

    team_orb_pct = team_orb / (team_orb + opponent_drb) if (team_orb + opponent_drb) > 0 else 0.0
    num_team_plays = team_fga + team_fta * actual_fta_to_poss + team_tov
    team_play_pct = team_scoring_poss / num_team_plays if num_team_plays > 0 else 0.0

    credit_to_rebounder = (1 - team_orb_pct) * team_play_pct
    credit_to_scorer = team_orb_pct * (1 - team_play_pct)
    team_orb_weight_denom = credit_to_rebounder + credit_to_scorer
    team_orb_weight = credit_to_rebounder / team_orb_weight_denom if team_orb_weight_denom > 0 else 0.0

    team_score_rebound_pct = (roster_orb * team_play_pct) / team_scoring_poss if team_scoring_poss > 0 else 0.0
    team_orb_contrib = team_orb_weight * team_score_rebound_pct

    orb_part = orb * team_orb_weight * team_play_pct

    sc_poss = (fg_part + sum(ast_part) + ft_part) * (1 - team_orb_contrib) + orb_part
    # (legacy assist code)
    sc_poss_classic = (fg_part_classic + ast_part_classic + ft_part) * (1 - team_orb_contrib) + orb_part

    # Note: this is the main vector for leaking poss (more noticeable in small samples)
    fgx_poss = (fga - fgm) * (1 - RETAIN_POSS_WITH_REBOUND_RATE * team_orb_pct)
    ftx_poss = prob_miss_both_ft * actual_fta_to_poss * fta if fta > 0 else 0.0
    tot_poss = sc_poss + fgx_poss + ftx_poss + tov

    shot_poss_decomp = [
        fgm_minus_assist_penalty[ii] * (1 - team_orb_contrib)
        + (real_attempts[ii] - made[ii]) * (1 - RETAIN_POSS_WITH_REBOUND_RATE * team_orb_pct)
        for ii in range(3)
    ]
    rim_poss, mid_poss, three_poss = shot_poss_decomp

    pprod_fg_part_classic = pts_from_fg * (1 - team_assist_contrib_classic)
    pprod_fg_part = _sum_by(fgm_minus_assist_penalty, lambda f, ii: f * _SHOT_BONUS[ii])

    pprod_fg_decomp = [fgm_minus_assist_penalty[ii] * _SHOT_BONUS[ii] * (1 - team_orb_contrib) for ii in range(3)]
    rim_pts_prod, mid_pts_prod, three_pts_prod = pprod_fg_decomp

    other_efg = (team_fgm - fgm + 0.5 * (team_3pm - fg3pm)) / others_fga if others_fga > 0 else 0.0
    other_pts_per_fgm = (team_pts_from_fg - pts_from_fg) / others_fgm if others_fgm > 0 else 0.0
    pprod_ast_part_classic = 0.5 * other_efg * ast * other_pts_per_fgm
    pprod_ast_part = _sum_by(ast_part, lambda a, ii: _SHOT_BONUS[ii] * a)

    ast_three_pprod = ast_part[2] * _SHOT_BONUS[2] * (1 - team_orb_contrib)
    ast_two_pprod = (pprod_ast_part - ast_three_pprod) * (1 - team_orb_contrib)
    ast_two_poss = (ast_part[0] + ast_part[1]) * (1 - team_orb_contrib)
    ast_three_poss = ast_part[2] * (1 - team_orb_contrib)

    team_fts_hit_1plus = team_prob_hit_1plus_ft * actual_fta_to_poss * team_fta if team_fta > 0 else 0.0
    team_pts_per_score = team_pts / (team_fgm + team_fts_hit_1plus) if (team_fgm + team_fts_hit_1plus) > 0 else 0.0
    pprod_orb_part = orb * team_orb_weight * team_play_pct * team_pts_per_score

    pprod = (pprod_fg_part + pprod_ast_part + ftm) * (1 - team_orb_contrib) + pprod_orb_part
    # Legacy assist algo:
    pprod_classic = (pprod_fg_part_classic + pprod_ast_part_classic + ftm) * (1 - team_orb_contrib) + pprod_orb_part
    tot_poss_classic = sc_poss_classic + fgx_poss + ftx_poss + tov
    o_rtg_classic = 100 * (pprod_classic / tot_poss_classic) if tot_poss > 0 else 0.0

    o_rtg = 100 * (pprod / tot_poss) if tot_poss > 0 else 0.0

    # Calculate actual ORtg usage and use that in all ORtg calcs:
    usage = (100 * tot_poss) / (team_poss or 1.0)

    o_adj = avg_efficiency / def_sos
    # (not used any more, kept for display parity)
    sd_at_usage = usage * -0.144 + 13.023
    sds_above_mean = (o_rtg - avg_efficiency) / sd_at_usage if sd_at_usage > 0 else 0.0
    sd_at_usage_20 = 10.143
    regressed_o_rtg = avg_efficiency + sds_above_mean * sd_at_usage_20

    productivity = _build_productivity(o_rtg, o_adj, usage, avg_efficiency)
    adj_o_rtg = productivity["Adj_ORtg"]
    adj_o_rtg_plus = productivity["Adj_ORtgPlus"]
    usage_bonus = productivity["Usage_Bonus"]
    sos_bonus = productivity["SoS_Bonus"]

    # If the values have been overridden, also calculate the un-overridden values:
    raw_o_rtg: dict[str, float] | None
    raw_adj_rating: dict[str, float] | None
    raw_usage: dict[str, float] | None
    if override_adjusted:
        raw_result = build_o_rtg(stat_set, roster_stats_by_code, extra_team_stat_info, avg_efficiency, False, False)
        raw_o_rtg, raw_adj_rating, raw_usage = raw_result[0], raw_result[1], raw_result[2]
    else:
        raw_o_rtg = raw_adj_rating = raw_usage = None

    diags: ORtgDiagnostics | None = None
    if calc_diags:
        raw_usage_val = None if raw_usage is None else max(raw_usage["value"], 0.0)
        diags = {
            # Basic player numbers:
            "rawFga": fga,
            "rawFgx": fga - fgm,
            "rawFgm": fgm,
            "ptsFgm": pts_from_fg,
            "rawFtm": ftm,
            "rawAssist": ast,
            "rawAssistInfo": [f"{v:.0f}" for v in reversed(assists_totals)],  # (3p first)
            "rawPts": pts_from_fg + ftm,
            "rawOrb": orb,
            "rawTo": tov,
            # Shooting breakdowns, just for display:
            "raw3Fga": _num(stat_set, "total_off_3p_attempts", 0.0),
            "raw2midFga": _num(stat_set, "total_off_2pmid_attempts", 0.0),
            "raw2rimFga": _num(stat_set, "total_off_2prim_attempts", 0.0),
            "raw3Fgm": stat_get("total_off_3p_made"),
            "raw2midFgm": _num(stat_set, "total_off_2pmid_made", 0.0),
            "raw2rimFgm": _num(stat_set, "total_off_2prim_made", 0.0),
            # Basic team numbers:
            "teamOrb": team_orb,
            "teamPts": team_pts,
            "teamFga": team_fga,
            "teamFgm": team_fgm,
            "teamFta": team_fta,
            "teamFtPct": team_ftm / team_fta if team_fta > 0 else 0.0,
            "teamOrbPct": team_orb_pct,
            "teamTo": team_tov,
            "teamPoss": team_poss,
            # 1] Points produced calcs:
            "eFG": e_fg,
            "teamPtsPerScore": team_pts_per_score,
            "teamFtHitOnePlus": team_fts_hit_1plus,
            "teamProbFtHitOnePlus": team_prob_hit_1plus_ft,
            "rosterOrb": roster_orb,
            "teamOrbCreditToRebounder": credit_to_rebounder,
            "teamOrbCreditToScorer": credit_to_scorer,
            "teamScoreFromReboundPct": team_score_rebound_pct,
            "teamOrbWeight": team_orb_weight,
            "othersAssist": others_ast,
            "otherEfg": other_efg,
            "otherEfgInfo": [f"{100 * v:.1f}" for v in reversed(capped_efg_by_shot_type)],  # (3p first)
            "otherPtsPerFgm": other_pts_per_fgm,
            "teamOrbContribPct": team_orb_contrib,
            "teamScoredPlayPct": team_play_pct,
            # Old school vs new assist%:
            "teamAssistRate_Classic": q_ast_classic,
            "ppFgTeamAstPct_Classic": team_assist_contrib_classic,
            "teamAssistRate": q_ast,
            "ppFgTeamAstPct": team_assist_contrib,
            "teamAssistedEfg": team_assisted_efg,
            # Pts produced:
            "ptsProd": pprod,
            "ppOrb": pprod_orb_part,
            "ppAssist": pprod_ast_part,
            "ppAssist_Classic": pprod_ast_part_classic,
            "ppFg": pprod_fg_part,
            # 2] Possession Calcs:
            "ftPoss": actual_fta_to_poss * fta,
            "actualFtaToPoss": actual_fta_to_poss,
            "ftPct": ftm / fta if fta > 0 else 0.0,
            "missedBothFTs": prob_miss_both_ft,
            "offPlaysLessPoss": fga + fta * actual_fta_to_poss + tov - off_poss,
            "offPoss": off_poss,
            "fgPart": fg_part,
            "ftPart": ft_part,
            "astPart": sum(ast_part),
            "astPart_Classic": ast_part_classic,
            "orbPart": orb_part,
            "teamScoringPoss": team_scoring_poss,
            "teamPlays": num_team_plays,
            "adjPoss": tot_poss,
            "scoringPoss": sc_poss,
            "fgxPoss": fgx_poss,
            "ftxPoss": ftx_poss,
            # Adjusted calcs:
            "oRtg": o_rtg,
            "oRtg_Classic": o_rtg_classic,
            "defSos": def_sos,
            "avgEff": avg_efficiency,
            "SD_at_Usage": sd_at_usage,  # (these 4 aren't used any more but kept for info)
            "SDs_Above_Mean": sds_above_mean,
            "SD_at_Usage_20": sd_at_usage_20,
            "Regressed_ORtg": regressed_o_rtg,
            "Usage": max(usage, 0.0),  # (sane in edge cases; can replace off_usage)
            "Raw_Usage": raw_usage_val,
            "Usage_Bonus": usage_bonus,
            "SoS_Bonus": sos_bonus,
            "adjORtg": adj_o_rtg,
            "adjORtgPlus": adj_o_rtg_plus,
            # Some decomposition for Net Points calcs:
            "rimPoss": rim_poss,
            "midPoss": mid_poss,
            "threePoss": three_poss,
            "astThreePoss": ast_three_poss,
            "astTwoPoss": ast_two_poss,
            "rimPtsProd": rim_pts_prod,
            "midPtsProd": mid_pts_prod,
            "threePtsProd": three_pts_prod,
            "astThreePProd": ast_three_pprod,
            "astTwoPProd": ast_two_pprod,
            # Adjustment for pts/possession discrepancies (set by callers, default identity):
            "adjPtsFactor": 1,
            "adjPossFactor": 1,
        }

    return (
        {"value": o_rtg} if tot_poss > 0 else None,
        {"value": adj_o_rtg_plus} if tot_poss > 0 else None,
        raw_o_rtg if (calc_diags or override_adjusted) else {"value": usage},
        raw_adj_rating,
        diags,
    )
