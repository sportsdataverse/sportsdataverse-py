"""RAPM (regularized adjusted plus-minus) priors + player-context construction.

Faithful port of hoop-explorer's ``RapmUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/RapmUtils.ts``, 1660+ LOC). Task 3.2 (Phase 3) ported the
**initialization layer**: the three ``RapmPriorInfo`` / ``RapmPlayerContext`` /
``RapmConfig`` types (``RapmUtils.ts:124/147/175``), :func:`build_priors`
(``RapmUtils.buildPriors``, ``RapmUtils.ts:237``), and
:func:`build_player_context` (``RapmUtils.buildPlayerContext``,
``RapmUtils.ts:427``). **Task 3.3 adds the design-matrix + target-vector
layer**: :func:`calc_player_weights` (``RapmUtils.calcPlayerWeights``,
``RapmUtils.ts:544``), :func:`calc_lineup_outputs`
(``RapmUtils.calcLineupOutputs``, ``RapmUtils.ts:598``), and the small
:func:`_get_strong_weight` helper (``RapmUtils.ts:135``) both depend on. This
is the **dict -> ``numpy.ndarray`` boundary**: everything upstream of these
two functions (``build_priors``/``build_player_context``) stays plain-dict
math; from here on, per-lineup/per-player numeric data is materialized into
``numpy`` arrays. Later Phase-3 tasks add the regression-solve layer
(``pickRidgeRegression``, ``injectRapmIntoPlayers``, ``calcCollinearityDiag``
-- see the "Deliberately NOT ported (this task)" section below) on top of the
matrices/vectors this module now produces.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``RapmUtils.ts`` from
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

**Structural TypedDict field names are snake_cased (contrast with
``mbb_ratings``'s diagnostics-dict convention).** ``RapmPriorInfo`` /
``RapmPlayerContext`` / ``RapmConfig`` are *glue* types this port introduces
to structure ``RapmUtils``'s own internal state -- unlike ``mbb_ratings.py``'s
``ORtgDiagnostics`` / ``DRtgDiagnostics`` (which keep upstream field names
verbatim because they are validated bit-for-bit against a vendored oracle
fixture that would otherwise require a lossy translation layer), these three
types' *own* field names are snake_cased per the project's Python API
convention (matching the plan brief's explicit instruction). The *content*
dicts these types wrap (``LineupStatSet``/``IndivStatSet``-shaped stat
objects, e.g. ``players_strong[i]["off_adj_ppp"]``) are, as elsewhere in this
port, kept verbatim (those field names were already snake_case in the TS
source, e.g. ``off_adj_rtg``, so no translation is needed either way).

Ported behavior (``RapmUtils.ts`` anchors):

- :data:`DEFAULT_RAPM_CONFIG` -- verbatim from ``RapmUtils.ts:181``
  (``defaultRapmConfig``): ``{prior_mode: -1, removal_pct: 0.06,
  fixed_regression: -1}``.
- :func:`build_priors` -- port of ``RapmUtils.buildPriors``
  (``RapmUtils.ts:237-407``). Builds the strong/weak per-player priors RAPM
  blends into the ridge-regression solve (Tasks 3.4-3.5 own the blend
  itself). **RAPM prior source (cross-module coupling):** in production,
  ``playersBaseline`` is populated from ``mbb_ratings.build_productivity``'s
  ``Adj_ORtgPlus`` (aliased upstream as ``off_adj_rtg``/``def_adj_rtg`` on
  each player's baseline stat doc) -- see ``mbb_ratings.py``'s own
  docstring, which documents this exact coupling from the other side. **This
  task's oracle test does NOT exercise that production path** -- per
  ``tests/fixtures/hoop_explorer/README.md``'s "Note for Tasks 3.2-3.6",
  ``RapmUtils.test.ts`` hand-rolls a synthetic ``off_adj_rtg``/``def_adj_rtg``
  overlay (``5.0 - 0.5*ii`` / ``-5.0 + ii*0.5``, indexed by baseline-bucket
  order) directly onto the vendored ``samplePlayerStatsResponse`` baseline
  docs, rather than calling ``build_productivity`` -- the test replays that
  exact overlay, not the real production wiring.
- :func:`build_player_context` -- port of ``RapmUtils.buildPlayerContext``
  (``RapmUtils.ts:427-541``). Filters out low-possession players
  (``removal_pct`` threshold), flags fully-removed lineups (``rapmRemove``,
  a side effect on the caller's own ``lineups`` list -- consumed by
  :func:`mbb_lineup_stats.calculate_aggregated_lineup_stats`, called
  immediately afterward on the same, now-flagged list), builds the
  ``player_to_col``/``col_to_player`` index, and folds ``prior_info`` via
  :func:`build_priors`.

**The ``filtered_lineups`` closure is ported as a Python callable, not a
materialized ``{"off": [...], "def": [...]}`` dict** (the brief's other
documented option). Rationale: the upstream ``filteredLineups`` field
(``RapmUtils.ts:516-519``) is itself a closure over a single,
eagerly-computed ``currFilteredLineupSet`` array (built once via the
``lineups.flatMap(...)`` pass, *before* the closure is created) -- it is not
recomputed per call, just re-filtered by ``off``/``def`` possession
truthiness on each invocation. A Python callable (``Callable[[str],
list[LineupStatSet]]``) preserves this exact "compute the removal pass once,
filter by prefix on demand" semantics with zero behavioral drift; a
pre-materialized two-key dict would be equivalent for *this* task's two
call-shapes (``"off"``/``"def"``) but would require re-deriving the
per-prefix filter eagerly instead of lazily and would diverge from the TS
source's own shape (a function, not a plain object) for anyone reading the
two side-by-side. ``RapmPlayerContext["filtered_lineups"]`` is therefore
typed ``Callable[[str], list[LineupStatSet]]`` and is fully picklable-unsafe
(as in TS) -- callers that need to serialize a ``RapmPlayerContext`` (e.g. to
JSON) must drop this key first, exactly as the upstream jest oracle itself
does (``_.omit(results, ["filteredLineups", "teamInfo"])`` before
``toMatchSnapshot()``).

**Known landmines (preserved for fidelity):**

1. **Python ``{}`` truthiness != JS ``{}`` truthiness.** ``build_priors``'s
   ``playersWeak``/``playersStrong`` map closures (``RapmUtils.ts:320-398``)
   read ``const stats = playersBaseline[player] || {}; if (stats) {...} else
   return {};`` -- since a JS plain object literal is *always* truthy (even
   ``{}``), the ``else`` branch is dead code upstream and the ``if`` branch
   *always* runs, defaulting every missing-baseline field to ``0`` via
   ``getVal``. A naive Python port gating on ``if stats:`` after ``stats =
   players_baseline.get(player) or {}`` would get this backwards -- an empty
   dict is falsy in Python, so it would silently divert every player absent
   from ``players_baseline`` to a bare ``{}`` result instead of the intended
   all-zero-defaulted feature dict (breaking, e.g., the oracle's expectation
   that a missing player's ``def_to`` is ``-0.01`` and ``def_ftr`` is
   ``-0.05``, not absent). This port therefore has **no** ``if stats:`` gate
   at all -- the computation always runs, matching the JS branch that always
   executes.
2. **Unguarded division** (matches upstream, no ``|| 1`` safety net):
   ``build_player_context``'s ``origPoss / totalLineups``
   (``RapmUtils.ts:460``) raises ``ZeroDivisionError`` in Python where JS
   would silently produce ``Infinity``/``NaN`` -- reachable only if
   ``total_lineups`` is exactly ``0`` (e.g. an empty ``players`` list, or a
   players list whose first entry carries zero possessions on every side),
   not exercised by this task's oracle fixtures.
3. **Falsy-coalesce on a *product*, not a per-factor guard:**
   ``removalThreshold = config.removalPct * totalLineups || 1``
   (``RapmUtils.ts:448``) coalesces the *whole product* to ``1`` when it is
   falsy (i.e. exactly ``0`` -- either factor being ``0`` suffices), not
   ``max(product, 1)``. Ported as ``removal_threshold = product if product
   else 1.0``.
4. **``get_prior_basis``'s ``disable_prior_basis = True`` branch is dead
   code, ported verbatim anyway** (``RapmUtils.ts:257-280``): the upstream
   function hardcodes the flag ``True`` and always takes the early
   ``return 0`` path; the (real, TS-authored) fallback math below it is
   preserved unreachable, in case a future upstream re-enable needs a 1:1
   Python counterpart already in place.
5. **``build_player_context`` mutates the caller's ``lineups`` list in
   place** (sets ``lineup["rapmRemove"] = True`` on every lineup composed
   *entirely* of removed players, ``RapmUtils.ts:478-491`` -- "THIS FLATMAP
   HAS SIDE-EFFECTS" per the upstream comment). This is deliberate and
   load-bearing: the very same function relies on it, calling
   :func:`mbb_lineup_stats.calculate_aggregated_lineup_stats` on the
   *same*, now-flagged ``lineups`` list immediately afterward to compute
   ``team_info`` (diverting fully-removed lineups into that function's
   ``all_lineups`` sub-accumulator instead of the main team total). Callers
   that need an unmutated copy of ``lineups`` must pass a deep copy in.
6. ``_.every`` on an empty JS array is vacuously ``True``; Python's
   ``all(())`` on an empty generator is likewise ``True`` -- a lineup with
   no resolvable ``players_array`` membership is therefore (correctly, and
   identically to upstream) treated as "composed entirely of removed
   players" and excluded from ``filtered_lineups``.

**Deliberately NOT ported (this task -- Tasks 3.4-3.6 own these):**

- ``buildWeakPriorFromRapm`` (``RapmUtils.ts:410-419``) -- the "recursive
  prior" helper, needed once ``calculatePredictedOut``/``calculateResidualError``
  land.
- ``RapmPreProcDiagnostics`` / ``RapmProcessingInputs`` / ``RapmInfo``
  (``RapmUtils.ts:187-216``) -- return-shape types for
  ``calcCollinearityDiag`` / ``pickRidgeRegression`` / the top-level
  orchestration glue, respectively; introduced alongside the functions that
  produce them.
- ``pickRidgeRegression``, ``injectRapmIntoPlayers``, ``calcCollinearityDiag``
  -- the ridge-regression-solve / collinearity-diagnostics surface, Tasks
  3.4-3.6.

**Task 3.3 coverage gap (inherited from upstream, not introduced by this
port):** neither ``calcLineupOutputs``'s own jest test nor the vendored
fixture it uses ever exercises a real ``value``/``old_value`` divergence --
`` lineupReport``'s lineups were built via ``insertOldValues``, which stamps
``old_value = value`` on every luck-affected field, so the
``useOldValIfPossible=[False, True]`` variant asserts byte-identical output
to the ``[False, False]`` default. This proves the value-key plumbing does
not crash, not that luck-adjustment changes the RAPM numbers correctly --
the luck-divergence math itself is validated by Phase 2's ``mbb_luck``
tests, not here. See ``tests/fixtures/hoop_explorer/README.md``'s
classification map, item 3, for the full accounting.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Literal, TypedDict

import numpy as np
from numpy.typing import NDArray

from sportsdataverse.mbb.mbb_lineup_stats import (
    LineupStatSet,
    _num,
    calculate_aggregated_lineup_stats,
)
from sportsdataverse.mbb.mbb_lineup_stats import _get_player_set as _lineup_get_player_set

#: ``ValueKey`` (``RapmUtils.ts:121``) -- selects whether prior/aggregation
#: math reads a stat's luck-adjusted ``old_value`` or its raw ``value``.
ValueKey = Literal["value", "old_value"]

#: Loosely-typed pass-through content aliases (same convention as
#: ``mbb_lineup_stats.LineupStatSet`` / ``mbb_ratings.ORtgDiagnostics``):
#: these are plain dict aliases for documentation, not enforced shapes.
PlayerOnOffStats = dict[str, Any]
IndivStatSet = dict[str, Any]
PureStatSet = dict[str, Any]
PlayerId = str


class RapmConfig(TypedDict):
    """Port of ``RapmConfig`` (``RapmUtils.ts:175-179``)."""

    #: ``-1`` for adaptive prior, or ``0``-``1`` for a fixed strong-prior weight.
    prior_mode: float
    removal_pct: float
    #: ``-1`` to calculate the ridge lambda, or a fixed ``0``-``1`` regression amount.
    fixed_regression: float


#: Verbatim from ``RapmUtils.ts:181-185`` (``defaultRapmConfig``). Read-only
#: by every caller of :func:`build_player_context` -- never mutated by this
#: module (mirrors the TS default, which is likewise never mutated by
#: ``buildPlayerContext``/``buildPriors``).
DEFAULT_RAPM_CONFIG: RapmConfig = {
    "prior_mode": -1,
    "removal_pct": 0.06,
    "fixed_regression": -1,
}


class RapmPriorInfo(TypedDict):
    """Port of ``RapmPriorInfo`` (``RapmUtils.ts:124-133``)."""

    strong_weight: float
    #: Allows the RAPM solve to diverge from the KenPom-derived prior entirely.
    no_weak_prior: bool
    #: If ``no_weak_prior``, use the initial RAPM to make up the KenPom shortfall.
    use_recursive_weak_prior: bool
    #: Only meaningful when ``unbias_weight > 0`` (unused as of this task).
    include_strong: dict[str, bool]
    players_strong: list[dict[str, float | None]]
    players_weak: list[dict[str, float]]
    #: Handles cases when the prior is close to ``0``.
    basis: dict[str, float]
    #: Whether luck-adjustment (``old_value``) was used for this prior.
    key_used: ValueKey


class RapmPlayerContext(TypedDict):
    """Port of ``RapmPlayerContext`` (``RapmUtils.ts:147-173``).

    See the module docstring for why ``filtered_lineups`` is a Python
    callable rather than a materialized dict.
    """

    #: If ``> 0``, an additional synthetic row is added with the desired
    #: final result (currently always ``0.0`` -- see :func:`build_player_context`).
    unbias_weight: float
    #: ``{player_id: [pct_of_total_poss, unused_legacy_zero, baseline_stat_doc]}``.
    removed_players: dict[str, list[Any]]
    #: The column index corresponding to each remaining player.
    player_to_col: dict[str, int]
    #: The player id in each column (index-aligned with the solve matrices).
    col_to_player: list[str]
    #: A shallow filter of the (side-effect-flagged) lineups, minus ones with
    #: a removed player or no off/def possessions -- see the module
    #: docstring's closure-shape note.
    filtered_lineups: Callable[[str], list[LineupStatSet]]
    #: An aggregated view of ``filtered_lineups``' underlying lineup set.
    team_info: LineupStatSet
    #: The D1-average efficiency this context was built against.
    avg_efficiency: float
    num_players: int
    num_off_lineups: int
    num_def_lineups: int
    off_lineup_poss: float
    def_lineup_poss: float
    prior_info: RapmPriorInfo
    config: RapmConfig


def _empty_indiv() -> IndivStatSet:
    """Port of ``StatModels.emptyIndiv()`` (referenced by ``RapmUtils.ts:463``).

    Returns a fresh dict each call -- callers must not share a single
    instance across multiple removed-player entries.
    """
    return {"key": "empty", "doc_count": 0}


def _get_val_for_key(field: Any, value_key: ValueKey) -> float:
    """Port of the ``buildPriors``-local ``getVal`` closure (``RapmUtils.ts:245-247``).

    ``field?.[valueKey] ?? field?.value`` (chosen via ``_.isNil``, so an
    explicit ``0`` at ``field[value_key]`` is honored, not just presence),
    then falsy-coalesced to ``0`` (JS ``... || 0``). This is a distinct
    falsy-coalesce shape from :func:`mbb_lineup_stats._num` (single fixed
    key) and :func:`mbb_ratings._nullish` (nullish, not falsy, single key)
    -- it picks between two candidate keys *before* the final falsy-coalesce.
    """
    if not isinstance(field, dict):
        return 0.0
    candidate = field.get(value_key)
    if candidate is None:
        candidate = field.get("value")
    return candidate if candidate else 0.0


def build_priors(
    players_baseline: dict[PlayerId, IndivStatSet],
    stats_averages: PureStatSet,
    avg_efficiency: float,
    col_to_player: list[str],
    prior_mode: float,
    value_key: ValueKey = "value",
) -> RapmPriorInfo:
    """Build strong/weak per-player RAPM priors for every column.

    Faithful port of ``RapmUtils.buildPriors`` (``RapmUtils.ts:237-407``).
    See the module docstring's landmine list, item 1, for the critical
    Python-vs-JS ``{}``-truthiness gotcha this function's implementation
    deliberately avoids.

    Args:
        players_baseline: ``{player_id: IndivStatSet}`` -- the most-general
            per-player baseline info (in production, sourced from
            ``mbb_ratings.build_productivity``'s output; see the module
            docstring's "RAPM prior source" note).
        stats_averages: League/context average stat set, used by the
            (currently dead-code, see landmine 4) ``get_prior_basis``
            fallback and by ``with_avg_or_undef``'s nil-check gate.
        avg_efficiency: League/context average efficiency.
        col_to_player: The player ids, in column order -- ``playersStrong``/
            ``playersWeak`` are index-aligned with this list.
        prior_mode: ``-1`` for adaptive mode, ``-2`` (or lower) for no prior,
            ``0``-``1`` for a fixed strong-prior weight.
        value_key: ``"value"`` or ``"old_value"`` -- allows priors to be
            built from luck-adjusted parameters.

    Returns:
        A :class:`RapmPriorInfo`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_rapm import build_priors

            priors = build_priors({}, {}, 100.0, ["Wiggins, Aaron"], -1)
            print(priors["players_weak"][0])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """

    def get_val(field: Any) -> float:
        return _get_val_for_key(field, value_key)

    no_weak_prior = prior_mode < -1.5

    def get_prior_basis(off_or_def: str) -> float:
        """Port of ``getPriorBasis`` (``RapmUtils.ts:256-281``) -- see
        landmine 4, this function's real math is currently unreachable
        because ``disable_prior_basis`` is hardcoded ``True`` upstream.
        """
        disable_prior_basis = True
        if disable_prior_basis:
            return 0.0
        prior_sum = sum(
            get_val(stats.get(f"{off_or_def}_adj_rtg")) * get_val(stats.get(f"{off_or_def}_team_poss_pct"))
            for stats in players_baseline.values()
        )
        tie_goes_pve_iff_off = prior_sum == 0 and off_or_def == "off"
        if prior_sum > 0 or tie_goes_pve_iff_off:
            # (reduce to lower "replacement value" for offense, raise the bar for defense)
            return max(3.0 - prior_sum, 0.0) * 0.2
        # (opposite)
        return min(0.0, -3.0 - prior_sum) * 0.2

    off_basis = 0.0 if no_weak_prior else get_prior_basis("off")
    def_basis = 0.0 if no_weak_prior else get_prior_basis("def")

    def with_avg_or_undef(
        field: str,
        stats: PureStatSet,
        fn: Callable[[float, float], float],
        avg_weight: float = 1.0,
    ) -> float | None:
        """Port of ``withAvgOrUndef`` (``RapmUtils.ts:286-299``)."""
        if stats_averages.get(field) is None or stats.get(field) is None:
            return None
        avg = get_val(stats_averages.get(field))
        player = get_val(stats.get(field))
        return fn(avg * avg_weight, player)

    def default_prior(
        field: str,
        stats: PureStatSet,
        usage: float,
        avg_weight: float = 1.0,
    ) -> float | None:
        """Port of ``defaultPrior`` (``RapmUtils.ts:300-312``)."""
        return with_avg_or_undef(field, stats, lambda avg, player: (player - avg) * usage, avg_weight)

    # NOTE (landmine 1): no `if stats:` gate below -- see module docstring.
    # `stats = players_baseline.get(player) or {}` is always a dict, and the
    # upstream JS equivalent (`playersBaseline[player] || {}`) is *always*
    # truthy, so the JS `if (stats) {...} else return {}` always takes the
    # true branch -- this port always runs the computation to match.
    players_weak: list[dict[str, float]] = []
    for player in col_to_player:
        stats = players_baseline.get(player) or {}
        o_rtg_factor = avg_efficiency / (get_val(stats.get("def_adj_opp")) or avg_efficiency)
        off_usage = get_val(stats.get("off_usage"))
        alt_off_rating = off_usage * (get_val(stats.get("off_rtg")) * o_rtg_factor - avg_efficiency)
        players_weak.append(
            {
                "off_adj_ppp": get_val(stats.get("off_adj_rtg")) + off_basis,
                "def_adj_ppp": get_val(stats.get("def_adj_rtg")) + def_basis,
                "off_adj_ppp_alt": alt_off_rating + off_basis,
                "off_usage": off_usage,
            }
        )

    players_strong: list[dict[str, float | None]] = []
    for player in col_to_player:
        stats = players_baseline.get(player) or {}
        off_usage = get_val(stats.get("off_usage"))
        raw_efg = default_prior("off_efg", stats, off_usage)
        players_strong.append(
            {
                "off_adj_ppp": get_val(stats.get("off_adj_rtg")) + off_basis,
                # THESE PRIORS ARE VERY ARBITRARY -- "directionally correct" only (upstream comment):
                "off_efg": (raw_efg + max(0.0, get_val(stats.get("off_assist")) - 0.15) * 0.08 if raw_efg else None),
                "off_to": default_prior("off_to", stats, off_usage),
                # (arbitrary chosen average steal rate)
                "def_to": get_val(stats.get("def_to")) - 0.01,
                # (semi-arbitrary weights to avoid this prior being too strong)
                "off_orb": default_prior("off_orb", stats, 0.25, 0.2),
                "off_ftr": default_prior("off_ftr", stats, 0.5 * off_usage),
                "def_orb": default_prior("def_orb", stats, -0.2, 0.25),
                # (e.g. 5 fouls (==0.05) / 50 -> ~0.1 increase in FTR)
                "def_ftr": (get_val(stats.get("def_ftr")) - 0.025) * 2.0,
                # peripherals:
                "off_assist": default_prior("off_assist", stats, off_usage, 0.2),
                "off_3pr": default_prior("off_3pr", stats, off_usage),
                "off_2pmidr": default_prior("off_2pmidr", stats, off_usage),
                "off_2primr": default_prior("off_2primr", stats, off_usage),
                # shot making:
                "off_3p": default_prior("off_3p", stats, off_usage * get_val(stats.get("off_3pr"))),
                "off_2p": default_prior("off_2p", stats, off_usage * (1.0 - get_val(stats.get("off_3pr")))),
                "off_2pmid": default_prior("off_2pmid", stats, off_usage * get_val(stats.get("off_2pmid"))),
                "off_2prim": default_prior("off_2prim", stats, off_usage * get_val(stats.get("off_2prim"))),
            }
        )

    return {
        "include_strong": {},
        "strong_weight": 0.0 if no_weak_prior else prior_mode,
        "no_weak_prior": no_weak_prior,
        "use_recursive_weak_prior": prior_mode < -2.5,
        "players_weak": players_weak,
        "players_strong": players_strong,
        "key_used": value_key,
        "basis": {"off": off_basis, "def": def_basis},
    }


def build_player_context(
    players: list[PlayerOnOffStats],
    lineups: list[LineupStatSet],
    players_baseline: dict[PlayerId, IndivStatSet],
    stats_averages: PureStatSet,
    avg_efficiency: float,
    agg_value_key: ValueKey = "value",
    config: RapmConfig = DEFAULT_RAPM_CONFIG,
) -> RapmPlayerContext:
    """Build the context object the RAPM matrix-solve layer consumes.

    Faithful port of ``RapmUtils.buildPlayerContext`` (``RapmUtils.ts:427-541``).
    Removes low-possession players (``config["removal_pct"]`` of total
    on+off possessions), flags fully-removed lineups (mutating ``lineups``
    in place -- see the module docstring's landmine 5), builds the
    player-to-column index, and folds :func:`build_priors` into
    ``prior_info``.

    Args:
        players: The per-player on/off splits (``PlayerOnOffStats``), e.g.
            ``mbb_lineup_stats.lineup_to_team_report(...)["players"]``.
        lineups: The per-lineup ``LineupStatSet`` docs feeding this team's
            aggregate (**mutated in place** -- see landmine 5).
        players_baseline: ``{player_id: IndivStatSet}`` -- forwarded to
            :func:`build_priors` unchanged.
        stats_averages: League/context average stat set -- forwarded to
            :func:`build_priors` unchanged.
        avg_efficiency: League/context average efficiency.
        agg_value_key: ``"value"`` or ``"old_value"`` -- forwarded to
            :func:`build_priors` as its ``value_key`` (only affects prior
            calculations, not the lineup-filtering/aggregation above it).
        config: Removal-percent / prior-mode / regression config. Defaults
            to :data:`DEFAULT_RAPM_CONFIG`; never mutated by this function
            (only ``config["removal_pct"]``/``config["prior_mode"]`` are
            read), matching the TS default parameter's own read-only usage.

    Returns:
        A :class:`RapmPlayerContext`.

    Raises:
        ZeroDivisionError: If ``total_lineups`` (derived from ``players[0]``'s
            on/off possession totals) is exactly ``0`` -- see the module
            docstring's landmine 2. Not exercised by this task's oracle
            fixtures.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report
            from sportsdataverse.mbb.mbb_rapm import build_player_context, DEFAULT_RAPM_CONFIG

            report = lineup_to_team_report({"lineups": buckets, "error_code": None})
            ctx = build_player_context(
                report["players"], buckets, {}, {}, 100.0, "value", DEFAULT_RAPM_CONFIG
            )
            print(ctx["num_players"], ctx["team_info"]["off_poss"]["value"])

        Filtering lineups by side (the ``filtered_lineups`` closure)::

            off_lineups = ctx["filtered_lineups"]("off")
            def_lineups = ctx["filtered_lineups"]("def")

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    # The `unbiasWeight` constructor param was removed upstream (RapmUtils.ts:435-436
    # "REMOVED CODE" comment) -- it is now always this hardcoded local, not a
    # function parameter.
    unbias_weight = 0.0

    first_on = (players[0].get("on") if players else None) or {}
    first_off = (players[0].get("off") if players else None) or {}
    total_lineups = (
        _num(first_on, "off_poss", 0.0)
        + _num(first_off, "off_poss", 0.0)
        + _num(first_on, "def_poss", 0.0)
        + _num(first_off, "def_poss", 0.0)
    )
    # NOTE (landmine 3): falsy-coalesce on the *product*, not a per-factor guard.
    removal_product = config["removal_pct"] * total_lineups
    removal_threshold = removal_product if removal_product else 1.0

    removed_players: dict[str, list[Any]] = {}
    for p in players:
        player_id = p.get("playerId") or ""
        on = p.get("on") or {}
        # NOTE (landmine 2): unguarded division, matches upstream -- see module docstring.
        orig_poss = _num(on, "off_poss", 0.0) + _num(on, "def_poss", 0.0)
        if orig_poss < removal_threshold:
            # NOTE: `is None` (not `or`) -- `playersBaseline[playerId] ||
            # StatModels.emptyIndiv()` in JS only substitutes on
            # missing/null/undefined, and an *empty* dict is truthy in JS
            # (unlike Python's falsy `{}`). Using `or` here would wrongly
            # discard a legitimately-empty-but-present baseline entry.
            baseline = players_baseline.get(player_id)
            if baseline is None:
                baseline = _empty_indiv()
            removed_players[player_id] = [
                orig_poss / total_lineups,
                0.0,  # (unused now)
                {
                    **baseline,
                    "off_poss": on.get("off_poss") or {},
                    "def_poss": on.get("def_poss") or {},
                },
            ]

    # Now go through lineups, dropping any composed entirely of removed
    # players -- THIS LOOP HAS SIDE EFFECTS: it stamps `rapmRemove` onto the
    # caller's own lineup dicts (see module docstring landmine 5), which
    # `calculate_aggregated_lineup_stats(lineups)` below relies on.
    curr_filtered_lineup_set: list[LineupStatSet] = []
    for lineup in lineups:
        lineup_player_ids = _lineup_get_player_set(lineup).keys()
        should_remove_lineup = all(pid in removed_players for pid in lineup_player_ids)
        if should_remove_lineup:
            lineup["rapmRemove"] = True
        else:
            curr_filtered_lineup_set.append(lineup)

    # Calculate the aggregated team stats (note: includes pre-luck-adjusted
    # stats if the main stats are luck adjusted) -- must run AFTER the
    # rapmRemove-flagging loop above, on the same (now-flagged) `lineups`.
    team_info = calculate_aggregated_lineup_stats(lineups)

    def _player_id(p: PlayerOnOffStats) -> str:
        return p.get("playerId") or ""

    sorted_player_ids = [
        _player_id(p)
        for p in sorted(
            (p for p in players if _player_id(p) not in removed_players),
            key=lambda p: -_num(p.get("on") or {}, "off_poss", 0.0),
        )
    ]

    def filtered_lineups(prefix: str) -> list[LineupStatSet]:
        """Port of the ``filteredLineups`` closure (``RapmUtils.ts:516-519``)
        -- see the module docstring's closure-shape note.
        """
        return [lineup for lineup in curr_filtered_lineup_set if _num(lineup, f"{prefix}_poss", 0.0)]

    num_off_lineups = len(filtered_lineups("off"))
    num_def_lineups = len(filtered_lineups("def"))

    return {
        "unbias_weight": unbias_weight,
        "removed_players": removed_players,
        "player_to_col": {pid: idx for idx, pid in enumerate(sorted_player_ids)},
        "col_to_player": sorted_player_ids,
        "filtered_lineups": filtered_lineups,
        "team_info": team_info,
        "avg_efficiency": avg_efficiency,
        "num_players": len(sorted_player_ids),
        "num_off_lineups": num_off_lineups,
        "num_def_lineups": num_def_lineups,
        "off_lineup_poss": _num(team_info, "off_poss", 0.0),
        "def_lineup_poss": _num(team_info, "def_poss", 0.0),
        "prior_info": build_priors(
            players_baseline,
            stats_averages,
            avg_efficiency,
            sorted_player_ids,
            config["prior_mode"],
            agg_value_key,
        ),
        "config": config,
    }


def _get_strong_weight(prior: RapmPriorInfo, maybe_adaptive_fallback: float | None) -> float:
    """Port of ``getStrongWeight`` (``RapmUtils.ts:135-144``).

    A fixed ``prior["strong_weight"]`` (``>= 0``, set via ``config["prior_mode"]``)
    always wins; only when it's in adaptive mode (``< 0``, the sentinel for
    "adaptive strong prior") does the caller-supplied per-player
    ``maybe_adaptive_fallback`` (falsy-coalesced to ``0.0``) apply.
    """
    if prior["strong_weight"] >= 0:
        return prior["strong_weight"]
    return maybe_adaptive_fallback if maybe_adaptive_fallback else 0.0


def calc_player_weights(ctx: RapmPlayerContext) -> list[NDArray[np.float64]]:
    """Build the off/def player-weight (design) matrices for the RAPM solve.

    Faithful port of ``RapmUtils.calcPlayerWeights`` (``RapmUtils.ts:544-595``).
    One row per (filtered) lineup, one column per remaining player; each
    filled cell is ``sqrt(lineup_possessions / total_side_possessions)`` --
    the possession-weighted design-matrix entry the ridge regression (Task
    3.4) solves against. This is the first function in the module where a
    ``dict``-shaped ``RapmPlayerContext`` gets materialized into a
    ``numpy.ndarray`` -- see the module docstring's "dict -> ``numpy.ndarray``
    boundary" note.

    Args:
        ctx: A :class:`RapmPlayerContext`, e.g. from :func:`build_player_context`.

    Returns:
        ``[off_weights, def_weights]`` -- two ``numpy.ndarray`` matrices of
        shape ``(num_{off,def}_lineups [+1 if ctx["unbias_weight"] > 0],
        ctx["num_players"])``. The optional extra row (only emitted when
        ``ctx["unbias_weight"] > 0`` -- always ``0.0`` in production per
        :func:`build_player_context`'s hardcoded local, but settable directly
        on the returned context dict, as the oracle test does) holds each
        column's ``unbias_weight``-scaled sum-of-squares, an "unbiasing
        observation" row (``RapmUtils.ts:578-593``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_rapm import calc_player_weights

            off_weights, def_weights = calc_player_weights(ctx)
            print(off_weights.shape)  # (num_off_lineups, num_players)

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    extra = ctx["unbias_weight"] > 0
    off_weights: NDArray[np.float64] = np.zeros((ctx["num_off_lineups"] + (1 if extra else 0), ctx["num_players"]))
    def_weights: NDArray[np.float64] = np.zeros((ctx["num_def_lineups"] + (1 if extra else 0), ctx["num_players"]))

    def populate_matrix(in_matrix: NDArray[np.float64], prefix: str) -> None:
        """Port of the ``populateMatrix`` closure (``RapmUtils.ts:558-573``)."""
        lineup_poss_count = (ctx["off_lineup_poss"] if prefix == "off" else ctx["def_lineup_poss"]) or 1.0
        for index, lineup in enumerate(ctx["filtered_lineups"](prefix)):
            poss_count = _num(lineup, f"{prefix}_poss", 0.0)
            poss_count_weight = math.sqrt(poss_count / lineup_poss_count)
            for player_id in _lineup_get_player_set(lineup):
                player_index = ctx["player_to_col"].get(player_id)
                if player_index is not None and player_index >= 0:
                    in_matrix[index, player_index] = poss_count_weight
                # (else this player is filtered out so ignore -- we'll just use their adj_rtg)

    populate_matrix(off_weights, "off")
    populate_matrix(def_weights, "def")

    # Add the possession %s for each player (RapmUtils.ts:578-593) -- an
    # "unbiasing observation" row, unreachable in production (unbias_weight
    # is hardcoded 0.0 in build_player_context) but exercised by the oracle
    # test, which sets ctx["unbias_weight"] directly on the returned context.
    if ctx["unbias_weight"] > 0:

        def add_extra_row(in_matrix: NDArray[np.float64], prefix: str) -> None:
            bottom_row = ctx["num_off_lineups"] if prefix == "off" else ctx["num_def_lineups"]
            # The extra row itself is still all-zero at this point, so
            # summing over every row (rather than just the populated ones)
            # is equivalent to the TS transpose-then-sum, which likewise
            # includes the (zero) extra row in its column sums.
            in_matrix[bottom_row, :] = ctx["unbias_weight"] * np.sum(in_matrix**2, axis=0)

        add_extra_row(off_weights, "off")
        add_extra_row(def_weights, "def")

    return [off_weights, def_weights]


def calc_lineup_outputs(
    field: str,
    off_offset: float,
    def_offset: float,
    ctx: RapmPlayerContext,
    adaptive_correl_weights: list[float] | None = None,
    use_old_val_if_possible: tuple[bool, bool] = (False, False),
) -> list[NDArray[np.float64]]:
    """Build the off/def target vectors the RAPM design matrices are fit against.

    Faithful port of ``RapmUtils.calcLineupOutputs`` (``RapmUtils.ts:598-751``).
    For each filtered lineup, computes a possession-weighted residual: the
    lineup's own stat value, plus any global luck adjustment, minus the
    accumulated "prior offset" contributed by every player on the lineup
    (a strong-prior blend for kept players -- see :func:`_get_strong_weight`
    -- or a fixed baseline contribution for removed players).

    Upstream keeps this as a plain ``Array<Array<number>>`` (*not* a mathjs
    ``Matrix``, unlike :func:`calc_player_weights`'s ``offWeights``/
    ``defWeights`` -- ``RapmUtils.test.ts``'s own ``tidyResults`` helper for
    this function has a visibly different shape, see the classification map
    in ``tests/fixtures/hoop_explorer/README.md``). This port still
    materializes both output vectors as ``numpy.ndarray`` for consistency
    with :func:`calc_player_weights` at the same dict -> array boundary --
    Task 3.4's ridge-regression solve consumes both as arrays regardless of
    the upstream distinction.

    Args:
        field: The stat suffix to read off each lineup, e.g. ``"adj_ppp"``
            (read as ``{prefix}_{field}``, e.g. ``"off_adj_ppp"``).
        off_offset: The D1-average offensive value for ``field`` (the
            regression's starting/baseline value on the RHS).
        def_offset: The D1-average defensive value for ``field``.
        ctx: A :class:`RapmPlayerContext`, e.g. from :func:`build_player_context`.
        adaptive_correl_weights: Optional per-player adaptive-correlation
            weights (index-aligned with ``ctx["col_to_player"]``), used as
            the strong-prior blend fallback when ``ctx["prior_info"]
            ["strong_weight"] < 0`` -- see :func:`_get_strong_weight`.
        use_old_val_if_possible: ``(use_old_val_for_off, use_old_val_for_def)``
            -- whether to prefer each lineup/team stat's luck-adjusted
            ``old_value`` over its raw ``value`` when present. This is the
            luck-adjustment hook Task 3.1's classification map flags as an
            **inherited coverage gap**: the vendored oracle fixture has
            ``old_value == value`` on every field (via ``insertOldValues``),
            so neither jest nor this port's replay test ever observes this
            flag change the resulting numbers -- only that passing it
            doesn't crash. See the module docstring's "Task 3.3 coverage
            gap" note.

    Returns:
        ``[off_outputs, def_outputs]`` -- two 1-D ``numpy.ndarray`` target
        vectors, index-aligned with ``ctx["filtered_lineups"]("off"/"def")``
        (plus one extra element each when ``ctx["unbias_weight"] > 0``, an
        "unbiasing observation" target -- always unreached in production,
        same as :func:`calc_player_weights`'s extra row).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_rapm import calc_lineup_outputs

            off_outputs, def_outputs = calc_lineup_outputs(
                "adj_ppp", 100.0, 100.0, ctx
            )
            print(off_outputs.shape)  # (num_off_lineups,)

        Luck-adjusted variant (reads ``old_value`` where present)::

            off_luck, def_luck = calc_lineup_outputs(
                "adj_ppp", 100.0, 100.0, ctx, use_old_val_if_possible=(True, True)
            )

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """

    def get_off_val(o: Any) -> float:
        return _get_val_for_key(o, "old_value" if use_old_val_if_possible[0] else "value")

    def get_def_val(o: Any) -> float:
        return _get_val_for_key(o, "old_value" if use_old_val_if_possible[1] else "value")

    get_val: dict[str, Callable[[Any], float]] = {"off": get_off_val, "def": get_def_val}
    offsets = {"off": off_offset, "def": def_offset}

    def do_global_luck_adj(off_or_def: str) -> float:
        """Port of the ``doGlobalLuckAdj`` closure (``RapmUtils.ts:625-646``)."""
        if field != "adj_ppp":
            return 0.0
        use_old_val = use_old_val_if_possible[0] if off_or_def == "off" else use_old_val_if_possible[1]
        if not use_old_val or ctx["prior_info"]["key_used"] != "value":
            return 0.0
        all_lineups = ctx["team_info"].get("all_lineups") or {}
        field_stat = all_lineups.get(f"{off_or_def}_{field}") or {}
        if not isinstance(field_stat, dict) or field_stat.get("old_value") is None:
            return 0.0
        return (field_stat.get("value") or 0.0) - (field_stat.get("old_value") or 0.0)

    global_luck_adj_offsets = {"off": do_global_luck_adj("off"), "def": do_global_luck_adj("def")}

    def calculate_vector(prefix: str) -> list[float]:
        """Port of the ``calculateVector`` closure (``RapmUtils.ts:651-726``)."""
        lineup_poss_count = (ctx["off_lineup_poss"] if prefix == "off" else ctx["def_lineup_poss"]) or 1.0
        out: list[float] = []
        for lineup in ctx["filtered_lineups"](prefix):
            poss_count = _num(lineup, f"{prefix}_poss", 0.0)
            poss_count_weight = math.sqrt(poss_count / lineup_poss_count)
            val = get_val[prefix](lineup.get(f"{prefix}_{field}"))

            prior_offset = offsets[prefix]
            for player_id in _lineup_get_player_set(lineup):
                player_index = ctx["player_to_col"].get(player_id)
                if player_index is not None and player_index >= 0:
                    # (case 1: a kept/rotation player -- blend in their strong prior)
                    strong_weight = _get_strong_weight(
                        ctx["prior_info"],
                        adaptive_correl_weights[player_index] if adaptive_correl_weights is not None else None,
                    )
                    strong_val = ctx["prior_info"]["players_strong"][player_index].get(f"{prefix}_{field}") or 0.0
                    prior_offset += strong_weight * strong_val
                else:
                    # (case 2: a removed player -- use their fixed baseline
                    # contribution on the RHS instead)
                    removed_player_info = ctx["removed_players"].get(player_id)
                    if removed_player_info is not None:
                        removed_player_stat = removed_player_info[2] or {}
                        # (temp overrides so shot-rate fields sum to 1 for removed
                        # players -- restored via the `del`s below, matching the
                        # upstream "avoid mutating the removedPlayerStat" comment)
                        removed_player_stat["def_3pr"] = {"value": 0.33}
                        removed_player_stat["def_2primr"] = {"value": 0.33}
                        removed_player_stat["def_2pmidr"] = {"value": 0.33}
                        if field == "adj_ppp":
                            removed_above_mean = (
                                get_val[prefix](removed_player_stat.get(f"{prefix}_adj_rtg"))
                                + ctx["prior_info"]["basis"][prefix]
                            )
                        else:
                            removed_above_mean = (
                                get_val[prefix](removed_player_stat.get(f"{prefix}_{field}")) - offsets[prefix]
                            )
                        del removed_player_stat["def_3pr"]
                        del removed_player_stat["def_2primr"]
                        del removed_player_stat["def_2pmidr"]
                        prior_offset += removed_above_mean
                    # (else exception case -- no-op, matches TS `return acc`)

            out.append((val + global_luck_adj_offsets[prefix] - prior_offset) * poss_count_weight)
        return out

    extra = ctx["unbias_weight"] > 0

    def build_side(prefix: str) -> NDArray[np.float64]:
        vector = calculate_vector(prefix)
        if extra:
            if ctx["prior_info"]["include_strong"].get(f"{prefix}_{field}"):
                vector = vector + [0.0]
            else:
                team_val = get_val[prefix](ctx["team_info"].get(f"{prefix}_{field}"))
                vector = vector + [ctx["unbias_weight"] * (team_val - offsets[prefix])]
        return np.array(vector, dtype=np.float64)

    return [build_side("off"), build_side("def")]
