"""KenPom-style strength-of-schedule + home-court-advantage adjustment engine.

Faithful port of hoop-explorer's ``buildStrengthAdjustedStats``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/bin/buildStrengthAdjustedStats.ts``, 677 LOC). Task 5f.4 (Phase 5f)
ports the **numerical engine + compute assembly**, NOT the CLI/file-I/O:
constants, ``fieldKeys``, ``getPerGameRaw``, ``getGameWeight``,
``computePossessionSplits``, ``computeLeagueAveragesFromPerGame``,
``getTeamRawFromPerGame``, ``computeOpponentStrengths``, the
``runIterativeAdjustmentWithHCA`` solver, and the COMPUTE half of ``main()``
(re-exposed as :func:`build_strength_adjusted_stats`). The upstream is an
oracle-less CLI script (no jest); this port's tests are hand-derived from the
TS on paper, with the arithmetic documented inline for reviewer re-derivation.

For each of four per-game shooting rates (``efg``/``3p``/``2pmid``/``2prim``)
the engine iterates a KenPom-style per-game adjustment
``Adj_game = Raw_game * (league / (OppAdj +/- HCA))`` to convergence, then
re-estimates a per-stat home-court advantage from home/away possession-
imbalance residuals.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``buildStrengthAdjustedStats.ts`` from
`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
(the hoop-explorer.com SPA), which is licensed under the Apache License,
Version 2.0 (the upstream repo's ``LICENSE`` file; full text at
`<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section 4's
redistribution-of-derivative-works obligations, sportsdataverse-py (itself
MIT-licensed) retains the upstream copyright notice for this derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full third-party
attribution entry (upstream URL, license, and exactly what was derived).

**Data model.** Input teams are the loose ``team_details`` JSON shape
(``dict[str, Any]``): a team is ``{team_name, conf, opponents: [...]}`` and
each opponent row is one game carrying ``off_poss``/``def_poss``/
``location_type``/``team_scored``/``oppo_scored`` plus the per-side shot
counters ``{off,def}_{2pmid,2prim,3p}_{attempts,made}``. The TS reads these
via dynamic key access (``opp[`${pre}2pmid_attempts`]``); this port mirrors
that with ``dict.get`` + the JS-nullish ``?? 0`` folded to ``v if v is not
None else 0``. The **result** is returned as frozen dataclasses
(:class:`StrengthAdjustedResult`, :class:`TeamStrengthAdjusted`,
:class:`FieldAverage`), whose innermost per-field ``{off, def}`` value stays a
plain ``dict[str, float]`` -- keeping the TS-verbatim ``"off"``/``"def"`` keys
(``def`` is a Python keyword, so a dataclass field could not carry that name).

**Landmines preserved verbatim from the TS** (see the task report for the
full accounting):

- **Cross-guard on the per-game adjustment.** The OFF branch
  (``adjOffG = rawOffG * league_off / denomOff``, ``ts:385``) is gated on
  ``league_def > 0`` -- the *opposite* side's league average -- and the DEF
  branch on ``league_off > 0`` (``ts:388``). This looks like an upstream typo
  but is preserved exactly (never "fixed").
- **Asymmetric HCA residual prediction.** ``predOff`` multiplies by
  ``avg_opp_def / league_def`` but ``predDef`` multiplies by
  ``league_off / avg_opp_off`` -- the ratio inverts between sides
  (``ts:463-470``). Preserved.
- **Cross-named opponent strengths.** In
  :func:`compute_opponent_strengths`, ``avg_opp_def`` is weighted by the
  *offensive* game weights and reads each opponent's ``def`` adjustment,
  while ``avg_opp_off`` is weighted by *defensive* weights and reads the
  opponent's ``off`` (``ts:283-296``). Preserved.
- **"Keep current value" fallback.** In the team sweep, a field with no valid
  games (``sumW == 0``) keeps the team's *current* adjusted value, NOT 0
  (``ts:408-409``).
- **Nullish ``?? 0`` vs. the ``att <= 0 -> None`` guard.** A missing stat key
  reads as 0, but a game whose relevant attempts total 0 yields ``None`` (the
  game is skipped in the weighted mean), NOT a 0-rate. These are distinct.
- **``adj_hca`` sign.** ``off = adj.off + hca_off``; ``def = adj.def -
  hca_def`` (``ts:638-639``).

**Dead / I/O-only upstream code deliberately NOT ported** (see the report):

- ``getStatValue`` (``ts:64``) -- never called anywhere in the file.
- Inside ``computeOpponentStrengths`` (``ts:264``) the TS destructures
  ``const { off: offKey, def: defKey } = fieldKeys(field)`` but never
  references ``offKey``/``defKey`` -- a dead destructure, omitted here as a
  no-op (:func:`field_keys` itself is still ported, per the task spec).
- All ``console.log`` / ``--debug`` blocks (``debugContribs``/
  ``debugTeamName``/``debugField``) -- pure stdout, zero effect on returned
  values.
- ``main()``'s ``fs`` reads, ``process.argv`` parsing, ``dataLastUpdated``,
  ``BatchMiscUtils.reduceNumberSize``, the ``lastUpdated``/``gender``/``year``
  output wrapper, and the ``JSON.stringify`` non-finite-to-``null`` replacer --
  all serialization / file glue with no bearing on the numbers.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, NamedTuple, Optional, Sequence

__all__ = [
    "STRENGTH_ADJUSTED_FIELDS",
    "MAX_ITERATIONS",
    "TOLERANCE",
    "IMBALANCE_MIN",
    "PossessionSplits",
    "FieldAverage",
    "TeamStrengthAdjusted",
    "StrengthAdjustedResult",
    "IterationResult",
    "field_keys",
    "get_per_game_raw",
    "get_game_weight",
    "compute_possession_splits",
    "compute_league_averages_from_per_game",
    "get_team_raw_from_per_game",
    "compute_opponent_strengths",
    "run_iterative_adjustment_with_hca",
    "build_strength_adjusted_stats",
]

#: The four per-game shooting rates the engine adjusts (``ts:23``). ``TIERS``
#: (``ts:22``) is CLI-only (which tier files to read) and is intentionally not
#: ported.
STRENGTH_ADJUSTED_FIELDS: tuple[str, ...] = ("efg", "3p", "2pmid", "2prim")

#: Solver iteration cap (``ts:24``). A TS module const; exposed as a keyword
#: arg on :func:`run_iterative_adjustment_with_hca` /
#: :func:`build_strength_adjusted_stats` so tests can pin a single iteration.
MAX_ITERATIONS: int = 100

#: Convergence tolerance on the max per-team/field change (``ts:25``).
TOLERANCE: float = 1e-6

#: Minimum absolute home/away possession imbalance for a team to contribute to
#: the HCA residual estimate (``ts:26``).
IMBALANCE_MIN: float = 1e-6

# Loose-JSON aliases (the ``team_details`` shape) -- dynamic key access, per
# the TS ``opp[`${pre}...`]`` / ``team.opponents`` reads.
TeamDetail = dict[str, Any]
OpponentGame = dict[str, Any]

# ``{"off": float, "def": float}`` / ``{"league_off": ..., "league_def": ...}``
# / ``{"hca_off": ..., "hca_def": ...}`` -- the innermost per-side value bags,
# kept as dicts to preserve the TS-verbatim ``off``/``def`` keys (``def`` is a
# Python keyword and cannot be a dataclass field name).
SideValues = dict[str, float]
FieldSideMap = dict[str, SideValues]
AdjValues = dict[str, FieldSideMap]
LeagueAverages = dict[str, SideValues]
HcaPerField = dict[str, SideValues]


@dataclass(frozen=True)
class PossessionSplits:
    """Home/away/neutral possession totals for one team (``ts:143-152``).

    Off and def possessions are bucketed by the game's ``location_type``
    (missing -> ``"Neutral"``). The HCA residual step reads the off/def
    imbalance ``(home - away) / total`` off these totals.
    """

    home_off_poss: float
    away_off_poss: float
    neutral_off_poss: float
    total_off_poss: float
    home_def_poss: float
    away_def_poss: float
    neutral_def_poss: float
    total_def_poss: float


@dataclass(frozen=True)
class FieldAverage:
    """League average + estimated HCA for one stat field (``ts:620-625``).

    ``league_off``/``league_def`` are the possession-weighted league means of
    the per-game raw rate; ``hca_off``/``hca_def`` are the residual-derived
    home-court advantages the solver converged on.
    """

    league_off: float
    league_def: float
    hca_off: float
    hca_def: float


@dataclass(frozen=True)
class TeamStrengthAdjusted:
    """One team's raw / adjusted / HCA-adjusted rates (``ts:642-648``).

    Each of ``raw`` / ``adj`` / ``adj_hca`` maps a stat field
    (``efg``/``3p``/``2pmid``/``2prim``) to a ``{"off": float, "def": float}``
    dict. ``adj`` is the strength-of-schedule-adjusted value; ``adj_hca`` adds
    the home-court term (``off + hca_off``, ``def - hca_def``).
    """

    team_name: str
    conf: str
    raw: FieldSideMap
    adj: FieldSideMap
    adj_hca: FieldSideMap


@dataclass(frozen=True)
class StrengthAdjustedResult:
    """The compute output of :func:`build_strength_adjusted_stats`.

    Mirrors ``main()``'s ``{ averages, teams }`` object (``ts:656-662``) minus
    the ``lastUpdated``/``gender``/``year`` serialization wrapper.
    """

    averages: dict[str, FieldAverage]
    teams: list[TeamStrengthAdjusted]


class IterationResult(NamedTuple):
    """Return of :func:`run_iterative_adjustment_with_hca` (``ts:314-317``).

    ``adj_values`` maps ``team_name -> field -> {"off","def"}`` (the converged
    strength-of-schedule adjustment); ``hca_per_field`` maps ``field ->
    {"hca_off","hca_def"}``.
    """

    adj_values: AdjValues
    hca_per_field: HcaPerField


def _num(source: OpponentGame, key: str) -> float:
    """JS nullish read ``source[key] ?? 0`` as a float.

    A missing key (JS ``undefined``) folds to ``0.0``; a present ``0`` stays
    ``0.0`` (``??`` treats only ``null``/``undefined`` as nullish, never a
    real zero -- distinct from a Python-``or`` fallback).
    """
    value = source.get(key)
    return float(value) if value is not None else 0.0


def _opponents(team: TeamDetail) -> list[OpponentGame]:
    """The team's opponent games, or ``[]`` (TS ``team.opponents ?? []`` /
    ``team.opponents || []`` -- both collapse a missing list to empty)."""
    opps = team.get("opponents")
    return list(opps) if opps is not None else []


def _adj_side(field_map: Optional[FieldSideMap], field: str, side: str) -> Optional[float]:
    """Optional-chain ``oppAdj?.[field]?.[side]`` -> the float, or ``None``.

    Returns ``None`` if the adj map is absent or lacks the field/side entry
    (JS ``undefined`` at any link); otherwise the stored value (which may be a
    legitimate ``0.0`` and must NOT be coalesced away).
    """
    if field_map is None:
        return None
    entry = field_map.get(field)
    if entry is None:
        return None
    return entry.get(side)


def field_keys(field: str) -> dict[str, str]:
    """Off/def stat-key names for a field (``fieldKeys``, ``ts:77-79``).

    Args:
        field: A stat field (``"efg"`` / ``"3p"`` / ``"2pmid"`` / ``"2prim"``).

    Returns:
        ``{"off": f"off_{field}", "def": f"def_{field}"}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import field_keys

            keys = field_keys("3p")
            print(keys["off"], keys["def"])  # off_3p def_3p

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return {"off": f"off_{field}", "def": f"def_{field}"}


def get_per_game_raw(opp: OpponentGame, field: str, side: str) -> Optional[float]:
    """Per-game raw shooting rate from one opponent row (``getPerGameRaw``, ``ts:82-116``).

    ``efg`` is ``(2pmid_made + 2prim_made + 1.5 * 3p_made) / (2pmid_att +
    2prim_att + 3p_att)``; ``3p`` / ``2pmid`` / ``2prim`` are ``made /
    attempts``. Every counter read is nullish (missing -> 0); the sole guard
    is on total attempts.

    Args:
        opp: One opponent game dict.
        field: A stat field; an unknown field returns ``None``.
        side: ``"off"`` or ``"def"`` (selects the ``off_``/``def_`` prefix).

    Returns:
        The rate as a float, or ``None`` when the relevant attempts total is
        ``<= 0`` (game skipped by the weighted means -- **not** a 0-rate).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import get_per_game_raw

            game = {"off_3p_made": 4, "off_3p_attempts": 10}
            print(get_per_game_raw(game, "3p", "off"))  # 0.4

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    pre = "off_" if side == "off" else "def_"
    if field == "efg":
        a2m = _num(opp, f"{pre}2pmid_attempts") + _num(opp, f"{pre}2prim_attempts") + _num(opp, f"{pre}3p_attempts")
        if a2m <= 0:
            return None
        made = _num(opp, f"{pre}2pmid_made") + _num(opp, f"{pre}2prim_made") + 1.5 * _num(opp, f"{pre}3p_made")
        return made / a2m
    if field == "3p":
        a = _num(opp, f"{pre}3p_attempts")
        if a <= 0:
            return None
        return _num(opp, f"{pre}3p_made") / a
    if field == "2pmid":
        a = _num(opp, f"{pre}2pmid_attempts")
        if a <= 0:
            return None
        return _num(opp, f"{pre}2pmid_made") / a
    if field == "2prim":
        a = _num(opp, f"{pre}2prim_attempts")
        if a <= 0:
            return None
        return _num(opp, f"{pre}2prim_made") / a
    return None


def get_game_weight(opp: OpponentGame, field: str, side: str) -> float:
    """Weight for one game/field/side (``getGameWeight``, ``ts:119-140``).

    The field-specific shot volume (FGA for ``efg``, 3PA for ``3p``,
    ``2pmid_attempts`` / ``2prim_attempts`` for the mid/rim fields); when that
    is ``0`` (no shots of that type), **falls back to** ``off_poss`` /
    ``def_poss`` so the game still carries weight.

    Args:
        opp: One opponent game dict.
        field: A stat field.
        side: ``"off"`` or ``"def"``.

    Returns:
        The (non-negative) game weight.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import get_game_weight

            game = {"off_3p_attempts": 0, "off_poss": 70}
            print(get_game_weight(game, "3p", "off"))  # 70.0 (poss fallback)

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    pre = "off_" if side == "off" else "def_"
    w = 0.0
    if field == "efg":
        w = _num(opp, f"{pre}2pmid_attempts") + _num(opp, f"{pre}2prim_attempts") + _num(opp, f"{pre}3p_attempts")
    elif field == "3p":
        w = _num(opp, f"{pre}3p_attempts")
    elif field == "2pmid":
        w = _num(opp, f"{pre}2pmid_attempts")
    elif field == "2prim":
        w = _num(opp, f"{pre}2prim_attempts")
    if w > 0:
        return w
    return _num(opp, "off_poss") if side == "off" else _num(opp, "def_poss")


def compute_possession_splits(team: TeamDetail) -> PossessionSplits:
    """Home/away/neutral possession totals for a team (``computePossessionSplits``, ``ts:154-186``).

    Each opponent game's ``off_poss``/``def_poss`` (missing -> 0) is bucketed
    by ``location_type`` (missing or any non ``"Home"``/``"Away"`` value ->
    the neutral bucket).

    Args:
        team: A ``team_details`` team dict.

    Returns:
        A :class:`PossessionSplits`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import compute_possession_splits

            team = {"opponents": [{"off_poss": 70, "def_poss": 68, "location_type": "Home"}]}
            print(compute_possession_splits(team).home_off_poss)  # 70.0

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    home_off = away_off = neutral_off = 0.0
    home_def = away_def = neutral_def = 0.0
    for opp in _opponents(team):
        off_poss = _num(opp, "off_poss")
        def_poss = _num(opp, "def_poss")
        raw_loc = opp.get("location_type")
        loc = raw_loc if raw_loc is not None else "Neutral"
        if loc == "Home":
            home_off += off_poss
            home_def += def_poss
        elif loc == "Away":
            away_off += off_poss
            away_def += def_poss
        else:
            neutral_off += off_poss
            neutral_def += def_poss
    return PossessionSplits(
        home_off_poss=home_off,
        away_off_poss=away_off,
        neutral_off_poss=neutral_off,
        total_off_poss=home_off + away_off + neutral_off,
        home_def_poss=home_def,
        away_def_poss=away_def,
        neutral_def_poss=neutral_def,
        total_def_poss=home_def + away_def + neutral_def,
    )


def compute_league_averages_from_per_game(
    teams: Sequence[TeamDetail],
    fields: Sequence[str] = STRENGTH_ADJUSTED_FIELDS,
) -> LeagueAverages:
    """Possession-weighted league means per field (``computeLeagueAveragesFromPerGame``, ``ts:189-221``).

    For each field, the weighted mean of every team's per-game raw rate over
    all their games; only games with a non-``None`` raw and a positive weight
    contribute. An empty accumulator yields ``0``.

    Args:
        teams: All teams.
        fields: The stat fields to average (default
            :data:`STRENGTH_ADJUSTED_FIELDS`).

    Returns:
        ``{field: {"league_off": float, "league_def": float}}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import compute_league_averages_from_per_game

            teams = [{"team_name": "A", "opponents": [{"off_3p_made": 5, "off_3p_attempts": 10}]}]
            print(compute_league_averages_from_per_game(teams, ["3p"])["3p"]["league_off"])  # 0.5

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    out: LeagueAverages = {}
    for field in fields:
        sum_off_w = sum_off_wx = sum_def_w = sum_def_wx = 0.0
        for t in teams:
            for opp in _opponents(t):
                raw_off = get_per_game_raw(opp, field, "off")
                raw_def = get_per_game_raw(opp, field, "def")
                w_off = get_game_weight(opp, field, "off")
                w_def = get_game_weight(opp, field, "def")
                if raw_off is not None and w_off > 0:
                    sum_off_w += w_off
                    sum_off_wx += w_off * raw_off
                if raw_def is not None and w_def > 0:
                    sum_def_w += w_def
                    sum_def_wx += w_def * raw_def
        out[field] = {
            "league_off": sum_off_wx / sum_off_w if sum_off_w > 0 else 0.0,
            "league_def": sum_def_wx / sum_def_w if sum_def_w > 0 else 0.0,
        }
    return out


def get_team_raw_from_per_game(team: TeamDetail, field: str) -> SideValues:
    """A team's field rate as the weighted mean of its per-game raws (``getTeamRawFromPerGame``, ``ts:224-250``).

    Same accumulation as :func:`compute_league_averages_from_per_game` but
    scoped to one team's games; empty -> ``0``.

    Args:
        team: A ``team_details`` team dict.
        field: A stat field.

    Returns:
        ``{"off": float, "def": float}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import get_team_raw_from_per_game

            team = {"opponents": [{"off_3p_made": 4, "off_3p_attempts": 10}]}
            print(get_team_raw_from_per_game(team, "3p")["off"])  # 0.4

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    sum_off_w = sum_off_wx = sum_def_w = sum_def_wx = 0.0
    for opp in _opponents(team):
        raw_off = get_per_game_raw(opp, field, "off")
        raw_def = get_per_game_raw(opp, field, "def")
        w_off = get_game_weight(opp, field, "off")
        w_def = get_game_weight(opp, field, "def")
        if raw_off is not None and w_off > 0:
            sum_off_w += w_off
            sum_off_wx += w_off * raw_off
        if raw_def is not None and w_def > 0:
            sum_def_w += w_def
            sum_def_wx += w_def * raw_def
    return {
        "off": sum_off_wx / sum_off_w if sum_off_w > 0 else 0.0,
        "def": sum_def_wx / sum_def_w if sum_def_w > 0 else 0.0,
    }


def compute_opponent_strengths(
    team: TeamDetail,
    team_by_name: dict[str, TeamDetail],
    fields: Sequence[str],
    adj_values: AdjValues,
) -> dict[str, SideValues]:
    """Schedule-weighted opponent strength per field (``computeOpponentStrengths``, ``ts:253-299``).

    **Cross-named on purpose:** ``avg_opp_def`` is weighted by the *offensive*
    game weights and reads each opponent's ``def`` adjustment; ``avg_opp_off``
    is weighted by *defensive* weights and reads the opponent's ``off``. Each
    opponent value is its current adjusted value, falling back to its raw
    per-game value when no adjustment exists yet. Games whose opponent is not
    in ``team_by_name`` (or whose off+def weights are both ``<= 0``) are
    skipped.

    Args:
        team: The team whose schedule is being summarized.
        team_by_name: ``{team_name: team_detail}`` for opponent lookup.
        fields: The stat fields to compute.
        adj_values: Current ``{team_name: field: {"off","def"}}`` adjustments.

    Returns:
        ``{field: {"avg_opp_def": float, "avg_opp_off": float}}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import compute_opponent_strengths

            team = {"team_name": "A", "opponents": [{"oppo_name": "B", "off_3p_attempts": 10}]}
            by_name = {"A": team, "B": {"team_name": "B"}}
            adj = {"B": {"3p": {"off": 0.5, "def": 0.3}}}
            print(compute_opponent_strengths(team, by_name, ["3p"], adj)["3p"]["avg_opp_def"])  # 0.3

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    result: dict[str, SideValues] = {}
    for field in fields:
        # NOTE: the TS destructures ``fieldKeys(field)`` into offKey/defKey
        # here (ts:264) but never uses them -- dead code, omitted as a no-op.
        sum_off_weight = sum_def_weight = 0.0
        weighted_opp_def = weighted_opp_off = 0.0
        for opp in _opponents(team):
            opp_name: Any = opp.get("oppo_name")
            opp_team = team_by_name.get(opp_name)
            if opp_team is None:
                continue
            w_off = get_game_weight(opp, field, "off")
            w_def = get_game_weight(opp, field, "def")
            if w_off <= 0 and w_def <= 0:
                continue
            opp_adj = adj_values.get(opp_name)
            opp_def_x = _adj_side(opp_adj, field, "def")
            if opp_def_x is None:
                opp_def_x = get_team_raw_from_per_game(opp_team, field)["def"]
            opp_off_x = _adj_side(opp_adj, field, "off")
            if opp_off_x is None:
                opp_off_x = get_team_raw_from_per_game(opp_team, field)["off"]
            # (TS ``typeof oppDefX === "number"`` guard is always true here --
            # the raw fallback guarantees a float -- so it is elided.)
            if w_off > 0:
                weighted_opp_def += w_off * opp_def_x
                sum_off_weight += w_off
            if w_def > 0:
                weighted_opp_off += w_def * opp_off_x
                sum_def_weight += w_def
        result[field] = {
            "avg_opp_def": weighted_opp_def / sum_off_weight if sum_off_weight > 0 else 0.0,
            "avg_opp_off": weighted_opp_off / sum_def_weight if sum_def_weight > 0 else 0.0,
        }
    return result


def run_iterative_adjustment_with_hca(
    teams: Sequence[TeamDetail],
    team_by_name: dict[str, TeamDetail],
    fields: Sequence[str],
    league_averages: LeagueAverages,
    poss_splits: dict[str, PossessionSplits],
    *,
    max_iterations: int = MAX_ITERATIONS,
    tolerance: float = TOLERANCE,
) -> IterationResult:
    """KenPom-style SoS + HCA fixed-point solver (``runIterativeAdjustmentWithHCA``, ``ts:306-527``).

    Each iteration (Jacobi -- all teams read the *previous* iteration's
    adjustments, then commit together):

    1. Per team/field, adjust every game
       ``adj_game = raw_game * (league / (opp_adj +/- hca))`` and take the
       weighted mean; a field with no valid games keeps its current value.
    2. Re-estimate per-field HCA from home/away possession-imbalance residuals
       ``hca = sum((raw - pred) * |imbalance|) / sum(|imbalance|)`` over teams
       with ``|imbalance| >= IMBALANCE_MIN``.

    Stops when the max per-team/field change drops below ``tolerance`` or after
    ``max_iterations`` sweeps (the HCA re-estimate still runs on the final
    sweep). The cross-guard on the per-game branch, the asymmetric residual
    prediction, and the cross-named opponent strengths are all preserved -- see
    the module docstring's landmine list.

    Args:
        teams: The teams to solve over.
        team_by_name: ``{team_name: team_detail}`` for opponent lookup.
        fields: The stat fields to solve.
        league_averages: Output of
            :func:`compute_league_averages_from_per_game`.
        poss_splits: ``{team_name:`` :class:`PossessionSplits` ``}``.
        max_iterations: Iteration cap (default :data:`MAX_ITERATIONS`; pin to
            ``1`` to inspect a single sweep).
        tolerance: Convergence tolerance (default :data:`TOLERANCE`).

    Returns:
        An :class:`IterationResult` (``adj_values``, ``hca_per_field``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import (
                STRENGTH_ADJUSTED_FIELDS,
                compute_league_averages_from_per_game,
                compute_possession_splits,
                run_iterative_adjustment_with_hca,
            )

            by_name = {t["team_name"]: t for t in teams}
            league = compute_league_averages_from_per_game(teams)
            splits = {t["team_name"]: compute_possession_splits(t) for t in teams}
            result = run_iterative_adjustment_with_hca(
                teams, by_name, STRENGTH_ADJUSTED_FIELDS, league, splits,
            )
            print(result.hca_per_field["3p"]["hca_off"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    adj_values: AdjValues = {}
    for t in teams:
        raw: FieldSideMap = {field: get_team_raw_from_per_game(t, field) for field in fields}
        adj_values[t["team_name"]] = copy.deepcopy(raw)
    hca_per_field: HcaPerField = {field: {"hca_off": 0.0, "hca_def": 0.0} for field in fields}

    for _iteration in range(max_iterations):
        next_values: AdjValues = {}
        max_change = 0.0

        for team in teams:
            cur = adj_values[team["team_name"]]
            next_entry: FieldSideMap = {}

            for field in fields:
                league = league_averages[field]
                league_off = league["league_off"]
                league_def = league["league_def"]
                hca = hca_per_field[field]
                sum_off_w = sum_off_wx = sum_def_w = sum_def_wx = 0.0

                for opp in _opponents(team):
                    opp_name: Any = opp.get("oppo_name")
                    opp_team = team_by_name.get(opp_name)
                    opp_adj = adj_values.get(opp_name) if opp_team is not None else None
                    opp_def_x = _adj_side(opp_adj, field, "def")
                    if opp_def_x is None and opp_team is not None:
                        opp_def_x = get_team_raw_from_per_game(opp_team, field)["def"]
                    opp_off_x = _adj_side(opp_adj, field, "off")
                    if opp_off_x is None and opp_team is not None:
                        opp_off_x = get_team_raw_from_per_game(opp_team, field)["off"]

                    raw_off_g = get_per_game_raw(opp, field, "off")
                    raw_def_g = get_per_game_raw(opp, field, "def")
                    w_off = get_game_weight(opp, field, "off")
                    w_def = get_game_weight(opp, field, "def")

                    raw_loc = opp.get("location_type")
                    loc = raw_loc if raw_loc is not None else "Neutral"
                    hca_sign_off = 1 if loc == "Away" else (-1 if loc == "Home" else 0)
                    hca_sign_def = -1 if loc == "Home" else (1 if loc == "Away" else 0)
                    denom_off = (opp_def_x if opp_def_x is not None else 0.0) + hca_sign_off * hca["hca_off"]
                    denom_def = (opp_off_x if opp_off_x is not None else 0.0) + hca_sign_def * hca["hca_def"]

                    adj_off_g = raw_off_g if raw_off_g is not None else 0.0
                    adj_def_g = raw_def_g if raw_def_g is not None else 0.0
                    # Cross-guard preserved: OFF branch gated on league_def>0,
                    # DEF branch on league_off>0 (ts:385,388).
                    if denom_off > 0 and league_def > 0 and raw_off_g is not None:
                        adj_off_g = raw_off_g * (league_off / denom_off)
                    if denom_def > 0 and league_off > 0 and raw_def_g is not None:
                        adj_def_g = raw_def_g * (league_def / denom_def)

                    if raw_off_g is not None and w_off > 0:
                        sum_off_w += w_off
                        sum_off_wx += w_off * adj_off_g
                    if raw_def_g is not None and w_def > 0:
                        sum_def_w += w_def
                        sum_def_wx += w_def * adj_def_g

                # No valid games -> keep the CURRENT value, not 0 (ts:408-409).
                new_off = sum_off_wx / sum_off_w if sum_off_w > 0 else cur[field]["off"]
                new_def = sum_def_wx / sum_def_w if sum_def_w > 0 else cur[field]["def"]
                next_entry[field] = {"off": new_off, "def": new_def}
                max_change = max(
                    max_change,
                    abs(new_off - cur[field]["off"]),
                    abs(new_def - cur[field]["def"]),
                )
            next_values[team["team_name"]] = next_entry

        for name, entry in next_values.items():
            adj_values[name] = entry

        # Re-estimate HCA from residuals (recomputes opponent strengths off the
        # just-committed adj_values, per team, per field).
        for field in fields:
            sum_off_num = sum_off_den = sum_def_num = sum_def_den = 0.0
            for team in teams:
                splits = poss_splits[team["team_name"]]
                imbalance_off = (
                    (splits.home_off_poss - splits.away_off_poss) / splits.total_off_poss
                    if splits.total_off_poss > 0
                    else 0.0
                )
                imbalance_def = (
                    (splits.home_def_poss - splits.away_def_poss) / splits.total_def_poss
                    if splits.total_def_poss > 0
                    else 0.0
                )

                team_raw = get_team_raw_from_per_game(team, field)
                raw_off = team_raw["off"]
                raw_def = team_raw["def"]
                adj = adj_values[team["team_name"]]
                s = compute_opponent_strengths(team, team_by_name, fields, adj_values)[field]
                league = league_averages[field]
                league_off = league["league_off"]
                league_def = league["league_def"]

                # Asymmetric prediction preserved: predOff *= avg_opp_def/league_def,
                # predDef *= league_off/avg_opp_off (ratio inverts) (ts:463-470).
                pred_off = (
                    adj[field]["off"] * (s["avg_opp_def"] / league_def)
                    if (s["avg_opp_def"] > 0 and league_def > 0)
                    else raw_off
                )
                pred_def = (
                    adj[field]["def"] * (league_off / s["avg_opp_off"])
                    if (s["avg_opp_off"] > 0 and league_off > 0)
                    else raw_def
                )

                if abs(imbalance_off) >= IMBALANCE_MIN:
                    hca_contrib_off = (raw_off - pred_off) / imbalance_off
                    sum_off_num += hca_contrib_off * abs(imbalance_off)
                    sum_off_den += abs(imbalance_off)
                if abs(imbalance_def) >= IMBALANCE_MIN:
                    hca_contrib_def = (raw_def - pred_def) / imbalance_def
                    sum_def_num += hca_contrib_def * abs(imbalance_def)
                    sum_def_den += abs(imbalance_def)

            hca_per_field[field]["hca_off"] = sum_off_num / sum_off_den if sum_off_den > 0 else 0.0
            hca_per_field[field]["hca_def"] = sum_def_num / sum_def_den if sum_def_den > 0 else 0.0

        if max_change < tolerance:
            break

    return IterationResult(adj_values=adj_values, hca_per_field=hca_per_field)


def build_strength_adjusted_stats(
    teams: Sequence[TeamDetail],
    *,
    max_iterations: int = MAX_ITERATIONS,
    tolerance: float = TOLERANCE,
) -> StrengthAdjustedResult:
    """Run the full strength-adjustment compute over a team list.

    Ports the COMPUTE half of the CLI ``main()`` (``ts:594-662``): dedupe
    teams by name (first-wins, as ``main`` does across its tier files),
    compute possession splits + league averages, run
    :func:`run_iterative_adjustment_with_hca`, then assemble each team's
    ``raw`` / ``adj`` / ``adj_hca`` field maps. The file/CLI glue
    (``fs``/``argv``/``dataLastUpdated``/serialization) is intentionally not
    ported -- pass an already-loaded ``team_details`` list.

    Args:
        teams: The ``team_details`` team dicts (each ``{team_name, conf,
            opponents: [...]}``). Duplicate ``team_name``s keep the first
            occurrence.
        max_iterations: Solver iteration cap (default :data:`MAX_ITERATIONS`).
        tolerance: Solver convergence tolerance (default :data:`TOLERANCE`).

    Returns:
        A :class:`StrengthAdjustedResult` (``averages`` per field +
        per-team ``raw``/``adj``/``adj_hca``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_strength import build_strength_adjusted_stats

            result = build_strength_adjusted_stats(team_details)
            print(result.averages["3p"].league_off)
            print(result.teams[0].adj["3p"])  # {"off": ..., "def": ...}

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    team_by_name: dict[str, TeamDetail] = {}
    for t in teams:
        name = t["team_name"]
        if name not in team_by_name:
            team_by_name[name] = t
    unique_teams = list(team_by_name.values())

    poss_splits = {t["team_name"]: compute_possession_splits(t) for t in unique_teams}
    league_averages = compute_league_averages_from_per_game(unique_teams, STRENGTH_ADJUSTED_FIELDS)
    adj_values, hca_per_field = run_iterative_adjustment_with_hca(
        unique_teams,
        team_by_name,
        STRENGTH_ADJUSTED_FIELDS,
        league_averages,
        poss_splits,
        max_iterations=max_iterations,
        tolerance=tolerance,
    )

    averages: dict[str, FieldAverage] = {}
    for field in STRENGTH_ADJUSTED_FIELDS:
        league = league_averages[field]
        hca = hca_per_field[field]
        averages[field] = FieldAverage(
            league_off=league["league_off"],
            league_def=league["league_def"],
            hca_off=hca["hca_off"],
            hca_def=hca["hca_def"],
        )

    out_teams: list[TeamStrengthAdjusted] = []
    for t in unique_teams:
        raw: FieldSideMap = {}
        adj: FieldSideMap = {}
        adj_hca: FieldSideMap = {}
        adj_for_team = adj_values.get(t["team_name"])
        if adj_for_team is None:
            adj_for_team = {}
        for field in STRENGTH_ADJUSTED_FIELDS:
            raw[field] = get_team_raw_from_per_game(t, field)
            team_field = adj_for_team.get(field)
            adj[field] = team_field if team_field is not None else raw[field]
            hca = hca_per_field[field]
            base_off = team_field["off"] if team_field is not None else raw[field]["off"]
            base_def = team_field["def"] if team_field is not None else raw[field]["def"]
            adj_hca[field] = {
                "off": base_off + hca["hca_off"],
                "def": base_def - hca["hca_def"],
            }
        conf = t.get("conf")
        out_teams.append(
            TeamStrengthAdjusted(
                team_name=t["team_name"],
                conf=conf if conf is not None else "",
                raw=raw,
                adj=adj,
                adj_hca=adj_hca,
            )
        )
    return StrengthAdjustedResult(averages=averages, teams=out_teams)
