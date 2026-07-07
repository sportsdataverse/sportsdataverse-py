"""Hand-derived tests for ``mbb_ncaa_strength`` (buildStrengthAdjustedStats port).

The upstream (``cbb-on-off-analyzer/src/bin/buildStrengthAdjustedStats.ts``) is
an oracle-less CLI script (no jest). Per the Phase-5d.3 discipline, every
expectation below is **hand-derived from the TS on paper** and the arithmetic
is documented in-line so a reviewer can re-derive it. All fixtures are tiny and
fully offline; there is no network and no polars anywhere.

Derivation key (mirrors ``get_per_game_raw`` / ``get_game_weight``):

- ``efg`` per game = ``(2pmid_made + 2prim_made + 1.5*3p_made) /
  (2pmid_att + 2prim_att + 3p_att)``; ``3p``/``2pmid``/``2prim`` = made/att.
- ``efg`` weight = FGA = ``2pmid_att + 2prim_att + 3p_att``; per-shot-type
  weight = that type's attempts; both fall back to ``off_poss``/``def_poss``
  when 0.
- A field with ``attempts <= 0`` yields ``None`` (skipped in the weighted
  mean), NOT a 0-rate.
"""

from __future__ import annotations

import pytest

from sportsdataverse.mbb.mbb_ncaa_strength import (
    IMBALANCE_MIN,
    MAX_ITERATIONS,
    STRENGTH_ADJUSTED_FIELDS,
    TOLERANCE,
    build_strength_adjusted_stats,
    compute_league_averages_from_per_game,
    compute_opponent_strengths,
    compute_possession_splits,
    field_keys,
    get_game_weight,
    get_per_game_raw,
    get_team_raw_from_per_game,
    run_iterative_adjustment_with_hca,
)

ABS = 1e-9  # tolerance for exact hand-derived arithmetic


# --------------------------------------------------------------------------- #
# constants + field_keys
# --------------------------------------------------------------------------- #
def test_constants_verbatim():
    """Module constants copied verbatim from ``ts:23-26``."""
    assert STRENGTH_ADJUSTED_FIELDS == ("efg", "3p", "2pmid", "2prim")
    assert MAX_ITERATIONS == 100
    assert TOLERANCE == 1e-6
    assert IMBALANCE_MIN == 1e-6


def test_field_keys():
    """``fieldKeys`` (``ts:77-79``): ``off_<field>`` / ``def_<field>``."""
    assert field_keys("3p") == {"off": "off_3p", "def": "def_3p"}
    assert field_keys("efg") == {"off": "off_efg", "def": "def_efg"}


# --------------------------------------------------------------------------- #
# get_per_game_raw
# --------------------------------------------------------------------------- #
def test_get_per_game_raw_efg_numerator():
    """efg = (2pmid_made + 2prim_made + 1.5*3p_made) / FGA (``ts:88-98``).

    game: 2pmid 2/4, 2prim 3/6, 3p 4/10.
    FGA = 4 + 6 + 10 = 20. made = 2 + 3 + 1.5*4 = 2 + 3 + 6 = 11. efg = 11/20 = 0.55.
    """
    game = {
        "off_2pmid_made": 2,
        "off_2pmid_attempts": 4,
        "off_2prim_made": 3,
        "off_2prim_attempts": 6,
        "off_3p_made": 4,
        "off_3p_attempts": 10,
    }
    assert get_per_game_raw(game, "efg", "off") == pytest.approx(0.55, abs=ABS)
    # per-shot-type rates off the same row:
    assert get_per_game_raw(game, "3p", "off") == pytest.approx(0.4, abs=ABS)
    assert get_per_game_raw(game, "2pmid", "off") == pytest.approx(0.5, abs=ABS)
    assert get_per_game_raw(game, "2prim", "off") == pytest.approx(0.5, abs=ABS)


def test_get_per_game_raw_efg_missing_keys_are_zero():
    """Missing counters read as 0; only 3p present -> efg = 1.5*3pm/3pa.

    game: 3p 2/5 only. FGA = 0 + 0 + 5 = 5. made = 0 + 0 + 1.5*2 = 3. efg = 3/5 = 0.6.
    """
    game = {"off_3p_made": 2, "off_3p_attempts": 5}
    assert get_per_game_raw(game, "efg", "off") == pytest.approx(0.6, abs=ABS)


def test_get_per_game_raw_zero_attempts_is_none_not_zero_rate():
    """``att <= 0 -> None`` (skip), distinct from a 0-rate (``ts:93,102,...``).

    A present ``made`` with 0 attempts, and an entirely-missing field, both
    return ``None`` -- NOT ``0.0``.
    """
    assert get_per_game_raw({"off_3p_made": 3, "off_3p_attempts": 0}, "3p", "off") is None
    assert get_per_game_raw({}, "3p", "off") is None
    assert get_per_game_raw({}, "efg", "off") is None
    assert get_per_game_raw({"off_3p_attempts": 5}, "unknown_field", "off") is None


def test_get_per_game_raw_def_side():
    """``side='def'`` reads the ``def_`` prefix (``ts:87``)."""
    game = {"def_2prim_made": 6, "def_2prim_attempts": 10}
    assert get_per_game_raw(game, "2prim", "def") == pytest.approx(0.6, abs=ABS)
    assert get_per_game_raw(game, "2prim", "off") is None  # off side has no attempts


# --------------------------------------------------------------------------- #
# get_game_weight
# --------------------------------------------------------------------------- #
def test_get_game_weight_field_volumes():
    """Weight = the field-specific attempts (``ts:126-137``).

    efg weight = 4 + 6 + 10 = 20; 3p = 10; 2pmid = 4; 2prim = 6.
    """
    game = {"off_2pmid_attempts": 4, "off_2prim_attempts": 6, "off_3p_attempts": 10}
    assert get_game_weight(game, "efg", "off") == pytest.approx(20.0, abs=ABS)
    assert get_game_weight(game, "3p", "off") == pytest.approx(10.0, abs=ABS)
    assert get_game_weight(game, "2pmid", "off") == pytest.approx(4.0, abs=ABS)
    assert get_game_weight(game, "2prim", "off") == pytest.approx(6.0, abs=ABS)


def test_get_game_weight_poss_fallback():
    """Weight 0 -> fall back to ``off_poss``/``def_poss`` (``ts:138-139``)."""
    game = {"off_poss": 70, "def_poss": 68}  # no attempt fields
    assert get_game_weight(game, "efg", "off") == pytest.approx(70.0, abs=ABS)
    assert get_game_weight(game, "3p", "def") == pytest.approx(68.0, abs=ABS)
    # present attempts win over the poss fallback:
    game2 = {"off_3p_attempts": 12, "off_poss": 70}
    assert get_game_weight(game2, "3p", "off") == pytest.approx(12.0, abs=ABS)


# --------------------------------------------------------------------------- #
# compute_possession_splits
# --------------------------------------------------------------------------- #
def test_compute_possession_splits():
    """Bucket off/def poss by location; missing location -> Neutral (``ts:154-186``).

    Home 70/68, Away 65/66, Neutral 72/70, plus a location-less 60/62 (-> Neutral).
    neutral_off = 72 + 60 = 132; total_off = 70 + 65 + 132 = 267.
    neutral_def = 70 + 62 = 132; total_def = 68 + 66 + 132 = 266.
    """
    team = {
        "opponents": [
            {"off_poss": 70, "def_poss": 68, "location_type": "Home"},
            {"off_poss": 65, "def_poss": 66, "location_type": "Away"},
            {"off_poss": 72, "def_poss": 70, "location_type": "Neutral"},
            {"off_poss": 60, "def_poss": 62},
        ]
    }
    s = compute_possession_splits(team)
    assert s.home_off_poss == pytest.approx(70.0, abs=ABS)
    assert s.away_off_poss == pytest.approx(65.0, abs=ABS)
    assert s.neutral_off_poss == pytest.approx(132.0, abs=ABS)
    assert s.total_off_poss == pytest.approx(267.0, abs=ABS)
    assert s.home_def_poss == pytest.approx(68.0, abs=ABS)
    assert s.away_def_poss == pytest.approx(66.0, abs=ABS)
    assert s.neutral_def_poss == pytest.approx(132.0, abs=ABS)
    assert s.total_def_poss == pytest.approx(266.0, abs=ABS)


# --------------------------------------------------------------------------- #
# compute_league_averages_from_per_game + get_team_raw_from_per_game
# --------------------------------------------------------------------------- #
def test_compute_league_averages_weighted():
    """Weighted mean of per-game raw over all teams' games (``ts:189-221``).

    A off 4/10 (w10), B off 6/10 (w10) -> league_off = (10*0.4 + 10*0.6)/20 = 0.5.
    A def 3/10 (w10), B def 5/10 (w10) -> league_def = (10*0.3 + 10*0.5)/20 = 0.4.
    """
    teams = [
        {
            "team_name": "A",
            "opponents": [{"off_3p_made": 4, "off_3p_attempts": 10, "def_3p_made": 3, "def_3p_attempts": 10}],
        },
        {
            "team_name": "B",
            "opponents": [{"off_3p_made": 6, "off_3p_attempts": 10, "def_3p_made": 5, "def_3p_attempts": 10}],
        },
    ]
    league = compute_league_averages_from_per_game(teams, ["3p"])
    assert league["3p"]["league_off"] == pytest.approx(0.5, abs=ABS)
    assert league["3p"]["league_def"] == pytest.approx(0.4, abs=ABS)


def test_compute_league_averages_empty_is_zero():
    """No qualifying games -> 0 (``ts:216-217``)."""
    league = compute_league_averages_from_per_game([{"team_name": "A", "opponents": []}], ["3p"])
    assert league["3p"] == {"league_off": 0.0, "league_def": 0.0}


def test_get_team_raw_weighted_over_games():
    """Team raw = weighted mean of its per-game raws (``ts:224-250``).

    g1: off 4/10 (w10), def 3/10 (w10); g2: off 3/5 (0.6, w5), def 2/5 (0.4, w5).
    off = (10*0.4 + 5*0.6)/15 = 7/15; def = (10*0.3 + 5*0.4)/15 = 5/15.
    """
    team = {
        "opponents": [
            {"off_3p_made": 4, "off_3p_attempts": 10, "def_3p_made": 3, "def_3p_attempts": 10},
            {"off_3p_made": 3, "off_3p_attempts": 5, "def_3p_made": 2, "def_3p_attempts": 5},
        ]
    }
    raw = get_team_raw_from_per_game(team, "3p")
    assert raw["off"] == pytest.approx(7.0 / 15.0, abs=ABS)
    assert raw["def"] == pytest.approx(5.0 / 15.0, abs=ABS)


# --------------------------------------------------------------------------- #
# compute_opponent_strengths (the cross-naming landmine)
# --------------------------------------------------------------------------- #
def test_compute_opponent_strengths_cross_naming():
    """avg_opp_def uses OFF weights + opp DEF; avg_opp_off uses DEF weights + opp OFF (``ts:283-296``).

    A plays B (off_att 10, def_att 8) and C (off_att 20, def_att 12).
    adj: B = {off 0.5, def 0.3}, C = {off 0.4, def 0.6}.
    avg_opp_def = (10*0.3 [B.def] + 20*0.6 [C.def]) / (10 + 20) = 15/30 = 0.5.
    avg_opp_off = (8*0.5 [B.off] + 12*0.4 [C.off]) / (8 + 12) = 8.8/20 = 0.44.
    """
    team = {
        "team_name": "A",
        "opponents": [
            {"oppo_name": "B", "off_3p_attempts": 10, "def_3p_attempts": 8},
            {"oppo_name": "C", "off_3p_attempts": 20, "def_3p_attempts": 12},
            {"oppo_name": "X_unknown", "off_3p_attempts": 99, "def_3p_attempts": 99},  # skipped (not in map)
        ],
    }
    team_by_name = {"A": team, "B": {"team_name": "B"}, "C": {"team_name": "C"}}
    adj = {
        "B": {"3p": {"off": 0.5, "def": 0.3}},
        "C": {"3p": {"off": 0.4, "def": 0.6}},
    }
    s = compute_opponent_strengths(team, team_by_name, ["3p"], adj)["3p"]
    assert s["avg_opp_def"] == pytest.approx(0.5, abs=ABS)
    assert s["avg_opp_off"] == pytest.approx(0.44, abs=ABS)


def test_compute_opponent_strengths_falls_back_to_opp_raw():
    """No adj entry for the opponent -> its per-game raw is used (``ts:278-281``).

    B has no adj entry but is in the map with games; getTeamRawFromPerGame(B, 3p):
    off = 8/10 = 0.8, def = 3/10 = 0.3. A plays B once (off_att 10, def_att 10).
    avg_opp_def = 10*0.3 / 10 = 0.3; avg_opp_off = 10*0.8 / 10 = 0.8.
    """
    b_team = {
        "team_name": "B",
        "opponents": [{"off_3p_made": 8, "off_3p_attempts": 10, "def_3p_made": 3, "def_3p_attempts": 10}],
    }
    team = {"team_name": "A", "opponents": [{"oppo_name": "B", "off_3p_attempts": 10, "def_3p_attempts": 10}]}
    team_by_name = {"A": team, "B": b_team}
    s = compute_opponent_strengths(team, team_by_name, ["3p"], {})["3p"]
    assert s["avg_opp_def"] == pytest.approx(0.3, abs=ABS)
    assert s["avg_opp_off"] == pytest.approx(0.8, abs=ABS)


# --------------------------------------------------------------------------- #
# solver degenerate case (a): identical + perfectly balanced -> adj == raw, hca == 0
# --------------------------------------------------------------------------- #
def _identical_game(oppo: str) -> dict:
    """A game whose off_ and def_ counters are identical: 3p 4/10, 2pmid 3/6,
    2prim 4/5. efg = (3 + 4 + 1.5*4)/(6 + 5 + 10) = 13/21 both sides."""
    fields = {}
    for side in ("off", "def"):
        fields[f"{side}_3p_made"] = 4
        fields[f"{side}_3p_attempts"] = 10
        fields[f"{side}_2pmid_made"] = 3
        fields[f"{side}_2pmid_attempts"] = 6
        fields[f"{side}_2prim_made"] = 4
        fields[f"{side}_2prim_attempts"] = 5
    return {"oppo_name": oppo, "location_type": "Neutral", **fields}


def test_solver_identical_balanced_is_fixed_point():
    """All teams identical + all-neutral schedule -> adj == raw == league, hca == 0.

    Every game is identical, so league_off == league_def == the raw rate for
    each field (3p 0.4, 2pmid 0.5, 2prim 0.8, efg 13/21). Each per-game
    adjustment is raw*(league/opp_adj) = raw*(r/r) = raw, so the solver is at a
    fixed point from iteration 0. All-neutral -> imbalance 0 -> hca == 0.
    """
    teams = [
        {"team_name": "A", "conf": "X", "opponents": [_identical_game("B")]},
        {"team_name": "B", "conf": "X", "opponents": [_identical_game("A")]},
    ]
    result = build_strength_adjusted_stats(teams)
    expected = {"efg": 13.0 / 21.0, "3p": 0.4, "2pmid": 0.5, "2prim": 0.8}
    for field in STRENGTH_ADJUSTED_FIELDS:
        avg = result.averages[field]
        assert avg.league_off == pytest.approx(expected[field], abs=ABS)
        assert avg.league_def == pytest.approx(expected[field], abs=ABS)
        assert avg.hca_off == pytest.approx(0.0, abs=ABS)
        assert avg.hca_def == pytest.approx(0.0, abs=ABS)
    for team in result.teams:
        for field in STRENGTH_ADJUSTED_FIELDS:
            for side in ("off", "def"):
                assert team.raw[field][side] == pytest.approx(expected[field], abs=ABS)
                assert team.adj[field][side] == pytest.approx(expected[field], abs=ABS)
                # hca == 0 -> adj_hca == adj
                assert team.adj_hca[field][side] == pytest.approx(expected[field], abs=ABS)


# --------------------------------------------------------------------------- #
# solver degenerate case (b): home+away games but balanced possessions -> hca == 0
# --------------------------------------------------------------------------- #
def test_solver_balanced_home_away_gives_zero_hca():
    """Home and Away games with equal possessions -> imbalance 0 -> hca == 0 both sides.

    Each team plays a Home and an Away game with off_poss == def_poss == 70 in
    both, so (home - away)/total == 0 < IMBALANCE_MIN for every team; no team
    contributes to the residual, so hca_off == hca_def == 0 for all fields.
    (This exercises the imbalance path directly, unlike the all-neutral case.)
    """

    def rim_game(oppo: str, loc: str) -> dict:
        return {
            "oppo_name": oppo,
            "location_type": loc,
            "off_poss": 70,
            "def_poss": 70,
            "off_2prim_made": 5,
            "off_2prim_attempts": 10,
            "def_2prim_made": 5,
            "def_2prim_attempts": 10,
        }

    teams = [
        {"team_name": "A", "conf": "X", "opponents": [rim_game("B", "Home"), rim_game("B", "Away")]},
        {"team_name": "B", "conf": "X", "opponents": [rim_game("A", "Home"), rim_game("A", "Away")]},
    ]
    result = build_strength_adjusted_stats(teams)
    for field in STRENGTH_ADJUSTED_FIELDS:
        assert result.averages[field].hca_off == pytest.approx(0.0, abs=ABS)
        assert result.averages[field].hca_def == pytest.approx(0.0, abs=ABS)


# --------------------------------------------------------------------------- #
# solver (c): a single fully hand-derived iteration on a 2-team fixture
# --------------------------------------------------------------------------- #
def _rim_only_fixture() -> list[dict]:
    """A rim-only (2prim) 2-team fixture. Because only 2prim shots exist,
    efg is identical to 2prim (efg num = 2prim_made, efg den = 2prim_att), and
    3p/2pmid have no attempts (raw None -> adj stays 0).

    A vs B (Neutral): off 6/10 (0.6), def 4/10 (0.4).
    B vs A (Neutral): off 5/10 (0.5), def 5/10 (0.5).
    """
    return [
        {
            "team_name": "A",
            "conf": "X",
            "opponents": [
                {
                    "oppo_name": "B",
                    "location_type": "Neutral",
                    "off_2prim_made": 6,
                    "off_2prim_attempts": 10,
                    "def_2prim_made": 4,
                    "def_2prim_attempts": 10,
                },
            ],
        },
        {
            "team_name": "B",
            "conf": "Y",
            "opponents": [
                {
                    "oppo_name": "A",
                    "location_type": "Neutral",
                    "off_2prim_made": 5,
                    "off_2prim_attempts": 10,
                    "def_2prim_made": 5,
                    "def_2prim_attempts": 10,
                },
            ],
        },
    ]


def test_solver_single_iteration_exact():
    """One hand-derived iteration on the rim-only fixture (``ts:335-423``).

    league_off(2prim) = (10*0.6 + 10*0.5)/20 = 0.55.
    league_def(2prim) = (10*0.4 + 10*0.5)/20 = 0.45.
    raw: A = {off 0.6, def 0.4}; B = {off 0.5, def 0.5}. hca starts 0.

    Iter 0, field 2prim (all Neutral -> hca sign 0, denom = opp_adj):
      A vs B: denomOff = B.def = 0.5; denomDef = B.off = 0.5.
        adjOff = 0.6*(0.55/0.5) = 0.66;  adjDef = 0.4*(0.45/0.5) = 0.36.
      B vs A: denomOff = A.def = 0.4; denomDef = A.off = 0.6.
        adjOff = 0.5*(0.55/0.4) = 0.6875; adjDef = 0.5*(0.45/0.6) = 0.375.
    Single game each -> team adj == the per-game adj. efg mirrors 2prim
    exactly; 2pmid/3p have no games -> keep current (== raw == 0). All-neutral
    -> hca stays 0.
    """
    teams = _rim_only_fixture()
    team_by_name = {t["team_name"]: t for t in teams}
    league = compute_league_averages_from_per_game(teams, STRENGTH_ADJUSTED_FIELDS)
    splits = {t["team_name"]: compute_possession_splits(t) for t in teams}

    # league averages sanity (hand-derived above):
    assert league["2prim"]["league_off"] == pytest.approx(0.55, abs=ABS)
    assert league["2prim"]["league_def"] == pytest.approx(0.45, abs=ABS)
    assert league["efg"]["league_off"] == pytest.approx(0.55, abs=ABS)  # efg == 2prim here
    assert league["efg"]["league_def"] == pytest.approx(0.45, abs=ABS)
    assert league["3p"] == {"league_off": 0.0, "league_def": 0.0}
    assert league["2pmid"] == {"league_off": 0.0, "league_def": 0.0}

    result = run_iterative_adjustment_with_hca(
        teams, team_by_name, STRENGTH_ADJUSTED_FIELDS, league, splits, max_iterations=1
    )
    adj = result.adj_values

    for field in ("2prim", "efg"):
        assert adj["A"][field]["off"] == pytest.approx(0.66, abs=ABS)
        assert adj["A"][field]["def"] == pytest.approx(0.36, abs=ABS)
        assert adj["B"][field]["off"] == pytest.approx(0.6875, abs=ABS)
        assert adj["B"][field]["def"] == pytest.approx(0.375, abs=ABS)
    for field in ("3p", "2pmid"):
        assert adj["A"][field] == {"off": 0.0, "def": 0.0}
        assert adj["B"][field] == {"off": 0.0, "def": 0.0}
    for field in STRENGTH_ADJUSTED_FIELDS:
        assert result.hca_per_field[field] == {"hca_off": 0.0, "hca_def": 0.0}


# --------------------------------------------------------------------------- #
# end-to-end shape + real multi-iteration convergence (3-team round-robin)
# --------------------------------------------------------------------------- #
# NOTE on why a 3-team fixture (not the 2-team one) is used for convergence:
# the KenPom off/def-coupled multiplicative update is unstable on a 2-team
# graph -- ``off`` blows up and ``def`` collapses toward 0 over the 100
# iterations (it never converges; verified). That divergence is itself faithful
# to the TS (identical math), but it makes a poor convergence oracle. A
# 3-team round-robin has enough connectivity to converge to sensible values
# near the league mean, so it is used here for the end-to-end convergence
# assertion. The 2-team fixture above is used only for the exact 1-iteration
# test (a single iteration is well-defined regardless of long-run behaviour).
def _round_robin_fixture() -> list[dict]:
    """A symmetric rim-only 3-team round-robin (each plays the other two,
    all Neutral). Constructed symmetric under swapping A<->B AND off<->def:

    A: vs B off 6/10 def 4/10; vs C off 5/10 def 5/10
    B: vs A off 4/10 def 6/10; vs C off 5/10 def 5/10
    C: vs A off 5/10 def 5/10; vs B off 5/10 def 5/10
    """

    def g(oppo: str, om: int, dm: int) -> dict:
        return {
            "oppo_name": oppo,
            "location_type": "Neutral",
            "off_2prim_made": om,
            "off_2prim_attempts": 10,
            "def_2prim_made": dm,
            "def_2prim_attempts": 10,
        }

    return [
        {"team_name": "A", "conf": "X", "opponents": [g("B", 6, 4), g("C", 5, 5)]},
        {"team_name": "B", "conf": "X", "opponents": [g("A", 4, 6), g("C", 5, 5)]},
        {"team_name": "C", "conf": "X", "opponents": [g("A", 5, 5), g("B", 5, 5)]},
    ]


# Captured from the converged Python run of ``_round_robin_fixture``. Independently
# constrained by: league/raw hand-derivations below, the A<->B off/def symmetry,
# efg==2prim, 3p/2pmid==0, and hca==0 -- see the assertions in the test.
_GOLDEN_2PRIM = {
    "A": {"off": 0.5294307544089638, "def": 0.46250036448227183},
    "B": {"off": 0.46250036448227183, "def": 0.5294307544089638},
    "C": {"off": 0.5050391041814134, "def": 0.5050391041814134},
}


def test_build_strength_adjusted_stats_shape_and_convergence():
    """End-to-end shape + real multi-iteration convergence (``ts:594-662``).

    League (rim-only, all weights equal at 10): every off game averages to
    league_off = league_def = 0.5. Raws: A {off 0.55, def 0.45}, B {off 0.45,
    def 0.55}, C {off 0.5, def 0.5} (mean of the team's two games).

    Oracle-less invariants that constrain the converged adj without a magic
    golden (the golden is an additional regression pin):

    * A<->B off/def symmetry: the fixture is symmetric under swapping A<->B and
      off<->def, so converged ``A.off == B.def`` and ``A.def == B.off``, and C
      is self-symmetric (``off == def``).
    * efg adj == 2prim adj (rim-only -> efg is identical to 2prim every
      iteration); 3p/2pmid stay 0 (no attempts).
    * hca == 0 (all-neutral) -> adj_hca == adj.
    """
    teams = _round_robin_fixture()
    result = build_strength_adjusted_stats(teams)

    # shape
    assert set(result.averages) == set(STRENGTH_ADJUSTED_FIELDS)
    for field in STRENGTH_ADJUSTED_FIELDS:
        avg = result.averages[field]
        assert {avg.league_off, avg.league_def, avg.hca_off, avg.hca_def}  # 4 attrs present
    assert [t.team_name for t in result.teams] == ["A", "B", "C"]
    for team in result.teams:
        for bucket in (team.raw, team.adj, team.adj_hca):
            assert set(bucket) == set(STRENGTH_ADJUSTED_FIELDS)
            for field in STRENGTH_ADJUSTED_FIELDS:
                assert set(bucket[field]) == {"off", "def"}

    by_name = {t.team_name: t for t in result.teams}

    # league + raw hand-derived
    assert result.averages["2prim"].league_off == pytest.approx(0.5, abs=ABS)
    assert result.averages["2prim"].league_def == pytest.approx(0.5, abs=ABS)
    assert by_name["A"].raw["2prim"] == pytest.approx({"off": 0.55, "def": 0.45}, abs=ABS)
    assert by_name["B"].raw["2prim"] == pytest.approx({"off": 0.45, "def": 0.55}, abs=ABS)
    assert by_name["C"].raw["2prim"] == pytest.approx({"off": 0.5, "def": 0.5}, abs=ABS)

    # A<->B off/def symmetry (exact structural invariant of the fixture)
    assert by_name["A"].adj["2prim"]["off"] == pytest.approx(by_name["B"].adj["2prim"]["def"], abs=ABS)
    assert by_name["A"].adj["2prim"]["def"] == pytest.approx(by_name["B"].adj["2prim"]["off"], abs=ABS)
    assert by_name["C"].adj["2prim"]["off"] == pytest.approx(by_name["C"].adj["2prim"]["def"], abs=ABS)

    for name in ("A", "B", "C"):
        team = by_name[name]
        # efg == 2prim invariant (holds at convergence)
        assert team.adj["efg"] == pytest.approx(team.adj["2prim"], abs=ABS)
        # empty fields stay 0
        assert team.adj["3p"] == pytest.approx({"off": 0.0, "def": 0.0}, abs=ABS)
        assert team.adj["2pmid"] == pytest.approx({"off": 0.0, "def": 0.0}, abs=ABS)
        # hca == 0 -> adj_hca == adj everywhere
        for field in STRENGTH_ADJUSTED_FIELDS:
            assert team.adj_hca[field] == pytest.approx(team.adj[field], abs=ABS)
        # captured regression golden
        assert team.adj["2prim"] == pytest.approx(_GOLDEN_2PRIM[name], abs=1e-6)
