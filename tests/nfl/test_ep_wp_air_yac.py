"""Offline tests for the air/YAC EPA-WPA column family + per-game running totals.

Faithful port targets (nflfastR ``R/helper_add_ep_wp.R``):

* ``add_air_yac_ep`` / ``add_air_yac_ep_variables`` -> ``_derive_air_yac_epa``
* ``add_air_yac_wp`` / ``add_air_yac_wp_variables`` -> ``_derive_air_yac_wpa``
* ``add_ep_variables`` totals block (L735-801)      -> ``_add_epa_running_totals``
* ``add_wp_variables`` totals block (L1252-1324)    -> ``_add_wpa_running_totals``
* ``vegas_home_wpa`` (L1153)                        -> kept by ``_derive_wpa``

Model isolation
---------------
``air_epa`` / ``air_wpa`` are model-dependent (an EP / WP re-score on the
air-yards-projected game state).  The model-free logic (the airEPA 4-branch
formula, ``yac = epa - air``, ``comp_*`` completion gating, NA->0 home/away
signing, per-game ``cum_sum`` totals) is tested EXACTLY by monkeypatching the
two scorer seams:

* ``ep_wp.calculate_expected_points`` -> deterministic ``ep`` from the
  (mutated) ``yardline_100``, so the expected ``airEP`` is hand-computable and
  the test doubles as proof the air-yards state substitution reached the
  scorer.
* ``ep_wp._score_wp_naive`` -> constant array, so ``air_wpa = airWP - wp`` is
  hand-computable.

The bundled real models are exercised once in structural (non-numeric) tests.

The nflfastR OT branch of ``add_air_yac_wp_variables`` is dead code upstream
(``helper_add_ep_wp.R`` line 1907 re-assigns ``pass_overtime_df`` from the
untouched main-branch frame immediately before writing back), so a faithful
port applies the main-branch formula to overtime rows too — pinned by
``test_wpa_overtime_rows_use_main_branch_formula``.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

import sportsdataverse.nfl.ep_wp as ep_wp
from sportsdataverse.nfl.ep_wp import (
    _AIR_YAC_EPA_COLS,
    _AIR_YAC_WPA_COLS,
    _EPA_RUNNING_TOTAL_COLS,
    _WPA_RUNNING_TOTAL_COLS,
    _add_epa_running_totals,
    _add_wpa_running_totals,
    _derive_air_yac_epa,
    _derive_air_yac_wpa,
    _derive_epa,
    _derive_wpa,
)

# ---------------------------------------------------------------------------
# Fixture builder — one-game, nflverse-shape rows with hand-supplied ep / epa /
# wp / wpa.  Individual rows override only the fields they exercise.
# ---------------------------------------------------------------------------

_DEFAULT: dict = {
    "game_id": "2023_01_AAA_BBB",
    "season": 2023,
    "qtr": 1.0,
    "posteam": "BBB",  # == home_team by default (home perspective)
    "home_team": "BBB",
    "away_team": "AAA",
    "defteam": "AAA",
    "play_type": "pass",
    "yardline_100": 50.0,
    "ydstogo": 10.0,
    "down": 1.0,
    "half_seconds_remaining": 1500.0,
    "game_seconds_remaining": 3300.0,
    "score_differential": 0.0,
    "posteam_timeouts_remaining": 3.0,
    "defteam_timeouts_remaining": 3.0,
    "roof": "outdoors",
    "receive_2h_ko": 0.0,
    "air_yards": None,
    "complete_pass": 0.0,
    "penalty": 0.0,
    "yards_after_catch": None,
    "two_point_attempt": 0.0,
    "ep": 0.0,
    "epa": 0.0,
    "wp": 0.5,
    "wpa": 0.0,
}


def _frame(rows: list[dict]) -> pl.DataFrame:
    full = []
    for i, r in enumerate(rows):
        merged = dict(_DEFAULT)
        merged["play_id"] = i + 1
        merged.update(r)
        full.append(merged)
    return pl.DataFrame(full)


def _one_game() -> pl.DataFrame:
    """8 hand-computed plays: completion, incompletion, TD-in-air, rush,
    4th-down turnover-in-air, 2pt attempt, zero-YAC completion, marker."""
    return _frame(
        [
            # A: completed pass, yac > 0 — the plain airEP - ep branch.
            dict(
                yardline_100=70.0,
                air_yards=10.0,
                ydstogo=5.0,
                down=1.0,
                ep=1.2,
                epa=0.8,
                complete_pass=1.0,
                yards_after_catch=5.0,
                wp=0.50,
                wpa=0.03,
            ),
            # B: incompletion — air_epa nonzero, comp_* gated to 0.
            dict(
                yardline_100=50.0,
                air_yards=12.0,
                ydstogo=8.0,
                down=2.0,
                ep=0.5,
                epa=-0.6,
                complete_pass=0.0,
                wp=0.48,
                wpa=-0.04,
            ),
            # C: throw into the endzone (yardline - air <= 0) — 7 - ep branch.
            dict(
                yardline_100=10.0,
                air_yards=12.0,
                ydstogo=8.0,
                down=1.0,
                ep=3.0,
                epa=-1.4,
                complete_pass=0.0,
                wp=0.60,
                wpa=-0.02,
            ),
            # D: rush — air/yac null, contributes 0 to totals.
            dict(play_type="run", air_yards=None, ep=0.9, epa=0.4, wp=0.50, wpa=0.01),
            # E: 4th-and-10 thrown 6 yards (Turnover_Ind) by the AWAY team.
            dict(
                posteam="AAA",
                defteam="BBB",
                yardline_100=40.0,
                air_yards=6.0,
                ydstogo=10.0,
                down=4.0,
                ep=0.3,
                epa=-0.9,
                complete_pass=0.0,
                wp=0.40,
                wpa=-0.06,
            ),
            # F: two-point attempt — air/yac forced null.
            dict(
                two_point_attempt=1.0,
                down=None,
                air_yards=2.0,
                yardline_100=3.0,
                ep=1.0,
                epa=-0.5,
                complete_pass=0.0,
                wp=0.52,
                wpa=-0.01,
            ),
            # G: zero-YAC completion — yac := 0, air := epa (EPA) / wpa (WPA).
            dict(
                yardline_100=30.0,
                air_yards=8.0,
                ydstogo=10.0,
                down=2.0,
                ep=2.0,
                epa=1.1,
                complete_pass=1.0,
                yards_after_catch=0.0,
                wp=0.55,
                wpa=0.02,
            ),
            # H: marker row — everything null; comp_* stays null (R NA if_else).
            dict(
                play_type="no_play",
                air_yards=None,
                ep=None,
                epa=None,
                wp=None,
                wpa=None,
                complete_pass=None,
                penalty=None,
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Scorer stubs (module-attr seams; the derivations resolve them off ep_wp)
# ---------------------------------------------------------------------------


@pytest.fixture()
def stub_air_scorers(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_cep(df: pl.DataFrame, *, return_as_pandas: bool = False) -> pl.DataFrame:
        # Deterministic EP from the (air-yards-substituted) yardline so the
        # expected airEP is hand-computable: airEP = (50 - yardline_100) / 10.
        return df.with_columns(((50.0 - pl.col("yardline_100").cast(pl.Float64)) / 10.0).alias("ep"))

    def fake_wp_naive(df: pl.DataFrame) -> np.ndarray:
        return np.full(df.height, 0.55, dtype=np.float64)

    monkeypatch.setattr(ep_wp, "calculate_expected_points", fake_cep)
    monkeypatch.setattr(ep_wp, "_score_wp_naive", fake_wp_naive)


# ---------------------------------------------------------------------------
# air / yac EPA — per-play formulas
# ---------------------------------------------------------------------------


def _col(out: pl.DataFrame, col: str, play_id: int):
    return out.filter(pl.col("play_id") == play_id)[col][0]


def test_air_epa_plain_branch_and_yac_identity(stub_air_scorers: None) -> None:
    """A: mutated yardline = 70-10 = 60 -> airEP = -1.0 -> air = -2.2, yac = epa - air."""
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 1) == pytest.approx(-2.2)
    assert _col(out, "yac_epa", 1) == pytest.approx(0.8 - (-2.2))
    # identity: yac_epa == epa - air_epa on the completion
    assert _col(out, "yac_epa", 1) == pytest.approx(_col(out, "epa", 1) - _col(out, "air_epa", 1))


def test_air_epa_nonzero_on_incompletion_comp_gated_to_zero(stub_air_scorers: None) -> None:
    """B: air_epa = (50-38)/10 - 0.5 = 0.7 on the incompletion; comp_* == 0."""
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 2) == pytest.approx(0.7)
    assert _col(out, "yac_epa", 2) == pytest.approx(-0.6 - 0.7)
    assert _col(out, "comp_air_epa", 2) == 0.0
    assert _col(out, "comp_yac_epa", 2) == 0.0


def test_air_epa_td_in_air_is_seven_minus_ep(stub_air_scorers: None) -> None:
    """C: yardline - air <= 0 -> air_epa = 7 - ep (model-independent)."""
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 3) == pytest.approx(7.0 - 3.0)
    assert _col(out, "yac_epa", 3) == pytest.approx(-1.4 - 4.0)


def test_air_epa_turnover_ind_flips_air_ep(stub_air_scorers: None) -> None:
    """E: 4th-and-10, 6-yard throw -> flip state; airEP = (50-66)/10 = -1.6;
    air_epa = -(-1.6) - 0.3 = 1.3."""
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 5) == pytest.approx(1.3)
    assert _col(out, "yac_epa", 5) == pytest.approx(-0.9 - 1.3)


def test_air_epa_two_point_attempt_is_null(stub_air_scorers: None) -> None:
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 6) is None
    assert _col(out, "yac_epa", 6) is None
    # complete_pass == 0 -> comp_* gate to 0, not null (R if_else false branch)
    assert _col(out, "comp_air_epa", 6) == 0.0


def test_air_epa_zero_yac_completion_overrides(stub_air_scorers: None) -> None:
    """G: penalty==0 & yac==0 & complete==1 -> yac_epa = 0, air_epa = epa."""
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 7) == pytest.approx(1.1)
    assert _col(out, "yac_epa", 7) == 0.0


def test_air_epa_null_on_rush_and_marker_rows(stub_air_scorers: None) -> None:
    out = _derive_air_yac_epa(_one_game())
    assert _col(out, "air_epa", 4) is None  # rush
    assert _col(out, "air_epa", 8) is None  # marker
    # marker row: complete_pass null -> comp_* null (dplyr::if_else NA cond)
    assert _col(out, "comp_air_epa", 8) is None
    assert _col(out, "comp_yac_epa", 8) is None


def test_air_epa_totals_cumulative_hand_values(stub_air_scorers: None) -> None:
    out = _derive_air_yac_epa(_one_game()).sort("play_id")
    # home-signed comp_air per play: [-2.2, 0, 0, 0, 0, 0, 1.1, 0 (null->0)]
    assert out["total_home_comp_air_epa"].to_list() == pytest.approx([-2.2, -2.2, -2.2, -2.2, -2.2, -2.2, -1.1, -1.1])
    assert out["total_away_comp_air_epa"][-1] == pytest.approx(1.1)
    # raw (ungated), home-signed: [-2.2, 0.7, 4.0, 0, -1.3 (away play), 0, 1.1, 0]
    assert out["total_home_raw_air_epa"][-1] == pytest.approx(2.3)
    # raw yac home-signed: [3.0, -1.3, -5.4, 0, +2.2, 0, 0, 0]
    assert out["total_home_raw_yac_epa"][-1] == pytest.approx(-1.5)


def test_air_epa_all_columns_float64(stub_air_scorers: None) -> None:
    out = _derive_air_yac_epa(_one_game())
    for c in _AIR_YAC_EPA_COLS:
        assert out.schema[c] == pl.Float64, f"{c} is {out.schema[c]}, want Float64"


def test_air_epa_short_circuit_all_null_air_yards(stub_air_scorers: None) -> None:
    """R add_air_yac_ep: no non-NA air_yards -> every column NA (Float64)."""
    df = _frame([dict(play_type="run"), dict(play_type="run")])
    out = _derive_air_yac_epa(df)
    for c in _AIR_YAC_EPA_COLS:
        assert out.schema[c] == pl.Float64
        assert out[c].null_count() == out.height, f"{c} should be all null"


def test_air_epa_missing_inputs_null_emit(stub_air_scorers: None) -> None:
    """Frames lacking a required §5 input get schema-stable all-null columns."""
    out = _derive_air_yac_epa(_one_game().drop("yards_after_catch"))
    for c in _AIR_YAC_EPA_COLS:
        assert out.schema[c] == pl.Float64
        assert out[c].null_count() == out.height


def test_air_epa_totals_reset_at_game_boundary(stub_air_scorers: None) -> None:
    """Two-game concat: cumulative totals must restart per game (leak test)."""
    g1 = _frame(
        [
            dict(
                yardline_100=50.0,
                air_yards=5.0,
                ydstogo=10.0,
                ep=1.0,
                epa=0.5,
                complete_pass=1.0,
                yards_after_catch=3.0,
            ),
            dict(play_type="run"),
        ]
    )
    g2 = g1.with_columns(pl.lit("2023_02_CCC_BBB").alias("game_id"))
    out = _derive_air_yac_epa(pl.concat([g1, g2], how="vertical"))
    # mutated yardline = 45 -> airEP = 0.5 -> air_epa = 0.5 - 1.0 = -0.5
    per_game = out.group_by("game_id", maintain_order=True).agg(
        pl.col("total_home_raw_air_epa").first().alias("first_total"),
        pl.col("total_home_raw_air_epa").last().alias("last_total"),
    )
    firsts = per_game["first_total"].to_list()
    assert firsts[0] == pytest.approx(-0.5)
    # second game restarts at its OWN first value — not the accumulated -1.0
    assert firsts[1] == pytest.approx(-0.5)


# ---------------------------------------------------------------------------
# air / yac WPA — per-play formulas (constant 0.55 airWP stub)
# ---------------------------------------------------------------------------


def test_air_wpa_plain_branch_and_yac_identity(stub_air_scorers: None) -> None:
    out = _derive_air_yac_wpa(_one_game())
    assert _col(out, "air_wpa", 1) == pytest.approx(0.55 - 0.50)
    assert _col(out, "yac_wpa", 1) == pytest.approx(0.03 - 0.05)
    assert _col(out, "comp_air_wpa", 1) == pytest.approx(0.05)


def test_air_wpa_incompletion_comp_gated_to_zero(stub_air_scorers: None) -> None:
    out = _derive_air_yac_wpa(_one_game())
    assert _col(out, "air_wpa", 2) == pytest.approx(0.55 - 0.48)
    assert _col(out, "comp_air_wpa", 2) == 0.0
    assert _col(out, "comp_yac_wpa", 2) == 0.0


def test_air_wpa_turnover_ind_uses_one_minus_air_wp(stub_air_scorers: None) -> None:
    """E: Turnover_Ind -> airWP = 1 - 0.55 = 0.45; air_wpa = 0.45 - 0.40."""
    out = _derive_air_yac_wpa(_one_game())
    assert _col(out, "air_wpa", 5) == pytest.approx(0.45 - 0.40)
    assert _col(out, "yac_wpa", 5) == pytest.approx(-0.06 - 0.05)


def test_air_wpa_two_point_attempt_is_null(stub_air_scorers: None) -> None:
    out = _derive_air_yac_wpa(_one_game())
    assert _col(out, "air_wpa", 6) is None
    assert _col(out, "yac_wpa", 6) is None


def test_air_wpa_zero_yac_completion_overrides(stub_air_scorers: None) -> None:
    """G: yac==0 completion -> yac_wpa = 0, air_wpa = wpa."""
    out = _derive_air_yac_wpa(_one_game())
    assert _col(out, "air_wpa", 7) == pytest.approx(0.02)
    assert _col(out, "yac_wpa", 7) == 0.0


def test_air_wpa_clock_expiry_zeroes_air_wp(stub_air_scorers: None) -> None:
    """half_seconds - 5.704673 <= 0 -> airWP = 0 -> air_wpa = -wp."""
    df = _frame(
        [
            dict(
                yardline_100=50.0,
                air_yards=10.0,
                half_seconds_remaining=4.0,
                ep=1.0,
                epa=0.5,
                complete_pass=1.0,
                yards_after_catch=2.0,
                wp=0.5,
                wpa=0.1,
            ),
        ]
    )
    out = _derive_air_yac_wpa(df)
    assert out["air_wpa"][0] == pytest.approx(-0.5)


def test_wpa_overtime_rows_use_main_branch_formula(stub_air_scorers: None) -> None:
    """nflfastR's OT air-WP branch is dead code (L1907 re-assignment overwrites
    it), so a qtr==5 pass must get the plain main-branch air_wpa."""
    df = _frame(
        [
            dict(
                qtr=5.0,
                yardline_100=50.0,
                air_yards=10.0,
                ydstogo=8.0,
                down=1.0,
                ep=1.0,
                epa=0.5,
                complete_pass=1.0,
                yards_after_catch=2.0,
                wp=0.5,
                wpa=0.1,
            ),
        ]
    )
    out = _derive_air_yac_wpa(df)
    assert out["air_wpa"][0] == pytest.approx(0.55 - 0.5)


def test_air_wpa_all_columns_float64_and_totals_reset(stub_air_scorers: None) -> None:
    g1 = _frame(
        [
            dict(
                yardline_100=50.0,
                air_yards=5.0,
                ydstogo=10.0,
                ep=1.0,
                epa=0.5,
                complete_pass=1.0,
                yards_after_catch=3.0,
                wp=0.5,
                wpa=0.1,
            ),
            dict(play_type="run"),
        ]
    )
    g2 = g1.with_columns(pl.lit("2023_02_CCC_BBB").alias("game_id"))
    out = _derive_air_yac_wpa(pl.concat([g1, g2], how="vertical"))
    for c in _AIR_YAC_WPA_COLS:
        assert out.schema[c] == pl.Float64, f"{c} is {out.schema[c]}, want Float64"
    per_game = out.group_by("game_id", maintain_order=True).agg(
        pl.col("total_home_raw_air_wpa").first().alias("first_total"),
    )
    # air_wpa = 0.55 - 0.5 = 0.05 in both games; game 2 must restart at 0.05.
    assert per_game["first_total"].to_list() == pytest.approx([0.05, 0.05])


def test_air_wpa_short_circuit_all_null_air_yards(stub_air_scorers: None) -> None:
    df = _frame([dict(play_type="run"), dict(play_type="run")])
    out = _derive_air_yac_wpa(df)
    for c in _AIR_YAC_WPA_COLS:
        assert out.schema[c] == pl.Float64
        assert out[c].null_count() == out.height


# ---------------------------------------------------------------------------
# Running totals — add_ep_variables / add_wp_variables blocks
# ---------------------------------------------------------------------------


def _totals_frame() -> pl.DataFrame:
    rows = [
        # g1: home run, away pass, null-epa marker, null play_type
        dict(game_id="g1", posteam="BBB", play_type="run", epa=1.0, wpa=0.10),
        dict(game_id="g1", posteam="AAA", play_type="pass", epa=-0.5, wpa=-0.05),
        dict(game_id="g1", posteam=None, play_type=None, epa=None, wpa=None),
        dict(game_id="g1", posteam="BBB", play_type=None, epa=2.0, wpa=0.20),
        # g2: totals must restart
        dict(game_id="g2", posteam="BBB", play_type="run", epa=1.0, wpa=0.10),
    ]
    return pl.DataFrame(rows).with_columns(
        pl.lit("BBB").alias("home_team"),
        pl.lit("AAA").alias("away_team"),
    )


def test_epa_running_totals_hand_values() -> None:
    out = _add_epa_running_totals(_totals_frame())
    g1 = out.filter(pl.col("game_id") == "g1")
    # home-signed epa: [1.0, +0.5, 0 (null), 2.0]
    assert g1["total_home_epa"].to_list() == pytest.approx([1.0, 1.5, 1.5, 3.5])
    assert g1["total_away_epa"].to_list() == pytest.approx([-1.0, -1.5, -1.5, -3.5])
    # rush/pass gates: null play_type contributes 0 (R NA -> 0)
    assert g1["total_home_rush_epa"].to_list() == pytest.approx([1.0, 1.0, 1.0, 1.0])
    assert g1["total_home_pass_epa"].to_list() == pytest.approx([0.0, 0.5, 0.5, 0.5])
    assert g1["total_away_pass_epa"].to_list() == pytest.approx([0.0, -0.5, -0.5, -0.5])
    # reset at game boundary
    g2 = out.filter(pl.col("game_id") == "g2")
    assert g2["total_home_epa"].to_list() == pytest.approx([1.0])
    for c in _EPA_RUNNING_TOTAL_COLS:
        assert out.schema[c] == pl.Float64


def test_wpa_running_totals_hand_values() -> None:
    out = _add_wpa_running_totals(_totals_frame())
    g1 = out.filter(pl.col("game_id") == "g1")
    # nflfastR emits only the rush/pass WPA totals (no total_home_wpa)
    assert "total_home_wpa" not in out.columns
    assert g1["total_home_rush_wpa"].to_list() == pytest.approx([0.1, 0.1, 0.1, 0.1])
    assert g1["total_home_pass_wpa"].to_list() == pytest.approx([0.0, 0.05, 0.05, 0.05])
    assert g1["total_away_pass_wpa"].to_list() == pytest.approx([0.0, -0.05, -0.05, -0.05])
    g2 = out.filter(pl.col("game_id") == "g2")
    assert g2["total_home_rush_wpa"].to_list() == pytest.approx([0.1])
    for c in _WPA_RUNNING_TOTAL_COLS:
        assert out.schema[c] == pl.Float64


def test_derive_epa_emits_running_totals() -> None:
    """The shared totals helper is wired into the nflverse ``_derive_epa``."""
    df = pl.DataFrame(
        [
            dict(
                game_id="g1",
                play_id=1,
                season=2023,
                qtr=1.0,
                sp=0.0,
                down=1.0,
                play_type="run",
                posteam="BBB",
                home_team="BBB",
                away_team="AAA",
                home_score=20,
                away_score=17,
                desc="(15:00) run",
                ep=1.0,
                td_team=None,
                field_goal_result=None,
                extra_point_result=None,
                two_point_conv_result=None,
                safety=0,
                kickoff_attempt=0,
                extra_point_attempt=0,
                two_point_attempt=0,
            ),
            dict(
                game_id="g1",
                play_id=2,
                season=2023,
                qtr=1.0,
                sp=0.0,
                down=2.0,
                play_type="pass",
                posteam="BBB",
                home_team="BBB",
                away_team="AAA",
                home_score=20,
                away_score=17,
                desc="(14:20) pass",
                ep=2.0,
                td_team=None,
                field_goal_result=None,
                extra_point_result=None,
                two_point_conv_result=None,
                safety=0,
                kickoff_attempt=0,
                extra_point_attempt=0,
                two_point_attempt=0,
            ),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    for c in _EPA_RUNNING_TOTAL_COLS:
        assert c in out.columns
        assert out.schema[c] == pl.Float64
    # play1 epa = lead(home_ep) - home_ep = 2.0 - 1.0 = 1.0 (run by home)
    assert out["total_home_rush_epa"].to_list() == pytest.approx([1.0, 1.0])


def test_derive_wpa_emits_vegas_home_wpa_and_totals() -> None:
    df = pl.DataFrame(
        [
            dict(
                game_id="g1",
                play_id=1,
                season=2023,
                qtr=1.0,
                sp=0.0,
                down=1.0,
                play_type="run",
                posteam="BBB",
                home_team="BBB",
                away_team="AAA",
                home_score=20,
                away_score=17,
                desc="(15:00) run",
                wp=0.5,
                vegas_wp=0.5,
            ),
            dict(
                game_id="g1",
                play_id=2,
                season=2023,
                qtr=1.0,
                sp=0.0,
                down=2.0,
                play_type="pass",
                posteam="BBB",
                home_team="BBB",
                away_team="AAA",
                home_score=20,
                away_score=17,
                desc="(14:20) pass",
                wp=0.6,
                vegas_wp=0.62,
            ),
        ]
    )
    out = _derive_wpa(df).sort("play_id")
    assert "vegas_home_wpa" in out.columns
    assert out.schema["vegas_home_wpa"] == pl.Float64
    # posteam == home both plays: vegas_home_wpa play1 = 0.62 - 0.5 = 0.12
    assert out["vegas_home_wpa"][0] == pytest.approx(0.12)
    for c in _WPA_RUNNING_TOTAL_COLS:
        assert c in out.columns
        assert out.schema[c] == pl.Float64
    # play1 wpa = lead(home_wp) - home_wp = 0.1 (run by home)
    assert out["total_home_rush_wpa"].to_list() == pytest.approx([0.1, 0.1])


# ---------------------------------------------------------------------------
# Real bundled models — structural assertions only (no hardcoded outputs)
# ---------------------------------------------------------------------------


def _realistic_game() -> pl.DataFrame:
    return _frame(
        [
            # completion with YAC (plain branch)
            dict(
                yardline_100=65.0,
                air_yards=8.0,
                ydstogo=10.0,
                down=1.0,
                ep=1.0,
                epa=0.5,
                complete_pass=1.0,
                yards_after_catch=4.0,
                wp=0.55,
                wpa=0.02,
            ),
            # incompletion — air_epa must still be populated
            dict(
                yardline_100=45.0,
                air_yards=12.0,
                ydstogo=7.0,
                down=2.0,
                ep=1.8,
                epa=-0.7,
                complete_pass=0.0,
                wp=0.52,
                wpa=-0.03,
            ),
            # rush — stays null
            dict(play_type="run", ep=0.9, epa=0.1, wp=0.5, wpa=0.01),
            # TD at the catch spot — model-independent 7 - ep relationship
            dict(
                yardline_100=5.0,
                air_yards=7.0,
                ydstogo=5.0,
                down=1.0,
                ep=4.2,
                epa=-2.0,
                complete_pass=0.0,
                wp=0.7,
                wpa=-0.05,
            ),
        ]
    )


def test_air_epa_real_model_structural() -> None:
    out = _derive_air_yac_epa(_realistic_game()).sort("play_id")
    air = out["air_epa"].to_list()
    # non-null on qualifying rows (incl. the incompletion), null on the rush
    assert air[0] is not None and air[1] is not None and air[3] is not None
    assert air[2] is None
    # sane bounds: ep in [-10, 10], airEP in [-7, 7] -> |air_epa| <= 17
    for v in (air[0], air[1], air[3]):
        assert abs(v) <= 17.0
    # yac identity on the completion (no zero-yac override fired)
    assert out["yac_epa"][0] == pytest.approx(0.5 - air[0])
    # TD-at-catch-spot special case: exactly 7 - ep
    assert air[3] == pytest.approx(7.0 - 4.2)
    assert out.schema["air_epa"] == pl.Float64


def test_air_wpa_real_model_structural() -> None:
    out = _derive_air_yac_wpa(_realistic_game()).sort("play_id")
    air = out["air_wpa"].to_list()
    assert air[0] is not None and air[1] is not None
    assert air[2] is None
    # airWP and wp are both probabilities -> air_wpa strictly inside (-1, 1)
    for v in (air[0], air[1]):
        assert -1.0 < v < 1.0
    assert out["yac_wpa"][0] == pytest.approx(0.02 - air[0])
    assert out.schema["air_wpa"] == pl.Float64


# ---------------------------------------------------------------------------
# enrich_nfl_pbp wiring — the family runs inside the nflverse orchestrator
# ---------------------------------------------------------------------------


def test_enrich_wires_air_yac_family(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.nfl.test_enrich import (
        _stub_cp,
        _stub_ep_factory,
        _stub_wp,
        _stub_xyac,
        _synthetic_frame,
    )

    monkeypatch.setattr(ep_wp, "calculate_expected_points", _stub_ep_factory({}))
    monkeypatch.setattr(ep_wp, "calculate_win_probability", _stub_wp)
    monkeypatch.setattr(ep_wp, "calculate_completion_probability", _stub_cp)
    monkeypatch.setattr(ep_wp, "calculate_xyac", _stub_xyac)
    monkeypatch.setattr(ep_wp, "_score_wp_naive", lambda df: np.full(df.height, 0.55, dtype=np.float64))

    # complete the §5 input surface the synthetic frame doesn't carry
    df = _synthetic_frame().with_columns(
        pl.lit(0.0).alias("penalty"),
        pl.lit(None, dtype=pl.Float64).alias("yards_after_catch"),
    )
    out = ep_wp.enrich_nfl_pbp(df, add_fourth_down=False)
    assert isinstance(out, pl.DataFrame)

    for c in (
        *_AIR_YAC_EPA_COLS,
        *_AIR_YAC_WPA_COLS,
        *_EPA_RUNNING_TOTAL_COLS,
        *_WPA_RUNNING_TOTAL_COLS,
        "vegas_home_wpa",
    ):
        assert c in out.columns, f"enrich output missing {c}"
        assert out.schema[c] == pl.Float64, f"{c} is {out.schema[c]}, want Float64"

    # the qualifying pass (air_yards == 8.0) got a real air_epa / air_wpa
    qualifying = out.filter(pl.col("air_yards").is_not_null() & (pl.col("play_type") == "pass"))
    assert qualifying.height > 0
    assert qualifying["air_epa"].null_count() < qualifying.height
    assert qualifying["air_wpa"].null_count() < qualifying.height
