"""Regression guards for the 0.36-live -> polars-main CFB pbp reconciliation.

These pin the behaviors ported from the pandas 0.36-live branch:
  * `cleaned_text` (strips clock / formation / depth+direction) so verb-anchored
    yardage parsers match modern ESPN phrasing ("rush middle for" -> "rush for").
  * `yds_rushed` / `yds_receiving` populate on direction-word text (raw `text`
    silently returned null for every direction-carrying rush/completion).
  * `kneel_down` detection + exclusion from `scrimmage_play`.

All run the FULL pipeline offline against captured fixtures (no network).
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.cfb.cfb_pbp as cfb_pbp_mod
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess, kickoff_vec

FIX = Path(__file__).parent / "fixtures"


def _run_proc(monkeypatch, gid: int) -> CFBPlayProcess:
    """Run the full offline pipeline and return the processed CFBPlayProcess (so
    callers that also need `create_box_score` reuse the same already-run instance)."""
    summary = json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr(cfb_pbp_mod, "download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.join_participants = False  # offline: skip participants/roster fetch
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    return proc


def _run_fixture(monkeypatch, gid: int) -> pl.DataFrame:
    return pl.from_dicts(_run_proc(monkeypatch, gid).plays_json, infer_schema_length=None)


def test_cleaned_text_strips_direction_and_formation(monkeypatch):
    """cleaned_text removes the leading clock, Shotgun/No-Huddle tags, and the
    short/deep/left/middle/right modifiers -- so 'rush middle for' collapses to
    'rush for' (otherwise the verb-anchored extractors miss it)."""
    df = _run_fixture(monkeypatch, 401754598)
    assert "cleaned_text" in df.columns
    # raw text carries the direction word; cleaned_text must not
    assert df.filter(pl.col("text").str.contains("(?i)rush middle for")).height > 0
    assert df.filter(pl.col("cleaned_text").str.contains("(?i)rush (?:left|middle|right) for")).height == 0
    # leading game clock stripped
    assert df.filter(pl.col("cleaned_text").str.contains(r"^\(\d{1,2}:\d{2}\)")).height == 0


def test_rush_yardage_populated_on_direction_word_text(monkeypatch):
    """401754598's rushes are all 'rush {dir} for N' -- raw-text extraction left
    yds_rushed null for the entire game; cleaned_text restores it."""
    df = _run_fixture(monkeypatch, 401754598)
    rushes = df.filter(pl.col("rush") == True)
    assert rushes.height > 0
    assert rushes["yds_rushed"].drop_nulls().len() > 0, "yds_rushed still null on direction-word rushes"


def test_receiving_yardage_populated_on_direction_word_text(monkeypatch):
    """'pass complete short middle to ...' broke the 'complete to' guard on raw
    text; cleaned_text restores yds_receiving on completions."""
    df = _run_fixture(monkeypatch, 401754598)
    completions = df.filter(pl.col("completion") == True)
    assert completions.height > 0
    assert completions["yds_receiving"].drop_nulls().len() > 0, "yds_receiving null on 'complete short middle to'"


def test_kneel_down_detected_and_excluded_from_scrimmage(monkeypatch):
    """A blown-out game ends in a kneel; it must flag kneel_down=True and be
    excluded from scrimmage_play (so it doesn't dilute rush/EPA/drive stats)."""
    df = _run_fixture(monkeypatch, 401628455)
    assert "kneel_down" in df.columns
    kneels = df.filter(pl.col("kneel_down") == True)
    assert kneels.height >= 1, "expected >=1 kneel-down in a blowout"
    assert (kneels["scrimmage_play"] == False).all(), "kneel-downs must not be scrimmage plays"
    # control: genuine rushes stay scrimmage plays
    normal_rush = df.filter((pl.col("type.text") == "Rush").and_(pl.col("kneel_down") == False))
    assert normal_rush.height > 0
    assert (normal_rush["scrimmage_play"] == True).all()


def test_kneel_down_does_not_perturb_per_play_epa(monkeypatch):
    """Kneel exclusion changes scrimmage-filtered AGGREGATES, not the per-play EP
    column -- every play still carries an EPA value."""
    df = _run_fixture(monkeypatch, 401628455)
    assert df["EPA"].drop_nulls().len() == df.height


# --- Tier B: EPA/WPA-input fixes (ported with before/after measurement) ---


def test_b1_kickoff_fair_catch_touchback(monkeypatch):
    """B1: a 2018+ fair-caught kickoff is a touchback -> kickoff_tb True and the
    receiving offense starts at its 25 (end.yardsToEndzone == 75)."""
    df = _run_fixture(monkeypatch, 401677179)  # 2024 IU game w/ fair-caught kickoffs
    fc = df.filter((pl.col("kickoff_play") == True).and_(pl.col("text").str.contains("(?i)fair c")))
    assert fc.height >= 1, "fixture must contain a fair-caught kickoff"
    assert (fc["kickoff_tb"] == True).all()
    assert (fc["end.yardsToEndzone"] == 75).all()


def test_b2_errored_punt_end_yardline(monkeypatch):
    """B2: a punt that changed possession gets end.yardsToEndzone from the next
    play's start (M.Nichols punt -> SU11 -> 89); a touchback punt stays 80."""
    df = _run_fixture(monkeypatch, 401754571)
    nichols = df.filter(pl.col("text").str.contains(r"(?i)M\.Nichols punt 57 yards to the SU22"))
    assert nichols.height == 1
    assert nichols["end.yardsToEndzone"][0] == 89
    tb = df.filter((pl.col("punt") == True).and_(pl.col("punt_tb") == True))
    assert tb.height >= 1
    assert (tb["end.yardsToEndzone"] == 80).all(), "punt_tb override must still win over the B2 flip"


def test_b3_missing_end_state_fill(monkeypatch):
    """B3: end-state-missing plays are attributed to the team that took over
    (end.pos_team.id non-null) and the yardline is backfilled (Haynes -> 2)."""
    df = _run_fixture(monkeypatch, 401754579)
    miss = df.filter(pl.col("end_state_missing") == True)
    assert miss.height >= 1, "fixture must contain end-state-missing plays"
    assert miss["end.pos_team.id"].drop_nulls().len() == miss.height
    haynes = df.filter(pl.col("text").str.contains(r"(?i)Haynes rush middle for 0 yards"))
    assert haynes.height == 1
    assert haynes["end.yardsToEndzone"][0] == 2


def test_b4_ot_plays_sorted_by_sequence(monkeypatch):
    """B4: overtime plays (period.number >= 5) are ordered by sequenceNumber."""
    df = _run_fixture(monkeypatch, 401754543)
    ot = df.filter(pl.col("period.number").cast(pl.Int32, strict=False) >= 5)
    assert ot.height >= 2, "fixture must contain overtime plays"
    seq = ot["sequenceNumber"].cast(pl.Int64, strict=False).to_list()
    assert seq == sorted(seq), "OT plays must be ordered by sequenceNumber"


# --- B5: penalty assessed between a scoring play and the ensuing kickoff ---
#
# The 2024 USC/LSU edge: ESPN logs a penalty (e.g. unsportsmanlike conduct) as a
# play sitting BETWEEN a scoring play and the ensuing kickoff, and attributes a
# score-diff change to it. Left alone the penalty inherits the prior play's field
# position and gets a large spurious EPA/WPA. 0.36-live's `penalty_assessed_on_kickoff`
# flags it and applies the kickoff-touchback treatment (start/end at the 25, 1st & 10,
# wp_before -> wp_touchback), neutralizing the spurious value.
#
# Reconciliation (see dev/cfb_036live_reconciliation_status_2026-06-29.md): this flag
# is DISJOINT from main's existing `kickoff_vec & penalty_in_text` EP path (a Kickoff
# row carrying penalty text) -- overlap is 0 on every fixture. The literal 0.36-live
# predicate ALSO matches Timeouts that sit between a score and a kickoff; main already
# scores those EPA=0, so the port adds a `penalty_flag` guard to exclude them.


def test_b5_flag_targets_genuine_penalty_between_score_and_kickoff(monkeypatch):
    """USC/LSU 2024 has TWO unsportsmanlike-conduct penalties assessed between a
    scoring play and the ensuing kickoff; both are flagged, both are Penalty rows."""
    df = _run_fixture(monkeypatch, 401628334)
    assert "penalty_assessed_on_kickoff" in df.columns
    flagged = df.filter(pl.col("penalty_assessed_on_kickoff") == True)
    assert flagged.height == 2, "USC/LSU: 2 penalties between a score and a kickoff"
    assert (flagged["type.text"] == "Penalty").all()
    # disjoint from main's existing kickoff-penalty EP path (no double-handling)
    overlap = df.filter(
        (pl.col("penalty_assessed_on_kickoff") == True).and_(pl.col("type.text").is_in(kickoff_vec)),
    )
    assert overlap.height == 0


def test_b5_penalty_flag_guard_excludes_timeouts(monkeypatch):
    """The literal 0.36-live predicate flags Timeouts that sit between a score and a
    kickoff; the penalty_flag guard must exclude them. 401754543 has 5 such timeouts;
    none may be flagged, and each keeps its correct EPA=0 / unchanged wp."""
    df = _run_fixture(monkeypatch, 401754543)
    assert df.filter(pl.col("penalty_assessed_on_kickoff") == True).height == 0
    timeouts = df.filter(pl.col("type.text") == "Timeout")
    assert timeouts.height >= 1
    assert (timeouts["EPA"] == 0).all(), "main already scores Timeouts EPA=0"


def test_b5_touchback_treatment_neutralizes_spurious_epa(monkeypatch):
    """The flagged penalty gets touchback field position (start & end yards-to-goal
    == 75, 1st & 10) and its spurious EPA collapses (LSU's was -3.31 pre-fix)."""
    df = _run_fixture(monkeypatch, 401628334)
    flagged = df.filter(pl.col("penalty_assessed_on_kickoff") == True)
    assert (flagged["start.yardsToEndzone"] == 75).all()
    assert (flagged["end.yardsToEndzone"] == 75).all()
    assert (flagged["start.down"] == 1).all()
    assert (flagged["start.distance"] == 10).all()
    # neutralized: spurious pre-fix magnitude was 3.31; post-fix is small
    assert flagged["EPA"].abs().max() < 1.0


def test_b5_wp_before_substituted_with_touchback(monkeypatch):
    """wp_before is replaced by the touchback WP for the flagged play (the 0.36-live
    L5095 substitution, ported by extending _apply_wp_derivation's kickoff branch)."""
    df = _run_fixture(monkeypatch, 401628334)
    flagged = df.filter(pl.col("penalty_assessed_on_kickoff") == True)
    assert (flagged["wp_before"] - flagged["wp_touchback"]).abs().max() < 1e-9


def test_b5_akron_delay_of_game_no_play_neutralized(monkeypatch):
    """401628455's OSU delay-of-game NO PLAY (a Penalty between a pick-six and the
    kickoff) is the single flagged play; its spurious +2.18 EPA is neutralized."""
    df = _run_fixture(monkeypatch, 401628455)
    flagged = df.filter(pl.col("penalty_assessed_on_kickoff") == True)
    assert flagged.height == 1
    assert flagged["EPA"].abs().max() < 1.0


def test_b5_no_flag_on_penalty_on_kickoff_games(monkeypatch):
    """Games whose only KO-penalty plays are penalty-ON-kickoff (main's existing
    path) must NOT trip the new flag -- guarantees zero collateral movement."""
    for gid in (400869270, 401236002):
        df = _run_fixture(monkeypatch, gid)
        assert df.filter(pl.col("penalty_assessed_on_kickoff") == True).height == 0


# --- A3: interception return yardage must not count as offense ---
#
# ESPN reports a pick-six's RETURN yardage in `statYardage` on the (offensive)
# interception play, so the throwing offense's yardage totals are inflated by the
# defense's return. 0.36-live zeroed `statYardage` on interceptions; main can't --
# it derives penalty residuals from `statYardage` (e.g. `statYardage - yds_int_return`)
# -- so the fix instead EXCLUDES interception plays at the offensive-yardage
# AGGREGATION sites (box `off_yards`/`total_off_yards`/`yards_per_play` and the
# per-drive `drive_offense_yards`), leaving the shared column untouched.
#
# 401628455: pos_team 2006 (OSU) throws a 29-yd pick-six ("Gabe Powers 29 Yd
# Interception Return") plus one plain INT; its scrimmage off_yards is 140 (29
# inflated) and must drop to 111. pos_team 194 (Akron, 0 INT) is unchanged at 251.

_A3_GID = 401628455
_A3_THROWING_TEAM = 2006
_A3_OTHER_TEAM = 194


def test_a3_interception_statyardage_column_not_mutated(monkeypatch):
    """The per-play `statYardage` is the shared source for penalty-residual
    derivation, so A3 must NOT zero it: the pick-six row keeps statYardage == 29."""
    df = _run_fixture(monkeypatch, _A3_GID)
    pick6 = df.filter(pl.col("text").str.contains(r"(?i)Gabe Powers 29 Yd Interception Return"))
    assert pick6.height == 1
    assert pick6["statYardage"][0] == 29, "A3 must not mutate the shared statYardage column"
    assert pick6["int"][0] is True


def test_a3_interception_excluded_from_drive_offense_yards(monkeypatch):
    """The pick-six is a scrimmage play, so without the fix its 29 return yards land
    in `drive_offense_yards`; A3 zeroes that contribution (drive totals stay clean)."""
    df = _run_fixture(monkeypatch, _A3_GID)
    pick6 = df.filter(pl.col("text").str.contains(r"(?i)Gabe Powers 29 Yd Interception Return"))
    assert pick6["drive_offense_yards"][0] == 0


def test_a3_box_off_yards_excludes_interception_return(monkeypatch):
    """The throwing team's box `off_yards`/`total_off_yards` equal its all-scrimmage
    statYardage sum MINUS the pick-six return (29) -- the exclusion contract, asserted
    as a relationship so it stays valid regardless of A5's completion reconstruction."""
    proc = _run_proc(monkeypatch, _A3_GID)
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    box = proc.create_box_score(df)
    team = {r["pos_team"]: r for r in box["team"]}

    scrim = df.filter((pl.col("scrimmage_play") == True).and_(pl.col("pos_team") == _A3_THROWING_TEAM))
    incl_int = scrim["statYardage"].sum()
    int_yds = scrim.filter(pl.col("int") == True)["statYardage"].sum()
    assert int_yds == 29, "the 29-yard pick-six return"
    assert team[_A3_THROWING_TEAM]["off_yards"] == incl_int - int_yds
    assert team[_A3_THROWING_TEAM]["total_off_yards"] == incl_int - int_yds
    # yards_per_play keeps the INT in the denominator as a 0-yard play
    assert team[_A3_THROWING_TEAM]["yards_per_play"] == pytest.approx((incl_int - int_yds) / scrim.height)

    # the 0-INT team's offense is untouched by the exclusion
    other = df.filter((pl.col("scrimmage_play") == True).and_(pl.col("pos_team") == _A3_OTHER_TEAM))
    assert other.filter(pl.col("int") == True).height == 0
    assert team[_A3_OTHER_TEAM]["off_yards"] == other["statYardage"].sum()


def test_a3_box_total_yards_excludes_interception_return(monkeypatch):
    """The team-level `total_yards` (all-plays statYardage sum) carried the same
    pick-six inflation as off_yards -- a 29-yard return attributed to the offense that
    threw it. Excluded for consistency with off_yards (otherwise total_yards < off_yards
    is impossible yet total_yards would wrongly exceed it by the return)."""
    proc = _run_proc(monkeypatch, _A3_GID)
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    box = proc.create_box_score(df)
    team = {r["pos_team"]: r for r in box["team"]}

    allp = df.filter(pl.col("pos_team") == _A3_THROWING_TEAM)
    incl_int = allp["statYardage"].sum()
    int_yds = allp.filter(pl.col("int") == True)["statYardage"].sum()
    assert int_yds == 29
    assert team[_A3_THROWING_TEAM]["total_yards"] == incl_int - int_yds
    # the 0-INT team's all-plays total is unaffected
    other = df.filter(pl.col("pos_team") == _A3_OTHER_TEAM)
    assert team[_A3_OTHER_TEAM]["total_yards"] == other["statYardage"].sum()


# --- A5: statYardage==0 completion reconstruction (bugged 2024-WK1 feeds) ---
#
# Some ESPN feeds (the 2024 Week 1 batch) report statYardage==0 on completions that
# actually gained yards (the text carries no "for N yards"). 0.36-live rebuilds the
# yardage from the yardline delta: `start.yardsToEndzone - end.yardsToEndzone` (same
# possessing team), or `start.yardsToEndzone - (100 - end.yardsToEndzone)` when the
# play flips possession. Ported at the top of `__add_yardage_cols` so it runs on RAW
# yardlines (before __process_epa clamps end-of-half to 99) and is guarded to
# non-penalty plays (`penalty_detail` null) so the penalty-residual chain is untouched.
#
# 401628455 (OSU/Akron, 2024 WK1) is such a bugged game: all 33 completions ship
# statYardage==0.

_A5_GID = 401628455


def test_a5_reconstructs_bugged_completion_statyardage(monkeypatch):
    """A same-team completion reported as statYardage==0 is rebuilt to the yardline
    delta (Jordon Simmons catch: 58 -> 52 == 6 yards)."""
    df = _run_fixture(monkeypatch, _A5_GID)
    play = df.filter(pl.col("text").str.contains(r"(?i)complete to Jordon Simmons"))
    assert play.height >= 1
    row = play.row(0, named=True)
    assert row["start.team.id"] == row["end.team.id"], "same-team branch"
    expected = row["start.yardsToEndzone"] - row["end.yardsToEndzone"]
    assert row["statYardage"] == expected
    assert row["statYardage"] > 0, "no longer the bugged 0"


def test_a5_no_bugged_zero_yardage_completions_remain(monkeypatch):
    """After reconstruction, no non-penalty 'complete to' completion that moved the
    ball still carries statYardage==0 (the whole bugged class is repaired)."""
    df = _run_fixture(monkeypatch, _A5_GID)
    # completions that moved the ball (excludes the rare caught-at-the-line 0-yarder).
    # NB: end.yardsToEndzone is clamped to 99 on TD/end-of-half plays in the final
    # frame, so we can't recompute the raw delta here -- the robust invariant is that
    # NONE are left at the bugged statYardage==0.
    comps = df.filter(
        (pl.col("completion") == True)
        .and_(pl.col("penalty_detail").is_null())
        .and_(pl.col("start.yardsToEndzone") != pl.col("end.yardsToEndzone")),
    )
    assert comps.height >= 20, "401628455 is the bugged 2024-WK1 game"
    assert comps.filter(pl.col("statYardage") == 0).height == 0, "bugged-zero class fully repaired"


def test_a5_does_not_touch_interception_statyardage(monkeypatch):
    """A5 only rebuilds 'complete to' completions; an interception return ('29 Yd
    Interception Return') is never a completion, so its statYardage stays put (29)."""
    df = _run_fixture(monkeypatch, _A5_GID)
    pick6 = df.filter(pl.col("text").str.contains(r"(?i)Gabe Powers 29 Yd Interception Return"))
    assert pick6.height == 1
    assert pick6["statYardage"][0] == 29


def test_a5_does_not_perturb_per_play_epa(monkeypatch):
    """statYardage is not an EP feature (EP reads yardlines), so reconstruction must
    leave every play's EPA intact and populated."""
    df = _run_fixture(monkeypatch, _A5_GID)
    assert df["EPA"].drop_nulls().len() == df.height
