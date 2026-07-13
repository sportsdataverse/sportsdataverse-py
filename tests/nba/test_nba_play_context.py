"""Tests for the NBA play-context engine (Cleaning the Glass recreation).

Oracle gates in this file (never lowered to make a test pass):

* **Transition frequency band** — CTG's own published Play-Context table shows
  team transition frequencies around 0.14 (e.g. Denver 14.3%), and Synergy's
  transition play-type frequency runs ~0.15-0.16 league-wide. At the calibrated
  ``DEFAULT_TRANSITION_SECONDS = 6.0`` the three committed engine fixtures produce
  a mean of **0.163** (per game 0.155 / 0.163 / 0.171); the gate is [0.12, 0.21].
  Fitting run: ``dev/ctg_transition_calibration.py``.
* **Start-type totality** — every possession gets a start type and a CTG bucket;
  the buckets are exactly CTG's five.
* **Putback** — asserted against a hand-verified real play (game 0022200001,
  ``order_index`` 9: Harris grabs his own offensive rebound and tips it in,
  unassisted) rather than a synthetic fixture.
* **Garbage-time honesty** — the margin-only flag must declare itself as such.
"""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nba import nba_play_context_constants as C
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import (
    PLAY_CONTEXT_SHOTS_SCHEMA,
    add_ctg_shot_zones,
    add_play_context,
    add_start_type_detail,
    add_transition,
    build_play_context_shots,
    flag_garbage_time,
    flag_heave_possessions,
    team_play_context,
)
from sportsdataverse.nba.nba_possessions import build_possessions

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022200001", "0022300001", "0022100001"]

# Observed on the fixtures at transition_seconds=6.0 (dev/ctg_transition_calibration.py).
OBSERVED_TRANSITION_FREQ_MEAN = 0.163
TRANSITION_FREQ_GATE = (0.12, 0.21)


def _enh(game_id: str) -> pl.DataFrame:
    payload = json.loads((FXROOT / game_id / "playbyplayv3.json").read_text())
    return enhanced_pbp_from_payload(payload)


@pytest.fixture(scope="module")
def frames() -> dict[str, pl.DataFrame]:
    return {gid: _enh(gid) for gid in GAMES}


# ---------------------------------------------------------------------------
# Shot zones
# ---------------------------------------------------------------------------


def test_ctg_shot_zones_partition_every_field_goal(frames):
    pbp = add_ctg_shot_zones(frames["0022200001"])
    fg = pbp.filter(pl.col("is_field_goal") == 1)
    assert fg.height > 100
    # every FG gets a zone; no FG is left unclassified
    assert fg["ctg_shot_zone"].null_count() == 0
    assert set(fg["ctg_shot_zone"].unique()).issubset(set(C.CTG_SHOT_ZONES))
    # non-FG rows carry no zone
    assert pbp.filter(pl.col("is_field_goal") != 1)["ctg_shot_zone"].null_count() > 0


def test_ctg_zones_differ_from_official_nba_zones(frames):
    """CTG splits the midrange at the FT-line distance, not the paint boundary."""
    from sportsdataverse.nba.nba_play_context import _shot_distance_ft
    from sportsdataverse.nba.nba_shot_zones import add_shot_zones

    pbp = add_ctg_shot_zones(add_shot_zones(frames["0022200001"]))
    fg = pbp.filter(pl.col("is_field_goal") == 1)
    # A 2pt shot 4-7 ft out is `in_the_paint_non_ra` officially but `short_mid` in CTG.
    # Select on the EXACT coordinate distance the classifier uses, not the rounded
    # v3 ``shot_distance`` column — a shot the feed rounds to 4 ft can truly be 3.6 ft
    # (correctly ``at_rim``), so the rounded column straddles the 4-foot rim boundary.
    both = fg.filter((pl.col("shot_value") == 2) & _shot_distance_ft().is_between(4, 7))
    if both.height:
        assert set(both["ctg_shot_zone"].unique()) == {"short_mid"}


def test_ctg_shot_zones_empty_input_returns_schema():
    empty = pl.DataFrame(schema=dict(frames_schema()))
    out = add_ctg_shot_zones(empty)
    assert "ctg_shot_zone" in out.columns
    assert out.height == 0


def frames_schema():
    return _enh(GAMES[0]).schema


# ---------------------------------------------------------------------------
# Start-type taxonomy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_start_type_detail_is_total_and_buckets_are_ctg(frames, game_id):
    enh = frames[game_id]
    poss = add_start_type_detail(build_possessions(enh), enh)

    assert poss["possession_start_type_detail"].null_count() == 0
    assert poss["possession_start_type_ctg"].null_count() == 0
    # exactly CTG's five buckets, nothing invented
    assert set(poss["possession_start_type_ctg"].unique()).issubset(set(C.CTG_START_BUCKETS))


def test_start_type_detail_is_zone_split(frames):
    """The whole point of the upgrade: makes/misses carry their shot zone."""
    enh = frames["0022200001"]
    poss = add_start_type_detail(build_possessions(enh), enh)
    detail = set(poss["possession_start_type_detail"].unique())

    # zone-qualified members are actually produced (the coarse engine could not)
    assert any(d.startswith("OffAtRim") for d in detail)
    assert any(d.startswith("OffArc3") for d in detail)
    assert any(d.startswith("OffCorner3") for d in detail)
    assert {"OffFTMake", "OffLiveBallTurnover", "OffTimeout", "OffDeadball"} & detail


def test_period_opening_possession_is_deadball(frames):
    enh = frames["0022200001"]
    poss = add_start_type_detail(build_possessions(enh), enh)
    openers = poss.filter(pl.col("number_in_period") == 1)
    assert openers.height >= 4  # at least one per regulation period
    assert set(openers["possession_start_type_detail"].unique()) == {C.START_TYPE_DEADBALL}


def test_timeout_beats_made_basket(frames):
    """pbpstats precedence: an after-timeout possession is OffTimeout, never OffXMake."""
    enh = frames["0022200001"]
    poss = add_start_type_detail(build_possessions(enh), enh)
    ato = poss.filter(pl.col("possession_start_type") == C.START_TYPE_TIMEOUT)
    assert ato.height > 0
    assert set(ato["possession_start_type_detail"].unique()) == {C.START_TYPE_TIMEOUT}
    assert set(ato["possession_start_type_ctg"].unique()) == {"off_timeout"}


# ---------------------------------------------------------------------------
# Transition — the calibrated oracle gate
# ---------------------------------------------------------------------------


def test_transition_frequency_matches_ctg_synergy_band(frames):
    """ORACLE GATE. CTG publishes ~0.14 transition freq; Synergy ~0.15-0.16.

    Observed mean on these three fixtures at the calibrated 6.0s default: 0.163.
    Gate = [0.12, 0.21]. If this fails, debug the transition rule (start reference,
    initial-play detection) — do NOT widen the gate.
    """
    freqs = []
    for gid in GAMES:
        enh = frames[gid]
        poss = add_play_context(enh)
        clean = poss.filter(
            (pl.col("count_as_possession") == True)  # noqa: E712
            & (pl.col("is_garbage_time") == False)  # noqa: E712
            & (pl.col("is_heave_possession") == False)  # noqa: E712
        )
        assert clean.height >= 150, "fixture shrank — gate would pass vacuously"
        freqs.append(clean["is_transition"].mean())

    mean = sum(freqs) / len(freqs)
    lo, hi = TRANSITION_FREQ_GATE
    assert lo <= mean <= hi, f"transition freq {mean:.3f} outside CTG/Synergy band {TRANSITION_FREQ_GATE}"
    assert mean == pytest.approx(OBSERVED_TRANSITION_FREQ_MEAN, abs=0.02)


def test_transition_seconds_is_monotonic(frames):
    """A longer window can only admit more transition possessions."""
    enh = frames["0022200001"]
    prev = -1.0
    for secs in (4.0, 6.0, 8.0, 10.0):
        poss = add_play_context(enh, transition_seconds=secs)
        freq = poss["is_transition"].mean()
        assert freq >= prev
        prev = freq


def test_hoop_math_10s_college_default_overshoots_nba(frames):
    """Documents WHY the default is 6.0s, not hoop-math's college 10s.

    The 10s rule is a college convention with a different denominator; on NBA
    possessions it roughly doubles CTG's published transition rate.
    """
    enh = frames["0022200001"]
    at_6 = add_play_context(enh, transition_seconds=6.0)["is_transition"].mean()
    at_10 = add_play_context(enh, transition_seconds=10.0)["is_transition"].mean()
    assert at_10 > 2 * TRANSITION_FREQ_GATE[1] * 0.75  # ~0.35 observed
    assert at_6 < at_10


def test_period_opener_never_transition(frames):
    poss = add_play_context(frames["0022200001"])
    openers = poss.filter(pl.col("number_in_period") == 1)
    assert not openers["is_transition"].any()


def test_timeout_start_never_transition(frames):
    """After a timeout the defense is set by construction — no variant may call it transition."""
    for variant in C.TRANSITION_VARIANTS:
        poss = add_play_context(frames["0022200001"], transition_variant=variant)
        ato = poss.filter(pl.col("possession_start_type_ctg") == "off_timeout")
        assert not ato["is_transition"].any(), variant


def test_haslametrics_variant_is_steal_only(frames):
    enh = frames["0022200001"]
    poss = add_play_context(enh, transition_variant="haslametrics")
    trans = poss.filter(pl.col("is_transition") == True)  # noqa: E712
    assert trans.height > 0
    assert set(trans["possession_start_type_ctg"].unique()) == {"off_steal"}


def test_bigballr_variant_excludes_deadball_starts(frames):
    poss = add_play_context(frames["0022200001"], transition_variant="bigballr")
    trans = poss.filter(pl.col("is_transition") == True)  # noqa: E712
    assert "off_deadball" not in set(trans["possession_start_type_ctg"].unique())


def test_unknown_transition_variant_raises(frames):
    enh = frames["0022200001"]
    poss = add_start_type_detail(build_possessions(enh), enh)
    with pytest.raises(ValueError, match="variant must be one of"):
        add_transition(poss, enh, variant="nope")


# ---------------------------------------------------------------------------
# Putbacks / second chance — asserted on a hand-verified REAL play
# ---------------------------------------------------------------------------


def test_putback_detected_on_real_verified_play(frames):
    """Game 0022200001, order_index 9: Harris rebounds his team's miss and tips it
    back in, unassisted, within 2s — a textbook putback. Verified by reading the
    raw pbp, not generated from the engine's own output.
    """
    enh = frames["0022200001"]
    poss = add_play_context(enh)
    shots = build_play_context_shots(poss, enh)

    row = shots.filter(pl.col("order_index") == 9)
    assert row.height == 1
    assert row["is_putback"][0] is True
    assert row["is_assisted"][0] is False
    assert row["shot_value"][0] == 2


def test_three_pointers_are_never_putbacks(frames):
    enh = frames["0022200001"]
    shots = build_play_context_shots(add_play_context(enh), enh)
    assert shots.filter((pl.col("shot_value") == 3) & (pl.col("is_putback") == True)).height == 0  # noqa: E712


def test_assisted_shots_are_never_putbacks(frames):
    enh = frames["0022200001"]
    shots = build_play_context_shots(add_play_context(enh), enh)
    assert shots.filter((pl.col("is_assisted") == True) & (pl.col("is_putback") == True)).height == 0  # noqa: E712


def test_transition_wins_over_putback(frames):
    """CTG, verbatim: a putback that came out of transition IS transition."""
    enh = frames["0022100001"]  # the fixture with the most putbacks (15)
    poss = add_play_context(enh)
    shots = build_play_context_shots(poss, enh)
    trans_putbacks = shots.filter((pl.col("is_putback") == True))  # noqa: E712
    # any putback inside a transition possession must be labelled transition, not putback
    for r in trans_putbacks.to_dicts():
        if poss.filter(pl.col("possession_number") == r["possession_number"])["is_transition"][0]:
            assert r["shot_context"] == "transition"
        else:
            assert r["shot_context"] == "putback"


def test_shots_schema_and_contexts(frames):
    enh = frames["0022200001"]
    shots = build_play_context_shots(add_play_context(enh), enh)
    assert shots.schema == PLAY_CONTEXT_SHOTS_SCHEMA
    assert set(shots["shot_context"].unique()).issubset(set(C.PLAY_CONTEXTS))
    assert shots.height > 100


def test_shots_empty_input_returns_schema():
    empty_poss = pl.DataFrame(schema={"possession_number": pl.Int64, "is_transition": pl.Boolean})
    out = build_play_context_shots(empty_poss, pl.DataFrame())
    assert out.schema == PLAY_CONTEXT_SHOTS_SCHEMA
    assert out.height == 0


# ---------------------------------------------------------------------------
# CTG default filters
# ---------------------------------------------------------------------------


def test_heave_possessions_are_end_of_first_three_quarters_only(frames):
    poss = flag_heave_possessions(build_possessions(frames["0022200001"]))
    heaves = poss.filter(pl.col("is_heave_possession") == True)  # noqa: E712
    assert heaves.height > 0
    assert set(heaves["period"].unique()).issubset(set(C.HEAVE_PERIODS))
    assert heaves["start_seconds_remaining"].max() <= C.HEAVE_POSSESSION_SECONDS
    # Q4 is exempt — a late Q4 possession is a real possession
    assert poss.filter((pl.col("period") == 4) & (pl.col("is_heave_possession") == True)).height == 0  # noqa: E712


def test_garbage_time_is_fourth_quarter_only_and_declares_its_basis(frames):
    enh = frames["0022100001"]  # a blowout (24 garbage-time possessions observed)
    poss = flag_garbage_time(build_possessions(enh), enh)

    gt = poss.filter(pl.col("is_garbage_time") == True)  # noqa: E712
    assert gt.height > 0
    assert set(gt["period"].unique()) == {C.GARBAGE_TIME_PERIOD}
    # honesty gate: without starter data the flag MUST declare itself margin-only
    assert set(poss["garbage_time_basis"].unique()) == {"margin_only"}


def test_garbage_time_respects_margin_bands(frames):
    """Every flagged possession must actually satisfy CTG's margin x minutes band."""
    enh = frames["0022100001"]
    poss = flag_garbage_time(build_possessions(enh), enh)
    score = (
        enh.sort("order_index")
        .with_columns(
            pl.col("score_home").cast(pl.Int64, strict=False).forward_fill().fill_null(0).alias("h"),
            pl.col("score_away").cast(pl.Int64, strict=False).forward_fill().fill_null(0).alias("a"),
        )
        .select("order_index", "h", "a")
    )
    margin = {r["order_index"]: abs(r["h"] - r["a"]) for r in score.to_dicts()}

    for r in poss.filter(pl.col("is_garbage_time") == True).to_dicts():  # noqa: E712
        clock = r["start_seconds_remaining"]
        m = margin[r["start_order_index"]]
        band = next(b for b in C.GARBAGE_TIME_BANDS if b[1] < clock <= b[0])
        assert m >= band[2], f"flagged at margin {m} but band requires >= {band[2]}"


def test_starters_clause_tightens_garbage_time(frames):
    """Supplying starter counts can only REMOVE possessions from garbage time."""
    enh = frames["0022100001"]
    base = flag_garbage_time(build_possessions(enh), enh)
    n_margin_only = base.filter(pl.col("is_garbage_time") == True).height  # noqa: E712

    # pretend the starters never left: every possession has 10 starters on the floor
    all_starters = {n: 10 for n in base["possession_number"].to_list()}
    strict = flag_garbage_time(build_possessions(enh), enh, starters_on_court=all_starters)
    assert strict.filter(pl.col("is_garbage_time") == True).height == 0  # noqa: E712
    assert set(strict["garbage_time_basis"].unique()) == {"margin_and_starters"}
    assert n_margin_only > 0  # the margin-only flag really is a superset


# ---------------------------------------------------------------------------
# Orchestrator + aggregation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_add_play_context_appends_every_column(frames, game_id):
    from sportsdataverse.nba.nba_play_context import PLAY_CONTEXT_POSSESSIONS_SCHEMA

    poss = add_play_context(frames[game_id])
    for col, dtype in PLAY_CONTEXT_POSSESSIONS_SCHEMA.items():
        assert col in poss.columns, col
        assert poss.schema[col] == dtype, col
    # additive: the possession engine's own columns survive untouched
    assert {"points", "offense_team_id", "count_as_possession"}.issubset(set(poss.columns))


def test_add_play_context_does_not_change_possession_count(frames):
    """Strictly additive — enrichment must not add or drop a possession."""
    enh = frames["0022200001"]
    assert add_play_context(enh).height == build_possessions(enh).height


def test_team_play_context_ptsadded_uses_league_baseline(frames):
    """Pts+/Poss = (team transition PPP - LEAGUE non-transition PPP) x freq."""
    poss = add_play_context(frames["0022200001"])
    ctx = team_play_context(poss, league_non_transition_ppp=100.0)
    assert ctx.height == 2

    for r in ctx.to_dicts():
        expected = (r["transition_pts_per_100"] - 100.0) * r["transition_freq"]
        assert r["transition_pts_added_per_100"] == pytest.approx(expected)
        assert 0.0 <= r["transition_freq"] <= 1.0
        assert r["poss"] > 50


def test_team_play_context_filters_are_on_by_default(frames):
    poss = add_play_context(frames["0022100001"])  # has garbage time
    filtered = team_play_context(poss)
    unfiltered = team_play_context(poss, apply_ctg_filters=False)
    assert unfiltered["poss"].sum() > filtered["poss"].sum()


def test_team_play_context_pandas_and_empty():
    out = team_play_context(pl.DataFrame(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert len(out) == 0
