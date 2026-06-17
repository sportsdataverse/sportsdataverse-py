"""Offline tests for ``sportsdataverse.nfl.ep_wp.calculate_epa``.

``calculate_epa`` lifts the EPA-*derivation* half of
``NFLPlayProcess.__process_epa`` into a shared, model-free function.  It
consumes a frame that **already** carries the scored EP point estimates under
the ESPN-internal names (``EP_start`` / ``EP_end`` / ``EP_start_touchback``)
plus the play-classification / flag columns, and derives ``EPA`` (nflfastR
lead-difference + scoring overlays).  Because the EP point estimates are
hand-supplied in these fixtures, every expected ``epa`` value is computable by
hand — no XGBoost model files are required.

Tests
-----
* A single-game frame exercising a normal gain, a TD (``7 - ep``), a FG
  (``3 - ep``), a turnover (sign flip), a kickoff (touchback EP), an
  end-of-half play (``-EP_start``), a penalty (``EP_between``) and a timeout
  (``EPA = 0``).
* A two-game concatenated frame asserting NO cross-game leak: the first play
  of game 2 must not borrow game 1's last ``EP_end`` as its lag.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import calculate_epa


# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------

# Column superset the derivation references.  Every flag defaults to a benign
# value; individual rows override only what they need.
_DEFAULT_ROW = {
    "type.text": "Pass Reception",
    "text": "",
    "EP_start": 0.0,
    "EP_end": 0.0,
    "EP_start_touchback": 0.0,
    "change_of_pos_team": False,
    "downs_turnover": False,
    "kickoff_onside": False,
    "scoring_play": False,
    "end_of_half": False,
    "penalty_in_text": False,
}


def _frame(game_id: str, rows: list[dict]) -> pl.DataFrame:
    """Build a single-game frame from partial row dicts (defaults filled in)."""
    full = []
    for r in rows:
        merged = dict(_DEFAULT_ROW)
        merged.update(r)
        merged["game_id"] = game_id
        full.append(merged)
    return pl.DataFrame(full)


# ---------------------------------------------------------------------------
# Single-game derivation
# ---------------------------------------------------------------------------


def test_normal_gain_is_lead_difference() -> None:
    """A non-scoring scrimmage play: EPA = EP_end - EP_start."""
    df = _frame(
        "G1",
        [
            # play 0: nothing before it -> lag_EP_end is null -> EP_between
            #         uses EP_start - null; the normal-play branch only cares
            #         about EP_end - EP_start = 3.0 - 1.0 = 2.0
            {"type.text": "Pass Reception", "EP_start": 1.0, "EP_end": 3.0},
        ],
    )
    out = calculate_epa(df)
    assert out["epa"].to_list() == pytest.approx([2.0])
    assert out["ep"].to_list() == pytest.approx([3.0])  # ep == ep_end


def test_touchdown_overlay() -> None:
    """Offense TD (kick PAT good) -> EP_end overlaid to 7; EPA = 7 - EP_start."""
    df = _frame(
        "G1",
        [
            {
                "type.text": "Passing Touchdown",
                "text": "Pass complete for a touchdown (Kicker 30 yard field goal kick)",
                "EP_start": 4.0,
                "EP_end": 5.0,  # ignored — overlay forces EP_end = 7
                "scoring_play": True,
            },
        ],
    )
    out = calculate_epa(df)
    # 7 (overlay) - 4 (EP_start) = 3
    assert out["epa"].to_list() == pytest.approx([3.0])


def test_field_goal_overlay() -> None:
    """Made FG -> EP_end overlaid to 3; EPA = 3 - EP_start."""
    df = _frame(
        "G1",
        [
            {
                "type.text": "Field Goal Good",
                "text": "30 yard field goal is good",
                "EP_start": 2.5,
                "EP_end": 1.0,  # ignored — overlay forces EP_end = 3
                "scoring_play": True,
            },
        ],
    )
    out = calculate_epa(df)
    assert out["epa"].to_list() == pytest.approx([0.5])  # 3 - 2.5


def test_turnover_sign_flip() -> None:
    """A turnover (end_change_vec) flips EP_end sign; EPA = -EP_end - EP_start."""
    df = _frame(
        "G1",
        [
            {
                "type.text": "Punt",  # in end_change_vec, not a kickoff
                "EP_start": 1.5,
                "EP_end": 2.0,  # flipped to -2.0
            },
        ],
    )
    out = calculate_epa(df)
    # EP_end becomes -2.0; EPA = -2.0 - 1.5 = -3.5
    assert out["epa"].to_list() == pytest.approx([-3.5])


def test_kickoff_uses_touchback_ep_start() -> None:
    """Kickoff -> EP_start replaced by EP_start_touchback."""
    df = _frame(
        "G1",
        [
            {
                "type.text": "Kickoff",
                "EP_start": 99.0,  # ignored — replaced by touchback
                "EP_start_touchback": 1.2,
                "EP_end": 0.7,
            },
        ],
    )
    out = calculate_epa(df)
    # EPA = EP_end - EP_start_touchback = 0.7 - 1.2 = -0.5
    assert out["epa"].to_list() == pytest.approx([-0.5])


def test_end_of_half_play() -> None:
    """Non-scoring end-of-half play: EPA = -EP_start."""
    df = _frame(
        "G1",
        [
            {"type.text": "Pass Incompletion", "EP_start": 1.0, "EP_end": 2.0},
            {
                "type.text": "Pass Incompletion",
                "EP_start": 1.3,
                "EP_end": 2.0,
                "end_of_half": True,
                "scoring_play": False,
            },
        ],
    )
    out = calculate_epa(df)
    assert out["epa"].to_list()[1] == pytest.approx(-1.3)


def test_timeout_is_zero() -> None:
    """A Timeout play always has EPA = 0."""
    df = _frame(
        "G1",
        [
            {"type.text": "Pass Reception", "EP_start": 1.0, "EP_end": 2.0},
            {"type.text": "Timeout", "EP_start": 5.0, "EP_end": 9.0},
        ],
    )
    out = calculate_epa(df)
    assert out["epa"].to_list()[1] == pytest.approx(0.0)


def test_penalty_uses_ep_between() -> None:
    """Penalty-in-text (non-kickoff, not type Penalty): EPA includes EP_between.

    With no possession change on the prior play, ``EP_between = EP_start -
    lag_EP_end`` and ``EPA = EP_end - EP_start + EP_between``.
    """
    df = _frame(
        "G1",
        [
            # prior play ends with EP_end = 2.0 (no possession change)
            {"type.text": "Rush", "EP_start": 1.0, "EP_end": 2.0},
            # penalty play: EP_start 2.2, EP_end 2.8, lag_EP_end = 2.0
            {
                "type.text": "Rush",
                "text": "holding penalty enforced",
                "EP_start": 2.2,
                "EP_end": 2.8,
                "penalty_in_text": True,
            },
        ],
    )
    out = calculate_epa(df)
    # EP_between = 2.2 - 2.0 = 0.2 ; EPA = 2.8 - 2.2 + 0.2 = 0.8
    assert out["epa"].to_list()[1] == pytest.approx(0.8)


def test_output_columns_present() -> None:
    """calculate_epa adds lowercase ep / epa / ep_start / ep_end."""
    df = _frame("G1", [{"EP_start": 1.0, "EP_end": 2.0}])
    out = calculate_epa(df)
    for col in ("ep", "epa", "ep_start", "ep_end"):
        assert col in out.columns


# ---------------------------------------------------------------------------
# Scoring-attempt EP_start = 0.92 override
# ---------------------------------------------------------------------------

_SCORING_ATTEMPT_TYPES = [
    ("Extra Point Good", 1),
    ("Extra Point Missed", 0),
    ("Two-Point Conversion Good", 2),
    ("Two-Point Conversion Missed", 0),
    ("Two Point Pass", 2),
    ("Two Point Rush", 2),
    ("Blocked PAT", 0),
]


@pytest.mark.parametrize("play_type,points_value", _SCORING_ATTEMPT_TYPES)
def test_scoring_attempt_ep_start_forced_to_0_92(play_type: str, points_value: int) -> None:
    """Scoring-attempt plays must use EP_start = 0.92 regardless of the supplied value.

    The model EP_start for PAT / 2pt / Blocked-PAT plays is meaningless; EPA
    must equal ``points_value - 0.92``.  This test supplies EP_start = 1.5
    (deliberately != 0.92) and EP_end = <points_value> so that without the
    override the branch would compute ``points_value - 1.5`` instead.

    RED before the Critical fix; GREEN after.
    """
    # EP_end is set to points_value so the scoring overlay fires and returns
    # exactly points_value (the overlay branches for Extra Point / 2pt land
    # on 1 or 2 respectively; Blocked PAT / missed land on 0).
    # We supply a non-0.92 EP_start to prove the override rewrites it.
    df = _frame(
        "G1",
        [
            {
                "type.text": play_type,
                "text": "",
                "EP_start": 1.5,  # deliberately != 0.92 — must be overridden
                "EP_end": float(points_value),
                "scoring_play": True,
            },
        ],
    )
    out = calculate_epa(df)
    expected_epa = pytest.approx(points_value - 0.92, abs=1e-9)
    assert out["epa"].to_list()[0] == expected_epa, (
        f"{play_type}: expected epa={points_value - 0.92:.4f} "
        f"(points_value={points_value} - 0.92), got {out['epa'].to_list()[0]}"
    )


# ---------------------------------------------------------------------------
# Multi-game no-leak invariant
# ---------------------------------------------------------------------------


def test_no_cross_game_leak() -> None:
    """First play of game 2 must NOT use game 1's last EP_end as its lag.

    Game 1's last play ends with EP_end = 6.0 (a huge value).  Game 2's first
    play is a penalty whose EPA depends on ``lag_EP_end`` via ``EP_between``.
    With a correct ``.shift(1).over("game_id")`` the lag for G2's first play is
    null (the boundary is respected); matching ``__process_epa`` verbatim
    (which does NOT null-fill ``lag_EP_end``), ``EP_between`` and therefore the
    penalty-branch ``EPA`` are null there.  Crucially they are NOT the leaked
    value that a global ``.shift(1)`` would have produced.

    Two assertions pin this down:

    * The G2 lag came from inside G2 (null), not from G1's 6.0.
    * A second G2 play (with a real in-game predecessor) computes a correct,
      non-leaked EPA.
    """
    g1 = _frame(
        "G1",
        [
            {"type.text": "Rush", "EP_start": 1.0, "EP_end": 2.0},
            {"type.text": "Rush", "EP_start": 3.0, "EP_end": 6.0},
        ],
    )
    g2 = _frame(
        "G2",
        [
            # G2 first play: lag must be null (no leak from G1's 6.0)
            {"type.text": "Rush", "EP_start": 0.5, "EP_end": 1.0},
            # G2 second play: penalty whose lag_EP_end is G2 row 0's EP_end (1.0)
            {
                "type.text": "Rush",
                "text": "holding penalty enforced",
                "EP_start": 2.2,
                "EP_end": 2.8,
                "penalty_in_text": True,
            },
        ],
    )
    out = calculate_epa(pl.concat([g1, g2], how="vertical"))
    g2 = out.filter(pl.col("game_id") == "G2")

    # No leak: G2 row 0 lag_EP_end is null (not G1's 6.0).
    assert g2["lag_EP_end"].to_list()[0] is None

    # G2 row 1 penalty: lag_EP_end = 1.0 (from G2 row 0, in-game), no possession
    # change -> EP_between = 2.2 - 1.0 = 1.2 ;
    # EPA = EP_end - EP_start + EP_between = 2.8 - 2.2 + 1.2 = 1.8
    g2_penalty_epa = g2["epa"].to_list()[1]
    assert g2_penalty_epa == pytest.approx(1.8)

    # Had the shift leaked across the boundary, G2 row 0 would have used G1's
    # 6.0 as its lag — corrupting this in-game chain.  Assert we are NOT that:
    # a leak would give EP_between = 2.2 - (-? )... the key guard is the null
    # boundary above plus the exact 1.8 here.
    assert g2_penalty_epa != pytest.approx(-3.2)
