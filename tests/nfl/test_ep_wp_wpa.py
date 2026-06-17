"""Offline tests for ``sportsdataverse.nfl.ep_wp.calculate_wpa``.

``calculate_wpa`` lifts the WPA-*derivation* half of
``NFLPlayProcess.__process_wpa`` into a shared, model-free function.  It
consumes a frame that **already** carries the scored win-probability point
estimates under the ESPN-internal names (``wp_before`` / ``wp_touchback`` /
``wp_after``) plus the play-classification / flag columns, and derives the
``wp_before`` / ``wp_after`` overlays, the posteam->home perspective columns,
the end-of-half / OT ``wp_after`` two-path, and ``wpa = wp_after - wp_before``.
Because the WP point estimates are hand-supplied in these fixtures, every
expected ``wpa`` value is computable by hand -- no XGBoost model files are
required.

Tests
-----
* A normal play: ``wpa == lead(home_wp) - home_wp`` from the posteam's
  perspective, and the posteam->home sign flip is exercised by mixing a
  posteam-is-home play with a posteam-is-away play.
* A kickoff play exercising the leading ``wp_before`` -> ``wp_touchback``
  overlay (the Task-2-style dropped-leading-block guard).
* An end-of-half / OT play exercising the ``wp_after`` two-path.
* A two-game concatenated frame asserting NO cross-game leak: the first play
  of game 2 must not borrow game 1's last ``wp_before`` as its lead, and the
  end-of-game ``game_play_number == max()`` branch must be scoped per game.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import calculate_wpa


# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------

# Column superset the derivation references.  Every flag defaults to a benign
# value; individual rows override only what they need.  ``homeTeamId`` defaults
# to "H"; set ``start.pos_team.id`` / ``end.pos_team.id`` to "H" (posteam is
# home) or "A" (posteam is away) per row to exercise the perspective flip.
_DEFAULT_ROW = {
    "type.text": "Pass Reception",
    "wp_before": 0.5,
    "wp_touchback": 0.5,
    "wp_after": 0.5,
    "homeTeamId": "H",
    "start.pos_team.id": "H",
    "end.pos_team.id": "H",
    "start.pos_team_receives_2H_kickoff": False,
    "change_of_pos_team": False,
    "scoringPlay": False,
    "kickoff_onside": False,
    "end_of_half": False,
    "status_type_completed": False,
    "pos_score_diff_end": 0,
    "lead_play_type": "Pass Reception",
    "lead_pos_team": "H",
    "game_play_number": 1,
}


def _frame(game_id: str, rows: list[dict]) -> pl.DataFrame:
    """Build a single-game frame from partial row dicts (defaults filled in)."""
    full = []
    for i, r in enumerate(rows):
        merged = dict(_DEFAULT_ROW)
        merged.update(r)
        merged["game_id"] = game_id
        merged.setdefault("game_play_number", i + 1)
        if "game_play_number" not in r:
            merged["game_play_number"] = i + 1
        full.append(merged)
    return pl.DataFrame(full)


# ---------------------------------------------------------------------------
# Single-game derivation
# ---------------------------------------------------------------------------


def test_normal_play_wpa_is_lead_home_wp_difference() -> None:
    """A normal mid-game play: wpa = wp_after - wp_before.

    Play 0 is a normal scrimmage play whose ``wp_after`` resolves (via the
    change-of-possession / scoring otherwise-branch) to the model ``wp_after``
    value supplied.  ``wpa = wp_after - wp_before``.
    """
    df = _frame(
        "G1",
        [
            # posteam == home; nothing special -> wp_after stays at 0.7
            {"wp_before": 0.6, "wp_after": 0.7},
            # a trailing benign play so play 0 is mid-game (has a lead)
            {"wp_before": 0.7, "wp_after": 0.7},
        ],
    )
    out = calculate_wpa(df)
    # play 0: wp_after (0.7) - wp_before (0.6) = 0.1
    assert out["wpa"].to_list()[0] == pytest.approx(0.1)
    # first-class lowercase aliases present
    assert "wp" in out.columns
    assert "def_wp" in out.columns
    assert "home_wp" in out.columns
    assert "away_wp" in out.columns


def test_posteam_to_home_perspective_flip() -> None:
    """home_wp / away_wp flip with the posteam->home perspective.

    For a posteam-is-home play, ``home_wp_before == wp_before`` and
    ``away_wp_before == 1 - wp_before``.  For a posteam-is-away play the two
    swap.  This is the headline team-flip sign assertion.
    """
    df = _frame(
        "G1",
        [
            # posteam is HOME
            {"wp_before": 0.62, "wp_after": 0.62, "start.pos_team.id": "H", "end.pos_team.id": "H"},
            # posteam is AWAY
            {"wp_before": 0.62, "wp_after": 0.62, "start.pos_team.id": "A", "end.pos_team.id": "A"},
        ],
    )
    out = calculate_wpa(df)
    # row 0 (posteam home): home_wp_before is the offense wp (0.62)
    assert out["home_wp_before"].to_list()[0] == pytest.approx(0.62)
    assert out["away_wp_before"].to_list()[0] == pytest.approx(1 - 0.62)
    # row 1 (posteam away): home_wp_before is the *defense* wp (1 - 0.62)
    assert out["home_wp_before"].to_list()[1] == pytest.approx(1 - 0.62)
    assert out["away_wp_before"].to_list()[1] == pytest.approx(0.62)
    # first-class home_wp mirrors home_wp_before
    assert out["home_wp"].to_list() == pytest.approx(out["home_wp_before"].to_list())


def test_kickoff_uses_touchback_wp_before() -> None:
    """Kickoff -> wp_before replaced by wp_touchback (leading overlay)."""
    df = _frame(
        "G1",
        [
            {
                "type.text": "Kickoff",
                "wp_before": 0.99,  # ignored -- replaced by touchback
                "wp_touchback": 0.40,
                "wp_after": 0.45,
            },
            {"wp_before": 0.45, "wp_after": 0.45},
        ],
    )
    out = calculate_wpa(df)
    # wp_before for the kickoff row is overlaid to the touchback value
    assert out["wp_before"].to_list()[0] == pytest.approx(0.40)
    # wpa = wp_after (0.45) - wp_before (0.40 touchback) = 0.05
    assert out["wpa"].to_list()[0] == pytest.approx(0.05)


def test_end_of_half_two_path() -> None:
    """End-of-half play exercises the wp_after two-path.

    When ``end_of_half`` is set, posteam unchanged and not a timeout, the
    ``wp_after`` is taken from ``lead_wp_before`` (the next play's pre-snap
    win probability) rather than the model ``wp_after``.
    """
    df = _frame(
        "G1",
        [
            # end-of-half play; posteam unchanged (start == lead_pos_team == "H")
            {
                "wp_before": 0.55,
                "wp_after": 0.99,  # ignored -- two-path takes lead_wp_before
                "end_of_half": True,
                "start.pos_team.id": "H",
                "lead_pos_team": "H",
            },
            # the lead play whose wp_before (0.61) flows into row 0's wp_after
            {"wp_before": 0.61, "wp_after": 0.61},
        ],
    )
    out = calculate_wpa(df)
    # row 0 wp_after == lead_wp_before == 0.61; wpa = 0.61 - 0.55 = 0.06
    assert out["wp_after"].to_list()[0] == pytest.approx(0.61)
    assert out["wpa"].to_list()[0] == pytest.approx(0.06)


def test_end_of_game_winner_gets_wp_after_one() -> None:
    """Final play of a completed game with a positive score diff -> wp_after = 1.0."""
    df = _frame(
        "G1",
        [
            {
                "wp_before": 0.80,
                "wp_after": 0.80,
                "status_type_completed": True,
                "lead_play_type": None,  # no next play -> end-of-game branch
                "pos_score_diff_end": 4,  # offense leads -> wins
                "game_play_number": 1,
            },
        ],
    )
    out = calculate_wpa(df)
    assert out["wp_after"].to_list()[0] == pytest.approx(1.0)
    # wpa = 1.0 - 0.80 = 0.20
    assert out["wpa"].to_list()[0] == pytest.approx(0.20)


# ---------------------------------------------------------------------------
# No cross-game leak
# ---------------------------------------------------------------------------


def test_no_cross_game_leak_on_lead() -> None:
    """Concatenated frame: per-game guards prevent WP leaking across game boundaries.

    **Why the old 2+2 fixture was not discriminating:** both games shared
    ``game_play_number`` spans {1, 2}, so the global ``max()`` equalled the
    per-game ``max()`` for every row — the ``.over("game_id")`` guard was never
    load-bearing.

    **This fixture is discriminating on BOTH guards:**

    Guard 1 — ``.max().over("game_id")``:
        Game 1 ends at ``game_play_number=3``; game 2 ends at
        ``game_play_number=5`` (the global max).  A global ``.max()`` would
        return 5 for every row, so game-1's last play (gpn=3) would NOT satisfy
        ``gpn == 5`` and the end-of-game branch would never fire → ``wp_after``
        stays at 0.45 instead of the expected 1.0.  The per-game
        ``.max().over("game_id")`` correctly returns 3 for game-1 rows, making
        the branch fire and producing ``wp_after=1.0``.

    Guard 2 — ``.shift(-1).over("game_id")``:
        Game-1 play 1 (index 1 in the concat) is a possession change with
        ``scoringPlay=False``.  Its ``wp_after = 1 - lead_wp_before``.  With
        ``.over("game_id")``, ``lead_wp_before`` comes from game-1 play 2 whose
        ``wp_before=0.45`` → ``wp_after = 1 - 0.45 = 0.55``.  Without the
        guard, the shift borrows the globally-next row (game-2 play 0) whose
        ``wp_before=0.60`` → ``wp_after = 1 - 0.60 = 0.40 ≠ 0.55``.
    """
    # Game 1: 3 plays (game_play_number 1..3).
    # Play 1 is a possession change (scoringPlay=False) — its wp_after depends
    # on lead_wp_before, making guard-2 load-bearing.
    # Play 2 is the end-of-game winner — its detection relies on per-game max,
    # making guard-1 load-bearing.
    rows_g1 = [
        # play 0 — normal mid-game play; wp_after stays at model value
        {"wp_before": 0.30, "wp_after": 0.30, "game_play_number": 1},
        # play 1 — possession change, no score → wp_after = 1 - lead_wp_before
        # With .over("game_id") lead_wp_before = game-1 play-2's wp_before = 0.45
        # Without the guard:       lead_wp_before = game-2 play-0's wp_before = 0.60
        {
            "wp_before": 0.40,
            "wp_after": 0.40,  # overwritten by change-of-possession branch
            "start.pos_team.id": "H",
            "end.pos_team.id": "A",  # possession changes
            "scoringPlay": False,
            "game_play_number": 2,
        },
        # play 2 — final play, completed game, offense leads → wp_after = 1.0
        # Relies on game_play_number == max().over("game_id") (=3), NOT global max (=5).
        {
            "wp_before": 0.45,
            "wp_after": 0.45,  # overwritten by end-of-game branch
            "status_type_completed": True,
            "lead_play_type": "Pass Reception",  # not null → relies on max() guard
            "pos_score_diff_end": 7,
            "game_play_number": 3,
        },
    ]
    # Game 2: 5 plays (game_play_number 1..5).
    # Global max is 5; per-game max for game-1 is 3 — that's the discriminating gap.
    rows_g2 = [
        {"wp_before": 0.60, "wp_after": 0.60, "game_play_number": 1},
        {"wp_before": 0.61, "wp_after": 0.61, "game_play_number": 2},
        {"wp_before": 0.62, "wp_after": 0.62, "game_play_number": 3},
        {"wp_before": 0.63, "wp_after": 0.63, "game_play_number": 4},
        {
            "wp_before": 0.65,
            "wp_after": 0.65,  # overwritten by end-of-game branch
            "status_type_completed": True,
            "lead_play_type": "Pass Reception",
            "pos_score_diff_end": 7,
            "game_play_number": 5,
        },
    ]
    df = pl.concat([_frame("G1", rows_g1), _frame("G2", rows_g2)], how="vertical")
    out = calculate_wpa(df)
    wp_after = out["wp_after"].to_list()

    # --- Guard 1: per-game max() makes BOTH final plays end-of-game winners ---
    # Game-1 index 2 (gpn=3 == per-game max 3) → 1.0
    assert wp_after[2] == pytest.approx(1.0), "game-1 last play must be wp_after=1.0 (per-game max guard)"
    # Game-2 index 7 (gpn=5 == per-game max 5 == global max) → 1.0
    assert wp_after[7] == pytest.approx(1.0), "game-2 last play must be wp_after=1.0"

    # --- Guard 2: shift(-1).over("game_id") scopes lead to within game 1 ---
    # Game-1 play 1 (index 1): possession change, scoringPlay=False.
    # wp_after = 1 - lead_wp_before; lead_wp_before = game-1 play-2's wp_before = 0.45
    # Expected wp_after = 1 - 0.45 = 0.55
    assert wp_after[1] == pytest.approx(0.55), (
        "game-1 possession-change play: wp_after must use game-1's own lead, not game-2's"
    )

    # --- No-leak: game-2 row 0's wpa is self-consistent (lead from game-2 only) ---
    # Game-2 play 0 (index 3): normal play, wp_before=0.60, wp_after stays at 0.60 → wpa=0.0
    assert out["wpa"].to_list()[3] == pytest.approx(0.0)


def test_change_of_possession_scoring_branches() -> None:
    """The last two wp_after branches: pos-team change with / without a score.

    When ``start.pos_team.id != end.pos_team.id``:
      * ``scoringPlay=False`` → ``wp_after = 1 - lead_wp_before``
        (turnover / pick / fumble: defense now has ball, so the new offense
        win probability is the complement of the old offense's lead wp)
      * ``scoringPlay=True``  → ``wp_after = lead_wp_before``
        (pick-six / fumble-TD etc.: scoring team retains the ball after the
        score; their lead wp is taken directly as the new offense wp)

    Hand-computed expected values:
        lead_wp_before is play-1's wp_before = 0.70 in both frames.
        non-scoring branch: wp_after = 1 - 0.70 = 0.30
        scoring branch:     wp_after =     0.70
    """
    # --- scoringPlay=False: turnover / change of possession without score ---
    df_no_score = _frame(
        "G1",
        [
            {
                "wp_before": 0.50,
                "wp_after": 0.99,  # ignored — overwritten by pos-change branch
                "start.pos_team.id": "H",
                "end.pos_team.id": "A",  # possession changes
                "scoringPlay": False,
            },
            # lead play — its wp_before (0.70) flows into play-0's wp_after
            {"wp_before": 0.70, "wp_after": 0.70},
        ],
    )
    out_no_score = calculate_wpa(df_no_score)
    # wp_after = 1 - lead_wp_before = 1 - 0.70 = 0.30
    assert out_no_score["wp_after"].to_list()[0] == pytest.approx(0.30)
    assert out_no_score["wpa"].to_list()[0] == pytest.approx(0.30 - 0.50)

    # --- scoringPlay=True: pick-six / fumble-TD style ---
    df_score = _frame(
        "G2",
        [
            {
                "wp_before": 0.50,
                "wp_after": 0.99,  # ignored — overwritten by pos-change branch
                "start.pos_team.id": "H",
                "end.pos_team.id": "A",  # possession changes (scoring team keeps ball)
                "scoringPlay": True,
            },
            # lead play — its wp_before (0.70) flows into play-0's wp_after
            {"wp_before": 0.70, "wp_after": 0.70},
        ],
    )
    out_score = calculate_wpa(df_score)
    # wp_after = lead_wp_before = 0.70
    assert out_score["wp_after"].to_list()[0] == pytest.approx(0.70)
    assert out_score["wpa"].to_list()[0] == pytest.approx(0.70 - 0.50)


def test_missing_wp_columns_raises_keyerror() -> None:
    """Absent WP point estimates -> KeyError (mirrors calculate_epa contract)."""
    df = pl.DataFrame({"game_id": ["G1"], "type.text": ["Pass Reception"]})
    with pytest.raises(KeyError):
        calculate_wpa(df)
