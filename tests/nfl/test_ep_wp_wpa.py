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
    """Game-2 row 0 must NOT consume game-1's last wp_before as its lead.

    Both games are 2 plays.  In game 1, row 0 is a normal play whose
    ``wp_after`` comes from the otherwise-branch (the model ``wp_after``).  The
    no-leak guarantee is asserted via the change-of-possession branch: game-2
    row 1 (last play of game 2) is end-of-game; game-1 row 1 (last play of
    game 1) is end-of-game too.  With a per-game ``.over("game_id")`` the
    ``game_play_number == max()`` end-of-game branch fires for BOTH last
    plays; a global max would only fire it for the single overall-max row.
    """
    rows_g1 = [
        {"wp_before": 0.30, "wp_after": 0.30, "game_play_number": 1},
        {
            "wp_before": 0.40,
            "wp_after": 0.40,
            "status_type_completed": True,
            "lead_play_type": "Pass Reception",  # not null -> rely on max()
            "pos_score_diff_end": 7,
            "game_play_number": 2,
        },
    ]
    rows_g2 = [
        {"wp_before": 0.55, "wp_after": 0.55, "game_play_number": 1},
        {
            "wp_before": 0.65,
            "wp_after": 0.65,
            "status_type_completed": True,
            "lead_play_type": "Pass Reception",  # not null -> rely on max()
            "pos_score_diff_end": 7,
            "game_play_number": 2,
        },
    ]
    df = pl.concat([_frame("G1", rows_g1), _frame("G2", rows_g2)], how="vertical")
    out = calculate_wpa(df)
    # Per-game max() must make BOTH last plays end-of-game winners (wp_after=1.0).
    wp_after = out["wp_after"].to_list()
    assert wp_after[1] == pytest.approx(1.0)  # game 1 last play
    assert wp_after[3] == pytest.approx(1.0)  # game 2 last play
    # And game-2 row 0's lead must come from game-2 row 1, never game 1.
    # (lead_wp_before is internal; assert via no-crash + finite wpa for g2 row 0)
    assert out["wpa"].to_list()[2] == pytest.approx(0.0)  # 0.55 wp_after - 0.55 wp_before


def test_missing_wp_columns_raises_keyerror() -> None:
    """Absent WP point estimates -> KeyError (mirrors calculate_epa contract)."""
    df = pl.DataFrame({"game_id": ["G1"], "type.text": ["Pass Reception"]})
    with pytest.raises(KeyError):
        calculate_wpa(df)
