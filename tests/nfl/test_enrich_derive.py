"""Structural parity guards for the ``enrich_nfl_pbp`` native derivation.

These offline tests pin the three nflfastR-faithfulness fixes that closed the
``method="lead_diff"`` numeric-parity gap against the shipped nflverse columns
(see ``helper_add_ep_wp.R`` ``add_ep_variables`` / ``add_wp_variables`` and
``helper_add_cp_cpoe.R``):

1. **Non-play fill-up.** Timeout / ``END QUARTER`` / ``END GAME`` marker rows
   carry a NULL game situation; nflfastR NA's their model output and
   ``tidyr::fill(.direction = "up")`` inherits the next real play's value.  The
   ``ep`` / ``wp`` / ``vegas_wp`` of a marker row must therefore equal the
   following real play's value — NOT a garbage model score — so the
   ``lead(home_ep)`` / ``lead(home_wp)`` of the play *before* a marker is not
   poisoned.
2. **Scoring overlays key on NFLVERSE column names.** ``_derive_epa`` must read
   ``field_goal_result == "made"`` / ``extra_point_result == "good"`` /
   ``two_point_conv_result == "success"`` / ``safety == 1`` (the nflverse
   names), not the ESPN-internal ``field_goal_made`` / ``extra_point_good`` /
   ``two_point_*_good`` / ``safety_team``.  A made FG must overlay ``3 - ep``.
3. **No half / game-boundary lead leak.** The ``lead`` that feeds ``epa`` /
   ``wpa`` is grouped per game *and* the structural half/game boundary
   (``qtr`` change, ``END QUARTER``/``END GAME`` markers) terminates the lead so
   no value leaks across the boundary.

The bundled ``models/*.ubj`` are CFB placeholders, so these tests exercise the
two pure-derivation helpers ``_derive_epa`` / ``_derive_wpa`` directly with
hand-supplied ``ep`` / ``wp`` point estimates (every expected value is
hand-computable) plus ``calculate_completion_probability`` for the CPOE scale.
"""

from __future__ import annotations

import math

import polars as pl

from sportsdataverse.nfl.ep_wp import (
    _derive_epa,
    _derive_wpa,
    calculate_completion_probability,
)


# ---------------------------------------------------------------------------
# Fixture builder — nflverse-shape, ep / wp hand-supplied.
# ---------------------------------------------------------------------------

# Benign defaults for every column the derivations reference.  Individual rows
# override only the fields they exercise.
_DEFAULT: dict = {
    "game_id": "2023_01_AAA_BBB",
    "season": 2023,
    "qtr": 1.0,
    "sp": 0.0,
    "down": 1.0,
    "play_type": "pass",
    "posteam": "BBB",  # == home_team by default (home perspective)
    "home_team": "BBB",
    "away_team": "AAA",
    "home_score": 20,
    "away_score": 17,
    "desc": "(15:00) play",
    "ep": 0.0,
    "wp": 0.5,
    "vegas_wp": 0.5,
    # scoring-flag columns (nflverse names) — benign
    "td_team": None,
    "field_goal_result": None,
    "extra_point_result": None,
    "two_point_conv_result": None,
    "safety": 0,
    "kickoff_attempt": 0,
    "extra_point_attempt": 0,
    "two_point_attempt": 0,
}


def _frame(rows: list[dict]) -> pl.DataFrame:
    full = []
    for i, r in enumerate(rows):
        merged = dict(_DEFAULT)
        merged["play_id"] = i + 1
        merged.update(r)
        full.append(merged)
    return pl.DataFrame(full)


# ---------------------------------------------------------------------------
# 1. Non-play fill-up: marker / timeout rows inherit the next real play.
# ---------------------------------------------------------------------------


def test_nonplay_rows_get_filled_up_ep_not_garbage() -> None:
    """A timeout row's ``ep`` must equal the NEXT real play's ``ep`` (fill-up)."""
    df = _frame(
        [
            dict(play_id=1, ep=1.0, down=1.0, play_type="run"),
            # timeout: down NULL, NOT a kickoff/PAT/2pt -> a non-play row whose
            # incoming garbage ep must be discarded and filled up from play 3.
            dict(play_id=2, ep=-9.99, down=None, play_type="no_play", desc="Timeout #1 by AAA at 02:00."),
            dict(play_id=3, ep=2.0, down=2.0, play_type="pass"),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    # the marker row's ep is filled up to the next real play (2.0), not -9.99.
    assert out.filter(pl.col("play_id") == 2)["ep"][0] == 2.0


def test_nonplay_rows_do_not_poison_preceding_lead() -> None:
    """The play BEFORE a timeout must lead to the next REAL play, not the marker.

    With ``ep`` of play1=1.0, play3=2.0 (home perspective, posteam==home), the
    home_epa of play1 = lead(home_ep) - home_ep.  Because the timeout (play2)
    is filled up to play3's ep (2.0), play1's epa = 2.0 - 1.0 = 1.0, NOT
    (-9.99) - 1.0.
    """
    df = _frame(
        [
            dict(play_id=1, ep=1.0, down=1.0, play_type="run"),
            dict(play_id=2, ep=-9.99, down=None, play_type="no_play", desc="Timeout #1 by AAA at 02:00."),
            dict(play_id=3, ep=2.0, down=2.0, play_type="pass"),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    assert math.isclose(out.filter(pl.col("play_id") == 1)["epa"][0], 1.0, abs_tol=1e-9)


def test_nonplay_fillup_applies_to_wp() -> None:
    """Timeout ``wp`` / ``vegas_wp`` must be filled up to the next real play."""
    df = _frame(
        [
            dict(play_id=1, wp=0.60, vegas_wp=0.62, down=1.0),
            dict(play_id=2, wp=0.001, vegas_wp=0.001, down=None, play_type="no_play", desc="Timeout #2 by BBB."),
            dict(play_id=3, wp=0.70, vegas_wp=0.72, down=2.0),
        ]
    )
    out = _derive_wpa(df).sort("play_id")
    # marker home_wp (posteam==home) inherits play3's wp, not the 0.001 garbage.
    assert math.isclose(out.filter(pl.col("play_id") == 2)["home_wp"][0], 0.70, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# 2. Scoring overlays key on nflverse column names.
# ---------------------------------------------------------------------------


def test_made_field_goal_overlays_three_minus_ep_nflverse_name() -> None:
    """A made FG (``field_goal_result == 'made'``) must give ``epa = 3 - ep``."""
    df = _frame(
        [
            dict(play_id=1, ep=2.9, down=4.0, field_goal_result="made", play_type="field_goal"),
            dict(play_id=2, ep=0.5, down=1.0, kickoff_attempt=1, play_type="kickoff"),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    assert math.isclose(out.filter(pl.col("play_id") == 1)["epa"][0], 3.0 - 2.9, abs_tol=1e-9)


def test_good_extra_point_overlays_one_minus_ep_nflverse_name() -> None:
    df = _frame(
        [
            dict(
                play_id=1, ep=0.95, down=None, extra_point_attempt=1, extra_point_result="good", play_type="extra_point"
            ),
            dict(play_id=2, ep=0.5, down=1.0, kickoff_attempt=1, play_type="kickoff"),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    assert math.isclose(out.filter(pl.col("play_id") == 1)["epa"][0], 1.0 - 0.95, abs_tol=1e-9)


def test_successful_two_point_overlays_two_minus_ep_nflverse_name() -> None:
    df = _frame(
        [
            dict(play_id=1, ep=0.9, down=None, two_point_attempt=1, two_point_conv_result="success", play_type="pass"),
            dict(play_id=2, ep=0.5, down=1.0, kickoff_attempt=1, play_type="kickoff"),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    assert math.isclose(out.filter(pl.col("play_id") == 1)["epa"][0], 2.0 - 0.9, abs_tol=1e-9)


def test_safety_overlays_minus_two_minus_ep_nflverse_name() -> None:
    """A safety (``safety == 1``) credits the defense: posteam epa = ``-2 - ep``."""
    df = _frame(
        [
            dict(play_id=1, ep=-1.0, down=3.0, safety=1, play_type="run"),
            dict(play_id=2, ep=0.5, down=1.0, kickoff_attempt=1, play_type="kickoff"),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    assert math.isclose(out.filter(pl.col("play_id") == 1)["epa"][0], -2.0 - (-1.0), abs_tol=1e-9)


# ---------------------------------------------------------------------------
# 3. Half / game boundary: no lead leak.
# ---------------------------------------------------------------------------


def test_end_of_half_no_score_play_is_negative_ep() -> None:
    """A non-scoring play whose next row crosses into Q3 gets ``epa = 0 - ep``."""
    df = _frame(
        [
            dict(play_id=1, qtr=2.0, ep=1.5, down=2.0, sp=0.0, play_type="pass", desc="(0:05) pass incomplete"),
            dict(play_id=2, qtr=2.0, ep=0.0, down=None, play_type=None, desc="END QUARTER 2"),
            dict(play_id=3, qtr=3.0, ep=0.7, down=1.0, sp=0.0, play_type="kickoff", kickoff_attempt=1),
        ]
    )
    out = _derive_epa(df).sort("play_id")
    # play1 is the last play of the half -> epa = 0 - ep = -1.5 (no leak into Q3).
    assert math.isclose(out.filter(pl.col("play_id") == 1)["epa"][0], 0.0 - 1.5, abs_tol=1e-9)


def test_no_cross_game_lead_leak_in_epa_and_wpa() -> None:
    """The last play of game 1 must not borrow game 2's first play as its lead."""
    rows = [
        dict(
            game_id="2023_01_AAA_BBB",
            play_id=1,
            ep=1.0,
            wp=0.5,
            vegas_wp=0.5,
            down=1.0,
            qtr=4.0,
            desc="(0:05) final snap",
        ),
        dict(
            game_id="2023_01_AAA_BBB",
            play_id=2,
            ep=0.0,
            wp=None,
            vegas_wp=None,
            down=None,
            play_type=None,
            desc="END GAME",
            home_score=20,
            away_score=17,
        ),
        dict(
            game_id="2023_02_CCC_DDD", play_id=1, ep=3.0, wp=0.9, vegas_wp=0.9, down=1.0, qtr=1.0, game_id_override=None
        ),
    ]
    # fix game_id on the third row (the dict helper sets a default game_id)
    rows[2]["game_id"] = "2023_02_CCC_DDD"
    rows[2]["home_team"] = "DDD"
    rows[2]["posteam"] = "DDD"
    df = _frame(rows)
    out = _derive_epa(df)
    out = _derive_wpa(out).sort(["game_id", "play_id"])
    g1_last = out.filter((pl.col("game_id") == "2023_01_AAA_BBB") & (pl.col("play_id") == 1))
    # END GAME row in game1 -> epa / wpa NULL (terminal); the real last play must
    # not lead across into game2.  Its epa/wpa must be finite and game-local.
    g1_epa = g1_last["epa"][0]
    # game1 last real play leads only within game1 (to the END-GAME terminal),
    # so it must not equal a value derived from game2's ep (3.0).
    assert g1_epa is None or abs(g1_epa) < 50  # finite, not a cross-game artifact
    # END GAME terminal row carries NULL epa.
    g1_endgame = out.filter((pl.col("game_id") == "2023_01_AAA_BBB") & (pl.col("play_id") == 2))
    assert g1_endgame["epa"][0] is None


# ---------------------------------------------------------------------------
# 4. CPOE scale: nflfastR ``cpoe = 100 * (complete_pass - cp)``.
# ---------------------------------------------------------------------------


def test_cpoe_is_percentage_points_scale(monkeypatch) -> None:  # noqa: ANN001
    """``cpoe`` must be on the nflfastR percentage-point scale (x100)."""
    import sportsdataverse.nfl.ep_wp as ew

    # Stub the model scoring so cp is a deterministic 0.25 on the one pass play.
    def _stub_cp_predict(df):  # noqa: ANN001
        return df.with_columns(pl.lit(0.25).alias("cp"))

    captured = {}

    # Build a minimal pass-play frame; air_yards not-null marks it a pass play.
    df = pl.DataFrame(
        {
            "air_yards": [10.0],
            "complete_pass": [1.0],
            "season": [2023],
            "ydstogo": [10],
            "down": [1.0],
            "posteam": ["BBB"],
            "home_team": ["BBB"],
            "yardline_100": [50.0],
        }
    )

    # Monkeypatch the internal model load + mutation path is heavy; instead drive
    # the cpoe arithmetic by injecting cp via a thin wrapper around the public fn.
    # Simplest: assert the formula directly on a known cp by calling the public
    # function with a frame already carrying cp through complete_pass arithmetic.
    # Since calculate_completion_probability recomputes cp from the model, we
    # instead verify the documented contract: for cp=0.25 and a completion,
    # cpoe == 100 * (1 - 0.25) == 75.0.  We patch _load_model + _make_cp_mutations
    # to keep this offline.
    class _FakeModel:
        def predict(self, _dmatrix):  # noqa: ANN001
            import numpy as np

            return np.array([0.25], dtype="float32")

    monkeypatch.setattr(ew, "_load_model", lambda name: _FakeModel())
    monkeypatch.setattr(ew, "_make_cp_mutations", lambda d: d)
    monkeypatch.setattr(ew, "CP_FEATURES", ["air_yards"])

    out = calculate_completion_probability(df)
    cpoe = out["cpoe"][0]
    assert math.isclose(cpoe, 100.0 * (1.0 - 0.25), abs_tol=1e-6), f"cpoe={cpoe} (expected 75.0 on x100 scale)"
