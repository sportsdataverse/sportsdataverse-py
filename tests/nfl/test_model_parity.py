"""Model-integration parity guards for the two NFL PBP construction paths.

These offline tests pin the cross-path model-integration wiring added so the
nflverse ``enrich_nfl_pbp`` orchestrator and the ESPN ``NFLPlayProcess``
pipeline agree column-for-column:

* ``_derive_qbr_epa`` — the per-play Total-QBR EPA components (nflverse port of
  the ESPN ``__process_qbr`` step): the ``qb_epa`` clamp / fumble floor, the WP
  leverage ``weight``, the play-family EPA / weight partitions, ``action_play``.
* ``_add_fourth_down_decisions`` — the default-on nfl4th 4th-down decision
  surface attached to a full nflverse frame: decision columns are scored only on
  the qualifying 4th-down rows and null on every other play; it degrades to
  all-null columns (schema-stable) when the download-on-demand models are
  unavailable / incompatible.

They are model-free where possible (the QBR components are pure arithmetic over
hand-supplied ``qb_epa`` / ``home_wp`` / flags), so every expected value is
hand-computable and the suite stays fast and offline.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl.ep_wp import _add_fourth_down_decisions, _derive_qbr_epa

_DECISION_COLS = [
    "go_wp",
    "first_down_prob",
    "wp_succeed",
    "wp_fail",
    "fg_make_prob",
    "make_fg_wp",
    "miss_fg_wp",
    "fg_wp",
    "punt_wp",
    "go_boost",
    "go_wp_diff",
    "punt_wp_diff",
    "fg_wp_diff",
    "fourth_down_recommendation",
]


# ---------------------------------------------------------------------------
# _derive_qbr_epa — QBR component contract (model-free arithmetic).
# ---------------------------------------------------------------------------


def _qbr_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            # qb_epa: clamp at -5; fumble row gets -3.5 regardless of qb_epa.
            "qb_epa": [1.2, -7.0, 0.0, 2.0, -1.0],
            "home_wp": [0.5, 0.05, 0.85, 0.95, 0.5],
            "pass": [1, 0, 0, 1, 0],
            "rush": [0, 1, 0, 0, 0],
            "sack": [0, 0, 1, 0, 0],
            "fumble": [0, 0, 0, 0, 1],
            "penalty": [0, 0, 0, 0, 0],
        }
    )


def test_qbr_epa_columns_added() -> None:
    out = _derive_qbr_epa(_qbr_frame())
    for c in (
        "qbr_epa",
        "weight",
        "non_fumble_sack",
        "sack_epa",
        "pass_epa",
        "rush_epa",
        "pen_epa",
        "sack_weight",
        "pass_weight",
        "rush_weight",
        "pen_weight",
        "action_play",
    ):
        assert c in out.columns, c


def test_qbr_epa_clamp_and_fumble_floor() -> None:
    out = _derive_qbr_epa(_qbr_frame())
    qbr = out["qbr_epa"].to_list()
    assert qbr[0] == 1.2  # unchanged
    assert qbr[1] == -5.0  # clamped at -5
    assert qbr[2] == 0.0  # unchanged
    assert qbr[3] == 2.0  # unchanged
    assert qbr[4] == -3.5  # fumble floor


def test_qbr_weight_leverage_buckets() -> None:
    out = _derive_qbr_epa(_qbr_frame())
    w = out["weight"].to_list()
    assert w[0] == 1.0  # mid leverage (0.5)
    assert w[1] == 0.6  # home_wp < 0.1
    assert w[2] == 0.9  # 0.8 <= wp < 0.9
    assert w[3] == 0.6  # wp > 0.9
    assert w[4] == 1.0


def test_qbr_play_family_partitions() -> None:
    out = _derive_qbr_epa(_qbr_frame())
    # pass_epa only on pass rows; rush_epa only on rush rows; sack_epa only on
    # non-fumble sacks; pen_epa only on penalty rows.
    assert out["pass_epa"].to_list() == [1.2, None, None, 2.0, None]
    assert out["rush_epa"].to_list() == [None, -5.0, None, None, None]
    assert out["sack_epa"].to_list() == [None, None, 0.0, None, None]
    assert out["non_fumble_sack"].to_list() == [False, False, True, False, False]
    # action_play = qb_epa != 0
    assert out["action_play"].to_list() == [True, True, False, True, True]


def test_qbr_epa_noop_when_columns_absent() -> None:
    # Missing qb_epa / home_wp -> faithful no-op (frame returned unchanged).
    df = pl.DataFrame({"epa": [1.0, 2.0]})
    out = _derive_qbr_epa(df)
    assert out.columns == df.columns


# ---------------------------------------------------------------------------
# _add_fourth_down_decisions — default-on surface, scoped to 4th-down rows.
# ---------------------------------------------------------------------------


def _nflverse_4th_frame() -> pl.DataFrame:
    # A tiny full-frame: one 1st down, one 4th down, one 4th down with null
    # yardline (excluded), so the decision columns must be null on every row but
    # the single scorable 4th down.
    base = {
        "game_id": "2023_01_AAA_BBB",
        "season": 2023,
        "week": 1,
        "season_type": "REG",
        "posteam": "BBB",
        "defteam": "AAA",
        "home_team": "BBB",
        "away_team": "AAA",
        "roof": "outdoors",
        "qtr": 2,
        "quarter_seconds_remaining": 600,
        "score_differential": 0,
        "posteam_timeouts_remaining": 3,
        "defteam_timeouts_remaining": 3,
        "home_opening_kickoff": 1,
        "spread_line": -3.0,
        "total_line": 45.0,
    }
    rows = [
        {**base, "play_id": 1, "down": 1, "ydstogo": 10, "yardline_100": 75},
        {**base, "play_id": 2, "down": 4, "ydstogo": 2, "yardline_100": 50},
        {**base, "play_id": 3, "down": 4, "ydstogo": 5, "yardline_100": None},
    ]
    return pl.DataFrame(rows)


def test_fourth_down_columns_present_and_scoped() -> None:
    out = _add_fourth_down_decisions(_nflverse_4th_frame())
    # All decision columns must exist (schema stability), regardless of whether
    # the download-on-demand models scored (they may be unavailable offline).
    for c in _DECISION_COLS:
        assert c in out.columns, c
    # Non-4th-down rows (down 1) and the null-yardline 4th-down row must be null
    # on every decision column.
    non_fourth = out.filter((pl.col("down") != 4) | pl.col("yardline_100").is_null())
    for c in _DECISION_COLS:
        assert non_fourth[c].null_count() == non_fourth.height, c


def test_fourth_down_row_count_preserved() -> None:
    frame = _nflverse_4th_frame()
    out = _add_fourth_down_decisions(frame)
    assert out.height == frame.height


def test_fourth_down_null_when_join_keys_absent() -> None:
    # No game_id / play_id -> emit null decision columns, don't raise.
    df = pl.DataFrame({"down": [4], "yardline_100": [50]})
    out = _add_fourth_down_decisions(df)
    for c in _DECISION_COLS:
        assert c in out.columns
        assert out[c].null_count() == out.height
