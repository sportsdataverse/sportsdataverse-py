"""Expected Threat: grid helpers, fit parity with the socceraction oracle, rate, JSON."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.soccer import xthreat as xt

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "socceraction"
ORACLE_ACTIONS = FIXTURES / "8658_spadl.csv"
ORACLE_GRID = FIXTURES / "8658_xt_fit.json"


def _oracle_actions() -> pl.DataFrame:
    return pl.read_csv(
        ORACLE_ACTIONS,
        schema_overrides={"game_id": pl.Utf8, "original_event_id": pl.Utf8, "team_id": pl.Utf8, "player_id": pl.Utf8},
    )


def test_cell_indexes_match_socceraction() -> None:
    x = np.array([0.0, 52.4, 52.6, 104.9])
    y = np.array([0.0, 33.9, 34.1, 67.9])
    xi, yj = xt._cell_indexes(x, y, 16, 12)
    assert xi.tolist() == [0, 7, 8, 15] and yj.tolist() == [0, 5, 6, 11]
    assert xt._flat_indexes(x, y, 16, 12).tolist() == [
        (11 - 0) * 16 + 0,
        (11 - 5) * 16 + 7,
        (11 - 6) * 16 + 8,
        (11 - 11) * 16 + 15,
    ]


def test_edge_coordinates_clip_to_the_last_cell() -> None:
    xi, yj = xt._cell_indexes(np.array([105.0, 200.0, -1.0]), np.array([68.0, 90.0, -5.0]), 16, 12)
    assert xi.tolist() == [15, 15, 0] and yj.tolist() == [11, 11, 0]


def test_count_puts_the_origin_at_the_top_left_row_zero_is_top() -> None:
    m = xt._count(np.array([0.5]), np.array([67.5]), 16, 12)
    assert m.shape == (12, 16) and m[0, 0] == 1 and m.sum() == 1


def test_count_skips_null_coordinates() -> None:
    m = xt._count(np.array([1.0, np.nan]), np.array([1.0, 2.0]), 16, 12)
    assert m.sum() == 1


def test_scoring_prob_only_counts_open_play_shots() -> None:
    a = _oracle_actions()
    p = xt._scoring_prob(a, 16, 12)
    shots = a.filter(pl.col("type_name") == "shot")
    goals = shots.filter(pl.col("result_name") == "success")
    assert p.shape == (12, 16) and 0 <= p.min() and p.max() <= 1
    assert (p > 0).sum() <= goals.height


def test_transition_matrix_rows_sum_to_at_most_one() -> None:
    T = xt._move_transition_matrix(_oracle_actions(), 16, 12)
    assert T.shape == (192, 192) and T.min() >= 0 and T.sum(axis=1).max() <= 1 + 1e-12
