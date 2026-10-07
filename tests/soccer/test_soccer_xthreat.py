"""Expected Threat: grid helpers, fit parity with the socceraction oracle, rate, JSON."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

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


def test_fit_reproduces_the_socceraction_grid() -> None:
    model = xt.XThreat().fit(_oracle_actions())
    oracle = np.array(json.loads(ORACLE_GRID.read_text(encoding="utf-8")))
    assert model.xT.shape == oracle.shape == (12, 16)
    assert np.abs(model.xT - oracle).max() < 1e-6
    assert 0 <= model.xT.min() and model.xT.max() <= 1


def test_fit_with_no_moves_is_a_zero_grid() -> None:
    only_shots = _oracle_actions().filter(pl.col("type_name") == "shot")
    model = xt.XThreat().fit(only_shots)
    assert model.xT.shape == (12, 16) and np.isfinite(model.xT).all()


def test_null_coordinates_are_excluded_from_every_count() -> None:
    a = _oracle_actions()
    poisoned = pl.concat([a, a.head(3).with_columns(pl.lit(None, dtype=pl.Float64).alias("end_x"))])
    assert np.abs(xt.XThreat().fit(poisoned).xT - xt.XThreat().fit(a).xT).max() < 1e-9


def test_max_iter_raises() -> None:
    with pytest.raises(RuntimeError):
        xt.XThreat(max_iter=1, eps=1e-300).fit(_oracle_actions())


def test_rate_values_only_successful_moves() -> None:
    a = _oracle_actions()
    model = xt.XThreat().fit(a)
    s = model.rate(a)
    assert s.dtype == pl.Float64 and s.name == "xt_value" and len(s) == a.height
    is_move = (a["type_name"].is_in(list(xt.MOVE_TYPES)) & (a["result_name"] == "success")).to_numpy()
    assert s.is_null().to_numpy()[~is_move].all() and (~s.is_null().to_numpy()[is_move]).all()


def test_rate_with_no_moves_is_all_null() -> None:
    a = _oracle_actions()
    model = xt.XThreat().fit(a)
    assert model.rate(a.filter(pl.col("type_name") == "shot")).is_null().all()


def test_rate_own_box_to_opponent_box_is_positive() -> None:
    model = xt.XThreat().fit(_oracle_actions())
    row = pl.DataFrame(
        {
            "type_name": ["pass"],
            "result_name": ["success"],
            "start_x": [10.0],
            "start_y": [34.0],
            "end_x": [95.0],
            "end_y": [34.0],
        }
    )
    assert model.rate(row)[0] > 0


def test_not_fitted_raises() -> None:
    with pytest.raises(xt.NotFittedError):
        xt.XThreat().rate(_oracle_actions())


def test_json_round_trip_and_socceraction_format(tmp_path: Path) -> None:
    model = xt.XThreat().fit(_oracle_actions())
    model.to_json(tmp_path / "m.json")
    back = xt.XThreat.from_json(tmp_path / "m.json")
    assert np.array_equal(back.xT, model.xT) and back.meta == model.meta
    bare = xt.XThreat.from_json(ORACLE_GRID)  # socceraction's save_model format
    assert bare.xT.shape == (12, 16)


def test_bundled_model_loads_and_is_sane() -> None:
    m = xt.load_xthreat_model()
    assert m.xT.shape == (12, 16) and 0 <= m.xT.min() and m.xT.max() <= 1
    center = m.xT[[5, 6], :].mean(axis=0)
    assert np.all(np.diff(center[:14]) >= -1e-9)  # non-decreasing toward the goal up to the box
    for key in ("competitions", "matches", "actions", "kloppy_version", "fit_date", "eps", "iterations", "license"):
        assert key in m.meta


def test_soccer_xthreat_rate_appends_the_column() -> None:
    a = _oracle_actions()
    out = xt.soccer_xthreat_rate(a)
    assert out.columns == a.columns + ["xt_value"] and out.schema["xt_value"] == pl.Float64
    assert out.filter(pl.col("xt_value").is_not_null()).height > 0
    pdf = xt.soccer_xthreat_rate(a, return_as_pandas=True)
    assert "xt_value" in pdf.columns
