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
ORACLE_RATE = FIXTURES / "8658_xt_rate.csv"


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
    assert np.abs(model.xT - oracle).max() < 1e-12
    assert model.iterations == 43  # "# iterations:  43" in 8658_xt_fit_log.txt (socceraction's own count)
    assert 0 <= model.xT.min() and model.xT.max() <= 1


def test_fit_with_no_moves_equals_the_scoring_probability() -> None:
    only_shots = _oracle_actions().filter(pl.col("type_name") == "shot")
    model = xt.XThreat().fit(only_shots)
    # no moves: p_move = 0, so every cell is p_scoring * p_shot with p_shot = 1 wherever a shot happened
    assert np.allclose(model.xT, xt._scoring_prob(only_shots, 16, 12))


def test_null_end_coordinate_moves_count_toward_p_move_but_not_the_transition_matrix() -> None:
    a = _oracle_actions()
    shots = a.filter(pl.col("type_name") == "shot")
    shot_cells = set(xt._flat_indexes(*xt._xy(shots, "start"), 16, 12).tolist())
    moves = a.filter(pl.col("type_name") == "pass")
    in_shot_cell = np.isin(xt._flat_indexes(*xt._xy(moves, "start"), 16, 12), list(shot_cells))
    extra = moves.filter(pl.Series(in_shot_cell)).head(5).with_columns(pl.lit(None, dtype=pl.Float64).alias("end_x"))
    assert extra.height == 5
    poisoned = pl.concat([a, extra])
    assert np.array_equal(xt._move_transition_matrix(poisoned, 16, 12), xt._move_transition_matrix(a, 16, 12))
    _, p_move_before = xt._action_prob(a, 16, 12)
    _, p_move_after = xt._action_prob(poisoned, 16, 12)
    assert (p_move_after > p_move_before).any()
    assert np.abs(xt.XThreat().fit(poisoned).xT - xt.XThreat().fit(a).xT).max() > 0


def test_fit_tolerates_a_null_result_name() -> None:
    a = _oracle_actions()
    nulled = a.with_columns(
        pl.when(pl.col("action_id") % 7 == 0).then(None).otherwise(pl.col("result_name")).alias("result_name")
    )
    assert xt.XThreat().fit(nulled).xT.shape == (12, 16)


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


def test_rate_matches_the_socceraction_oracle() -> None:
    a = _oracle_actions()
    oracle = pl.read_csv(ORACLE_RATE, schema_overrides={"game_id": pl.Utf8, "original_event_id": pl.Utf8})
    assert oracle["action_id"].to_list() == a["action_id"].to_list()
    model = xt.XThreat.from_json(ORACLE_GRID)
    for ours in (model.rate(a), xt.soccer_xthreat_rate(a, model=model)["xt_value"]):
        got, want = ours.to_numpy(), oracle["xt"].to_numpy()  # nulls come back as NaN on both sides
        assert np.array_equal(np.isnan(got), np.isnan(want)) and np.isnan(want).sum() == 453
        assert np.abs(got[~np.isnan(got)] - want[~np.isnan(want)]).max() < 1e-12
        assert np.abs(want[~np.isnan(want)]).max() > 0.001  # a real signal, not a trivially zero column


def test_rate_reads_the_grid_right_way_up() -> None:
    model = xt.XThreat.from_json(ORACLE_GRID)
    row = pl.DataFrame(
        {
            "type_name": ["pass"],
            "result_name": ["success"],
            "start_x": [10.0],
            "start_y": [5.0],
            "end_x": [95.0],
            "end_y": [60.0],
        }
    )
    # y = 5 is in grid row 11 - 0 = 11 (bottom); y = 60 is in row 11 - 10 = 1: different rows
    xi_start, xi_end = 10 * 16 // 105, 95 * 16 // 105
    yj_start, yj_end = int(5 / 68 * 12), int(60 / 68 * 12)
    want = model.xT[11 - yj_end, xi_end] - model.xT[11 - yj_start, xi_start]
    assert yj_start != yj_end and want != 0
    assert model.rate(row)[0] == pytest.approx(want, abs=1e-15)


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
    assert m.xT.max() > 0.1 and center[-1] > center[0]  # rejects an all-zero grid
    # measured 2026-10-07: 222 of 230 matches fit, 8 skipped on a kloppy deserializer error
    assert m.meta["matches"] == sum(c["matches"] for c in m.meta["competitions"])
    assert sum(c["discovered"] for c in m.meta["competitions"]) == m.meta["matches"] + len(m.meta["skipped"])
    assert len(m.meta["skipped"]) == 8
    for key in ("competitions", "matches", "actions", "kloppy_version", "fit_date", "eps", "iterations", "license"):
        assert key in m.meta


def test_soccer_xthreat_rate_appends_the_column() -> None:
    a = _oracle_actions()
    out = xt.soccer_xthreat_rate(a)
    assert out.columns == a.columns + ["xt_value"] and out.schema["xt_value"] == pl.Float64
    assert out.filter(pl.col("xt_value").is_not_null()).height > 0
    pdf = xt.soccer_xthreat_rate(a, return_as_pandas=True)
    assert "xt_value" in pdf.columns
