"""Tests for WS2: baseline comparator, group-cut metrics, experiment ledger."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.experiment_ledger import (
    ExperimentRun,
    log_run,
    push_run,
    run_row,
)
from sportsdataverse._common.metrics import baseline_test, group_error_metrics

# ------------------------------------------------------------- baseline_test


def test_baseline_test_model_wins() -> None:
    y = np.array([1, 0, 1, 0])
    res = baseline_test(y, np.array([0.9, 0.1, 0.8, 0.2]), np.array([0.5] * 4))
    assert res.metric == "brier"
    assert res.beat_baseline
    assert res.delta < 0
    assert res.model_metric == pytest.approx(0.025)
    assert res.baseline_metric == pytest.approx(0.25)


def test_baseline_test_model_loses_and_mae() -> None:
    y = np.array([10.0, 20.0])
    res = baseline_test(y, np.array([15.0, 15.0]), np.array([10.0, 20.0]), metric="mae")
    assert not res.beat_baseline
    assert res.delta == pytest.approx(5.0)


def test_baseline_test_unknown_metric() -> None:
    with pytest.raises(ValueError, match="Unknown metric"):
        baseline_test(np.array([1]), np.array([1.0]), np.array([1.0]), metric="nope")


# ------------------------------------------------------- group_error_metrics


def _preds() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2024, 2024, 2025, 2025],
            "pos": ["QB", "RB", "QB", "RB"],
            "p_over": [0.9, 0.4, 0.8, 0.2],
            "is_over": [1.0, 0.0, 1.0, 1.0],
        }
    )


def test_group_error_metrics_shape_and_values() -> None:
    out = group_error_metrics(_preds(), pred_col="p_over", actual_col="is_over", group_cols=["season", "pos"])
    assert out.columns == ["group", "group_value", "n", "rmse", "mae"]
    assert out.height == 4  # 2 seasons + 2 positions
    s2024 = out.filter((pl.col("group") == "season") & (pl.col("group_value") == "2024"))
    assert s2024["mae"][0] == pytest.approx((0.1 + 0.4) / 2)


def test_group_error_metrics_probabilistic_columns() -> None:
    out = group_error_metrics(
        _preds(),
        pred_col="p_over",
        actual_col="is_over",
        group_cols=["pos"],
        probabilistic=True,
    )
    assert {"brier", "log_loss", "hit_rate"} <= set(out.columns)
    qb = out.filter(pl.col("group_value") == "QB")
    assert qb["hit_rate"][0] == pytest.approx(1.0)


# ----------------------------------------------------------------- ledger


def _run(**overrides) -> ExperimentRun:
    base = dict(
        sport="nfl",
        model_name="wp_spread",
        metric="brier",
        model_metric=0.181,
        baseline_name="vegas",
        baseline_metric=0.185,
        beat_baseline=True,
        config={"eta": 0.05, "depth": 6},
        features=("spread_time", "score_differential"),
        depends_on=("ep_model",),
        release_tag="nfl_model_artifacts",
    )
    base.update(overrides)
    return ExperimentRun(**base)


def test_config_hash_stable_and_sensitive() -> None:
    assert _run().config_hash == _run().config_hash
    assert _run().config_hash != _run(config={"eta": 0.10, "depth": 6}).config_hash
    assert len(_run().config_hash) == 12


def test_run_row_flattens_json_fields() -> None:
    row = run_row(_run())
    assert json.loads(row["features"]) == ["spread_time", "score_differential"]
    assert json.loads(row["depends_on"]) == ["ep_model"]
    assert json.loads(row["config"]) == {"depth": 6, "eta": 0.05}
    assert row["logged_at"].endswith("+00:00")


def test_log_run_appends(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.parquet"
    first = log_run(ledger_path, _run())
    second = log_run(ledger_path, _run(model_name="cp_model", model_metric=0.2))
    assert first.height == 1
    assert second.height == 2
    assert set(second["model_name"].to_list()) == {"wp_spread", "cp_model"}


def test_push_run_posts_with_bearer(monkeypatch) -> None:
    calls: list = []

    class _Resp:
        status_code = 201

    def poster(url, json=None, headers=None, timeout=None):
        calls.append((url, json, headers))
        return _Resp()

    ok = push_run(_run(), url="https://example.test/ingest", token="tok", poster=poster)
    assert ok
    url, payload, headers = calls[0]
    assert url == "https://example.test/ingest"
    assert payload["model_name"] == "wp_spread"
    assert headers == {"Authorization": "Bearer tok"}


def test_push_run_no_url_is_warning_noop(monkeypatch) -> None:
    monkeypatch.delenv("SDV_PLATFORM_INGEST_URL", raising=False)
    called = []
    with pytest.warns(UserWarning, match="not pushed"):
        ok = push_run(_run(), poster=lambda *a, **k: called.append(1))
    assert not ok
    assert called == []


def test_push_run_transport_failure_never_raises() -> None:
    def exploding_poster(*a, **k):
        raise ConnectionError("down")

    with pytest.warns(UserWarning, match="push failed"):
        assert push_run(_run(), url="https://example.test", poster=exploding_poster) is False
