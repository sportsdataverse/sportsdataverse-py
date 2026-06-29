from __future__ import annotations

import polars as pl

from tools.validation import cli
from tools.validation.findings import CheckContext


def test_run_dataset_aggregates_findings(monkeypatch):
    frame = pl.DataFrame({"game_id": ["1", "2"], "epa": [0.1, 0.2]})  # bad join-key dtype
    ctx = CheckContext(
        domain="nfl",
        dataset="nfl_pbp",
        schema={"game_id": "Int64", "epa": "Float64"},
        required_columns=("game_id",),
        join_keys=("game_id",),
    )
    monkeypatch.setattr(cli, "resolve", lambda dataset, release=None: (frame, ctx))
    out = cli.run_dataset("nfl_pbp")
    assert any(d["check"] == "schema_contract" and d["severity"] == "error" for d in out)


def test_main_returns_1_on_error(monkeypatch):
    monkeypatch.setattr(
        cli,
        "run_dataset",
        lambda dataset, release=None: [{"severity": "error", "check": "x", "dataset": "d", "message": "m"}],
    )
    assert cli.main(["run", "--dataset", "nfl_pbp", "--json"]) == 1
