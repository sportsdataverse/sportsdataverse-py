from __future__ import annotations

from pathlib import Path

import polars as pl

from tools.validation.checks import schema_contract
from tools.validation.registry import (
    DATASETS,
    DatasetSpec,
    _resolve_spec,
    load_schema,
)

_FIXTURE = Path(__file__).parent / "fixtures" / "cfb_model_pbp_sample.parquet"

_SMALL_SCHEMA: dict[str, str] = {
    "game_id": "Int64",
    "id": "Int64",
    "wp_before": "Float64",
}

_SMALL_SPEC = DatasetSpec(
    name="t",
    domain="cfb",
    parquet_glob=str(_FIXTURE),
    schema=_SMALL_SCHEMA,
    join_keys=("game_id", "id"),
    required_columns=("game_id", "id"),
    range_constraints={"wp_before": (0.0, 1.0)},
    oracle_domain="cfb",
)


def test_load_schema_returns_full_cfb_contract() -> None:
    schema = load_schema("cfb_model_pbp")
    assert len(schema) == 42
    for col in ("game_id", "ep_before", "wp_after", "epa", "completion_prob"):
        assert col in schema, f"expected column {col!r} in schema"


def test_cfb_dataset_registered() -> None:
    assert "cfb_model_pbp" in DATASETS
    spec = DATASETS["cfb_model_pbp"]
    assert spec.oracle_domain == "cfb"
    assert "game_id" in spec.join_keys
    assert spec.prob_groups == ()


def test_resolve_spec_roundtrip_with_fixture() -> None:
    frame, ctx = _resolve_spec(_SMALL_SPEC)
    assert frame.shape == (3, 3)
    assert ctx.oracle is not None
    assert ctx.oracle.domain == "cfb"
    assert ctx.schema == _SMALL_SCHEMA


def test_checks_run_on_resolved_fixture() -> None:
    frame, ctx = _resolve_spec(_SMALL_SPEC)

    # clean frame — no findings
    findings = schema_contract.run("t", frame, ctx)
    assert findings == [], f"unexpected findings on clean fixture: {findings}"

    # corrupt game_id dtype — should produce a join-key dtype ERROR
    bad_frame = frame.with_columns(pl.col("game_id").cast(pl.Utf8))
    bad_findings = schema_contract.run("t", bad_frame, ctx)
    assert any(f.locator.get("is_join_key") is True for f in bad_findings), (
        f"expected a join-key dtype ERROR, got: {bad_findings}"
    )
