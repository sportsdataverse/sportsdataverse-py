"""Tests for the data-contract gate threaded through ``release.AuditSpec``.

Same discipline as the publish-audit gate tests: the contract is derived
from a real frame ("expectations from history"), a conforming asset sails
through, drift warns without blocking, and a completeness-class violation
blocks BEFORE any upload. The gh chokepoint is never reached on a block.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse import release
from sportsdataverse.modeling.integrity import derive_contract
from sportsdataverse.release import AuditSpec, _audit_parquet_files


def _frame(rows: int = 100) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2024 + (i % 2) for i in range(rows)],
            "game_id": [f"g{i:04d}" for i in range(rows)],
            "pts": [float(i % 30) for i in range(rows)],
        }
    )


def _write(tmp_path: Path, name: str, frame: pl.DataFrame) -> Path:
    path = tmp_path / name
    frame.write_parquet(path)
    return path


def test_conforming_asset_passes_and_fingerprints(tmp_path: Path) -> None:
    frame = _frame()
    contract = derive_contract(frame, name="stats", key=["game_id"], partition_key="season")
    path = _write(tmp_path, "stats.parquet", frame)
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # a conforming asset emits NO warnings
        sidecars = _audit_parquet_files([path], AuditSpec(contract=contract))
    assert sidecars and sidecars[0].exists()


def test_completeness_violation_blocks_upload(tmp_path: Path) -> None:
    frame = _frame()
    contract = derive_contract(frame, name="stats", partition_key="season")
    # a vanished season partition — the silent-shortfall incident class
    path = _write(tmp_path, "stats.parquet", frame.filter(pl.col("season") != 2025))
    with pytest.raises(RuntimeError, match="missing_partition"):
        _audit_parquet_files([path], AuditSpec(contract=contract))


def test_drift_violation_warns_but_uploads(tmp_path: Path) -> None:
    frame = _frame()
    contract = derive_contract(frame, name="stats")
    drifted = frame.with_columns(pl.lit("weird").alias("brand_new_col"))
    path = _write(tmp_path, "stats.parquet", drifted)
    with pytest.warns(UserWarning, match="unexpected_column"):
        sidecars = _audit_parquet_files([path], AuditSpec(contract=contract))
    assert sidecars  # warn-class findings never block


def test_mapping_routes_contracts_by_asset(tmp_path: Path) -> None:
    frame = _frame()
    contract = derive_contract(frame, name="stats")
    good = _write(tmp_path, "stats.parquet", frame)
    # a different, non-conforming dataset in the same upload set is SKIPPED
    # because no contract is routed to it
    other = _write(tmp_path, "other.parquet", pl.DataFrame({"x": [1, 2, 3]}))
    sidecars = _audit_parquet_files([good, other], AuditSpec(contract={"stats.parquet": contract}))
    assert len(sidecars) == 2
    # ...and routing by stem works too, still blocking on violation
    bad = _write(tmp_path, "stats2.parquet", frame.drop("pts"))
    with pytest.raises(RuntimeError, match="missing_column"):
        _audit_parquet_files([bad], AuditSpec(contract={"stats2": derive_contract(frame, name="stats2")}))


def test_upload_path_blocks_before_gh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(release, "_invoke_gh", lambda args, **kw: calls.append(list(args)) or "")
    frame = _frame()
    contract = derive_contract(frame, name="stats")
    bad = _write(tmp_path, "stats.parquet", frame.head(3))  # under the row floor
    with pytest.raises(RuntimeError, match="min_rows"):
        release.sportsdataverse_upload([bad], tag="test-tag", audit=AuditSpec(contract=contract))
    assert calls == []  # nothing reached gh — the bad asset never partially published
