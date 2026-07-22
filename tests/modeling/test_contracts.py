"""Gates for declarative data contracts, on real classified frames.

Substrate: the classified football play frame (enum play_class, numeric
yardage, real periods) and NBA player logs (game_id partitions as the
per-partition floor demo). The load-bearing invariant is derive->validate
round-trip cleanliness: a contract derived from a frame must pass that frame
with zero findings, so only genuine regressions ever fire.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.modeling.integrity import (
    derive_contract,
    read_contract,
    validate_frame,
    write_contract,
)
from sportsdataverse.nba.nba_possession_sim import player_game_logs_from_pbp
from sportsdataverse.nfl.nfl_drive_sim import plays_from_espn_drives


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    summary = json.loads(pathlib.Path("tests/fixtures/espn/summary_nfl.json").read_text(encoding="utf-8"))
    return plays_from_espn_drives(summary)


@pytest.fixture(scope="module")
def logs() -> pl.DataFrame:
    frames = []
    for gid in ("0022100001", "0022200001", "0022300001"):
        payload = json.loads(
            pathlib.Path(f"tests/fixtures/nba_engine/{gid}/playbyplayv3.json").read_text(encoding="utf-8")
        )
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return player_game_logs_from_pbp(pl.concat(frames, how="diagonal_relaxed"))


def test_derived_contract_passes_its_own_frame(plays: pl.DataFrame, logs: pl.DataFrame) -> None:
    for name, frame, partition in (("nfl_plays", plays, "period"), ("nba_logs", logs, "game_id")):
        contract = derive_contract(frame, name=name, partition_key=partition)
        report = validate_frame(frame, contract)
        assert report.ok and report.violations == [], (name, report.violations)
    with pytest.raises(ValueError, match="empty frame"):
        derive_contract(plays.clear(), name="empty")


def test_contract_round_trips_as_json(plays: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    contract = derive_contract(plays, name="nfl_plays", partition_key="period")
    path = tmp_path / "nfl_plays.contract.json"
    write_contract(path, contract)
    assert read_contract(path) == contract
    text = path.read_text(encoding="utf-8")
    assert "produced_at" not in text  # deterministic, diff-friendly
    # the enum domain made it into the file (the taxonomy expectation)
    assert '"play_class"' in text and '"penalty"' in text


def test_completeness_class_violations_block(plays: pl.DataFrame, logs: pl.DataFrame) -> None:
    contract = derive_contract(plays, name="nfl_plays")
    # dtype flip
    report = validate_frame(plays.with_columns(pl.col("yards").cast(pl.Utf8)), contract)
    assert not report.ok
    assert any(v.kind == "dtype_mismatch" and v.column == "yards" for v in report.blocking)
    # dropped required column
    report = validate_frame(plays.drop("play_class"), contract)
    assert any(v.kind == "missing_column" and v.column == "play_class" for v in report.blocking)
    # row shrink beyond tolerance
    report = validate_frame(plays.head(10), contract)
    assert any(v.kind == "min_rows" for v in report.blocking)
    # a vanished partition (the silent-shortfall incident class)
    part_contract = derive_contract(logs, name="nba_logs", partition_key="game_id")
    report = validate_frame(logs.filter(pl.col("game_id") != "0022300001"), part_contract)
    kinds = {(v.kind, v.column) for v in report.blocking}
    assert ("missing_partition", "0022300001") in kinds
    # duplicate key
    key_contract = derive_contract(logs, name="nba_logs", key=["game_id", "player_id"])
    assert key_contract.key == ["game_id", "player_id"]  # observed unique -> enforced
    report = validate_frame(pl.concat([logs, logs.head(1)]), key_contract)
    assert any(v.kind == "duplicate_key" for v in report.blocking)
    with pytest.raises(ValueError, match="BLOCKED"):
        report.raise_if_blocking()


def test_drift_class_violations_warn(plays: pl.DataFrame) -> None:
    contract = derive_contract(plays, name="nfl_plays")
    # a NEW enum value = the taxonomy-drift warning, not a block
    drifted = plays.with_columns(
        pl.when(pl.arange(0, pl.len()) == 0)
        .then(pl.lit("brand_new_type"))
        .otherwise(pl.col("play_class"))
        .alias("play_class")
    )
    report = validate_frame(drifted, contract)
    assert report.ok  # warns, does not block
    assert any(v.kind == "domain" and "brand_new_type" in v.detail for v in report.warnings)
    # null-rate creep warns
    nulled = plays.with_columns(
        pl.when(pl.arange(0, pl.len()) < plays.height // 2).then(None).otherwise(pl.col("yards")).alias("yards")
    )
    report = validate_frame(nulled, contract)
    assert any(v.kind == "null_rate" and v.column == "yards" for v in report.warnings)
    # out-of-bounds numerics warn; unexpected columns warn
    # pl.lit defaults to Int32 — cast so the dtype holds and the BOUNDS check runs
    wild = plays.with_columns(pl.lit(10_000).cast(pl.Int64).alias("yards"), pl.lit("x").alias("brand_new_col"))
    report = validate_frame(wild, contract)
    kinds = {v.kind for v in report.warnings}
    assert {"bounds", "unexpected_column"} <= kinds
    # strict mode upgrades drift to blocking
    assert not validate_frame(drifted, contract, strict=True).ok
    # the report frames cleanly (and empty reports keep the schema)
    assert set(report.to_frame().columns) == {"kind", "column", "detail", "blocking"}
    assert validate_frame(plays, contract).to_frame().height == 0
