"""Tests for the publish-integrity audit (WS1)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.publish_audit import (
    FINGERPRINT_SUFFIX,
    PublishAudit,
    append_manifest,
    audit_asset,
    completeness_report,
    drift_report,
    fingerprint_frame,
    fingerprint_parquet,
    read_fingerprint,
    write_fingerprint,
)


def _frame(rows: int = 100, mean: float = 10.0, seasons: "list[int] | None" = None) -> pl.DataFrame:
    seasons = seasons or [2024, 2025]
    return pl.DataFrame(
        {
            "season": [seasons[i % len(seasons)] for i in range(rows)],
            "game_id": [f"g{i:05d}" for i in range(rows)],
            "pts": [mean + (i % 5) - 2.0 for i in range(rows)],
            "note": [None if i % 10 == 0 else "x" for i in range(rows)],
        }
    )


# ---------------------------------------------------------------- fingerprint


def test_fingerprint_shape_nulls_dtypes() -> None:
    fp = fingerprint_frame(_frame(), asset="a.parquet")
    assert fp["n_rows"] == 100
    assert fp["n_cols"] == 4
    assert fp["asset"] == "a.parquet"
    assert fp["columns"]["note"]["null_count"] == 10
    assert fp["columns"]["season"]["dtype"] == "Int64"
    assert "mean" in fp["columns"]["pts"]
    assert "mean" not in fp["columns"]["game_id"]


def test_fingerprint_deterministic_and_sensitive() -> None:
    a = fingerprint_frame(_frame())
    b = fingerprint_frame(_frame())
    assert a["columns"]["pts"]["sha256"] == b["columns"]["pts"]["sha256"]
    changed = fingerprint_frame(_frame(mean=10.001))
    assert a["columns"]["pts"]["sha256"] != changed["columns"]["pts"]["sha256"]
    # untouched columns keep their hash
    assert a["columns"]["game_id"]["sha256"] == changed["columns"]["game_id"]["sha256"]


def test_fingerprint_empty_frame() -> None:
    fp = fingerprint_frame(pl.DataFrame({"season": pl.Series([], dtype=pl.Int64)}))
    assert fp["n_rows"] == 0
    assert fp["columns"]["season"]["mean"] is None


def test_fingerprint_parquet_and_sidecar_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "asset.parquet"
    _frame().write_parquet(p)
    fp = fingerprint_parquet(p)
    assert fp["file_bytes"] == p.stat().st_size
    assert len(fp["file_sha256"]) == 64
    sidecar = write_fingerprint(p, fp)
    assert sidecar.name == "asset.parquet" + FINGERPRINT_SUFFIX
    # read via sidecar path AND via asset path
    assert read_fingerprint(sidecar)["file_sha256"] == fp["file_sha256"]
    assert read_fingerprint(p)["file_sha256"] == fp["file_sha256"]
    # sidecar is valid, sorted JSON
    assert json.loads(sidecar.read_text(encoding="utf-8"))["version"] == 1


# ---------------------------------------------------------------------- drift


def test_drift_identical_is_quiet() -> None:
    fp = fingerprint_frame(_frame())
    warnings, l2 = drift_report(fingerprint_frame(_frame()), fp)
    assert warnings == []
    assert l2 == 0.0


def test_drift_mean_shift_and_l2() -> None:
    prior = fingerprint_frame(_frame(mean=10.0))
    current = fingerprint_frame(_frame(mean=20.0))
    warnings, l2 = drift_report(current, prior)
    assert any("pts" in w and "sigma" in w for w in warnings)
    assert l2 > 0.5


def test_drift_column_set_and_dtype_changes() -> None:
    prior = fingerprint_frame(_frame())
    current_df = _frame().drop("note").with_columns(pl.lit(1).alias("new_col"))
    warnings, _ = drift_report(fingerprint_frame(current_df), prior)
    assert any("added" in w and "new_col" in w for w in warnings)
    assert any("removed" in w and "note" in w for w in warnings)
    retyped = _frame().with_columns(pl.col("season").cast(pl.Utf8))
    warnings2, _ = drift_report(fingerprint_frame(retyped), prior)
    assert any("dtype" in w and "season" in w for w in warnings2)


def test_drift_null_rate_delta() -> None:
    prior = fingerprint_frame(_frame())
    holey = _frame().with_columns(
        pl.when(pl.int_range(pl.len()) % 2 == 0).then(None).otherwise(pl.col("note")).alias("note")
    )
    warnings, _ = drift_report(fingerprint_frame(holey), prior)
    assert any("null rate" in w and "note" in w for w in warnings)


# -------------------------------------------------------------- completeness


def test_completeness_row_floor_blocks() -> None:
    errors = completeness_report(_frame(rows=50), key_cols=["season"], row_floor=100)
    assert any("below floor" in e for e in errors)


def test_completeness_missing_key_col_blocks() -> None:
    errors = completeness_report(_frame(), key_cols=["not_a_col"])
    assert any("missing" in e for e in errors)


def test_completeness_shrink_vs_prior_blocks(tmp_path: Path) -> None:
    prior_fp = fingerprint_frame(_frame(rows=100))
    completeness_report(prior_fp, key_cols=["season", "game_id"], df=_frame(rows=100))
    # fewer rows + a lost season vs prior -> both errors
    cur = _frame(rows=40, seasons=[2024])
    cur_fp = fingerprint_frame(cur)
    errors = completeness_report(cur_fp, key_cols=["season", "game_id"], df=cur, prior=prior_fp)
    assert any("shrank vs prior" in e and "row count" in e for e in errors)
    assert any("season" in e for e in errors)


def test_completeness_pass_records_keys() -> None:
    df = _frame()
    fp = fingerprint_frame(df)
    errors = completeness_report(fp, key_cols=["season", "game_id"], df=df, row_floor=10)
    assert errors == []
    assert fp["keys"]["season"]["n_distinct"] == 2
    assert fp["keys"]["season"]["min"] == 2024
    assert fp["keys"]["game_id"]["n_distinct"] == 100


def test_completeness_string_key_growth_not_flagged() -> None:
    # "g1499" < "g999" lexicographically -- a growing string-id range must NOT
    # error; only numeric keys get the max-regression check.
    prior_df = pl.DataFrame({"game_id": [f"g{i}" for i in range(1000)]})
    prior_fp = fingerprint_frame(prior_df)
    completeness_report(prior_fp, key_cols=["game_id"], df=prior_df)
    cur_df = pl.DataFrame({"game_id": [f"g{i}" for i in range(1000, 2000)]})
    cur_fp = fingerprint_frame(cur_df)
    errors = completeness_report(cur_fp, key_cols=["game_id"], df=cur_df, prior=prior_fp)
    assert errors == []


def test_completeness_requires_frame_for_keys() -> None:
    with pytest.raises(ValueError, match="backing frame"):
        completeness_report(fingerprint_frame(_frame()), key_cols=["season"])


# --------------------------------------------------------------- audit_asset


def test_audit_asset_end_to_end(tmp_path: Path) -> None:
    prior_path = tmp_path / "asset.parquet"
    _frame(rows=100).write_parquet(prior_path)
    prior = audit_asset(prior_path, key_cols=["season", "game_id"], row_floor=10)
    assert isinstance(prior, PublishAudit)
    assert prior.ok
    assert (tmp_path / ("asset.parquet" + FINGERPRINT_SUFFIX)).exists()

    # next release: same shape, drifted mean -> ok with warnings, never blocked
    _frame(rows=110, mean=25.0).write_parquet(prior_path)
    nxt = audit_asset(
        prior_path,
        key_cols=["season", "game_id"],
        prior=prior.fingerprint,
        row_floor=10,
    )
    assert nxt.ok
    assert nxt.drift_l2 > 0.0
    assert any("sigma" in w for w in nxt.drift_warnings)

    # short scrape: rows collapse -> blocked
    _frame(rows=20).write_parquet(prior_path)
    short = audit_asset(prior_path, key_cols=["season", "game_id"], prior=nxt.fingerprint)
    assert not short.ok
    assert any("shrank" in e for e in short.errors)


def test_manifest_append_log(tmp_path: Path) -> None:
    asset = tmp_path / "asset.parquet"
    _frame().write_parquet(asset)
    audit = audit_asset(asset, key_cols=["season"], write_sidecar=False)
    manifest_path = tmp_path / "manifest.parquet"
    m1 = append_manifest(manifest_path, audit)
    m2 = append_manifest(manifest_path, audit)
    assert m1.height == 1
    assert m2.height == 2
    assert m2.columns == [
        "asset",
        "n_rows",
        "file_sha256",
        "drift_l2",
        "keys",
        "produced_at",
        "git_sha",
    ]
    assert json.loads(m2["keys"][0])["season"]["n_distinct"] == 2
