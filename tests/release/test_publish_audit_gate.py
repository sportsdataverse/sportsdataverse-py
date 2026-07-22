"""Tests for the publish-integrity audit gate in ``release.py`` (WS1)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse import release
from sportsdataverse._common.publish_audit import FINGERPRINT_SUFFIX, fingerprint_frame
from sportsdataverse.release import AuditSpec


@pytest.fixture()
def gh_recorder(monkeypatch):
    """Replace the gh subprocess chokepoint; record every arg list."""
    calls: list[list[str]] = []

    def fake_invoke(args, **kwargs):
        calls.append(list(args))
        return ""

    monkeypatch.setattr(release, "_invoke_gh", fake_invoke)
    return calls


def _frame(rows: int = 100) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2024 + (i % 2) for i in range(rows)],
            "game_id": [f"g{i:04d}" for i in range(rows)],
            "pts": [float(i % 30) for i in range(rows)],
        }
    )


def _uploaded_names(calls: list[list[str]]) -> list[str]:
    names: list[str] = []
    for call in calls:
        if call[:2] == ["release", "upload"]:
            names.extend(Path(a).name for a in call[3:] if not a.startswith("--"))
    return names


def test_upload_default_has_no_audit(gh_recorder, tmp_path: Path) -> None:
    asset = tmp_path / "data.parquet"
    _frame().write_parquet(asset)
    release.sportsdataverse_upload([asset], tag="t")
    assert not any(n.endswith(FINGERPRINT_SUFFIX) for n in _uploaded_names(gh_recorder))


def test_upload_audit_true_ships_sidecar(gh_recorder, tmp_path: Path) -> None:
    asset = tmp_path / "data.parquet"
    _frame().write_parquet(asset)
    release.sportsdataverse_upload([asset], tag="t", audit=True)
    names = _uploaded_names(gh_recorder)
    assert "data.parquet" in names
    assert ("data.parquet" + FINGERPRINT_SUFFIX) in names
    assert (tmp_path / ("data.parquet" + FINGERPRINT_SUFFIX)).exists()


def test_upload_audit_blocks_before_any_upload(gh_recorder, tmp_path: Path) -> None:
    asset = tmp_path / "data.parquet"
    _frame(rows=10).write_parquet(asset)
    with pytest.raises(RuntimeError, match="BLOCKED"):
        release.sportsdataverse_upload([asset], tag="t", audit=AuditSpec(key_cols=("season",), row_floor=100))
    # fail-fast: the gh chokepoint was never reached
    assert gh_recorder == []


def test_upload_audit_drift_warns_but_uploads(gh_recorder, tmp_path: Path) -> None:
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    prior_fp = fingerprint_frame(_frame().with_columns(pl.col("pts") + 100.0), asset="data.parquet")
    import json

    (prior_dir / ("data.parquet" + FINGERPRINT_SUFFIX)).write_text(json.dumps(prior_fp), encoding="utf-8")
    asset = tmp_path / "data.parquet"
    _frame().write_parquet(asset)
    with pytest.warns(UserWarning, match="publish audit drift"):
        release.sportsdataverse_upload([asset], tag="t", audit=AuditSpec(key_cols=("season",), prior_dir=prior_dir))
    assert "data.parquet" in _uploaded_names(gh_recorder)


def test_upload_audit_manifest_appends(gh_recorder, tmp_path: Path) -> None:
    asset = tmp_path / "data.parquet"
    _frame().write_parquet(asset)
    manifest = tmp_path / "manifest.parquet"
    release.sportsdataverse_upload([asset], tag="t", audit=AuditSpec(manifest=manifest))
    assert pl.read_parquet(manifest).height == 1


def test_save_default_ships_fingerprint_sidecar(gh_recorder) -> None:
    release.sportsdataverse_save(
        _frame(),
        file_name="ds",
        sportsdataverse_type="test dataset",
        release_tag="t",
        pkg_function="sportsdataverse.test.load_ds()",
        file_types=("parquet",),
    )
    names = _uploaded_names(gh_recorder)
    assert "ds.parquet" in names
    assert ("ds.parquet" + FINGERPRINT_SUFFIX) in names


def test_save_audit_off_is_r_parity(gh_recorder) -> None:
    release.sportsdataverse_save(
        _frame(),
        file_name="ds",
        sportsdataverse_type="test dataset",
        release_tag="t",
        pkg_function="sportsdataverse.test.load_ds()",
        file_types=("parquet",),
        audit=None,
    )
    assert not any(n.endswith(FINGERPRINT_SUFFIX) for n in _uploaded_names(gh_recorder))


def test_save_audit_spec_blocks(gh_recorder) -> None:
    with pytest.raises(RuntimeError, match="BLOCKED"):
        release.sportsdataverse_save(
            _frame(rows=10),
            file_name="ds",
            sportsdataverse_type="test dataset",
            release_tag="t",
            pkg_function="sportsdataverse.test.load_ds()",
            file_types=("parquet",),
            audit=AuditSpec(row_floor=100),
        )
    assert gh_recorder == []
