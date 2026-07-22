"""Publish-integrity audit for release assets (WS1).

One shared pre-upload gate every producer calls before ``gh release upload``:

* :func:`fingerprint_frame` / :func:`fingerprint_parquet` -- per-column SHA256 +
  shape + null counts + dtypes + numeric summary stats, serialized to a JSON
  sidecar (``<asset>.fingerprint.json``) via :func:`write_fingerprint`.
* :func:`drift_report` -- magnitude-of-change vs the prior release's
  fingerprint (standardized mean shifts, null-rate deltas, column set/dtype
  changes, and a single L2 drift magnitude). **Warn-level** -- drift never
  blocks an upload.
* :func:`completeness_report` -- distinct/min/max ranges of the key columns
  (season / date / game id) + row-count floors vs the prior release.
  **Error-level** -- a failed completeness check blocks the upload.
* :func:`audit_asset` -- orchestrates the three for one asset.
* :func:`append_manifest` -- append-log manifest (parquet) of published assets.

Drift is computed fingerprint-vs-fingerprint, so auditing a new asset never
requires downloading the prior asset itself -- only its small JSON sidecar.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
producers and ``tools/publish_audit/cli.py`` import from here.
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json
import math
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import polars as pl

FINGERPRINT_VERSION = 1
FINGERPRINT_SUFFIX = ".fingerprint.json"

#: Warn when a shared numeric column's mean moves by more than this many
#: prior-release standard deviations.
DEFAULT_MEAN_SHIFT_SIGMA = 0.5
#: Warn when a shared column's null rate moves by more than this fraction.
DEFAULT_NULL_RATE_DELTA = 0.05
#: Completeness tolerance: rows / key-distinct counts may shrink by at most
#: this fraction vs the prior release before the audit errors.
DEFAULT_SHRINK_TOLERANCE = 0.02


@dataclasses.dataclass(frozen=True)
class PublishAudit:
    """Result of auditing one release asset.

    Attributes:
        asset: Asset label (usually the file basename).
        fingerprint: The asset's fingerprint dict (see :func:`fingerprint_frame`).
        drift_warnings: Warn-level drift findings vs the prior fingerprint
            (empty when no prior was supplied or nothing moved).
        drift_l2: Single L2 magnitude of the standardized mean shifts across
            shared numeric columns (0.0 when no prior / no shared columns).
        errors: Error-level completeness findings; non-empty means BLOCK.
    """

    asset: str
    fingerprint: Dict[str, Any]
    drift_warnings: List[str]
    drift_l2: float
    errors: List[str]

    @property
    def ok(self) -> bool:
        """True when no error-level finding was raised (drift never blocks)."""
        return not self.errors


def _git_sha(cwd: Optional[Union[str, Path]] = None) -> Optional[str]:
    """Best-effort short git SHA of the producing checkout (None off-repo)."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(cwd) if cwd is not None else None,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    sha = out.stdout.strip()
    return sha if out.returncode == 0 and sha else None


def _json_scalar(value: Any) -> Any:
    """Coerce a polars scalar to a JSON-stable value (NaN/inf -> None)."""
    if value is None:
        return None
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, (int, str, bool)):
        return value
    return str(value)


def fingerprint_frame(df: pl.DataFrame, *, asset: str = "") -> Dict[str, Any]:
    """Fingerprint a frame: shape + per-column SHA256, nulls, dtype, stats.

    The per-column content hash is the SHA256 of the column serialized with
    ``write_csv`` -- deterministic for a given polars version and cheap even on
    multi-million-row frames. Numeric columns additionally carry
    ``mean``/``std``/``min``/``max`` so :func:`drift_report` can measure
    magnitude-of-change from two fingerprints alone.

    Args:
        df: The frame to fingerprint.
        asset: Optional asset label recorded in the fingerprint.

    Returns:
        A JSON-serializable fingerprint dict (``version``, ``asset``,
        ``n_rows``, ``n_cols``, ``columns``, ``produced_at``, ``git_sha``).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.modeling.integrity.publish_audit import fingerprint_frame
            fp = fingerprint_frame(pl.DataFrame({"season": [2024, 2025]}))
            print(fp["n_rows"], list(fp["columns"]))
    """
    columns: Dict[str, Any] = {}
    for col in df.columns:
        series = df.get_column(col)
        dtype = series.dtype
        # ponytail: csv-serialize-then-hash; swap for arrow-buffer hashing if
        # publish-time cost ever matters.
        payload = df.select(col).write_csv().encode("utf-8", errors="replace")
        entry: Dict[str, Any] = {
            "dtype": str(dtype),
            "null_count": int(series.null_count()),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        if dtype.is_numeric():
            entry["mean"] = _json_scalar(series.mean())
            entry["std"] = _json_scalar(series.std())
            entry["min"] = _json_scalar(series.min())
            entry["max"] = _json_scalar(series.max())
        columns[col] = entry
    return {
        "version": FINGERPRINT_VERSION,
        "asset": asset,
        "n_rows": df.height,
        "n_cols": df.width,
        "columns": columns,
        "produced_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "git_sha": _git_sha(),
    }


def fingerprint_parquet(path: Union[str, Path]) -> Dict[str, Any]:
    """Fingerprint a parquet asset (adds file-level SHA256 + byte size).

    Args:
        path: Path to the parquet file.

    Returns:
        The :func:`fingerprint_frame` dict plus ``file_sha256`` / ``file_bytes``.

    Example:
        Quick start::

            from sportsdataverse.modeling.integrity.publish_audit import fingerprint_parquet
            fp = fingerprint_parquet("mbb_pbp_2025.parquet")
            print(fp["file_sha256"][:12], fp["n_rows"])
    """
    p = Path(path)
    fp = fingerprint_frame(pl.read_parquet(p), asset=p.name)
    fp["file_sha256"] = hashlib.sha256(p.read_bytes()).hexdigest()
    fp["file_bytes"] = p.stat().st_size
    return fp


def write_fingerprint(asset_path: Union[str, Path], fingerprint: Dict[str, Any]) -> Path:
    """Write the fingerprint sidecar next to the asset.

    Args:
        asset_path: The asset the fingerprint describes.
        fingerprint: The fingerprint dict.

    Returns:
        The sidecar path (``<asset>.fingerprint.json``).

    Example:
        Pipeline step::

            from sportsdataverse.modeling.integrity.publish_audit import (
                fingerprint_parquet, write_fingerprint,
            )
            write_fingerprint("a.parquet", fingerprint_parquet("a.parquet"))
    """
    sidecar = Path(str(asset_path) + FINGERPRINT_SUFFIX)
    sidecar.write_text(json.dumps(fingerprint, indent=2, sort_keys=True), encoding="utf-8")
    return sidecar


def read_fingerprint(path: Union[str, Path]) -> Dict[str, Any]:
    """Read a fingerprint sidecar (accepts the sidecar path or the asset path).

    Args:
        path: Sidecar path, or the asset path whose sidecar sits next to it.

    Returns:
        The fingerprint dict.

    Raises:
        FileNotFoundError: When neither the path nor its sidecar exists.
    """
    p = Path(path)
    if not str(p).endswith(FINGERPRINT_SUFFIX):
        sidecar = Path(str(p) + FINGERPRINT_SUFFIX)
        p = sidecar if sidecar.exists() else p
    return dict(json.loads(p.read_text(encoding="utf-8")))


def drift_report(
    current: Dict[str, Any],
    prior: Dict[str, Any],
    *,
    mean_shift_sigma: float = DEFAULT_MEAN_SHIFT_SIGMA,
    null_rate_delta: float = DEFAULT_NULL_RATE_DELTA,
) -> "tuple[List[str], float]":
    """Warn-level drift between two fingerprints (never blocks an upload).

    Args:
        current: Fingerprint of the asset about to be published.
        prior: Fingerprint of the prior release of the same asset.
        mean_shift_sigma: Warn when a shared numeric column's mean moved by
            more than this many prior standard deviations.
        null_rate_delta: Warn when a shared column's null rate moved by more
            than this fraction.

    Returns:
        ``(warnings, drift_l2)`` -- human-readable warning strings plus the L2
        magnitude of standardized mean shifts across shared numeric columns.

    Example:
        Quick start::

            from sportsdataverse.modeling.integrity.publish_audit import (
                drift_report, fingerprint_frame, read_fingerprint,
            )
            warnings, l2 = drift_report(fingerprint_frame(df), read_fingerprint("prior.parquet"))
    """
    warnings: List[str] = []
    cur_cols: Dict[str, Any] = current.get("columns", {})
    pri_cols: Dict[str, Any] = prior.get("columns", {})

    added = sorted(set(cur_cols) - set(pri_cols))
    removed = sorted(set(pri_cols) - set(cur_cols))
    if added:
        warnings.append(f"columns added vs prior: {added}")
    if removed:
        warnings.append(f"columns removed vs prior: {removed}")

    cur_rows = int(current.get("n_rows", 0))
    pri_rows = int(prior.get("n_rows", 0))
    if pri_rows > 0:
        row_delta = (cur_rows - pri_rows) / pri_rows
        if abs(row_delta) > 0:
            warnings.append(f"row count {pri_rows} -> {cur_rows} ({row_delta:+.1%})")

    shifts: List[float] = []
    for col in sorted(set(cur_cols) & set(pri_cols)):
        cur_c, pri_c = cur_cols[col], pri_cols[col]
        if cur_c.get("dtype") != pri_c.get("dtype"):
            warnings.append(f"{col}: dtype {pri_c.get('dtype')} -> {cur_c.get('dtype')}")
        if cur_rows > 0 and pri_rows > 0:
            cur_null = cur_c.get("null_count", 0) / cur_rows
            pri_null = pri_c.get("null_count", 0) / pri_rows
            if abs(cur_null - pri_null) > null_rate_delta:
                warnings.append(f"{col}: null rate {pri_null:.1%} -> {cur_null:.1%}")
        cur_mean, pri_mean = cur_c.get("mean"), pri_c.get("mean")
        pri_std = pri_c.get("std")
        if cur_mean is not None and pri_mean is not None:
            if pri_std is not None and pri_std > 0:
                shift = abs(cur_mean - pri_mean) / pri_std
            else:
                shift = abs(cur_mean - pri_mean)
            shifts.append(shift)
            if shift > mean_shift_sigma:
                warnings.append(f"{col}: mean {pri_mean:.4g} -> {cur_mean:.4g} (shift {shift:.2f} sigma)")
    drift_l2 = math.sqrt(sum(s * s for s in shifts)) if shifts else 0.0
    return warnings, drift_l2


def completeness_report(
    fingerprint_or_df: Union[Dict[str, Any], pl.DataFrame],
    *,
    key_cols: Sequence[str] = (),
    df: Optional[pl.DataFrame] = None,
    prior: Optional[Dict[str, Any]] = None,
    row_floor: Optional[int] = None,
    tolerance: float = DEFAULT_SHRINK_TOLERANCE,
) -> List[str]:
    """Error-level completeness findings; non-empty result means BLOCK.

    Checks, in order: absolute ``row_floor``, row shrinkage vs the prior
    release beyond ``tolerance``, and per key column (season / date / game id)
    distinct-count shrinkage or range loss vs the prior release. Key-column
    ranges are recorded INTO the fingerprint (``keys`` entry) as a side effect
    so the prior side of the comparison is fingerprint-only on the next run.

    Args:
        fingerprint_or_df: The current fingerprint dict (pass ``df=`` too so
            key ranges can be computed), or a bare frame.
        key_cols: Identity columns whose coverage must never silently shrink.
        df: The frame backing the fingerprint (required for ``key_cols`` when
            a fingerprint dict is passed).
        prior: Prior release fingerprint (with ``keys``) to compare against.
        row_floor: Absolute minimum row count.
        tolerance: Allowed fractional shrinkage vs the prior release.

    Returns:
        List of error strings (empty = complete).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.modeling.integrity.publish_audit import completeness_report
            errors = completeness_report(df, key_cols=["season"], row_floor=1000)
            assert not errors, errors
    """
    if isinstance(fingerprint_or_df, pl.DataFrame):
        frame: Optional[pl.DataFrame] = fingerprint_or_df
        fingerprint: Dict[str, Any] = {"n_rows": fingerprint_or_df.height}
    else:
        fingerprint = fingerprint_or_df
        frame = df

    errors: List[str] = []
    n_rows = int(fingerprint.get("n_rows", 0))
    if row_floor is not None and n_rows < row_floor:
        errors.append(f"row count {n_rows} below floor {row_floor}")

    keys: Dict[str, Any] = {}
    for col in key_cols:
        if frame is None:
            raise ValueError("key_cols requires the backing frame (pass df=)")
        if col not in frame.columns:
            errors.append(f"key column '{col}' missing from asset")
            continue
        series = frame.get_column(col)
        keys[col] = {
            "n_distinct": int(series.n_unique()),
            "min": _json_scalar(series.min()),
            "max": _json_scalar(series.max()),
        }
    if keys:
        fingerprint["keys"] = keys

    if prior is not None:
        pri_rows = int(prior.get("n_rows", 0))
        if pri_rows > 0 and n_rows < pri_rows * (1.0 - tolerance):
            errors.append(f"row count {n_rows} shrank vs prior {pri_rows} beyond tolerance {tolerance:.0%}")
        for col, pri_key in (prior.get("keys") or {}).items():
            cur_key = keys.get(col)
            if cur_key is None:
                continue
            pri_distinct = int(pri_key.get("n_distinct", 0))
            if pri_distinct > 0 and cur_key["n_distinct"] < pri_distinct * (1.0 - tolerance):
                errors.append(f"{col}: distinct {cur_key['n_distinct']} shrank vs prior {pri_distinct}")
            pri_max, cur_max = pri_key.get("max"), cur_key.get("max")
            # Numeric keys only: lexicographic compare on string ids ("g1009" <
            # "g999") would flag growing datasets; string keys are covered by
            # the distinct-count shrink check above.
            if isinstance(pri_max, (int, float)) and isinstance(cur_max, (int, float)) and cur_max < pri_max:
                errors.append(f"{col}: max {cur_max} regressed below prior {pri_max}")
    return errors


def audit_asset(
    path: Union[str, Path],
    *,
    key_cols: Sequence[str] = (),
    prior: Optional[Dict[str, Any]] = None,
    row_floor: Optional[int] = None,
    tolerance: float = DEFAULT_SHRINK_TOLERANCE,
    mean_shift_sigma: float = DEFAULT_MEAN_SHIFT_SIGMA,
    write_sidecar: bool = True,
) -> PublishAudit:
    """Audit one parquet asset pre-upload: fingerprint + drift + completeness.

    Args:
        path: Parquet asset to audit.
        key_cols: Identity columns for the completeness check.
        prior: Prior release fingerprint (drift + shrink comparisons).
        row_floor: Absolute minimum row count (completeness).
        tolerance: Allowed fractional shrinkage vs prior (completeness).
        mean_shift_sigma: Drift warn threshold in prior standard deviations.
        write_sidecar: Write ``<asset>.fingerprint.json`` next to the asset.

    Returns:
        A :class:`PublishAudit`; ``result.ok`` False means DO NOT upload.

    Example:
        Producer gate::

            from sportsdataverse.modeling.integrity.publish_audit import audit_asset, read_fingerprint
            result = audit_asset("wnba_pbp_2026.parquet", key_cols=["season", "game_id"],
                                 prior=read_fingerprint("prior/wnba_pbp_2026.parquet"))
            if not result.ok:
                raise SystemExit("\\n".join(result.errors))
    """
    p = Path(path)
    frame = pl.read_parquet(p)
    fingerprint = fingerprint_frame(frame, asset=p.name)
    fingerprint["file_sha256"] = hashlib.sha256(p.read_bytes()).hexdigest()
    fingerprint["file_bytes"] = p.stat().st_size

    errors = completeness_report(
        fingerprint,
        key_cols=key_cols,
        df=frame,
        prior=prior,
        row_floor=row_floor,
        tolerance=tolerance,
    )
    warnings: List[str] = []
    drift_l2 = 0.0
    if prior is not None:
        warnings, drift_l2 = drift_report(fingerprint, prior, mean_shift_sigma=mean_shift_sigma)
    if write_sidecar:
        write_fingerprint(p, fingerprint)
    return PublishAudit(
        asset=p.name,
        fingerprint=fingerprint,
        drift_warnings=warnings,
        drift_l2=drift_l2,
        errors=errors,
    )


def append_manifest(manifest_path: Union[str, Path], audit: PublishAudit) -> pl.DataFrame:
    """Append one audited asset to the parquet manifest append-log.

    The manifest is an append-only log (one row per published asset per run)
    mirroring the wehoop producer manifest pattern: ``asset``, ``n_rows``,
    ``file_sha256``, ``drift_l2``, ``keys`` (JSON), ``produced_at``, ``git_sha``.

    Args:
        manifest_path: Manifest parquet path (created when absent).
        audit: The asset's :class:`PublishAudit`.

    Returns:
        The full manifest frame after the append.

    Example:
        Pipeline step::

            from sportsdataverse.modeling.integrity.publish_audit import append_manifest, audit_asset
            append_manifest("manifest.parquet", audit_asset("a.parquet"))
    """
    fp = audit.fingerprint
    row = pl.DataFrame(
        {
            "asset": [audit.asset],
            "n_rows": [int(fp.get("n_rows", 0))],
            "file_sha256": [fp.get("file_sha256")],
            "drift_l2": [float(audit.drift_l2)],
            "keys": [json.dumps(fp.get("keys", {}), sort_keys=True)],
            "produced_at": [fp.get("produced_at")],
            "git_sha": [fp.get("git_sha")],
        }
    )
    mp = Path(manifest_path)
    if mp.exists():
        manifest = pl.concat([pl.read_parquet(mp), row], how="diagonal_relaxed")
    else:
        manifest = row
    manifest.write_parquet(mp)
    return manifest
