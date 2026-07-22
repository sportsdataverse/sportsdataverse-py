"""CLI for the publish-integrity audit (WS1).

Producer usage (Python or R, pre-upload)::

    uv run python -m tools.publish_audit.cli out/*.parquet \
        --prior-dir prior_fingerprints/ --key-cols season,game_id --row-floor 1000

Exit code is non-zero when ANY asset fails a completeness check (BLOCK).
Drift findings are warn-only and never affect the exit code.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from sportsdataverse._common.publish_audit import (
    DEFAULT_SHRINK_TOLERANCE,
    FINGERPRINT_SUFFIX,
    audit_asset,
    append_manifest,
    read_fingerprint,
)


def _collect_assets(paths: List[str]) -> List[Path]:
    assets: List[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            assets.extend(sorted(p.glob("*.parquet")))
        else:
            assets.append(p)
    return assets


def _prior_for(asset: Path, prior_dir: Optional[str]) -> Optional[Dict[str, Any]]:
    if prior_dir is None:
        return None
    sidecar = Path(prior_dir) / (asset.name + FINGERPRINT_SUFFIX)
    if not sidecar.exists():
        return None
    return read_fingerprint(sidecar)


def main(argv: Optional[List[str]] = None) -> int:
    """Audit release assets pre-upload; returns process exit code."""
    parser = argparse.ArgumentParser(
        prog="publish_audit",
        description="Fingerprint + drift + completeness gate for release assets.",
    )
    parser.add_argument("paths", nargs="+", help="Parquet files or directories of them.")
    parser.add_argument("--prior-dir", help="Directory holding prior-release fingerprint sidecars.")
    parser.add_argument("--key-cols", default="", help="Comma-separated identity columns.")
    parser.add_argument("--row-floor", type=int, default=None, help="Absolute minimum row count.")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_SHRINK_TOLERANCE,
        help="Allowed fractional shrinkage vs prior (default %(default)s).",
    )
    parser.add_argument("--no-sidecar", action="store_true", help="Skip writing fingerprint sidecars.")
    parser.add_argument("--manifest", help="Append audited assets to this parquet manifest log.")
    parser.add_argument("--json", action="store_true", help="Emit one JSON report to stdout.")
    args = parser.parse_args(argv)

    key_cols = [c.strip() for c in args.key_cols.split(",") if c.strip()]
    assets = _collect_assets(args.paths)
    if not assets:
        print("publish_audit: no parquet assets found", file=sys.stderr)
        return 2

    reports: List[Dict[str, Any]] = []
    failed = False
    for asset in assets:
        result = audit_asset(
            asset,
            key_cols=key_cols,
            prior=_prior_for(asset, args.prior_dir),
            row_floor=args.row_floor,
            tolerance=args.tolerance,
            write_sidecar=not args.no_sidecar,
        )
        if args.manifest:
            append_manifest(args.manifest, result)
        failed = failed or not result.ok
        reports.append(
            {
                "asset": result.asset,
                "ok": result.ok,
                "n_rows": result.fingerprint.get("n_rows"),
                "drift_l2": result.drift_l2,
                "drift_warnings": result.drift_warnings,
                "errors": result.errors,
            }
        )
        if not args.json:
            status = "OK   " if result.ok else "BLOCK"
            print(f"[{status}] {result.asset}  rows={result.fingerprint.get('n_rows')}  drift_l2={result.drift_l2:.3f}")
            for w in result.drift_warnings:
                print(f"    warn: {w}")
            for e in result.errors:
                print(f"    ERROR: {e}")
    if args.json:
        print(json.dumps(reports, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
