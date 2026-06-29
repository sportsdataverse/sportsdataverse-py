from __future__ import annotations

import glob
import os

import polars as pl

from tools.validation.findings import OracleLike


class CfbSelfOracle:
    """CFB has no external oracle: invariants + release-regression only.

    The optional 0.36-live reconciliation diff is handled by the Tier-2 harness, not here.
    """

    domain = "cfb"
    column_map: dict[str, str] = {}
    thresholds: dict[str, float] = {}

    def reference_frame(self, dataset: str, keys: pl.DataFrame) -> pl.DataFrame | None:
        return None


class NflfastrOracle:
    """NFL numeric ground truth: nflfastR output read from a parquet glob."""

    domain = "nfl"
    column_map = {"ep": "ep", "epa": "epa", "wp": "wp", "vegas_wp": "vegas_wp", "cp": "cp"}
    # Correlation floors for parity vs the RAW full-history nflverse reference
    # (all play types). The producer is model-domain feature-substituted, so a
    # raw-vs-model comparison legitimately runs ~0.93-0.99 on some columns —
    # sub-floor matches are EXPECTED WARN needs_judgment findings routed to
    # Tier-2, not regressions (model-domain parity is ~0.99+). Recalibrating
    # these floors / filtering the reference to the model domain is a Tier-2 follow-up.
    thresholds = {"ep": 0.99, "epa": 0.99, "wp": 0.99, "vegas_wp": 0.99}

    def __init__(self, source_glob: str | None = None) -> None:
        self._source_glob = source_glob

    def reference_frame(self, dataset: str, keys: pl.DataFrame) -> pl.DataFrame | None:
        if self._source_glob is None:
            return None
        path = os.path.expandvars(self._source_glob)
        if not glob.glob(path):
            return None
        lazy = pl.scan_parquet(path)
        ref_cols = lazy.collect_schema().names()
        on = [c for c in keys.columns if c in ref_cols]
        if not on:
            return None
        want = on + [c for c in self.column_map.values() if c in ref_cols and c not in on]
        ref = lazy.select(want).collect()
        # Align reference join-key dtypes to the producer's (R writes ints as Float64;
        # this also coerces any other width mismatch — a non-coercible type raises
        # loudly instead of yielding a silent near-empty join).
        key_schema = keys.schema
        casts = [pl.col(c).cast(key_schema[c]) for c in on if ref.schema[c] != key_schema[c]]
        if casts:
            ref = ref.with_columns(casts)
        return ref.join(keys, on=on, how="inner")


ORACLES: dict[str, OracleLike] = {
    "cfb": CfbSelfOracle(),
    "nfl": NflfastrOracle(source_glob="${SDV_VALIDATION_NFL_DATA_ROOT}/python/.cache/nflverse_pbp/pbp_*.parquet"),
}
