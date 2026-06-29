from __future__ import annotations

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
    thresholds = {"ep": 0.99, "epa": 0.99, "wp": 0.99, "vegas_wp": 0.99}

    def __init__(self, source_glob: str | None = None) -> None:
        self._source_glob = source_glob

    def reference_frame(self, dataset: str, keys: pl.DataFrame) -> pl.DataFrame | None:
        if self._source_glob is None:
            return None
        path = os.path.expandvars(self._source_glob)
        lazy = pl.scan_parquet(path)
        ref_cols = lazy.collect_schema().names()
        on = [c for c in keys.columns if c in ref_cols]
        if not on:
            return None
        want = on + [c for c in self.column_map.values() if c in ref_cols and c not in on]
        ref = lazy.select(want).collect()
        # R serialises integer columns as Float64 in parquet; cast join keys to
        # match the producer's Int64 dtype so the join succeeds without coercion.
        key_schema = keys.schema
        casts = [
            pl.col(c).cast(key_schema[c]) for c in on if ref.schema[c] != key_schema[c] and ref.schema[c] == pl.Float64
        ]
        if casts:
            ref = ref.with_columns(casts)
        return ref.join(keys, on=on, how="inner")


ORACLES: dict[str, OracleLike] = {
    "cfb": CfbSelfOracle(),
    "nfl": NflfastrOracle(source_glob="${SDV_VALIDATION_NFL_DATA_ROOT}/python/.cache/nflverse_pbp/pbp_*.parquet"),
}
