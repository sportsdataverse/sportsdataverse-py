from __future__ import annotations

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
        ref = pl.read_parquet(self._source_glob)
        on = [c for c in keys.columns if c in ref.columns]
        return ref.join(keys, on=on, how="inner")


ORACLES: dict[str, OracleLike] = {"cfb": CfbSelfOracle(), "nfl": NflfastrOracle()}
