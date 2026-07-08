"""Oracle gates for the NGS over-expected models (offline, committed fixtures).

Fixtures: real 2022+2023 NGS season-level slices captured 2026-07-08 via
``load_nfl_nextgen_stats`` (see ``tests/fixtures/nfl_ngs/README.md``).

Gate discipline: floors below are from observed values on the committed
fixtures — NEVER lower a gate to pass; debug the model instead.
"""

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_ngs_constants import next_season_stability
from sportsdataverse.nfl.nfl_ngs_tracking import nfl_ngs_yac_oe

FIX = "tests/fixtures/nfl_ngs"


def _fixture_loader(stat_type):
    df = pl.read_parquet(f"{FIX}/ngs_{stat_type}_2022_2023.parquet")

    def _loader(seasons, stat_type=stat_type, return_as_pandas=False):
        return df.filter(pl.col("season").is_in(pl.Series([int(s) for s in seasons]).implode()))

    return _loader


def test_yac_raw_equals_ngs_field():
    """Concurrent oracle: yac_oe_raw is EXACTLY the NGS-shipped field."""
    out = nfl_ngs_yac_oe([2023], _loader=_fixture_loader("receiving"))
    raw = (
        pl.read_parquet(f"{FIX}/ngs_receiving_2022_2023.parquet")
        .filter((pl.col("season") == 2023) & (pl.col("week") == 0))
        .select("player_gsis_id", "avg_yac_above_expectation")
    )
    assert out.schema["player_gsis_id"] == raw.schema["player_gsis_id"]
    j = out.join(raw, on="player_gsis_id", how="inner")
    assert j.height == out.height == 115  # observed fixture row count
    assert np.allclose(j["yac_oe_raw"].to_numpy(), j["avg_yac_above_expectation"].to_numpy(), atol=1e-9)


def test_yac_shrink_pulls_inward():
    """Every row lands between raw and the qualified prior mean (mu-relative)."""
    out = nfl_ngs_yac_oe([2023], _loader=_fixture_loader("receiving"))
    q = out.filter(pl.col("receptions") >= 10)
    mu = float(np.average(q["yac_oe_raw"].to_numpy(), weights=q["receptions"].to_numpy()))
    raw = out["yac_oe_raw"].to_numpy()
    shrunk = out["yac_oe_shrunk"].to_numpy()
    assert np.all(np.abs(shrunk - mu) <= np.abs(raw - mu) + 1e-9)
    rel = out["reliability"].to_numpy()
    assert np.all((rel >= 0.0) & (rel <= 1.0))


def test_yac_stability_shrunk_beats_raw():
    """Stability oracle: corr(shrunk_2022, raw_2023) >= corr(raw_2022, raw_2023).

    As-of-season boundary: the 2022 prior is fit on 2022 rows only; 2023 enters
    only as the evaluation target. Observed on the committed fixture:
    raw->raw 0.3939, shrunk->raw 0.4010 (n=83 joined players; reliability
    range on 2023 is [0.528, 0.868]). Do NOT lower.
    """
    ld = _fixture_loader("receiving")
    cur = nfl_ngs_yac_oe([2022], _loader=ld)
    nxt = nfl_ngs_yac_oe([2023], _loader=ld).select("player_gsis_id", pl.col("yac_oe_raw").alias("raw_next"))
    s_shrunk = next_season_stability(cur, nxt, "player_gsis_id", "yac_oe_shrunk", "raw_next")
    s_raw = next_season_stability(cur, nxt, "player_gsis_id", "yac_oe_raw", "raw_next")
    # never lower this gate to pass — debug the shrinkage prior fit instead
    assert s_shrunk >= s_raw - 1e-6
