"""ESPN Total QBR is the oracle for the NFL QBR feature order (gap-C-083 = W-C3, task 2.7).

`fixtures/espn_qbr_2023_slice.parquet` holds every 2023 quarterback-game ESPN rated (573) (provenance:
`fixtures/README.md`): the six model inputs the processor builds from the committed nfl-raw summary, and
ESPN's own `qbr_total`. Scoring the bundled booster in `qbr_vars` order must rank quarterbacks closer to
ESPN than the pre-2.7 order did; the floor is the measured post-reorder value, rounded down.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from xgboost import Booster, DMatrix

from sportsdataverse.nfl.model_vars import qbr_vars

ROOT = Path(__file__).resolve().parents[2]
SLICE = Path(__file__).parent / "fixtures" / "espn_qbr_2023_slice.parquet"
OLD_ORDER = ["qbr_epa", "sack_epa", "pass_epa", "rush_epa", "pen_epa", "spread"]  # pre-2.7 qbr_vars
# Measured 2026-10-11 on the slice: trainer order 0.7857, pre-2.7 order 0.7048 (pass/sack swapped: 0.6596).
# R795-1 (empty play-type group -> 0, as the trainer did): trainer order 0.8097, pre-2.7 order 0.7334.
FLOOR = 0.809
EMPTY_GROUP = ["rush_epa", "sack_epa", "pen_epa"]


def _spearman(order: list[str]) -> float:
    # The slice was recorded before R795-1: its nulls (76 rush, 87 sack, 45 penalty) are QBs with no such play,
    # which the processor now serves as 0 (the trainer's replace_na). A re-record makes this fill a no-op.
    df = pl.read_parquet(SLICE).with_columns(pl.col(EMPTY_GROUP).fill_null(0.0))
    b = Booster()
    b.load_model(str(ROOT / "sportsdataverse" / "nfl" / "models" / "qbr_model.ubj"))
    # Unnamed DMatrix: score the column ORDER alone, so a wrong qbr_vars shows up as a worse ranking.
    pred = b.predict(DMatrix(df.select(order).to_numpy()), validate_features=False)
    return pl.DataFrame({"p": pred, "q": df["qbr_total"]}).select(pl.corr("p", "q", method="spearman")).item()


def test_slice_is_the_2023_season():
    df = pl.read_parquet(SLICE)
    assert df.height >= 500 and df["qbr_total"].null_count() == 0
    assert df.select(pl.struct("game_id", "player_id").n_unique()).item() == df.height


def test_qbr_vars_order_ranks_quarterbacks_like_espn():
    got = _spearman(list(qbr_vars))
    print("QBR_SPEARMAN", round(got, 4), "pre-2.7 order", round(_spearman(OLD_ORDER), 4))
    assert got >= FLOOR, got
    assert got > _spearman(OLD_ORDER) + 0.02, got
