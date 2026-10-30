"""#623: a passer ESPN spells two ways ("Noah Kim" / "N. Kim", game 401866408) is one box-score row.

The live path joins the roster, so every Kim play carries his id 4690158 while the sack text reads
"N. Kim". A supplied summary is not joined, so the test sets the id the roster join assigns.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl

from sportsdataverse.cfb import CFBPlayProcess

# outside fixtures/summary_* so the whole-game oracle does not pick it up
FIX = Path(__file__).parent / "fixtures" / "box_names" / "summary_401866408.json.gz"
KIM_ID = 4690158


def test_two_spellings_of_one_passer_are_one_row() -> None:
    summary = json.load(gzip.open(FIX, "rt", encoding="utf-8"))
    proc = CFBPlayProcess(gameId=401866408)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    plays = proc.plays_frame.with_columns(
        passer_player_id=pl.when(pl.col("passer_player_name").is_in(["N.Kim", "N. Kim"]))
        .then(pl.lit(KIM_ID, dtype=pl.Int64))
        .otherwise(pl.col("passer_player_id"))
    )
    rows = proc.create_box_score(plays)["pass"]
    kim = [r for r in rows if "Kim" in str(r["passer_player_name"])]
    # before #623: an "N.Kim" row with his attempts and a 0-for-0 "N. Kim" row from the sack
    assert len(kim) == 1, [(r["passer_player_name"], r["Att"]) for r in kim]
