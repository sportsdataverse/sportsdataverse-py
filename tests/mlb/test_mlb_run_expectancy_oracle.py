"""RE24 oracle gate: sportsdataverse RE24 vs the published Tango/*The Book* table.

Corpus: tests/fixtures/mlb_game_state/pbp_corpus.parquet (2000-04-03..2000-06-30,
inside the 1999-2002 span the committed re24_tango_book.parquet covers -- see
tests/fixtures/mlb_game_state/README.md for full provenance).

Gate (never lower to pass -- debug the model / widen the corpus instead):
  - per-state |re_mine - re_tango| <= 0.05 runs
  - bases-empty/0-out anchor in [0.45, 0.58]
  - strict monotonicity: RE non-increasing in outs within a base_state
"""

import polars as pl

from sportsdataverse.mlb.mlb_run_expectancy import mlb_run_expectancy_matrix

FIXTURE_DIR = "tests/fixtures/mlb_game_state"


def test_re24_matches_tango():
    pbp = pl.read_parquet(f"{FIXTURE_DIR}/pbp_corpus.parquet")
    tango = pl.read_parquet(f"{FIXTURE_DIR}/re24_tango_book.parquet")
    mine = mlb_run_expectancy_matrix(pbp=pbp)

    assert mine.schema["base_state"] == tango.schema["base_state"]
    assert mine.schema["outs"] == tango.schema["outs"]
    j = mine.join(tango, on=["base_state", "outs"], how="inner", validate="1:1")
    assert j.height == 24

    # per-state absolute tolerance
    max_diff = (j["re"] - j["re_right"]).abs().max()
    assert max_diff <= 0.05, f"max per-state |re_mine - re_tango| = {max_diff:.4f} (floor 0.05)"

    # anchor: bases empty, 0 outs
    empty0 = j.filter((pl.col("base_state") == "___") & (pl.col("outs") == 0))["re"][0]
    assert 0.45 <= empty0 <= 0.58, f"bases-empty/0-out RE = {empty0:.4f} (expected in [0.45, 0.58])"

    # strict monotonicity: within a base_state, RE decreases (or holds) as outs increase
    for bs in mine["base_state"].unique().to_list():
        r = mine.filter(pl.col("base_state") == bs).sort("outs")["re"].to_list()
        assert all(earlier >= later - 1e-9 for earlier, later in zip(r, r[1:])), (
            f"RE not monotone non-increasing in outs for base_state={bs!r}: {r}"
        )
