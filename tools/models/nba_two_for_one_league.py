"""BB-D44's league line: :func:`two_for_one_summary` per regular season and pooled, NBA and WNBA.

Run (checkouts of hoopR-nba-stats-data and wehoop-wnba-stats-data)::

    uv run python tools/models/nba_two_for_one_league.py <hoopR-nba-stats-data>/nba_stats <wehoop-wnba-stats-data>/wnba_stats

Each season's published possessions are flagged with
:func:`~sportsdataverse.nba.nba_possession_flags.add_two_for_one_flags` at the shipped window and
floor. A possessions file without the BB-D7 columns (the WNBA files) gets them from
:func:`~sportsdataverse.nba.nba_possession_flags.add_possession_flags` first, because the summary
drops garbage time. Regular season only (``game_id`` type digit ``2``). Writes
``nba_two_for_one_league_{nba,wnba}.csv`` beside this file, one row per season plus a pooled row
(``season`` null), whose bootstrap resamples whole games across every season. The numbers are
descriptive: ``net_diff`` is went minus held in matched clock bins, not the value of going.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_possession_flags import (
    TWO_FOR_ONE_SCHEMA,
    add_possession_flags,
    add_two_for_one_flags,
    two_for_one_summary,
)

LEAGUES = {
    "nba": ("nba_possessions_{}", "nba_play_by_play_{}", range(2016, 2027)),
    "wnba": ("wnba_possessions_{}", "wnba_play_by_play_{}", range(2006, 2027)),
}


def season(data: Path, poss_name: str, pbp_name: str, year: int) -> pl.DataFrame:
    poss = pl.read_parquet(data / "possessions" / "parquet" / f"{poss_name.format(year)}.parquet")
    pbp = pl.read_parquet(data / "pbp" / "parquet" / f"{pbp_name.format(year)}.parquet")
    poss = poss.drop(*TWO_FOR_ONE_SCHEMA, strict=False)
    if "is_garbage_time" not in poss.columns:
        poss = add_possession_flags(poss, pbp)
    flagged = add_two_for_one_flags(poss, pbp)
    return flagged.filter(pl.col("game_id").str.slice(2, 1) == "2").with_columns(pl.lit(year).alias("season"))


def league(data: Path, poss_name: str, pbp_name: str, years: range) -> pl.DataFrame:
    allf = pl.concat([season(data, poss_name, pbp_name, y) for y in years], how="diagonal_relaxed")
    per = two_for_one_summary(allf, by=("season",))
    pooled = two_for_one_summary(allf, by=()).with_columns(pl.lit(None, dtype=per.schema["season"]).alias("season"))
    return pl.concat([per, pooled.select(per.columns)])


if __name__ == "__main__":
    for (name, (poss_name, pbp_name, years)), arg in zip(LEAGUES.items(), sys.argv[1:3], strict=True):
        out = league(Path(arg), poss_name, pbp_name, years)
        out.write_csv(Path(__file__).with_name(f"nba_two_for_one_league_{name}.csv"), float_precision=3)
        with pl.Config(tbl_rows=40, tbl_cols=20, tbl_width_chars=250, float_precision=3):
            print(name)
            print(out)
