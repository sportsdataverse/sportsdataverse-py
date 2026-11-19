"""Gate :func:`sportsdataverse.nba.nba_rapm_variants.nba_multiseason_rapm` against an external 8-season RAPM.

The oracle is Jeremias Engelmann's single RAPM fit over the 2019-20 through 2026-27 NBA regular
seasons (posted 2026-11-02): a CSV keyed by NBA ``PERSON_ID`` with ``ID, Player, Off, Def, Poss, Tot``
(``Def`` negative = good, ``Tot = Off - Def``). It carries no license, so it is read from a local path
and never committed.

The script fits our multi-season RAPM over the same window (``nba_stats_possessions`` release assets,
regular-season games only, via the ``nba_stats_schedules`` release), joins on ``player_id`` (dtype
asserted equal first) and prints the match rate, the Spearman rho of our total / offense / defense
against his at three possession floors, and the ratio of our spread to his. It exits 1 when Spearman
on the total at ``Poss >= 10,000`` is below :data:`RHO_FLOOR`, the register's bar, or when fewer than
:data:`MATCH_RATE_FLOOR` of his players match.

Usage::

    uv run --frozen python tools/gate_multiseason_rapm.py ORACLE_CSV [--out report.json] [--cache poss.parquet]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions, load_nba_stats_schedules
from sportsdataverse.nba.nba_rapm_variants import nba_multiseason_rapm

#: Season START years of the window (2019-20 .. 2026-27); the release stamps ``season`` with the end year.
STARTS = range(2019, 2027)
#: Spearman floor on the total at ``Poss >= 10,000`` (Intake Register ACC-2026-0001). Never lowered.
RHO_FLOOR = 0.95
#: Share of the oracle's players that must match on ``player_id`` (observed 1,224 / 1,224 on 2026-11-18).
MATCH_RATE_FLOOR = 0.99
FLOORS = (0, 10_000, 30_000)
_LINEUP = [f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)]


def regular_season_possessions() -> pl.DataFrame:
    """The window's regular-season possessions, only the columns the fit reads."""
    frames = []
    for start in STARTS:
        rs = (
            load_nba_stats_schedules(start)
            .filter(pl.col("season_type") == "regular-season")
            .select(pl.col("game_id").cast(pl.Utf8).unique())
        )
        poss = load_nba_stats_possessions(start).with_columns(pl.col("game_id").cast(pl.Utf8))
        frames.append(
            poss.join(rs, on="game_id", how="semi")
            .drop_nulls(["points"])
            .select("game_id", "season", "points", *_LINEUP)
        )
        print(f"{start}-{str(start + 1)[2:]}: {frames[-1].height:,} regular-season possessions", flush=True)
    return pl.concat(frames)


def gate(ours: pl.DataFrame, oracle: pl.DataFrame) -> dict:
    """Match rate, Spearman and spread of ``ours`` (``nba_multiseason_rapm``) against the oracle."""
    oracle = oracle.rename({"ID": "player_id"})
    assert oracle.schema["player_id"] == ours.schema["player_id"], (
        oracle.schema["player_id"],
        ours.schema["player_id"],
    )
    j = oracle.join(ours, on="player_id", how="inner")

    def rho(df: pl.DataFrame, a: str, b: str) -> float:
        return round(df.select(pl.corr(a, b, method="spearman")).item(), 4)

    report: dict = {"oracle_rows": oracle.height, "matched": j.height, "match_rate": round(j.height / oracle.height, 4)}
    for floor in FLOORS:
        k = j.filter(pl.col("Poss") >= floor)
        report[f"poss_ge_{floor}"] = {
            "n": k.height,
            "rho_total": rho(k, "Tot", "multiseason_rapm"),
            # his Def is negative-good, ours positive-good: report the agreement as a positive rho
            "rho_off": rho(k, "Off", "o_multiseason_rapm"),
            "rho_def": -rho(k, "Def", "d_multiseason_rapm"),
            "std_ratio": round(k["multiseason_rapm"].std() / k["Tot"].std(), 4),
        }
    report["rho_floor"] = RHO_FLOOR
    report["match_rate_floor"] = MATCH_RATE_FLOOR
    report["pass"] = report["poss_ge_10000"]["rho_total"] >= RHO_FLOOR and report["match_rate"] >= MATCH_RATE_FLOOR
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("oracle_csv", type=Path)
    ap.add_argument("--out", type=Path, help="write the report as JSON here")
    ap.add_argument("--cache", type=Path, help="parquet of the pooled possessions (read if present, else written)")
    args = ap.parse_args()

    if args.cache and args.cache.exists():
        poss = pl.read_parquet(args.cache)
    else:
        poss = regular_season_possessions()
        if args.cache:
            poss.write_parquet(args.cache)
    print(f"window: {poss.height:,} possessions, seasons {sorted(poss['season'].unique().to_list())}", flush=True)

    oracle = pl.read_csv(args.oracle_csv, schema_overrides={"ID": pl.Int64})
    report = {"possessions": poss.height, **gate(nba_multiseason_rapm(poss), oracle)}
    print(json.dumps(report, indent=2))
    if args.out:
        args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
