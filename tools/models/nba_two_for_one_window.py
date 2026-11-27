"""BB-D44's 2-for-1 floor and window, from the published NBA possessions, 2015-16 to 2025-26.

Run (a hoopR-nba-stats-data checkout: its committed ``nba_stats/{possessions,pbp}/parquet``)::

    uv run python tools/models/nba_two_for_one_window.py <hoopR-nba-stats-data>/nba_stats

Regular-season possessions (``game_id`` ``002``), outside garbage time, and in period 4 and
overtime only within 10 points (``two_for_one_summary``'s filter), are flagged with
:func:`~sportsdataverse.nba.nba_possession_flags.add_two_for_one_flags` over a wide window so
every possession near the end of a period carries its outcome. Two curves, in 1-second bins,
written beside this file as ``nba_two_for_one_window.csv`` (per season and pooled, ``season``
null):

* ``extra_by_first_shot``: ``P(extra_possession)`` against ``first_shot_clock``. The opponent can
  hold for the last shot only when it gets the ball with 24 or fewer seconds left, so the rate
  rises a few seconds above 24: from about 0.48 (16-21 s) to about 0.93 (30-40 s), a ramp over
  22-33 s rather than a one-bin jump. ``GO_FLOOR`` is the candidate (24 to 28) where a step
  function best fits the curve: the count-weighted least-squares change point over 16-39 s.
  (The single largest bin-to-bin jump was tried first and is unstable: 24 to 28 by season.)
* ``go_by_change``: the go rate (``first_shot_clock >= GO_FLOOR``) against ``change_clock``, beside
  the ``natural`` rate a mid-period possession would reach with the same time (the share of
  possessions changing hands with 2-10 minutes left whose first shot comes within
  ``change_clock - GO_FLOOR`` seconds) and ``excess`` = go minus natural, the hurry. Bins here are
  ``(b - 1, b]``, the window's own shape. The window is the run of bins around the peak whose
  pooled ``excess`` is at least ``EXCESS_MIN`` (5 points): ``(first bin - 1, last bin]``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_possession_flags import TWO_FOR_ONE_SCHEMA, add_two_for_one_flags

SEASONS = range(2016, 2027)
FLOORS = (24.0, 25.0, 26.0, 27.0, 28.0)
WIDE = (10.0, 90.0)
STEP_RANGE = (16.0, 40.0)
EXCESS_MIN = 0.05
MID = (120.0, 600.0)


def season_frame(data: Path, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The season's window possessions (wide) and its mid-period seconds-to-first-shot sample."""
    poss = pl.read_parquet(data / "possessions" / "parquet" / f"nba_possessions_{season}.parquet")
    pbp = pl.read_parquet(data / "pbp" / "parquet" / f"nba_play_by_play_{season}.parquet")
    f = add_two_for_one_flags(poss.drop(*TWO_FOR_ONE_SCHEMA, strict=False), pbp, window=WIDE).filter(
        (pl.col("game_id").str.slice(2, 1) == "2") & (pl.col("is_garbage_time") == False)  # noqa: E712
    )
    keep = (pl.col("period") < 4) | (pl.col("margin_at_change").abs() <= 10)
    w = f.filter(pl.col("two_for_one").is_not_null() & keep).select(
        pl.lit(season).alias("season"), "change_clock", "first_shot_clock", "end_seconds_remaining", "extra_possession"
    )
    mid = f.filter(pl.col("change_clock").is_between(*MID) & (pl.col("period") < 4)).select(
        pl.lit(season).alias("season"), "seconds_to_first_shot"
    )
    return w, mid


def extra_curve(w: pl.DataFrame) -> pl.DataFrame:
    return (
        w.drop_nulls("first_shot_clock")
        .group_by("season", pl.col("first_shot_clock").floor().alias("bin"))
        .agg(pl.len().alias("n"), pl.col("extra_possession").mean().alias("rate"))
        .with_columns(pl.lit("extra_by_first_shot").alias("curve"))
    )


def go_curve(w: pl.DataFrame, mid: pl.DataFrame, floor: float) -> pl.DataFrame:
    rows = []
    for (season,), sw in w.group_by(["season"]):
        sts = (
            mid.filter(pl.col("season") == season)["seconds_to_first_shot"]
            if season is not None
            else mid["seconds_to_first_shot"]
        )
        shots = np.sort(sts.drop_nulls().to_numpy())
        cdf = sw.with_columns(
            pl.Series(
                "natural", np.searchsorted(shots, (sw["change_clock"] - floor).to_numpy(), side="right") / sts.len()
            )
        )
        rows.append(cdf)
    return (
        pl.concat(rows)
        .group_by("season", pl.col("change_clock").ceil().alias("bin"))
        .agg(
            pl.len().alias("n"),
            (pl.col("first_shot_clock") >= floor).fill_null(False).mean().alias("rate"),
            pl.col("natural").mean(),
            ((pl.col("first_shot_clock") < floor).fill_null(True) & (pl.col("end_seconds_remaining") <= 24))
            .mean()
            .alias("held_rate"),
        )
        .with_columns((pl.col("rate") - pl.col("natural")).alias("excess"), pl.lit("go_by_change").alias("curve"))
    )


def pick_floor(extra: pl.DataFrame) -> float:
    """The candidate floor whose step function (one level below, one at and above) fits best."""
    pooled = extra.filter(pl.col("season").is_null() & pl.col("bin").is_between(*STEP_RANGE, closed="left"))
    b, y, n = (pooled[c].to_numpy() for c in ("bin", "rate", "n"))
    sse = {}
    for f in FLOORS:
        lo = b < f
        fit = np.where(lo, np.average(y[lo], weights=n[lo]), np.average(y[~lo], weights=n[~lo]))
        sse[f] = float(np.sum(n * (y - fit) ** 2))
    return min(sse, key=sse.get)


def pick_window(go: pl.DataFrame) -> tuple[float, float]:
    pooled = go.filter(pl.col("season").is_null()).sort("bin")
    peak = pooled["bin"][pooled["excess"].arg_max()]
    hit = pooled["excess"] >= EXCESS_MIN
    bins = pooled["bin"].to_list()
    i = bins.index(peak)
    lo_i, hi_i = i, i
    while lo_i > 0 and hit[lo_i - 1]:
        lo_i -= 1
    while hi_i < len(bins) - 1 and hit[hi_i + 1]:
        hi_i += 1
    # bin b holds change clocks in (b - 1, b], so the run is the window (lo, hi]
    return float(bins[lo_i] - 1), float(bins[hi_i])


if __name__ == "__main__":
    data = Path(sys.argv[1])
    frames = [season_frame(data, s) for s in SEASONS]
    w = pl.concat(f[0] for f in frames)
    mid = pl.concat(f[1] for f in frames)
    w_all = pl.concat([w, w.with_columns(pl.lit(None, dtype=pl.Int32).alias("season"))])
    extra = extra_curve(w_all)
    floor = pick_floor(extra)
    go = go_curve(
        w_all.with_columns(pl.col("season").cast(pl.Int64)), mid.with_columns(pl.col("season").cast(pl.Int64)), floor
    )
    lo, hi = pick_window(go)
    cols = ["curve", "season", "bin", "n", "rate", "natural", "excess", "held_rate"]
    res = pl.concat([extra, go], how="diagonal_relaxed").select(cols).sort("curve", "season", "bin", nulls_last=True)
    res.write_csv(Path(__file__).with_suffix(".csv"), float_precision=6)
    with pl.Config(tbl_rows=200, tbl_cols=20):
        pooled = res.filter(pl.col("season").is_null())
        print(pooled.filter((pl.col("curve") == "extra_by_first_shot") & pl.col("bin").is_between(16, 40)))
        print(pooled.filter((pl.col("curve") == "go_by_change") & pl.col("bin").is_between(20, 70)))
        steps = (
            res.filter((pl.col("curve") == "extra_by_first_shot") & pl.col("season").is_not_null())
            .filter(pl.col("bin").is_between(22, 28))
            .pivot("bin", index="season", values="rate", sort_columns=True)
        )
        print(steps)
    print(f"GO_FLOOR {floor}; WINDOW ({lo}, {hi}]")
