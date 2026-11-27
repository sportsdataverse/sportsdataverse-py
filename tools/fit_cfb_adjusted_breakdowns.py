"""Fit the publication tiers and shrinkage constants of ``cfb_adjusted_breakdowns``.

Pre-registered rule (2014-2023 train, 2024-2025 holdout reported only):

* split-half by the target team's game parity: games ordered by (season type, week, game id); the offense side keeps
  the plays whose offense is in its odd (even) game, the defense side the defense's; each half is refit;
  ``r`` = Pearson across teams in both halves; ``SB = 2r / (1 + r)``;
* ``k`` per metric, side and value (raw / adj): per season ``c`` = covariance of the halves, ``v`` = mean of their
  variances, ``h`` = harmonic mean of the half play counts, noise per play ``e = (v - c) h``; pooled
  ``k = sum(e) / sum(c)`` over the train seasons (no signal when ``sum(c) <= 0``);
* tier A (adjusted): adjusted SB >= 0.50 every train season and its mean >= raw's mean + 0.03; tier B (raw with
  shrinkage): else raw mean SB >= 0.40; tier C: not published. Metrics marked non-adjustable cannot be tier A.

Input: the cfbfastR-cfb-data checkout's released pbp + schedule parquets through its own
``cfb_data_build.summaries_input.prepare_plays_input`` (the frame the team summaries are built from)::

    uv run python tools/fit_cfb_adjusted_breakdowns.py --data-repo ../cfbfastR-dev/cfbfastR-cfb-data --out build/bd

Writes ``split_half_by_season.csv``, ``tiers.csv`` and ``meta.json`` (both checkouts' commits) and prints the ``_PUBLISHED`` literal.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.cfb.cfb_adjusted_breakdowns import _METRICS, _breakdown_values, _kept_plays

TRAIN = tuple(range(2014, 2024))
HOLDOUT = (2024, 2025)
SB_ADJ_MIN, SB_GAIN_MIN, SB_RAW_MIN = 0.50, 0.03, 0.40


def _head(repo: Path) -> str:
    """The checkout's commit (with ``+dirty`` when the tree has changes): recorded next to the outputs."""
    sha = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(repo), "status", "--porcelain"], capture_output=True, text=True).stdout
    return sha + ("+dirty" if dirty.strip() else "")


def load_season(data_repo: Path, season: int) -> pl.DataFrame:
    """Released pbp + schedule -> the team-summaries input frame, via the producer's own function."""
    sys.path.insert(0, str(data_repo / "python"))
    from cfb_data_build.summaries_input import prepare_plays_input

    pbp = pl.read_parquet(data_repo / f"cfb/pbp/parquet/play_by_play_{season}.parquet")
    sched = pl.read_parquet(data_repo / f"cfb/cfb_schedules/parquet/cfb_schedules_{season}.parquet")
    return prepare_plays_input(pbp, sched, season)


def parity(clean: pl.DataFrame, key: str) -> pl.DataFrame:
    """True on the plays where ``key``'s team is in an odd-numbered game of its season."""
    games = (
        clean.select(key, "game_id", "seasonType", "week")
        .unique()
        .sort("seasonType", "week", "game_id")
        .with_columns(k=pl.int_range(pl.len()).over(key))
        .select(key, "game_id", odd=pl.col("k") % 2 == 0)
    )
    return games


def season_rows(clean: pl.DataFrame, season: int) -> list[dict]:
    names = list(_METRICS)
    rows = []
    for side, key in (("off", "pos_team_id"), ("def", "def_pos_team_id")):
        flagged = clean.join(parity(clean, key), on=[key, "game_id"], how="left")
        halves = {
            odd: _breakdown_values(flagged.filter(pl.col("odd") == odd).drop("odd"), names).filter(
                pl.col("side") == side
            )
            for odd in (True, False)
        }
        both = halves[True].join(halves[False], on=["metric", "team_id"], suffix="_2")
        for metric in names:
            b = both.filter(pl.col("metric") == metric)
            assert b.height >= 10, f"{season} {metric} {side}: {b.height} teams in both halves"
            for value in ("raw", "adj"):
                x, y = b[value].to_numpy(), b[f"{value}_2"].to_numpy()
                n = np.concatenate([b["n"].to_numpy(), b["n_2"].to_numpy()]).astype(float)
                r = float(np.corrcoef(x, y)[0, 1])
                c = float(np.cov(x, y)[0, 1])
                v = float((np.var(x, ddof=1) + np.var(y, ddof=1)) / 2)
                h = float(len(n) / np.sum(1.0 / n))
                rows.append(
                    {
                        "season": season,
                        "metric": metric,
                        "side": side,
                        "value": value,
                        "teams": b.height,
                        "r": r,
                        "sb": 2 * r / (1 + r),
                        "cov": c,
                        "var": v,
                        "h": h,
                        "noise": (v - c) * h,
                    }
                )
    return rows


def tiers(sh: pl.DataFrame) -> pl.DataFrame:
    train = sh.filter(pl.col("season").is_in(TRAIN))
    agg = train.group_by("metric", "side", "value").agg(
        sb_mean=pl.col("sb").mean(),
        sb_min=pl.col("sb").min(),
        k=pl.col("noise").sum() / pl.col("cov").sum(),
        signal=pl.col("cov").sum(),
        seasons=pl.len(),
    )
    hold = (
        sh.filter(pl.col("season").is_in(HOLDOUT))
        .group_by("metric", "side", "value")
        .agg(sb_holdout=pl.col("sb").mean())
    )
    wide = agg.join(hold, on=["metric", "side", "value"], how="left").pivot(
        on="value", index=["metric", "side"], values=["sb_mean", "sb_min", "k", "signal", "seasons", "sb_holdout"]
    )
    adjustable = pl.col("metric").replace_strict({m: v[2] for m, v in _METRICS.items()}, return_dtype=pl.Boolean)
    tier_a = (
        adjustable
        & (pl.col("seasons_adj") == len(TRAIN))
        & (pl.col("sb_min_adj") >= SB_ADJ_MIN)
        & (pl.col("sb_mean_adj") - pl.col("sb_mean_raw") >= SB_GAIN_MIN)
        & (pl.col("signal_adj") > 0)
    )
    tier_b = (pl.col("sb_mean_raw") >= SB_RAW_MIN) & (pl.col("signal_raw") > 0)
    order = {m: i for i, m in enumerate(_METRICS)}
    return (
        wide.with_columns(
            tier=pl.when(tier_a).then(pl.lit("adj")).when(tier_b).then(pl.lit("raw")).otherwise(pl.lit("none"))
        )
        .with_columns(k=pl.when(pl.col("tier") == "adj").then(pl.col("k_adj")).otherwise(pl.col("k_raw")))
        .with_columns(_o=pl.col("metric").replace_strict(order, return_dtype=pl.Int64))
        .sort("_o", "side", descending=[False, True])
        .drop("_o")
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-repo", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seasons", type=int, nargs="*", default=[*TRAIN, *HOLDOUT])
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    path, meta_path = a.out / "split_half_by_season.csv", a.out / "meta.json"
    meta = {"sdv_py": _head(Path(__file__).parent.parent), "data_repo": _head(a.data_repo)}
    # Resume only from a table written by the same two clean commits; anything else is recomputed from scratch.
    dirty = any(v.endswith("+dirty") for v in meta.values())
    same = meta_path.exists() and json.loads(meta_path.read_text()) == meta
    if path.exists() and (dirty or not same):
        path.unlink()
    done = pl.read_csv(path) if path.exists() else None
    meta_path.write_text(json.dumps(meta, indent=1))
    for season in a.seasons:
        if done is not None and season in set(done["season"].to_list()):
            continue
        rows = season_rows(_kept_plays(load_season(a.data_repo, season)), season)
        new = pl.DataFrame(rows)
        done = new if done is None else pl.concat([done, new], how="vertical_relaxed")
        done.write_csv(path)
        print(season, "done", flush=True)
    assert done is not None
    t = tiers(done)
    t.write_csv(a.out / "tiers.csv")
    with pl.Config(tbl_rows=100, tbl_cols=20, tbl_width_chars=250, float_precision=3):
        print(
            t.select(
                "metric",
                "side",
                "tier",
                "k",
                "sb_mean_raw",
                "sb_mean_adj",
                "sb_min_adj",
                "sb_holdout_raw",
                "sb_holdout_adj",
            )
        )
    print("_PUBLISHED = {")
    for r in t.filter(pl.col("tier") != "none").iter_rows(named=True):
        print(f'    ("{r["metric"]}", "{r["side"]}"): ("{r["tier"]}", {r["k"]:.1f}),')
    print("}")


if __name__ == "__main__":
    main()
