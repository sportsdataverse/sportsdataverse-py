"""Reproduce the opponent-adjusted EPA evaluation behind sdv-py #598.

Rebuilds every number in the PR: the lambda sweep on the train seasons, the
held-out gates E1-E4 / F1-F2, the garbage-time (fit band) change on its own,
and the out-of-sample check with a game-clustered bootstrap.

Inputs are the FBS-vs-FBS pass/rush plays exactly as the published team
summaries see them: the ``cfbfastR-cfb-data`` checkout's own
``cfb_data_build.summaries_input.prepare_plays_input`` over its released pbp
parquet plus ``load_cfb_schedule`` (network).

Postseason handling: a through-week snapshot keeps REGULAR-SEASON plays only
(``seasonType == 2`` and ``week <= W``) -- bowls restart at week 1 with
``seasonType == 3``. The season "final" is the full season including the
postseason, as published.

Methods compared:
  published  the pre-#598 method, rebuilt from the unchanged internals: old
             ``0.1 <= wp_before <= 0.9`` band + standardized reference-level
             ridge (``_fit_opponent_ridge``, lambda 0.035). Reproduces the
             published 2025 tables exactly.
  adjusted   ``cfb_adjusted_epa`` as shipped (naive-WP band, per-team penalty).
  raw        per-play EPA offense minus EPA allowed, no adjustment.

Usage::

    PYTHONPATH=<sdv-py checkout> python -m tools.validation.cfb_adjusted_epa_eval prep \\
        --cfb-data /mnt/sdv_repos/cfbfastR-cfb-data --data <dir> 2019 2021 2022 2023 2024 2025
    PYTHONPATH=<sdv-py checkout> python -m tools.validation.cfb_adjusted_epa_eval sweep --data <dir>
    PYTHONPATH=<sdv-py checkout> python -m tools.validation.cfb_adjusted_epa_eval eval --data <dir> --lam 0.075

Train seasons 2019/2021/2022 (2020 skipped: COVID schedule); eval 2023-2025.
Caveat: 2023-2025 were looked at more than once while the method was settled
(before the band change, for the retune, for the lighter-lambda table), so they
are not a clean holdout any more; the 2026 full season is.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.cfb import cfb_adjusted_epa
from sportsdataverse.cfb.cfb_adjusted_epa import (
    _ADJ_REQUIRED,
    _FIT_WP,
    _RIDGE_LAMBDA,
    _REQUIRED_COLUMNS,
    _TEAM_SEASON_PLAYS,
    _adjust_games,
    _fit_opponent_ridge,
    _fit_team_strengths,
    _prepare,
)

TRAIN, EVAL = (2019, 2021, 2022), (2023, 2024, 2025)
GRID = [round(0.005 * i, 3) for i in range(1, 16)] + [0.1, 0.15, 0.2, 0.3]
OLD_TEAM_SEASON_PLAYS = (
    440.0  # mean of the old-band per-season medians (439.8), as _TEAM_SEASON_PLAYS is for the new band
)
KEEP = [
    "game_id",
    "pos_team",
    "pos_team_id",
    "def_pos_team_id",
    "home",
    "neutral_site",
    "EPA",
    "pass",
    "rush",
    "wp_before",
    "wp_before_naive",
    "week",
    "seasonType",
]


def prep(cfb_data: Path, out: Path, seasons: list[int]) -> None:
    sys.path.insert(0, str(cfb_data / "python"))
    from cfb_data_build.summaries_input import prepare_plays_input

    from sportsdataverse.cfb import load_cfb_schedule

    out.mkdir(parents=True, exist_ok=True)
    for yr in seasons:
        pbp = pl.read_parquet(cfb_data / f"cfb/pbp/parquet/play_by_play_{yr}.parquet")
        df = prepare_plays_input(pbp, load_cfb_schedule(seasons=[yr]), yr)
        df.select([c for c in KEEP if c in df.columns]).write_parquet(out / f"plays_{yr}.parquet")
        print(yr, df.height, "plays", df["game_id"].n_unique(), "games", flush=True)


def _team_net(base: pl.DataFrame, offense: pl.DataFrame, defense: pl.DataFrame) -> pl.DataFrame:
    """The season aggregation of ``cfb_adjusted_epa``: mean over valid games, >= 2 of them."""
    opp = _adjust_games(base, offense, defense, fill_strength=None)
    return (
        opp.group_by("pos_team_id")
        .agg(
            valid_games=(pl.col("adj_off_epa").is_not_null() & pl.col("adj_def_epa").is_not_null()).sum(),
            net=(pl.col("adj_off_epa") - pl.col("adj_def_epa")).mean(),
        )
        .filter(pl.col("net").is_not_null() & (pl.col("valid_games") >= 2))
        .rename({"pos_team_id": "team_id"})
    )


def published(plays: pl.DataFrame) -> pl.DataFrame:
    base, clean = _prepare(plays, _REQUIRED_COLUMNS)  # old wp_before 10-90% band
    offense, defense, _ = _fit_opponent_ridge(clean, _RIDGE_LAMBDA)
    return _team_net(base, offense, defense)


def adjusted(plays: pl.DataFrame, lam: float) -> pl.DataFrame:
    return cfb_adjusted_epa(plays, ridge_lambda=lam).select(
        "team_id", pl.col("net_adj_epa").alias("net"), "valid_games"
    )


def shrink_only(plays: pl.DataFrame, lam: float) -> pl.DataFrame:
    """New per-team penalty on the OLD band (the shrinkage change without the band change)."""
    base, clean = _prepare(plays, _REQUIRED_COLUMNS)
    offense, defense, _ = _fit_team_strengths(clean, lam * OLD_TEAM_SEASON_PLAYS / _TEAM_SEASON_PLAYS)
    return _team_net(base, offense, defense)


def raw(plays: pl.DataFrame) -> pl.DataFrame:
    base, _ = _prepare(plays, _ADJ_REQUIRED, _FIT_WP)
    off = base.group_by("pos_team_id").agg(o=pl.col("EPA").mean()).rename({"pos_team_id": "team_id"})
    dfn = base.group_by("def_pos_team_id").agg(d=pl.col("EPA").mean()).rename({"def_pos_team_id": "team_id"})
    return off.join(dfn, on="team_id").select("team_id", net=pl.col("o") - pl.col("d"))


def spearman(a: pl.DataFrame, b: pl.DataFrame) -> float:
    j = a.select("team_id", x="net").join(b.select("team_id", y="net"), on="team_id")
    return float(j.select(pl.corr("x", "y", method="spearman")).item())


def through_week(plays: pl.DataFrame, week: int) -> pl.DataFrame:
    return plays.filter((pl.col("seasonType") == 2) & (pl.col("week") <= week))


def regular_weeks(plays: pl.DataFrame) -> list[int]:
    return sorted(plays.filter(pl.col("seasonType") == 2)["week"].unique().to_list())


def fit_band_medians(data: Path, seasons: tuple[int, ...]) -> list[float]:
    out = []
    for yr in seasons:
        _, clean = _prepare(pl.read_parquet(data / f"plays_{yr}.parquet"), _ADJ_REQUIRED, _FIT_WP)
        out.append(float(clean.group_by("pos_team_id").len()["len"].median()))
    return out


def sweep(data: Path) -> None:
    meds = fit_band_medians(data, TRAIN)
    print(
        f"fit-band median offensive plays per team {meds}; mean {np.mean(meds):.1f} (_TEAM_SEASON_PLAYS = {_TEAM_SEASON_PLAYS})"
    )
    rows = []
    for yr in TRAIN:
        plays = pl.read_parquet(data / f"plays_{yr}.parquet")
        finals = {lam: adjusted(plays, lam) for lam in GRID}
        for w in regular_weeks(plays):
            snap = through_week(plays, w)
            rw = raw(snap)
            for lam in GRID:
                s = adjusted(snap, lam)
                if s.height >= 10:
                    rows.append(
                        dict(season=yr, week=w, lam=lam, margin=spearman(s, finals[lam]) - spearman(rw, finals[lam]))
                    )
        print(yr, flush=True)
    t = (
        pl.DataFrame(rows)
        .filter(pl.col("week") >= 4)
        .group_by("lam")
        .agg(
            worst_margin_wk4_plus=pl.col("margin").min(),
            seasons_failing=pl.col("season").filter(pl.col("margin") < 0).n_unique(),
        )
        .sort("lam")
    )
    print(t)
    passing = t.filter(pl.col("seasons_failing") == 0)["lam"]
    print("smallest lambda meeting the week-4 target in every train season:", passing.min() if passing.len() else None)


def game_margins(plays: pl.DataFrame, after_week: int) -> pl.DataFrame:
    """One row per (game, offense): realized net EPA/play margin, for games after ``after_week`` (bowls included)."""
    base, _ = _prepare(plays, _ADJ_REQUIRED, _FIT_WP)
    later = base.filter(((pl.col("seasonType") == 2) & (pl.col("week") > after_week)) | (pl.col("seasonType") == 3))
    g = later.group_by("game_id", "pos_team_id", "def_pos_team_id").agg(e=pl.col("EPA").mean())
    other = g.select("game_id", pl.col("pos_team_id").alias("def_pos_team_id"), pl.col("e").alias("e_opp"))
    return g.join(other, on=["game_id", "def_pos_team_id"]).select(
        "game_id", "pos_team_id", "def_pos_team_id", margin=pl.col("e") - pl.col("e_opp")
    )


def _pred(margins: pl.DataFrame, ratings: pl.DataFrame, name: str) -> pl.DataFrame:
    r = ratings.select("team_id", "net")
    return (
        margins.join(r, left_on="pos_team_id", right_on="team_id")
        .join(r, left_on="def_pos_team_id", right_on="team_id", suffix="_opp")
        .select("game_id", "pos_team_id", "margin", pl.lit(name).alias("m"), pred=pl.col("net") - pl.col("net_opp"))
    )


def oos_paired(
    plays: pl.DataFrame, a: pl.DataFrame, b: pl.DataFrame, week: int, n_boot: int = 1000, seed: int = 598
) -> dict:
    """corr(rating diff, realized margin) for ratings a and b on the SAME games, and a game-clustered bootstrap of a - b."""
    m = game_margins(plays, week)
    pa, pb = _pred(m, a, "a"), _pred(m, b, "b")
    j = pa.join(pb.select("game_id", "pos_team_id", pred_b="pred"), on=["game_id", "pos_team_id"])
    games = j["game_id"].unique().to_list()
    gidx = {g: i for i, g in enumerate(games)}
    gid = np.array([gidx[g] for g in j["game_id"].to_list()])
    y, xa, xb = j["margin"].to_numpy(), j["pred"].to_numpy(), j["pred_b"].to_numpy()
    rng = np.random.default_rng(seed)
    rows_by_game = [np.flatnonzero(gid == i) for i in range(len(games))]
    diffs = []
    for _ in range(n_boot):
        pick = np.concatenate([rows_by_game[i] for i in rng.integers(0, len(games), len(games))])
        diffs.append(np.corrcoef(xa[pick], y[pick])[0, 1] - np.corrcoef(xb[pick], y[pick])[0, 1])
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    ca, cb = np.corrcoef(xa, y)[0, 1], np.corrcoef(xb, y)[0, 1]
    return dict(week=week, games=len(games), corr_a=ca, corr_b=cb, diff=ca - cb, ci_lo=lo, ci_hi=hi)


def evaluate(data: Path, lam: float) -> None:
    gates, oos = [], []
    for yr in EVAL:
        plays = pl.read_parquet(data / f"plays_{yr}.parquet")
        pub, fin = published(plays), adjusted(plays, lam)
        shr = shrink_only(plays, lam)
        j = pub.join(shr, on="team_id", suffix="_s")
        d = (j["net_s"] - j["net"]).to_numpy()
        old_band = shr.with_columns(rk=pl.col("net").rank(descending=True)).join(
            fin.with_columns(rk=pl.col("net").rank(descending=True)), on="team_id", suffix="_new"
        )
        rec = dict(
            season=yr,
            lam=lam,
            F1_shrink_only=spearman(pub, shr),
            F2_shrink_only=float(np.abs(d - d.mean()).max()),
            band_change_spearman=spearman(shr, fin),
            band_change_max_abs=float((old_band["net_new"] - old_band["net"]).abs().max()),
            band_change_max_rank_move=float((old_band["rk_new"] - old_band["rk"]).abs().max()),
            full_vs_published=spearman(pub, fin),
        )
        e1_a, e1_b, e1_r, e2, e3, churn_a, churn_b = [], [], [], {}, [], [], []
        prev_a = prev_b = None
        for w in [w for w in regular_weeks(plays) if w <= 10]:
            snap = through_week(plays, w)
            a, b, r = adjusted(snap, lam), published(snap), raw(snap)
            if a.height < 10 or b.height < 10:
                continue
            if 2 <= w <= 5:
                e1_a.append(spearman(a, pub))
                e1_b.append(spearman(b, pub))
                e1_r.append(spearman(r, pub))
            e2[w] = spearman(a, fin) - spearman(r, fin)
            if w >= 3:
                e3.append(float(a["net"].abs().max()))
            ra, rb = (x.select("team_id", rk=pl.col("net").rank(descending=True)) for x in (a, b))
            if 3 <= w <= 6 and prev_a is not None:
                churn_a.append(
                    float(
                        (ra.join(prev_a, on="team_id")["rk"] - ra.join(prev_a, on="team_id")["rk_right"]).abs().mean()
                    )
                )
                churn_b.append(
                    float(
                        (rb.join(prev_b, on="team_id")["rk"] - rb.join(prev_b, on="team_id")["rk_right"]).abs().mean()
                    )
                )
            prev_a, prev_b = ra, rb
            if 3 <= w <= 8:
                oos.append(dict(season=yr, vs="raw", **oos_paired(plays, a, r, w)))
                oos.append(dict(season=yr, vs="published", **oos_paired(plays, a, b, w)))
        rec.update(
            E1_adjusted=float(np.mean(e1_a)),
            E1_published=float(np.mean(e1_b)),
            E1_raw=float(np.mean(e1_r)),
            E2_week4=e2.get(4),
            E2_worst_week4_plus=min(v for k, v in e2.items() if k >= 4),
            E3_max_abs_net=max(e3),
            E4_churn_adjusted=float(np.mean(churn_a)),
            E4_churn_published=float(np.mean(churn_b)),
        )
        gates.append(rec)
        print(yr, flush=True)
    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_width_chars(250)
    pl.Config.set_float_precision(4)
    g, o = pl.DataFrame(gates), pl.DataFrame(oos)
    print(g)
    print(o)
    (data / f"gates_{lam}.json").write_text(json.dumps(dict(gates=gates, oos=oos), indent=1))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cmd", choices=["prep", "sweep", "eval"])
    ap.add_argument("seasons", nargs="*", type=int)
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--cfb-data", type=Path, default=Path("/mnt/sdv_repos/cfbfastR-cfb-data"))
    ap.add_argument("--lam", type=float, default=0.075)
    args = ap.parse_args()
    if args.cmd == "prep":
        prep(args.cfb_data, args.data, args.seasons or [*TRAIN, *EVAL])
    elif args.cmd == "sweep":
        sweep(args.data)
    else:
        evaluate(args.data, args.lam)


if __name__ == "__main__":
    main()
