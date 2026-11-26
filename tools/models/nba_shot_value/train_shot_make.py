"""Fit, gate and bundle the NBA and WNBA shot-make boosters (the location model of ``score_shot_xpoints``).

Run (the ``{nba,wnba}_stats_pbp`` releases; ``--data-root`` reads a producer checkout's parquet instead,
the same files)::

    uv run python tools/models/nba_shot_value/train_shot_make.py --league-id 00
    uv run python tools/models/nba_shot_value/train_shot_make.py --league-id 10

**The model.** P(make) of a regular-season field-goal attempt from the play-by-play alone:
the distance from the hoop (from the location), ``x_legacy``, ``y_legacy``, ``shot_value`` and one indicator per ``sub_type`` (the
shot type, e.g. ``"Driving Layup Shot"``) seen at least :data:`MIN_LEVEL_N` times in the training
seasons (``nba_shot_value._shot_make_design``, the matrix the package scores with). No player, team or
game-state input. The shot chart carries the same fields (``loc_x`` / ``loc_y`` / ``action_type``), so
the one booster scores both.

**The comparator** is the model it replaces: the make rate of the shot's zone triple
(``shot_zone_basic x shot_zone_area x shot_zone_range``, the ``LeagueAverages`` cells). The release has
no zone columns, so :func:`zone_triple` rebuilds them from the location (99.75% exact on the committed
2022-23 shot-chart fixture, ``tests/tools/test_train_shot_make.py``). Two versions are scored:

* ``zone_served``: the scored season's own cell rates, as ``score_shot_xpoints`` serves them (the
  ``LeagueAverages`` of the season the shots are in, which include the shots themselves). This is
  the comparator the gate uses, and it is favourable to the zone table.
* ``zone_prior``: the training seasons' cell rates, the out-of-sample version.

**Seasons** (named by the year they end). Walk-forward: every season in ``walk`` is scored by a booster
fitted on the :data:`TRAIN_SEASONS` seasons before it, never on itself. The shipped booster is the
walk-forward fit of the ``holdout`` season (trained on the three before it); ``monitor`` seasons (the
one in progress) are scored by that booster and reported, not gated. Inside a fit, the boosting rounds
are picked by early stopping on the last training season, then the booster is refit on all three.

**Gates**, fixed before the first run and applied to every walk-forward season (the holdout included):

1. skill: the per-shot log-loss difference ``zone_served - model`` has a 95% game-clustered bootstrap
   interval wholly above 0;
2. calibration in the large: ``|mean(p) - mean(y)|`` at most :data:`MAX_LARGE_BIAS`;
3. calibration slope (logistic, ``y`` on ``logit(p)``) inside :data:`SLOPE_BAND`.

**Threes keep the zone value.** The booster gains nothing on threes (and was below the zone table on
2019 and the 2027 monitor season), so ``score_shot_xpoints`` scores a three with its zone value (owner
ruling 2026-11-25), and the gates score the model the same way: threes on ``zone_served``. The booster's
own scores, threes included, are reported as ``model_raw`` / ``raw_vs_zone_served``.

**Outcome (2026-11-25, as served).** NBA (walk 2019-2026, shipped booster trained 2023-2025): the log-loss
gain over the served zone table is 3.0% to 4.0% every season, every interval wholly above 0, every slope in
the band, and every gate passes on the 2026 holdout (3.0%; rim 9.6%, two-pointers from 4 ft 1.2%) and on
the 2027 monitor season so far. Gate 2 misses 2019 (+0.0109), 2021 (-0.0154) and 2023 (-0.0112): the
owner accepted them (:data:`OWNER_ACCEPTED`). WNBA (walk 2016-2026): fails 2016-2020 (the feed recoded its
shot types around 2018, and 2020 is the bubble) and gate 2 in 2022; 2021-2026 gain 1.2% to 1.5%. No WNBA
booster ships; its report is kept as the record. A season-level shift from the zone table's totals was
tried after the first run and dropped: it made the bias larger in 6 of the 13 seasons it scored (both
NBA seasons among them) and turned no failing season into a pass.

**Never loosen a gate to make a refit pass**: argue the change in its PR. An accepted failure is listed
in :data:`OWNER_ACCEPTED` with its exact value, so any other failure, or an accepted one moving, still
stops the run.

Hyperparameters are fixed a priori (no search). Outputs (beside this script): ``shot_make_<lg>_holdout.json``
(the report: every season's metrics, gates and the shipped fit), ``shot_make_<lg>_importance.json``
(mean |SHAP| on the holdout), ``shot_make_<lg>_partition.parquet`` (every game id and its split) and the
booster ``sportsdataverse/nba/models/<lg>_shot_make.ubj``; ``--fixture`` also cuts the offline test
fixture from the holdout season.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.nba.nba_shot_value import (
    SHOT_MAKE_MODELS,
    _shot_make_design,
    _shot_make_feature_names,
)

REPO = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
MODELS = REPO / "sportsdataverse" / "nba" / "models"
FIXTURE_DIR = REPO / "tests" / "fixtures" / "nba_shot_make"

LEAGUES: dict[str, dict[str, Any]] = {
    "00": {
        "lg": "nba",
        "artifact": "nba_shot_make.ubj",
        "walk": list(range(2019, 2027)),
        "holdout": 2026,
        "monitor": [2027],
        "fixture_games": 20,
    },
    # Gated and not shipped (2026-11-25): fails 2016-2020 and 2022 (see shot_make_wnba_holdout.json).
    "10": {
        "lg": "wnba",
        "artifact": "wnba_shot_make.ubj",
        "walk": list(range(2016, 2027)),
        "holdout": 2026,
        "monitor": [],
        "fixture_games": 20,
    },
}
TRAIN_SEASONS = 3
#: Owner-accepted gate failures (owner ruling of 2026-11-25, relayed by the coordinator), by league: the
#: exact failure lines of the run that saved the booster. The booster is saved only when every failure is
#: listed here, and the report lists them under ``owner_accepted``. They are not passes. NBA gate 2
#: (|bias| <= 0.01) as served (threes on the zone value): 2019 +0.0109, 2021 -0.0154, 2023 -0.0112. The
#: zone table fitted on the same training seasons misses 2021 and 2023 as well (-0.0120, -0.0134: the
#: league's make rate moved), but not 2019 (-0.0034).
OWNER_ACCEPTED: dict[str, tuple[str, ...]] = {
    "00": (
        "walk_forward 2019: gate 2 calibration in the large +0.0109",
        "walk_forward 2021: gate 2 calibration in the large -0.0154",
        "walk_forward 2023: gate 2 calibration in the large -0.0112",
    ),
}
MIN_LEVEL_N = 200
MAX_LARGE_BIAS = 0.01
SLOPE_BAND = (0.9, 1.1)
BOOT = 2000
_XGB_PARAMS: dict[str, Any] = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "max_depth": 4,
    "eta": 0.05,
    "min_child_weight": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "lambda": 1.0,
    "tree_method": "hist",
    "seed": 7,
    # pinned: the histogram sums depend on the thread count, so a fixed count makes a refit reproducible
    "nthread": 4,
}
_MAX_ROUNDS = 2000
_WORKERS = int(os.environ.get("SHOT_MAKE_WORKERS", "4"))
_EPS = 1e-6
_COLS = ["game_id", "action_number", "sub_type", "shot_value", "shot_distance", "x_legacy", "y_legacy", "shot_result"]


def load_season(league_id: str, season: int, data_root: Path | None) -> pl.DataFrame:
    """The season's regular-season field-goal attempts (``game_id`` Utf8, ``y`` 1 on a make)."""
    lg = LEAGUES[league_id]["lg"]
    if data_root is not None:
        d = data_root / f"{lg}_stats" / "pbp" / "parquet"
        path = d / f"{lg}_play_by_play_{season}.parquet"
        pbp = pl.scan_parquet(path if path.exists() else d / f"play_by_play_{season}.parquet")
    elif league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_pbp

        pbp = load_wnba_stats_pbp([season]).lazy()
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_stats_pbp

        pbp = load_nba_stats_pbp([season - 1]).lazy()
    out = (
        pbp.filter(pl.col("is_field_goal") == 1)
        .select(_COLS)
        .with_columns(
            pl.col("game_id").cast(pl.Utf8),
            pl.col("action_number").cast(pl.Int64),
            pl.col("shot_value").cast(pl.Int64),
            (pl.col("shot_result") == "Made").cast(pl.Int8).alias("y"),
            pl.lit(season, dtype=pl.Int64).alias("season"),
        )
        .filter(pl.col("game_id").str.slice(2, 1) == "2", pl.col("shot_value").is_in([2, 3]))
        .collect()
    )
    if out.is_empty():
        raise ValueError(f"{lg} {season}: no regular-season field goals")
    return out


def zone_triple(shots: pl.DataFrame, x: str = "x_legacy", y: str = "y_legacy") -> pl.DataFrame:
    """Rebuild the stats feed's ``shot_zone_basic`` / ``_area`` / ``_range`` from a shot's location and value."""
    X, Y = pl.col(x).cast(pl.Float64), pl.col(y).cast(pl.Float64)
    r = (X**2 + Y**2).sqrt()
    ang = pl.arctan2(X, Y).degrees().abs()
    three, corner, back = pl.col("shot_value") == 3, Y <= 87.5, Y >= 400
    side = pl.when(X < 0).then(pl.lit("Left Side(L)")).otherwise(pl.lit("Right Side(R)"))
    wing = pl.when(X < 0).then(pl.lit("Left Side Center(LC)")).otherwise(pl.lit("Right Side Center(RC)"))
    center = pl.lit("Center(C)")
    basic = (
        pl.when(Y > 422.5)
        .then(pl.lit("Backcourt"))
        .when(three & corner)
        .then(pl.when(X < 0).then(pl.lit("Left Corner 3")).otherwise(pl.lit("Right Corner 3")))
        .when(three)
        .then(pl.lit("Above the Break 3"))
        .when(r < 40)
        .then(pl.lit("Restricted Area"))
        .when((X.abs() <= 80) & (Y <= 138))
        .then(pl.lit("In The Paint (Non-RA)"))
        .otherwise(pl.lit("Mid-Range"))
    )
    area = (
        pl.when(back)
        .then(pl.lit("Back Court(BC)"))
        .when(three & corner)
        .then(side)
        .when(three)
        .then(pl.when(ang <= 18).then(center).when(ang <= 69).then(wing).otherwise(side))
        .when(r < 80)
        .then(center)
        .when(r <= 160)
        .then(pl.when(ang <= 30).then(center).otherwise(side))
        .otherwise(pl.when(ang <= 18).then(center).when(ang <= 54).then(wing).otherwise(side))
    )
    rng = (
        pl.when(back)
        .then(pl.lit("Back Court Shot"))
        .when(three)
        .then(pl.lit("24+ ft."))
        .when(r <= 80)
        .then(pl.lit("Less Than 8 ft."))
        .when(r <= 160)
        .then(pl.lit("8-16 ft."))
        .otherwise(pl.lit("16-24 ft."))
    )
    return shots.with_columns(basic.alias("zone_basic"), area.alias("zone_area"), rng.alias("zone_range"))


_ZONE = ["zone_basic", "zone_area", "zone_range"]


def zone_predict(table_from: pl.DataFrame, scored: pl.DataFrame) -> np.ndarray:
    """Each scored shot's cell make rate in ``table_from`` (unmatched: its range's rate, then the overall)."""
    cells = table_from.group_by(_ZONE).agg(pl.col("y").mean().alias("_p"))
    ranges = table_from.group_by("zone_range").agg(pl.col("y").mean().alias("_pr"))
    out = scored.join(cells, on=_ZONE, how="left").join(ranges, on="zone_range", how="left")
    return out["_p"].fill_null(out["_pr"]).fill_null(float(table_from["y"].mean())).to_numpy()


def levels(train: pl.DataFrame) -> list[str]:
    """The shot types with at least :data:`MIN_LEVEL_N` training attempts, most common first."""
    vc = train["sub_type"].drop_nulls().value_counts(sort=True)
    return vc.filter(pl.col("count") >= MIN_LEVEL_N)["sub_type"].to_list()


def fit(train: dict[int, pl.DataFrame]) -> xgb.Booster:
    """Early-stop on the last training season, then refit on all of them with that many rounds."""
    *fit_s, val_s = sorted(train)
    every = pl.concat(train.values())
    lv = levels(every)
    names = _shot_make_feature_names(lv)

    def dm(f: pl.DataFrame) -> xgb.DMatrix:
        return xgb.DMatrix(_shot_make_design(f, lv), label=f["y"].to_numpy(), feature_names=names)

    probe = xgb.train(
        _XGB_PARAMS,
        dm(pl.concat([train[s] for s in fit_s])),
        _MAX_ROUNDS,
        evals=[(dm(train[val_s]), "val")],
        early_stopping_rounds=50,
        verbose_eval=False,
    )
    rounds = int(probe.best_iteration) + 1
    booster = xgb.train(_XGB_PARAMS, dm(every), num_boost_round=rounds)
    booster.set_attr(
        shot_types=json.dumps(lv),
        rounds=str(rounds),
        validation_season=str(val_s),
        train_seasons=",".join(map(str, sorted(train))),
        train_make_rate=repr(float(every["y"].mean())),
        fitting_script="tools/models/nba_shot_value/train_shot_make.py",
    )
    return booster


def predict(booster: xgb.Booster, shots: pl.DataFrame) -> np.ndarray:
    return booster.inplace_predict(_shot_make_design(shots, json.loads(booster.attr("shot_types"))))


def _ll(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, _EPS, 1 - _EPS)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def calibration(y: np.ndarray, p: np.ndarray) -> tuple[float, float]:
    """(intercept with logit(p) as an offset, slope of y on logit(p)), both by Newton's method."""
    z = np.log(np.clip(p, _EPS, 1 - _EPS) / (1 - np.clip(p, _EPS, 1 - _EPS)))
    a = 0.0
    for _ in range(30):
        q = 1 / (1 + np.exp(-(z + a)))
        a += np.sum(y - q) / max(np.sum(q * (1 - q)), _EPS)
    b = np.zeros(2)
    X = np.column_stack([np.ones_like(z), z])
    for _ in range(30):
        q = 1 / (1 + np.exp(-(X @ b)))
        w = q * (1 - q)
        b += np.linalg.solve(X.T @ (X * w[:, None]), X.T @ (y - q))
    return float(a), float(b[1])


def ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    idx = np.minimum((p * bins).astype(int), bins - 1)
    return float(
        sum(abs(y[idx == k].mean() - p[idx == k].mean()) * (idx == k).sum() for k in range(bins) if (idx == k).any())
        / len(y)
    )


def paired(y: np.ndarray, base: np.ndarray, model: np.ndarray, games: np.ndarray, seed: int) -> dict[str, float]:
    """Mean per-shot log-loss and Brier gain of ``model`` over ``base``, with a 95% game-clustered bootstrap."""
    d_ll = _ll(y, base) - _ll(y, model)
    d_br = (y - base) ** 2 - (y - model) ** 2
    _, inv = np.unique(games, return_inverse=True)
    G = inv.max() + 1
    n_g = np.bincount(inv, minlength=G).astype(float)
    W = np.random.default_rng(seed).multinomial(G, np.full(G, 1 / G), size=BOOT).astype(float)
    out = {"games": int(G), "shots": len(y)}
    for k, d in (("log_loss", d_ll), ("brier", d_br)):
        b = (W @ np.bincount(inv, weights=d, minlength=G)) / (W @ n_g)
        lo, hi = np.percentile(b, [2.5, 97.5])
        out |= {f"d_{k}": float(d.mean()), f"d_{k}_lo": float(lo), f"d_{k}_hi": float(hi)}
    out["rel_log_loss"] = float(d_ll.mean() / _ll(y, base).mean())
    return out


def score(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    a, s = calibration(y, p)
    return {
        "log_loss": float(_ll(y, p).mean()),
        "brier": float(np.mean((y - p) ** 2)),
        "ece": ece(y, p),
        "large_bias": float(p.mean() - y.mean()),
        "calibration_intercept": a,
        "calibration_slope": s,
    }


def _band(shots: pl.DataFrame) -> np.ndarray:
    return (
        shots.select(
            pl.when((pl.col("x_legacy").cast(pl.Float64) ** 2 + pl.col("y_legacy").cast(pl.Float64) ** 2).sqrt() < 40)
            .then(pl.lit("rim 0-3 ft"))
            .when(pl.col("shot_value") == 3)
            .then(pl.lit("3PA"))
            .otherwise(pl.lit("2PA 4+ ft"))
        )
        .to_series()
        .to_numpy()
    )


def season_metrics(test: pl.DataFrame, train: pl.DataFrame, booster: xgb.Booster, seed: int) -> dict[str, Any]:
    y = test["y"].to_numpy().astype(float)
    g = test["game_id"].to_numpy()
    raw, served = predict(booster, test), zone_predict(test, test)
    three = test["shot_value"].to_numpy() == 3
    p = {
        # as score_shot_xpoints serves it: threes take the zone value (owner ruling 2026-11-25)
        "model": np.where(three, served, raw),
        "model_raw": raw,
        "zone_served": served,
        "zone_prior": zone_predict(train, test),
    }
    band = _band(test)
    return {
        "shots": len(y),
        "games": int(test["game_id"].n_unique()),
        "make_rate": float(y.mean()),
        "rounds": int(booster.attr("rounds")),
        **{f"{k}": score(y, v) for k, v in p.items()},
        "vs_zone_served": paired(y, p["zone_served"], p["model"], g, seed),
        "vs_zone_prior": paired(y, p["zone_prior"], p["model"], g, seed + 1),
        "raw_vs_zone_served": paired(y, p["zone_served"], p["model_raw"], g, seed + 3),
        "vs_zone_served_by_band": {
            b: paired(y[band == b], p["zone_served"][band == b], p["model"][band == b], g[band == b], seed + 2)
            for b in ("rim 0-3 ft", "2PA 4+ ft", "3PA")
        },
    }


def gate(label: str, m: dict[str, Any]) -> list[str]:
    out = []
    if m["vs_zone_served"]["d_log_loss_lo"] <= 0:
        out.append(
            f"{label}: gate 1 skill, log-loss gain over zone_served CI lower {m['vs_zone_served']['d_log_loss_lo']:+.5f} <= 0"
        )
    if abs(m["model"]["large_bias"]) > MAX_LARGE_BIAS:
        out.append(f"{label}: gate 2 calibration in the large {m['model']['large_bias']:+.4f}")
    if not SLOPE_BAND[0] <= m["model"]["calibration_slope"] <= SLOPE_BAND[1]:
        out.append(f"{label}: gate 3 calibration slope {m['model']['calibration_slope']:.3f}")
    return out


def write_lineage(
    cfg: dict[str, Any], seasons: dict[int, pl.DataFrame], booster: xgb.Booster, split: dict[int, str]
) -> None:
    lg = cfg["lg"]
    part = pl.concat(
        f.select("game_id")
        .unique()
        .with_columns(pl.lit(s, dtype=pl.Int64).alias("season"), pl.lit(split[s]).alias("split"))
        for s, f in seasons.items()
        if s in split
    ).sort("season", "game_id")
    part.write_parquet(HERE / f"shot_make_{lg}_partition.parquet", compression="zstd")
    hold = seasons[cfg["holdout"]]
    contrib = booster.predict(
        xgb.DMatrix(
            _shot_make_design(hold, json.loads(booster.attr("shot_types"))), feature_names=booster.feature_names
        ),
        pred_contribs=True,
    )
    imp = dict(
        zip([*booster.feature_names, "bias"], [round(float(v), 5) for v in np.abs(contrib).mean(axis=0)], strict=True)
    )
    imp = dict(sorted(imp.items(), key=lambda kv: -kv[1]))
    (HERE / f"shot_make_{lg}_importance.json").write_text(
        json.dumps(imp, indent=2) + "\n", encoding="utf-8", newline="\n"
    )


def write_fixture(cfg: dict[str, Any], hold: pl.DataFrame) -> None:
    """The first ``fixture_games`` holdout games' field goals (by ``game_id``), the columns the scorer reads."""
    games = hold.select("game_id").unique().sort("game_id").head(cfg["fixture_games"])
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    hold.join(games, on="game_id", how="semi").select(_COLS).sort("game_id", "action_number").write_parquet(
        FIXTURE_DIR / f"{cfg['lg']}_fga_{cfg['holdout']}.parquet", compression="zstd", compression_level=19
    )


def _walk_season(job: tuple[dict[int, pl.DataFrame], pl.DataFrame, int]) -> tuple[bytearray, dict[str, Any]]:
    """Fit on the training seasons, score the test season; the booster comes back as its raw bytes."""
    tr, test, seed = job
    booster = fit(tr)
    return booster.save_raw(), season_metrics(test, pl.concat(tr.values()), booster, seed=seed)


def train(league_id: str, data_root: Path | None, *, fixture: bool = False) -> dict[str, Any]:
    cfg = LEAGUES[league_id]
    needed = sorted(
        {s - k for s in cfg["walk"] for k in range(1, TRAIN_SEASONS + 1)} | set(cfg["walk"]) | set(cfg["monitor"])
    )
    seasons = {s: zone_triple(load_season(league_id, s, data_root)) for s in needed}
    split: dict[int, str] = {}
    report: dict[str, Any] = {
        "league_id": league_id,
        "params": _XGB_PARAMS,
        "train_seasons_per_fit": TRAIN_SEASONS,
        "min_level_n": MIN_LEVEL_N,
        "gates": {
            "skill": "d_log_loss vs zone_served, 95% game-clustered CI wholly > 0",
            "max_large_bias": MAX_LARGE_BIAS,
            "slope_band": SLOPE_BAND,
        },
        "seasons": {},
    }
    failed: list[str] = []
    shipped = None
    jobs = {s: ({t: seasons[t] for t in range(s - TRAIN_SEASONS, s)}, seasons[s], s) for s in cfg["walk"]}
    # one process per walk season (seed and thread count fixed, so the same fit as a serial run)
    with ProcessPoolExecutor(max_workers=min(len(jobs), _WORKERS)) as pool:
        done = dict(zip(jobs, pool.map(_walk_season, jobs.values()), strict=True))
    for s in cfg["walk"]:
        tr = jobs[s][0]
        raw, m = done[s]
        booster = xgb.Booster()
        booster.load_model(raw)
        kind = "holdout" if s == cfg["holdout"] else "walk_forward"
        report["seasons"][str(s)] = {"split": kind, "train_seasons": sorted(tr), **m}
        failed += gate(f"{kind} {s}", m)
        print(
            f"{s}: d_ll vs served {m['vs_zone_served']['d_log_loss']:+.5f} ({m['vs_zone_served']['rel_log_loss']:+.2%}) "
            f"[{m['vs_zone_served']['d_log_loss_lo']:+.5f}, {m['vs_zone_served']['d_log_loss_hi']:+.5f}], "
            f"vs prior {m['vs_zone_prior']['rel_log_loss']:+.2%}, slope {m['model']['calibration_slope']:.3f}, "
            f"bias {m['model']['large_bias']:+.4f}, rounds {m['rounds']}",
            flush=True,
        )
        if s == cfg["holdout"]:
            shipped, ship_train = booster, tr
    assert shipped is not None
    split |= dict.fromkeys(ship_train, "train") | {cfg["holdout"]: "holdout"} | dict.fromkeys(cfg["monitor"], "monitor")
    for s in cfg["monitor"]:
        m = season_metrics(seasons[s], pl.concat(ship_train.values()), shipped, seed=s)
        report["seasons"][str(s)] = {"split": "monitor", "train_seasons": sorted(ship_train), **m}
    # the season being served is gated too, as a warning: a failure there is reported, never blocks
    report["monitor_gates"] = [f for s in cfg["monitor"] for f in gate(f"monitor {s}", report["seasons"][str(s)])]
    shipped.set_attr(holdout_season=str(cfg["holdout"]))
    report["shipped"] = {
        "artifact": f"sportsdataverse/nba/models/{cfg['artifact']}",
        "train_seasons": sorted(ship_train),
        "holdout_season": cfg["holdout"],
        "monitor_seasons": cfg["monitor"],
        "rounds": int(shipped.attr("rounds")),
        "shot_types": json.loads(shipped.attr("shot_types")),
        "n_train_shots": int(sum(f.height for f in ship_train.values())),
    }
    report["failed_gates"] = failed
    accepted = OWNER_ACCEPTED.get(league_id, ())
    report["owner_accepted"] = [f for f in failed if f in accepted]
    (HERE / f"shot_make_{cfg['lg']}_holdout.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    if [f for f in failed if f not in accepted]:
        raise SystemExit("gates failed:\n" + "\n".join(failed))
    if cfg["artifact"] != SHOT_MAKE_MODELS.get(league_id):
        raise SystemExit(
            f"gates pass: add {league_id!r}: {cfg['artifact']!r} to nba_shot_value.SHOT_MAKE_MODELS to ship"
        )
    shipped.save_model(str(MODELS / cfg["artifact"]))
    report["shipped"]["sha256"] = hashlib.sha256((MODELS / cfg["artifact"]).read_bytes()).hexdigest()
    (HERE / f"shot_make_{cfg['lg']}_holdout.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    write_lineage(cfg, seasons, shipped, split)
    if fixture:
        write_fixture(cfg, seasons[cfg["holdout"]])
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-id", default="00", choices=sorted(LEAGUES))
    ap.add_argument("--data-root", type=Path, help="a producer checkout's data dir (holding <lg>_stats/)")
    ap.add_argument("--fixture", action="store_true", help="also cut tests/fixtures/nba_shot_make from the holdout")
    args = ap.parse_args()
    train(args.league_id, args.data_root, fixture=args.fixture)
