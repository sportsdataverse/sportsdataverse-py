"""Fit and gate the men's and women's college shot-make boosters (the location model of ``mbb_shot_quality``).

Run (the ``mbb_shots`` / ``wbb_shots`` ESPN releases; ``--data-root`` reads a producer checkout's parquet instead,
the same files)::

    uv run python tools/models/mbb_shot_value/train_shot_make.py --league mens
    uv run python tools/models/mbb_shot_value/train_shot_make.py --league womens

**The model.** P(make) of a college field-goal attempt from the canonical shot frame
(``mbb_shot_data``): its basket-relative location (``shot_x`` / ``shot_y``) and distance (``dist_ft``), its
value (``point_value``) and its coarse type (``shot_type``: ``rim`` / ``jump`` / ``arc3``, the type the current
model already reads); :func:`design` builds the matrix. No player, team or game-state input.

**The comparator** is the model it would replace, ``mbb_shot_quality_model``: the empirical-Bayes make rate of
the shot's ``shot_zone x shot_type`` cell. Two versions are scored:

* ``zone_served``: the scored season's own table, as ``mbb_shot_quality(shots)`` builds it by default (it
  includes the scored shots). This is the comparator the gate uses, and it favours the table.
* ``zone_prior``: the training seasons' table, the out-of-sample version.

As served (the NBA rule of 2026-11-25, fixed here before the first run), threes keep the table's value; the
booster's own scores, threes included, are reported as ``model_raw`` / ``raw_vs_zone_served``.

**Seasons** (named by the year they end; every game, no season-type filter). Only shots with coordinates
reach the canonical frame, and the release locates a few hundred to 1,700 games a season before 2026 (6,000+
from 2026): the earlier seasons are a subset of games, not the whole league.
Every season in ``walk`` is scored by a booster fitted on the :data:`TRAIN_SEASONS` seasons before it. The
shipped booster is the walk-forward fit of the ``holdout`` season; ``monitor`` seasons (the one in progress)
are scored by it and gated as a warning. Early stopping on the last training season, then a refit on all three.

**Gates**, fixed before the first run (the NBA fitter's, imported: ``tools/models/nba_shot_value/train_shot_make.py``),
applied to every walk-forward season, the holdout included: (1) the log-loss gain over ``zone_served`` has a 95%
game-clustered bootstrap interval wholly above 0; (2) ``|mean(p) - mean(y)|`` at most 0.01; (3) the calibration
slope inside [0.9, 1.1]. **Never loosen a gate to make a refit pass.** An owner-accepted failure would be listed
in :data:`OWNER_ACCEPTED` with its exact value; none is.

Hyperparameters are the NBA fitter's, fixed a priori (no search). Output (beside this script): the report
``shot_make_<lg>_holdout.json``.

**Outcome (2026-11-26): neither league passes, nothing ships.** The gain over the served table is small
(men's 0.5% to 1.3%, women's 0.6% to 0.9%) and the calibration does not hold: men's slope 0.80 on 2025 and,
on the 2026 holdout, slope 1.21 and level -0.0136; women's slope 0.88 on 2024 (the women's 2026 holdout passes:
slope 1.10, level +0.0019). Why is not established. 2026 is the first season the release locates every game
(6,000+ against 400 to 1,700 before), so the booster may meet a different set of games than it was fitted on;
but the cell table fitted on the same training seasons (``zone_prior``) stays calibrated on the men's 2026
(slope 1.00, level -0.0010), and the men's 2025 and women's 2024 failures come before that change.
``mbb_shot_quality_model`` stays the college model.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.mbb.mbb_shot_quality import mbb_shot_quality, mbb_shot_quality_model
from sportsdataverse.mbb.mbb_shots_adapter import espn_shots_to_canonical

REPO = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "nba_tsm", REPO / "tools" / "models" / "nba_shot_value" / "train_shot_make.py"
)
nba = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nba)

#: The design's columns, in order: the canonical frame's location, distance and value, then one indicator per
#: coarse shot type.
FEATURES: list[str] = [
    "dist_ft",
    "shot_x",
    "shot_y",
    "point_value",
    "shot_type=rim",
    "shot_type=jump",
    "shot_type=arc3",
]
#: Walk seasons, fixed before the first run by coverage alone: a season is walked when it and each of its three
#: training seasons hold at least 100 games with located shots in the release (men's 2014-2027 all do; the
#: women's release has none for 2017-2020).
LEAGUES: dict[str, dict[str, Any]] = {
    "mens": {"lg": "mbb", "dir": "mbb", "walk": list(range(2017, 2027)), "holdout": 2026, "monitor": [2027]},
    "womens": {"lg": "wbb", "dir": "wbb", "walk": [2024, 2025, 2026], "holdout": 2026, "monitor": [2027]},
}
TRAIN_SEASONS = 3
OWNER_ACCEPTED: dict[str, tuple[str, ...]] = {}
_WORKERS = int(os.environ.get("SHOT_MAKE_WORKERS", "4"))


def load_season(league: str, season: int, data_root: Path | None) -> pl.DataFrame:
    """The season's canonical field goals with a location (``game_id`` Utf8, ``y`` 1 on a make)."""
    cfg = LEAGUES[league]
    if data_root is not None:
        espn = pl.read_parquet(data_root / cfg["dir"] / "shots" / "parquet" / f"shots_{season}.parquet")
    elif league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_shots

        espn = load_wbb_shots([season])
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_shots

        espn = load_mbb_shots([season])
    out = (
        espn_shots_to_canonical(espn, league=league, season=season)
        .filter(pl.col("shot_x").is_not_null() & pl.col("shot_y").is_not_null() & pl.col("made").is_not_null())
        .with_columns(pl.col("made").cast(pl.Int8).alias("y"), pl.lit(season, dtype=pl.Int64).alias("season"))
    )
    if out.is_empty():
        raise ValueError(f"{cfg['lg']} {season}: no located field goals")
    return out


def design(shots: pl.DataFrame) -> np.ndarray:
    """The design matrix (null as NaN)."""
    t = pl.col("shot_type")
    return shots.select(
        *(pl.col(c).cast(pl.Float64) for c in ("dist_ft", "shot_x", "shot_y", "point_value")),
        *((t == k).fill_null(False).cast(pl.Float64).alias(f"shot_type={k}") for k in ("rim", "jump", "arc3")),
    ).to_numpy()


def table_predict(table_from: pl.DataFrame, scored: pl.DataFrame, league: str) -> np.ndarray:
    """Each scored shot's ``mbb_shot_quality`` make chance from a table fitted on ``table_from`` (absent: overall)."""
    xm = mbb_shot_quality(scored, model=mbb_shot_quality_model(table_from, league=league), league=league)["xmake"]
    return xm.fill_null(float(table_from["y"].mean())).to_numpy()


def fit(train: dict[int, pl.DataFrame]) -> xgb.Booster:
    *fit_s, val_s = sorted(train)
    every = pl.concat(train.values())

    def dm(f: pl.DataFrame) -> xgb.DMatrix:
        return xgb.DMatrix(design(f), label=f["y"].to_numpy(), feature_names=FEATURES)

    probe = xgb.train(
        nba._XGB_PARAMS,
        dm(pl.concat([train[s] for s in fit_s])),
        nba._MAX_ROUNDS,
        evals=[(dm(train[val_s]), "val")],
        early_stopping_rounds=50,
        verbose_eval=False,
    )
    rounds = int(probe.best_iteration) + 1
    booster = xgb.train(nba._XGB_PARAMS, dm(every), num_boost_round=rounds)
    booster.set_attr(
        rounds=str(rounds),
        validation_season=str(val_s),
        train_seasons=",".join(map(str, sorted(train))),
        train_make_rate=repr(float(every["y"].mean())),
        fitting_script="tools/models/mbb_shot_value/train_shot_make.py",
    )
    return booster


def _band(shots: pl.DataFrame) -> np.ndarray:
    return (
        shots.select(
            pl.when(pl.col("point_value") == 3)
            .then(pl.lit("3PA"))
            .when(pl.col("dist_ft") < 4)
            .then(pl.lit("rim 0-3 ft"))
            .otherwise(pl.lit("2PA 4+ ft"))
        )
        .to_series()
        .to_numpy()
    )


def season_metrics(
    test: pl.DataFrame, train: pl.DataFrame, booster: xgb.Booster, seed: int, league: str
) -> dict[str, Any]:
    y = test["y"].to_numpy().astype(float)
    g = test["game_id"].to_numpy()
    raw = booster.inplace_predict(design(test))
    served = table_predict(test, test, league)
    three = test["point_value"].to_numpy() == 3
    p = {
        "model": np.where(three, served, raw),
        "model_raw": raw,
        "zone_served": served,
        "zone_prior": table_predict(train, test, league),
    }
    band = _band(test)
    return {
        "shots": len(y),
        "games": int(test["game_id"].n_unique()),
        "make_rate": float(y.mean()),
        "rounds": int(booster.attr("rounds")),
        **{k: nba.score(y, v) for k, v in p.items()},
        "vs_zone_served": nba.paired(y, p["zone_served"], p["model"], g, seed),
        "vs_zone_prior": nba.paired(y, p["zone_prior"], p["model"], g, seed + 1),
        "raw_vs_zone_served": nba.paired(y, p["zone_served"], p["model_raw"], g, seed + 3),
        "vs_zone_served_by_band": {
            b: nba.paired(y[band == b], p["zone_served"][band == b], p["model"][band == b], g[band == b], seed + 2)
            for b in ("rim 0-3 ft", "2PA 4+ ft", "3PA")
        },
    }


def _walk_season(job: tuple[dict[int, pl.DataFrame], pl.DataFrame, int, str]) -> tuple[bytearray, dict[str, Any]]:
    tr, test, seed, league = job
    booster = fit(tr)
    return booster.save_raw(), season_metrics(test, pl.concat(tr.values()), booster, seed, league)


def train(league: str, data_root: Path | None) -> dict[str, Any]:
    cfg = LEAGUES[league]
    needed = sorted(
        {s - k for s in cfg["walk"] for k in range(1, TRAIN_SEASONS + 1)} | set(cfg["walk"]) | set(cfg["monitor"])
    )
    seasons = {s: load_season(league, s, data_root) for s in needed}
    report: dict[str, Any] = {
        "league": league,
        "params": nba._XGB_PARAMS,
        "features": FEATURES,
        "train_seasons_per_fit": TRAIN_SEASONS,
        "gates": {
            "skill": "d_log_loss vs zone_served, 95% game-clustered CI wholly > 0",
            "max_large_bias": nba.MAX_LARGE_BIAS,
            "slope_band": nba.SLOPE_BAND,
        },
        "seasons": {},
    }
    jobs = {s: ({t: seasons[t] for t in range(s - TRAIN_SEASONS, s)}, seasons[s], s, league) for s in cfg["walk"]}
    with ProcessPoolExecutor(max_workers=min(len(jobs), _WORKERS)) as pool:
        done = dict(zip(jobs, pool.map(_walk_season, jobs.values()), strict=True))
    failed: list[str] = []
    shipped = ship_train = None
    for s in cfg["walk"]:
        raw, m = done[s]
        kind = "holdout" if s == cfg["holdout"] else "walk_forward"
        report["seasons"][str(s)] = {"split": kind, "train_seasons": sorted(jobs[s][0]), **m}
        failed += nba.gate(f"{kind} {s}", m)
        v = m["vs_zone_served"]
        print(
            f"{s}: d_ll vs served {v['d_log_loss']:+.5f} ({v['rel_log_loss']:+.2%}) [{v['d_log_loss_lo']:+.5f}, "
            f"{v['d_log_loss_hi']:+.5f}], raw {m['raw_vs_zone_served']['rel_log_loss']:+.2%}, slope "
            f"{m['model']['calibration_slope']:.3f}, bias {m['model']['large_bias']:+.4f}, rounds {m['rounds']}",
            flush=True,
        )
        if s == cfg["holdout"]:
            shipped = xgb.Booster()
            shipped.load_model(raw)
            ship_train = jobs[s][0]
    assert shipped is not None and ship_train is not None
    for s in cfg["monitor"]:
        m = season_metrics(seasons[s], pl.concat(ship_train.values()), shipped, s, league)
        report["seasons"][str(s)] = {"split": "monitor", "train_seasons": sorted(ship_train), **m}
    report["monitor_gates"] = [f for s in cfg["monitor"] for f in nba.gate(f"monitor {s}", report["seasons"][str(s)])]
    shipped.set_attr(holdout_season=str(cfg["holdout"]))
    report["shipped"] = {  # the candidate: written only if every gate passes (none has)
        "bundled": False,
        "artifact": f"sportsdataverse/mbb/models/{cfg['lg']}_shot_make.ubj",
        "train_seasons": sorted(ship_train),
        "holdout_season": cfg["holdout"],
        "monitor_seasons": cfg["monitor"],
        "rounds": int(shipped.attr("rounds")),
        "n_train_shots": int(sum(f.height for f in ship_train.values())),
    }
    accepted = OWNER_ACCEPTED.get(league, ())
    report["failed_gates"] = failed
    report["owner_accepted"] = [f for f in failed if f in accepted]
    out = HERE / f"shot_make_{cfg['lg']}_holdout.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    if [f for f in failed if f not in accepted]:
        raise SystemExit("gates failed:\n" + "\n".join(failed))
    # ponytail: nothing has passed, so nothing is bundled; a pass means saving the booster and wiring
    # mbb_shot_quality to score with it (as score_shot_xpoints does for the NBA).
    raise SystemExit("gates pass: bundle the booster and add the location model to mbb_shot_quality")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="mens", choices=sorted(LEAGUES))
    ap.add_argument("--data-root", type=Path, help="a producer checkout's data dir (holding mbb/ or wbb/)")
    args = ap.parse_args()
    train(args.league, args.data_root)
