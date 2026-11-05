"""Fit, gate and bundle the NBA / WNBA EPV boosters (BB-D24).

Run (the ``{nba,wnba}_stats_pbp`` / ``_possessions`` releases; ``--data-root`` reads the producer
checkouts' parquet instead, the same files)::

    uv run python tools/models/nba_epv/train_epv.py --league-id 00
    uv run python tools/models/nba_epv/train_epv.py --league-id 10

Seasons (named by the year they end; train, holdout and confirmation disjoint):

* train: the boosting rounds are picked by early stopping on the last training season (a
  validation season inside train, never the holdout), then the booster is refit on every
  training season;
* holdout: the gates below are read here, and the offline test fixture
  (``tests/fixtures/bb_epv``) is cut from it;
* confirmation: scored once with the frozen booster and reported beside the holdout; it must
  pass the same gates.

Only regular-season games (``game_id`` ``002...``) are fitted and gated. The label is the
offense's points from the state to the end of the possession (``remaining_points``), the loss
squared error: the Brier score of a points target.

Gates, each failing the run (the values seen when the shipped boosters were fitted are in
``epv_<lg>_holdout.json``):

1. ``mse`` beats the cell-mean baseline (train means of ``start_type x last_event x ft_pending x
   transition``) on the holdout and the confirmation season;
2. calibration in the large: ``|mean(epv) - mean(remaining_points)|`` at most
   :data:`MAX_LARGE_BIAS` (a points-per-possession drift between seasons);
3. calibration by state bucket (``last_event x transition``, buckets of at least
   :data:`MIN_BUCKET` states): the largest ``|bias|`` at most :data:`MAX_BUCKET_BIAS`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.nba.nba_epv import EPV_FEATURES, add_epv, epv_states

REPO = Path(__file__).resolve().parents[3]
MODELS_DIR = REPO / "sportsdataverse" / "nba" / "models"
FIXTURE_DIR = REPO / "tests" / "fixtures" / "bb_epv"
HERE = Path(__file__).resolve().parent

LEAGUES: dict[str, dict[str, Any]] = {
    "00": {
        "lg": "nba",
        "artifact": "nba_epv.ubj",
        "train": [2021, 2022, 2023],
        "holdout": 2024,
        "confirmation": 2025,
        "fixture_games": 40,
    },
    "10": {
        "lg": "wnba",
        "artifact": "wnba_epv.ubj",
        "train": [2021, 2022, 2023],
        "holdout": 2024,
        "confirmation": 2025,
        "fixture_games": 20,
    },
}
#: Gate 2: |mean(epv) - mean(label)| over a whole season, in points. Derived from the fits that
#: shipped: the largest seen was 0.011 (NBA holdout 2024), plus 0.01 of headroom for a season's
#: points-per-possession drift (about 0.01 a season in the NBA from 2016 to 2023).
MAX_LARGE_BIAS = 0.02
#: Gate 3: the largest |bias| over the state buckets with at least MIN_BUCKET states, in points.
#: Derived the same way: the largest seen was 0.050 (NBA holdout 2024), plus 0.01.
MAX_BUCKET_BIAS = 0.06
MIN_BUCKET = 5000
_CELL = ["start_type", "last_event", "ft_pending", "transition"]
_BUCKET = ["last_event", "transition"]
_XGB_PARAMS: dict[str, Any] = {
    "objective": "reg:squarederror",
    "eval_metric": "rmse",
    "max_depth": 6,
    "eta": 0.1,
    "min_child_weight": 50,
    "subsample": 0.8,
    "tree_method": "hist",
    "seed": 42,
}
_MAX_ROUNDS = 1000
#: The pbp and possession columns the fixture keeps (what epv_states reads).
_FIXTURE_PBP = [
    "season", "game_id", "order_index", "action_number", "period", "seconds_remaining", "event_type", "sub_type",
    "description",
    "shot_distance", "shot_value", "team_id", "person_id", "location", "score_home", "score_away",
    *[f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)],
]  # fmt: skip
_FIXTURE_POSS = [
    "game_id", "period", "possession_number", "number_in_period", "start_order_index", "end_order_index",
    "start_seconds_remaining", "end_seconds_remaining", "possession_start_type", "offense_team_id",
    "defense_team_id", "points", *[f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)],
]  # fmt: skip


def load_season(league_id: str, season: int, data_root: Path | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The season's (pbp, possessions), regular season only."""
    if data_root is not None:
        lg = LEAGUES[league_id]["lg"]
        sub = data_root / f"{lg}_stats"
        pbp_name = f"{lg}_play_by_play_{season}.parquet"
        pbp = pl.read_parquet(sub / "pbp" / "parquet" / pbp_name)
        poss = pl.read_parquet(sub / "possessions" / "parquet" / f"{lg}_possessions_{season}.parquet")
    elif league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as W

        pbp, poss = W.load_wnba_stats_pbp([season]), W.load_wnba_stats_possessions([season])
    else:
        from sportsdataverse.nba import nba_loaders as N

        pbp, poss = N.load_nba_stats_pbp([season - 1]), N.load_nba_stats_possessions([season - 1])
    regular = pl.col("game_id").cast(pl.Utf8).str.slice(2, 1) == "2"
    return pbp.filter(regular), poss.filter(regular)


def _dmat(states: pl.DataFrame) -> xgb.DMatrix:
    x = states.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy()
    return xgb.DMatrix(x, label=states["remaining_points"].to_numpy(), feature_names=EPV_FEATURES)


def fit(train: dict[int, pl.DataFrame]) -> xgb.Booster:
    """Early-stop on the last training season, refit on all of them, stamp ``start_epv_mean``."""
    *fit_s, val_s = sorted(train)
    probe = xgb.train(
        _XGB_PARAMS,
        _dmat(pl.concat([train[s] for s in fit_s])),
        _MAX_ROUNDS,
        evals=[(_dmat(train[val_s]), "val")],
        early_stopping_rounds=30,
        verbose_eval=False,
    )
    rounds = int(probe.best_iteration) + 1
    every = pl.concat(train.values())
    booster = xgb.train(_XGB_PARAMS, _dmat(every), num_boost_round=rounds)
    starts = every.filter(pl.col("step") == 0).select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy()
    booster.set_attr(
        start_epv_mean=repr(float(np.mean(booster.inplace_predict(starts)))),
        train_label_mean=repr(float(every["remaining_points"].mean())),
        rounds=str(rounds),
        validation_season=str(val_s),
    )
    return booster


def cell_means(train: pl.DataFrame) -> pl.DataFrame:
    return train.group_by(_CELL).agg(pl.col("remaining_points").mean().alias("_cell"))


def metrics(
    states: pl.DataFrame, booster: xgb.Booster, cells: pl.DataFrame, const: float, start_const: float
) -> dict[str, Any]:
    """The gate metrics of ``booster`` on one season's states (see the module docstring)."""
    pred = booster.inplace_predict(states.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy())
    s = states.with_columns(pl.Series("epv", np.asarray(pred, dtype=np.float64))).join(cells, on=_CELL, how="left")
    s = s.with_columns(pl.col("_cell").fill_null(const), (pl.col("epv") - pl.col("remaining_points")).alias("_err"))
    y, p = s["remaining_points"].cast(pl.Float64).to_numpy(), s["epv"].to_numpy()
    mse = float(np.mean((p - y) ** 2))
    buckets = s.group_by(_BUCKET).agg(pl.len().alias("n"), pl.col("_err").mean().alias("bias"))
    small = buckets.filter(pl.col("n") < MIN_BUCKET)
    buckets = buckets.filter(pl.col("n") >= MIN_BUCKET)
    worst = buckets.sort(pl.col("bias").abs(), descending=True).row(0, named=True)
    start = s.filter(pl.col("step") == 0)
    return {
        "states": s.height,
        "possessions": start.height,
        "mse": mse,
        "mse_constant": float(np.mean((const - y) ** 2)),
        "mse_cell_mean": float(np.mean((s["_cell"].to_numpy() - y) ** 2)),
        "skill_vs_constant": 1 - mse / float(np.mean((const - y) ** 2)),
        "mse_start": float(np.mean((start["epv"].to_numpy() - start["remaining_points"].to_numpy()) ** 2)),
        "mse_start_constant": float(np.mean((start_const - start["remaining_points"].to_numpy()) ** 2)),
        "mean_epv": float(p.mean()),
        "mean_label": float(y.mean()),
        "large_bias": float(p.mean() - y.mean()),
        "calibration_slope": float(np.polyfit(p, y, 1)[0]),
        "buckets": buckets.height,
        "max_bucket_bias": float(abs(worst["bias"])),
        "worst_bucket": {k: worst[k] for k in (*_BUCKET, "n", "bias")},
        # report only: the buckets under MIN_BUCKET states, too few for a stable mean
        "small_buckets": small.height,
        "small_bucket_max_bias": float(small["bias"].abs().max()) if small.height else None,
    }


def gate(name: str, m: dict[str, Any]) -> list[str]:
    """The failed gates of one season's metrics."""
    out = []
    if not m["mse"] < m["mse_cell_mean"]:
        out.append(f"{name}: mse {m['mse']:.4f} does not beat the cell-mean baseline {m['mse_cell_mean']:.4f}")
    if abs(m["large_bias"]) > MAX_LARGE_BIAS:
        out.append(f"{name}: calibration-in-the-large bias {m['large_bias']:+.4f} over {MAX_LARGE_BIAS}")
    if m["max_bucket_bias"] > MAX_BUCKET_BIAS:
        out.append(f"{name}: bucket bias {m['max_bucket_bias']:.4f} over {MAX_BUCKET_BIAS} ({m['worst_bucket']})")
    return out


def load_lineups(league_id: str, season: int, data_root: Path | None) -> pl.DataFrame:
    """The season's ``{nba,wnba}_stats_game_lineups`` release (the ten on the floor at each action)."""
    if data_root is not None:
        lg = LEAGUES[league_id]["lg"]
        return pl.read_parquet(
            data_root / f"{lg}_stats" / "game_lineups" / "parquet" / f"{lg}_lineups_{season}.parquet"
        )
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_game_lineups

        return load_wnba_stats_game_lineups([season])
    from sportsdataverse.nba.nba_loaders import load_nba_stats_game_lineups

    return load_nba_stats_game_lineups([season - 1])


def write_fixture(league_id: str, season: int, pbp: pl.DataFrame, poss: pl.DataFrame, tens: pl.DataFrame) -> None:
    """The first ``fixture_games`` holdout games (by ``game_id``) for the offline tests."""
    lg, n = LEAGUES[league_id]["lg"], LEAGUES[league_id]["fixture_games"]
    games = poss.select(pl.col("game_id").cast(pl.Utf8)).unique().sort("game_id").head(n)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    pbp_cols = [c for c in _FIXTURE_PBP if c in pbp.columns]  # the WNBA pbp has no player slots
    tens_cols = ["game_id", "action_number", *[f"{s}_player_{i}" for s in ("home", "away") for i in range(1, 6)]]
    for name, frame, cols in (
        ("pbp", pbp, pbp_cols),
        ("possessions", poss, _FIXTURE_POSS),
        ("game_lineups", tens, tens_cols),
    ):
        out = frame.with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi").select(cols)
        out.write_parquet(FIXTURE_DIR / f"{lg}_{name}_{season}.parquet", compression="zstd", compression_level=19)


def train(league_id: str, data_root: Path | None) -> dict[str, Any]:
    cfg = LEAGUES[league_id]
    seasons = [*cfg["train"], cfg["holdout"], cfg["confirmation"]]
    if len(set(seasons)) != len(seasons):
        raise ValueError(f"train, holdout and confirmation seasons must be disjoint: {seasons}")
    raw = {s: load_season(league_id, s, data_root) for s in seasons}
    states = {s: epv_states(poss, pbp) for s, (pbp, poss) in raw.items()}
    tr = {s: states[s] for s in cfg["train"]}
    booster = fit(tr)
    booster.set_attr(
        train_seasons=",".join(map(str, cfg["train"])),
        holdout_season=str(cfg["holdout"]),
        confirmation_season=str(cfg["confirmation"]),
        fitting_script="tools/models/nba_epv/train_epv.py",
    )
    every = pl.concat(tr.values())
    cells, const = cell_means(every), float(every["remaining_points"].mean())
    start_const = float(every.filter(pl.col("step") == 0)["remaining_points"].mean())
    report: dict[str, Any] = {
        "league_id": league_id,
        "train_seasons": cfg["train"],
        "holdout_season": cfg["holdout"],
        "confirmation_season": cfg["confirmation"],
        "n_train_states": every.height,
        "rounds": int(booster.attr("rounds")),
        "start_epv_mean": float(booster.attr("start_epv_mean")),
        "params": _XGB_PARAMS,
        "features": EPV_FEATURES,
        "gates": {"max_large_bias": MAX_LARGE_BIAS, "max_bucket_bias": MAX_BUCKET_BIAS, "min_bucket": MIN_BUCKET},
        "holdout": metrics(states[cfg["holdout"]], booster, cells, const, start_const),
        "confirmation": metrics(states[cfg["confirmation"]], booster, cells, const, start_const),
    }
    # The possession identity over the holdout: EPV added sums to points - start + after-end.
    plays = add_epv(states[cfg["holdout"]], booster=booster)
    ident = plays.group_by("game_id", "possession_number").agg(
        (pl.col("epv_added").sum() - pl.col("points").sum() + pl.col("epv_start").first() - pl.col("epv_after").last())
        .abs()
        .alias("r")
    )
    report["holdout"]["identity_max_residual"] = float(ident["r"].max())
    failed = gate("holdout", report["holdout"]) + gate("confirmation", report["confirmation"])
    report["failed_gates"] = failed
    (HERE / f"epv_{cfg['lg']}_holdout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("rounds", "start_epv_mean", "holdout", "confirmation")}, indent=2))
    if failed:
        raise SystemExit("gates failed:\n" + "\n".join(failed))
    booster.save_model(str(MODELS_DIR / cfg["artifact"]))
    write_fixture(league_id, cfg["holdout"], *raw[cfg["holdout"]], load_lineups(league_id, cfg["holdout"], data_root))
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-id", default="00", choices=sorted(LEAGUES))
    ap.add_argument("--data-root", type=Path, help="a producer checkout's data dir (holding <lg>_stats/)")
    args = ap.parse_args()
    train(args.league_id, args.data_root)
