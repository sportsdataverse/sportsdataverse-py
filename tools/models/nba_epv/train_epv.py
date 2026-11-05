"""Fit, gate and bundle the NBA / WNBA EPV boosters (BB-D24).

Run (the ``{nba,wnba}_stats_pbp`` / ``_possessions`` releases; ``--data-root`` reads the producer
checkouts' parquet instead, the same files)::

    uv run python tools/models/nba_epv/train_epv.py --league-id 00
    uv run python tools/models/nba_epv/train_epv.py --league-id 10

Only the NBA booster ships. The WNBA fit runs and writes its report, but fails gate 2 on its
2026 confirmation season (WNBA points per possession rose about 0.04 that season), so nothing is
bundled for it; refit when a season after 2026 can confirm a fit that includes 2026.

Seasons (named by the year they end; train, holdout, confirmation and monitor disjoint):

* train: the boosting rounds are picked by early stopping on the last training season (a
  validation season inside train, never the holdout), then the booster is refit on every
  training season;
* holdout: the gate thresholds below were read here (so the holdout passes them by
  construction), and the offline test fixture (``tests/fixtures/bb_epv``) is cut from it;
* confirmation and monitor: scored once with the frozen booster; they are the real tests of the
  thresholds. The monitor season is the one being served now. The booster serves seasons from
  its first training season on (``nba_epv._check_seasons``); earlier ones are refused, because
  the older feeds code a third of their two-point attempts at 0 ft (the rim zone).

Only regular-season games (``game_id`` ``002...``) are fitted and gated. The label is the
offense's points from the state to the end of the possession (``remaining_points``), the loss
squared error: the Brier score of a points target.

Gates, each failing the run for every scored season but train (the values seen when the shipped
boosters were fitted are in ``epv_<lg>_holdout.json``; never loosen a threshold to make a refit
pass, argue the change in its PR):

1. skill over a lookup table: the per-game paired difference in squared error, model minus the
   cell-mean baseline (train means of ``start_type x last_event x ft_pending x transition x
   clock``, the clock in three bands), has a 95% interval over games wholly below 0;
2. calibration in the large: ``|mean(epv) - mean(remaining_points)|`` at most
   :data:`MAX_LARGE_BIAS`;
3. calibration by state bucket (``last_event x transition``, every bucket of at least
   :data:`MIN_BUCKET` states): ``|bias|`` at most :data:`MAX_BUCKET_BIAS` plus three standard
   errors of the bucket mean, and the gated buckets hold at least :data:`MIN_GATED_SHARE` of the
   states;
4. calibration slope (``remaining_points`` on ``epv``, least squares) inside
   :data:`SLOPE_BAND`.

Outputs beside this script: ``epv_<lg>_holdout.json`` (metrics, decile reliability table, gates),
``epv_<lg>_partition.parquet`` (every game_id and its split) and ``epv_<lg>_importance.json``
(mean absolute SHAP contribution per feature on the holdout).
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
        "train": [2022, 2023, 2024],
        "holdout": 2025,
        "confirmation": 2026,
        "monitor": [],
        "fixture_games": 40,
    },
    "10": {
        "lg": "wnba",
        "artifact": "wnba_epv.ubj",
        "train": [2022, 2023, 2024],
        "holdout": 2025,
        "confirmation": 2026,
        "monitor": [],
        "fixture_games": 20,
    },
}
#: Gate 2: |mean(epv) - mean(label)| over a whole season, in points. Read off the 2024 holdout:
#: the largest seen was 0.011 (NBA), plus 0.01 of headroom for a season's points-per-possession
#: drift (about 0.01 a season in the NBA from 2016 to 2023).
MAX_LARGE_BIAS = 0.02
#: Gate 3: |bias| per state bucket, in points, on top of three standard errors of the bucket mean.
#: Read off the 2024 holdout the same way: the largest bias in a 5,000-state NBA bucket was 0.050.
MAX_BUCKET_BIAS = 0.06
MIN_BUCKET = 500
#: Gate 3: the gated buckets must hold this share of a season's states (observed 0.999+).
MIN_GATED_SHARE = 0.99
#: Gate 4: the calibration slope (observed 1.01 to 1.03 on the 2024 and 2025 seasons).
SLOPE_BAND = (0.9, 1.1)
#: The baseline's clock bands, in seconds left in the period: a last heave, a last shot clock, the rest.
_CLOCK_BANDS = (2.0, 24.0)
_CELL = ["start_type", "last_event", "ft_pending", "transition", "_clock"]
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


def _local(data_root: Path, lg: str, kind: str, stem: str, season: int) -> pl.DataFrame:
    """A producer checkout's parquet (the WNBA 2026 pbp file is written without its league prefix)."""
    d = data_root / f"{lg}_stats" / kind / "parquet"
    path = d / f"{lg}_{stem}_{season}.parquet"
    return pl.read_parquet(path if path.exists() else d / f"{stem}_{season}.parquet")


def load_season(league_id: str, season: int, data_root: Path | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The season's (pbp, possessions), regular season only."""
    if data_root is not None:
        lg = LEAGUES[league_id]["lg"]
        pbp = _local(data_root, lg, "pbp", "play_by_play", season)
        poss = _local(data_root, lg, "possessions", "possessions", season)
    elif league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as W

        pbp, poss = (
            W.load_wnba_stats_pbp([season]),
            W.load_wnba_stats_possessions([season]),
        )
    else:
        from sportsdataverse.nba import nba_loaders as N

        pbp, poss = (
            N.load_nba_stats_pbp([season - 1]),
            N.load_nba_stats_possessions([season - 1]),
        )
    regular = pl.col("game_id").cast(pl.Utf8).str.slice(2, 1) == "2"
    return pbp.filter(regular), poss.filter(regular)


def _x(states: pl.DataFrame) -> np.ndarray:
    return states.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy()


def _dmat(states: pl.DataFrame) -> xgb.DMatrix:
    return xgb.DMatrix(
        _x(states),
        label=states["remaining_points"].to_numpy(),
        feature_names=EPV_FEATURES,
    )


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
    starts = _x(every.filter(pl.col("step") == 0))
    booster.set_attr(
        start_epv_mean=repr(float(np.mean(booster.inplace_predict(starts)))),
        train_label_mean=repr(float(every["remaining_points"].mean())),
        rounds=str(rounds),
        validation_season=str(val_s),
    )
    return booster


def _with_clock(states: pl.DataFrame) -> pl.DataFrame:
    s = pl.col("seconds_remaining")
    return states.with_columns(
        pl.when(s <= _CLOCK_BANDS[0]).then(0).when(s <= _CLOCK_BANDS[1]).then(1).otherwise(2).alias("_clock")
    )


def cell_means(train: pl.DataFrame) -> pl.DataFrame:
    return _with_clock(train).group_by(_CELL).agg(pl.col("remaining_points").mean().alias("_cell"))


def _scored(states: pl.DataFrame, booster: xgb.Booster, cells: pl.DataFrame, const: float) -> pl.DataFrame:
    """The states with ``epv``, the baseline ``_cell`` and both squared errors."""
    pred = np.asarray(booster.inplace_predict(_x(states)), dtype=np.float64)
    y = pl.col("remaining_points").cast(pl.Float64)
    return (
        _with_clock(states)
        .with_columns(pl.Series("epv", pred))
        .join(cells, on=_CELL, how="left")
        .with_columns(pl.col("_cell").fill_null(const))
        .with_columns(
            (pl.col("epv") - y).alias("_err"),
            ((pl.col("epv") - y) ** 2).alias("_se"),
            ((pl.col("_cell") - y) ** 2).alias("_se_cell"),
        )
    )


def _paired(s: pl.DataFrame) -> dict[str, float]:
    """Gate 1: per-game mean of (model - baseline) squared error, its mean over games and 95% interval."""
    d = s.group_by("game_id").agg((pl.col("_se") - pl.col("_se_cell")).mean().alias("d"))["d"].to_numpy()
    half = 1.96 * float(np.std(d, ddof=1)) / np.sqrt(len(d))
    return {
        "games": len(d),
        "mean": float(d.mean()),
        "ci_lo": float(d.mean()) - half,
        "ci_hi": float(d.mean()) + half,
    }


def _buckets(s: pl.DataFrame) -> dict[str, Any]:
    """Gate 3: the state buckets' bias against MAX_BUCKET_BIAS plus three standard errors."""
    b = (
        s.group_by(_BUCKET)
        .agg(
            pl.len().alias("n"),
            pl.col("_err").mean().alias("bias"),
            pl.col("_err").std().alias("sd"),
        )
        .with_columns((MAX_BUCKET_BIAS + 3 * pl.col("sd") / pl.col("n").sqrt()).alias("allowed"))
    )
    gated = b.filter(pl.col("n") >= MIN_BUCKET)
    worst = gated.sort(pl.col("bias").abs() - pl.col("allowed"), descending=True).row(0, named=True)
    return {
        "buckets": b.height,
        "gated_buckets": gated.height,
        "gated_state_share": float(gated["n"].sum() / s.height),
        "max_bucket_bias": float(gated["bias"].abs().max()),
        "bucket_failures": gated.filter(pl.col("bias").abs() > pl.col("allowed")).height,
        "worst_bucket": {k: worst[k] for k in (*_BUCKET, "n", "bias", "allowed")},
        "ungated_max_bias": float(b.filter(pl.col("n") < MIN_BUCKET)["bias"].abs().max() or 0.0),
    }


def _deciles(s: pl.DataFrame) -> list[dict[str, float]]:
    """The reliability table: mean EPV and mean label per decile of EPV."""
    q = s.with_columns((pl.col("epv").rank("ordinal") * 10 // (s.height + 1)).alias("decile"))
    t = q.group_by("decile").agg(
        pl.len().alias("n"),
        pl.col("epv").mean().alias("mean_epv"),
        pl.col("remaining_points").mean().alias("mean_y"),
    )
    return t.sort("decile").to_dicts()


def metrics(
    states: pl.DataFrame,
    booster: xgb.Booster,
    cells: pl.DataFrame,
    const: float,
    start_const: float,
) -> dict[str, Any]:
    """The gate metrics of ``booster`` on one season's states (see the module docstring)."""
    s = _scored(states, booster, cells, const)
    y, p = s["remaining_points"].cast(pl.Float64).to_numpy(), s["epv"].to_numpy()
    mse = float(s["_se"].mean())
    start = s.filter(pl.col("step") == 0)
    return {
        "states": s.height,
        "possessions": start.height,
        "mse": mse,
        "mse_constant": float(np.mean((const - y) ** 2)),
        "mse_cell_mean": float(s["_se_cell"].mean()),
        "skill_vs_constant": 1 - mse / float(np.mean((const - y) ** 2)),
        "paired_vs_cell_mean": _paired(s),
        "mse_start": float(start["_se"].mean()),
        "mse_start_constant": float(np.mean((start_const - start["remaining_points"].to_numpy()) ** 2)),
        "mean_epv": float(p.mean()),
        "mean_label": float(y.mean()),
        "large_bias": float(p.mean() - y.mean()),
        "calibration_slope": float(np.polyfit(p, y, 1)[0]),
        **_buckets(s),
        "deciles": _deciles(s),
    }


def gate(name: str, m: dict[str, Any]) -> list[str]:
    """The failed gates of one season's metrics."""
    out = []
    if not m["paired_vs_cell_mean"]["ci_hi"] < 0:
        out.append(f"{name}: paired mse vs the cell-mean baseline not below 0: {m['paired_vs_cell_mean']}")
    if abs(m["large_bias"]) > MAX_LARGE_BIAS:
        out.append(f"{name}: calibration-in-the-large bias {m['large_bias']:+.4f} over {MAX_LARGE_BIAS}")
    if m["bucket_failures"] or m["gated_state_share"] < MIN_GATED_SHARE:
        out.append(f"{name}: {m['bucket_failures']} buckets over the bias gate, worst {m['worst_bucket']}")
    if not SLOPE_BAND[0] <= m["calibration_slope"] <= SLOPE_BAND[1]:
        out.append(f"{name}: calibration slope {m['calibration_slope']:.3f} outside {SLOPE_BAND}")
    return out


def load_lineups(league_id: str, season: int, data_root: Path | None) -> pl.DataFrame:
    """The season's ``{nba,wnba}_stats_game_lineups`` release (the ten on the floor at each action)."""
    if data_root is not None:
        return _local(data_root, LEAGUES[league_id]["lg"], "game_lineups", "lineups", season)
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_game_lineups

        return load_wnba_stats_game_lineups([season])
    from sportsdataverse.nba.nba_loaders import load_nba_stats_game_lineups

    return load_nba_stats_game_lineups([season - 1])


def write_fixture(
    league_id: str,
    season: int,
    pbp: pl.DataFrame,
    poss: pl.DataFrame,
    tens: pl.DataFrame,
) -> None:
    """The first ``fixture_games`` holdout games (by ``game_id``) for the offline tests."""
    lg, n = LEAGUES[league_id]["lg"], LEAGUES[league_id]["fixture_games"]
    games = poss.select(pl.col("game_id").cast(pl.Utf8)).unique().sort("game_id").head(n)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    pbp_cols = [c for c in _FIXTURE_PBP if c in pbp.columns]  # the WNBA pbp has no player slots
    tens_cols = [
        "game_id",
        "action_number",
        *[f"{s}_player_{i}" for s in ("home", "away") for i in range(1, 6)],
    ]
    for name, frame, cols in (
        ("pbp", pbp, pbp_cols),
        ("possessions", poss, _FIXTURE_POSS),
        ("game_lineups", tens, tens_cols),
    ):
        out = frame.with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi").select(cols)
        out.write_parquet(
            FIXTURE_DIR / f"{lg}_{name}_{season}.parquet",
            compression="zstd",
            compression_level=19,
        )


def _splits(cfg: dict[str, Any]) -> dict[int, str]:
    return {
        **dict.fromkeys(cfg["train"], "train"),
        cfg["holdout"]: "holdout",
        cfg["confirmation"]: "confirmation",
        **dict.fromkeys(cfg["monitor"], "monitor"),
    }


def write_lineage(cfg: dict[str, Any], states: dict[int, pl.DataFrame], booster: xgb.Booster) -> None:
    """The partition (every game_id and its split) and the per-feature mean |SHAP| on the holdout."""
    split = _splits(cfg)
    part = pl.concat(
        s.select("game_id").unique().with_columns(pl.lit(season).alias("season"), pl.lit(split[season]).alias("split"))
        for season, s in states.items()
    ).sort("season", "game_id")
    part.write_parquet(HERE / f"epv_{cfg['lg']}_partition.parquet", compression="zstd")
    contrib = booster.predict(_dmat(states[cfg["holdout"]]), pred_contribs=True)
    imp = dict(
        zip(
            [*EPV_FEATURES, "bias"],
            np.abs(contrib).mean(axis=0).round(5).tolist(),
            strict=True,
        )
    )
    (HERE / f"epv_{cfg['lg']}_importance.json").write_text(json.dumps(imp, indent=2) + "\n", encoding="utf-8")


def _identity(states: pl.DataFrame, booster: xgb.Booster) -> float:
    """Over a season: |EPV added summed over a possession - (points - start + after-end)|, at most."""
    plays = add_epv(states, booster=booster)
    ident = plays.group_by("game_id", "possession_number").agg(
        (pl.col("epv_added").sum() - pl.col("points").sum() + pl.col("epv_start").first() - pl.col("epv_after").last())
        .abs()
        .alias("r")
    )
    return float(ident["r"].max())


def train(league_id: str, data_root: Path | None) -> dict[str, Any]:
    cfg = LEAGUES[league_id]
    split = _splits(cfg)
    if len(split) != len(cfg["train"]) + 2 + len(cfg["monitor"]):
        raise ValueError(f"train, holdout, confirmation and monitor seasons must be disjoint: {cfg}")
    raw = {s: load_season(league_id, s, data_root) for s in split}
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
    scored = {s: metrics(states[s], booster, cells, const, start_const) for s in split if split[s] != "train"}
    report: dict[str, Any] = {
        "league_id": league_id,
        "splits": {str(s): v for s, v in split.items()},
        "n_train_states": every.height,
        "rounds": int(booster.attr("rounds")),
        "start_epv_mean": float(booster.attr("start_epv_mean")),
        "params": _XGB_PARAMS,
        "features": EPV_FEATURES,
        "gates": {
            "max_large_bias": MAX_LARGE_BIAS,
            "max_bucket_bias": MAX_BUCKET_BIAS,
            "min_bucket": MIN_BUCKET,
            "min_gated_share": MIN_GATED_SHARE,
            "slope_band": SLOPE_BAND,
        },
        "seasons": {str(s): {"split": split[s], **m} for s, m in scored.items()},
        "holdout_identity_max_residual": _identity(states[cfg["holdout"]], booster),
    }
    failed = [f for s, m in scored.items() for f in gate(f"{split[s]} {s}", m)]
    report["failed_gates"] = failed
    (HERE / f"epv_{cfg['lg']}_holdout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    brief = {
        s: {k: m[k] for k in ("mse", "mse_cell_mean", "large_bias", "calibration_slope")} for s, m in scored.items()
    }
    print(json.dumps({"rounds": report["rounds"], "seasons": brief, "failed": failed}, indent=2))
    if failed:
        raise SystemExit("gates failed:\n" + "\n".join(failed))
    booster.save_model(str(MODELS_DIR / cfg["artifact"]))
    write_lineage(cfg, states, booster)
    write_fixture(
        league_id,
        cfg["holdout"],
        *raw[cfg["holdout"]],
        load_lineups(league_id, cfg["holdout"], data_root),
    )
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-id", default="00", choices=sorted(LEAGUES))
    ap.add_argument(
        "--data-root",
        type=Path,
        help="a producer checkout's data dir (holding <lg>_stats/)",
    )
    args = ap.parse_args()
    train(args.league_id, args.data_root)
