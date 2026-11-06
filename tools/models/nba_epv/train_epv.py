"""Fit, gate and bundle the NBA, WNBA and college EPV boosters (BB-D24).

Run (the ``{nba,wnba}_stats_pbp`` / ``_possessions`` and ``ncaa_{mbb,wbb}_pbp`` releases;
``--data-root`` reads a producer checkout's parquet instead, the same files)::

    uv run python tools/models/nba_epv/train_epv.py --league-id 00
    uv run python tools/models/nba_epv/train_epv.py --league-id 10
    uv run python tools/models/nba_epv/train_epv.py --league-id mbb
    uv run python tools/models/nba_epv/train_epv.py --league-id wbb

The NBA, men's and women's college boosters ship. The WNBA fit runs and writes its report but
ships nothing. Its gate-2 threshold is its own (:func:`max_large_bias`: 0.01 plus the SD of its
year-over-year points per possession, 2011-2025), and its level reads each game's date from the
``wnba_stats_schedules`` release. With both it passes gates 1, 2 and 4 on 2025 and 2026, but one
state bucket (a shooting foul in a transition possession, which it already over-prices on the 2025
holdout) fails gate 3 on 2026; that bucket passes without the level. Thresholds are left as they are.

The WNBA and college boosters carry an as-of scoring level (``nba_epv.epv_level``): a season's
points per possession move (men's college rose from 1.011 in 2022 to 1.075 in 2026), and a booster
alone predicts its training seasons' level. Its weight ``level_k`` is fitted on seasons before the
holdout (:func:`level_k`); the gates score the level-scaled EPV, as it is served, and the report
also gives each season's bias without it, by month and per state bucket.

**The level was added after the seasons it is gated on were seen** (the WNBA's 2026 failure, and the
2025 / 2026 college points per possession), so for the men's booster 2026 is a design season for
gate 2, not a clean confirmation: it passes with the level (-0.008) and fails without it (-0.031).
2027 (2026-27) is the pre-registered confirmation of the level: when it completes, add it as a
``monitor`` season and gate it with these thresholds, unchanged. The women's booster passes gate 2
on both seasons without the level too (the level moves its bias from -0.005 to -0.009).

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

Only regular-season NBA / WNBA games (``game_id`` ``002...``) are fitted and gated; college fits
and gates every game (its contest ids carry no season type). The label is the
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

from sportsdataverse.mbb.mbb_epv import ncaa_epv_inputs
from sportsdataverse.nba.nba_epv import EPV_FEATURES, add_epv, epv_level, epv_states

REPO = Path(__file__).resolve().parents[3]
FIXTURE_DIR = REPO / "tests" / "fixtures" / "bb_epv"
HERE = Path(__file__).resolve().parent
_NBA_MODELS = REPO / "sportsdataverse" / "nba" / "models"
_COLLEGE_MODELS = REPO / "sportsdataverse" / "mbb" / "models"

#: ``level``: the booster carries an as-of scoring level (``nba_epv.epv_level``), its weight
#: ``level_k`` fitted on the ``history`` seasons (all before the holdout). The NBA booster, fitted
#: first, has none.
LEAGUES: dict[str, dict[str, Any]] = {
    "00": {
        "lg": "nba",
        "models": _NBA_MODELS,
        "artifact": "nba_epv.ubj",
        "train": [2022, 2023, 2024],
        "holdout": 2025,
        "confirmation": 2026,
        "monitor": [],
        "fixture_games": 40,
        "level": False,
    },
    "10": {
        "lg": "wnba",
        "models": _NBA_MODELS,
        "artifact": "wnba_epv.ubj",
        "train": [2022, 2023, 2024],
        "holdout": 2025,
        "confirmation": 2026,
        "monitor": [],
        "fixture_games": 20,
        "level": True,
        "history": list(range(2010, 2025)),
        # gate 2's drift allowance is the WNBA's own: the season-to-season change in points per
        # possession over these seasons (2011-2025, through the holdout; never the confirmation)
        "gate2_drift": list(range(2010, 2026)),
    },
    "mbb": {
        "lg": "mbb",
        "models": _COLLEGE_MODELS,
        "artifact": "mbb_epv.ubj",
        "train": [2022, 2023, 2024],
        "holdout": 2025,
        "confirmation": 2026,
        "monitor": [],
        "fixture_games": 20,
        "level": True,
        "history": list(range(2010, 2025)),
    },
    "wbb": {
        "lg": "wbb",
        "models": _COLLEGE_MODELS,
        "artifact": "wbb_epv.ubj",
        "train": [2022, 2023, 2024],
        "holdout": 2025,
        "confirmation": 2026,
        "monitor": [],
        "fixture_games": 20,
        "level": True,
        "history": list(range(2010, 2025)),
    },
}
_COLLEGE = ("mbb", "wbb")
#: The level's trailing window: a season's level is compared with the mean of this many before it.
_LEVEL_TRAIL = 3
#: Gate 2: |mean(epv) - mean(label)| over a whole season, in points. The rule: an allowance for the
#: holdout's own error (:data:`GATE2_BASE`) plus the league's season-to-season drift in points per
#: possession (the SD of the year-over-year change). For the NBA, read off the 2024 holdout: the
#: largest seen was 0.011, plus 0.01 for its drift (about 0.01 a season from 2016 to 2023); the
#: college boosters use the same 0.02. The WNBA's drift is derived from its seasons
#: (``gate2_drift``, :func:`max_large_bias`).
MAX_LARGE_BIAS = 0.02
GATE2_BASE = 0.01
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


def _college_pbp(league_id: str, season: int, data_root: Path | None) -> pl.DataFrame:
    """The season's ``ncaa_{mbb,wbb}_pbp`` release (every game: college has no season-type code in its ids)."""
    if data_root is not None:
        return pl.read_parquet(data_root / league_id / "pbp" / "parquet" / f"ncaa_{league_id}_pbp_{season}.parquet")
    if league_id == "wbb":
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_pbp

        return load_ncaa_wbb_pbp([season])
    from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp

    return load_ncaa_mbb_pbp([season])


def season_ppp(league_id: str, season: int, data_root: Path | None) -> tuple[float, float]:
    """A season's points per possession and the variance of a possession's points (the possessions release)."""
    if league_id in _COLLEGE:
        if data_root is not None:
            d = data_root / league_id / "possessions" / "parquet" / f"ncaa_{league_id}_possessions_{season}.parquet"
            pts = pl.read_parquet(d, columns=["pts"])["pts"]
        else:
            from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_possessions
            from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_possessions

            load = load_ncaa_wbb_possessions if league_id == "wbb" else load_ncaa_mbb_possessions
            pts = load([season])["pts"]
    else:
        lg = LEAGUES[league_id]["lg"]
        if data_root is not None:
            poss = _local(data_root, lg, "possessions", "possessions", season)
        elif league_id == "10":
            from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_possessions

            poss = load_wnba_stats_possessions([season])
        else:
            from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions

            poss = load_nba_stats_possessions([season - 1])
        pts = poss.filter(pl.col("game_id").cast(pl.Utf8).str.slice(2, 1) == "2")["points"]
    return float(pts.mean()), float(pts.var())


def level_k(cfg: dict[str, Any], league_id: str, data_root: Path | None) -> dict[str, float]:
    """The level's empirical-Bayes weight, from the ``history`` seasons (all before the holdout).

    ``tau2`` is the mean squared gap between a season's points per possession and the mean of the
    :data:`_LEVEL_TRAIL` seasons before it; ``sigma2`` a possession's variance in the training
    seasons. ``level_k = sigma2 / tau2`` possessions: the weight at which a season's own games
    tell as much about its level as the training level does.
    """
    if max(cfg["history"]) >= cfg["holdout"]:
        raise ValueError("the level's history must end before the holdout season")
    ppp = {s: season_ppp(league_id, s, data_root)[0] for s in cfg["history"]}
    gaps = [
        ppp[s] - float(np.mean([ppp[s - j] for j in range(1, _LEVEL_TRAIL + 1)]))
        for s in cfg["history"]
        if all(s - j in ppp for j in range(1, _LEVEL_TRAIL + 1))
    ]
    tau2 = float(np.mean(np.square(gaps)))
    sigma2 = float(np.mean([season_ppp(league_id, s, data_root)[1] for s in cfg["train"]]))
    return {"tau2": tau2, "sigma2": sigma2, "level_k": sigma2 / tau2, "n_gaps": len(gaps)}


def max_large_bias(cfg: dict[str, Any], league_id: str, data_root: Path | None) -> dict[str, Any]:
    """Gate 2's threshold: :data:`GATE2_BASE` plus the SD of the year-over-year change in a league's
    points per possession over its ``gate2_drift`` seasons (all on or before the holdout).

    Leagues without ``gate2_drift`` keep :data:`MAX_LARGE_BIAS`, the NBA-derived value.
    """
    if "gate2_drift" not in cfg:
        return {"max_large_bias": MAX_LARGE_BIAS, "rule": "MAX_LARGE_BIAS (NBA-derived)"}
    if max(cfg["gate2_drift"]) > cfg["holdout"]:
        raise ValueError("gate 2's drift seasons must end at the holdout")
    ppp = {s: season_ppp(league_id, s, data_root)[0] for s in cfg["gate2_drift"]}
    yoy = [ppp[s] - ppp[s - 1] for s in cfg["gate2_drift"] if s - 1 in ppp]
    sd = float(np.std(yoy, ddof=1))
    return {
        "max_large_bias": GATE2_BASE + sd,
        "rule": "GATE2_BASE + sd(year-over-year points per possession)",
        "base": GATE2_BASE,
        "drift_sd": sd,
        "drift_seasons": [min(cfg["gate2_drift"]) + 1, max(cfg["gate2_drift"])],
        "season_ppp": {str(k): v for k, v in ppp.items()},
    }


def _game_order(schedule: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` and ``game_order`` (the game's date) from a ``wnba_stats_schedules`` release."""
    return schedule.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d").alias("game_order"),
    ).unique()


def _wnba_order(season: int, data_root: Path | None) -> pl.DataFrame:
    """The WNBA games' dates (``wnba_stats_schedules``), for the level: same-day games never see each other."""
    if data_root is not None:
        return _game_order(_local(data_root, "wnba", "schedules", "stats_schedule", season))
    from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_schedules

    return _game_order(load_wnba_stats_schedules([season]))


def load_inputs(
    league_id: str, season: int, data_root: Path | None
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame | None, pl.DataFrame | None, pl.DataFrame | None]:
    """The season's (pbp, possessions, game lineups, game order, college release) in the engine's shape.

    The NBA and WNBA read their releases (regular season only; the lineups only for the
    fixture, so ``None`` here); the WNBA's games are ordered by their schedule date. College
    converts its release with ``ncaa_epv_inputs`` and orders games by date.
    """
    if league_id in _COLLEGE:
        release = _college_pbp(league_id, season, data_root)
        poss, pbp, tens, games = ncaa_epv_inputs(release)
        return pbp, poss, tens, games.select("game_id", "game_order"), release
    pbp, poss = load_season(league_id, season, data_root)
    return pbp, poss, None, (_wnba_order(season, data_root) if league_id == "10" else None), None


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


def fit(train: dict[int, pl.DataFrame], level: dict[str, float] | None = None) -> xgb.Booster:
    """Early-stop on the last training season, refit on all of them, stamp ``start_epv_mean`` (and the level)."""
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
    if level is not None:
        # the training seasons' points per possession, as the labels count them
        booster.set_attr(
            level_ppp=repr(float(every.filter(pl.col("step") == 0)["remaining_points"].mean())),
            level_k=repr(float(level["level_k"])),
        )
    return booster


def _with_clock(states: pl.DataFrame) -> pl.DataFrame:
    s = pl.col("seconds_remaining")
    return states.with_columns(
        pl.when(s <= _CLOCK_BANDS[0]).then(0).when(s <= _CLOCK_BANDS[1]).then(1).otherwise(2).alias("_clock")
    )


def cell_means(train: pl.DataFrame) -> pl.DataFrame:
    return _with_clock(train).group_by(_CELL).agg(pl.col("remaining_points").mean().alias("_cell"))


def _scored(
    states: pl.DataFrame,
    booster: xgb.Booster,
    cells: pl.DataFrame,
    const: float,
    order: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """The states with ``epv``, the baseline ``_cell`` and both squared errors.

    A booster with a level has ``epv`` scaled by its game's ``level_scale``, as ``add_epv`` serves
    it, and so has the baseline, so gate 1 measures the states' skill, not the level's.
    ``_epv_raw`` keeps the unscaled value.
    """
    pred = np.asarray(booster.inplace_predict(_x(states)), dtype=np.float64)
    y = pl.col("remaining_points").cast(pl.Float64)
    out = _with_clock(states).with_columns(pl.Series("epv", pred), pl.Series("_epv_raw", pred))
    out = out.join(cells, on=_CELL, how="left").with_columns(pl.col("_cell").fill_null(const))
    if booster.attr("level_ppp") is not None:
        out = out.join(epv_level(states, booster, order), on="game_id", how="left").with_columns(
            pl.col("epv") * pl.col("level_scale"), pl.col("_cell") * pl.col("level_scale")
        )
    return out.with_columns(
        (pl.col("epv") - y).alias("_err"),
        ((pl.col("epv") - y) ** 2).alias("_se"),
        ((pl.col("_cell") - y) ** 2).alias("_se_cell"),
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
            (pl.col("_epv_raw") - pl.col("remaining_points")).mean().alias("bias_without_level"),
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
        "worst_bucket": {k: worst[k] for k in (*_BUCKET, "n", "bias", "allowed", "bias_without_level")},
        "ungated_max_bias": float(b.filter(pl.col("n") < MIN_BUCKET)["bias"].abs().max() or 0.0),
    }


def _game_se(s: pl.DataFrame) -> float:
    """The standard error of the level bias with games as clusters (a ratio estimator)."""
    g = s.group_by("game_id").agg(pl.col("_err").sum().alias("e"), pl.len().alias("n"))
    e, n = g["e"].to_numpy(), g["n"].to_numpy().astype(np.float64)
    bias = e.sum() / n.sum()
    return float(np.sqrt(np.sum((e - bias * n) ** 2) / (len(n) * (len(n) - 1))) / n.mean())


def _by_month(s: pl.DataFrame, order: pl.DataFrame | None) -> list[dict[str, Any]]:
    """Level bias by month of the season, with and without the level (college: games are dated)."""
    if order is None:
        return []
    m = s.join(order, on="game_id", how="left").with_columns(pl.col("game_order").dt.strftime("%Y-%m").alias("month"))
    t = m.group_by("month").agg(
        pl.col("game_id").n_unique().alias("games"),
        pl.col("_err").mean().alias("bias"),
        (pl.col("_epv_raw") - pl.col("remaining_points")).mean().alias("bias_without_level"),
    )
    return t.sort("month").to_dicts()


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
    order: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """The gate metrics of ``booster`` on one season's states (see the module docstring)."""
    s = _scored(states, booster, cells, const, order)
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
        "large_bias_without_level": float(s["_epv_raw"].mean() - y.mean()),
        "large_bias_game_se": _game_se(s),
        "calibration_slope": float(np.polyfit(p, y, 1)[0]),
        **_buckets(s),
        "by_month": _by_month(s, order),
        "deciles": _deciles(s),
    }


def gate(name: str, m: dict[str, Any], max_bias: float = MAX_LARGE_BIAS) -> list[str]:
    """The failed gates of one season's metrics (``max_bias``: gate 2's threshold for the league)."""
    out = []
    if not m["paired_vs_cell_mean"]["ci_hi"] < 0:
        out.append(f"{name}: paired mse vs the cell-mean baseline not below 0: {m['paired_vs_cell_mean']}")
    if abs(m["large_bias"]) > max_bias:
        out.append(f"{name}: calibration-in-the-large bias {m['large_bias']:+.4f} over {max_bias:.4f}")
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
    order: pl.DataFrame | None = None,
) -> None:
    """The first ``fixture_games`` holdout games (by ``game_id``) for the offline tests (and their dates)."""
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
        *((("schedule", order, ["game_id", "game_order"]),) if order is not None else ()),
    ):
        out = frame.with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi").select(cols)
        out.write_parquet(
            FIXTURE_DIR / f"{lg}_{name}_{season}.parquet",
            compression="zstd",
            compression_level=19,
        )


def write_college_fixture(league_id: str, season: int, pbp: pl.DataFrame) -> None:
    """The first ``fixture_games`` holdout games (by ``contest_id``) of the college release, the columns
    ``ncaa_epv_inputs`` reads."""
    from sportsdataverse.mbb.mbb_epv import _PBP_COLS

    n = LEAGUES[league_id]["fixture_games"]
    games = pbp.select(pl.col("contest_id").cast(pl.Utf8)).unique().sort("contest_id").head(n)
    out = pbp.with_columns(pl.col("contest_id").cast(pl.Utf8)).filter(
        pl.col("contest_id").is_in(games["contest_id"].implode())
    )
    out.select(_PBP_COLS).write_parquet(
        FIXTURE_DIR / f"{league_id}_pbp_{season}.parquet", compression="zstd", compression_level=19
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


def _identity(states: pl.DataFrame, booster: xgb.Booster, order: pl.DataFrame | None) -> float:
    """Over a season: |EPV added summed over a possession - (points - start + after-end)|, at most."""
    plays = add_epv(states, booster=booster, game_order=order)
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
    raw = {s: load_inputs(league_id, s, data_root) for s in split}
    states = {s: epv_states(poss, pbp) for s, (pbp, poss, *_) in raw.items()}
    order = {s: r[3] for s, r in raw.items()}
    tr = {s: states[s] for s in cfg["train"]}
    level = level_k(cfg, league_id, data_root) if cfg["level"] else None
    gate2 = max_large_bias(cfg, league_id, data_root)
    booster = fit(tr, level)
    booster.set_attr(
        train_seasons=",".join(map(str, cfg["train"])),
        holdout_season=str(cfg["holdout"]),
        confirmation_season=str(cfg["confirmation"]),
        fitting_script="tools/models/nba_epv/train_epv.py",
    )
    every = pl.concat(tr.values())
    cells, const = cell_means(every), float(every["remaining_points"].mean())
    start_const = float(every.filter(pl.col("step") == 0)["remaining_points"].mean())
    scored = {s: metrics(states[s], booster, cells, const, start_const, order[s]) for s in split if split[s] != "train"}
    report: dict[str, Any] = {
        "league_id": league_id,
        "splits": {str(s): v for s, v in split.items()},
        "n_train_states": every.height,
        "rounds": int(booster.attr("rounds")),
        "start_epv_mean": float(booster.attr("start_epv_mean")),
        "level": None if level is None else {**level, "level_ppp": float(booster.attr("level_ppp"))},
        "params": _XGB_PARAMS,
        "features": EPV_FEATURES,
        "gates": {
            "max_large_bias": gate2["max_large_bias"],
            "max_large_bias_rule": gate2,
            "max_bucket_bias": MAX_BUCKET_BIAS,
            "min_bucket": MIN_BUCKET,
            "min_gated_share": MIN_GATED_SHARE,
            "slope_band": SLOPE_BAND,
        },
        "seasons": {str(s): {"split": split[s], **m} for s, m in scored.items()},
        "holdout_identity_max_residual": _identity(states[cfg["holdout"]], booster, order[cfg["holdout"]]),
    }
    failed = [f for s, m in scored.items() for f in gate(f"{split[s]} {s}", m, gate2["max_large_bias"])]
    report["failed_gates"] = failed
    (HERE / f"epv_{cfg['lg']}_holdout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    brief = {
        s: {k: m[k] for k in ("mse", "mse_cell_mean", "large_bias", "calibration_slope")} for s, m in scored.items()
    }
    print(json.dumps({"rounds": report["rounds"], "seasons": brief, "failed": failed}, indent=2))
    if failed:
        raise SystemExit("gates failed:\n" + "\n".join(failed))
    booster.save_model(str(cfg["models"] / cfg["artifact"]))
    write_lineage(cfg, states, booster)
    if league_id in _COLLEGE:
        write_college_fixture(league_id, cfg["holdout"], raw[cfg["holdout"]][4])  # the release the gates scored
    else:
        pbp, poss, _, hold_order, _ = raw[cfg["holdout"]]
        tens = load_lineups(league_id, cfg["holdout"], data_root)
        write_fixture(league_id, cfg["holdout"], pbp, poss, tens, hold_order)
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-id", default="00", choices=sorted(LEAGUES))
    ap.add_argument(
        "--data-root",
        type=Path,
        help="a producer checkout's data dir (holding <lg>_stats/, or mbb/ / wbb/ for college)",
    )
    args = ap.parse_args()
    train(args.league_id, args.data_root)
