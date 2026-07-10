"""Train + bundle the in-game-WP model (model 4), per league_id (Phase 3, Task 3.2).

Gitignored working script (``dev/`` is not tracked). Run:

    SDV_PY_LIVE_TESTS=1 uv run python dev/nba_prediction/train_in_game_wp.py --league-id 00 --season 2023

Trains on a season NOT used by the 2023-24 gates (default 2022-23, int ``2023``)
so the Phase-3 calibration backtest stays out-of-sample. Downloads that season's
``load_nba_pbp`` (ESPN release, any IP -- no stats.nba.com), computes per-play
features (``score_diff``, ``sqrt_sec_left``, ``pregame_logit``, ``home_has_ball``)
where ``pregame_logit`` comes from each game's full-season-ratings pregame home
prob, labels every play with the game's realized ``home_win``, and fits a model.

ESCALATED to shallow xgboost (Task 3.2's sanctioned fallback): the plain sklearn
logistic failed the Task-3.4 per-bucket calibration gate (max bucket gap ~0.11 --
a 4-feature linear logit can't bend the low end of the curve). A depth-3 xgboost
(120 rounds) roughly halves the worst-bucket gap. The booster is written to the
committed per-league ``.ubj`` artifact (``nba_in_game_wp.ubj`` for NBA), mirroring
the MBB spine's ``.ubj`` in-game-WP artifact; the scorer detects ``.ubj`` vs
``.json`` by the ``in_game_wp_artifact`` filename in LEAGUE_CONSTANTS.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import polars as pl
import xgboost as xgb

from sportsdataverse.nba.nba_game_predict import _IN_GAME_FEATURES, in_game_features, nba_predict_games
from sportsdataverse.nba.nba_loaders import load_nba_pbp, load_nba_schedule
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

MODELS_DIR = Path(__file__).resolve().parents[2] / "sportsdataverse" / "nba" / "models"


def _league_pbp_schedule(league_id: str):  # noqa: ANN202
    """(load_pbp, load_schedule) for a league_id: NBA/G-League -> nba, WNBA -> wnba."""
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_pbp, load_wnba_schedule

        return load_wnba_pbp, load_wnba_schedule
    return load_nba_pbp, load_nba_schedule


_XGB_PARAMS = {
    "max_depth": 3,
    "eta": 0.1,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "subsample": 0.8,
    "seed": 42,
}
_XGB_ROUNDS = 120


def _cast_ids(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.with_columns(
        [pl.col(c).cast(pl.Int64, strict=False).cast(pl.Utf8).alias(c) for c in cols if c in df.columns]
    )


def train(*, league_id: str = "00", season: int = 2023, artifact: str = "nba_in_game_wp.ubj") -> None:
    load_pbp, load_schedule = _league_pbp_schedule(league_id)
    ratings = nba_team_ratings(season, league_id=league_id)  # full-season -> per-game pregame prob

    sched = load_schedule([season]).rename({"home_id": "home_team_id", "away_id": "away_team_id"})
    if "date" in sched.columns:
        sched = sched.drop("date")
    sched = sched.rename({"game_date": "date"})
    games = (
        sched.filter(pl.col("status_type_completed") == True)  # noqa: E712
        .select(
            pl.col("id").alias("game_id"),
            "home_team_id",
            "away_team_id",
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
            pl.col("neutral_site").cast(pl.Boolean),
        )
        .unique(subset=["game_id"])
    )
    games = _cast_ids(games, ["game_id", "home_team_id", "away_team_id"])
    preds = nba_predict_games(
        games.select("game_id", "home_team_id", "away_team_id", "neutral_site"), ratings, league_id=league_id
    )
    game_prob = (
        preds.join(games.select("game_id", "home_score", "away_score"), on="game_id")
        .with_columns(
            (pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("home_win"),
            pl.col("home_win_prob").fill_null(0.5).alias("pregame_home_prob"),
        )
        .select("game_id", "pregame_home_prob", "home_win")
    )

    pbp = load_pbp([season]).with_columns(pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8))

    feats_rows, labels = [], []
    for row in game_prob.iter_rows(named=True):
        g = pbp.filter(pl.col("game_id") == row["game_id"])
        if g.height == 0:
            continue
        f = in_game_features(g, float(row["pregame_home_prob"]))
        feats_rows.append(f.select(_IN_GAME_FEATURES).to_numpy())
        labels.append(np.full(f.height, row["home_win"], dtype=float))

    X = np.vstack(feats_rows)
    y = np.concatenate(labels)

    dtrain = xgb.DMatrix(X, label=y, feature_names=_IN_GAME_FEATURES)
    booster = xgb.train(_XGB_PARAMS, dtrain, num_boost_round=_XGB_ROUNDS)
    out = MODELS_DIR / artifact
    booster.save_model(str(out))

    in_sample_brier = float(np.mean((booster.inplace_predict(X) - y) ** 2))
    print(f"league_id={league_id} season={season} n_events={X.shape[0]} in_sample_brier={in_sample_brier:.4f}")
    print(f"wrote {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-id", default="00")
    ap.add_argument("--season", type=int, default=2023)
    ap.add_argument("--artifact", default="nba_in_game_wp.ubj")
    args = ap.parse_args()
    train(league_id=args.league_id, season=args.season, artifact=args.artifact)
