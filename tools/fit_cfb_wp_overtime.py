# tools/fit_cfb_wp_overtime.py
"""Fit the overtime correction to the CFB win-probability boosters.

The regulation boosters (``wp_spread`` / ``wp_naive``) were trained on a frame that
drops every game that reached overtime (cfbfastR-cfb-data
``model_training/ingest.py::clean_plays``, ``max_per > 4``). They therefore estimate
P(win | state, game settled in regulation): a tied game with 20 seconds left is
learned only from games somebody won in regulation, and an overtime state is pure
extrapolation. This script fits the three pieces that turn that into P(win | state):

* ``wp_ot_reach.ubj`` -- P(game reaches overtime | regulation state), an XGBoost
  binary model on the regulation rows of EVERY game (label: the game went to OT).
  Scorers combine ``(1 - q) * wp_regulation + q * tie_value``.
* ``overtime.spread_slope`` -- P(win | overtime reached) = sigmoid(slope * spread).
* ``overtime.drive_model`` -- P(touchdown / field goal / no score) for the current
  overtime possession, a multinomial logistic on (yards to goal, down, distance),
  which :mod:`sportsdataverse.cfb.cfb_wp_overtime` rolls through the overtime rules.

Seasons: fit on 2004-2021, early stopping on 2019-2021 held out of a 2004-2018 fit,
report on 2022-2025 (disjoint). Everything is written to the model card, which the
runtime reads; no constant is restated in code.

Usage::

    uv run python -m tools.fit_cfb_wp_overtime --pbp-root /mnt/sdv_repos/cfbfastR-cfb-data/cfb \
        --report /tmp/wp_overtime_report.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import numpy as np
import polars as pl
import xgboost as xgb
from scipy.optimize import minimize_scalar
from sklearn.linear_model import LogisticRegression

from sportsdataverse.cfb.model_vars import wp_final_names, wp_naive_final_names, wp_start_columns

MODEL_DIR = Path("sportsdataverse/cfb/models")
TRAIN = (2004, 2021)
EARLY_STOP_FIT, EARLY_STOP_VALID = (2004, 2018), (2019, 2021)
HOLDOUT = (2022, 2025)

#: WP-booster feature name -> published pbp column. The q model reads a subset of the
#: WP feature frame so the scorers can evaluate it on the frame they already built.
Q_SOURCES = {
    "adj_TimeSecsRem": "start.adj_TimeSecsRem",
    "pos_score_diff_start": "pos_score_diff_start",
    "down": "start.down",
    "distance": "start.distance",
    "yards_to_goal": "start.yardsToEndzone",
    "pos_team_timeouts_rem_before": "start.posTeamTimeouts",
    "def_pos_team_timeouts_rem_before": "start.defPosTeamTimeouts",
    "period": "period",
}
Q_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "eta": 0.05,
    "max_depth": 5,
    "min_child_weight": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.9,
    "tree_method": "hist",
    "seed": 0,
}
#: Drive-outcome classes, in the order the card stores the coefficient rows.
DRIVE_CLASSES = ["touchdown", "field_goal", "no_score"]
#: Try conversion rates: the rates the try board in cfb_pbp.__process_wpa pins.
P_XP, P_2PT = 0.92, 0.46


def drive_features(ytg, down, distance) -> np.ndarray:
    """The drive model's design matrix; mirrored by cfb_wp_overtime._drive_features."""
    ytg = np.asarray(ytg, dtype=float) / 25.0
    down = np.asarray(down, dtype=float)
    ld = np.log1p(np.asarray(distance, dtype=float))
    return np.column_stack([ytg, ytg**2, down == 2, down == 3, down == 4, ld, (down == 4) * ld]).astype(float)


def load_states(root: Path, seasons: range) -> pl.DataFrame:
    cols = [
        *Q_SOURCES.values(),
        *wp_start_columns,
        "season",
        "game_id",
        "game_play_number",
        "type.text",
        "start.pos_team.id",
        "homeTeamId",
        "start.pos_team_spread",
        "scrimmage_play",
        "wp_before",
        "wp_before_naive",
    ]
    out = []
    for s in seasons:
        f = pl.read_parquet(root / f"pbp/parquet/play_by_play_{s}.parquet", columns=list(dict.fromkeys(cols)))
        periods = f.group_by("game_id").agg(maxp=pl.col("period").max(), minp=pl.col("period").min())
        sched = (
            pl.read_parquet(
                root / f"schedules/parquet/cfb_schedule_{s}.parquet", columns=["game_id", "home_winner", "status"]
            )
            .filter(pl.col("status") == "STATUS_FINAL")
            .select(pl.col("game_id").cast(pl.Int64), pl.col("home_winner").cast(pl.Boolean))
            .unique("game_id")
        )
        f = (
            f.join(periods, on="game_id")
            .filter(pl.col("minp") >= 1)
            .join(sched, on="game_id")
            .sort("game_id", "game_play_number")
            .with_columns(
                reached_ot=pl.col("maxp") > 4,
                won=pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("home_winner"))
                .otherwise(~pl.col("home_winner")),
                first_team=pl.col("start.pos_team.id")
                .filter(pl.col("start.down").is_between(1, 4))
                .first()
                .over("game_id", "period"),
                drive_td=pl.col("type.text")
                .str.contains("(?i)touchdown")
                .any()
                .over("game_id", "period", "start.pos_team.id"),
                drive_fg=(pl.col("type.text") == "Field Goal Good")
                .any()
                .over("game_id", "period", "start.pos_team.id"),
            )
            .with_columns(ot_second=(pl.col("period") >= 5) & (pl.col("start.pos_team.id") != pl.col("first_team")))
            .filter(pl.col("start.down").is_between(1, 4) & pl.col("won").is_not_null())
        )
        out.append(f)
    return pl.concat(out, how="diagonal_relaxed").with_columns(
        pl.col(c).cast(pl.Float64)
        for c in dict.fromkeys(
            [*Q_SOURCES.values(), *wp_start_columns, "start.pos_team_spread", "wp_before", "wp_before_naive"]
        )
    )


def between(f: pl.DataFrame, span) -> pl.DataFrame:
    return f.filter(pl.col("season").is_between(*span))


def q_matrix(f: pl.DataFrame, label: bool = True) -> xgb.DMatrix:
    x = f.select([pl.col(src).alias(name) for name, src in Q_SOURCES.items()]).to_pandas()
    return xgb.DMatrix(x, label=f["reached_ot"].cast(pl.Float64).to_numpy() if label else None)


def scores(p: np.ndarray, y: np.ndarray) -> dict:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return {
        "n": int(len(y)),
        "brier": round(float(np.mean((p - y) ** 2)), 5),
        "logloss": round(float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))), 5),
    }


def calibration_table(f: pl.DataFrame, before: str, after: str) -> pl.DataFrame:
    return (
        f.with_columns(
            t=pl.when(pl.col("period") >= 5)
            .then(pl.lit("OT"))
            .when(pl.col("period") < 4)
            .then(pl.lit("Q1-3"))
            .otherwise(
                pl.col("start.adj_TimeSecsRem")
                .cut(
                    [30, 60, 120, 300, 600],
                    labels=["Q4 0-30s", "Q4 30-60s", "Q4 1-2m", "Q4 2-5m", "Q4 5-10m", "Q4 10-15m"],
                )
                .cast(pl.Utf8)
            ),
            d=pl.col("pos_score_diff_start")
            .clip(-9, 9)
            .cut([-9, -4, -1, 0, 3, 8], labels=["<=-9", "-8..-4", "-3..-1", "0", "1..3", "4..8", ">=9"])
            .cast(pl.Utf8),
            y=pl.col("won").cast(pl.Float64),
        )
        .group_by("t", "d")
        .agg(
            n=pl.len(),
            won=pl.col("y").mean(),
            before=pl.col(before).mean(),
            after=pl.col(after).mean(),
        )
        .with_columns(gap_before=pl.col("before") - pl.col("won"), gap_after=pl.col("after") - pl.col("won"))
        .sort("t", "d")
    )


def fit(args: argparse.Namespace, regulation: pl.DataFrame, overtime: pl.DataFrame) -> None:
    """Fit the three pieces on 2004-2021 and write the booster + card (metrics come after)."""
    # --- tie value: P(win | OT reached) = sigmoid(slope * pos_team_spread), one row per OT game -----
    games = (
        regulation.filter(pl.col("reached_ot") & pl.col("start.pos_team_spread").is_not_null())
        .group_by("game_id")
        .agg(pl.col("season").first(), pl.col("start.pos_team_spread").first().alias("sp"), pl.col("won").first())
    )

    def nll(b: float, f: pl.DataFrame) -> float:
        p = np.clip(1 / (1 + np.exp(-b * f["sp"].to_numpy())), 1e-6, 1 - 1e-6)
        y = f["won"].cast(pl.Float64).to_numpy()
        return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))

    slope = float(minimize_scalar(lambda b: nll(b, between(games, TRAIN)), bounds=(0, 0.5), method="bounded").x)

    # --- q: P(reach OT | regulation state) ------------------------------------------------------
    params = {**Q_PARAMS, "nthread": args.nthread}
    probe = xgb.train(
        params,
        q_matrix(between(regulation, EARLY_STOP_FIT)),
        num_boost_round=3000,
        evals=[(q_matrix(between(regulation, EARLY_STOP_VALID)), "valid")],
        early_stopping_rounds=50,
        verbose_eval=False,
    )
    rounds = probe.best_iteration + 1
    q_model = xgb.train(params, q_matrix(between(regulation, TRAIN)), num_boost_round=rounds)

    # --- OT drive outcome: multinomial logistic, C fixed a priori (no tuning on the holdout) ------
    ot_train = between(overtime, TRAIN)
    label = np.where(ot_train["drive_td"].to_numpy(), 0, np.where(ot_train["drive_fg"].to_numpy(), 1, 2))
    drive = LogisticRegression(C=1.0, max_iter=5000).fit(
        drive_features(ot_train["start.yardsToEndzone"], ot_train["start.down"], ot_train["start.distance"]), label
    )

    args.out.mkdir(parents=True, exist_ok=True)
    q_model.save_model(str(args.out / "wp_ot_reach.ubj"))
    card = {
        "model_type": "wp_ot_reach",
        "xgboost_version": xgb.__version__,
        "objective": "binary:logistic",
        "features": list(Q_SOURCES),
        "n_features": len(Q_SOURCES),
        "label": "game reached overtime",
        "training_seasons": list(TRAIN),
        "early_stopping": {"fit_seasons": list(EARLY_STOP_FIT), "valid_seasons": list(EARLY_STOP_VALID)},
        "holdout_seasons": list(HOLDOUT),
        "n_training_rows": between(regulation, TRAIN).height,
        "hyperparameters": Q_PARAMS,
        "num_boost_round": rounds,
        "source": "cfbfastR-cfb-data cfb/pbp/parquet/play_by_play_{season}.parquet (published)",
        "trained_date": dt.date.today().isoformat(),
        "fitting_script": "tools/fit_cfb_wp_overtime.py",
        "overtime": {
            "spread_slope": round(slope, 6),
            "spread_slope_training_games": between(games, TRAIN).height,
            "p_extra_point": P_XP,
            "p_two_point": P_2PT,
            "drive_model": {
                "classes": DRIVE_CLASSES,
                "features": [
                    "ytg/25",
                    "(ytg/25)^2",
                    "down==2",
                    "down==3",
                    "down==4",
                    "log1p(distance)",
                    "(down==4)*log1p(distance)",
                ],
                "coef": np.round(drive.coef_, 6).tolist(),
                "intercept": np.round(drive.intercept_, 6).tolist(),
                "training_rows": ot_train.height,
            },
        },
    }
    (args.out / "wp_ot_reach.card.json").write_text(json.dumps(card, indent=2) + "\n", encoding="utf-8")


def evaluate(args: argparse.Namespace, regulation: pl.DataFrame, overtime: pl.DataFrame) -> None:
    """Score the 2022-25 holdout with the SHIPPED runtime, and write the metrics into the card.

    Imported only now: the runtime reads the card and booster this script just wrote, so the
    gate measures the code that ships, not a copy of its algebra.
    """
    from sportsdataverse.cfb import cfb_wp_overtime as rt

    hold = between(regulation, HOLDOUT).filter((pl.col("scrimmage_play") == True) & pl.col("wp_before").is_not_null())  # noqa: E712
    X = hold.select([pl.col(src).alias(name) for src, name in zip(wp_start_columns, wp_final_names)])
    X_naive = X.select(wp_naive_final_names)
    hold = hold.with_columns(
        wp_after_fix=pl.Series(rt.adjust_wp(hold["wp_before"].to_numpy(), X)),
        wp_naive_after_fix=pl.Series(rt.adjust_wp(hold["wp_before_naive"].to_numpy(), X_naive)),
        q=pl.Series(rt.reach_overtime_prob(X.to_pandas())),
    )
    oth = between(overtime, HOLDOUT)
    oth = oth.with_columns(
        wp_after_fix=pl.Series(
            rt.ot_live_wp(
                oth["pos_score_diff_start"].to_numpy(),
                oth["start.down"].to_numpy(),
                oth["start.distance"].to_numpy(),
                oth["start.yardsToEndzone"].to_numpy(),
                rt.tie_value(oth["start.pos_team_spread"].to_numpy()),
                oth["ot_second"].to_numpy(),
            )
        )
    )

    y_reg = hold["won"].cast(pl.Float64).to_numpy()
    q4 = (hold["period"] == 4).to_numpy()
    tied_late = q4 & (hold["pos_score_diff_start"] == 0).to_numpy() & (hold["start.adj_TimeSecsRem"] <= 120).to_numpy()
    y_ot = oth["won"].cast(pl.Float64).to_numpy()
    before, after = hold["wp_before"].to_numpy(), hold["wp_after_fix"].to_numpy()
    metrics = {
        "regulation": {
            "before": scores(before, y_reg),
            "after": scores(after, y_reg),
            "naive_before": scores(hold["wp_before_naive"].to_numpy(), y_reg),
            "naive_after": scores(hold["wp_naive_after_fix"].to_numpy(), y_reg),
        },
        "q4": {"before": scores(before[q4], y_reg[q4]), "after": scores(after[q4], y_reg[q4])},
        "tied_last_2m": {
            "before": scores(before[tied_late], y_reg[tied_late]),
            "after": scores(after[tied_late], y_reg[tied_late]),
            "won": round(float(y_reg[tied_late].mean()), 4),
            "mean_before": round(float(before[tied_late].mean()), 4),
            "mean_after": round(float(after[tied_late].mean()), 4),
        },
        "overtime": {
            "before": scores(oth["wp_before"].to_numpy(), y_ot),
            "after": scores(oth["wp_after_fix"].to_numpy(), y_ot),
            "flat_half": scores(np.full(len(y_ot), 0.5), y_ot),
        },
        "q_logloss_holdout": scores(hold["q"].to_numpy(), hold["reached_ot"].cast(pl.Float64).to_numpy())["logloss"],
    }
    card_path = args.out / "wp_ot_reach.card.json"
    card = json.loads(card_path.read_text(encoding="utf-8"))
    card["holdout_metrics"] = metrics
    card_path.write_text(json.dumps(card, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metrics, indent=2))

    if args.report:
        # Overtime labels are noisy per play (ESPN possession errors), so also score whole
        # possessions: the first snap of each, against the game's result.
        poss = (
            oth.sort("game_id", "game_play_number")
            .group_by("game_id", "period", "start.pos_team.id", maintain_order=True)
            .agg(
                pl.col("ot_second").first(),
                pl.col("pos_score_diff_start").first(),
                pl.col("won").first(),
                pl.col("wp_before").first(),
                pl.col("wp_after_fix").first(),
            )
            .group_by(pl.col("ot_second").alias("second"), pl.col("pos_score_diff_start").alias("margin"))
            .agg(
                n=pl.len(),
                won=pl.col("won").cast(pl.Float64).mean(),
                before=pl.col("wp_before").mean(),
                after=pl.col("wp_after_fix").mean(),
            )
            .filter(pl.col("n") >= 10)
            .sort("second", "margin")
        )
        reg_tab = calibration_table(hold.filter(pl.col("period") == 4), "wp_before", "wp_after_fix")
        ot_tab = calibration_table(oth, "wp_before", "wp_after_fix")
        with pl.Config(tbl_rows=200, tbl_formatting="MARKDOWN", tbl_hide_dataframe_shape=True, float_precision=3):
            args.report.write_text(
                f"# CFB WP overtime correction: holdout {HOLDOUT[0]}-{HOLDOUT[1]}\n\n"
                f"```json\n{json.dumps(metrics, indent=2)}\n```\n\n## Q4 (scrimmage plays)\n\n{reg_tab}\n\n"
                f"## Overtime (scrimmage plays, possession order known)\n\n{ot_tab}\n\n"
                f"## Overtime possessions (first snap), by possession order and margin\n\n{poss}\n",
                encoding="utf-8",
            )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--pbp-root", type=Path, default=Path("/mnt/sdv_repos/cfbfastR-cfb-data/cfb"))
    ap.add_argument("--out", type=Path, default=MODEL_DIR)
    ap.add_argument("--report", type=Path, default=None, help="write the calibration tables here (markdown)")
    ap.add_argument("--nthread", type=int, default=8)
    ap.add_argument("--skip-fit", action="store_true", help="re-score the holdout with the committed artifacts")
    args = ap.parse_args()

    states = load_states(args.pbp_root, range(TRAIN[0], HOLDOUT[1] + 1))
    regulation = states.filter(pl.col("period") <= 4)
    overtime = states.filter(
        (pl.col("period") >= 5)
        & pl.col("start.yardsToEndzone").is_between(1, 99)
        & pl.col("start.distance").is_between(1, 99)
    )
    if not args.skip_fit:
        fit(args, regulation, overtime)
    evaluate(args, regulation, overtime)


if __name__ == "__main__":
    main()
