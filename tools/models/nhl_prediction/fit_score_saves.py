"""Fit and validate the regulation score model and the goalie saves interval for LEAGUE_CONSTANTS["nhl"].

Regulation score (``reg_draw_inflation``): walks 2022-23 .. 2025-26 with strict as-of ratings exactly as
``fit_pregame.py`` does, feeding each day's slate through ``nhl_predict_games``. Outcomes come from
``load_nhl_pbp_full``'s GOAL events by ``period_type``: REGULAR goals give the 60-minute score, and a game
level after regulation adds one goal for the OVERTIME scorer or the shootout winner (the pbp logs every
shootout attempt as a GOAL, so counting them all would overstate the final total). The shipped draw
inflation is the 3-way log-loss MLE on all four seasons. Each of 2023-24 .. 2025-26 is evaluated leave one
season out: scored with a draw inflation and flat outcome rates fitted on the other three seasons.

Goalie saves: ``load_nhl_goalie_boxscores`` publishes 2023-24 onward, so the saves constants are fitted on
2023-24 and 2024-25 / 2025-26 are the holdout. Regular-season starters only.

Run once (downloads the four seasons' pbp + schedules and three seasons of goalie boxes):

    uv run python tools/models/nhl_prediction/fit_score_saves.py

Prints the fitted values -- paste them into ``sportsdataverse/nhl/nhl_prediction_constants.py`` -- and the
holdout metrics, and rewrites the two holdout fixtures under ``tests/fixtures/nhl_prediction/``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import numpy as np
import polars as pl
from fit_pregame import build_game_rates
from scipy.optimize import minimize_scalar
from scipy.stats import nbinom

import sportsdataverse.nhl.nhl_prediction_constants as const_mod
from sportsdataverse.nhl.nhl_loaders import load_nhl_goalie_boxscores, load_nhl_pbp_full
from sportsdataverse.nhl.nhl_market import _with_regulation_outcomes, nhl_predict_games
from sportsdataverse.nhl.nhl_player_props import nhl_goalie_saves
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, get_constants
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "nhl_prediction"
SCORE_SEASONS = (2023, 2024, 2025, 2026)
SCORE_FIT = 2023
SAVES_SEASONS = (2024, 2025, 2026)
SAVES_FIT = 20232024


def set_constants(**changes: float) -> None:
    """Swap fields of LEAGUE_CONSTANTS["nhl"] in place (this script only)."""
    const_mod.LEAGUE_CONSTANTS["nhl"] = dataclasses.replace(const_mod.LEAGUE_CONSTANTS["nhl"], **changes)


def game_outcomes(season: int) -> pl.DataFrame:
    """Regulation and final score per game from the pbp's GOAL events."""
    pbp = load_nhl_pbp_full([season])
    goals = pbp.filter((pl.col("event_type") == "GOAL") & pl.col("game_id").is_not_null()).with_columns(
        pl.col("game_id").cast(pl.Int64), (pl.col("event_team_abbr") == pl.col("home_abbr")).alias("home")
    )

    def count(period_type: str, home: bool) -> pl.Expr:
        return ((pl.col("period_type") == period_type) & (pl.col("home") == home)).sum()

    out = goals.group_by("game_id").agg(
        count("REGULAR", True).alias("reg_home"),
        count("REGULAR", False).alias("reg_away"),
        count("OVERTIME", True).alias("ot_home"),
        count("OVERTIME", False).alias("ot_away"),
        count("SHOOTOUT", True).alias("so_home"),
        count("SHOOTOUT", False).alias("so_away"),
    )
    tied = pl.col("reg_home") == pl.col("reg_away")
    home_extra = tied & ((pl.col("ot_home") > pl.col("ot_away")) | (pl.col("so_home") > pl.col("so_away")))
    away_extra = tied & ((pl.col("ot_away") > pl.col("ot_home")) | (pl.col("so_away") > pl.col("so_home")))
    return out.with_columns(
        (pl.col("reg_home") + home_extra.cast(pl.Int64)).alias("final_home"),
        (pl.col("reg_away") + away_extra.cast(pl.Int64)).alias("final_away"),
    ).select("game_id", "reg_home", "reg_away", "final_home", "final_away")


def walk_season(season: int) -> pl.DataFrame:
    """As-of pregame predictions for every regular-season game after the first 20 dates."""
    const = get_constants("nhl")
    rates, sched = build_game_rates(season)
    dates = sorted(rates["date"].unique().to_list())
    parts = []
    for d in dates[20:]:
        adj = adjust_rate_opponent(
            as_of_ratings_split(rates, d),
            for_col="xgf",
            against_col="xga",
            hfa=const.hfa,
            avg=const.avg_xgf,
            shrink_k=const.shrink_k,
        ).select("team", pl.col("adj_for").alias("adj_xgf"), pl.col("adj_against").alias("adj_xga"))
        games = sched.filter(pl.col("date") == d).select(
            pl.col("game_id").cast(pl.Int64),
            pl.col("home_abbr").alias("home_team"),
            pl.col("away_abbr").alias("away_team"),
            "neutral_site",
        )
        known = adj["team"].implode()
        games = games.filter(pl.col("home_team").is_in(known) & pl.col("away_team").is_in(known))
        if games.height:
            parts.append(nhl_predict_games(games, adj).select("game_id", "home_win_prob", "exp_total"))
    preds = pl.concat(parts).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
    return preds.join(game_outcomes(season), on="game_id", how="inner")


def three_way(frame: pl.DataFrame, theta: float | None = None) -> np.ndarray:
    if theta is not None:
        set_constants(reg_draw_inflation=theta)
    out = _with_regulation_outcomes(frame, "nhl")
    return out.select("home_reg_win_prob", "reg_tie_prob", "away_reg_win_prob").to_numpy()


def outcome(frame: pl.DataFrame) -> np.ndarray:
    h, a = frame["reg_home"].to_numpy(), frame["reg_away"].to_numpy()
    return np.where(h > a, 0, np.where(h == a, 1, 2))


def log_loss(P: np.ndarray, y: np.ndarray) -> float:
    return float(-np.mean(np.log(P[np.arange(len(y)), y])))


def brier(P: np.ndarray, y: np.ndarray) -> float:
    return float(np.mean(((P - np.eye(3)[y]) ** 2).sum(axis=1)))


def fit_regulation() -> None:
    corpus = pl.concat([walk_season(s) for s in SCORE_SEASONS])
    theta = _theta_mle(corpus)
    print(f"reg_draw_inflation = {theta:.4f}  <- 3-way log-loss MLE on {SCORE_SEASONS} ({corpus.height} games)")
    print(f"  (reference) MLE on {SCORE_FIT} alone: {_theta_mle(corpus.filter(pl.col('season') == SCORE_FIT)):.4f}")
    # Leave one season out: each holdout season is scored with a theta and flat outcome rates fitted on the
    # other three seasons only, so no scored season is in its own fit.
    for season in SCORE_SEASONS[1:]:
        rest, d = corpus.filter(pl.col("season") != season), corpus.filter(pl.col("season") == season)
        loso, y = _theta_mle(rest), outcome(d)
        base = np.bincount(outcome(rest), minlength=3) / rest.height
        fitted, indep = three_way(d, loso), three_way(d, 0.0)
        flat = np.tile(base, (len(y), 1))
        print(
            f"  LOSO {season} n={len(y)}: theta {loso:.4f}, other-season rates {base.round(4).tolist()}; "
            f"log loss {log_loss(fitted, y):.4f} (independent Poisson {log_loss(indep, y):.4f}, "
            f"flat rates {log_loss(flat, y):.4f}); Brier {brier(fitted, y):.4f} ({brier(indep, y):.4f}, "
            f"{brier(flat, y):.4f}); tie pred/actual {fitted[:, 1].mean():.4f}/{(y == 1).mean():.4f}; "
            f"home-reg pred/actual {fitted[:, 0].mean():.4f}/{(y == 0).mean():.4f}"
        )
    set_constants(reg_draw_inflation=theta)
    corpus.filter(pl.col("season") != SCORE_FIT).sort("game_id").write_parquet(
        FIXTURES / "regulation_holdout_2024_2026.parquet"
    )


def _theta_mle(frame: pl.DataFrame) -> float:
    y = outcome(frame)
    return float(minimize_scalar(lambda t: log_loss(three_way(frame, t), y), bounds=(0.0, 2.0), method="bounded").x)


def _starter_actuals(box: pl.DataFrame) -> pl.DataFrame:
    return box.filter((pl.col("starter") == True) & (pl.col("game_id").cast(pl.Utf8).str.slice(4, 2) == "02")).select(  # noqa: E712
        pl.col("game_id").cast(pl.Int64), "player_id", "shots_against", "saves"
    )


def fit_saves() -> None:
    box = load_nhl_goalie_boxscores(list(SAVES_SEASONS))
    fit_box = box.filter(pl.col("season") == SAVES_FIT)
    team = fit_box.group_by("game_id", "home_away").agg(pl.col("shots_against").sum())
    avg_sa = float(team["shots_against"].mean())
    hfa = float(
        team.filter(pl.col("home_away") == "away")["shots_against"].mean()
        - team.filter(pl.col("home_away") == "home")["shots_against"].mean()
    )
    starters = _starter_actuals(fit_box)
    sv0 = float(starters["saves"].sum() / starters["shots_against"].sum())
    set_constants(avg_shots_against=avg_sa, shots_hfa=hfa, save_pct_prior=sv0, starter_shot_share=1.0)

    def projected(**changes: float) -> pl.DataFrame:
        set_constants(**changes)
        proj = nhl_goalie_saves([2024], load_goalie_boxscores=lambda _: fit_box)
        return proj.join(_starter_actuals(fit_box), on=["game_id", "player_id"])

    p = projected()
    share = float(p["shots_against"].sum() / p["exp_shots_against"].sum())

    def binom_nll(kappa: float) -> float:
        q = projected(save_pct_kappa=kappa)
        s, n, sv = q["saves"].to_numpy(), q["shots_against"].to_numpy(), q["exp_save_pct"].to_numpy()
        return float(-np.sum(s * np.log(sv) + (n - s) * np.log1p(-sv)))

    kappa = float(minimize_scalar(binom_nll, bounds=(10.0, 20000.0), method="bounded", options={"xatol": 1.0}).x)
    p = projected(starter_shot_share=share, save_pct_kappa=kappa)
    sa, mu = p["shots_against"].to_numpy(), p["exp_shots_against"].to_numpy()
    size = float(
        minimize_scalar(lambda r: -np.sum(nbinom.logpmf(sa, r, r / (r + mu))), bounds=(1.0, 2000.0), method="bounded").x
    )
    set_constants(shots_nb_size=size)
    print(f"avg_shots_against  = {avg_sa:.2f}  (2023-24 shots on goal per team-game)")
    print(f"shots_hfa          = {hfa:.3f}")
    print(f"starter_shot_share = {share:.4f}")
    print(f"save_pct_prior     = {sv0:.4f}")
    print(f"save_pct_kappa     = {kappa:.0f}")
    print(f"shots_nb_size      = {size:.2f}  (NB MLE of starters' shots against)")
    _report_saves(box)
    holdout = box.filter(pl.col("season") == 20252026).select(
        "game_id", "game_date", "team_abbrev", "home_away", "player_id", "starter", "shots_against", "saves"
    )
    holdout.sort("game_id", "player_id").write_parquet(FIXTURES / "goalie_box_2026.parquet")


def _report_saves(box: pl.DataFrame) -> None:
    for coverage in (0.5, 0.8, 0.9):
        proj = nhl_goalie_saves(list(SAVES_SEASONS), coverage=coverage, load_goalie_boxscores=lambda _: box)
        joined = proj.join(_starter_actuals(box), on=["game_id", "player_id"])
        for (season,), d in joined.group_by("season", maintain_order=True):
            y = d["saves"].to_numpy()
            inside = (y >= d["saves_lo"].to_numpy()) & (y <= d["saves_hi"].to_numpy())
            m = d["exp_saves"].to_numpy()
            r = get_constants("nhl").shots_nb_size
            implied = nbinom.cdf(d["saves_hi"].to_numpy(), r, r / (r + m)) - nbinom.cdf(
                d["saves_lo"].to_numpy() - 1, r, r / (r + m)
            )
            print(
                f"  {season} {int(coverage * 100)}% interval: covers {inside.mean():.4f} "
                f"(model {implied.mean():.4f}), n={len(y)}, mean saves pred/actual {m.mean():.2f}/{y.mean():.2f}, "
                f"MAE {np.abs(y - m).mean():.2f}"
            )


if __name__ == "__main__":
    fit_regulation()
    fit_saves()
