"""WBB prediction-stack oracle gates (Phase 7 parity, same thresholds as mbb).

Mirrors ``tests/mbb/test_mbb_prediction_backtest.py`` over the
``tests/fixtures/wbb_prediction`` corpus with ``league="womens"``. The two
observed-floor gates (spread / total MAE vs the closing line) carry women's
documented floors: WBB book coverage is thin (97 of 4,075 eligible games had
a usable close) and women's lines are noisier.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_bracketology import project_bracket
from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob, mbb_predict_games
from sportsdataverse.mbb.mbb_prediction_constants import (
    brier_score,
    calibration_table,
    mae,
    spearman_corr,
)
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, adjust_tempo, raw_game_efficiency
from tests.mbb.test_mbb_prediction_backtest import _MIN_PRIOR_GAMES
from pathlib import Path

FIX_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "wbb_prediction"

_FIXTURES = (
    "results_2024",
    "team_box_2024",
    "torvik_2024",
    "espn_predictor_sample",
    "espn_odds_sample",
    "espn_bpi_2024",
    "pbp_sample_2024",
    "ncaa_tourney_2024",
)


@pytest.fixture(scope="module")
def wbb_corpus() -> dict[str, pl.DataFrame]:
    corpus: dict[str, pl.DataFrame] = {}
    for name in _FIXTURES:
        path = FIX_DIR / f"{name}.parquet"
        if path.exists():
            corpus[name] = pl.read_parquet(path)
    return corpus


@pytest.fixture(scope="module")
def wbb_weekly_backtest(wbb_corpus) -> pl.DataFrame:
    """Weekly as-of pregame predictions for the WBB 2024 season."""
    results = wbb_corpus["results_2024"].with_columns(pl.col("date").dt.truncate("1w").alias("cutoff"))
    box = wbb_corpus["team_box_2024"]
    frames = []
    for (cutoff,), week in results.group_by("cutoff", maintain_order=True):
        prior = results.filter(pl.col("date") < cutoff)
        if prior.height < 300:
            continue
        eff = raw_game_efficiency(prior, box.filter(pl.col("game_date") < cutoff))
        counts = eff.group_by("team_id").agg(pl.len().alias("n"))
        ratings = (
            adjust_efficiency(eff, league="womens")
            .join(adjust_tempo(eff, league="womens"), on=["season", "team_id"])
            .select("team_id", "adj_o", "adj_d", "adj_em", "adj_tempo")
        )
        preds = mbb_predict_games(
            week.select("game_id", "home_team_id", "away_team_id", "neutral_site"), ratings, league="womens"
        )
        frames.append(
            preds.join(
                week.select(
                    "game_id",
                    "neutral_site",
                    (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("actual_margin"),
                    (pl.col("home_score") + pl.col("away_score")).cast(pl.Float64).alias("actual_total"),
                ),
                on="game_id",
            )
            .join(counts.rename({"team_id": "home_team_id", "n": "home_n"}), on="home_team_id", how="left")
            .join(counts.rename({"team_id": "away_team_id", "n": "away_n"}), on="away_team_id", how="left")
        )
    return pl.concat(frames).filter(
        (pl.col("home_n") >= _MIN_PRIOR_GAMES)
        & (pl.col("away_n") >= _MIN_PRIOR_GAMES)
        & pl.col("exp_margin").is_not_null()
    )


def test_wbb_corpus_complete(wbb_corpus):
    for name in _FIXTURES:
        assert name in wbb_corpus, f"missing wbb fixture {name}"
        assert wbb_corpus[name].height > 0


def test_wbb_ratings_vs_torvik(wbb_corpus):
    """Phase-1 gate (same threshold): Spearman >= 0.95. Observed: 0.9948."""
    adj = adjust_efficiency(
        raw_game_efficiency(wbb_corpus["results_2024"], wbb_corpus["team_box_2024"]), league="womens"
    )
    j = adj.join(wbb_corpus["torvik_2024"], on="team_id", how="inner")
    assert j.height >= 300
    rho = spearman_corr(j.get_column("adj_em").to_numpy(), j.get_column("adj_em_right").to_numpy())
    assert rho >= 0.95, f"wbb torvik spearman = {rho:.4f}"


def test_wbb_pregame_brier_vs_espn(wbb_weekly_backtest, wbb_corpus):
    """Phase-2 gate (same threshold): Brier <= ESPN + 0.01. Observed: 0.1613 vs 0.1570."""
    pred = wbb_corpus["espn_predictor_sample"]
    j = wbb_weekly_backtest.join(
        pred.select("game_id", pl.col("home_win_prob").alias("espn_p")), on="game_id", how="inner"
    )
    assert j.height >= 150
    y = (j.get_column("actual_margin").to_numpy() > 0).astype(float)
    b_ours = brier_score(y, j.get_column("home_win_prob").to_numpy())
    b_espn = brier_score(y, j.get_column("espn_p").to_numpy())
    assert b_ours <= b_espn + 0.01, f"wbb brier ours={b_ours:.5f} espn={b_espn:.5f}"


def test_wbb_pregame_spread_and_total_mae_vs_close(wbb_weekly_backtest, wbb_corpus):
    """Phase-2 observed-floor gates: spread MAE <= 4.0, total MAE <= 6.5.

    Women's floors documented from observed (spread 3.61, total 5.56; n=97 --
    WBB book coverage is thin and lines are noisier than mens, where the
    observed values were 1.95 / 2.90 on n=218).
    """
    j = wbb_weekly_backtest.join(wbb_corpus["espn_odds_sample"], on="game_id", how="inner")
    assert j.height >= 75, f"wbb odds intersection too small: {j.height}"
    m_spread = mae(j.get_column("exp_margin").to_numpy(), -j.get_column("close_spread_home").to_numpy())
    m_total = mae(j.get_column("exp_total").to_numpy(), j.get_column("close_total").to_numpy())
    assert m_spread <= 4.0, f"wbb spread MAE = {m_spread:.3f}"
    assert m_total <= 6.5, f"wbb total MAE = {m_total:.3f}"
    assert abs(float(j.get_column("exp_total").mean()) - float(j.get_column("close_total").mean())) <= 2.0


def test_wbb_in_game_wp_decile_calibration(wbb_corpus):
    """Phase-3 gate (same threshold): decile gap <= 0.03. Observed: 0.0224."""
    sample = wbb_corpus["pbp_sample_2024"]
    preds, obs = [], []
    for (_gid,), sub in sample.group_by("game_id", maintain_order=True):
        wp = mbb_in_game_win_prob(sub, float(sub["pregame_home_prob"][0]), league="womens")
        preds.append(wp.get_column("home_win_prob").to_numpy())
        obs.append(sub.get_column("home_win").to_numpy())
    p, y = np.concatenate(preds), np.concatenate(obs)
    tbl = calibration_table(y, p, n_bins=10)
    gaps = (tbl.get_column("mean_pred") - tbl.get_column("mean_actual")).abs()
    assert float(gaps.max()) <= 0.03, str(tbl.with_columns(gaps.alias("gap")))


def test_wbb_sos_vs_espn_bpi(wbb_corpus):
    """Phase-4 gate (same threshold): SoS Spearman >= 0.9. Observed: 0.9846."""
    adj = adjust_efficiency(
        raw_game_efficiency(wbb_corpus["results_2024"], wbb_corpus["team_box_2024"]), league="womens"
    ).with_columns(pl.col("adj_em").rank(method="min", descending=True).over("season").cast(pl.Int64).alias("rank"))
    sos = strength_of_schedule(wbb_corpus["results_2024"], adj, league="womens")
    j = sos.join(
        wbb_corpus["espn_bpi_2024"].select("team_id", pl.col("sos_rank").alias("bpi_sos_rank")),
        on="team_id",
        how="inner",
    )
    assert j.height >= 300
    rho = spearman_corr(j.get_column("sos").to_numpy(), -j.get_column("bpi_sos_rank").to_numpy())
    assert rho >= 0.9, f"wbb sos spearman = {rho:.4f}"


def test_wbb_bracketology_seed_order_vs_committee(wbb_corpus):
    """Phase-5 gate (same threshold): seed-order Spearman >= 0.9. Observed: 0.9756."""
    tourney = wbb_corpus["ncaa_tourney_2024"]
    cutoff = tourney.get_column("date").min()
    results = wbb_corpus["results_2024"].filter(pl.col("date") < cutoff)
    box = wbb_corpus["team_box_2024"].filter(pl.col("game_date") < cutoff)
    ratings = adjust_efficiency(raw_game_efficiency(results, box), league="womens").with_columns(
        pl.col("adj_em").rank(method="min", descending=True).over("season").cast(pl.Int64).alias("rank"),
        ((pl.col("adj_em") - pl.col("adj_em").mean().over("season")) / pl.col("adj_em").std().over("season")).alias(
            "adj_em_z"
        ),
    )
    resume = strength_of_schedule(results, ratings, league="womens").join(
        ratings.select("season", "team_id", "adj_em_z"), on=["season", "team_id"], how="inner"
    )
    field = project_bracket(resume, auto_bids=set(), league="womens")
    seeds = tourney.group_by("team_id").agg(pl.col("seed").min())
    j = field.join(seeds, on="team_id", how="inner")
    assert j.height == 68
    rho = spearman_corr(-j.get_column("resume_score").to_numpy(), j.get_column("seed").to_numpy())
    assert rho >= 0.9, f"wbb resume-vs-seed spearman = {rho:.4f}"


def test_wbb_neutral_site_calibration_slope(wbb_weekly_backtest):
    """Phase-6 gate (same threshold): slope in [0.9, 1.1]. Observed: 0.9165."""
    neutral = wbb_weekly_backtest.filter(pl.col("neutral_site") == True)  # noqa: E712
    assert neutral.height >= 300
    p = neutral.get_column("home_win_prob").to_numpy()
    y = (neutral.get_column("actual_margin").to_numpy() > 0).astype(float)
    slope = float(np.polyfit(p, y, 1)[0])
    assert 0.9 <= slope <= 1.1, f"wbb neutral-site slope = {slope:.4f}"
