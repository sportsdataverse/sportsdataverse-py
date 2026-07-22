"""Gates for the numpy-only random-intercept mixed-effects fitter.

Primary gates run on REAL data — player scoring rows from the committed v3
fixture games — with the out-of-sample comparison asserted at what the data
supports: partial pooling beats no pooling under leave-one-game-out, and in
this thin-panel/high-noise regime (1-2 games per player, sigma2 >> tau2)
heavy shrinkage toward the grand mean is the CORRECT behavior, so complete
pooling is allowed to win overall.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.mixed_effects import fit_random_intercepts, shrunk_group_means
from sportsdataverse.modeling.features import fit_pooled_intercepts
from sportsdataverse.nba.nba_possession_sim import player_game_logs_from_pbp


@pytest.fixture(scope="module")
def real_logs() -> pl.DataFrame:
    frames = []
    for gid in ("0022100001", "0022200001", "0022300001"):
        payload = json.loads(
            pathlib.Path(f"tests/fixtures/nba_engine/{gid}/playbyplayv3.json").read_text(encoding="utf-8")
        )
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return player_game_logs_from_pbp(pl.concat(frames, how="diagonal_relaxed"))


@pytest.fixture(scope="module")
def cross_league(real_logs: pl.DataFrame) -> pl.DataFrame:
    """Real per-player game points across three leagues (NBA v3 pbp logs +
    the committed ESPN college summary boxscores), league-namespaced keys.
    The college points come from a direct walk of the raw capture so the
    fixture stays portable (no ESPN parser-layer dependency)."""
    nba = real_logs.select(
        pl.lit("nba").alias("league"),
        ("nba_" + pl.col("player_id").cast(pl.Utf8)).alias("player_key"),
        pl.col("pts").cast(pl.Float64),
    )
    parts = [nba]
    # full literal paths on purpose: the engine sync vendors fixtures by
    # scanning path literals, and an f-string brace here globs to the
    # whole espn dir (61 MB) instead of these two captures
    college_captures = (
        ("tests/fixtures/espn/summary_mbb.json", "mbb"),
        ("tests/fixtures/espn/summary_wbb.json", "wbb"),
    )
    for capture_path, league in college_captures:
        payload = json.loads(pathlib.Path(capture_path).read_text(encoding="utf-8"))
        rows = []
        for team in payload["boxscore"]["players"]:
            for stat in team["statistics"]:
                idx = stat["keys"].index("points")
                for entry in stat["athletes"]:
                    stats = entry.get("stats") or []
                    if len(stats) <= idx:
                        continue
                    try:
                        pts = float(stats[idx])
                    except ValueError:
                        continue
                    rows.append(
                        {
                            "league": league,
                            "player_key": f"{league}_{entry['athlete']['id']}",
                            "pts": pts,
                        }
                    )
        parts.append(pl.DataFrame(rows, schema={"league": pl.Utf8, "player_key": pl.Utf8, "pts": pl.Float64}))
    return pl.concat(parts)


def test_fit_on_real_player_scoring(real_logs: pl.DataFrame) -> None:
    fit = fit_random_intercepts(real_logs, response="pts", group="player_id")
    assert fit.converged
    assert fit.tau2 > 0 and fit.sigma2 > 0
    assert fit.sigma2 > fit.tau2  # per-game scoring noise dominates this panel
    # every BLUP prediction sits between the raw group mean and the grand mean
    table = shrunk_group_means(real_logs, response="pts", group="player_id")
    for row in table.iter_rows(named=True):
        low, high = sorted((row["raw_mean"], fit.mu))
        assert low - 1e-9 <= row["shrunk_mean"] <= high + 1e-9, row
        assert 0.0 <= row["shrinkage"] < 1.0
    # unseen group falls back to the grand mean
    assert fit.predict("nope") == pytest.approx(fit.mu)
    assert fit.shrinkage("nope") == 0.0


def test_partial_pooling_beats_no_pooling_out_of_sample(real_logs: pl.DataFrame) -> None:
    """Leave-one-game-out on the real logs: shrunk < raw group means (MSE)."""
    sse = {"shrunk": 0.0, "raw": 0.0}
    for hold in real_logs["game_id"].unique().to_list():
        train = real_logs.filter(pl.col("game_id") != hold)
        test = real_logs.filter(pl.col("game_id") == hold)
        fit = fit_random_intercepts(train, response="pts", group="player_id")
        raw_means = {g: m for g, m in train.group_by("player_id").agg(pl.col("pts").mean()).iter_rows()}
        for pid, actual in test.select("player_id", "pts").iter_rows():
            sse["shrunk"] += (fit.predict(pid) - actual) ** 2
            sse["raw"] += (raw_means.get(pid, fit.mu) - actual) ** 2
    assert sse["shrunk"] < sse["raw"]


def test_estimator_limits() -> None:
    """Analytic invariants: no signal -> full pooling; clear signal -> little."""
    rng = np.random.default_rng(5)
    flat = pl.DataFrame({"g": [i % 8 for i in range(160)], "y": rng.normal(10.0, 3.0, 160)})
    fit_flat = fit_random_intercepts(flat, response="y", group="g")
    assert all(fit_flat.shrinkage(g) < 0.5 for g in fit_flat.counts)
    separated = pl.DataFrame(
        {"g": [i % 4 for i in range(200)], "y": [10.0 * (i % 4) + float(rng.normal(0, 0.1)) for i in range(200)]}
    )
    fit_sep = fit_random_intercepts(separated, response="y", group="g")
    assert all(fit_sep.shrinkage(g) > 0.99 for g in fit_sep.counts)
    for g in fit_sep.counts:
        raw = float(np.mean([10.0 * g]))
        assert fit_sep.predict(g) == pytest.approx(raw, abs=0.2)
    with pytest.raises(ValueError, match="non-null observation"):
        fit_random_intercepts(pl.DataFrame({"g": [], "y": []}), response="y", group="g")


def test_pooled_cross_league_fit_on_real_boxscores(cross_league: pl.DataFrame) -> None:
    fit = fit_pooled_intercepts(cross_league, response="pts", group="player_key", pool="league")
    assert fit.converged
    assert set(fit.pool_effects) == {"mbb", "nba", "wbb"}
    # league scoring environments separate as observed in the raw data
    assert fit.pool_effects["nba"] > fit.pool_effects["wbb"] > fit.pool_effects["mbb"]
    assert fit.tau2_pool > 0 and fit.tau2_group > 0 and fit.sigma2 > fit.tau2_group
    # every prediction sits between the raw mean and that league's line
    raw = cross_league.group_by("league", "player_key").agg(pl.col("pts").mean().alias("raw"))
    for row in raw.iter_rows(named=True):
        line = fit.mu + fit.pool_effects[row["league"]]
        low, high = sorted((line, row["raw"]))
        assert low - 1e-9 <= fit.predict(row["league"], row["player_key"]) <= high + 1e-9, row
    # shrinkage is monotone in sample count (this panel has n in {1, 2})
    by_n = {n: fit.shrinkage(g) for g, n in fit.group_counts.items()}
    assert 0.0 < by_n[1] < by_n[2] < 1.0
    # unseen entities fall back level by level
    assert fit.predict("nba", "nope") == pytest.approx(fit.mu + fit.pool_effects["nba"])
    assert fit.predict("xfl", "nope") == pytest.approx(fit.mu)
    assert fit.shrinkage("nope") == 0.0
    # deterministic refit
    again = fit_pooled_intercepts(cross_league, response="pts", group="player_key", pool="league")
    assert again.mu == fit.mu and again.group_effects == fit.group_effects


def test_pooled_reduces_to_the_one_way_fit(cross_league: pl.DataFrame) -> None:
    """Single pool level -> the statsmodels-anchored one-way fixed point."""
    nba = cross_league.filter(pl.col("league") == "nba")
    one = fit_random_intercepts(nba, response="pts", group="player_key")
    red = fit_pooled_intercepts(nba, response="pts", group="player_key", pool="league")
    assert red.converged
    assert abs(red.pool_effects["nba"]) < 1e-9  # the gauge sweep zeroes a lone pool
    for g in one.effects:
        assert red.predict("nba", g) == pytest.approx(one.predict(g), abs=1e-6)
    assert red.tau2_group == pytest.approx(one.tau2, abs=1e-6)
    assert red.sigma2 == pytest.approx(one.sigma2, abs=1e-6)


def test_pooled_crossed_samples_share_one_global_effect(cross_league: pl.DataFrame) -> None:
    """Relabel one real two-game player's second game into a second pool:
    both observations still inform ONE global effect (the call-up case)."""
    counts = cross_league.group_by("player_key").agg(pl.len().alias("n"))
    player = counts.filter(pl.col("n") == 2)["player_key"][0]
    marked = cross_league.with_row_index("i")
    second_row = marked.filter(pl.col("player_key") == player)["i"][1]
    relabeled = marked.with_columns(
        pl.when(pl.col("i") == second_row).then(pl.lit("nbax")).otherwise(pl.col("league")).alias("league")
    ).drop("i")
    fit = fit_pooled_intercepts(relabeled, response="pts", group="player_key", pool="league")
    assert fit.converged
    assert fit.group_counts[player] == 2  # pooled ACROSS the two leagues
    assert fit.shrinkage(player) > max(fit.shrinkage(g) for g, n in fit.group_counts.items() if n == 1)
    # one global b: the player's two league predictions differ by exactly
    # the league effects
    gap = fit.predict("nba", player) - fit.predict("nbax", player)
    assert gap == pytest.approx(fit.pool_effects["nba"] - fit.pool_effects["nbax"], abs=1e-12)


def test_pooled_validation_errors(cross_league: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="different columns"):
        fit_pooled_intercepts(cross_league, response="pts", group="league", pool="league")
    with pytest.raises(ValueError, match="non-null observation"):
        fit_pooled_intercepts(cross_league.head(0), response="pts", group="player_key", pool="league")


def test_statsmodels_parity_when_available(real_logs: pl.DataFrame) -> None:
    """Optional oracle: pin the EM fit to statsmodels MixedLM when installed."""
    sm = pytest.importorskip("statsmodels.formula.api")
    pdf = real_logs.select("player_id", "pts").to_pandas()
    model = sm.mixedlm("pts ~ 1", pdf, groups=pdf["player_id"]).fit(reml=False)
    fit = fit_random_intercepts(real_logs, response="pts", group="player_id")
    assert fit.mu == pytest.approx(float(model.params["Intercept"]), rel=0.05)
    assert fit.tau2 == pytest.approx(float(model.cov_re.iloc[0, 0]), rel=0.15)
    assert fit.sigma2 == pytest.approx(float(model.scale), rel=0.15)
