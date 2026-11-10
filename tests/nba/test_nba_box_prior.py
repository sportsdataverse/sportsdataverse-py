"""Box prior v2 (``nba_box_prior``, BB-D38): career target, features, walk-forward and player cross-fit.

Fixtures: ``tests/fixtures/bb_box_prior_v2`` (README there), all real: the first 100 regular-season games
of 2023-24 (possessions and leaguegamelog lines), the players' 2022-23 / 2023-24 ages, height and listed
position, and the 2021-22 / 2022-23 feature + career-target rows the 2023-24 prior trained on.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_box_logs import box_features
from sportsdataverse.nba.nba_box_prior import (
    BOX_PRIOR_FEATURES,
    BOX_PRIOR_PAD_POSS,
    apply_box_prior,
    box_prior_features,
    box_prior_training_seasons,
    career_rapm_target,
    season_gram,
    train_box_prior,
)
from sportsdataverse.nba.nba_rapm import RAPM_PENALTY, nba_rapm

_FX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_box_prior_v2"


@pytest.fixture(scope="module")
def poss() -> pl.DataFrame:
    return pl.read_parquet(_FX / "nba_possessions_2024_slice.parquet")


@pytest.fixture(scope="module")
def halves(poss) -> dict[int, pl.DataFrame]:
    """The slice's first and last 50 games as two 'seasons' (real possessions, relabelled only)."""
    g = poss["game_id"].unique().sort()
    return {1: poss.filter(pl.col("game_id").is_in(g.head(50).implode())), 2: poss.filter(pl.col("game_id").is_in(g.tail(50).implode()))}  # fmt: skip


_NO_AGES = pl.DataFrame({"player_id": [], "season": [], "age": []}, schema={"player_id": pl.Int64, "season": pl.Int64, "age": pl.Float64})  # fmt: skip


def _maxdiff(a: pl.DataFrame, b: pl.DataFrame) -> float:
    """Largest absolute difference between two feature frames of the same players (polars sums reorder: ~1e-15)."""
    a, b = a.sort("player_id"), b.sort("player_id")
    assert a["player_id"].equals(b["player_id"])
    return float((a.drop("player_id") - b.drop("player_id")).select(pl.all().abs().max()).max_horizontal().item())


def _rapm(frame: pl.DataFrame) -> pl.DataFrame:
    return nba_rapm(frame, alpha=RAPM_PENALTY["00"]).select("player_id", "o_rapm", "d_rapm")


def test_one_season_without_ages_is_plain_rapm(poss) -> None:
    """The assembled career fit, on one season with no aging term, is nba_rapm at the same penalty."""
    t = career_rapm_target({2024: season_gram(poss)}, _NO_AGES, seasons=[2024], center=2024)
    r = _rapm(poss)
    assert t.schema["player_id"] == r.schema["player_id"]
    j = t.join(r, on="player_id")
    assert j.height == r.height == t.height
    assert (j["o_target"] - j["o_rapm"]).abs().max() < 1e-6
    assert (j["d_target"] - j["d_rapm"]).abs().max() < 1e-6


def test_the_time_weight_reaches_the_fit(halves) -> None:
    """A near-zero half-life drops the far season: the career fit is the near season's RAPM."""
    grams = {s: season_gram(f) for s, f in halves.items()}
    near = career_rapm_target(grams, _NO_AGES, seasons=[1, 2], center=1, half_life=0.01).filter(pl.col("season") == 1)
    j = near.join(_rapm(halves[1]), on="player_id")
    assert j.height == near.height
    assert (j["o_target"] - j["o_rapm"]).abs().max() < 1e-4
    # and at a long half-life the far season moves it (the weight is applied, not ignored)
    wide = career_rapm_target(grams, _NO_AGES, seasons=[1, 2], center=1, half_life=4.0).filter(pl.col("season") == 1)
    assert (
        wide.join(near, on="player_id")["o_target"] - wide.join(near, on="player_id")["o_target_right"]
    ).abs().max() > 0.1


def test_the_aging_curve_is_one_curve_for_every_player(halves) -> None:
    """A player's target moves between seasons only by the shared quadratic aging term."""
    bio = pl.read_parquet(_FX / "nba_bio_2023_2024.parquet")
    ages = bio.filter(pl.col("season") == 2023).select("player_id", pl.lit(1, pl.Int64).alias("season"), "age")
    ages = pl.concat([ages, ages.with_columns(pl.lit(2, pl.Int64).alias("season"), pl.col("age") + 3.0)])
    grams = {s: season_gram(f) for s, f in halves.items()}
    t = career_rapm_target(grams, ages, seasons=[1, 2], center=1)
    both = t.filter(pl.col("season") == 1).join(t.filter(pl.col("season") == 2), on="player_id", suffix="_2")
    both = both.join(ages.filter(pl.col("season") == 1), on="player_id")
    assert both.height >= 300  # observed 317 players in both halves with an age
    a1 = (both["age"].to_numpy() - 27.0) / 10.0
    a2 = a1 + 0.3
    A = np.column_stack([a2 - a1, a2**2 - a1**2])
    for side in ("o_target", "d_target"):
        diff = (both[f"{side}_2"] - both[side]).to_numpy()
        coef, *_ = np.linalg.lstsq(A, diff, rcond=None)
        assert np.abs(A @ coef - diff).max() < 1e-8
        assert np.abs(diff).max() > 0.01  # the curve is not flat: the aging term is in the fit


def test_walk_forward_and_leave_season_out() -> None:
    avail = range(1997, 2027)
    assert box_prior_training_seasons(2000, avail) == [s for s in avail if s != 2000]
    assert box_prior_training_seasons(2005, avail) == [s for s in avail if s != 2005]
    assert box_prior_training_seasons(2006, avail) == list(range(1997, 2006))
    assert box_prior_training_seasons(2026, avail) == list(range(1997, 2026))
    assert 2027 not in box_prior_training_seasons(2027, [*avail, 2027])


def test_features_are_padded_league_relative_and_follow_game_ids(poss) -> None:
    logs = pl.read_parquet(_FX / "nba_player_logs_2024_slice.parquet")
    team = pl.read_parquet(_FX / "nba_team_logs_2024_slice.parquet")
    bio = pl.read_parquet(_FX / "nba_bio_2023_2024.parquet").filter(pl.col("season") == 2024)
    f = box_prior_features(logs, team, poss, bio)
    assert f.columns == ["player_id", "poss", "def_poss", *BOX_PRIOR_FEATURES]
    assert f.null_count().sum_horizontal().item() == 0
    # pts_rel is (pts - league) * poss / (poss + pad), the league mean weighted by on-court possessions
    box = box_features(logs, team)
    assert box.schema["player_id"] == f.schema["player_id"]
    j = f.join(box.select("player_id", "pts"), on="player_id")
    league = (j["pts"] * j["poss"]).sum() / f["poss"].sum()
    want = (j["pts"] - league) * j["poss"] / (j["poss"] + BOX_PRIOR_PAD_POSS)
    assert (want - j["pts_rel"]).abs().max() < 1e-9
    # restricting to a fold's games rebuilds every feature from those games only
    games = poss["game_id"].unique().sort().head(60).to_list()
    g = box_prior_features(logs, team, poss, bio, game_ids=games)
    assert g["poss"].sum() == poss.filter(pl.col("game_id").is_in(games)).height * 5
    assert g.height < f.height
    jj = g.join(f, on="player_id", suffix="_all")
    assert (jj["pts_rel"] - jj["pts_rel_all"]).abs().max() > 1.0
    # a held-out game's box lines never reach the features: rewrite them and nothing moves
    out_game = poss["game_id"].unique().sort()[80]
    bumped = logs.with_columns(
        pl.when(pl.col("game_id") == out_game).then(pl.col(c) + 40).otherwise(pl.col(c)).alias(c)
        for c in ("pts", "ast", "oreb", "dreb", "fga", "min")
    )
    assert bumped.filter(pl.col("game_id") == out_game).height > 0
    assert _maxdiff(box_prior_features(bumped, team, poss, bio, game_ids=games), g) < 1e-9
    # ...while the same rewrite of a training game does move them (the check can fail)
    in_bump = logs.with_columns(
        pl.when(pl.col("game_id") == games[0]).then(pl.col("pts") + 40).otherwise(pl.col("pts")).alias("pts")
    )
    assert _maxdiff(box_prior_features(in_bump, team, poss, bio, game_ids=games), g) > 0.01


def test_the_cross_fit_never_trains_a_player_on_himself() -> None:
    """Mutate one player's target: his own prior does not move, his fold-mates' do not, others' do."""
    pan = pl.read_parquet(_FX / "nba_panel_2022_2023.parquet")
    feats = pan.filter(pl.col("season") == 2023)
    base = apply_box_prior(feats, train_box_prior(pan, season=2024))
    pid = int(pan.sort("poss", descending=True)["player_id"][0])
    bumped = pan.with_columns(pl.when(pl.col("player_id") == pid).then(pl.col("o_target") + 50.0).otherwise(pl.col("o_target")).alias("o_target"))  # fmt: skip
    after = apply_box_prior(feats, train_box_prior(bumped, season=2024))
    j = base.join(after, on="player_id", suffix="_b").with_columns((pl.col("player_id") % 5).alias("fold"))
    moved = (j["o_prior"] - j["o_prior_b"]).abs()
    same_fold = j["fold"] == pid % 5
    assert moved.filter(same_fold).max() < 1e-9
    assert moved.filter(~same_fold).min() > 0  # every other fold's rule saw the change
    with pytest.raises(ValueError, match="own season"):
        train_box_prior(pan, season=2023)


def test_the_prior_tracks_its_target_out_of_fold() -> None:
    """Out-of-fold, the prior still ranks players like the career target (observed rho 0.669, 368 players; the 2021-22 rule scoring the 2022-23 rows of the panel)."""
    pan = pl.read_parquet(_FX / "nba_panel_2022_2023.parquet")
    p = apply_box_prior(
        pan.filter(pl.col("season") == 2023), train_box_prior(pan.filter(pl.col("season") == 2022), season=2023)
    )
    j = p.join(pan.filter(pl.col("season") == 2023), on="player_id").filter(pl.col("poss") >= 1000)
    assert j.height >= 300
    rho = j.select(pl.corr("box_prior", pl.col("o_target") + pl.col("d_target"), method="spearman")).item()
    assert rho > 0.6
