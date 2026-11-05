"""BB-D20: college player play-style archetypes (box + shot + lineup-role features), on real captures.

Fixtures (``tests/fixtures/bb_play_style``, README there; none synthetic): the committed MBB and
WBB training corpus (2025 and 2026 qualifying player-seasons) and one MBB team's 2025 shots. The
stability gates are the values the bundled artifacts report on the 2026 holdout, truncated to two
decimals; never lower one to make a change pass.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common import play_style as ps
from sportsdataverse.mbb import mbb_play_style as mps

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_play_style"
GATES = {
    "mbb": {"holdout_ari": 0.82, "holdout_persistence": 0.50},
    "wbb": {"holdout_ari": 0.74, "holdout_persistence": 0.49},
}


@cache
def _corpus(prefix: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{prefix}_play_style_player.parquet")


def test_shot_features_reproduce_the_corpus():
    """One team's full 2025 shots give its players the corpus's assisted, dunk and tip shares."""
    shots = pl.read_parquet(FIX / "mbb_shots_2025_team.parquet")
    built = mps._shot_features(shots)
    team = str(shots["team_id"][0])
    corpus = _corpus("mbb").filter((pl.col("season") == 2025) & (pl.col("team_id") == team))
    joined = corpus.join(built, on=["player_id", "season"], suffix="_built")
    assert joined.height == corpus.height > 5
    for c in ("assisted_share", "dunk_share", "tip_share"):
        np.testing.assert_allclose(joined[c].to_numpy(), joined[f"{c}_built"].round(6).to_numpy(), atol=1e-6)


def test_assisted_share_counts_made_shots_with_an_assister():
    """Independent count for one player: made non-free-throw shots with ``athlete_id_2`` over made shots."""
    shots = pl.read_parquet(FIX / "mbb_shots_2025_team.parquet")
    fga = shots.filter(~pl.col("type_text").str.contains("(?i)free") & (pl.col("score_value") >= 2))
    top = fga["athlete_id_1"].drop_nulls().mode()[0]
    mine = fga.filter((pl.col("athlete_id_1") == top) & (pl.col("scoring_play") == True))  # noqa: E712
    expect = mine["athlete_id_2"].is_not_null().sum() / mine.height
    got = mps._shot_features(shots).filter(pl.col("player_id") == str(top))["assisted_share"].item()
    assert got == pytest.approx(expect)
    assert 0 < got < 1


@pytest.mark.parametrize("prefix", ["mbb", "wbb"])
def test_artifact_is_the_fit_of_the_committed_corpus(prefix):
    art = mps._artifact(prefix)
    assert art["train_seasons"] == [2025] and art["holdout_seasons"] == [2026]
    assert art["feature_cols"] == list(mps.COLLEGE_PLAY_STYLE_FEATURES)
    refit = ps.fit_play_style(
        _corpus(prefix),
        art["feature_cols"],
        k=art["k"],
        train_seasons=[2025],
        phrases=mps.COLLEGE_PLAY_STYLE_PHRASES,
        seed=art["seed"],
    )
    np.testing.assert_allclose(refit["centers"], art["centers"], atol=1e-9)
    assert refit["labels"] == art["labels"]


@pytest.mark.parametrize("prefix", ["mbb", "wbb"])
def test_stability_gates_on_the_2026_holdout(prefix):
    art = mps._artifact(prefix)
    rep = ps.stability_report(_corpus(prefix), art, entity="player_id", holdout_seasons=[2026], n_boot=1)
    for metric, gate in GATES[prefix].items():
        assert rep[metric] == pytest.approx(art["stability"][metric], abs=1e-4)
        assert rep[metric] >= gate, metric
    assert rep["holdout_persistence"] > 2 * rep["holdout_chance_persistence"]
    assert rep["n_persistence_pairs"] > 1000


def test_zscores_are_clipped():
    """A near-constant WBB feature (dunks) cannot put a player dozens of sds from every center."""
    z = ps.season_zscores(_corpus("wbb"), mps.COLLEGE_PLAY_STYLE_FEATURES)
    raw = (pl.col("dunk_share") - pl.col("dunk_share").mean()) / pl.col("dunk_share").std(ddof=0)
    assert _corpus("wbb").filter(pl.col("season") == 2026).select(raw.max()).item() > 10
    assert z.select(pl.max_horizontal(pl.col(mps.COLLEGE_PLAY_STYLE_FEATURES).abs().max())).item() == ps.Z_CLIP


def test_public_frame(monkeypatch):
    monkeypatch.setattr(mps, "college_play_style_features", lambda seasons, league="mens": _corpus("mbb"))
    out = mps.mbb_play_styles([2025, 2026])
    assert out.columns == ["player_id", "season", "team_id", "player", "min", *ps.ASSIGN_SCHEMA]
    assert out.height == _corpus("mbb").height and out["archetype"].null_count() == 0
    assert set(out["archetype"]) == set(mps._artifact("mbb")["labels"])
    monkeypatch.setattr(mps, "college_play_style_features", lambda seasons, league="mens": pl.DataFrame())
    empty = mps.mbb_play_styles(2030)
    assert empty.height == 0 and empty.columns == out.columns
