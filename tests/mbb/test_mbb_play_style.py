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
    "mbb": {"holdout_ari": 0.79, "holdout_persistence": 0.50},
    "wbb": {"holdout_ari": 0.73, "holdout_persistence": 0.50},
}
#: Smallest / largest archetype share in 2026 (observed MBB 0.1192 / 0.1905, WBB 0.1079 / 0.1827).
SHARE_GATES = {"mbb": (0.11, 0.20), "wbb": (0.10, 0.19)}


@cache
def _corpus(prefix: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{prefix}_play_style_player.parquet")


def test_shot_features_reproduce_the_corpus():
    """One team's full 2025 shots give its players the corpus's assisted and dunk shares."""
    shots = pl.read_parquet(FIX / "mbb_shots_2025_team.parquet")
    built = mps._shot_features(shots)
    team = str(shots["team_id"][0])
    corpus = _corpus("mbb").filter((pl.col("season") == 2025) & (pl.col("team_id") == team))
    joined = corpus.join(built, on=["player_id", "season", "team_id"], suffix="_built")
    assert joined.height == corpus.height > 5
    for c in ("assisted_share", "dunk_share"):
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
    rep = ps.stability_report(_corpus(prefix), art, entity="player_id", holdout_seasons=[2026], n_boot=1, n_ci=0)
    for metric, gate in GATES[prefix].items():
        assert rep[metric] == pytest.approx(art["stability"][metric], abs=1e-4)
        assert rep[metric] >= gate, metric
    assert rep["holdout_persistence"] > 2 * rep["holdout_chance_persistence"]
    assert rep["n_persistence_pairs"] > 1000  # observed 1,440 / 1,435
    assert art["stability"]["holdout_ari_ci"][0] > art["stability"]["null_holdout_ari"]
    assert rep["holdout_ari"] > rep["null_holdout_ari"] == pytest.approx(art["stability"]["null_holdout_ari"], abs=1e-4)
    assert SHARE_GATES[prefix][0] <= rep["min_share"] and rep["max_share"] <= SHARE_GATES[prefix][1]


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


def test_lineup_role_features_are_hand_counts(monkeypatch):
    """One team's real 2025 box: starts and minutes per game counted by hand for a part-time starter."""
    from sportsdataverse.mbb import mbb_player_value_constants as pv

    box = pl.read_parquet(FIX / "mbb_player_box_2025_team.parquet")
    shots = pl.read_parquet(FIX / "mbb_shots_2025_team.parquet")
    monkeypatch.setattr(pv, "_load_player_box", lambda seasons, league: box)
    monkeypatch.setattr(pv, "_load_shots", lambda seasons, league: shots)
    monkeypatch.setattr(mps, "_load_shots", lambda seasons, league: shots)
    feats = mps.college_play_style_features(2025)
    games: dict[int, list[tuple[float, bool]]] = {}
    for r in box.iter_rows(named=True):
        if r["minutes"] is not None and r["minutes"] > 0:
            games.setdefault(r["athlete_id"], []).append((r["minutes"], bool(r["starter"])))
    part_time = [a for a, g in games.items() if 0 < sum(s for _, s in g) < len(g) and str(a) in feats["player_id"]]
    assert part_time
    for athlete in part_time:
        g = games[athlete]
        row = feats.filter(pl.col("player_id") == str(athlete))
        assert row["starter_share"].item() == pytest.approx(sum(s for _, s in g) / len(g))
        assert row["mpg_share"].item() == pytest.approx(sum(m for m, _ in g) / len(g) / 40)


def test_wbb_wrapper_routes_the_womens_league(monkeypatch):
    from sportsdataverse.wbb.wbb_play_style import wbb_play_styles

    seen = {}

    def features(seasons, league="mens"):
        seen["league"] = league
        return _corpus("wbb")

    monkeypatch.setattr(mps, "college_play_style_features", features)
    out = wbb_play_styles([2025, 2026])
    assert seen["league"] == "womens"
    assert set(out["archetype"]) == set(mps._artifact("wbb")["labels"])
