"""BB-D20: NBA offensive and defensive play-style archetypes, on committed real captures.

Fixtures (``tests/fixtures/bb_play_style``, README there; none synthetic): the committed training
corpus the fitter reads (every season's features, 2015-16 to 2025-26) and the 2024-25 raw release
rows the feature builders read. The oracle for the Synergy mix is stats.nba.com's own play-type
frequency (``poss_pct``). The stability gates are the values the bundled artifacts report on the
holdout seasons (2022-23 to 2025-26), truncated to two decimals; never lower one to make a change
pass. The ``bb-play-style`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common import play_style as ps
from sportsdataverse.nba import nba_play_style as nps

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_play_style"
#: Max |share - poss_pct| over one-team players: poss_pct is published to three decimals, so the
#: implied totals carry its rounding (2024-25; never raise it to pass).
DIFF_GATE = 0.0035  # observed max 0.0030
MODELS = [("offense", "player"), ("offense", "team"), ("defense", "player"), ("defense", "team")]

#: Observed on the holdout seasons by the bundled artifacts (truncated): holdout ARI (frozen
#: centers vs a fresh fit on the holdout rows) and year-over-year persistence.
GATES = {
    ("offense", "player"): {"holdout_ari": 0.78, "holdout_persistence": 0.73},
    ("offense", "team"): {"holdout_ari": 0.30, "holdout_persistence": 0.53},
    ("defense", "player"): {"holdout_ari": 0.72, "holdout_persistence": 0.75},
    ("defense", "team"): {"holdout_ari": 0.43, "holdout_persistence": 0.50},
}


def _raw(name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"nba_{name}_2025.parquet")


@cache
def _corpus(side: str, entity: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"nba_play_style_{side}_{entity}.parquet")


def _compare(built: pl.DataFrame, corpus: pl.DataFrame, key: list[str], cols: tuple[str, ...]) -> None:
    """``built`` (rounded as the corpus is) equals the corpus season row for row."""
    b = built.with_columns(pl.col(cols).cast(pl.Float64).round(6)).sort(key)
    c = corpus.filter(pl.col("season") == 2025).sort(key)
    assert b.select(key).equals(c.select(key))
    for col in cols:
        np.testing.assert_allclose(b[col].to_numpy(), c[col].to_numpy(), atol=1e-6, err_msg=col)


def test_synergy_mix_is_stats_nba_com_frequency():
    """A one-team player's play-type share is stats.nba.com's own frequency for that play type."""
    syn = _raw("synergy")
    off = syn.filter(pl.col("type_grouping") == "Offensive")
    one_team = off.group_by("player_id").agg(pl.col("team_id").n_unique()).filter(pl.col("team_id") == 1)
    mix = nps.synergy_mix(syn, by="player_id", grouping="Offensive").join(one_team.select("player_id"), on="player_id")
    oracle = off.with_columns(pl.col("play_type").str.to_lowercase()).join(mix, on=["player_id", "season"])
    diff = oracle.select(
        pl.max_horizontal(
            *[
                (pl.col(f"pt_{t}") - pl.col("poss_pct")).abs().filter(pl.col("play_type") == t).max()
                for t in nps._OFF_TYPES
            ]
        )
    ).item()
    assert oracle.height > 2900
    assert diff < DIFF_GATE


def test_offense_player_builder_reproduces_the_corpus():
    trk = {f: _raw(f"player_tracking_{f}") for f in ("drives", "possessions", "catchshoot", "pullupshot", "passing")}
    built = nps.offense_player_features(_raw("synergy"), _raw("player_stats_scoring"), trk)
    cols = nps.NBA_PLAY_STYLE_FEATURES[("offense", "player")]
    _compare(built, _corpus("offense", "player"), ["player_id"], cols)


def test_offense_team_builder_reproduces_the_corpus():
    poss = _raw("possessions").with_columns(pl.lit(2025, dtype=pl.Int64).alias("season"))
    built = nps.offense_team_features(_raw("synergy"), _raw("team_stats_scoring"), poss)
    _compare(built, _corpus("offense", "team"), ["team_id"], nps.NBA_PLAY_STYLE_FEATURES[("offense", "team")])


def test_defense_builders_reproduce_the_corpus():
    built = nps.defense_player_features(
        _raw("synergy"), _raw("matchups_rollup"), _raw("hustle_player"), _raw("player_tracking_defense")
    )
    _compare(built, _corpus("defense", "player"), ["player_id"], nps.NBA_PLAY_STYLE_FEATURES[("defense", "player")])
    built = nps.defense_team_features(_raw("team_stats_opponent"), _raw("team_stats_defense"), _raw("hustle_team"))
    _compare(built, _corpus("defense", "team"), ["team_id"], nps.NBA_PLAY_STYLE_FEATURES[("defense", "team")])


def test_possession_tempo_is_the_mean_length_by_start_type():
    """Independent walk of one team's season: previous possession's end minus this one's, by start type."""
    p = _raw("possessions").with_columns(pl.lit(2025, dtype=pl.Int64).alias("season"))
    tempo = nps._possession_tempo(p).filter(pl.col("team_id") == 1610612760)
    lengths: dict[str, list[float]] = {"OffMissedShot": [], "OffLiveBallTurnover": []}
    prev: dict[tuple, float] = {}
    for r in p.sort("game_id", "period", "possession_number").iter_rows(named=True):
        key = (r["game_id"], r["period"])
        before = prev.get(key)
        prev[key] = r["end_seconds_remaining"]
        mine = r["offense_team_id"] == 1610612760 and r["count_as_possession"]
        if mine and before is not None and r["possession_start_type"] in lengths:
            lengths[r["possession_start_type"]].append(before - r["end_seconds_remaining"])
    assert tempo["sec_after_miss"].item() == pytest.approx(np.mean(lengths["OffMissedShot"]))
    assert tempo["sec_after_live_tov"].item() == pytest.approx(np.mean(lengths["OffLiveBallTurnover"]))
    assert len(lengths["OffMissedShot"]) > 2000
    assert tempo["sec_after_live_tov"].item() < tempo["sec_after_miss"].item()  # steals are pushed


def test_player_qualifier_is_a_share_of_the_season_max():
    f = _corpus("offense", "player")
    floor = f.group_by("season").agg((pl.col("min").min() / pl.col("min").max()).alias("r"))
    assert floor["r"].min() >= nps.PLAYER_MIN_SHARE


@pytest.mark.parametrize(("side", "entity"), MODELS)
def test_artifact_is_the_fit_of_the_committed_corpus(side, entity):
    """Lineage: refitting the committed corpus on the artifact's training seasons gives its centers."""
    art = nps._artifact(side, entity)
    assert set(art["train_seasons"]).isdisjoint(art["holdout_seasons"])
    assert max(art["train_seasons"]) < min(art["holdout_seasons"])
    assert art["feature_cols"] == list(nps.NBA_PLAY_STYLE_FEATURES[(side, entity)])
    refit = ps.fit_play_style(
        _corpus(side, entity),
        art["feature_cols"],
        k=art["k"],
        train_seasons=art["train_seasons"],
        phrases=nps.NBA_PLAY_STYLE_PHRASES[(side, entity)],
        seed=art["seed"],
    )
    np.testing.assert_allclose(refit["centers"], art["centers"], atol=1e-9)
    assert refit["labels"] == art["labels"]


@pytest.mark.parametrize(("side", "entity"), MODELS)
def test_stability_gates_on_the_holdout_seasons(side, entity):
    """Holdout stability recomputed from the corpus: above the pinned gates and above chance."""
    art = nps._artifact(side, entity)
    rep = ps.stability_report(
        _corpus(side, entity), art, entity=f"{entity}_id", holdout_seasons=art["holdout_seasons"], n_boot=1
    )
    for metric, gate in GATES[(side, entity)].items():
        assert rep[metric] == pytest.approx(art["stability"][metric], abs=1e-4)
        assert rep[metric] >= gate, metric
    assert rep["holdout_persistence"] > 1.5 * rep["holdout_chance_persistence"]


def test_assignment_is_as_of_its_own_season():
    """A season scored alone equals the same season scored beside others: no cross-season leakage."""
    f = _corpus("offense", "player")
    art = nps._artifact("offense", "player")
    alone = ps.assign_play_style(f.filter(pl.col("season") == 2023), art, ["player_id", "season"])
    pooled = ps.assign_play_style(f, art, ["player_id", "season"]).filter(pl.col("season") == 2023)
    assert alone.sort("player_id").equals(pooled.sort("player_id"))
    # and a later season standardized against itself, not the training seasons: the shares differ
    z = ps.season_zscores(f, art["feature_cols"])
    assert z.filter(pl.col("season") == 2026)["fga3_share"].mean() == pytest.approx(0.0, abs=1e-9)


@pytest.mark.parametrize(
    ("side", "name", "label"),
    [
        ("offense", "Rudy Gobert", "rim runner"),
        ("offense", "Jalen Brunson", "primary ball handler"),
        ("offense", "Klay Thompson", "off-screen shooter"),
        ("defense", "Rudy Gobert", "big-man defender"),
        ("defense", "Dyson Daniels", "ball hawk"),
    ],
)
def test_known_2025_players(side, name, label):
    f = _corpus(side, "player")
    out = ps.assign_play_style(f, nps._artifact(side, "player"), ["player_name", "season"])
    assert out.filter((pl.col("season") == 2025) & (pl.col("player_name") == name))["archetype"].item() == label


def test_public_frames_join_both_sides(monkeypatch):
    """Offense covers 2017, defense starts 2018: a 2017 player row carries a null defense side."""

    def fake(seasons, *, side, entity, return_as_pandas=False):
        f = _corpus(side, entity)
        return f.filter(pl.col("season").is_in(nps._seasons(seasons, side, entity)))

    monkeypatch.setattr(nps, "nba_play_style_features", fake)
    players = nps.nba_player_play_styles([2017, 2025])
    assert players.columns[:5] == ["player_id", "season", "player_name", "team_id", "min"]
    assert (
        players.filter(pl.col("season") == 2017)["def_archetype"].null_count()
        == players.filter(pl.col("season") == 2017).height
    )
    both = players.filter(pl.col("season") == 2025)
    assert both["off_archetype"].null_count() == 0 and both["def_archetype"].null_count() < both.height
    teams = nps.nba_team_play_styles(2025)
    assert teams.height == 30 and teams["off_archetype"].null_count() == teams["def_archetype"].null_count() == 0
    assert (teams["off_margin"] >= 0).all() and (teams["off_second_archetype"] != teams["off_archetype"]).all()


def test_empty_and_bad_arguments():
    assert ps.assign_play_style(
        _corpus("defense", "team").head(0), nps._artifact("defense", "team"), ["team_id", "season"]
    ).columns == [
        "team_id",
        "season",
        *ps.ASSIGN_SCHEMA,
    ]
    assert nps._seasons([2015, 2016], "defense", "player") == []
    with pytest.raises(ValueError, match="side must be"):
        nps.nba_play_style_features(2025, side="both")
