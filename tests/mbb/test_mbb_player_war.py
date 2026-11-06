"""BB-D26: college WAR and VORP on real 2024-25 inputs.

Box BPM is scored from the committed player-value captures (``tests/fixtures/{mbb,wbb}_player_value``,
the same inputs the box-BPM oracle test reads), then anchored and valued. Oracles: BB-D22's season
records (final scores) and Barttorvik's published 2025 BPM, run through Basketball-Reference's
VORP formula ``(bpm + 2) * share of team minutes * games`` as the public comparison (Barttorvik
publishes no WAR or VORP). Gates are the observed values, rounded toward failing at the third
decimal; never loosen one to pass.
"""

from __future__ import annotations

import functools
import importlib
import re
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import spearmanr

from sportsdataverse._common.player_war import (
    PTS_PER_WIN,
    REPLACEMENT_LEVEL,
    _anchor,
    player_war,
    replacement_win_pct,
    season_rows,
)
from sportsdataverse._common.team_luck import QUALIFIER

bpm_mod = importlib.import_module("sportsdataverse.mbb.mbb_box_bpm")
war_mod = importlib.import_module("sportsdataverse.mbb.mbb_player_war")
wbb_mod = importlib.import_module("sportsdataverse.wbb.wbb_player_war")
FIX = Path(__file__).resolve().parents[1] / "fixtures"
LEAGUES = pytest.mark.parametrize("league", ["mbb", "wbb"])
#: Observed against Barttorvik 2025 (players with 200+ minutes); gates rounded toward failing.
OBS: dict[str, dict[str, float]] = {
    # joined 3205 / 3168; Spearman(war, bart VORP formula) 0.8605 / 0.8971; mean(rating - bart
    # bpm) +0.221 / +0.107; MAE 1.556 / 1.755
    "mbb": {"joined": 3205, "rho": 0.860, "diff": 0.221, "mae": 1.557},
    "wbb": {"joined": 3168, "rho": 0.897, "diff": 0.107, "mae": 1.756},
}
_NAME = {"mbb": "mens", "wbb": "womens"}


def _ratings(league: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{league}_player_value" / "team_ratings_2025.parquet")


@functools.cache
def _box(league: str) -> pl.DataFrame:
    agg = pl.read_parquet(FIX / f"{league}_player_value" / "player_seasons_2025.parquet")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bpm_mod, "aggregate_player_seasons", lambda seasons, league="mens": agg)
        mp.setattr(bpm_mod, "mbb_team_ratings", lambda seasons, league="mens", **kw: _ratings(league_key))
        league_key = league
        return bpm_mod.mbb_box_bpm(2025, league=_NAME[league])


@functools.cache
def _season(league: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    stints, net = war_mod._war_stints(_box(league), _ratings(league))
    return stints, net, player_war(stints, net, league=league, sources=["bpm"])


@LEAGUES
def test_stints_are_division_i_with_minutes_times_tempo(league: str) -> None:
    stints, net, _ = _season(league)
    d1 = _ratings(league).filter(pl.col("games") >= 10).with_columns(pl.col("team_id").cast(pl.Utf8))
    assert set(net["team_id"]) == set(d1["team_id"]) and set(stints["team_id"]) <= set(d1["team_id"])
    assert stints.height < _box(league).height  # the non-D1 opponents' players are dropped
    j = stints.join(d1.select("team_id", "adj_tempo"), on="team_id")
    assert np.allclose(j["poss"], j["min"] / 40 * j["adj_tempo"])
    assert abs(net["net"].mean()) < 1e-9  # D1-centred adj_em


@LEAGUES
def test_box_bpm_is_already_anchored(league: str) -> None:
    stints, net, _ = _season(league)
    a = _anchor(stints, net, ["bpm"]).join(
        stints.select("team_id", "player_id", pl.col("bpm").alias("box")), on=["team_id", "player_id"]
    )
    # box BPM is team-constrained to adj_em itself: re-anchoring moves no one by more than
    # 0.39 (MBB) / 0.44 (WBB), the gap from its qualified-minutes weights
    assert (a["bpm"] - a["box"]).abs().max() <= {"mbb": 0.386, "wbb": 0.435}[league]
    team = (
        a.group_by("team_id").agg((pl.col("bpm") * pl.col("poss")).sum(), pl.col("poss").sum()).join(net, on="team_id")
    )
    # each team adds up to its net rating, less one league shift: adj_em is centred on the plain
    # D1 mean, the ratings on the possession-weighted one (observed 0.137 MBB, 0.234 WBB)
    shift = team["bpm"] / team["poss"] - team["net"] / 5
    assert float(shift.max() - shift.min()) < 1e-9 and abs(float(shift.mean())) <= {"mbb": 0.138, "wbb": 0.234}[league]


@LEAGUES
def test_league_war_and_vorp_totals(league: str) -> None:
    stints, net, out = _season(league)
    repl, ppw = REPLACEMENT_LEVEL[league], PTS_PER_WIN[league]
    assert out["war"].sum() == pytest.approx(-repl * stints["poss"].sum() / 100 / ppw, rel=1e-9)
    assert out["vorp"].sum() == pytest.approx(-repl * 5 * net.height, rel=1e-9)
    rec = pl.read_parquet(FIX / "bb_team_luck" / f"{league}_season_records.parquet").filter(pl.col("season") == 2025)
    d1 = rec.filter(QUALIFIER).join(net.with_columns(pl.col("team_id").cast(pl.Int64)), on="team_id", how="semi")
    # 365 / 362 D1 teams; one MBB team (3229, 0-13, adj_em -83, in box BPM's 10-game D1
    # set) plays under half the most games, so BB-D22's qualifier drops it
    assert (net.height, d1.height) == {"mbb": (365, 364), "wbb": (362, 362)}[league]
    ppg = stints["poss"].sum() / 5 / d1["games"].sum()
    # observed: a replacement team wins .353 (MBB) / .350 (WBB) against an average D1 schedule
    assert replacement_win_pct(league, ppg) == pytest.approx({"mbb": 0.353, "wbb": 0.350}[league], abs=0.001)


@LEAGUES
def test_team_war_tracks_team_wins(league: str) -> None:
    stints, net, _ = _season(league)
    repl, ppw = REPLACEMENT_LEVEL[league], PTS_PER_WIN[league]
    a = _anchor(stints, net, ["bpm"]).with_columns(((pl.col("bpm") - repl) * pl.col("poss") / 100 / ppw).alias("w"))
    team = (
        a.group_by("team_id")
        .agg(pl.col("w").sum(), pl.col("poss").sum())
        .with_columns(pl.col("team_id").cast(pl.Int64))
    )
    rec = pl.read_parquet(FIX / "bb_team_luck" / f"{league}_season_records.parquet").filter(pl.col("season") == 2025)
    assert team.schema["team_id"] == rec.schema["team_id"] == pl.Int64
    team = team.join(rec.filter(QUALIFIER), on="team_id", validate="1:1")
    assert (net.height, team.height) == {"mbb": (365, 364), "wbb": (362, 362)}[league]
    exp = team["w"] + team["games"] / 2 + repl * team["poss"] / 100 / ppw
    # observed 5.196 (MBB) / 5.302 (WBB) wins, corr 0.784 / 0.818: an adjusted rating against
    # the real schedule, so strength of schedule shows up here as well as luck
    assert float((exp - team["wins"]).abs().mean()) <= {"mbb": 5.197, "wbb": 5.302}[league]
    assert np.corrcoef(exp.to_numpy(), team["wins"].to_numpy())[0, 1] >= {"mbb": 0.784, "wbb": 0.817}[league]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", "", s.lower())).strip()


@LEAGUES
def test_against_barttorvik_2025(league: str) -> None:
    _, _, out = _season(league)
    bart = pl.read_parquet(FIX / f"{league}_player_value" / "barttorvik_bpm_2025.parquet").filter(
        pl.col("team_id").is_not_null()
    )
    bart = bart.select(
        pl.col("team_id").cast(pl.Utf8),
        pl.col("player").map_elements(_norm, return_dtype=pl.Utf8).alias("k"),
        pl.col("bpm").alias("bart_bpm"),
        ((pl.col("bpm") + 2) * pl.col("min_per") / 100 * pl.col("gp")).alias("bart_vorp"),
    )
    ours = out.with_columns(pl.col("player_name").map_elements(_norm, return_dtype=pl.Utf8).alias("k"))
    assert ours.schema["team_id"] == bart.schema["team_id"] == pl.Utf8
    keys = ["team_id", "k"]  # a name twice on one roster cannot be told apart: dropped both sides
    j = ours.unique(keys, keep="none").join(bart.unique(keys, keep="none"), on=keys, validate="1:1")
    j = j.filter(pl.col("min") >= 200)
    obs = OBS[league]
    assert j.height >= obs["joined"]
    assert spearmanr(j["war"], j["bart_vorp"]).statistic >= obs["rho"]
    # level: our rating against Barttorvik's BPM, mean difference inside the observed +- 0.05
    diff = float((j["rating"] - j["bart_bpm"]).mean())
    assert obs["diff"] - 0.05 <= diff <= obs["diff"] + 0.05
    assert float((j["rating"] - j["bart_bpm"]).abs().mean()) <= obs["mae"]


@LEAGUES
def test_war_formula(league: str) -> None:
    stints, _, out = _season(league)
    repl, ppw = REPLACEMENT_LEVEL[league], PTS_PER_WIN[league]
    one = out.filter(pl.col("teams") == 1)
    assert np.allclose(one["war"], (one["rating"] - repl) * one["poss"] / 100 / ppw)
    lg_poss = stints["poss"].sum() / 5 / stints["team_id"].n_unique()
    assert np.allclose(one["vorp"], (one["rating"] - repl) * one["poss"] / lg_poss)
    assert "war_bpm" not in out.columns and out.schema["player_id"] == pl.Utf8


def test_mbb_player_war_reads_box_bpm_and_ratings(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, str]] = []

    def box(seasons: list[int], league: str = "mens") -> pl.DataFrame:
        seen.append(("box", league))
        return _box("wbb")

    def ratings(seasons: list[int], league: str = "mens") -> pl.DataFrame:
        seen.append(("ratings", league))
        return _ratings("wbb")

    def games(seasons: list[int]) -> pl.DataFrame:
        seen.append(("games", "womens"))
        return _bb("mbb_box_slice_2025.parquet").clear()

    monkeypatch.setattr(war_mod, "mbb_box_bpm", box)
    monkeypatch.setattr(war_mod, "mbb_team_ratings", ratings)
    monkeypatch.setattr("sportsdataverse.wbb.wbb_loaders.load_wbb_player_boxscore", games)
    # an empty box leaves the stints without game rows: refused, never a silently null interval
    with pytest.raises(ValueError, match="differ from their game rows"):
        wbb_mod.wbb_player_war(2025)
    assert seen == [("ratings", "womens"), ("box", "womens"), ("games", "womens")]
    calls: list[dict] = []
    monkeypatch.setattr(war_mod, "player_war", lambda stints, net, **kw: calls.append(kw) or stints)
    wbb_mod.wbb_player_war(2025)
    assert calls[0]["league"] == "wbb" and calls[0]["sources"] == ["bpm"]  # the WBB constants
    assert calls[0]["team_games"] is not None and calls[0]["player_games"] is not None
    with pytest.raises(ValueError, match="league"):
        war_mod.mbb_player_war(2025, league="coed")


def _bb(name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / "bb_player_war" / name)


def test_game_rows_on_a_real_box_slice() -> None:
    # every 2024-25 box row of ESPN team 2 (Auburn) and its ratings row
    box, ratings = _bb("mbb_box_slice_2025.parquet"), _bb("mbb_ratings_slice_2025.parquet")
    player_games, team_games = war_mod._game_rows(box, ratings)
    tempo = ratings["adj_tempo"].item()
    played = box.filter(pl.col("minutes").is_not_null() & (pl.col("minutes") > 0))
    ours = played.filter(pl.col("team_id").cast(pl.Utf8) == "2")
    assert player_games.height == ours.height < box.height  # DNPs and the opponents' rows are out
    assert player_games["poss"].sum() == pytest.approx(ours["minutes"].sum() / 40 * tempo)
    assert team_games.height == ours["game_id"].n_unique()
    assert team_games["poss"].sum() == pytest.approx(player_games["poss"].sum() / 5)
    margin = ours.group_by("game_id").agg((pl.col("team_score") - pl.col("opponent_team_score")).first())
    assert team_games["margin"].sum() == margin["team_score"].sum()


def test_war_interval_on_forty_teams() -> None:
    # a self-consistent 2024-25 capture of 40 D1 teams: stints, net and their game rows
    stints, net = _bb("mbb_stints_2025.parquet"), _bb("mbb_net_2025.parquet")
    team_games, player_games = _bb("mbb_team_games_2025.parquet"), _bb("mbb_player_games_2025.parquet")
    summed, _ = season_rows(player_games, team_games)
    j = summed.join(stints, on=["season", "team_id", "player_id"], how="full", coalesce=True, validate="1:1")
    assert j["poss"].null_count() == 0 and np.allclose(j["poss"], j["poss_right"])  # the box BPM minutes
    out = player_war(stints, net, league="mbb", sources=["bpm"], team_games=team_games, player_games=player_games)
    out = out.with_columns((pl.col("war_hi") - pl.col("war_lo")).alias("width"))
    assert out["width"].null_count() == 0 and ((out["war_lo"] <= out["war"]) & (out["war"] <= out["war_hi"])).all()
    # observed: median width 0.612; Johni Broome (most war) 5.20 in [4.61, 5.73], width 1.120;
    # Spearman(width, poss) 0.954
    assert out["width"].median() == pytest.approx(0.612, abs=0.001)
    assert out.sort("war", descending=True)["width"][0] == pytest.approx(1.120, abs=0.001)
    assert spearmanr(out["width"], out["poss"]).statistic >= 0.954


def test_the_public_college_path_ships_the_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    stints, net = _bb("mbb_stints_2025.parquet"), _bb("mbb_net_2025.parquet")
    games = (_bb("mbb_player_games_2025.parquet"), _bb("mbb_team_games_2025.parquet"))
    monkeypatch.setattr(war_mod, "mbb_box_bpm", lambda seasons, league="mens": stints)
    monkeypatch.setattr(war_mod, "mbb_team_ratings", lambda seasons, league="mens": net)
    monkeypatch.setattr(war_mod, "load_mbb_player_boxscore", lambda seasons: stints)
    monkeypatch.setattr(war_mod, "_war_stints", lambda box, ratings: (stints, net))
    monkeypatch.setattr(war_mod, "_game_rows", lambda box, ratings: games)
    out = war_mod.mbb_player_war(2025)
    assert out.height == 568 and out["war_lo"].null_count() == 0 and out["war_hi"].null_count() == 0


def test_a_played_row_without_an_athlete_is_dropped_on_both_sides(monkeypatch: pytest.MonkeyPatch) -> None:
    """The 2014 backfill failure, on every 2013-14 box row of ESPN team 2724 (Wichita State).

    The release carries one played row with no athlete (game 400500056, 2 minutes, every stat 0).
    """
    box, ratings = _bb("mbb_box_slice_2014.parquet"), _bb("mbb_ratings_slice_2014.parquet")
    played = box.filter(pl.col("minutes") > 0)
    assert played.filter(pl.col("athlete_id").is_null()).select("game_id", "minutes").rows() == [(400500056, 2.0)]
    pvc = importlib.import_module("sportsdataverse.mbb.mbb_player_value_constants")
    monkeypatch.setattr(pvc, "_load_player_box", lambda seasons, league: box)
    monkeypatch.setattr(pvc, "_load_shots", lambda seasons, league: pl.DataFrame())  # no 2014 shots release
    monkeypatch.setattr(bpm_mod, "mbb_team_ratings", lambda seasons, league="mens": ratings)
    monkeypatch.setattr(war_mod, "mbb_team_ratings", lambda seasons, league="mens": ratings)
    monkeypatch.setattr(war_mod, "load_mbb_player_boxscore", lambda seasons: box)
    agg = pvc.aggregate_player_seasons([2014])
    assert agg["player_id"].null_count() == 0
    assert agg["minutes"].sum() == pytest.approx(played["minutes"].sum() - 2.0)
    # the game rows drop it too, so a team-game is still exactly its players' possessions over 5
    player_games, team_games = war_mod._game_rows(box, ratings)
    assert player_games["player_id"].null_count() == 0
    assert player_games["poss"].sum() == pytest.approx(agg["minutes"].sum() / 40 * ratings["adj_tempo"].item())
    assert team_games["poss"].sum() * 5 == pytest.approx(player_games["poss"].sum())
    # the production path the backfill runs: raised "1 stints' possessions differ" before the fix
    out = war_mod.mbb_player_war(2014)
    assert out.height == agg.height and out["min"].sum() == pytest.approx(agg["minutes"].sum())
    assert out["war_lo"].null_count() == 0 and out["war_hi"].null_count() == 0
