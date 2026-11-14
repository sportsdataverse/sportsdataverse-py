"""BB-D26: NBA / WNBA WAR and VORP on real 2024-25 / 2025 inputs (``tests/fixtures/bb_player_war``).

The inputs are the releases ``nba_player_war`` reads (player impact, possessions, schedule),
captured and projected as the fixture README says. Oracles: the final scores of the schedule and
of BB-D22's season records, and Basketball-Reference's 2025 advanced tables (VORP, BPM, win
shares) for the comparison with public numbers. Gates are the observed values, rounded toward
failing at the third decimal; never loosen one to pass.
"""

from __future__ import annotations

import functools
import importlib
import unicodedata
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pytest
from scipy.stats import spearmanr

from sportsdataverse._common.player_war import (
    PER_GAME,
    PTS_PER_WIN,
    REPLACEMENT_LEVEL,
    _anchor,
    _boot_frames,
    _draw_counts,
    _replicate_war,
    player_war,
    player_war_league_averages,
    repair_bpm_team_constant,
    replacement_win_pct,
    season_rows,
)
from sportsdataverse.nba.nba_player_war import _box, _possession_games, _rating_stints, nba_player_war

war_module = importlib.import_module("sportsdataverse.nba.nba_player_war")
FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_player_war"
LUCK = FIX.parent / "bb_team_luck"
SOURCES = ["bpm", "rapm", "boban"]
#: Observed on the 2025 fixtures against Basketball-Reference; each gate is the observed value
#: rounded toward failing at the third decimal (bands: observed +- 0.005).
OBS: dict[str, Any] = {
    "nba_joined": 374,  # of 375 with 500+ minutes (one name does not match)
    "nba_rho_bpm": 0.901,  # repaired BPM vs bref BPM; the release's bpm: 0.708
    "nba_rho_war": 0.778,  # war vs bref VORP; the release's war: 0.691
    "nba_vorp_ratio": (0.952, 0.962),  # summed VORP, ours / bref: 0.957
    "nba_vorp_slope": (0.755, 0.765),  # ours on bref: 0.760
    "nba_bpm_mae": 0.830,  # |rating_bpm - bref bpm|: 0.830
    "nba_traded": 49,  # traded players with 500+ minutes
    "nba_traded_bpm_mae": 2.128,  # observed 2.127
    "jokic_gap": 0.115,  # 13.415 vs 13.3
    "wnba_joined": 106,  # all 106 with 500+ minutes
    "wnba_rho_ws": 0.807,  # war vs bref win shares
}


def _read(name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / name)


def _games(league: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(team_games, player_games): ``_possession_games`` over the whole 2025 regular season."""
    return _read(f"{league}_team_games_2025.parquet"), _read(f"{league}_player_games_2025.parquet")


def _inputs(league: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    team_games, player_games = _games(league)
    return (*season_rows(player_games, team_games), _read(f"{league}_impact_2025.parquet"))


@functools.cache
def _season(league: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(rated stints, team net, player_war output with its interval) of the 2025 fixtures."""
    stints, net, impact = _inputs(league)
    rated = _rating_stints(stints, net, impact)
    team_games, player_games = _games(league)
    box = _read(f"{league}_box_2025.parquet")
    out = player_war(
        rated, net, league=league, sources=SOURCES, team_games=team_games, player_games=player_games, box=box
    )
    return rated, net, out


LEAGUES = pytest.mark.parametrize("league", ["nba", "wnba"])


def _records(league: str) -> pl.DataFrame:
    return pl.read_parquet(LUCK / f"{league}_season_records.parquet").filter(pl.col("season") == 2025)


def test_possession_stints_count_regular_season_possessions_and_points() -> None:
    # 12 regular-season games and a playoff game of the 2024-25 possessions release
    raw, sched = _read("nba_possessions_slice_2025.parquet"), _read("nba_schedule_slice_2025.parquet")
    player_games, team_games = _possession_games(raw, sched)
    stints, net = season_rows(player_games, team_games)
    assert team_games.height == 2 * 12 and player_games["game_id"].n_unique() == 12  # one row per side
    rs = sched.filter(pl.col("season_type") == "regular-season")
    rs_rows = raw.join(rs.select("game_id"), on="game_id", how="semi").height
    assert rs_rows < raw.height  # the playoff game is in the slice and is dropped
    assert net["poss"].sum() == rs_rows  # each row is one possession for each side, halved
    assert stints["poss"].sum() == 5 * net["poss"].sum()  # five on the floor, (off + def) / 2
    sides = [("home", "away"), ("away", "home")]
    margin = (
        pl.concat(
            rs.select(pl.col(f"{a}_team_id").alias("team_id"), (pl.col(f"{a}_pts") - pl.col(f"{b}_pts")).alias("m"))
            for a, b in sides
        )
        .group_by("team_id")
        .agg(pl.col("m").sum())
    )
    j = net.with_columns((pl.col("net") * pl.col("poss") / 100).alias("pm")).join(margin, on="team_id")
    assert j.height == net.height == 18
    # points from the shot counts against the final scores: observed within 2.0 per team over 1-2 games
    assert (j["pm"] - j["m"]).abs().max() <= 2.0


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_bpm_repair_restores_the_team_constant(league: str) -> None:
    _, net, impact = _inputs(league)
    ratings = impact.select("season", "team_id", "min", "bpm")
    team = pl.col("team_id")

    def team_mean(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by(team)
            .agg(((pl.col("bpm") * pl.col("min")).sum() / pl.col("min").sum()).alias("wm"))
            .join(net, on="team_id")
        )

    raw = team_mean(ratings)
    # the release's minute-weighted team MEAN is the team's net rating (BPM 2.0 wants net / 5):
    # observed slope 0.942 (NBA) / 0.935 (WNBA)
    assert np.polyfit(raw["net"], raw["wm"], 1)[0] >= {"nba": 0.941, "wnba": 0.935}[league]
    fixed = team_mean(repair_bpm_team_constant(ratings, net))
    assert np.allclose(fixed["wm"], fixed["net"] / 5, atol=1e-9)
    twice = team_mean(repair_bpm_team_constant(repair_bpm_team_constant(ratings, net), net))
    assert np.allclose(twice["wm"], fixed["wm"], atol=1e-9)  # idempotent


@LEAGUES
def test_anchor_makes_each_team_add_up_to_its_margin(league: str) -> None:
    rated, net, _ = _season(league)
    a = _anchor(rated, net, SOURCES)
    team = (
        a.group_by("team_id")
        .agg(*[(pl.col(s) * pl.col("poss")).sum().alias(s) for s in SOURCES])
        .join(net, on="team_id")
    )
    margin = team["net"] * team["poss"]  # points above average * 100
    for s in SOURCES:  # o_adj_rapm / d_adj_rapm offsets and BPM's team constant are gone
        assert np.allclose(team[s], margin, atol=1e-6), s
    assert all(abs(a.select((pl.col(s) * pl.col("poss")).sum()).item()) < 1e-6 for s in SOURCES)


@LEAGUES
def test_league_war_is_wins_minus_replacement_team_wins(league: str) -> None:
    _, net, out = _season(league)
    rec = _records(league)
    wins, games = rec["wins"].sum(), rec["games"].sum()
    repl = replacement_win_pct(league, net["poss"].sum() / games)
    assert out["war"].sum() == pytest.approx(wins - repl * games, rel=1e-9)
    for s in SOURCES:  # one replacement level per league: every source has the same total
        assert out[f"war_{s}"].sum() == pytest.approx(out["war"].sum(), rel=1e-9)
    # VORP: (rating - R) summed over a season of possessions per team
    assert out["vorp"].sum() == pytest.approx(-REPLACEMENT_LEVEL[league] * 5 * net.height, rel=1e-9)
    # observed: a replacement team wins .189 (NBA 2024-25), .104 (WNBA 2025)
    assert repl == pytest.approx({"nba": 0.189, "wnba": 0.104}[league], abs=0.001)
    # the release's own war column adds up to more wins than the league played
    impact = _read(f"{league}_impact_2025.parquet")
    assert impact["war"].sum() > wins > out["war"].sum()


@LEAGUES
def test_team_war_tracks_team_wins(league: str) -> None:
    rated, net, _ = _season(league)
    repl, ppw = REPLACEMENT_LEVEL[league], PTS_PER_WIN[league]
    a = _anchor(rated, net, SOURCES).with_columns(
        ((pl.mean_horizontal(SOURCES) - repl) * pl.col("poss") / 100 / ppw).alias("war")
    )
    rec = _records(league)
    assert a.schema["team_id"] == net.schema["team_id"] == rec.schema["team_id"] == pl.Int64
    team = a.group_by("team_id").agg(pl.col("war").sum()).join(net, on="team_id").join(rec, on="team_id")
    assert team.height == net.height == {"nba": 30, "wnba": 13}[league]
    exp = team["war"] + team["games"] / 2 + 5 * repl * team["poss"] / 100 / ppw
    mae = float((exp - team["wins"]).abs().mean())
    # observed 2.686 (NBA) / 2.028 (WNBA) wins: the luck BB-D22 measures, nothing else
    assert mae <= {"nba": 2.687, "wnba": 2.029}[league]
    assert np.corrcoef(exp.to_numpy(), team["wins"].to_numpy())[0, 1] >= {"nba": 0.982, "wnba": 0.962}[league]


def test_a_player_without_a_rating_is_his_team_average() -> None:
    rated, net, _ = _season("nba")
    who, nan = rated.sort("poss", descending=True)["player_id"][:2]
    blank = rated.with_columns(
        *[pl.when(pl.col("player_id") == who).then(None).otherwise(pl.col(s)).alias(s) for s in SOURCES]
    ).with_columns(  # a NaN rating is treated as missing, not spread over the season
        *[pl.when(pl.col("player_id") == nan).then(float("nan")).otherwise(pl.col(s)).alias(s) for s in SOURCES]
    )
    a = _anchor(blank, net, SOURCES).filter(pl.col("player_id") == who)
    for s in SOURCES:  # net / 5: the margins sum to zero, so the league shift is nil
        assert np.allclose(a[s], a["net"] / 5, atol=1e-9), s
    full = player_war(blank, net, league="nba", sources=SOURCES)
    assert full["war"].is_finite().all() and full["war_rank"].null_count() == 0
    out = full.filter(pl.col("player_id") == who)
    assert out["war"].null_count() == 0 and out["war"].item() > 0


@LEAGUES
def test_war_and_vorp_formulas(league: str) -> None:
    _, net, out = _season(league)
    repl, ppw = REPLACEMENT_LEVEL[league], PTS_PER_WIN[league]
    lg_poss = net["poss"].sum() / net.height
    one = out.filter(pl.col("teams") == 1)
    assert np.allclose(one["war"], (one["rating"] - repl) * one["poss"] / 100 / ppw)
    assert np.allclose(one["vorp"], (one["rating"] - repl) * one["poss"] / lg_poss)
    assert np.allclose(out["rating"], out.select(pl.mean_horizontal([f"rating_{s}" for s in SOURCES])).to_series())
    assert np.allclose(out["war"], out.select(pl.mean_horizontal([f"war_{s}" for s in SOURCES])).to_series())
    # above replacement is positive, below is negative
    assert ((out["war"] > 0) == (out["rating"] > repl)).all()
    assert out["war_rank"].min() == 1.0 and out["war"].arg_max() == 0
    assert (out["war_n"] == out["poss"].round(0).cast(pl.Int64)).all()


def _name(s: str) -> str:
    try:  # bref_players_stats returns UTF-8 read as Latin-1 ("JokiÄ\x87"): undo it
        s = s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    for drop in (".", "'", " jr", " iii", " ii"):
        s = s.replace(drop, "")
    return s.strip()


def _bref(league: str) -> pl.DataFrame:
    """One Basketball-Reference row per player: a traded player's season row (2TM / 3TM, WNBA TOT)."""
    bref = _read(f"bref_{league}_advanced_2025.parquet")
    total = pl.col("team").str.contains(r"^(\dTM|TOT)$")
    multi = bref.filter(total)
    bref = pl.concat([multi, bref.filter(~pl.col("player").is_in(multi["player"].implode()))])
    bref = bref.with_columns(
        pl.col("player").map_elements(_name, return_dtype=pl.Utf8).alias("k"), total.alias("traded")
    )
    return bref.rename({c: f"bref_{c}" for c in bref.columns if c not in ("k", "mp", "traded")})


def _with_bref(league: str, out: pl.DataFrame) -> pl.DataFrame:
    ours = out.with_columns(pl.col("player_name").map_elements(_name, return_dtype=pl.Utf8).alias("k"))
    return ours.join(_bref(league), on="k", how="inner", validate="1:1").filter(pl.col("mp") >= 500)


def test_against_basketball_reference_nba_2025() -> None:
    j = _with_bref("nba", _season("nba")[2])
    raw = _read("nba_impact_2025.parquet").with_columns(
        pl.col("player_name").map_elements(_name, return_dtype=pl.Utf8).alias("k")
    )
    j = j.join(
        raw.select("k", pl.col("bpm").alias("release_bpm"), pl.col("war").alias("release_war")),
        on="k",
        how="left",
        validate="1:1",
    )
    assert j.height >= OBS["nba_joined"]  # of 375 with 500+ minutes

    def rho(a: str, b: str) -> float:
        return float(spearmanr(j[a], j[b]).statistic)

    # the BPM repair against bref BPM, and our WAR against bref VORP, beat the release's columns
    assert rho("rating_bpm", "bref_bpm") >= OBS["nba_rho_bpm"] > rho("release_bpm", "bref_bpm")
    assert rho("war", "bref_vorp") >= OBS["nba_rho_war"] > rho("release_war", "bref_vorp")
    # level: our VORP sits on bref's scale, a little below it (sum and slope)
    ratio = float(j["vorp"].sum() / j["bref_vorp"].sum())
    slope = float(np.polyfit(j["bref_vorp"].to_numpy(), j["vorp"].to_numpy(), 1)[0])
    assert OBS["nba_vorp_ratio"][0] <= ratio <= OBS["nba_vorp_ratio"][1]
    assert OBS["nba_vorp_slope"][0] <= slope <= OBS["nba_vorp_slope"][1]
    # the repaired BPM level: overall, and for traded players (anchored on each team they played for)
    gap = (j["rating_bpm"] - j["bref_bpm"]).abs()
    traded = j.filter(pl.col("traded"))
    assert traded.height >= OBS["nba_traded"] and float(gap.mean()) <= OBS["nba_bpm_mae"]
    assert float((traded["rating_bpm"] - traded["bref_bpm"]).abs().mean()) <= OBS["nba_traded_bpm_mae"]
    jokic = j.filter(pl.col("k") == "nikola jokic")
    assert abs(jokic["rating_bpm"].item() - jokic["bref_bpm"].item()) <= OBS["jokic_gap"]


def test_against_basketball_reference_wnba_2025() -> None:
    # Basketball-Reference publishes no VORP or BPM for the WNBA: the comparison is win shares
    j = _with_bref("wnba", _season("wnba")[2])
    assert j.height >= OBS["wnba_joined"]
    assert spearmanr(j["war"], j["bref_ws"]).statistic >= OBS["wnba_rho_ws"]


def test_nba_player_war_reads_the_three_releases(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, list[int]] = {}
    raw, sched = _read("nba_possessions_slice_2025.parquet"), _read("nba_schedule_slice_2025.parquet")
    impact = _read("nba_impact_2025.parquet")

    def loader(name: str, frame: pl.DataFrame):  # type: ignore[no-untyped-def]
        def f(seasons: list[int]) -> pl.DataFrame:
            calls[name] = seasons
            return frame

        return f

    monkeypatch.setattr(war_module, "_league_loaders", lambda lid: (loader("poss", raw), loader("sched", sched), 1))
    monkeypatch.setattr("sportsdataverse.nba.nba_loaders.load_nba_player_impact", loader("impact", impact))
    logs = _read("nba_game_logs_slice_2025.parquet")
    monkeypatch.setattr("sportsdataverse.nba.nba_loaders.load_nba_stats_player_game_logs", loader("logs", logs))
    out = nba_player_war(2025)
    # loaders take the start year
    assert calls == {"poss": [2024], "sched": [2024], "impact": [2025], "logs": [2024]}
    boxed = out.filter(pl.col("pts_pg_n") > 0)  # only the slice's three players have a box line
    assert 0 < boxed.height <= 3 and boxed["player_id"].is_in([1628983, 203999, 1630169]).all()
    assert out.height > 0 and set(out["season"]) == {2025}
    assert out["war_lo"].null_count() == 0  # the public path always ships the interval
    with pytest.raises(ValueError, match="league_id"):
        nba_player_war(2025, league_id="20")


def test_empty_input_returns_the_schema() -> None:
    stints, net, _ = _inputs("nba")
    out = player_war(
        stints.clear().with_columns(*[pl.lit(None, pl.Float64).alias(s) for s in SOURCES]),
        net,
        league="nba",
        sources=SOURCES,
    )
    assert out.height == 0 and "war_bpm" in out.columns and out.schema["player_id"] == pl.Int64
    assert out.schema["war_lo"] == out.schema["war_hi"] == pl.Float64


#: The 90% interval on the 2025 fixtures (N_BOOT replicates, seeded per team-season, so exact):
#: median width, the league leader's width (Jokic / A'ja Wilson), Spearman(width, possessions).
#: Monte Carlo error at 1000 replicates (seed 0 vs 1): median 0.017 wins of width (2.9%), max 0.25.
INTERVAL_OBS = {"nba": (0.795, 3.645, 0.935), "wnba": (0.793, 2.360, 0.922)}


@LEAGUES
def test_identity_replicate_is_the_point_estimate(league: str) -> None:
    rated, net, out = _season(league)
    team_games, player_games = _games(league)
    frames = _boot_frames(rated, net, team_games, player_games)
    ones = np.ones((1, frames["team_games"].height))
    war = _replicate_war(frames, ones, league=league, sources=SOURCES)[0]
    players = frames["stints"].group_by("p").agg(pl.col("player_id").first()).sort("p")
    got = players.with_columns(pl.Series("boot", war)).join(out, on="player_id", how="inner", validate="1:1")
    assert got.height == out.height
    assert np.allclose(got["boot"], got["war"], atol=1e-9)


@LEAGUES
def test_draws_keep_each_team_season_whole(league: str) -> None:
    rated, net, _ = _season(league)
    frames = _boot_frames(rated, net, *_games(league))
    tg = frames["team_games"]
    counts = _draw_counts(tg, 50)
    games = tg.group_by("t").agg(pl.len()).sort("t")
    per_team = np.stack([counts[:, tg["t"].to_numpy() == t].sum(axis=1) for t in games["t"]], axis=1)
    assert (per_team == games["len"].to_numpy()).all()  # every replicate plays each team's season length
    assert (counts == np.round(counts)).all() and counts.max() >= 3 and (counts == 0).any()  # with replacement


@LEAGUES
def test_war_interval(league: str) -> None:
    out = _season(league)[2].with_columns((pl.col("war_hi") - pl.col("war_lo")).alias("width"))
    assert out["war_lo"].null_count() == 0 and out["war_hi"].null_count() == 0
    assert ((out["war_lo"] <= out["war"]) & (out["war"] <= out["war_hi"])).all()
    median, lead, rho = INTERVAL_OBS[league]
    assert out["width"].median() == pytest.approx(median, abs=0.001)
    assert out.sort("war", descending=True)["width"][0] == pytest.approx(lead, abs=0.001)
    # possessions move together within a game: the interval widens with playing time
    assert spearmanr(out["width"], out["poss"]).statistic >= rho


def test_a_season_s_interval_does_not_depend_on_the_rest_of_the_call() -> None:
    rated, net, out = _season("nba")
    team_games, player_games = _games("nba")
    # the same frames again as a second season: each team-season draws from its own generator
    two = [pl.concat([df, df.with_columns(pl.lit(2024, pl.Int64).alias("season"))]) for df in (rated, net)]
    games = [
        pl.concat([df, df.with_columns(pl.lit(2024, pl.Int64).alias("season"))]) for df in (team_games, player_games)
    ]
    both = player_war(*two, league="nba", sources=SOURCES, team_games=games[0], player_games=games[1])
    again = both.filter(pl.col("season") == 2025).select("player_id", "war_lo", "war_hi")
    j = out.select("player_id", "war_lo", "war_hi").join(again, on="player_id", validate="1:1")
    assert (
        j.height == out.height and (j["war_lo"] == j["war_lo_right"]).all() and (j["war_hi"] == j["war_hi_right"]).all()
    )


def test_a_team_s_draws_do_not_depend_on_the_other_teams() -> None:
    rated, net, _ = _season("nba")
    team_games, player_games = _games("nba")
    frames = _boot_frames(rated, net, team_games, player_games)
    gone = net["team_id"][0]
    fewer = _boot_frames(
        rated.filter(pl.col("team_id") != gone),
        net.filter(pl.col("team_id") != gone),
        team_games.filter(pl.col("team_id") != gone),
        player_games.filter(pl.col("team_id") != gone),
    )
    full, part = _draw_counts(frames["team_games"], 20), _draw_counts(fewer["team_games"], 20)
    keys = ["team_id", "game_id"]
    kept = frames["team_games"].with_row_index("i").join(fewer["team_games"].with_row_index("j"), on=keys)
    assert kept.height == fewer["team_games"].height
    assert (full[:, kept["i"].to_numpy()] == part[:, kept["j"].to_numpy()]).all()
    # and two teams with the same 82 games draw different counts (the team is in the seed)
    tg = frames["team_games"]
    first, second = (tg.filter(pl.col("t") == t)["g"].to_numpy() for t in (0, 1))
    assert len(first) == len(second) == 82 and (full[:, first] != full[:, second]).any()


def test_stints_without_their_game_rows_are_refused() -> None:
    rated, net, _ = _season("nba")
    team_games, player_games = _games("nba")
    one_team = player_games["team_id"][0]
    with pytest.raises(ValueError, match="differ from their game rows"):
        player_war(
            rated,
            net,
            league="nba",
            sources=SOURCES,
            team_games=team_games,
            player_games=player_games.filter(pl.col("team_id") != one_team),
        )


def test_box_counts_the_regular_season_games_he_played() -> None:
    """``_box`` on every 2024-25 game log of three players: playoffs out, totals over the games he played."""
    logs = _read("nba_game_logs_slice_2025.parquet")
    box = _box(logs).sort("player_id")
    reg = logs.filter(pl.col("season_type") == "regular-season")
    assert reg.height < logs.height  # the slice holds playoff rows, and they are dropped
    want = reg.group_by("player_id").agg(pl.len().alias("games"), pl.col("pts").sum()).sort("player_id")
    assert box["games"].to_list() == want["games"].to_list() and box["pts"].to_list() == want["pts"].to_list()
    sga = box.filter(pl.col("player_id") == 1628983).row(0, named=True)
    assert (sga["games"], sga["pts"]) == (76, 2484.0)  # 32.68 points a game, the 2024-25 scoring title


@LEAGUES
def test_per_game_columns(league: str) -> None:
    """pts_pg / reb_pg / ast_pg are the box totals over games, ``_n`` the games, ranked over the games gate."""
    out = _season(league)[2]
    box = _read(f"{league}_box_2025.parquet")
    j = out.join(box, on=["season", "player_id"], how="inner", validate="1:1")
    # every player WAR rates who played a regular-season game has a box line (observed: all but 0 / 1)
    assert j.height >= out.height - 1
    for m, c in PER_GAME.items():
        assert np.allclose(j[m], j[c] / j["games"]) and (j[f"{m}_n"] == j["games"]).all()
    missing = out.join(box, on=["season", "player_id"], how="anti")
    assert (missing["pts_pg_n"] == 0).all() and missing["pts_pg"].null_count() == missing.height
    top = out.sort("pts_pg_rank").row(0, named=True)
    assert top["pts_pg_rank"] == 1.0
    if league == "nba":
        assert top["player_name"] == "Shai Gilgeous-Alexander" and top["pts_pg"] == pytest.approx(32.684, abs=1e-3)
        assert out.sort("ast_pg_rank")["player_name"][0] == "Trae Young"


@LEAGUES
def test_league_average_n_is_the_ranked_cohort(league: str) -> None:
    """``player_war_league_averages`` ``n`` equals the rows each column ranks, and inverts its ``_pct``."""
    out = _season(league)[2]
    la = player_war_league_averages(out, league=league)
    assert set(la["metric"]) == {"vorp", "war", *PER_GAME} and (la["category"] == "player_war").all()
    for r in la.iter_rows(named=True):
        ranked = out.filter(pl.col(f"{r['metric']}_rank").is_not_null())
        assert r["n"] == ranked.height > 10, r["metric"]  # the count a rank is out of
        n = r["n"]
        assert np.allclose(ranked[f"{r['metric']}_pct"], 100 * (n + 1 - ranked[f"{r['metric']}_rank"]) / (n + 1))
    # war ranks every player of the season; the per-game columns every player with a game (the pro gate is 1)
    assert la.filter(metric="war")["n"].item() == out.height
    assert la.filter(metric="pts_pg")["n"].item() == out.filter(pl.col("pts_pg_n") >= 1).height
