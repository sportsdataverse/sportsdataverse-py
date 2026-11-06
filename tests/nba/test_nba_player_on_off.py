"""BB-D5: NBA / WNBA player on/off and WOWY pairs, on committed real captures.

Fixtures (``tests/fixtures/bb_lineup_seasons``, README there; none synthetic): every 2024-25
regular-season game of three NBA teams from the ``nba_stats_*`` releases, stats.nba.com's
``teamplayeronoffsummary`` for those three teams (the oracle), and four 2005 WNBA games from the
two-half era.

The acceptance is the additive identity: a player's on and off possessions add up to his team's,
and so do the four WOWY splits of a pair. The team totals here come from the possessions release
and the play-by-play's final scores, not from the impact rows the builders read. The oracle gates
are the values observed on these teams; never lower one to make a change pass. The
``bb-player-on-off`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.rate_gates import ON_OFF_MIN_N
from sportsdataverse._common.lineup_seasons import lineup_seasons
from sportsdataverse._common.player_on_off import (
    WOWY_SIDES,
    player_on_off,
    player_on_off_schema,
    player_wowy,
    player_wowy_schema,
)
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_impact import nba_impact_rows
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
from sportsdataverse.nba.nba_player_on_off import nba_player_on_off, nba_player_wowy
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_player_on_off import wnba_player_on_off, wnba_player_wowy

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_lineup_seasons"
#: The three NBA teams whose whole regular season the fixture holds.
NBA_TEAMS = (1610612750, 1610612755, 1610612756)
_TOTALS = ("poss", "opp_poss", "pts", "opp_pts", "minutes")


def _read(name: str, lg: str = "nba", season: int = 2025) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{lg}_{name}_{season}.parquet")


@cache
def _rows(lg: str = "nba", season: int = 2025) -> pl.DataFrame:
    league_id = "00" if lg == "nba" else "10"
    names = ("pbp", "game_lineups", "possessions", "schedule")
    return nba_impact_rows(*[_read(n, lg, season) for n in names], league_id=league_id)


@cache
def _on_off(level: str | None = None) -> pl.DataFrame:
    return player_on_off(_rows(), player_dtype=pl.Int64(), min_poss=400, level=level)


@cache
def _wowy() -> pl.DataFrame:
    return player_wowy(_rows(), player_dtype=pl.Int64())


def _team_totals() -> pl.DataFrame:
    """Each fixture team's season possessions (the possessions release) and points (pbp final scores)."""
    sched = _read("schedule").select(pl.col("game_id").cast(pl.Utf8), "home_team_id", "away_team_id")
    finals = (
        _read("pbp")
        .group_by(pl.col("game_id").cast(pl.Utf8))
        .agg(pl.col("score_home", "score_away").cast(pl.Int64, strict=False).max())
        .join(sched, on="game_id", validate="1:1")
    )
    pts = (
        pl.concat(
            [
                finals.select(pl.col(f"{s}_team_id").alias("team_id"), pl.col(f"score_{s}").cast(pl.Int64).alias("pts"))
                for s in ("home", "away")
            ]
        )
        .group_by("team_id")
        .agg(pl.col("pts").sum().alias("team_pts"))
    )
    poss = (
        _read("possessions")
        .filter(pl.col("game_id").cast(pl.Utf8).is_in(sched["game_id"].implode()))
        .group_by(pl.col("offense_team_id").cast(pl.Int64).alias("team_id"))
        .agg(pl.len().cast(pl.Int64).alias("team_poss"))
    )
    return pts.join(poss, on="team_id", validate="1:1").filter(pl.col("team_id").is_in(NBA_TEAMS))


def test_on_plus_off_is_the_team_season() -> None:
    """The acceptance: every player's on + off is his team's season, against independent team totals."""
    out = _on_off().filter(pl.col("team_id").is_in(NBA_TEAMS))
    team = _team_totals()
    assert team.height == 3
    joined = out.join(team, on="team_id", how="left", validate="m:1")
    assert joined.height == 70  # every player who took the floor for the three teams
    assert (joined["team_games"] == 82).all() and (joined["games"] <= 82).all()
    assert (joined["poss_on"] + joined["poss_off"] == joined["team_poss"]).all()
    assert (joined["pts_on"] + joined["pts_off"] == joined["team_pts"]).all()
    for c in _TOTALS:  # one team total per team for every total
        per_team = joined.group_by("team_id").agg((pl.col(f"{c}_on") + pl.col(f"{c}_off")).round(4).n_unique())
        assert (per_team[f"{c}_on"] == 1).all(), c
    assert joined.filter(pl.any_horizontal(pl.col(f"{c}_off") < 0 for c in _TOTALS)).height == 0
    # off is the whole team season, the games he missed included: 82 games, 3,961 to 3,966 minutes
    assert (
        joined.group_by("team_id").agg((pl.col("minutes_on") + pl.col("minutes_off")).first())["minutes_on"].min()
        > 3960
    )


def test_matches_stats_nba_on_off_summary() -> None:
    """stats.nba.com's ``teamplayeronoffsummary`` on the players whose off side spans the team season."""
    oracle = _read("on_off_summary")
    on = oracle.filter(court_status="On").drop("court_status")
    off = oracle.filter(court_status="Off").drop("court_status", "gp")
    o = on.join(off, on=["team_id", "player_id"], suffix="_off", validate="1:1")
    ours = _on_off()
    assert o.schema["player_id"] == ours.schema["player_id"] and o.schema["team_id"] == ours.schema["team_id"]
    j = o.join(ours, on=["team_id", "player_id"], how="left", validate="1:1")
    assert j.height == 70 and j["poss_on"].null_count() == 0  # every stats.nba.com player is here
    assert (j["games"] - j["gp"]).abs().max() <= 4  # observed 4; median 0
    gate = 400
    # On the floor nothing depends on how the off side is counted: every player over the gate on it.
    on_gate = j.filter(pl.col("poss_on") + pl.col("opp_poss_on") >= gate)
    assert on_gate.height == 56
    d_on = (on_gate["net_rtg_on"] - on_gate["net_rating"]).abs()
    assert d_on.median() <= 0.53 and d_on.max() <= 3.57  # observed 0.521 / 3.561
    assert on_gate.select(pl.corr("net_rtg_on", "net_rating")).item() >= 0.983  # observed 0.9831
    # The clock: our stints against its box minutes (whole minutes), a season apart by 17 at most.
    d_min = on_gate["minutes_on"] - on_gate["min"]
    assert abs(d_min.median()) <= 0.16 and d_min.abs().median() <= 1.45  # observed 0.159 / 1.441
    assert d_min.abs().max() <= 17.1  # observed 17.09
    # stats.nba.com credits a possession to every player on the floor during it, so its ratings run
    # lower on both ends; in the net rating the two offsets cancel.
    assert 2.9 <= (on_gate["off_rtg_on"] - on_gate["off_rating"]).median() <= 3.0  # observed 2.976
    assert 2.9 <= (on_gate["def_rtg_on"] - on_gate["def_rating"]).median() <= 3.0  # observed 2.923
    # stats.nba.com's off-court covers only the games a player was on the roster: on 26 of the 70 its
    # on + off falls short of the team's season (a mid-season trade or signing), never beyond it.
    gap = pl.col("min") + pl.col("min_off") - pl.col("minutes_on") - pl.col("minutes_off")
    tenure = gap.abs() <= 3
    short = j.filter(~tenure)
    assert short.height == 26 and short.select(gap.max()).item() < -1500  # observed -1541 to -3913
    assert (short["games"] < short["team_games"]).all()
    full = on_gate.filter(tenure & (pl.col("poss_off") + pl.col("opp_poss_off") >= gate))
    assert full.height == 41
    d_off = (full["net_rtg_off"] - full["net_rating_off"]).abs()
    d_diff = (full["net_rtg_diff"] - (full["net_rating"] - full["net_rating_off"])).abs()
    assert d_off.median() <= 0.28 and d_off.max() <= 1.70  # observed 0.270 / 1.695
    assert d_diff.median() <= 0.69 and d_diff.max() <= 3.96  # observed 0.684 / 3.958
    on_off_gap = pl.col("net_rating") - pl.col("net_rating_off")
    assert full.select(pl.corr(pl.col("net_rtg_diff"), on_off_gap)).item() >= 0.961  # observed 0.9611


def test_wowy_splits_add_up_to_the_team() -> None:
    w = _wowy().filter(pl.col("team_id").is_in(NBA_TEAMS))
    team = _team_totals()
    j = w.join(team, on="team_id", validate="m:1")
    assert j.select((sum(pl.col(f"poss_{s}") for s in WOWY_SIDES) == pl.col("team_poss")).all()).item()
    assert j.select((sum(pl.col(f"pts_{s}") for s in WOWY_SIDES) == pl.col("team_pts")).all()).item()
    assert j.filter(pl.any_horizontal(pl.col(f"{c}_{s}") < 0 for c in _TOTALS for s in WOWY_SIDES)).height == 0
    # every pair of each team's players, the smaller id first: 20, 30 and 20 players
    assert w.height == 190 + 435 + 190 and (w["player_id_a"] < w["player_id_b"]).all()
    # 255 of them never shared the floor: zeros together, not nulls
    assert w.filter(pl.col("poss_both") == 0).height == 255
    assert w.select(pl.sum_horizontal(pl.col(f"{c}_{s}").null_count() for c in _TOTALS for s in WOWY_SIDES)).item() == 0
    # a rating with no possessions under it is null, never NaN or inf: 255 pairs never together
    ratings = [f"{r}_{s}" for r in ("off_rtg", "def_rtg", "net_rtg") for s in WOWY_SIDES]
    assert w.select(pl.sum_horizontal(pl.col(ratings).is_nan().sum())).item() == 0
    assert w.select(((pl.col("poss_both") == 0) == pl.col("off_rtg_both").is_null()).all()).item()
    # A's together + A alone is A's on-court season
    on = _on_off().select("team_id", pl.col("player_id").alias("player_id_a"), "poss_on", "pts_on")
    a = w.join(on, on=["team_id", "player_id_a"], validate="m:1")
    assert (a["poss_both"] + a["poss_a_only"] == a["poss_on"]).all()
    assert (a["pts_both"] + a["pts_a_only"] == a["pts_on"]).all()


def test_wowy_together_reuses_the_lineup_seasons_pairs() -> None:
    """A pair's possessions together are BB-D4's two-man unit plus the stints with an unknown fifth."""
    w = _wowy().filter(pl.col("team_id").is_in(NBA_TEAMS))
    pairs = lineup_seasons(
        _rows(), nba_lineup_count_rows(_read("possessions"), _read("schedule")), player_dtype=pl.Int64(), min_poss=400
    ).filter((pl.col("n_players") == 2) & pl.col("team_id").is_in(NBA_TEAMS))
    j = pairs.join(
        w, left_on=["team_id", "player_1", "player_2"], right_on=["team_id", "player_id_a", "player_id_b"], how="left"
    )
    assert j["poss_both"].null_count() == 0  # every two-man unit is a WOWY pair
    assert (j["poss_both"] >= j["poss"]).all()
    # equal unless the pair shared a stint with a gap in the source lineup: 533 of 560 pairs here
    assert j.filter(pl.col("poss_both") == pl.col("poss")).height == 533


def test_a_game_without_possessions_adds_nothing() -> None:
    """Its points and minutes would otherwise land on players with no possessions for them."""
    pbp, lineups = _read("pbp"), _read("game_lineups")
    poss, schedule = _read("possessions"), _read("schedule")
    no_poss = schedule["game_id"].sort().to_list()[1]
    assert poss.filter(pl.col("game_id") == no_poss).height > 0
    gapped = nba_impact_rows(pbp, lineups, poss.filter(pl.col("game_id") != no_poss), schedule)
    clean = nba_impact_rows(pbp, lineups, poss, schedule.filter(pl.col("game_id") != no_poss))
    assert player_on_off(gapped, player_dtype=pl.Int64(), min_poss=400).equals(
        player_on_off(clean, player_dtype=pl.Int64(), min_poss=400)
    )
    assert player_wowy(gapped, player_dtype=pl.Int64()).equals(player_wowy(clean, player_dtype=pl.Int64()))


def test_ranks_follow_the_gate_and_direction() -> None:
    out = _on_off()
    ranked = out.filter(pl.col("net_rtg_diff_rank").is_not_null())
    both = pl.min_horizontal(pl.col("poss_on") + pl.col("opp_poss_on"), pl.col("poss_off") + pl.col("opp_poss_off"))
    assert ranked.height == out.filter(both >= 400).height and ranked.height > 0
    best_def = ranked.sort("def_rtg_diff_rank").row(0, named=True)
    assert best_def["def_rtg_diff"] == ranked["def_rtg_diff"].min()  # lower is better on defense
    best_def_on = ranked.sort("def_rtg_on_rank").row(0, named=True)
    assert best_def_on["def_rtg_on"] == ranked["def_rtg_on"].min()
    best_net = ranked.sort("net_rtg_diff_rank").row(0, named=True)
    assert best_net["net_rtg_diff"] == ranked["net_rtg_diff"].max()
    assert out.select((pl.col("net_rtg_diff_n") == pl.min_horizontal("net_rtg_on_n", "net_rtg_off_n")).all()).item()
    assert (out["net_rtg_on_n"] == out["poss_on"] + out["opp_poss_on"]).all()
    assert (out["def_rtg_on_n"] == out["opp_poss_on"]).all() and (out["off_rtg_off_n"] == out["poss_off"]).all()


def test_wnba_half_seasons_count_the_whole_game() -> None:
    """2005 WNBA games (two 20-minute halves): on + off minutes are the games' lengths."""
    rows = _rows("wnba", 2005)
    out = player_on_off(rows, player_dtype=pl.Int64(), min_poss=200)
    team = out.group_by("team_id").agg((pl.col("minutes_on") + pl.col("minutes_off")).round(4).unique())
    assert team["minutes_on"].list.len().max() == 1
    # two 20-minute halves, then 5-minute overtimes (periods 3+): three regulation games and one 2OT
    periods = _read("pbp", "wnba", 2005).group_by(pl.col("game_id").cast(pl.Utf8)).agg(pl.col("period").max())
    games = _read("schedule", "wnba", 2005).with_columns(pl.col("game_id").cast(pl.Utf8)).join(periods, on="game_id")
    lengths: dict[int, float] = {}
    for g in games.to_dicts():
        for t in (g["home_team_id"], g["away_team_id"]):
            lengths[t] = lengths.get(t, 0.0) + 40.0 + 5.0 * (g["period"] - 2)
    assert sorted(set(games["period"])) == [2, 4]
    got = {r["team_id"]: r["minutes_on"][0] for r in team.to_dicts()}
    assert got == pytest.approx(lengths)


def test_public_builders_read_the_start_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(lg: str, name: str, want: int):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [want], f"{name}: called with {seasons}"
            return _read(name, lg)

        return load

    names = {"pbp": "pbp", "game_lineups": "game_lineups", "possessions": "possessions", "schedules": "schedule"}
    for loader, name in names.items():
        monkeypatch.setattr(nba_loaders, f"load_nba_stats_{loader}", fixture("nba", name, 2024))
        monkeypatch.setattr(wnba_loaders, f"load_wnba_stats_{loader}", fixture("wnba", name, 2025))
    assert nba_player_on_off(2025).equals(_on_off(level="nba"))
    assert not _on_off(level="nba").equals(_on_off())  # the adapter's level gate changes ranks
    assert nba_player_wowy(2025).equals(_wowy())
    assert nba_player_on_off(2025, season_type="preseason").height == 0  # the fixture has no preseason game
    wnba = wnba_player_on_off(2025)
    ranked = wnba.filter(pl.col("net_rtg_on_rank").is_not_null())
    assert ranked.filter(pl.col("poss_off") + pl.col("opp_poss_off") < 200).height == 0  # the WNBA floor is 200
    thin = wnba.filter(pl.col("net_rtg_on_n") < ON_OFF_MIN_N["wnba"]["net_rtg_on"])
    assert thin.height > 0 and thin["net_rtg_on_rank"].null_count() == thin.height  # the WNBA sample gate
    assert wnba_player_wowy(2025).height > 0


def test_empty_input_and_bad_args() -> None:
    empty_rows = nba_impact_rows(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    on_off = player_on_off(empty_rows, player_dtype=pl.Int64(), min_poss=1)
    assert on_off.height == 0 and on_off.schema == pl.Schema(player_on_off_schema(pl.Int64()))
    wowy = player_wowy(empty_rows, player_dtype=pl.Int64())
    assert wowy.height == 0 and wowy.schema == pl.Schema(player_wowy_schema(pl.Int64()))
    with pytest.raises(ValueError, match="league_id"):
        nba_player_on_off(2025, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        nba_player_wowy(2025, season_type="Regular Season")
