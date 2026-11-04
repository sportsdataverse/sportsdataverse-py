"""BB-D30: NBA Cup group standings, wild cards, the knockout sim and its 82-game accounting, on real data.

Fixtures (none synthetic; ``tests/fixtures/bb_nba_cup/README.md``): stats.nba.com's
``scheduleleaguev2`` Cup rows for 2023-24 through 2026-27, the overtime possessions of the Group
Play games, the regular-season schedules behind the prior-season record, and stats.nba.com's own
Cup standings (``iststandings``) as the oracle.

What is checked against the real world:

* **Standings:** every team's group rank, conference rank (``istWildcardRank``) and knockout
  seed in the three completed Cups, 90 rows, equal the league's; the point differential and
  points scored apply the overtime rule (2024-25 and 2025-26 equal the oracle on every team;
  its 2023-24 figures include overtime, so the four teams of that Cup's two overtime games differ
  by exactly their overtime margin and points).
* **Knockout field:** the seeds give the real quarterfinals: seed 1 hosts seed 4 (the wild
  card), seed 2 hosts seed 3.
* **The 82:** the Cup adds exactly two regular-season games per team (a quarterfinal plus a
  semifinal or a game between quarterfinal losers, or two replacement games) and never the
  final. In the season sim on 2026-27 as released (the knockout TBD) they fill every team's 80
  known games to 82 with nothing left for #1084's league-average stand-in; on a complete release
  they add nothing the schedule already has.
* **Validation gates** (as of each Cup's first night, 10,000 draws, the default seed, the season
  sim's model with #1084's prior; ``CUP_GATES``): the Brier of the quarterfinal, group and title
  odds against the real Cup, pinned at observed + 0.02 (title + 0.005); pooled, the quarterfinal
  and group odds beat uniform ones. With the group stage over, the field is certain.

The ``bb-nba-cup`` rows of ``tests/mutants/registry.yaml`` go red here: the tiebreak order, the
wild card, and the final counting toward the regular season.
"""

from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
CUP, PRO, RATES = ROOT / "bb_nba_cup", ROOT / "bb_pro_season", ROOT / "bb_team_season_rates"
cup_mod = importlib.import_module("sportsdataverse.nba.nba_cup")
sim_mod = importlib.import_module("sportsdataverse.nba.nba_season_sim")
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")

CHAMPIONS = {2024: 1610612747, 2025: 1610612749, 2026: 1610612752}  # LAL, MIL, NYK


def _schedule(season: int) -> pl.DataFrame:
    """The release schedule of a season (end year) from whichever fixture holds it."""
    path = next(
        d / f"nba_schedule_{season}.parquet" for d in (CUP, PRO) if (d / f"nba_schedule_{season}.parquet").exists()
    )
    return pl.read_parquet(path)


def _cup_schedule(season: int) -> pl.DataFrame:
    return pl.read_parquet(CUP / f"nba_cup_schedule_{season}.parquet")


def _cup(season: int, as_of: dt.date | None = None):  # type: ignore[no-untyped-def]
    """:func:`cup_inputs` over the fixtures, as the season sim builds it."""
    return cup_mod.cup_inputs(
        _cup_schedule(season),
        games=sim_mod._regular_games(_schedule(season), as_of),
        possessions=pl.read_parquet(CUP / f"nba_cup_overtime_{season}.parquet"),
        last_games=sim_mod._regular_games(_schedule(season - 1), None),
        as_of_date=as_of,
    )


def _oracle(season: int) -> pl.DataFrame:
    return pl.read_parquet(CUP / f"ist_standings_{season}.parquet")


_EMPTY_POSSESSIONS = pl.read_parquet(RATES / "nba_possessions_2024.parquet").head(0)


def _loaders(mp: pytest.MonkeyPatch) -> None:
    """The release loaders patched to the fixtures; a season with no possessions loads none (no prior)."""

    def possessions(seasons: list[int]) -> pl.DataFrame:
        name = f"nba_possessions_{seasons[0] + 1}.parquet"
        path = next((d / name for d in (PRO, RATES) if (d / name).exists()), None)
        return pl.read_parquet(path) if path is not None else _EMPTY_POSSESSIONS

    mp.setattr(tsr, "load_nba_stats_possessions", possessions)
    mp.setattr(tsr, "load_nba_stats_schedules", lambda seasons: _schedule(seasons[0] + 1))


def _cup_sim(season: int, **kwargs) -> pl.DataFrame:  # type: ignore[no-untyped-def]
    """``nba_cup_sim`` over the fixtures: the season sim's model, #1084's prior and draws."""
    with pytest.MonkeyPatch.context() as mp:
        _loaders(mp)
        return cup_mod.nba_cup_sim(season, cup_schedule=_cup_schedule(season), **kwargs)


def test_cup_games_mark_groups_rounds_and_tbd_teams() -> None:
    games = cup_mod.cup_games(pl.read_parquet(CUP / "nba_cup_schedule_2027.parquet"))
    assert games.schema == pl.Schema(cup_mod.CUP_GAMES_SCHEMA)
    assert games["season"].unique().to_list() == [2027]
    rounds = dict(games.group_by("cup_round").len().iter_rows())
    assert rounds == {"group": 60, "quarterfinal": 4, "semifinal": 2, "final": 1}
    ko = games.filter(pl.col("cup_round") != "group")
    assert ko["home_team_id"].null_count() == ko.height == 7  # TBD (team id 0) is null
    assert ko.filter(pl.col("cup_round") == "semifinal")["neutral"].to_list() == [False, False]  # 2026-27: at home
    assert ko.filter(pl.col("cup_round") == "final")["neutral"].to_list() == [True]
    groups = games.filter(pl.col("cup_round") == "group").group_by("cup_group").len()
    assert sorted(groups["cup_group"].to_list()) == [f"{c} Group {g}" for c in ("East", "West") for g in "ABC"]


@pytest.mark.parametrize("season", [2024, 2025, 2026])
def test_standings_reproduce_the_league(season: int) -> None:
    st = cup_mod.cup_standings(_cup(season))
    assert st.schema == pl.Schema(cup_mod.CUP_STANDINGS_SCHEMA)
    o = _oracle(season)
    assert o.schema["teamId"] == st.schema["team_id"]
    j = st.join(o, left_on="team_id", right_on="teamId", how="inner", validate="1:1")
    assert j.height == 30
    assert (j["cup_group"] == j["istGroup"]).all()
    assert (j["group_rank"] == j["istGroupRank"]).all()
    assert (j["conference_rank"] == j["istWildcardRank"]).all()
    assert (j["knockout_seed"].fill_null(0) == j["istKnockoutRank"].fill_null(0)).all()
    assert (j["wins"] == j["wins_right"]).all() and (j["games_remaining"] == 0).all()
    off = j.filter((pl.col("point_diff") != pl.col("diff")) | (pl.col("points") != pl.col("pts")))
    # the oracle's 2023-24 figures include overtime: only that Cup's two overtime games differ
    ot_teams = pl.read_parquet(CUP / f"nba_cup_overtime_{season}.parquet")["offense_team_id"].unique().sort()
    assert off["team_id"].sort().to_list() == (ot_teams.to_list() if season == 2024 else [])
    assert j.filter(pl.col("holds_wild_card"))["group_rank"].to_list() == [2, 2]
    assert sorted(j.filter(pl.col("leads_group"))["knockout_seed"].to_list()) == [1, 1, 2, 2, 3, 3]


def test_tiebreaks_decide_real_places() -> None:
    """2024-25 and 2025-26 each had twelve places set by head-to-head and three by point differential."""
    for season in (2025, 2026):
        why = cup_mod.cup_standings(_cup(season))["tiebreak"].value_counts()
        assert dict(why.iter_rows()) == {None: 15, "head_to_head": 12, "point_diff": 3}, season


@pytest.mark.parametrize("season", [2024, 2025, 2026])
def test_seeds_give_the_real_quarterfinals(season: int) -> None:
    seed = dict(cup_mod.cup_standings(_cup(season)).select("team_id", "knockout_seed").iter_rows())
    games = cup_mod.cup_games(pl.read_parquet(CUP / f"nba_cup_schedule_{season}.parquet"))
    qf = games.filter(pl.col("cup_round") == "quarterfinal")
    assert sorted((seed[h], seed[a]) for h, a in qf.select("home_team_id", "away_team_id").iter_rows()) == [
        (1, 4),
        (1, 4),
        (2, 3),
        (2, 3),
    ]


def test_as_of_blanks_the_group_stage() -> None:
    first = cup_mod.cup_games(pl.read_parquet(CUP / "nba_cup_schedule_2026.parquet"))["game_date"].min()
    st = cup_mod.cup_standings(_cup(2026, first), as_of_date=first)
    assert (st["games_remaining"] == 4).all() and (st["wins"] == 0).all()
    assert st["as_of_date"].unique().to_list() == [first]
    # all level: last season's record orders them, a drawing where two records were equal too
    assert set(st["tiebreak"].to_list()) == {"prior_record", "random"}
    assert st.filter(pl.col("tiebreak") == "random").height == 2


MID_2026 = dt.date(2025, 11, 14)


@pytest.fixture(scope="module")
def mid_2026() -> pl.DataFrame:
    return _cup_sim(2026, as_of_date=MID_2026, n_sims=4_000, seed=7)


def test_sim_odds_sum_and_carry_their_error(mid_2026: pl.DataFrame) -> None:
    sim = mid_2026
    assert sim.schema == pl.Schema(cup_mod.CUP_SIM_SCHEMA)
    sums = {c: round(float(sim[c].sum()), 9) for c in cup_mod._ODDS}
    assert sums == {
        "p_win_group": 6.0,
        "p_wild_card": 2.0,
        "p_quarterfinal": 8.0,
        "p_semifinal": 4.0,
        "p_final": 2.0,
        "p_champion": 1.0,
    }
    assert sim.group_by("cup_group").agg(pl.col("p_win_group").sum())["p_win_group"].round(9).to_list() == [1.0] * 6
    assert float(sim["p_quarterfinal_se"].max()) <= 0.5 / np.sqrt(4_000)
    assert (sim["p_champion"] <= sim["p_final"]).all() and (sim["p_final"] <= sim["p_semifinal"]).all()
    assert sim.equals(_cup_sim(2026, as_of_date=MID_2026, n_sims=4_000, seed=7))


def test_cup_odds_are_the_season_sims_own_draws(mid_2026: pl.DataFrame) -> None:
    """The Cup sim's ratings are the season sim's as-of ratings with #1084's prior."""
    with pytest.MonkeyPatch.context() as mp:
        _loaders(mp)
        season = sim_mod.nba_season_sim(2026, as_of_date=MID_2026, n_sims=50, seed=7)
    j = mid_2026.join(season.select("team_id", "adj_net_rtg"), on="team_id", suffix="_s", validate="1:1")
    np.testing.assert_allclose(j["adj_net_rtg"].to_numpy(), j["adj_net_rtg_s"].to_numpy())


def test_sim_adds_two_regular_season_games_per_team() -> None:
    """The knockout's 30 games give every team exactly 82; the final is not one of them."""
    from sportsdataverse.nba.nba_season_sim import _noise_sd, _team_arrays

    cup = _cup(2026, MID_2026)
    model = sim_mod.as_of_game_model(_EMPTY_POSSESSIONS.pipe(tsr.nba_possession_rate_rows, _schedule(2026)))
    rng = np.random.default_rng(1)
    net, sd = _team_arrays(model, cup.teams)
    margins = model.margin(net[cup.future[0]], net[cup.future[1]]) + 13 * rng.standard_normal((500, cup.future[0].size))
    draws = cup_mod.simulate_cup_draws(
        cup,
        model,
        np.broadcast_to(net, (500, net.size)).copy(),
        _noise_sd(model, sd[:, None], sd[None, :], False),
        margins,
        rng,
    )
    assert draws.kind.count("quarterfinal") == 4 and draws.kind.count("semifinal") == 2
    assert draws.kind.count("quarterfinal_loser") == 2 and draws.kind.count("replacement") == 22
    assert "final" not in draws.kind
    per_team = np.stack([np.bincount(np.r_[draws.home[s], draws.away[s]], minlength=30) for s in range(500)])
    assert (per_team == 2).all()
    homes = np.stack([np.bincount(draws.home[s], minlength=30) for s in range(500)])
    assert set(np.unique(homes)) <= {0, 1, 2}
    # the replacement games stay among the teams out of the knockout
    rep = [i for i, k in enumerate(draws.kind) if k == "replacement"]
    out = draws.seed == 0
    rows = np.arange(500)[:, None]
    assert out[rows, draws.home[:, rep]].all() and out[rows, draws.away[:, rep]].all()


@pytest.mark.parametrize("season", [2024, 2025, 2026])
def test_finished_cup_is_certain(season: int) -> None:
    """With every game played the sim returns the real field and champion with certainty."""
    sim = _cup_sim(season, n_sims=50, seed=1)
    st = cup_mod.cup_standings(_cup(season))
    field = set(st.filter(pl.col("knockout_seed").is_not_null())["team_id"].to_list())
    assert set(sim.filter(pl.col("p_quarterfinal") == 1.0)["team_id"].to_list()) == field
    assert sim.filter(pl.col("p_champion") == 1.0)["team_id"].to_list() == [CHAMPIONS[season]]
    assert set(sim["p_quarterfinal"].unique().to_list()) == {0.0, 1.0}


# Observed 2026-11-04 (10,000 draws, the default seed, as of each Cup's first night, the season
# sim's model with #1084's prior; 2023-24 has none, 2022-23 is not committed): Brier of
# p_quarterfinal / p_win_group / p_champion against the real field, group winners and champion.
# Gate: observed + 0.02 (+ 0.005 for the champion), never lowered; pooled over the three Cups the
# quarterfinal and group odds must beat the uniform 8/30 and 1/5 (Brier 0.1956 / 0.1600). The
# champion odds do not beat 1/30 (0.0322) on three Cups and are only pinned.
CUP_GATES = {  # season: (observed qf, win, champion Brier)
    2024: (0.1795, 0.1570, 0.0333),
    2025: (0.1743, 0.1331, 0.0357),
    2026: (0.2091, 0.1604, 0.0326),
}


@pytest.fixture(scope="module")
def cup_start_briers() -> dict[int, tuple[float, ...]]:
    out = {}
    for season in CUP_GATES:
        first = cup_mod.cup_games(_cup_schedule(season))["game_date"].min()
        sim = _cup_sim(season, as_of_date=first)
        assert sim["n_sims"].unique().to_list() == [10_000]
        st = cup_mod.cup_standings(_cup(season)).select("team_id", "knockout_seed", "group_rank")
        j = sim.join(st, on="team_id", how="inner", validate="1:1")
        truth = (
            j["knockout_seed"].is_not_null(),
            j["group_rank"] == 1,
            j["team_id"] == CHAMPIONS[season],
        )
        out[season] = tuple(
            float(((j[c].to_numpy() - y.cast(pl.Float64).to_numpy()) ** 2).mean())
            for c, y in zip(("p_quarterfinal", "p_win_group", "p_champion"), truth, strict=True)
        )
    return out


@pytest.mark.parametrize("season", list(CUP_GATES))
def test_cup_start_sim_against_the_real_cup(season: int, cup_start_briers: dict) -> None:
    qf, win, champ = cup_start_briers[season]
    gate_qf, gate_win, gate_champ = CUP_GATES[season]
    assert qf <= gate_qf + 0.02, qf
    assert win <= gate_win + 0.02, win
    assert champ <= gate_champ + 0.005, champ


def test_cup_start_sim_beats_uniform_odds(cup_start_briers: dict) -> None:
    qf, win, _ = (float(np.mean(v)) for v in zip(*cup_start_briers.values(), strict=True))
    assert qf < (8 / 30) * (22 / 30), qf
    assert win < 0.2 * 0.8, win


def _season_sim_2027(monkeypatch: pytest.MonkeyPatch, cup: bool) -> tuple[pl.DataFrame, list[np.ndarray]]:
    """``simulate_pro_season`` on 2026-27 as released (TBD knockout), with or without the Cup.

    Returns the season table and the league-average fill counts the engine drew.
    """
    as_of = dt.date(2026, 11, 3)
    games = sim_mod._regular_games(_schedule(2027), as_of)
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2026.parquet"), _schedule(2026))
    cols = games.select(*sim_mod._GAME_COLS)
    fills: list[np.ndarray] = []
    real = sim_mod._unscheduled_games

    def spy(model, teams, extra, uncertain, rng):  # type: ignore[no-untyped-def]
        fills.append(extra.copy())
        return real(model, teams, extra, uncertain, rng)

    monkeypatch.setattr(sim_mod, "_unscheduled_games", spy)
    out = sim_mod.simulate_pro_season(
        cols,
        sim_mod.as_of_game_model(rows),
        n_sims=500,
        seed=1,
        season_games=82,
        cup=_cup(2027, as_of) if cup else None,
    )
    return out, fills


def test_season_sim_plays_the_cup_slots_with_real_opponents(monkeypatch: pytest.MonkeyPatch) -> None:
    """2026-27 as released: the Cup fills every team's 80 known games to 82, no league-average stand-in."""
    out, fills = _season_sim_2027(monkeypatch, cup=True)
    assert set((out["wins"] + out["losses"] + out["games_remaining"]).to_list()) == {82}
    np.testing.assert_allclose((out["proj_wins"] + out["proj_losses"]).to_numpy(), 82.0)
    assert len(fills) == 1 and not fills[0].any()  # nothing left for the stopgap
    stop, stop_fills = _season_sim_2027(monkeypatch, cup=False)
    assert (stop_fills[-1] == 2).all()  # without the Cup each team's two slots are league-average
    np.testing.assert_allclose((stop["proj_wins"] + stop["proj_losses"]).to_numpy(), 82.0)
    # the Cup's slots are real games: a team out of the knockout plays others out of it (weaker
    # on average), so the strong teams' projections move least
    diff = out.join(stop, on="team_id", suffix="_avg", validate="1:1")
    assert float((diff["proj_wins"] - diff["proj_wins_avg"]).abs().max()) < 1.0


def test_filled_slots_are_not_added_again() -> None:
    """A complete release (2025-26) between the quarterfinals and semifinals: the Cup owns the
    knockout (the quarterfinals played, the semifinals simulated), every other slot is on it."""
    as_of = dt.date(2025, 12, 11)
    games = sim_mod._regular_games(_schedule(2026), as_of).select(*sim_mod._GAME_COLS)
    rows = tsr.nba_possession_rate_rows(pl.read_parquet(RATES / "nba_possessions_2026.parquet"), _schedule(2026))
    out, draws = sim_mod._simulate_season(
        games, sim_mod.as_of_game_model(rows), n_sims=300, seed=3, season_games=82, cup=_cup(2026, as_of)
    )
    assert draws is not None
    assert (draws.semifinal.sum(1) == 4).all() and len(np.unique(draws.semifinal, axis=0)) == 1  # QFs played
    np.testing.assert_allclose((out["proj_wins"] + out["proj_losses"]).to_numpy(), 82.0)
    assert set((out["wins"] + out["losses"] + out["games_remaining"]).to_list()) == {82}


def test_rejects_bad_inputs() -> None:
    with pytest.raises(ValueError, match="n_sims"):
        _cup_sim(2026, n_sims=0)
    games = sim_mod._regular_games(_schedule(2026), None).select(*sim_mod._GAME_COLS)
    model = sim_mod.as_of_game_model(_EMPTY_POSSESSIONS.pipe(tsr.nba_possession_rate_rows, _schedule(2026)))
    with pytest.raises(ValueError, match="Cup games to play"):  # the Cup unplayed, the season played
        sim_mod.simulate_pro_season(games, model, n_sims=5, cup=_cup(2026, dt.date(2025, 11, 1)))
    with pytest.raises(ValueError, match="NBA only"):
        sim_mod.simulate_pro_season(games, model, league_id="10", n_sims=5, cup=_cup(2026))
    games = cup_mod.cup_games(pl.read_parquet(CUP / "nba_cup_schedule_2026.parquet"))
    with pytest.raises(ValueError, match="six groups"):
        cup_mod.prepare_cup(games.filter(pl.col("cup_group") != "East Group A"))
    with pytest.raises(ValueError, match="first NBA Cup"):
        cup_mod.nba_cup_standings(2023, cup_schedule=pl.read_parquet(CUP / "nba_cup_schedule_2024.parquet"))
