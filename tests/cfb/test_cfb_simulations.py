"""Offline tests for the CFB season simulation engine (nflseedR-style port).

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe).
No network access — everything runs on the committed toy fixture.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_simulations import cfb_compute_results, cfb_simulations

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_toy"

N_SIMS = 100
SEED = 42


def _toy_unplayed() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Toy fixture with every result nulled — a fresh season to simulate."""
    games = pl.read_csv(FIXTURE_DIR / "toy_games.csv").with_columns(pl.lit(None, dtype=pl.Float64).alias("result"))
    teams = pl.read_csv(FIXTURE_DIR / "toy_teams.csv")
    return games, teams


@pytest.fixture(scope="module")
def sim_out() -> Dict[str, Any]:
    games, teams = _toy_unplayed()
    return cfb_simulations(games, teams, simulations=N_SIMS, seed=SEED, playoff_seeds=4)


class TestSimulationSmoke:
    def test_returns_expected_frames(self, sim_out: Dict[str, Any]) -> None:
        assert set(sim_out.keys()) == {"standings", "games", "overall", "game_summary"}
        for frame in sim_out.values():
            assert isinstance(frame, pl.DataFrame)

    def test_all_games_filled_and_stacked(self, sim_out: Dict[str, Any]) -> None:
        games = sim_out["games"]
        assert games.filter(pl.col("result").is_null()).height == 0
        assert games["sim"].n_unique() == N_SIMS
        # postseason rounds were generated (CFP field of 4 -> 3 POST games/sim)
        assert games.filter(pl.col("game_type") == "POST").height == 3 * N_SIMS

    def test_deterministic_with_fixed_seed(self) -> None:
        games, teams = _toy_unplayed()
        a = cfb_simulations(games, teams, simulations=20, seed=7, playoff_seeds=4)
        b = cfb_simulations(games, teams, simulations=20, seed=7, playoff_seeds=4)
        assert a["standings"].equals(b["standings"])
        assert a["games"].equals(b["games"])
        assert a["overall"].equals(b["overall"])

    def test_probabilities_in_unit_interval(self, sim_out: Dict[str, Any]) -> None:
        overall = sim_out["overall"]
        for col in ("won_conf", "made_playoff", "first_round_bye", "won_cfp", "win_pct"):
            s = overall[col]
            assert (s >= 0.0).all() and (s <= 1.0).all(), col

    def test_probability_mass_conserved(self, sim_out: Dict[str, Any]) -> None:
        overall = sim_out["overall"]
        # exactly one national champion per sim
        assert overall["won_cfp"].sum() == pytest.approx(1.0)
        # exactly one champion per conference per sim
        for conf in ("Alpha", "Beta"):
            conf_mass = overall.filter(pl.col("conference") == conf)["won_conf"].sum()
            assert conf_mass == pytest.approx(1.0), conf
        # independents can never win a conference
        ind = overall.filter(pl.col("conference") == "FBS Independents")
        assert ind["won_conf"].sum() == 0.0
        # the field has exactly playoff_seeds teams per sim
        assert overall["made_playoff"].sum() == pytest.approx(4.0)

    def test_standings_carry_seeds_and_champs(self, sim_out: Dict[str, Any]) -> None:
        st = sim_out["standings"]
        assert st.height == 9 * N_SIMS
        per_sim = st.group_by("sim").agg(
            pl.col("seed").is_not_null().sum().alias("n_field"),
            (pl.col("conf_champ") == True).sum().alias("n_champs"),  # noqa: E712
        )
        assert per_sim["n_field"].unique().to_list() == [4]
        assert per_sim["n_champs"].unique().to_list() == [2]

    def test_played_results_are_preserved(self) -> None:
        games, teams = _toy_unplayed()
        # pin week 1 results; the simulation must not overwrite them
        games = games.with_columns(pl.when(pl.col("week") == 1).then(3.0).otherwise(pl.col("result")).alias("result"))
        out = cfb_simulations(games, teams, simulations=10, seed=1, playoff_seeds=4)
        wk1 = out["games"].filter((pl.col("week") == 1) & (pl.col("game_type") == "REG"))
        assert wk1["result"].unique().to_list() == [3.0]

    def test_sim_include_reg_stops_before_postseason(self) -> None:
        games, teams = _toy_unplayed()
        out = cfb_simulations(games, teams, simulations=10, seed=1, sim_include="REG")
        assert out["games"].filter(pl.col("game_type") == "POST").height == 0
        # REG games simulated, CONF_CHAMP left unplayed
        assert out["games"].filter((pl.col("game_type") == "REG") & pl.col("result").is_null()).height == 0
        assert "seed" not in out["standings"].columns

    def test_invalid_sim_include_raises(self) -> None:
        games, teams = _toy_unplayed()
        with pytest.raises(ValueError, match="sim_include"):
            cfb_simulations(games, teams, simulations=1, sim_include="DRAFT")

    def test_custom_compute_results_is_pluggable(self) -> None:
        games, teams = _toy_unplayed()

        def home_always_wins(
            teams_df: pl.DataFrame, games_df: pl.DataFrame, week_num: int, **kwargs: Any
        ) -> Dict[str, pl.DataFrame]:
            games_df = games_df.with_columns(
                pl.when((pl.col("week") == week_num) & pl.col("result").is_null())
                .then(7.0)
                .otherwise(pl.col("result"))
                .alias("result")
            )
            return {"teams": teams_df, "games": games_df}

        out = cfb_simulations(games, teams, compute_results=home_always_wins, simulations=5, seed=3, playoff_seeds=4)
        reg = out["games"].filter(pl.col("game_type") == "REG")
        assert reg["result"].unique().to_list() == [7.0]


class TestComputeResults:
    def test_fills_only_target_week(self) -> None:
        import numpy as np

        games, teams = _toy_unplayed()
        games = games.with_columns(pl.lit(1, dtype=pl.Int64).alias("sim"))
        teams = teams.with_columns(pl.lit(1, dtype=pl.Int64).alias("sim"))
        out = cfb_compute_results(teams, games, 1, rng=np.random.default_rng(0))
        g = out["games"]
        assert g.filter((pl.col("week") == 1) & pl.col("result").is_null()).height == 0
        assert g.filter((pl.col("week") != 1) & pl.col("result").is_not_null()).height == 0
        # ELO ratings initialized and carried on the teams frame
        assert "elo" in out["teams"].columns

    def test_initial_elo_dict_is_applied(self) -> None:
        import numpy as np

        games, teams = _toy_unplayed()
        games = games.with_columns(pl.lit(1, dtype=pl.Int64).alias("sim"))
        teams = teams.with_columns(pl.lit(1, dtype=pl.Int64).alias("sim"))
        out = cfb_compute_results(teams, games, 1, rng=np.random.default_rng(0), elo={"A1": 2000.0})
        t = out["teams"]
        # unlisted teams start at 1500; only week-1 participants shift
        b3 = t.filter(pl.col("team") == "B3")["elo"][0]
        assert b3 != 1500.0  # B3 played week 1 -> shifted
        i1 = t.filter(pl.col("team") == "I1")["elo"][0]
        assert i1 == 1500.0  # I1 idle in week 1 -> untouched default
