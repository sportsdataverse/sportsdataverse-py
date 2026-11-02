"""Offline tests for the CFB season simulation engine (nflseedR-style port).

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe).
No network access — everything runs on the committed toy fixture.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_simulations import (
    _bracket_slots,
    _simulate_bracket,
    cfb_compute_results,
    cfb_simulations,
)

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


def _away_always_wins(
    teams_df: pl.DataFrame, games_df: pl.DataFrame, week_num: int, **kwargs: Any
) -> Dict[str, pl.DataFrame]:
    """Every unplayed game of the week goes to the away (worse-seeded) team."""
    games_df = games_df.with_columns(
        pl.when((pl.col("week") == week_num) & pl.col("result").is_null())
        .then(-3.0)
        .otherwise(pl.col("result"))
        .alias("result")
    )
    return {"teams": teams_df, "games": games_df}


class TestFixedBracket:
    """12-team CFP bracket is fixed: no reseeding after the first round."""

    @staticmethod
    def _run(n: int = 12) -> pl.DataFrame:
        import numpy as np

        field = pl.DataFrame(
            {"sim": [1] * n, "team": [f"S{s:02d}" for s in range(1, n + 1)], "seed": list(range(1, n + 1))}
        )
        teams = field.select("sim", "team", pl.lit("X").alias("conference"))
        games = pl.DataFrame(
            schema={
                "sim": pl.Int64,
                "week": pl.Int64,
                "game_type": pl.Utf8,
                "home_team": pl.Utf8,
                "away_team": pl.Utf8,
                "result": pl.Float64,
                "neutral": pl.Int64,
            }
        )
        champ, _, games = _simulate_bracket(field, teams, games, _away_always_wins, 20, np.random.default_rng(0))
        assert champ.height == 1
        return games.with_columns(
            pl.col("home_team").str.slice(1).cast(pl.Int64).alias("hi"),
            pl.col("away_team").str.slice(1).cast(pl.Int64).alias("lo"),
        )

    def test_bracket_slots_match_cfp_order(self) -> None:
        slots = _bracket_slots(12)
        order = [seed for seed, _ in sorted(slots.items(), key=lambda kv: kv[1])]
        assert order == [1, 8, 9, 4, 5, 12, 2, 7, 10, 3, 6, 11]

    def test_pairings_every_round(self) -> None:
        # the away team always wins, so the lower seed survives each game: a
        # reseeding bracket would pair QF 1 v 12 and 4 v 9 instead
        games = self._run()
        pairs = {w[0]: sorted(zip(d["hi"], d["lo"])) for w, d in games.group_by("week")}
        assert pairs[20] == [(5, 12), (6, 11), (7, 10), (8, 9)]
        assert pairs[21] == [(1, 9), (2, 10), (3, 11), (4, 12)]
        assert pairs[22] == [(9, 12), (10, 11)]  # 1/4 side v 2/3 side
        assert pairs[23] == [(11, 12)]

    def test_first_round_on_campus_then_neutral(self) -> None:
        games = self._run()
        neutral = dict(games.group_by("week").agg(pl.col("neutral").unique()).sort("week").iter_rows())
        assert neutral == {20: [0], 21: [1], 22: [1], 23: [1]}

    def test_power_of_two_field_has_no_byes(self) -> None:
        games = self._run(4)
        pairs = {w[0]: sorted(zip(d["hi"], d["lo"])) for w, d in games.group_by("week")}
        assert pairs[20] == [(1, 4), (2, 3)]
        assert pairs[21] == [(3, 4)]


class TestTiebreakerDataPassThrough:
    """`tiebreaker_data` reaches every standings call; `season`-keyed input picks the rules."""

    @staticmethod
    def _acc_epoch(season: int) -> tuple[pl.DataFrame, pl.DataFrame, Dict[str, pl.DataFrame]]:
        # A 2-0 and B 2-0 never met: the 2024 ACC cascade reaches pooled
        # opponents' win pct (A), the 2026 one goes h2h -> analytics (B)
        rows = [(1, "A", "C"), (2, "A", "D"), (1, "B", "C"), (2, "B", "E"), (3, "C", "D"), (4, "C", "E"), (3, "D", "E")]
        games = pl.DataFrame(
            {
                "season": [season] * len(rows),
                "week": [r[0] for r in rows],
                "game_type": ["REG"] * len(rows),
                "home_team": [r[1] for r in rows],
                "away_team": [r[2] for r in rows],
                "result": [7.0] * len(rows),
            }
        )
        teams = pl.DataFrame({"team": list("ABCDE"), "conference": ["ACC"] * 5})
        ratings = pl.DataFrame({"team": ["A", "B", "C"], "rating": [1.0, 2.0, 0.5]})
        return games, teams, {"analytics_ratings": ratings}

    @staticmethod
    def _leaders(out: Dict[str, Any]) -> set:
        return set(out["standings"].filter(pl.col("conf_rank") == 1)["team"].to_list())

    def test_analytics_rung_decides_every_sim(self) -> None:
        games, teams, data = self._acc_epoch(2026)
        with_data = cfb_simulations(games, teams, simulations=40, seed=5, sim_include="REG", tiebreaker_data=data)
        without = cfb_simulations(games, teams, simulations=40, seed=5, sim_include="REG")
        assert self._leaders(with_data) == {"B"}
        # without the ratings the analytics rung is skipped -> a draw (the 2026
        # pool rule also admits 2-2 C into the leaders' tier)
        assert len(self._leaders(without)) > 1

    def test_season_keyed_input_selects_registry_epoch(self) -> None:
        games, teams, data = self._acc_epoch(2025)
        out = cfb_simulations(games, teams, simulations=10, seed=5, sim_include="REG", tiebreaker_data=data)
        assert self._leaders(out) == {"A"}

    def test_conf_champ_game_uses_tiebreaker_data(self) -> None:
        # the generated CCG pairs the analytics-ordered top two in every sim
        games, teams, data = self._acc_epoch(2026)
        out = cfb_simulations(games, teams, simulations=20, seed=5, sim_include="CONF", tiebreaker_data=data)
        ccg = out["games"].filter(pl.col("game_type") == "CONF_CHAMP")
        assert ccg.height == 20
        assert set(zip(ccg["home_team"], ccg["away_team"])) == {("B", "A")}
