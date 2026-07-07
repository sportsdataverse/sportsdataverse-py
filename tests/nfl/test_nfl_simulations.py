"""Tests for sportsdataverse.nfl.nfl_simulations (nflseedR v2 simulation port).

Offline only -- drives the engine on the committed 2023 schedule fixture
(tests/fixtures/seedr/games_2023.csv) with results blanked from week 10 on.
Port provenance: nflseedR ``simulations.R`` L140-409,
``simulations_simulate_chunks.R`` L1-284, ``simulations_utils.R`` L1-290
(``nflseedR_compute_results`` = default ELO results generator).
"""

from pathlib import Path

import polars as pl
import pytest

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "seedr"

N_SIMS = 100
BLANK_FROM_WEEK = 10


def _partial_season() -> pl.DataFrame:
    """2023 REG schedule with results wiped from week 10 onwards."""
    games = pl.read_csv(FIXTURE_DIR / "games_2023.csv")
    games = games.filter(pl.col("game_type") == "REG")
    return games.with_columns(
        pl.when(pl.col("week") >= BLANK_FROM_WEEK).then(None).otherwise(pl.col("result")).alias("result")
    ).select(
        "season",
        "game_type",
        "week",
        "away_team",
        "home_team",
        "away_rest",
        "home_rest",
        "location",
        "result",
    )


@pytest.fixture(scope="module")
def sim_out() -> dict:
    from sportsdataverse.nfl import nfl_simulations

    return nfl_simulations(_partial_season(), simulations=N_SIMS, seed=42)


class TestSimulationSmoke:
    def test_output_keys(self, sim_out: dict) -> None:
        assert set(sim_out.keys()) == {
            "standings",
            "games",
            "overall",
            "team_wins",
            "game_summary",
        }
        for frame in sim_out.values():
            assert isinstance(frame, pl.DataFrame)

    def test_standings_shape(self, sim_out: dict) -> None:
        st = sim_out["standings"]
        assert st.height == 32 * N_SIMS
        assert st["sim"].n_unique() == N_SIMS
        # every sim resolves all 32 draft picks exactly once
        counts = st.group_by("sim").agg(
            pl.col("draft_rank").n_unique().alias("n"),
            pl.col("draft_rank").min().alias("mn"),
            pl.col("draft_rank").max().alias("mx"),
        )
        assert bool((counts["n"] == 32).all())
        assert bool((counts["mn"] == 1).all())
        assert bool((counts["mx"] == 32).all())
        # exits are translated to characters and every sim has one SB winner
        winners = st.filter(pl.col("exit") == "SB_WIN")
        assert winners.height == N_SIMS

    def test_probabilities_consistent(self, sim_out: dict) -> None:
        ov = sim_out["overall"]
        assert ov.height == 32
        for col in ("playoff", "div1", "seed1", "won_conf", "won_sb", "draft1", "draft5"):
            vals = ov[col]
            assert float(vals.min()) >= 0.0
            assert float(vals.max()) <= 1.0
        # per conference: exactly playoff_seeds playoff teams and one 1-seed
        per_conf = ov.group_by("conf").agg(
            pl.col("playoff").sum().alias("playoff_sum"),
            pl.col("seed1").sum().alias("seed1_sum"),
        )
        for row in per_conf.to_dicts():
            assert row["playoff_sum"] == pytest.approx(7.0)
            assert row["seed1_sum"] == pytest.approx(1.0)
        # league-wide: one SB winner, two conference champions, one 1st pick
        assert float(ov["won_sb"].sum()) == pytest.approx(1.0)
        assert float(ov["won_conf"].sum()) == pytest.approx(2.0)
        assert float(ov["draft1"].sum()) == pytest.approx(1.0)
        # each division has one winner
        per_div = ov.group_by("division").agg(pl.col("div1").sum().alias("s"))
        for row in per_div.to_dicts():
            assert row["s"] == pytest.approx(1.0)

    def test_known_results_untouched(self, sim_out: dict) -> None:
        """Played games keep their real result in every simulated season."""
        games = sim_out["games"]
        real = _partial_season().filter(pl.col("result").is_not_null())
        merged = games.join(
            real.select("week", "away_team", "home_team", pl.col("result").alias("real")),
            on=["week", "away_team", "home_team"],
            how="inner",
        )
        assert merged.height == real.height * N_SIMS
        assert merged.filter(pl.col("result") != pl.col("real")).height == 0

    def test_no_postseason_ties(self, sim_out: dict) -> None:
        post = sim_out["games"].filter(pl.col("game_type") != "REG")
        assert post.height == 13 * N_SIMS  # 6 WC + 4 DIV + 2 CON + 1 SB
        assert post["result"].null_count() == 0
        assert post.filter(pl.col("result") == 0).height == 0
        assert post["home_team"].null_count() == 0
        assert post["away_team"].null_count() == 0

    def test_team_wins_and_game_summary(self, sim_out: dict) -> None:
        tw = sim_out["team_wins"]
        assert tw.height == 32 * (17 * 2 + 1)  # 0..17 in half-win steps
        assert float(tw["over_prob"].min()) >= 0.0
        assert float(tw["over_prob"].max()) <= 1.0
        gs = sim_out["game_summary"]
        pcts = gs.select((pl.col("away_percentage") + pl.col("home_percentage")).alias("s"))
        assert pcts.filter((pl.col("s") - 1.0).abs() > 1e-9).height == 0

    def test_deterministic_under_seed(self) -> None:
        from sportsdataverse.nfl import nfl_simulations

        a = nfl_simulations(_partial_season(), simulations=20, seed=7)
        b = nfl_simulations(_partial_season(), simulations=20, seed=7)
        assert a["overall"].equals(b["overall"])
        assert a["standings"].equals(b["standings"])


class TestRegOnly:
    def test_sim_include_reg(self) -> None:
        from sportsdataverse.nfl import nfl_simulations

        out = nfl_simulations(_partial_season(), simulations=10, seed=1, sim_include="REG")
        st = out["standings"]
        assert st.height == 32 * 10
        assert "draft_rank" not in st.columns
        ov = out["overall"]
        assert ov["won_sb"].null_count() == 32
        assert ov["won_conf"].null_count() == 32
        assert ov["draft1"].null_count() == 32


class TestComputeResults:
    def test_fills_only_target_week(self) -> None:
        import numpy as np

        from sportsdataverse.nfl import nfl_compute_results

        games = pl.DataFrame(
            {
                "sim": [1, 1, 1],
                "game_type": ["REG", "REG", "REG"],
                "week": ["1", "1", "2"],
                "away_team": ["BUF", "KC", "BUF"],
                "home_team": ["MIA", "DEN", "NYJ"],
                "away_rest": [7, 7, 7],
                "home_rest": [7, 7, 7],
                "location": ["Home", "Home", "Home"],
                "result": pl.Series([3, None, None], dtype=pl.Int64),
            }
        )
        teams = pl.DataFrame({"sim": [1] * 5, "team": ["BUF", "MIA", "KC", "DEN", "NYJ"]})
        rng = np.random.default_rng(0)
        out = nfl_compute_results(teams, games, "1", rng=rng)
        g = out["games"]
        # known result untouched, week-1 gap filled, week-2 still missing
        assert g.filter(pl.col("week") == "1")["result"].null_count() == 0
        assert g.row(0, named=True)["result"] == 3
        assert g.filter(pl.col("week") == "2")["result"].null_count() == 1
        # results are integers rounded away from zero (never 0 from rounding)
        filled = g.row(1, named=True)["result"]
        assert isinstance(filled, int)
        # elo carried on teams for next week
        assert "elo" in out["teams"].columns
        assert out["teams"]["elo"].null_count() == 0

    def test_elo_override_and_carry(self) -> None:
        import numpy as np

        from sportsdataverse.nfl import nfl_compute_results

        games = pl.DataFrame(
            {
                "sim": [1],
                "game_type": ["REG"],
                "week": ["1"],
                "away_team": ["BUF"],
                "home_team": ["MIA"],
                "away_rest": [7],
                "home_rest": [7],
                "location": ["Home"],
                "result": pl.Series([None], dtype=pl.Int64),
            }
        )
        teams = pl.DataFrame({"sim": [1, 1], "team": ["BUF", "MIA"]})
        rng = np.random.default_rng(3)
        out = nfl_compute_results(teams, games, "1", rng=rng, elo={"BUF": 1700.0, "MIA": 1300.0})
        t = out["teams"].sort("team")
        result = out["games"]["result"][0]
        elo = dict(zip(t["team"].to_list(), t["elo"].to_list()))
        # zero-sum elo update, shifted from the provided initial ratings
        assert elo["BUF"] + elo["MIA"] == pytest.approx(3000.0)
        if result > 0:  # home (MIA) upset -> MIA gains
            assert elo["MIA"] > 1300.0
        else:
            assert elo["BUF"] > 1700.0
