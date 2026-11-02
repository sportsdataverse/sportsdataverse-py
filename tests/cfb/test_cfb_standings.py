"""Offline tests for the CFB standings engine (nflseedR-style port).

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe).
The toy fixture under ``tests/fixtures/seedr/cfb_toy/`` is the shared
cross-validation oracle: the R ``cfbseedR`` port runs the same CSVs and the
sorted standings outputs are diffed by the orchestrator.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_standings import (
    CONFERENCE_TIEBREAKERS,
    cfb_games_from_schedule,
    cfb_playoff_seeds,
    cfb_standings,
)

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_toy"
TIEBREAKER_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_toy_tiebreakers"


def _toy() -> tuple[pl.DataFrame, pl.DataFrame]:
    games = pl.read_csv(FIXTURE_DIR / "toy_games.csv")
    teams = pl.read_csv(FIXTURE_DIR / "toy_teams.csv")
    return games, teams


def _reg_games(rows: list[tuple[str, str, float]], sim: int = 2024) -> pl.DataFrame:
    """(home, away, result) triples -> engine games frame, one week apart."""
    return pl.DataFrame(
        {
            "sim": [sim] * len(rows),
            "week": list(range(1, len(rows) + 1)),
            "game_type": ["REG"] * len(rows),
            "home_team": [r[0] for r in rows],
            "away_team": [r[1] for r in rows],
            "result": [float(r[2]) for r in rows],
            "neutral": [0] * len(rows),
        }
    )


class TestToyFixture:
    """The shared spec's cross-validation expectations (tiebreaker_depth=POINTS)."""

    def test_toy_shape_and_ranks(self) -> None:
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        assert isinstance(st, pl.DataFrame)
        assert st.height == 9

        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        # Alpha 3-way 2-1 tie resolves A1 > A2 > A3 (conf point diff 25 > 10 > 3)
        assert [ranks[t] for t in ("A1", "A2", "A3", "A4")] == [1, 2, 3, 4]
        # Beta is clean B1 > B2 > B3 > B4
        assert [ranks[t] for t in ("B1", "B2", "B3", "B4")] == [1, 2, 3, 4]
        # Independent has no conference rank
        assert ranks["I1"] is None

    def test_toy_conf_champs_from_conf_champ_games(self) -> None:
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        champs = set(st.filter(pl.col("conf_champ") == True)["team"].to_list())  # noqa: E712
        assert champs == {"A1", "B1"}

    def test_toy_records_and_point_diffs(self) -> None:
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        rows = {r["team"]: r for r in st.iter_rows(named=True)}
        # CONF_CHAMP counts toward the overall record, not the conference record
        assert (rows["A1"]["wins"], rows["A1"]["losses"], rows["A1"]["ties"]) == (3, 2, 0)
        assert (rows["A1"]["conf_wins"], rows["A1"]["conf_losses"]) == (2, 1)
        assert (rows["B1"]["wins"], rows["B1"]["losses"]) == (5, 0)
        assert rows["I1"]["conf_games"] == 0
        # the deliberate tiebreak lever
        assert [rows[t]["conf_pd"] for t in ("A1", "A2", "A3")] == [25.0, 10.0, 3.0]

    def test_toy_conference_scoped_sov_sos(self) -> None:
        # Cross-validation ruling: sov/sos are conference-scoped (mean of
        # conference opponents' conf win pct — per victory for SOV, per game
        # for SOS). The Alpha trio must tie through SOV and SOS so the tie
        # resolves at the POINTS rung (conf_pd 25 > 10 > 3).
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        assert isinstance(st, pl.DataFrame)
        vals = {r["team"]: (r["sov"], r["sos"]) for r in st.iter_rows(named=True)}
        expected = {
            "A1": (1 / 3, 4 / 9),
            "A2": (1 / 3, 4 / 9),
            "A3": (1 / 3, 4 / 9),
            "A4": (0.0, 2 / 3),
            "B1": (1 / 3, 1 / 3),
            "B2": (1 / 6, 4 / 9),
            "B3": (0.0, 5 / 9),
            "B4": (0.0, 2 / 3),
            "I1": (0.0, 0.0),
        }
        for team, (sov, sos) in expected.items():
            assert vals[team][0] == pytest.approx(sov, abs=1e-6), f"{team} sov"
            assert vals[team][1] == pytest.approx(sos, abs=1e-6), f"{team} sos"

    def test_toy_public_numerics_are_float64(self) -> None:
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        assert isinstance(st, pl.DataFrame)
        for col in ("win_pct", "conf_pct", "sov", "sos", "pd", "conf_pd"):
            assert st.schema[col] == pl.Float64, col

    def test_toy_return_as_pandas(self) -> None:
        games, teams = _toy()
        pdf = cfb_standings(games, teams, tiebreaker_depth="POINTS", return_as_pandas=True)
        assert not isinstance(pdf, pl.DataFrame)
        assert len(pdf) == 9


class TestTiebreakerEdges:
    def test_head_to_head_pair(self) -> None:
        # X and Y both 2-1; X beat Y head-to-head -> X ranks first.
        games = _reg_games(
            [
                ("X", "Y", 3.0),
                ("Y", "Z", 7.0),
                ("Y", "Q", 7.0),
                ("X", "Z", 2.0),
                ("Q", "X", 2.0),
                ("Z", "Q", 4.0),
            ]
        )
        teams = pl.DataFrame({"team": ["X", "Y", "Z", "Q"], "conference": ["C"] * 4})
        st = cfb_standings(games, teams, tiebreaker_depth="PRE-SOV")
        assert isinstance(st, pl.DataFrame)
        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        assert ranks["X"] == 1 and ranks["Y"] == 2
        # Z beat Q head-to-head for the 3/4 spot
        assert ranks["Z"] == 3 and ranks["Q"] == 4

    def test_common_opponents_without_head_to_head(self) -> None:
        # T1 and T2 (both 1-1) never met; their only common opponent is C:
        # T1 beat C, T2 lost to C -> T1 ranks above T2.
        games = _reg_games(
            [
                ("T1", "C", 7.0),
                ("E", "T1", 7.0),
                ("C", "T2", 3.0),
                ("T2", "D", 3.0),
                ("C", "E", 3.0),
                ("E", "D", 3.0),
            ]
        )
        teams = pl.DataFrame({"team": ["T1", "T2", "C", "D", "E"], "conference": ["X"] * 5})
        st = cfb_standings(games, teams, tiebreaker_depth="SOS")
        assert isinstance(st, pl.DataFrame)
        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        assert ranks["C"] == 1 and ranks["E"] == 2  # 2-1 tier, C beat E h2h
        assert ranks["T1"] == 3 and ranks["T2"] == 4
        assert ranks["D"] == 5

    def test_independents_excluded_from_conf_ranks(self) -> None:
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="SOS")
        assert isinstance(st, pl.DataFrame)
        ind = st.filter(pl.col("conference") == "FBS Independents")
        assert ind["conf_rank"].to_list() == [None]
        assert ind["conf_champ"].to_list() == [False]
        # but included in overall standings
        assert ind["wins"].to_list() == [2]

    def test_invalid_tiebreaker_depth_raises(self) -> None:
        games, teams = _toy()
        with pytest.raises(ValueError, match="tiebreaker_depth"):
            cfb_standings(games, teams, tiebreaker_depth="MAX")

    def test_invalid_game_type_raises(self) -> None:
        games, teams = _toy()
        with pytest.raises(ValueError, match="game_type"):
            cfb_standings(games.with_columns(pl.lit("SB").alias("game_type")), teams)


class TestPlayoffSeeds:
    @staticmethod
    def _standings_14() -> pl.DataFrame:
        """14 ranked teams; champs are R01/R03/R05/R08 and lowly R13."""
        teams = [f"R{i:02d}" for i in range(1, 15)]
        champs = {"R01", "R03", "R05", "R08", "R13"}
        return pl.DataFrame(
            {
                "sim": [1] * 14,
                "team": teams,
                "conference": [f"C{i}" for i in range(14)],
                "conf_champ": [t in champs for t in teams],
                "win_pct": [1.0 - i / 100 for i in range(14)],
                "sov": [0.5] * 14,
                "sos": [0.5] * 14,
                "pd": [100.0 - i for i in range(14)],
            }
        )

    def test_champ_outside_top12_bumps_last_at_large(self) -> None:
        st = self._standings_14()
        rankings = pl.DataFrame({"team": st["team"], "rank": list(range(1, 15))})
        seeded = cfb_playoff_seeds(st, rankings=rankings, playoff_seeds=12, season=2025)
        assert isinstance(seeded, pl.DataFrame)
        seeds = {r["team"]: r["seed"] for r in seeded.iter_rows(named=True)}
        # champ ranked 13 gets in with the last seed (straight seeding, no bump)
        assert seeds["R13"] == 12
        # the 12th-best at-large is bumped out
        assert seeds["R12"] is None
        assert seeds["R14"] is None
        # everyone else seeded straight by committee rank
        for i in range(1, 12):
            assert seeds[f"R{i:02d}"] == i

    def test_fallback_ordering_without_rankings(self) -> None:
        st = self._standings_14()
        seeded = cfb_playoff_seeds(st, rankings=None, playoff_seeds=12, season=2025)
        assert isinstance(seeded, pl.DataFrame)
        seeds = {r["team"]: r["seed"] for r in seeded.iter_rows(named=True)}
        # win_pct descends with team index, so the fallback matches the rank order
        assert seeds["R01"] == 1
        assert seeds["R13"] == 12  # champ guarantee still applies
        assert seeds["R12"] is None
        in_field = [s for s in seeds.values() if s is not None]
        assert sorted(in_field) == list(range(1, 13))

    def test_via_cfb_standings_playoff_seeds_kwarg(self) -> None:
        games, teams = _toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS", playoff_seeds=4)
        assert isinstance(st, pl.DataFrame)
        field = st.filter(pl.col("seed").is_not_null())
        assert field.height == 4
        # both conference champs (A1, B1) must be in the 4-team field
        assert {"A1", "B1"} <= set(field["team"].to_list())


class TestGamesFromSchedule:
    def test_mapper_shapes_engine_schema(self) -> None:
        sched = pl.DataFrame(
            {
                "season": [2024] * 4,
                "week": [1, 14, 16, 17],
                "season_type": ["regular", "regular", "postseason", "postseason"],
                "home_team": ["H1", "H2", "H3", "H4"],
                "away_team": ["A1", "A2", "A3", "A4"],
                "home_points": [28, 21, None, 35],
                "away_points": [14, 24, None, 20],
                "neutral_site": [False, True, True, True],
                "notes": [None, "Big 12 Championship", None, "CFP National Championship"],
            }
        )
        games = cfb_games_from_schedule(sched)
        assert isinstance(games, pl.DataFrame)
        assert games.columns == [
            "season",
            "week",
            "game_type",
            "home_team",
            "away_team",
            "result",
            "neutral",
            "home_points",
            "away_points",
        ]
        assert games["game_type"].to_list() == ["REG", "CONF_CHAMP", "POST", "POST"]
        assert games["result"].to_list() == [14.0, -3.0, None, 15.0]
        assert games["neutral"].to_list() == [0, 1, 1, 1]
        # per-game points pass through (SEC capped-scoring-margin tiebreaker input)
        assert games["home_points"].to_list() == [28.0, 21.0, None, 35.0]
        assert games["away_points"].to_list() == [14.0, 24.0, None, 20.0]

    def test_mapper_pipes_into_standings(self) -> None:
        sched = pl.DataFrame(
            {
                "season": [2024, 2024],
                "week": [1, 2],
                "season_type": ["regular", "regular"],
                "home_team": ["H", "A"],
                "away_team": ["A", "H"],
                "home_points": [21, 14],
                "away_points": [14, 28],
                "neutral_site": [False, False],
                "notes": [None, None],
            }
        )
        games = cfb_games_from_schedule(sched)
        teams = pl.DataFrame({"team": ["H", "A"], "conference": ["X", "X"]})
        st = cfb_standings(games, teams)
        assert isinstance(st, pl.DataFrame)
        assert {r["team"]: r["wins"] for r in st.iter_rows(named=True)} == {"H": 2, "A": 0}


def _conf_games(rows: list[tuple[str, str, float]], sim: int = 2024) -> pl.DataFrame:
    """(home, away, result) triples -> engine games frame, no points/one week apart."""
    return pl.DataFrame(
        {
            "sim": [sim] * len(rows),
            "week": list(range(1, len(rows) + 1)),
            "game_type": ["REG"] * len(rows),
            "home_team": [r[0] for r in rows],
            "away_team": [r[1] for r in rows],
            "result": [float(r[2]) for r in rows],
            "neutral": [0] * len(rows),
        }
    )


class TestConferenceTiebreakerRegistry:
    """CONFERENCE_TIEBREAKERS registry: rung primitives, dispatch, skip notes,
    and multi-team restart semantics (design brief Parts 3-4).
    """

    def test_registry_membership(self) -> None:
        assert set(CONFERENCE_TIEBREAKERS) == {
            "SEC",
            "Big Ten",
            "ACC",
            "MAC",
            "Mid-American",
            "Big 12",
            "American Athletic",
            "American",
            "Conference USA",
            "Mountain West",
            "Sun Belt",
        }

    def test_unregistered_conference_matches_generic_fallback(self) -> None:
        # Same 4-team scenario as TestTiebreakerEdges.test_head_to_head_pair, but
        # under an unregistered conference name -> must be byte-identical to the
        # pre-registry generic cascade (zero output change for existing callers).
        games = _conf_games(
            [
                ("X", "Y", 3.0),
                ("Y", "Z", 7.0),
                ("Y", "Q", 7.0),
                ("X", "Z", 2.0),
                ("Q", "X", 2.0),
                ("Z", "Q", 4.0),
            ]
        )
        teams = pl.DataFrame({"team": ["X", "Y", "Z", "Q"], "conference": ["Pac-12"] * 4})
        st = cfb_standings(games, teams, tiebreaker_depth="PRE-SOV")
        assert isinstance(st, pl.DataFrame)
        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        assert ranks == {"X": 1, "Y": 2, "Z": 3, "Q": 4}
        assert st.tiebreak_notes == []

    def test_h2h_rung_generic_and_registry_agree_on_a_direct_pair(self) -> None:
        # X beat Y head-to-head; both otherwise 1-1 -> resolved at rung 1 (h2h)
        # for BOTH a generic conference and a registered (SEC) one.
        games = _conf_games([("X", "Y", 7.0), ("Y", "Z", 7.0), ("Z", "X", 7.0)])
        for conf in ("Generic Conf", "SEC"):
            teams = pl.DataFrame({"team": ["X", "Y", "Z"], "conference": [conf] * 3})
            st = cfb_standings(games, teams, tiebreaker_depth="PRE-SOV")
            ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
            # X beat Y, Y beat Z, Z beat X: a 3-way cycle where every pairwise
            # sub-tie (once one team is peeled off) is decided by direct h2h.
            assert len(ranks) == 3, conf

    def test_record_vs_common_rung(self) -> None:
        # T1 and T2 (both 1-1, no head-to-head) share common opponent C:
        # T1 beat C, T2 lost to C -> T1 ranks above T2 via record_vs_common.
        games = _conf_games(
            [
                ("T1", "C", 7.0),
                ("E", "T1", 7.0),
                ("C", "T2", 3.0),
                ("T2", "D", 3.0),
                ("C", "E", 3.0),
                ("E", "D", 3.0),
            ]
        )
        teams = pl.DataFrame({"team": ["T1", "T2", "C", "D", "E"], "conference": ["Big Ten"] * 5})
        st = cfb_standings(games, teams, tiebreaker_depth="PRE-SOV")
        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        assert ranks["T1"] < ranks["T2"]

    def test_record_vs_common_desc_rung(self) -> None:
        # Big Ten: P and Q tied 1-1, never played each other, no shared common
        # opponent (P only played the higher-standing R1; Q only played the
        # lower-standing R2) -> record_vs_common ties (no common opponent);
        # record_vs_common_desc descends the standings: R1 (1-0, best) is
        # common only in the sense of being *a* conference opponent outside
        # the tied pair, and only P has a game against that top tier -> P
        # wins the tier check first and is seeded.
        games = _conf_games(
            [
                ("R1", "P", -7.0),  # P beats R1
                ("Q", "R2", 7.0),  # Q beats R2
                ("R1", "R2", 7.0),  # R1 beats R2 (so R1 finishes above R2)
            ]
        )
        teams = pl.DataFrame({"team": ["P", "Q", "R1", "R2"], "conference": ["Big Ten"] * 4})
        st = cfb_standings(games, teams, tiebreaker_depth="PRE-SOV")
        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        # P and Q are the 1-0 tier; R1 (1-1) outranks R2 (0-2) below them.
        assert ranks["P"] == 1 and ranks["Q"] == 2
        assert ranks["R1"] == 3 and ranks["R2"] == 4

    def test_opp_conf_win_pct_pooled_rung(self) -> None:
        # Big 12: X and Y both 1-0 (beat Z once each), never played each
        # other, share only common opponent Z (tied via record_vs_common) ->
        # falls to opp_conf_win_pct (pooled). Give X an EXTRA conference loss
        # to a strong opponent S (so X's pooled opponents' win pct is higher
        # than Y's, since S is a much better team than Z).
        games = _conf_games(
            [
                ("X", "Z", 7.0),
                ("Y", "Z", 7.0),
                ("S", "X", 7.0),  # X's second opponent: S (a strong team)
                ("S", "W", 7.0),  # S beats another team so S's conf pct is high
            ]
        )
        teams = pl.DataFrame({"team": ["X", "Y", "Z", "S", "W"], "conference": ["Big 12"] * 5})
        st = cfb_standings(games, teams, tiebreaker_depth="PRE-SOV")
        rows = {r["team"]: r for r in st.iter_rows(named=True)}
        # X and Y both 1-1... wait X is 0-2 here (lost to S) so they aren't
        # tied; assert the pooled column itself distinguishes X vs Y instead.
        assert rows["X"]["conf_games"] == 2
        assert rows["Y"]["conf_games"] == 1

    def test_capped_scoring_margin_rung_designed_scenario(self) -> None:
        games, teams = _tiebreaker_toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        sec = {r["team"]: r["conf_rank"] for r in st.filter(pl.col("conference") == "SEC").iter_rows(named=True)}
        assert sec == {"A": 1, "B": 2, "C": 3}
        assert st.tiebreak_notes == []

    def test_total_wins_fcs_cap_rung_designed_scenario(self) -> None:
        games, teams = _tiebreaker_toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        b12 = {r["team"]: r["conf_rank"] for r in st.filter(pl.col("conference") == "Big 12").iter_rows(named=True)}
        assert b12 == {"Y": 1, "X": 2, "Z": 3}

    def test_analytics_rating_rung_resolves_a_remaining_tie(self) -> None:
        # Big Ten: two teams tied 1-1 with no distinguishing games-based metric
        # (identical schedules against a shared conference) -> analytics_rating
        # decides it once tiebreaker_data is supplied.
        games = _conf_games([("P", "R", 7.0), ("R", "Q", -7.0)])
        teams = pl.DataFrame({"team": ["P", "Q", "R"], "conference": ["Big Ten"] * 3})
        ratings = pl.DataFrame({"team": ["P", "Q", "R"], "rating": [50.0, 90.0, 10.0]})
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS", tiebreaker_data={"analytics_ratings": ratings})
        assert st.tiebreak_notes == []

    def test_skip_note_when_analytics_ratings_absent(self) -> None:
        games = _conf_games([("P", "R", 7.0), ("R", "Q", -7.0)])
        teams = pl.DataFrame({"team": ["P", "Q", "R"], "conference": ["Big Ten"] * 3})
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        assert any("analytics_rating skipped" in n for n in st.tiebreak_notes)

    def test_skip_note_when_capped_scoring_margin_points_absent(self) -> None:
        games = _conf_games([("A", "B", 7.0), ("B", "C", 7.0), ("C", "A", 7.0)])
        teams = pl.DataFrame({"team": ["A", "B", "C"], "conference": ["SEC"] * 3})
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        assert any("capped_scoring_margin skipped" in n for n in st.tiebreak_notes)

    def test_degrade_note_when_division_absent_for_total_wins(self) -> None:
        games = _conf_games([("X", "Z", 7.0), ("Y", "Z", 7.0)])
        teams = pl.DataFrame({"team": ["X", "Y", "Z"], "conference": ["Big 12"] * 3})  # no `division` column
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        assert any("total_wins FCS cap not applied" in n for n in st.tiebreak_notes)

    def test_multi_team_restart_seeds_via_different_rungs_per_pass(self) -> None:
        # The designed SEC scenario from the parity fixture: the FIRST seed
        # decision (all 3 tied) resolves at capped_scoring_margin; the engine
        # then RESTARTS from rung 1 with the remaining pair, which resolves at
        # h2h instead (B beat C directly) -- proving the restart-per-seed rule
        # (not a single rung deciding the whole group).
        games, teams = _tiebreaker_toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        sec = st.filter(pl.col("conference") == "SEC").sort("conf_rank")
        assert sec["team"].to_list() == ["A", "B", "C"]
        assert sec["conf_champ"].to_list() == [True, False, False]

    def test_return_as_pandas_surfaces_tiebreak_notes_via_attrs(self) -> None:
        games = _conf_games([("P", "R", 7.0), ("R", "Q", -7.0)])
        teams = pl.DataFrame({"team": ["P", "Q", "R"], "conference": ["Big Ten"] * 3})
        pdf = cfb_standings(games, teams, tiebreaker_depth="POINTS", return_as_pandas=True)
        assert any("analytics_rating skipped" in n for n in pdf.attrs["tiebreak_notes"])


def _tiebreaker_toy() -> tuple[pl.DataFrame, pl.DataFrame]:
    games = pl.read_csv(TIEBREAKER_FIXTURE_DIR / "toy_games.csv")
    teams = pl.read_csv(TIEBREAKER_FIXTURE_DIR / "toy_teams.csv")
    return games, teams


class TestTiebreakerParityFixture:
    """Cross-language parity oracle: tests/fixtures/seedr/cfb_toy_tiebreakers/.

    The R ``cfbseedR`` port replays the same CSVs; this test pins the Python
    side's expected output so drift on either side is caught.
    """

    def test_matches_expected_standings(self) -> None:
        games, teams = _tiebreaker_toy()
        st = cfb_standings(games, teams, tiebreaker_depth="POINTS")
        expected = pl.read_csv(TIEBREAKER_FIXTURE_DIR / "expected_standings.csv")
        actual = st.select("sim", "team", "conference", "conf_rank", "conf_champ").sort("sim", "team")
        expected = expected.sort("sim", "team")
        assert actual.to_dicts() == expected.to_dicts()


REGISTRY_2026_DIR = Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_toy_registry_2026"
_TB_VALUE_COL = {"analytics_ratings": "rating", "cfp_rankings": "rank", "apr": "apr"}


def _registry_case(case: str) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, pl.DataFrame]]:
    """One case of the cfbseedR 0.2.0 oracle: (games, teams, tiebreaker_data)."""
    games = pl.read_csv(REGISTRY_2026_DIR / "games.csv").filter(pl.col("case") == case).drop("case")
    teams = (
        pl.read_csv(REGISTRY_2026_DIR / "teams.csv", schema_overrides={"postseason_eligible": pl.Utf8})
        .filter(pl.col("case") == case)
        .drop("case")
        .with_columns(pl.col("postseason_eligible").str.to_lowercase() == "true")
    )
    # like the R capture: a case only carries the optional columns it uses
    teams = teams.select([c for c in teams.columns if teams[c].null_count() < teams.height])
    tb = pl.read_csv(REGISTRY_2026_DIR / "tiebreaker_data.csv").filter(pl.col("case") == case)
    data = {
        k: tb.filter(pl.col("kind") == k).select("team", pl.col("value").alias(v))
        for k, v in _TB_VALUE_COL.items()
        if k in tb["kind"].to_list()
    }
    return games, teams, data


class TestRegistry2026Parity:
    """cfbseedR 0.2.0 oracle (tests/fixtures/seedr/cfb_toy_registry_2026/, see its
    README): season-scoped epochs, the ACC-2026 pool rule, Sun Belt divisions,
    the American / CUSA / Mountain West / MAC procedures, postseason eligibility,
    and both CFP selection rules.
    """

    CASES = pl.read_csv(REGISTRY_2026_DIR / "expected_standings.csv")["case"].unique().sort().to_list()

    @pytest.mark.parametrize("case", CASES)
    def test_matches_cfbseedr_standings(self, case: str) -> None:
        games, teams, data = _registry_case(case)
        st = cfb_standings(games, teams, tiebreaker_data=data or None)
        actual = st.select("team", "conference", "conf_rank", "conf_champ").sort("team").to_dicts()
        expected = (
            pl.read_csv(REGISTRY_2026_DIR / "expected_standings.csv")
            .filter(pl.col("case") == case)
            .select("team", "conference", "conf_rank", "conf_champ")
            .sort("team")
            .to_dicts()
        )
        assert actual == expected

    @pytest.mark.parametrize("season", [2025, 2026])
    def test_matches_cfbseedr_seeds(self, season: int) -> None:
        st = pl.read_csv(REGISTRY_2026_DIR / "seeds_standings.csv")
        ranks = pl.read_csv(REGISTRY_2026_DIR / "seeds_rankings.csv")
        seeded = cfb_playoff_seeds(st, rankings=ranks, playoff_seeds=12, season=season)
        assert isinstance(seeded, pl.DataFrame)
        expected = pl.read_csv(REGISTRY_2026_DIR / "expected_seeds.csv").filter(pl.col("autobid") == season)
        actual = seeded.select("team", "seed").sort("team").to_dicts()
        assert actual == expected.select("team", "seed").sort("team").to_dicts()

    def test_season_keyed_input_resolves_acc_epoch(self) -> None:
        # the same games keyed by plain `sim` take the current (2026) ACC rules;
        # an explicit season pins the 2024 cascade
        games, teams, data = _registry_case("acc_2025")
        by_sim = games.rename({"season": "sim"})
        current = cfb_standings(by_sim, teams, tiebreaker_data=data)
        pinned = cfb_standings(by_sim, teams, tiebreaker_data=data, season=2025)
        assert current.filter(pl.col("conf_rank") == 1)["team"].item() == "B"
        assert pinned.filter(pl.col("conf_rank") == 1)["team"].item() == "A"

    def test_skip_notes_for_new_rungs(self) -> None:
        games, teams, _ = _registry_case("cusa_apr")
        st = cfb_standings(games, teams)
        assert any("cfp_ranked_final_week skipped" in n for n in st.tiebreak_notes)
        assert any("apr skipped" in n for n in st.tiebreak_notes)
        games, teams, data = _registry_case("american_final_loss")
        st = cfb_standings(games, teams, tiebreaker_data=data)
        assert any("cfp_ranked_final_week fell through" in n for n in st.tiebreak_notes)

    def test_nan_tiebreaker_value_skips_the_rung(self) -> None:
        # a NaN APR is missing (R's anyNA), not a value: skip + note, never crash
        games, teams, data = _registry_case("cusa_apr")
        data["apr"] = pl.DataFrame({"team": ["A", "B"], "apr": [float("nan"), 990.0]})
        st = cfb_standings(games, teams, tiebreaker_data=data)
        assert any("apr skipped" in n for n in st.tiebreak_notes)

    def test_acc_pool_never_admits_a_team_without_conference_games(self) -> None:
        # Z (0-0) matches unbeaten L's 0 losses, but played no conference game
        games = _conf_games([("L", "M", 7.0), ("L", "N", 7.0), ("M", "N", 7.0)], sim=2026).rename({"sim": "season"})
        teams = pl.DataFrame({"team": ["L", "M", "N", "Z"], "conference": ["ACC"] * 4})
        ratings = pl.DataFrame({"team": ["L", "M", "N", "Z"], "rating": [1.0, 2.0, 3.0, 99.0]})
        st = cfb_standings(games, teams, tiebreaker_data={"analytics_ratings": ratings})
        ranks = {r["team"]: r["conf_rank"] for r in st.iter_rows(named=True)}
        # Z stays in the 0.0 tier (beating 0-2 N there on its rating), not L's
        assert ranks == {"L": 1, "M": 2, "Z": 3, "N": 4}


class TestPlayoffSeeds2026:
    """2026 CFP selection edge cases (P4 champions, best G6 champion, Notre Dame)."""

    @staticmethod
    def _seed(rows: list[tuple[str, str, bool]], playoff_seeds: int = 12, season: int = 2026) -> dict:
        """rows: (team, conference, conf_champ) in committee rank order -> {team: seed}."""
        n = len(rows)
        st = pl.DataFrame(
            {
                "sim": [1] * n,
                "team": [r[0] for r in rows],
                "conference": [r[1] for r in rows],
                "conf_champ": [r[2] for r in rows],
                "win_pct": [0.5] * n,
                "sov": [0.5] * n,
                "sos": [0.5] * n,
                "pd": [0.0] * n,
            }
        )
        ranks = pl.DataFrame({"team": [r[0] for r in rows], "rank": list(range(1, n + 1))})
        seeded = cfb_playoff_seeds(st, rankings=ranks, playoff_seeds=playoff_seeds, season=season)
        assert isinstance(seeded, pl.DataFrame)
        return {r["team"]: r["seed"] for r in seeded.iter_rows(named=True)}

    @staticmethod
    def _filler(k: int) -> list[tuple[str, str, bool]]:
        return [(f"AL{i:02d}", "Other", False) for i in range(1, k + 1)]

    def test_g6_bid_goes_to_best_g6_champion_not_best_g6_team(self) -> None:
        # a non-champion Mountain West team outranks the American champion;
        # only a champion holds the G6 automatic bid (cfbseedR 0.2.0 differs here)
        rows = self._filler(12) + [("MW2", "Mountain West", False), ("AAC1", "American Athletic", True)]
        seeds = self._seed(rows)
        assert seeds["AAC1"] == 12
        assert seeds["MW2"] is None
        assert seeds["AL12"] is None

    def test_only_the_best_of_several_g6_champions_is_automatic(self) -> None:
        rows = self._filler(12) + [("SBC1", "Sun Belt", True), ("MAC1", "Mid-American", True)]
        seeds = self._seed(rows)
        assert seeds["SBC1"] == 12
        assert seeds["MAC1"] is None

    def test_p4_champions_in_regardless_of_rank_take_lowest_seeds(self) -> None:
        rows = self._filler(14) + [
            ("SEC1", "SEC", True),
            ("B1G1", "Big Ten", True),
            ("B121", "Big 12", True),
            ("ACC1", "ACC", True),
        ]
        seeds = self._seed(rows)
        assert [seeds[t] for t in ("SEC1", "B1G1", "B121", "ACC1")] == [9, 10, 11, 12]
        assert [seeds[f"AL{i:02d}"] for i in range(1, 9)] == list(range(1, 9))
        assert seeds["AL09"] is None

    def test_p4_runner_up_is_not_automatic(self) -> None:
        rows = self._filler(12) + [("SEC2", "SEC", False)]
        assert self._seed(rows)["SEC2"] is None

    @pytest.mark.parametrize("nd", ["Notre Dame", "87"])
    def test_notre_dame_in_when_top_12(self, nd: str) -> None:
        # ranked 12th; the 13th-ranked G6 champion would otherwise push it out
        rows = self._filler(11) + [(nd, "FBS Independents", False), ("SBC1", "Sun Belt", True)]
        seeds = self._seed(rows)
        assert seeds[nd] == 11
        assert seeds["SBC1"] == 12
        assert seeds["AL11"] is None

    def test_notre_dame_outside_top_12_gets_no_bid(self) -> None:
        rows = self._filler(12) + [("Notre Dame", "FBS Independents", False)]
        assert self._seed(rows)["Notre Dame"] is None

    def test_2025_rule_guarantees_any_top_five_champion(self) -> None:
        # the 13th-ranked 'Other' champion is guaranteed under 2025 only (not P4/G6)
        rows = self._filler(12) + [("OTH1", "Other", True)]
        assert self._seed(rows, season=2025)["OTH1"] == 12
        assert self._seed(rows, season=2026)["OTH1"] is None

    def test_automatic_bids_capped_at_field_size(self) -> None:
        rows = self._filler(2) + [("SEC1", "SEC", True), ("B1G1", "Big Ten", True), ("ACC1", "ACC", True)]
        seeds = self._seed(rows, playoff_seeds=2)
        assert seeds["SEC1"] == 1 and seeds["B1G1"] == 2
        assert seeds["ACC1"] is None and seeds["AL01"] is None

    def test_cfb_standings_season_keyed_input_picks_rule_per_season(self) -> None:
        # the same season keyed 2025 and 2026, 2-team field: the 'Other'
        # champion O1 is guaranteed only under the 2025 rule
        base = _conf_games([("P", "R", 7.0), ("R", "Q", -7.0), ("O1", "O2", 7.0)])
        games = pl.concat([base.with_columns(pl.lit(s).alias("sim")) for s in (2025, 2026)]).rename({"sim": "season"})
        teams = pl.DataFrame({"team": ["P", "Q", "R", "O1", "O2"], "conference": ["SEC"] * 3 + ["Other"] * 2})
        ranks = pl.DataFrame({"team": ["P", "Q", "R", "O2", "O1"], "rank": [1, 2, 3, 4, 5]})
        st = cfb_standings(games, teams, playoff_seeds=2, rankings=ranks)
        seeded = {(r["sim"], r["team"]): r["seed"] for r in st.iter_rows(named=True)}
        assert seeded[(2025, "O1")] is not None
        assert seeded[(2026, "O1")] is None
