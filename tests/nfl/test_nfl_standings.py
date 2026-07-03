"""Tests for sportsdataverse.nfl.nfl_season_standings (nflseedR v2 standings port).

Offline only -- golden parity fixture captured from nflseedR via Rscript
(see tests/fixtures/seedr/README.md) plus small hand-built tiebreaker cases.
"""

from pathlib import Path

import polars as pl
import pytest

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "seedr"

EXACT_COLS = [
    "div_rank",
    "conf_rank",
    "draft_rank",
    "exit",
    "games",
    "true_wins",
    "losses",
    "ties",
    "pf",
    "pa",
    "pd",
]
FLOAT_COLS = ["wins", "win_pct", "div_pct", "conf_pct", "sov", "sos"]


def _reg_games(rows: list[tuple[str, str, int]]) -> pl.DataFrame:
    """Build a minimal REG-season games frame from (winner, loser, week) rows.

    Winner is listed as the home team and wins by 7 (result = 7). A week
    tuple with winner == loser is not allowed; use ``_tie_game`` for ties.
    """
    return pl.DataFrame(
        {
            "sim": [1] * len(rows),
            "game_type": ["REG"] * len(rows),
            "week": [w for _, _, w in rows],
            "home_team": [h for h, _, _ in rows],
            "away_team": [a for _, a, _ in rows],
            "result": [7] * len(rows),
        }
    )


@pytest.fixture(scope="module")
def frames() -> "tuple[pl.DataFrame, pl.DataFrame]":
    from sportsdataverse.nfl import nfl_season_standings

    games = pl.read_csv(FIXTURE_DIR / "games_2023.csv")
    expected = pl.read_csv(FIXTURE_DIR / "standings_2023_draft.csv")
    got = nfl_season_standings(games, ranks="DRAFT")
    return got, expected


class TestParity2023:
    """Golden parity vs nflseedR 2.0.2 on the completed 2023 season.

    Oracle: ``nflseedR::nfl_standings(games, ranks = "DRAFT",
    tiebreaker_depth = "SOS")`` run via Rscript (R 4.5.3, nflseedR 2.0.2,
    games from ``nflreadr::load_schedules(2023)``). Ported R source:

    * ``R/standings.R`` L82-155 (``nfl_standings`` orchestration)
    * ``R/standings_init.R`` L1-87 (records, pf/pa/pd, win/div/conf pct, SOV, SOS)
    * ``R/standings_utils.R`` L1-97 (double games, h2h, validate, finalize)
    * ``R/standings_add_div_ranks.R`` L1-323 (division tiebreaker cascade)
    * ``R/standings_add_conf_ranks.R`` L1-576 (conference tiebreaker cascade)
    * ``R/standings_add_draft_ranks.R`` L1-404 (draft order tiebreaker cascade)

    The fixture contains zero "Coin Toss" tiebreaks (verified at capture
    time), so every rank is deterministic and exact equality is required on
    all rank columns. Float columns (sov/sos/win pcts) are recomputed from
    identical integer inputs, so agreement to 1e-9 is required (CSV
    round-trip is the only noise source).
    """

    def test_shape_and_join(self, frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
        got, expected = frames
        assert got.height == 32
        assert expected.height == 32
        # input used `season`, so output carries `season` (not `sim`)
        assert "season" in got.columns

    def test_exact_columns(self, frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
        got, expected = frames
        joined = got.join(expected, on="team", suffix="_r")
        assert joined.height == 32
        for col in EXACT_COLS:
            bad = joined.filter(
                pl.col(col).ne_missing(pl.col(f"{col}_r")) == True  # noqa: E712
            )
            assert bad.height == 0, f"{col} mismatch: {bad.select('team', col, f'{col}_r').to_dicts()}"

    def test_float_columns(self, frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
        got, expected = frames
        joined = got.join(expected, on="team", suffix="_r")
        for col in FLOAT_COLS:
            max_abs = joined.select((pl.col(col) - pl.col(f"{col}_r")).abs().max()).item()
            assert max_abs < 1e-9, f"{col} max abs diff {max_abs}"


class TestDivTiebreakers:
    def test_two_way_h2h(self) -> None:
        """2-way division tie broken by head-to-head win pct.

        BUF and MIA both 1-1; BUF won the head-to-head meeting.
        Port of ``break_div_ties_by_h2h`` (standings_add_div_ranks.R L118-150).
        """
        from sportsdataverse.nfl import nfl_season_standings

        games = _reg_games(
            [
                ("BUF", "MIA", 1),
                ("NE", "BUF", 2),
                ("MIA", "NYJ", 3),
            ]
        )
        st = nfl_season_standings(games, ranks="DIV")
        ranks = dict(zip(st["team"].to_list(), st["div_rank"].to_list()))
        assert ranks == {"NE": 1, "BUF": 2, "MIA": 3, "NYJ": 4}
        broken = dict(zip(st["team"].to_list(), st["div_tie_broken_by"].to_list()))
        assert broken["BUF"] == "Head-To-Head Win PCT (2)"
        assert broken["MIA"] == "Head-To-Head Win PCT (2)"

    def test_four_way_h2h_then_sos(self) -> None:
        """4-way division tie: h2h splits BAL/PIT off, SOS splits CIN/CLE.

        All of BAL/CIN/CLE/PIT are 1-1. Head-to-head win pct among the tied
        clubs: BAL 1.0 (beat PIT), PIT 0.5, CIN/CLE 0.0 (CIN played none of
        them -> 0 per the NA -> 0 rule in ``break_div_ties_by_h2h``). CIN
        and CLE re-tie for rank 3 and cascade past division record (0 = 0),
        common games (none), conference pct and SOV (equal) down to SOS
        (CIN 0.5 > CLE 1/3).
        """
        from sportsdataverse.nfl import nfl_season_standings

        games = _reg_games(
            [
                ("BAL", "PIT", 1),  # BAL div win
                ("TEN", "BAL", 2),
                ("CIN", "HOU", 1),  # non-div win
                ("JAX", "CIN", 2),
                ("CLE", "IND", 1),  # non-div win
                ("PIT", "CLE", 2),  # CLE div loss
            ]
        )
        st = nfl_season_standings(games, ranks="DIV")
        north = st.filter(pl.col("division") == "AFC North")
        ranks = dict(zip(north["team"].to_list(), north["div_rank"].to_list()))
        assert ranks["BAL"] == 1
        assert ranks["PIT"] == 2
        assert ranks["CIN"] == 3
        assert ranks["CLE"] == 4
        broken = dict(zip(north["team"].to_list(), north["div_tie_broken_by"].to_list()))
        assert broken["CIN"] == "SOS (2)"
        assert broken["BAL"] == "Head-To-Head Win PCT (4)"

    def test_pre_sov_depth_falls_to_coin_toss(self) -> None:
        """Same scenario at depth PRE-SOV: SOS is gated off, CIN/CLE go to coin toss."""
        from sportsdataverse.nfl import nfl_season_standings

        games = _reg_games(
            [
                ("BAL", "PIT", 1),
                ("TEN", "BAL", 2),
                ("CIN", "HOU", 1),
                ("JAX", "CIN", 2),
                ("CLE", "IND", 1),
                ("PIT", "CLE", 2),
            ]
        )
        st = nfl_season_standings(games, ranks="DIV", tiebreaker_depth="PRE-SOV")
        north = st.filter(pl.col("division") == "AFC North")
        ranks = dict(zip(north["team"].to_list(), north["div_rank"].to_list()))
        assert ranks["BAL"] == 1
        assert ranks["PIT"] == 2
        assert {ranks["CIN"], ranks["CLE"]} == {3, 4}
        broken = dict(zip(north["team"].to_list(), north["div_tie_broken_by"].to_list()))
        assert broken["CIN"] == "Coin Toss"

    def test_tie_counts_half_win(self) -> None:
        """A tied game (result == 0) counts as half a win in win_pct."""
        from sportsdataverse.nfl import nfl_season_standings

        games = pl.DataFrame(
            {
                "sim": [1, 1],
                "game_type": ["REG", "REG"],
                "week": [1, 2],
                "home_team": ["BUF", "BUF"],
                "away_team": ["MIA", "NYJ"],
                "result": [0, 7],
            }
        )
        st = nfl_season_standings(games, ranks="DIV")
        buf = st.filter(pl.col("team") == "BUF")
        assert buf["wins"].item() == 1.5
        assert buf["ties"].item() == 1
        assert buf["true_wins"].item() == 1
        assert buf["win_pct"].item() == 0.75


class TestConfTiebreakers:
    def _conf_games(self) -> pl.DataFrame:
        """Four AFC division winners; KC 3-0; BUF/BAL/HOU tied 2-1 with BUF
        sweeping both BAL and HOU head-to-head."""
        return _reg_games(
            [
                # KC wins out
                ("KC", "BUF", 1),
                ("KC", "DEN", 2),
                ("KC", "LV", 3),
                # BUF sweeps BAL + HOU
                ("BUF", "BAL", 2),
                ("BUF", "HOU", 3),
                # BAL 2-1
                ("BAL", "NYJ", 1),
                ("BAL", "PIT", 3),
                # HOU 2-1
                ("HOU", "MIA", 1),
                ("HOU", "CIN", 2),
            ]
        )

    def test_h2h_sweep(self) -> None:
        """3-way conference tie: BUF advances via head-to-head sweep.

        Port of ``break_conf_ties_by_h2h`` (standings_add_conf_ranks.R
        L222-281): sweep = 1 only when a club beat every other tied club it
        played AND played all of them; clubs missing a meeting get 0.5.
        """
        from sportsdataverse.nfl import nfl_season_standings

        st = nfl_season_standings(self._conf_games(), ranks="CONF")
        afc = st.filter(pl.col("conf") == "AFC")
        ranks = dict(zip(afc["team"].to_list(), afc["conf_rank"].to_list()))
        assert ranks["KC"] == 1
        assert ranks["BUF"] == 2
        assert {ranks["BAL"], ranks["HOU"]} == {3, 4}
        broken = dict(zip(afc["team"].to_list(), afc["conf_tie_broken_by"].to_list()))
        assert broken["BUF"] == "Head-To-Head Sweep (3)"

    def test_playoff_seeds_truncates(self) -> None:
        """conf_rank beyond ``playoff_seeds`` is null (nfl_standings.R L37-43,
        standings_add_conf_ranks.R L181-188)."""
        from sportsdataverse.nfl import nfl_season_standings

        st = nfl_season_standings(self._conf_games(), ranks="CONF", playoff_seeds=2)
        afc = st.filter(pl.col("conf") == "AFC")
        kept = afc.filter(pl.col("conf_rank").is_not_null() == True)  # noqa: E712
        assert kept.height == 2
        assert set(kept["conf_rank"].to_list()) == {1, 2}
