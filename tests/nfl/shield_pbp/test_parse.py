"""Tests for the core Shield driveChart -> base play frame parser."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp.parse import (
    _apply_clock_typo_fix,
    _clock_to_seconds,
    _game_half,
    _impute_clock,
    _seconds_remaining,
    _yardline_100,
    parse_game,
)

GAME = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield" / "2024_01_BAL_KC.json.gz"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_clock_to_seconds():
    assert _clock_to_seconds("15:00") == 900
    assert _clock_to_seconds("14:19") == 859
    assert _clock_to_seconds("0:03") == 3
    assert _clock_to_seconds(None) is None
    assert _clock_to_seconds("bad") is None


def test_yardline_100():
    assert _yardline_100("50", "KC") == 50
    assert _yardline_100("BAL 32", "BAL") == 68  # own 32 -> 68 to score
    assert _yardline_100("BAL 32", "KC") == 32  # opponent's 32 -> 32 to score
    assert _yardline_100("BAL 1", "KC") == 1
    assert _yardline_100(None, "KC") is None
    assert _yardline_100("BAL 32", None) is None


def test_seconds_remaining_quarters():
    assert _seconds_remaining(1, 900) == (1800, 3600)  # start of game
    assert _seconds_remaining(2, 0) == (0, 1800)  # end of 1st half
    assert _seconds_remaining(4, 0) == (0, 0)  # end of regulation
    assert _game_half(1) == "Half1" and _game_half(3) == "Half2" and _game_half(5) == "Overtime"


# ---------------------------------------------------------------------------
# TestClock — reference §4 (utils.R::time_to_seconds + every clock computation/
# imputation site in get_pbp_nfl / add_nflscrapr_mutations)
# ---------------------------------------------------------------------------


class TestClock:
    """Table-driven cases pinning nflfastR's exact clock semantics."""

    @pytest.mark.parametrize(
        "clock, expected",
        [
            ("15:00", 900),
            ("14:19", 859),
            ("0:03", 3),
            ("0:00", 0),
            (None, None),
            ("", None),
            ("bad", None),
            ("garbage:x", None),
            (":", None),
        ],
    )
    def test_clock_to_seconds_table(self, clock, expected):
        """utils.R::time_to_seconds — malformed / missing input is NA (None), never raises."""
        assert _clock_to_seconds(clock) == expected

    def test_q1_kickoff(self):
        # "15:00" q1 -> half_seconds_remaining 1800 / game_seconds_remaining 3600.
        qsr = _clock_to_seconds("15:00")
        assert qsr == 900
        assert _seconds_remaining(1, qsr) == (1800, 3600)

    def test_q2_end_of_half(self):
        # "0:00" q2 -> half 0 / game 1800.
        qsr = _clock_to_seconds("0:00")
        assert qsr == 0
        assert _seconds_remaining(2, qsr) == (0, 1800)

    def test_ot_uses_quarter_seconds_remaining_not_zero(self):
        """Reference §4 Site 4: OT (qtr >= 5) game_seconds_remaining ==
        quarter_seconds_remaining, NOT 0 — there is no "game clock" concept once
        regulation ends. "12:34" q5 exercises the OT convention.
        """
        qsr = _clock_to_seconds("12:34")
        assert qsr == 754
        half, game = _seconds_remaining(5, qsr)
        assert half == 754
        assert game == 754  # nflfastR convention: game == quarter_seconds_remaining in OT

    def test_ot_double_ot_same_convention(self):
        qsr = _clock_to_seconds("2:00")
        assert _seconds_remaining(6, qsr) == (120, 120)

    # -- Site 3: null-clock / marker-row imputation --------------------------

    def test_impute_quarter_end_forces_zero(self):
        # desc contains "END QUARTER" -> time forced to "00:00" regardless of the
        # raw clock (even when the raw clock is missing entirely).
        assert _impute_clock(None, "END QUARTER 1") == "00:00"
        assert _impute_clock("2:15", "END QUARTER 2") == "00:00"

    def test_impute_end_game_null_clock(self):
        # desc == "END GAME" with a missing clock -> forced to "00:00".
        assert _impute_clock(None, "END GAME") == "00:00"
        # ...but only when the clock is actually missing; a present clock on an
        # "END GAME" row is left alone (nflfastR's if_else only fires on NA).
        assert _impute_clock("0:05", "END GAME") == "0:05"

    def test_impute_game_marker_forces_kickoff(self):
        # Synthetic kickoff-of-game marker row -> forced to "15:00".
        assert _impute_clock(None, "GAME") == "15:00"
        assert _impute_clock("0:00", "GAME") == "15:00"

    def test_impute_null_clock_midquarter_not_imputed(self):
        """A genuinely missing clock on an ordinary (non-marker) row is left as
        None — nflfastR does not impute a value there, so it propagates through
        to a null quarter_seconds_remaining."""
        assert _impute_clock(None, "(2:15) L.Jackson pass short") is None
        assert _clock_to_seconds(_impute_clock(None, "(2:15) L.Jackson pass short")) is None

    # -- Site 2: hardcoded raw-clock-string typo fixes ------------------------

    def test_clock_typo_fix_applies_to_the_two_known_rows(self):
        assert _apply_clock_typo_fix("2012_04_NO_GB", 1085, "3:24") == "3:34"
        assert _apply_clock_typo_fix("2012_16_BUF_MIA", 2571, None) == "8:31"

    def test_clock_typo_fix_is_a_noop_elsewhere(self):
        assert _apply_clock_typo_fix("2024_01_BAL_KC", 1085, "3:24") == "3:24"
        assert _apply_clock_typo_fix(None, None, "3:24") == "3:24"


# ---------------------------------------------------------------------------
# Real-game parse — needs the committed 2024_01_BAL_KC raw fixture; scoped to
# this class (not the whole module) so the pure-helper tests above (including
# TestClock) always collect and run regardless of fixture availability.
# ---------------------------------------------------------------------------


class TestRealGameParse:
    pytestmark = [
        pytest.mark.skipif(not GAME.exists(), reason="2024_01_BAL_KC fixture not present"),
    ]

    @staticmethod
    def _df():
        return parse_game(json.load(gzip.open(GAME, "rt", encoding="utf-8")))

    def test_frame_shape_and_game_id(self):
        df = self._df()
        assert df.height > 130  # ~150+ non-deleted plays
        assert df["game_id"].unique().to_list() == ["2024_01_BAL_KC"]
        assert set(df["home_team"].unique()) == {"KC"}
        assert set(df["away_team"].unique()) == {"BAL"}

    def test_possession_resolves_and_alternates(self):
        df = self._df()
        # The opening drive is BAL (away); possession should cover both teams.
        pos = df.filter(pl.col("posteam").is_not_null())["posteam"].unique().to_list()
        assert set(pos) == {"BAL", "KC"}
        # First scrimmage play with a down belongs to BAL's opening drive.
        first = df.filter(pl.col("down").is_not_null()).head(1)
        assert first["posteam"][0] == "BAL"
        assert first["defteam"][0] == "KC"

    def test_field_and_clock_bounds(self):
        df = self._df()
        yl = df.filter(pl.col("yardline_100").is_not_null())["yardline_100"]
        assert yl.min() >= 1 and yl.max() <= 99
        gsr = df.filter(pl.col("game_seconds_remaining").is_not_null())["game_seconds_remaining"]
        assert gsr.min() >= 0 and gsr.max() <= 3600
        downs = set(df.filter(pl.col("down").is_not_null())["down"].unique().to_list())
        assert downs <= {1, 2, 3, 4}

    def test_play_type_distribution(self):
        df = self._df()
        pt = dict(df.group_by("play_type").len().iter_rows())
        assert pt.get("pass", 0) > 40
        assert pt.get("run", 0) > 40
        assert pt.get("field_goal", 0) >= 1
        assert pt.get("punt", 0) >= 1

    def test_known_completion_play(self):
        df = self._df()
        # L.Jackson 12-yd completion exists; check a completed pass row is coherent.
        comp = df.filter((pl.col("complete_pass") == 1) & (pl.col("passer_player_name") == "L.Jackson")).head(1)
        assert comp.height == 1
        assert comp["play_type"][0] == "pass"
        assert comp["posteam"][0] == "BAL"
        assert comp["air_yards"][0] is not None
