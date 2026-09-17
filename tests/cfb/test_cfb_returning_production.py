"""Tests for the Connelly-style returning-production core (T2.2 Phase 2)."""

from __future__ import annotations

import dataclasses
import importlib

import pytest

import polars as pl

from sportsdataverse.cfb.cfb_returning_production import _returning_from_frames

# The package re-exports the FUNCTION under the module's name, so attribute
# import would hand back the callable; resolve the module explicitly.
rp = importlib.import_module("sportsdataverse.cfb.cfb_returning_production")
# Captured before the autouse stub replaces them, for the tests that exercise them.
_PARTICIPANTS_BUILDER = rp._defense_from_participants
_PBP_SPLASH_BUILDER = rp._defense_from_pbp_splash


@pytest.fixture(autouse=True)
def _no_network_defense(monkeypatch):
    """The defensive sources read releases; stub them empty unless a test opts in."""
    empty = lambda _s: pl.DataFrame(schema=rp._PRODUCTION_SCHEMA)  # noqa: E731
    monkeypatch.setattr(rp, "_defense_from_participants", empty)
    monkeypatch.setattr(rp, "_defense_from_pbp_splash", empty)


def test_half_offense_returns() -> None:
    prod_prev = pl.DataFrame(
        {  # season S-1 = 2022 production
            "season": [2022, 2022],
            "team_id": ["A", "A"],
            "player_id": ["p1", "p2"],
            "unit": ["offense", "offense"],
            "prod_weight": [10.0, 10.0],
            "position": ["WR", "WR"],
        }
    )
    roster_curr = pl.DataFrame(
        {  # season S = 2023 roster: only p1 returns
            "season": [2023],
            "team_id": ["A"],
            "player_id": ["p1"],
        }
    )
    out = _returning_from_frames(prod_prev, roster_curr, division="fbs")
    row = out.filter((pl.col("season") == 2023) & (pl.col("team_id") == "A")).row(0, named=True)
    assert abs(row["off_returning"] - 0.5) < 1e-9


def test_units_aggregate_independently_and_overall_averages() -> None:
    prod_prev = pl.DataFrame(
        {
            "season": [2022] * 4,
            "team_id": ["A"] * 4,
            "player_id": ["p1", "p2", "d1", "d2"],
            "unit": ["offense", "offense", "defense", "defense"],
            "prod_weight": [30.0, 10.0, 5.0, 15.0],
            "position": ["QB", "WR", "LB", "CB"],
        }
    )
    roster_curr = pl.DataFrame(
        {
            "season": [2023, 2023],
            "team_id": ["A", "A"],
            "player_id": ["p1", "d2"],  # QB (30/40 off) + CB (15/20 def) return
        }
    )
    out = _returning_from_frames(prod_prev, roster_curr, division="fbs")
    row = out.row(0, named=True)
    assert abs(row["off_returning"] - 0.75) < 1e-9
    assert abs(row["def_returning"] - 0.75) < 1e-9
    assert abs(row["overall_returning"] - 0.75) < 1e-9
    assert row["n_returning"] == 2


def test_empty_input_returns_documented_schema() -> None:
    empty_prod = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "team_id": pl.Utf8,
            "player_id": pl.Utf8,
            "unit": pl.Utf8,
            "prod_weight": pl.Float64,
            "position": pl.Utf8,
        }
    )
    empty_roster = pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "player_id": pl.Utf8})
    out = _returning_from_frames(empty_prod, empty_roster, division="fbs")
    assert out.height == 0
    assert out.schema["off_returning"] == pl.Float64
    assert out.schema["n_returning"] == pl.Int64


def test_roster_keys_use_espn_team_id_without_name_crosswalk(monkeypatch) -> None:
    """espn_cfb_rosters (#399) has team_id but no `team`; the name join must be skipped."""
    espn_roster = pl.DataFrame(
        {
            "season": [2025, 2025, 2025],
            "team_id": [333, 333, None],
            "athlete_id": [1, 2, 3],
            "team_name": ["Crimson Tide", "Crimson Tide", "Crimson Tide"],
        }
    )
    monkeypatch.setattr(rp, "load_cfb_rosters", lambda season: espn_roster)
    monkeypatch.setattr(rp, "load_cfb_rosters_cfbd", lambda season: pl.DataFrame())

    def _no_info(season):  # pragma: no cover - the assertion is that it is never reached
        raise AssertionError("team_info crosswalk must not run when only the ESPN roster is present")

    monkeypatch.setattr(rp, "load_cfb_team_info", _no_info)
    out = rp._roster_keys(2025)
    assert out.columns == ["season", "team_id", "player_id"]
    assert out["team_id"].to_list() == ["333", "333"]  # null team_id row dropped
    assert out["player_id"].to_list() == ["1", "2"]
    assert out.schema["team_id"] == pl.Utf8 and out.schema["player_id"] == pl.Utf8


def test_roster_keys_union_espn_and_cfbd_for_the_season_in_progress(monkeypatch) -> None:
    """Week 1: ESPN game rosters cover a handful of teams; the CFBD preseason roster fills the rest.

    A player listed by both sources appears once; the CFBD school name is resolved
    through team_info to the same ESPN team id the ESPN roster carries.
    """
    espn_roster = pl.DataFrame({"season": [2026, 2026], "team_id": [333, 333], "athlete_id": [1, 2]})
    cfbd_roster = pl.DataFrame(
        {
            "athlete_id": ["2", "3", "4", "5"],
            "team": ["Alabama", "Alabama", "Auburn", "Nowhere State"],
        }
    )
    info = pl.DataFrame({"team_id": [333, 2], "school": ["Alabama", "Auburn"], "alt_name1": [None, None]})
    monkeypatch.setattr(rp, "load_cfb_rosters", lambda season: espn_roster)
    monkeypatch.setattr(rp, "load_cfb_rosters_cfbd", lambda season: cfbd_roster)
    monkeypatch.setattr(rp, "load_cfb_team_info", lambda season: info)
    monkeypatch.setattr(rp, "_MIN_ROSTER_MATCH", 0.5)  # 3 of 4 CFBD rows resolve; "Nowhere State" does not
    out = rp._roster_keys(2026).sort("team_id", "player_id")
    assert out.rows() == [(2026, "2", "4"), (2026, "333", "1"), (2026, "333", "2"), (2026, "333", "3")]


def test_roster_keys_cfbd_match_floor_still_asserts(monkeypatch) -> None:
    monkeypatch.setattr(rp, "load_cfb_rosters", lambda season: pl.DataFrame())
    monkeypatch.setattr(
        rp,
        "load_cfb_rosters_cfbd",
        lambda season: pl.DataFrame({"athlete_id": ["1", "2"], "team": ["Nowhere", "Elsewhere"]}),
    )
    monkeypatch.setattr(
        rp, "load_cfb_team_info", lambda season: pl.DataFrame({"team_id": [1], "school": ["Somewhere"]})
    )
    with pytest.raises(ValueError, match="resolved to a team id"):
        rp._roster_keys(2026)


class TestSeason2004:
    """2004: the one season whose S-1 production cannot come from the ESPN box.

    `load_cfb_player_box([2003])` RAISES SeasonNotFoundError (ESPN's player box
    starts in 2004), which propagated out of cfb_returning_production and killed
    a whole cfbfastR-cfb-data build -- run 34142076600, `A creation step for
    season 2004 exited with code 1`.
    """

    def test_load_box_below_the_floor_returns_empty_not_raises(self, monkeypatch):
        """Its docstring promised an empty frame; it raised instead."""
        from sportsdataverse.errors import SeasonNotFoundError

        def boom(_seasons):
            raise SeasonNotFoundError("Season 2003 not found, season cannot be less than 2004")

        monkeypatch.setattr(rp, "load_cfb_player_box", boom)
        assert rp._load_box(2003).height == 0

    def test_2004_falls_back_to_hosted_2003_production(self, monkeypatch):
        prod_2003 = pl.DataFrame(
            {
                "season": [2003, 2003, 2003],
                "team_id": ["333", "333", "333"],
                # two returned in 2004, one did not
                "player_id": ["1", "2", "cfbd2003:Alabama:gone player"],
                "player_name": ["a", "b", "gone player"],
                "unit": ["offense"] * 3,
                "prod_weight": [600.0, 300.0, 100.0],
                "position": [None, None, None],
            }
        )
        roster_2004 = pl.DataFrame({"season": [2004, 2004], "team_id": ["333", "333"], "player_id": ["1", "2"]})
        monkeypatch.setattr(rp, "_load_box", lambda _s: pl.DataFrame())
        monkeypatch.setattr(rp, "_load_production_2003", lambda: prod_2003)
        monkeypatch.setattr(rp, "_roster_keys", lambda _s: roster_2004)

        out = rp.cfb_returning_production(2004)
        assert out.height == 1
        row = out.row(0, named=True)
        assert row["season"] == 2004
        # 900 of 1000 yards returned
        assert row["off_returning"] == pytest.approx(0.9)
        assert row["is_estimated"] is True
        # 2003 play text carries no tacklers, and every season through 2016 is
        # null here anyway -- a splash-only number would make 2004 the lone
        # pre-2020 season with a defensive value.
        assert row["def_returning"] is None

    def test_a_normal_season_is_not_flagged_estimated(self, monkeypatch):
        """The flag must mark 2004 alone, not every row in the panel."""
        box = pl.DataFrame(
            {
                "season": [2004, 2004],
                "team_id": [333, 333],
                "athlete_id": [1, 2],
                "athlete_name": ["a", "b"],
                "passingYards": ["100", "0"],
                "rushingYards": ["0", "50"],
                "receivingYards": ["0", "0"],
            }
        )
        monkeypatch.setattr(rp, "_load_box", lambda _s: box)
        monkeypatch.setattr(
            rp,
            "_roster_keys",
            lambda _s: pl.DataFrame({"season": [2005], "team_id": ["333"], "player_id": ["1"]}),
        )
        called = []
        monkeypatch.setattr(rp, "_load_production_2003", lambda: called.append(1) or pl.DataFrame())
        out = rp.cfb_returning_production(2005)
        assert not called, "the 2003 table must not be fetched for a normal season"
        assert out["is_estimated"].to_list() == [False]

    def test_2004_still_skipped_when_the_hosted_table_is_unreachable(self, monkeypatch):
        """A 404 must skip the season, never emit a row flagged as estimated."""
        monkeypatch.setattr(rp, "_load_box", lambda _s: pl.DataFrame())
        monkeypatch.setattr(rp, "_load_production_2003", lambda: pl.DataFrame())
        out = rp.cfb_returning_production(2004)
        assert out.height == 0
        assert out.schema["is_estimated"] == pl.Boolean


class TestDefensiveSources:
    """def_returning reads coverage-complete sources, not the sparse ESPN defensive box."""

    def test_participants_score_tackles_splits_sacks_and_counts_tfl(self, monkeypatch):
        parts = pl.DataFrame(
            {
                "game_id": [1, 1, 1, 1],
                "play_id": [10, 11, 12, 13],
                # numpy-repr cells, as the release stores them
                "tackler_player_ids": ["['7']", "['7']", "[]", "[]"],
                "assisted_by_player_ids": ["[]", "['8' '9']", "[]", "[]"],
                "sacked_by_player_ids": ["[]", "[]", "['7' '8']", "[]"],
                "pass_defender_player_ids": ["[]", "[]", "[]", "['9']"],
            }
        )
        pbp = pl.DataFrame(
            {
                "game_id": [1, 1, 1, 1],
                "id": ["10", "11", "12", "13"],  # Utf8 play id, as the 2014 release ships it
                "def_pos_team_id": [55, 55, 55, 55],
                "statYardage": [4, -3, -7, 0],
            }
        )
        monkeypatch.setattr(rp, "load_cfb_play_participants", lambda _s: parts)
        monkeypatch.setattr(rp, "load_cfb_pbp", lambda _s: pbp)
        out = _PARTICIPANTS_BUILDER(2019)
        got = dict(zip(out["player_id"].to_list(), out["prod_weight"].to_list()))
        # 7: tackle (1) + tackle-for-loss play 11 (1 + TFL 1) + half a sack (2.0 / 2) = 4
        # 8: assist on the TFL play (1 + TFL 1) + half a sack (1) = 3
        # 9: assist on the TFL play (1 + 1) + a pass defended (1) = 3
        assert got == {"7": 4.0, "8": 3.0, "9": 3.0}
        assert set(out["team_id"].to_list()) == {"55"} and set(out["unit"].to_list()) == {"defense"}

    def test_pbp_splash_weights_and_defending_team(self, monkeypatch):
        pbp = pl.DataFrame(
            {
                "def_pos_team_id": [55, 55, 66],
                "sack_player_id": [7, None, None],
                "interception_player_id": [None, 8, None],
                "pass_breakup_player_id": [None, None, 9],
                "fumble_forced_player_id": [7, None, None],
            }
        )
        monkeypatch.setattr(rp, "load_cfb_pbp", lambda _s: pbp)
        out = _PBP_SPLASH_BUILDER(2009).sort("player_id")
        assert out.select("team_id", "player_id", "prod_weight").rows() == [
            ("55", "7", 3.0),
            ("55", "8", 1.0),
            ("66", "9", 1.0),
        ]

    @pytest.mark.parametrize(("season", "basis"), [(2020, "participants"), (2010, "pbp_splash")])
    def test_the_production_season_picks_the_source_and_labels_it(self, monkeypatch, season, basis):
        box = pl.DataFrame(
            {
                "team_id": [55, 55],
                "athlete_id": [1, 2],
                "athlete_name": ["a", "b"],
                "passingYards": ["100", "0"],
                "rushingYards": ["0", "50"],
                "receivingYards": ["0", "0"],
                "totalTackles": ["0", "9"],
            }
        )
        defense = pl.DataFrame(
            {
                "season": [season - 1],
                "team_id": ["55"],
                "player_id": ["3"],
                "player_name": [None],
                "unit": ["defense"],
                "prod_weight": [5.0],
                "position": [None],
            },
            schema=rp._PRODUCTION_SCHEMA,
        )
        other = lambda _s: pl.DataFrame(schema=rp._PRODUCTION_SCHEMA)  # noqa: E731
        monkeypatch.setattr(rp, "_load_box", lambda _s: box)
        monkeypatch.setattr(
            rp, "_defense_from_participants", (lambda _s: defense) if basis == "participants" else other
        )
        monkeypatch.setattr(rp, "_defense_from_pbp_splash", (lambda _s: defense) if basis == "pbp_splash" else other)
        roster = pl.DataFrame({"season": [season, season], "team_id": ["55", "55"], "player_id": ["1", "3"]})
        monkeypatch.setattr(rp, "_roster_keys", lambda _s: roster)
        out = rp.cfb_returning_production(season)
        row = out.row(0, named=True)
        assert row["def_basis"] == basis
        assert row["def_returning"] == 1.0  # player 3 (the source's defender) returned; the box tackler (2) is ignored
        assert list(out.columns) == list(rp._RETURNING_SCHEMA)

    def test_an_empty_source_keeps_the_box_defense_and_says_so(self, monkeypatch):
        box = pl.DataFrame(
            {
                "team_id": [55, 55],
                "athlete_id": [1, 2],
                "athlete_name": ["a", "b"],
                "passingYards": ["100", "0"],
                "rushingYards": ["0", "0"],
                "receivingYards": ["0", "0"],
                "totalTackles": ["0", "9"],
            }
        )
        monkeypatch.setattr(rp, "_load_box", lambda _s: box)
        monkeypatch.setattr(
            rp, "_roster_keys", lambda _s: pl.DataFrame({"season": [2025], "team_id": ["55"], "player_id": ["2"]})
        )
        row = rp.cfb_returning_production(2025).row(0, named=True)
        assert row["def_basis"] == "box" and row["def_returning"] == 1.0

    def test_a_team_without_defense_falls_back_to_offense_and_says_so(self, monkeypatch):
        mixed = dataclasses.replace(rp.get_constants("fbs"), returning_prod_weights={"offense": 0.45, "defense": 0.55})
        monkeypatch.setattr(rp, "get_constants", lambda _d: mixed)
        prod_prev = pl.DataFrame(
            {
                "season": [2022] * 3,
                "team_id": ["A", "A", "B"],
                "player_id": ["p1", "d1", "q1"],
                "unit": ["offense", "defense", "offense"],
                "prod_weight": [10.0, 10.0, 10.0],
                "position": [None] * 3,
            }
        )
        roster = pl.DataFrame({"season": [2023, 2023], "team_id": ["A", "B"], "player_id": ["p1", "q1"]})
        out = {r["team_id"]: r for r in _returning_from_frames(prod_prev, roster).to_dicts()}
        assert out["A"]["overall_basis"] == "offense+defense" and abs(out["A"]["overall_returning"] - 0.45) < 1e-9
        assert (
            out["B"]["overall_basis"] == "offense"
            and out["B"]["overall_returning"] == 1.0
            and out["B"]["def_returning"] is None
        )
