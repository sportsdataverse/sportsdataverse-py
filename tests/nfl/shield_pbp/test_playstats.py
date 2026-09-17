"""Tests for the §13 long-format playstats builder (build_playstats port).

Two fixture strategies, matching the repo's established pattern:

- A minimal synthetic Shield game  drives the hermetic unit tests below — these always run and give real
  red/green TDD signal without depending on the (large, not-always-checked-out)
  real game corpus.
- ``TestRealGamePlaystats`` reuses the same bundled ``2024_01_BAL_KC.json.gz``
  fixture ``test_parse.py``/``test_stat_ids.py`` use.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp.playstats import (
    PLAYSTATS_SCHEMA,
    build_playstats_frame,
    build_playstats_season,
)

GAME = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield" / "2024_01_BAL_KC.json.gz"


# ---------------------------------------------------------------------------
# Minimal synthetic Shield game payload
# ---------------------------------------------------------------------------


def _make_game() -> dict:
    """A 2024 KC(home)@... wait: BAL(home)/KC(away) synthetic game, 3 plays.

    homeTeam/awayTeam ``currentLogo`` trailing path segment is the club abbr
    (what ``_team_abbr()`` reads) — same convention as nfl-data's ``test_cli.py``.
    """
    return {
        "season": 2024,
        "week": 1,
        "seasonType": "REG",
        "homeTeam": {
            "teamId": "home-uuid",
            "currentLogo": "https://static.nfl.com/clubs/logos/BAL",
        },
        "awayTeam": {
            "teamId": "away-uuid",
            "currentLogo": "https://static.nfl.com/clubs/logos/KC",
        },
        "summary": {
            "homeTeam": {"teamId": "home-uuid"},
            "awayTeam": {"teamId": "away-uuid"},
        },
        "driveChart": {
            "drives": [
                {"teamId": "away-uuid", "startedPlaySequenceNumber": 1, "endedPlaySequenceNumber": 10},
            ],
            "plays": [
                {
                    "playId": 101,
                    "playSequenceNumber": 3,
                    "playType": "RUSH",
                    "playDeleted": False,
                    "stats": [
                        {
                            "statType": 10,  # rush attempt
                            "yards": 5,
                            "gsisPlayerId": "00-0001234",
                            "gsisPlayerName": "Test Runner",
                            "teamId": "away-uuid",
                        },
                    ],
                },
                {
                    "playId": 102,
                    "playSequenceNumber": 4,
                    "playType": "PASS",
                    "playDeleted": False,
                    "stats": [
                        {
                            "statType": 15,  # completion
                            "yards": 12,
                            "gsisPlayerId": "00-0034796",
                            "gsisPlayerName": "L.Jackson",
                            "teamId": "away-uuid",
                        },
                        {
                            "statType": 113,  # YAC
                            "yards": 4,
                            "gsisPlayerId": "00-0037197",
                            "gsisPlayerName": "Z.Flowers",
                            "teamId": "away-uuid",
                        },
                    ],
                },
                {
                    # Deleted play carrying a NON-empty stats array -- must
                    # contribute 0 rows (playDeleted guard, parity with
                    # parse_game, which also skips deleted plays).
                    "playId": 103,
                    "playSequenceNumber": 5,
                    "playType": "UNSPECIFIED",
                    "playDeleted": True,
                    "stats": [
                        {
                            "statType": 10,
                            "yards": 99,
                            "gsisPlayerId": "00-0009999",
                            "gsisPlayerName": "Ghost Runner",
                            "teamId": "away-uuid",
                        },
                    ],
                },
            ],
        },
    }


# ---------------------------------------------------------------------------
# Schema + row-count
# ---------------------------------------------------------------------------


def test_schema_columns_and_dtypes_match_reference():
    df = build_playstats_frame(_make_game(), "2024_01_KC_BAL")
    assert list(df.columns) == list(PLAYSTATS_SCHEMA)
    assert df.schema == PLAYSTATS_SCHEMA


def test_row_count_equals_total_stats_entries_on_non_deleted_plays():
    game = _make_game()
    total = sum(len(p.get("stats") or []) for p in game["driveChart"]["plays"] if not p.get("playDeleted"))
    df = build_playstats_frame(game, "2024_01_KC_BAL")
    assert total == 3
    assert df.height == total


def test_deleted_play_with_stats_emits_no_rows():
    """A deleted play carrying stat entries must NOT produce phantom rows
    (rows with no matching (game_id, play_id) in the wide pbp frame)."""
    df = build_playstats_frame(_make_game(), "2024_01_KC_BAL")
    assert df.filter(pl.col("play_id") == 103).height == 0


def test_empty_game_returns_zero_row_schema_frame():
    df = build_playstats_frame({}, "empty_game")
    assert df.height == 0
    assert df.schema == PLAYSTATS_SCHEMA


# ---------------------------------------------------------------------------
# Known rows
# ---------------------------------------------------------------------------


def test_seasonless_payload_with_explicit_game_id_resolves_era_abbr():
    # season recovered from the caller-supplied game_id must feed the team
    # resolver: _nflverse_abbr's relocation fixup is season-aware (LV is OAK
    # through 2019), and with season=None it raises on the None <= int compare.
    game = _make_game()
    del game["season"]
    game["awayTeam"]["currentLogo"] = "https://static.nfl.com/clubs/logos/LV"
    df = build_playstats_frame(game, "2019_01_OAK_BAL")
    assert df["season"].unique().to_list() == [2019]
    away_rows = df.filter(pl.col("team_abbr") == "OAK")
    assert away_rows.height > 0  # era-correct abbr, not LV


def test_known_rush_row_resolves_team_and_player():
    df = build_playstats_frame(_make_game(), "2024_01_KC_BAL")
    row = df.filter(pl.col("play_id") == 101).row(0, named=True)
    assert row["game_id"] == "2024_01_KC_BAL"
    assert row["season"] == 2024
    assert row["week"] == 1
    assert row["stat_id"] == 10
    assert row["yards"] == 5
    assert row["team_abbr"] == "KC"  # away team's rush attempt
    assert row["gsis_player_id"] == "00-0001234"
    assert row["player_name"] == "Test Runner"


def test_known_yac_row_on_multi_stat_play():
    df = build_playstats_frame(_make_game(), "2024_01_KC_BAL")
    row = df.filter((pl.col("play_id") == 102) & (pl.col("stat_id") == 113)).row(0, named=True)
    assert row["yards"] == 4
    assert row["gsis_player_id"] == "00-0037197"
    assert row["player_name"] == "Z.Flowers"
    # The play's other stat entry (the completion) is a separate row.
    comp = df.filter((pl.col("play_id") == 102) & (pl.col("stat_id") == 15)).row(0, named=True)
    assert comp["yards"] == 12
    assert comp["gsis_player_id"] == "00-0034796"


def test_stat_ids_filter_narrows_output():
    df = build_playstats_frame(_make_game(), "2024_01_KC_BAL", stat_ids=range(1, 11))
    assert df.height == 1
    assert df["stat_id"].to_list() == [10]


def test_empty_string_names_coerced_to_null():
    game = _make_game()
    game["driveChart"]["plays"][0]["stats"][0]["gsisPlayerName"] = ""
    df = build_playstats_frame(game, "2024_01_KC_BAL")
    row = df.filter(pl.col("play_id") == 101).row(0, named=True)
    assert row["player_name"] is None


# ---------------------------------------------------------------------------
# Season build (mirrors build.py::build_season's directory-iteration pattern)
# ---------------------------------------------------------------------------


def test_build_playstats_season_concatenates_games(tmp_path):
    season_dir = tmp_path / "raw" / "2024"
    season_dir.mkdir(parents=True)
    (season_dir / "2024_01_KC_BAL.json").write_text(json.dumps(_make_game()), encoding="utf-8")

    df = build_playstats_season(2024, raw_dir=tmp_path / "raw")

    assert df.height == 3
    assert df["game_id"].unique().to_list() == ["2024_01_KC_BAL"]
    assert df.schema == PLAYSTATS_SCHEMA


def test_build_playstats_season_empty_dir_returns_zero_row_schema_frame(tmp_path):
    (tmp_path / "raw" / "2024").mkdir(parents=True)
    df = build_playstats_season(2024, raw_dir=tmp_path / "raw")
    assert df.height == 0
    assert df.schema == PLAYSTATS_SCHEMA


def test_build_playstats_season_game_ids_subset(tmp_path):
    season_dir = tmp_path / "raw" / "2024"
    season_dir.mkdir(parents=True)
    game_a = _make_game()
    (season_dir / "2024_01_KC_BAL.json").write_text(json.dumps(game_a), encoding="utf-8")
    game_b = _make_game()
    game_b["driveChart"]["plays"] = game_b["driveChart"]["plays"][:1]
    (season_dir / "2024_02_KC_BAL.json").write_text(json.dumps(game_b), encoding="utf-8")

    df = build_playstats_season(2024, raw_dir=tmp_path / "raw", game_ids=["2024_01_KC_BAL"])
    assert df["game_id"].unique().to_list() == ["2024_01_KC_BAL"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _seed_raw_dir(tmp_path, season: int = 2024):
    season_dir = tmp_path / str(season)
    season_dir.mkdir(parents=True)
    (season_dir / f"{season}_01_KC_BAL.json").write_text(json.dumps(_make_game()), encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# Real-game fixture — needs the committed 2024_01_BAL_KC raw fixture; scoped to
# this class (not the whole module) so the hermetic tests above always collect
# and run regardless of fixture availability (matches test_parse.py's pattern).
# ---------------------------------------------------------------------------


class TestRealGamePlaystats:
    pytestmark = [
        pytest.mark.skipif(not GAME.exists(), reason="2024_01_BAL_KC fixture not present"),
    ]

    @staticmethod
    def _game() -> dict:
        return json.load(gzip.open(GAME, "rt", encoding="utf-8"))

    def _frame(self) -> pl.DataFrame:
        return build_playstats_frame(self._game(), "2024_01_BAL_KC")

    def test_row_count_matches_total_stats_entries(self):
        game = self._game()
        total = sum(len(p.get("stats") or []) for p in game["driveChart"]["plays"] if not p.get("playDeleted"))
        df = self._frame()
        # default stat_ids=1:1000 covers every known GSIS code in this corpus.
        assert df.height == total

    def test_schema(self):
        assert self._frame().schema == PLAYSTATS_SCHEMA

    def test_known_completion_present(self):
        df = self._frame()
        comp = df.filter((pl.col("stat_id") == 15) & (pl.col("gsis_player_id") == "00-0034796"))
        assert comp.height >= 1


def test_build_playstats_season_skips_preseason_games(tmp_path):
    season_dir = tmp_path / "raw" / "2024"
    season_dir.mkdir(parents=True)
    (season_dir / "2024_01_KC_BAL.json").write_text(json.dumps(_make_game()), encoding="utf-8")
    pre = {**_make_game(), "seasonType": "PRE"}
    (season_dir / "2024_PRE1_KC_BAL.json").write_text(json.dumps(pre), encoding="utf-8")

    df = build_playstats_season(2024, raw_dir=tmp_path / "raw")

    assert df["game_id"].unique().to_list() == ["2024_01_KC_BAL"]
