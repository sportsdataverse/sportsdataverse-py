"""Tests for the NGS season scraper (``scrape_ngs_week`` / ``scrape_ngs_season``).

The scraper mirrors nflverse/ngs-data's R ``load_week_ngs`` / ``save_ngs_type``
and produces frames column-compatible with the published nflverse NGS parquet
read by :func:`sportsdataverse.nfl.load_nfl_nextgen_stats`.

Two layers of coverage:

* **Offline unit tests** mock the module's HTTP getter (``_ngs_get``) with a
  synthetic statboard payload (camelCase) + a synthetic team directory, then
  assert the scraper (a) snake-cases every column, (b) resolves ``team_abbr``
  from ``teamId`` via the dropped-relocations team map, (c) returns an EMPTY
  documented-schema frame (not an exception) when the API yields no stats, and
  (d) tags rows with the loop ``week`` / ``season_type``.

* A **live test** (gated by ``SDV_PY_LIVE_TESTS=1``) hits the real NGS API and
  asserts ``scrape_ngs_season("passing", 2023)`` is non-empty and carries the
  key columns.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_ngs as M
from sportsdataverse.nfl import scrape_ngs_season, scrape_ngs_week
from tests.conftest import skip_if_no_live


# --------------------------------------------------------------------------- #
# Synthetic payloads (camelCase, the raw statboard shape).
# --------------------------------------------------------------------------- #
def _fake_statboard_payload() -> dict:
    """A 2-row ``/statboard/passing`` payload with a nested ``player`` block."""
    return {
        "season": 2023,
        "seasonType": "REG",
        "week": 1,
        "stats": [
            {
                "playerName": "P.Passer",
                "position": "QB",
                "teamId": "3800",
                "season": 2023,
                "seasonType": "REG",
                "week": 1,
                "completionPercentageAboveExpectation": 4.5,
                "avgTimeToThrow": 2.71,
                "passYards": 305,
                "player": {
                    "gsisId": "00-0011111",
                    "displayName": "Pat Passer",
                    "firstName": "Pat",
                    "lastName": "Passer",
                    "shortName": "P.Passer",
                    "jerseyNumber": 1,
                },
            },
            {
                "playerName": "S.Slinger",
                "position": "QB",
                "teamId": "0325",
                "season": 2023,
                "seasonType": "REG",
                "week": 1,
                "completionPercentageAboveExpectation": -1.2,
                "avgTimeToThrow": 2.55,
                "passYards": 188,
                "player": {
                    "gsisId": "00-0022222",
                    "displayName": "Sam Slinger",
                    "firstName": "Sam",
                    "lastName": "Slinger",
                    "shortName": "S.Slinger",
                    "jerseyNumber": 7,
                },
            },
        ],
    }


def _fake_teams_payload() -> list:
    """A team directory that includes a relocated-abbr duplicate to be dropped.

    ``teamId`` ``3800`` maps to current ``LAR`` and legacy ``STL`` (relocated);
    the scraper must drop the ``STL`` row so the join stays one-to-one and yields
    ``LAR``. ``teamId`` ``0325`` maps to ``ARI``.
    """
    return [
        {"teamId": "3800", "abbr": "LAR", "fullName": "Los Angeles Rams"},
        {"teamId": "3800", "abbr": "STL", "fullName": "St. Louis Rams"},
        {"teamId": "0325", "abbr": "ARI", "fullName": "Arizona Cardinals"},
    ]


@pytest.fixture(autouse=True)
def _reset_teams_cache():
    """Clear the module-level team-directory cache around each test."""
    M._TEAMS_CACHE = None
    yield
    M._TEAMS_CACHE = None


def _patch(monkeypatch, statboard: dict, teams: list) -> None:
    """Route ``_ngs_get`` to the synthetic payloads (statboard vs teams by path)."""

    def fake_get(path: str, params=None):
        if path.startswith("/statboard/"):
            return statboard
        if path == "/league/teams":
            return teams
        raise AssertionError(f"unexpected NGS path: {path}")

    monkeypatch.setattr(M, "_ngs_get", fake_get)


# --------------------------------------------------------------------------- #
# Offline unit tests.
# --------------------------------------------------------------------------- #
def test_scrape_week_snake_cases_columns(monkeypatch):
    _patch(monkeypatch, _fake_statboard_payload(), _fake_teams_payload())
    df = scrape_ngs_week("passing", 2023, week=1)
    assert df.height == 2
    # No camelCase survives; the nested player block is snake_cased too.
    assert all(c == c.lower() and " " not in c for c in df.columns)
    assert "completion_percentage_above_expectation" in df.columns
    assert "avg_time_to_throw" in df.columns
    assert "player_display_name" in df.columns
    assert "player_gsis_id" in df.columns
    # No raw camelCase leaked through.
    assert "completionPercentageAboveExpectation" not in df.columns


def test_scrape_week_resolves_team_abbr_and_drops_relocations(monkeypatch):
    _patch(monkeypatch, _fake_statboard_payload(), _fake_teams_payload())
    df = scrape_ngs_week("passing", 2023, week=1)
    assert "team_abbr" in df.columns
    by_id = {row["team_id"]: row["team_abbr"] for row in df.iter_rows(named=True)}
    # teamId 3800 had both LAR (current) and STL (relocated) -> STL dropped -> LAR.
    assert by_id["3800"] == "LAR"
    assert by_id["0325"] == "ARI"
    # Every row resolved a team abbreviation.
    assert df.filter(pl.col("team_abbr").is_null()).height == 0


def test_scrape_week_tags_week_and_season_type(monkeypatch):
    _patch(monkeypatch, _fake_statboard_payload(), _fake_teams_payload())
    df = scrape_ngs_week("passing", 2023, week=20, season_type="POST")
    assert df["week"].unique().to_list() == [20]
    assert df["season_type"].unique().to_list() == ["POST"]
    assert df["season"].unique().to_list() == [2023]


def test_scrape_week_empty_on_no_stats(monkeypatch):
    empty_payload = {"season": 2023, "seasonType": "REG", "week": 5, "stats": []}
    _patch(monkeypatch, empty_payload, _fake_teams_payload())
    df = scrape_ngs_week("passing", 2023, week=5)
    # Empty frame (not an exception) carrying the documented key schema.
    assert df.height == 0
    for col in ("season", "week", "player_display_name", "player_gsis_id", "team_abbr"):
        assert col in df.columns


def test_scrape_season_empty_when_every_week_empty(monkeypatch):
    empty_payload = {"season": 2023, "seasonType": "REG", "stats": []}
    _patch(monkeypatch, empty_payload, _fake_teams_payload())
    df = scrape_ngs_season("passing", 2023)
    assert df.height == 0
    assert "player_gsis_id" in df.columns


def test_scrape_season_dedupes_and_tags_week_zero(monkeypatch):
    # Every week returns the same 2 players; the season frame must de-dupe on
    # (season, week, player_gsis_id) and include the week-0 aggregate row.
    _patch(monkeypatch, _fake_statboard_payload(), _fake_teams_payload())
    df = scrape_ngs_season("passing", 2023, include_season_totals=True)
    assert df.height > 0
    # week 0 (season totals) present.
    assert 0 in df["week"].unique().to_list()
    # No duplicate (season, week, player_gsis_id) groups remain.
    dup = df.group_by(["season", "week", "player_gsis_id"]).len().filter(pl.col("len") > 1)
    assert dup.height == 0


def test_scrape_season_skips_totals_when_disabled(monkeypatch):
    _patch(monkeypatch, _fake_statboard_payload(), _fake_teams_payload())
    df = scrape_ngs_season("passing", 2023, include_season_totals=False)
    assert 0 not in df["week"].unique().to_list()


def test_scrape_week_pandas_roundtrip(monkeypatch):
    import pandas as pd

    _patch(monkeypatch, _fake_statboard_payload(), _fake_teams_payload())
    df = scrape_ngs_week("passing", 2023, week=1, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert "team_abbr" in df.columns


def test_teams_lookup_failure_does_not_abort(monkeypatch):
    # If the team directory fetch fails, the scrape still returns rows (just with
    # null team_abbr) rather than raising.
    def fake_get(path: str, params=None):
        if path.startswith("/statboard/"):
            return _fake_statboard_payload()
        raise RuntimeError("teams endpoint down")

    monkeypatch.setattr(M, "_ngs_get", fake_get)
    df = scrape_ngs_week("passing", 2023, week=1)
    assert df.height == 2
    assert "team_abbr" in df.columns
    assert df["team_abbr"].is_null().all()


# --------------------------------------------------------------------------- #
# Live test (gated).
# --------------------------------------------------------------------------- #
@skip_if_no_live
def test_scrape_season_live_passing_2023():
    df = scrape_ngs_season("passing", 2023)
    assert df.height > 0
    for col in (
        "season",
        "week",
        "season_type",
        "team_abbr",
        "player_display_name",
        "player_gsis_id",
        "completion_percentage_above_expectation",
        "avg_time_to_throw",
    ):
        assert col in df.columns, f"missing key column: {col}"
    # Season-aggregate (week 0) + regular-season weeks present.
    weeks = set(df["week"].unique().to_list())
    assert 0 in weeks
    assert weeks & set(range(1, 19))
    # team_abbr resolved for the vast majority of rows.
    resolved = df.filter(pl.col("team_abbr").is_not_null()).height
    assert resolved / df.height > 0.95
