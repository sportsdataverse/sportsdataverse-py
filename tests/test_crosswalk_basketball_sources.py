"""Offline gates for the basketball crosswalk source adapters.

Two defect classes are locked in here, both found when the live Track X-B flip
was attempted and aborted:

1. **Silent-empty contract.** ``stats_schedule_games`` / ``bart_super_sked``
   used to swallow every fetch failure and return a well-formed *empty* frame,
   so a dead source degraded the whole crosswalk to ``unmatched`` instead of
   failing. The tests below assert the **raise**, and separately assert that a
   provably-empty-but-valid payload still returns the typed empty frame.
2. **Wrong envelope for ``scheduleleaguev2``.** The payload is
   ``{"meta":…, "leagueSchedule": {"gameDates": [...]}}``, not the
   ``resultSets`` envelope, so it parsed to zero rows against a healthy API.
   Exercised against **real captured bodies** (see the fixture READMEs) --
   never a hand-written one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse._crosswalk_basketball_sources import (
    CrosswalkSourceError,
    bart_super_sked,
    espn_rosters,
    require_source,
    stats_schedule_games,
)
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

FIXTURES = {
    "nba": Path(__file__).parent / "fixtures" / "nba_stats" / "scheduleleaguev2_2025_26.json",
    "wnba": Path(__file__).parent / "fixtures" / "wnba_stats" / "scheduleleaguev2_2026.json",
}


def _payload(league: str) -> dict:
    with FIXTURES[league].open(encoding="utf-8") as handle:
        return json.load(handle)


def _patch_fetch(monkeypatch: pytest.MonkeyPatch, league: str, fake: Any) -> None:
    """Point the league's ``scheduleleaguev2`` wrapper at ``fake``."""
    module = "sportsdataverse.nba.nba_stats" if league == "nba" else "sportsdataverse.wnba.wnba_stats"
    name = f"{league}_stats_scheduleleaguev2"
    monkeypatch.setattr(f"{module}.{name}", fake, raising=True)


# --------------------------------------------------------------------------
# Defect 2 -- leagueSchedule envelope
# --------------------------------------------------------------------------


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_parser_renders_league_schedule(league: str) -> None:
    """The real captured body parses to one row per game, not zero."""
    df = parse_nba_stats_result_sets(_payload(league))
    assert isinstance(df, pl.DataFrame)
    expected = sum(len(d["games"]) for d in _payload(league)["leagueSchedule"]["gameDates"])
    assert expected > 0
    assert df.height == expected


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_parser_column_contract_matches_r(league: str) -> None:
    """Columns match ``hoopR::nba_schedule()`` / ``wehoop::wnba_schedule()``.

    In particular the nested ``homeTeam``/``awayTeam`` objects flatten to
    ``home_team_id`` (not ``home_team_team_id`` and not ``hometeam_teamid``),
    which is what ``stats_schedule_games`` selects on.
    """
    df = parse_nba_stats_result_sets(_payload(league))
    assert isinstance(df, pl.DataFrame)
    for column in (
        "game_id",
        "game_code",
        "game_date",
        "game_date_time_utc",
        "home_team_id",
        "away_team_id",
        "home_team_tricode",
        "away_team_tricode",
        "home_team_name",
        "home_team_city",
        "home_team_slug",
        "week_name",
        "season",
        "league_id",
        "season_type_id",
        "season_type_description",
    ):
        assert column in df.columns, column
    # list-valued members are dropped, as the R readers do
    assert not any(c.startswith(("broadcasters", "points_leaders")) for c in df.columns)


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_parser_derives_season_type_from_game_id(league: str) -> None:
    """``season_type_description`` comes from the 3rd char of ``game_id`` (R rule)."""
    df = parse_nba_stats_result_sets(_payload(league))
    assert isinstance(df, pl.DataFrame)
    labels = {
        "1": "Pre-Season",
        "2": "Regular Season",
        "3": "All-Star",
        "4": "Playoffs",
        "5": "Play-In Game",
    }
    for game_id, described in zip(df["game_id"].to_list(), df["season_type_description"].to_list()):
        assert described == labels.get(str(game_id)[2:3])
    assert df["season_type_description"].null_count() < df.height


def test_parser_returns_zero_row_frame_on_malformed_payload() -> None:
    """Parser contract: malformed input is a zero-row frame, never a raise."""
    for payload in ({}, {"leagueSchedule": {}}, {"leagueSchedule": {"gameDates": []}}):
        out = parse_nba_stats_result_sets(payload)
        assert isinstance(out, pl.DataFrame)
        assert out.height == 0


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_schedule_games_projects_the_real_capture(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    """End to end: real body -> parser -> adapter mini-schema, fully populated."""
    _patch_fetch(monkeypatch, league, lambda **kw: _payload(league))
    games = stats_schedule_games(league, 2026)
    assert games.height == sum(len(d["games"]) for d in _payload(league)["leagueSchedule"]["gameDates"])
    for column in ("game_date", "season_type", f"{league}_game_id", f"{league}_home_team_id"):
        assert games[column].null_count() == 0, column
    assert games["game_date"].dtype == pl.Date

    teams = stats_schedule_games(league, 2026, teams=True)
    assert teams.height > 0
    assert teams[f"{league}_team_id"].null_count() == 0
    assert teams[f"{league}_team_tricode"].null_count() == 0


# --------------------------------------------------------------------------
# Defect 1 -- a source that cannot produce data must fail loudly
# --------------------------------------------------------------------------


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_schedule_games_raises_when_the_fetch_fails(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    def boom(**kwargs: Any) -> dict:
        raise TimeoutError("connection reset")

    _patch_fetch(monkeypatch, league, boom)
    with pytest.raises(CrosswalkSourceError, match="scheduleleaguev2"):
        stats_schedule_games(league, 2026)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"meta": {"code": 200}},
        {"leagueSchedule": None},
        {"leagueSchedule": {"gameDates": None}},
        {"resultSets": []},
    ],
    ids=["none", "empty", "meta-only", "null-schedule", "null-gamedates", "wrong-envelope"],
)
def test_stats_schedule_games_raises_on_unproduced_payload(monkeypatch: pytest.MonkeyPatch, payload: Any) -> None:
    """An unrenderable payload is *unproduced*, and must not pass for empty."""
    _patch_fetch(monkeypatch, "wnba", lambda **kw: payload)
    with pytest.raises(CrosswalkSourceError):
        stats_schedule_games("wnba", 2026)


@pytest.mark.parametrize("teams", [False, True])
def test_stats_schedule_games_allows_a_provably_empty_season(monkeypatch: pytest.MonkeyPatch, teams: bool) -> None:
    """``gameDates: []`` is a real answer -- typed empty frame, no raise."""
    _patch_fetch(monkeypatch, "wnba", lambda **kw: {"leagueSchedule": {"seasonYear": "2026", "gameDates": []}})
    out = stats_schedule_games("wnba", 2026, teams=teams)
    assert out.height == 0
    expected = (
        ["wnba_team_id", "wnba_team_tricode", "wnba_team_name", "wnba_team_city", "wnba_team_slug"]
        if teams
        else ["game_date", "season_type", "wnba_game_id", "wnba_game_code", "wnba_home_team_id", "wnba_away_team_id"]
    )
    assert out.columns == expected


@pytest.mark.parametrize("athlete_key", ["athlete_id", "id"])
def test_espn_rosters_resolves_either_athlete_id_column(monkeypatch: pytest.MonkeyPatch, athlete_key: str) -> None:
    """wbb/wnba name the key ``athlete_id``; mbb/nba name it ``id`` -- both resolve.

    Looking for only ``athlete_id`` left every NBA and MBB roster with a null
    join key, so each player row silently failed to rejoin its own match.
    """
    roster = pl.DataFrame(
        {
            athlete_key: [4278039, 4433188],
            "full_name": ["Nickeil Alexander-Walker", "Devin Carter"],
            "jersey": ["7", "22"],
            "position_abbreviation": ["G", "G"],
            "date_of_birth": ["1998-09-02T07:00Z", "2002-03-18T08:00Z"],
        }
    )
    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda league: {"roster": lambda **kw: roster},
    )
    out = espn_rosters("nba", 1, "ATL", 2026)
    assert out["espn_athlete_id"].to_list() == ["4278039", "4433188"]


def test_espn_rosters_raises_when_the_athlete_id_column_is_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    """A populated roster with no athlete id is a rename upstream, not anonymity."""
    roster = pl.DataFrame({"full_name": ["A B"], "jersey": ["1"]})
    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda league: {"roster": lambda **kw: roster},
    )
    with pytest.raises(CrosswalkSourceError, match="no resolvable athlete id"):
        espn_rosters("nba", 1, "ATL", 2026)


def test_espn_rosters_still_tolerates_a_single_team_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Per-item tolerance is deliberate and unchanged -- only whole sources raise."""

    def boom(**kwargs: Any) -> pl.DataFrame:
        raise TimeoutError("one team is down")

    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda league: {"roster": boom},
    )
    out = espn_rosters("nba", 1, "ATL", 2026)
    assert out.height == 0
    assert "espn_athlete_id" in out.columns


def test_bart_super_sked_raises_when_the_fetch_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(url: str, **kwargs: Any) -> str:
        raise ConnectionError("torvik down")

    monkeypatch.setattr("sportsdataverse.mbb.torvik_runtime._get", boom, raising=True)
    with pytest.raises(CrosswalkSourceError, match="super_sked"):
        bart_super_sked("wbb", 2026)


def test_bart_super_sked_raises_when_a_payload_parses_to_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-empty blob that yields zero games is unproduced, not empty."""
    monkeypatch.setattr("sportsdataverse.mbb.torvik_runtime._get", lambda url, **kw: "<html>404</html>", raising=True)
    with pytest.raises(CrosswalkSourceError, match="zero games"):
        bart_super_sked("mbb", 2026)


def test_bart_super_sked_allows_a_provably_empty_season(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sportsdataverse.mbb.torvik_runtime._get", lambda url, **kw: "[]", raising=True)
    assert bart_super_sked("mbb", 2026).height == 0


# ---------------------------------------------------------------------------
# require_source -- the guard that replaced `except Exception: source = None`.
#
# The swallow it replaces is what hid a *missing provider module*: with no
# `fox_nba_teams` in the package at all, the ImportError was caught and the
# crosswalk built cleanly with silently-null fox_* columns for every team.
# ---------------------------------------------------------------------------


def test_require_source_passes_a_frame_through_including_an_empty_one() -> None:
    """A source that answered with no rows is legitimate -- no raise."""
    empty = pl.DataFrame(schema={"fox_team_id": pl.Utf8})
    assert require_source("provider()", lambda: empty).height == 0
    assert require_source("provider()", lambda: pl.DataFrame({"a": [1]})).height == 1


def test_require_source_raises_on_a_missing_provider_module() -> None:
    """The exact defect: an absent module read as a clean build."""

    def _fetch() -> Any:
        from sportsdataverse.nba.nba_fox_ext import fox_nba_teams_that_do_not_exist  # type: ignore[attr-defined]

        return fox_nba_teams_that_do_not_exist()

    with pytest.raises(CrosswalkSourceError, match="fox_nba_teams") as exc:
        require_source("fox_nba_teams()", _fetch)
    assert "ImportError" in str(exc.value), "the error must name why the source was missing"


@pytest.mark.parametrize("boom", [TimeoutError("down"), ValueError("bad payload")])
def test_require_source_raises_on_any_fetch_failure(boom: Exception) -> None:
    def _fetch() -> Any:
        raise boom

    with pytest.raises(CrosswalkSourceError, match=type(boom).__name__):
        require_source("provider()", _fetch)


@pytest.mark.parametrize("bad", [None, {}, [], "not a frame"])
def test_require_source_raises_when_the_result_is_not_a_frame(bad: Any) -> None:
    with pytest.raises(CrosswalkSourceError, match="expected a polars DataFrame"):
        require_source("provider()", lambda: bad)


def test_require_source_propagates_an_inner_source_error_unwrapped() -> None:
    """An adapter that already reported precisely must not be re-wrapped."""
    inner = CrosswalkSourceError("scheduleleaguev2 returned no leagueSchedule envelope")

    def _fetch() -> Any:
        raise inner

    with pytest.raises(CrosswalkSourceError) as exc:
        require_source("provider()", _fetch)
    assert exc.value is inner


_ESPN_DIR_COLS = ["team_id", "abbreviation", "display_name", "short_name", "team", "mascot"]


def _boom(*args: Any, **kwargs: Any) -> Any:
    raise TimeoutError("fox is down")


@pytest.mark.parametrize(
    ("league", "target", "extra"),
    [
        # nba/wnba resolve Stats before Fox, so hand those in pre-fetched: only
        # the Fox leg is under test here.
        ("nba", "fox_nba_teams", {"stats": pl.DataFrame()}),
        ("wnba", "fox_wnba_teams", {"stats": pl.DataFrame()}),
        ("mbb", "fox_mbb_teams_all", {"bart": pl.DataFrame()}),
        ("wbb", "fox_wbb_teams_all", {"bart": pl.DataFrame()}),
    ],
)
def test_team_crosswalk_raises_when_fox_cannot_be_produced(
    monkeypatch: pytest.MonkeyPatch, league: str, target: str, extra: dict
) -> None:
    """A dead Fox source must fail the build, not ship all-null fox_* columns."""
    import importlib

    monkeypatch.setattr(importlib.import_module(f"sportsdataverse.{league}.{league}_fox_ext"), target, _boom)
    crosswalk = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    # Bind the ESPN stub in the crosswalk module's own namespace -- it imported
    # espn_team_directory by value, so patching the source module is a no-op.
    monkeypatch.setattr(
        crosswalk,
        "espn_team_directory",
        lambda *a, **k: pl.DataFrame(schema={c: pl.Utf8 for c in _ESPN_DIR_COLS}),
    )
    with pytest.raises(CrosswalkSourceError, match=target):
        getattr(crosswalk, f"{league}_team_crosswalk")(season=2026, **extra)


# ---------------------------------------------------------------------------
# Provider imports must live INSIDE the guarded callable.
#
# Hoisting `from ...torvik import torvik_ratings` to the top of the builder
# defeated the guard twice over: a missing provider module raised a raw
# ImportError instead of CrosswalkSourceError, and a caller who supplied a
# pre-fetched `bart` frame still had to have the module installed.
# ---------------------------------------------------------------------------

_RATINGS_PROVIDER = {
    "mbb": ("sportsdataverse.mbb.torvik", "torvik_ratings"),
    "wbb": ("sportsdataverse.wbb.bart_wbb", "bart_wbb_ratings"),
}


def _stub_espn(monkeypatch: pytest.MonkeyPatch, crosswalk: Any) -> None:
    monkeypatch.setattr(
        crosswalk,
        "espn_team_directory",
        lambda *a, **k: pl.DataFrame(schema={c: pl.Utf8 for c in _ESPN_DIR_COLS}),
    )


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_team_crosswalk_raises_when_the_ratings_module_is_missing(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    """An absent Torvik provider must name itself as a CrosswalkSourceError."""
    import importlib
    import sys

    module_path, provider = _RATINGS_PROVIDER[league]
    monkeypatch.setitem(sys.modules, module_path, None)  # import of this module now raises
    crosswalk = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    _stub_espn(monkeypatch, crosswalk)
    with pytest.raises(CrosswalkSourceError, match=provider) as exc:
        getattr(crosswalk, f"{league}_team_crosswalk")(season=2026, fox=pl.DataFrame())
    assert module_path in str(exc.value), "the error must name the module that was missing"


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_team_crosswalk_supplied_ratings_frame_needs_no_provider_module(
    monkeypatch: pytest.MonkeyPatch, league: str
) -> None:
    """A pre-fetched `bart` frame must bypass the provider import entirely."""
    import importlib
    import sys

    module_path, _ = _RATINGS_PROVIDER[league]
    monkeypatch.setitem(sys.modules, module_path, None)
    crosswalk = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    _stub_espn(monkeypatch, crosswalk)
    out = getattr(crosswalk, f"{league}_team_crosswalk")(season=2026, fox=pl.DataFrame(), bart=pl.DataFrame())
    assert isinstance(out, pl.DataFrame)
    assert out.height == 0
