"""Offline tests for the 404-suggestion engine in sportsdataverse.errors."""

from __future__ import annotations

import pytest

from sportsdataverse.errors import (
    AssetFetchError,
    NoDataError,
    NoESPNDataError,
    SeasonNotFoundError,
    SportsDataverseError,
    _format_404,
    suggest_next_action,
)


def test_error_hierarchy_under_base() -> None:
    # Every package error is catchable via the single SportsDataverseError base,
    # while remaining catchable individually (backwards compatible).
    assert issubclass(SeasonNotFoundError, SportsDataverseError)
    assert issubclass(NoESPNDataError, SportsDataverseError)
    assert issubclass(SportsDataverseError, Exception)
    with pytest.raises(SportsDataverseError):
        raise NoESPNDataError("no data")


def test_no_espn_data_error_alias_is_the_canonical_class() -> None:
    """``NoESPNDataError`` must stay a true alias, not a parallel class.

    The error was named when it was ESPN-only, but ``download`` raises it for any
    404 -- release assets included -- so the canonical name is ``NoDataError``. If
    a refactor ever makes these two distinct classes, existing
    ``except NoESPNDataError`` code silently stops catching, which is exactly the
    failure this locks out.
    """
    assert NoESPNDataError is NoDataError
    # catchable in both directions, whichever name the caller happens to use
    with pytest.raises(NoESPNDataError):
        raise NoDataError("missing")
    with pytest.raises(NoDataError):
        raise NoESPNDataError("missing")


def test_failed_fetch_is_not_missing_data() -> None:
    """A fetch that FAILED must not be catchable as "there is no data".

    Collapsing the two would let a rate-limited (403) season be recorded as an
    empty one -- silent data loss rather than a visible error.
    """
    assert issubclass(AssetFetchError, SportsDataverseError)
    assert not issubclass(AssetFetchError, NoDataError)
    assert not issubclass(NoDataError, AssetFetchError)
    with pytest.raises(AssetFetchError):
        raise AssetFetchError("HTTP 403")


@pytest.mark.parametrize(
    "url,must_contain",
    [
        # Team-roster 404 -> find_team
        (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/99999/roster",
            "find_team(name, league='nba')",
        ),
        (
            "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/0/schedule",
            "find_team(name, league='mlb')",
        ),
        # Bare team URL -> find_team
        (
            "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/9999",
            "find_team(name, league='nfl')",
        ),
        # Athlete 404 -> find_athlete
        (
            "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/0/overview",
            "find_athlete(name, league='nhl', team=<team>)",
        ),
        (
            "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/123/stats",
            "find_athlete(name, league='mlb', team=<team>)",
        ),
        # Summary 404 -> find_event
        (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=999",
            "find_event(date, league='nba'",
        ),
        # Generic event 404 -> find_event
        (
            "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/0",
            "find_event(date, league='nfl'",
        ),
        # Old season -> season suggestion
        (
            "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/1850",
            "Season ID may predate available data",
        ),
        # Scoreboard 404 -> scoreboard-specific hint
        (
            "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=99999999",
            "dates=YYYYMMDD",
        ),
    ],
)
def test_suggest_next_action_produces_relevant_hint(url, must_contain):
    hint = suggest_next_action(url)
    assert hint is not None, f"no suggestion for {url}"
    assert must_contain in hint, f"hint for {url} should mention {must_contain!r}, got:\n  {hint}"


def test_suggest_next_action_returns_none_for_unrelated_url():
    """A URL that doesn't match any known ESPN entity pattern should
    return None — not every 404 needs a hint."""
    hint = suggest_next_action("https://example.com/random/api/path")
    assert hint is None


@pytest.mark.parametrize(
    "url,expected_league",
    [
        ("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/13/roster", "nba"),
        ("https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams/20/roster", "wnba"),
        ("https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/150/roster", "mbb"),
        ("https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/41/roster", "wbb"),
        ("https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/12/roster", "nfl"),
        ("https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/333/roster", "cfb"),
        ("https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/10/roster", "mlb"),
        ("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/22/roster", "nhl"),
    ],
)
def test_league_inference_works_for_every_sport(url, expected_league):
    """The error suggestion needs to identify the league from the URL
    so the find_* hint includes league=<the-right-one>."""
    hint = suggest_next_action(url)
    assert hint is not None
    assert f"league={expected_league!r}" in hint


def test_format_404_includes_url_and_suggestion():
    msg = _format_404(
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/0/roster",
    )
    assert "https://site.api.espn.com" in msg
    assert "Suggestion:" in msg
    assert "find_team(name, league='nba')" in msg
    assert "from `sportsdataverse import" in msg


def test_format_404_skips_suggestion_when_url_unrecognised():
    """Random URLs shouldn't get a contrived hint."""
    msg = _format_404("https://example.com/totally/unrelated")
    assert "Suggestion:" not in msg
    assert "https://example.com" in msg
