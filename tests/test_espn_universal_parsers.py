"""Offline parser tests for the universal-ESPN parsers in
``sportsdataverse._common_espn_parsers``.

These tests run against captured live payloads in
``tests/fixtures/espn/`` so the parser logic can be exercised without
hitting ESPN. Captures were collected 2026-05-23.

Live integration tests live in ``test_espn_live.py`` (gated by
``SDV_PY_LIVE_TESTS=1``); the offline tests here cover schemas and the
``return_parsed=True`` shim contract end to end.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "espn"


def _load(stem: str) -> dict:
    return json.loads((FIXTURE_DIR / f"{stem}.json").read_text(encoding="utf-8"))


# ===========================================================================
# Team-scoped Site v2
# ===========================================================================


def test_parse_team_schedule_returns_one_row_per_event():
    from sportsdataverse._common_espn_parsers import parse_team_schedule

    df = parse_team_schedule(_load("team_schedule_nba"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 1, "expected at least one scheduled event row"
    # Site v2 schedule events always carry id, date, name, season_year
    for col in ("id", "date", "name", "season_year"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_team_roster_returns_one_row_per_athlete():
    from sportsdataverse._common_espn_parsers import parse_team_roster

    df = parse_team_roster(_load("team_roster_nba"))
    assert isinstance(df, pl.DataFrame)
    # LAL has ~17 active players in this capture; tolerate roster churn
    assert df.height >= 10, f"expected >=10 athletes, got {df.height}"
    for col in ("id", "first_name", "last_name", "full_name"):
        assert col in df.columns, f"missing column {col!r}"


# ===========================================================================
# News / injuries
# ===========================================================================


def test_parse_news_returns_one_row_per_article():
    from sportsdataverse._common_espn_parsers import parse_news

    df = parse_news(_load("news_nba"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 1, "expected at least one article row"
    for col in ("id", "headline", "type"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_injuries_returns_one_row_per_team():
    from sportsdataverse._common_espn_parsers import parse_injuries

    df = parse_injuries(_load("injuries_nba"))
    assert isinstance(df, pl.DataFrame)
    # Live snapshot shows ~26 teams with injuries reported.
    assert df.height >= 1, f"expected >=1 team rows, got {df.height}"
    assert "id" in df.columns
    assert "display_name" in df.columns
    # The inner per-player injuries list is stringified for polars ingestion.
    assert "injuries" in df.columns


# ===========================================================================
# Generic items (Core v2 paginated + entries variants)
# ===========================================================================


@pytest.mark.parametrize(
    "fixture,expected_min_rows",
    [
        ("venues_core_nba",     1),  # 5-row capture, $ref-only items
        ("events_core_nba",     1),  # 1-row capture, $ref-only items
    ],
)
def test_parse_items_handles_items_key(fixture, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load(fixture))
    assert df.height >= expected_min_rows, (
        f"{fixture}: expected >= {expected_min_rows} rows, got {df.height}"
    )
    # Core v2 paginated items often contain just $ref pointers
    assert any(c.endswith("ref") or c == "_ref" or c == "$ref"
               for c in df.columns), f"no $ref column in {df.columns}"


def test_parse_items_handles_entries_key_for_statisticslog():
    """athlete_statisticslog ships ``{entries: [...]}`` not ``{items: [...]}``;
    parse_items's fallback key list should still find the rows."""
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load("athlete_statslog_lbj"))
    assert df.height >= 1, (
        "parse_items must fall back to the 'entries' key for "
        "athlete_statisticslog payloads"
    )


# ===========================================================================
# Empty-payload contract for ALL new parsers
# ===========================================================================


@pytest.mark.parametrize(
    "parser_name",
    [
        "parse_items",
        "parse_team_schedule",
        "parse_team_roster",
        "parse_news",
        "parse_injuries",
    ],
)
def test_new_parser_handles_empty_payload(parser_name):
    from sportsdataverse import _common_espn_parsers as parsers

    parser = getattr(parsers, parser_name)
    df = parser({})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0, f"{parser_name} should return 0 rows on empty"


# ===========================================================================
# pandas opt-in
# ===========================================================================


def test_parse_team_roster_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse._common_espn_parsers import parse_team_roster

    df = parse_team_roster(_load("team_roster_nba"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)


# ===========================================================================
# Registry expansion contract
# ===========================================================================


def test_endpoint_parsers_registry_includes_new_short_names():
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS

    # The five new families we just registered.
    expected_new = {
        "team_schedule", "team_roster",
        "news", "team_news", "athlete_news",
        "injuries", "team_injuries", "athlete_injuries",
        "venues", "franchises", "events", "athletes_index",
        "athlete_statisticslog",
    }
    missing = expected_new - set(ENDPOINT_PARSERS)
    assert not missing, (
        f"ENDPOINT_PARSERS missing newly-introduced entries: {sorted(missing)}"
    )


def test_return_parsed_kwarg_is_wired_for_new_short_names():
    """A wrapper for a newly-registered parser should now accept
    ``return_parsed=True`` and ``return_as_pandas=True`` kwargs."""
    import inspect

    from sportsdataverse.nba.nba_espn_ext import (
        espn_nba_news,
        espn_nba_team_roster,
        espn_nba_team_schedule,
        espn_nba_venues,
    )

    for fn in (espn_nba_team_schedule, espn_nba_team_roster,
               espn_nba_news, espn_nba_venues):
        sig = inspect.signature(fn)
        assert "return_parsed" in sig.parameters, (
            f"{fn.__name__} missing return_parsed kwarg"
        )
        assert "return_as_pandas" in sig.parameters, (
            f"{fn.__name__} missing return_as_pandas kwarg"
        )


def test_summary_parsers_handle_real_nba_summary_payload():
    """Live-captured Site v2 summary for the 2024 NBA Finals G5 (event
    401585607). Verifies all 5 sub-parsers + the dispatcher against a
    real ~700KB payload."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary,
        parse_summary_boxscore_player,
        parse_summary_boxscore_team,
        parse_summary_leaders,
        parse_summary_plays,
        parse_summary_winprobability,
    )

    payload = _load("summary_nba")

    # ---- boxscore_player ----
    df = parse_summary_boxscore_player(payload)
    assert df.height >= 20, f"expected >=20 athletes, got {df.height}"
    for col in ("team_id", "team_abbreviation", "athlete_id",
                "athlete_display_name", "starter", "active"):
        assert col in df.columns, f"missing column {col!r}"

    # ---- boxscore_team ----
    df = parse_summary_boxscore_team(payload)
    # 2 teams x ~23 stats each ~ 46 rows
    assert df.height >= 20, f"expected >=20 team-stat rows, got {df.height}"
    for col in ("team_id", "stat_name", "stat_label", "stat_display_value"):
        assert col in df.columns

    # ---- plays ----
    df = parse_summary_plays(payload)
    assert df.height >= 100, f"expected >=100 plays, got {df.height}"
    for col in ("id", "sequence_number", "text", "home_score", "away_score"):
        assert col in df.columns

    # ---- winprobability ----
    df = parse_summary_winprobability(payload)
    assert df.height >= 100, f"expected >=100 wp ticks, got {df.height}"
    for col in ("home_win_percentage", "tie_percentage", "play_id"):
        assert col in df.columns

    # ---- leaders ----
    df = parse_summary_leaders(payload)
    assert df.height >= 4, f"expected >=4 (team x category) leader rows, got {df.height}"
    for col in ("team_id", "category_name", "athlete_id", "value"):
        assert col in df.columns

    # ---- dispatcher: section=None returns dict of all 5 ----
    out = parse_summary(payload)
    assert set(out) == {
        "boxscore_player", "boxscore_team",
        "plays", "winprobability", "leaders",
    }
    for name, frame in out.items():
        assert frame.height > 0, f"dispatcher returned empty frame for {name}"

    # ---- dispatcher: section="plays" returns just that frame ----
    single = parse_summary(payload, section="plays")
    assert single.height >= 100


def test_summary_dispatcher_returns_zero_row_dict_on_empty_payload():
    from sportsdataverse._common_espn_parsers import parse_summary

    out = parse_summary({})
    assert isinstance(out, dict)
    for name, frame in out.items():
        assert frame.height == 0, f"{name}: expected 0 rows on empty payload"


def test_summary_dispatcher_raises_on_unknown_section():
    from sportsdataverse._common_espn_parsers import parse_summary

    with pytest.raises(ValueError, match="Unknown summary section"):
        parse_summary({}, section="not_a_section")


def test_summary_section_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse._common_espn_parsers import parse_summary

    df = parse_summary(_load("summary_nba"), section="plays",
                       return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 100


def test_summary_parser_routes_via_endpoint_parsers_registry():
    """``summary`` is registered in ENDPOINT_PARSERS so the
    return_parsed shim should route through parse_summary."""
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, parse_summary

    assert "summary" in ENDPOINT_PARSERS
    assert ENDPOINT_PARSERS["summary"] is parse_summary


def test_return_parsed_shim_dispatches_through_new_parser_for_team_roster():
    """The wrapper closure for espn_nba_team_roster should route through
    parse_team_roster when return_parsed=True. Verify offline by
    monkey-patching the core HTTP fetcher to return a known fixture."""
    import polars as pl

    import sportsdataverse._common_espn as ce

    fixture = _load("team_roster_nba")
    real_get = ce._get
    ce._get = lambda *args, **kwargs: fixture
    try:
        from sportsdataverse.nba.nba_espn_ext import espn_nba_team_roster

        raw = espn_nba_team_roster(team_id=13)
        assert isinstance(raw, dict)
        assert "athletes" in raw

        df = espn_nba_team_roster(team_id=13, return_parsed=True)
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 10
        assert "first_name" in df.columns
    finally:
        ce._get = real_get
