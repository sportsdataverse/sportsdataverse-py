"""Offline parser tests for NHL EDGE payloads.

These tests run against captured live payloads in
``tests/fixtures/nhl_edge/`` so the parser logic can be exercised without
hitting ``api-web.nhle.com``. Captures were collected 2026-05-23 against
the 2024-25 regular season.

Live integration tests (the ones that actually hit the API) live in
``test_espn_live.py`` and are gated by ``SDV_PY_LIVE_TESTS=1``.
"""

from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import load_fixture


def _load(name: str) -> dict:
    """Local alias for :func:`tests.conftest.load_fixture` bound to
    this module's fixture category ("nhl_edge")."""
    return load_fixture("nhl_edge", name)


# ===========================================================================
# Family 2 — parse_edge_detail (one row per call)
# ===========================================================================


@pytest.mark.parametrize(
    "fixture,min_columns",
    [
        ("skater_detail", 30),
        ("team_detail", 30),
        ("goalie_detail", 20),
        ("skater_shot_speed", 20),
    ],
)
def test_parse_edge_detail_returns_single_row_with_columns(fixture, min_columns):
    from sportsdataverse.nhl import parse_edge_detail

    df = parse_edge_detail(_load(fixture))
    assert isinstance(df, pl.DataFrame), f"expected polars frame, got {type(df)}"
    assert df.height == 1, f"detail payload should flatten to 1 row, got {df.height}"
    assert len(df.columns) >= min_columns, (
        f"expected >= {min_columns} flattened columns, got {len(df.columns)}: {df.columns[:8]}"
    )


def test_parse_edge_detail_carries_player_identifier_for_skater():
    from sportsdataverse.nhl import parse_edge_detail

    df = parse_edge_detail(_load("skater_detail"))
    assert "player_id" in df.columns
    assert "player_first_name_default" in df.columns or "player_first_name" in df.columns


def test_parse_edge_detail_carries_team_identifier_for_team():
    from sportsdataverse.nhl import parse_edge_detail

    df = parse_edge_detail(_load("team_detail"))
    assert "team_id" in df.columns
    assert "team_abbrev" in df.columns


# ===========================================================================
# Family 3 — parse_edge_shot_location (17-cell heat map)
# ===========================================================================


@pytest.mark.parametrize("fixture", ["team_shot_loc", "goalie_shot_loc"])
def test_parse_edge_shot_location_returns_17_row_grid(fixture):
    from sportsdataverse.nhl import parse_edge_shot_location

    df = parse_edge_shot_location(_load(fixture))
    assert df.height == 17, f"expected 17-cell grid, got {df.height} rows"
    assert "area" in df.columns


# ===========================================================================
# Family 4 — parse_edge_zone_time (strength-state splits)
# ===========================================================================


def test_parse_edge_zone_time_unrolls_strength_states_for_skater():
    from sportsdataverse.nhl import parse_edge_zone_time

    df = parse_edge_zone_time(_load("skater_zone_time"))
    # The /edge/skater-zone-time endpoint returns 4 strength-state rows
    # (5v5, PP, PK, all-strengths).
    assert df.height == 4, f"expected 4 strength splits, got {df.height}"
    assert "strength_code" in df.columns
    assert "offensive_zone_pctg" in df.columns


# ===========================================================================
# Sub-frame parsers — extract nested lists from detail payloads
# ===========================================================================


@pytest.mark.parametrize(
    "fixture",
    ["skater_detail", "team_detail", "goalie_detail", "team_shot_loc", "goalie_shot_loc"],
)
def test_parse_edge_sog_details_returns_17_rows(fixture):
    from sportsdataverse.nhl import parse_edge_sog_details

    df = parse_edge_sog_details(_load(fixture))
    assert df.height == 17, f"{fixture}: expected 17 SOG-detail rows, got {df.height}"
    assert "area" in df.columns


@pytest.mark.parametrize(
    "fixture,min_rows",
    [
        ("skater_detail", 4),
        ("team_detail", 4),
        ("goalie_detail", 4),
        # team-shot-location-detail ships 12-row totals; we accept >= 4.
        ("team_shot_loc", 4),
        ("goalie_shot_loc", 4),
    ],
)
def test_parse_edge_sog_summary_returns_location_codes(fixture, min_rows):
    from sportsdataverse.nhl import parse_edge_sog_summary

    df = parse_edge_sog_summary(_load(fixture))
    assert df.height >= min_rows, f"{fixture}: expected >= {min_rows} location-code rows, got {df.height}"
    assert "location_code" in df.columns


def test_parse_edge_hardest_shots_returns_10_row_list():
    from sportsdataverse.nhl import parse_edge_hardest_shots

    df = parse_edge_hardest_shots(_load("skater_shot_speed"))
    assert df.height == 10, f"expected 10 hardest-shots rows, got {df.height}"
    # shotSpeed is a nested {imperial, metric} dict in the live payload,
    # so the flattened columns are shot_speed_imperial / shot_speed_metric.
    assert any(c.startswith("shot_speed") for c in df.columns), f"missing shot_speed columns: {df.columns}"
    assert "game_date" in df.columns


# ===========================================================================
# Empty-payload handling
# ===========================================================================


@pytest.mark.parametrize(
    "parser_name",
    [
        "parse_edge_detail",
        "parse_edge_shot_location",
        "parse_edge_zone_time",
        "parse_edge_sog_details",
        "parse_edge_sog_summary",
        "parse_edge_hardest_shots",
        "parse_edge_top10",
        "parse_edge_payload",
    ],
)
def test_parser_handles_empty_payload(parser_name):
    """All EDGE parsers must return a zero-row frame on empty input."""
    import sportsdataverse.nhl as nhl_mod

    parser = getattr(nhl_mod, parser_name)
    df = parser({})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0, f"{parser_name} should return 0 rows for empty payload"


# ===========================================================================
# Pandas / polars contract
# ===========================================================================


def test_parse_edge_detail_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse.nhl import parse_edge_detail

    df = parse_edge_detail(_load("skater_detail"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame), f"expected pandas, got {type(df)}"
    assert len(df) == 1


def test_parse_edge_sog_details_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse.nhl import parse_edge_sog_details

    df = parse_edge_sog_details(_load("team_detail"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 17


# ===========================================================================
# Registry sanity
# ===========================================================================


def test_edge_endpoint_parsers_registry_covers_all_wrappers():
    """Every nhl_edge_* wrapper should have a registered parser, or fall
    through to the generic ``parse_edge_payload`` via parser_for_edge.
    """
    from sportsdataverse.nhl import EDGE_ENDPOINT_PARSERS, nhl_edge, parser_for_edge

    wrapper_names = [
        name for name in dir(nhl_edge) if name.startswith("nhl_edge_") and callable(getattr(nhl_edge, name))
    ]
    assert wrapper_names, "no nhl_edge_* wrappers found — import error?"

    for fn_name in wrapper_names:
        parser = parser_for_edge(fn_name)
        assert callable(parser), f"parser_for_edge({fn_name!r}) returned non-callable"

    # And ensure every registry entry references a real parser callable.
    for fn_name, parser in EDGE_ENDPOINT_PARSERS.items():
        assert callable(parser), f"EDGE_ENDPOINT_PARSERS[{fn_name!r}] is not callable"


def test_edge_subframe_parsers_registry_consistent():
    """EDGE_SUBFRAME_PARSERS keys must be a subset of registered wrappers."""
    from sportsdataverse.nhl import EDGE_SUBFRAME_PARSERS, nhl_edge

    wrapper_names = {
        name for name in dir(nhl_edge) if name.startswith("nhl_edge_") and callable(getattr(nhl_edge, name))
    }
    for fn_name in EDGE_SUBFRAME_PARSERS:
        assert fn_name in wrapper_names, f"EDGE_SUBFRAME_PARSERS references missing wrapper {fn_name!r}"
