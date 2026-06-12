"""Offline parser tests for the NHL Stats REST + Records parser layers.

* ``sportsdataverse.nhl.nhl_stats_rest_parsers.parse_nhl_stats_rest``
  handles the 21 wrappers in ``nhl_stats_rest``.
* ``sportsdataverse.nhl.nhl_records_parsers.parse_nhl_records``
  handles the 50 wrappers in ``nhl_records``.

Both parsers unwrap the same ``{data: [...], total: N}`` envelope used
by every endpoint in their respective surfaces.

Captured fixtures live in ``tests/fixtures/nhl_stats_rest/`` and
``tests/fixtures/nhl_records/``. See those directories' README.md for
provenance.
"""

from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import load_fixture

# Category constants — passed to the shared load_fixture helper.
# This file loads from two directories, hence the 2-arg ``_load``
# signature below.
STATS_REST_DIR = "nhl_stats_rest"
RECORDS_DIR = "nhl_records"


def _load(directory: str, stem: str) -> dict:
    """Local alias for :func:`tests.conftest.load_fixture` that keeps
    the 2-arg ``_load(directory, stem)`` call sites in this file
    working unchanged."""
    return load_fixture(directory, stem)


# ===========================================================================
# parse_nhl_stats_rest
# ===========================================================================


@pytest.mark.parametrize(
    "fixture,expected_min_rows",
    [
        ("stats_rest_season", 108),  # every NHL season since 1917
        ("stats_rest_franchise", 40),  # 32 active + 8 defunct
        ("stats_rest_country", 40),  # 49 in capture
        ("stats_rest_glossary", 200),  # 321 in capture
        ("stats_rest_skater_summary_2024", 20),
        ("stats_rest_goalie_summary_2024", 10),
        ("stats_rest_team_summary_2024", 32),  # all current NHL teams
    ],
)
def test_parse_nhl_stats_rest_works_on_data_endpoints(fixture, expected_min_rows):
    from sportsdataverse.nhl import parse_nhl_stats_rest

    df = parse_nhl_stats_rest(_load(STATS_REST_DIR, fixture))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= expected_min_rows, f"{fixture}: expected >= {expected_min_rows} rows, got {df.height}"


def test_parse_nhl_stats_rest_returns_zero_rows_for_meta_config():
    """``stats_rest_config`` is a meta payload with no ``data`` key —
    the parser must return a zero-row frame instead of raising."""
    from sportsdataverse.nhl import parse_nhl_stats_rest

    df = parse_nhl_stats_rest(_load(STATS_REST_DIR, "stats_rest_config"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


# ===========================================================================
# parse_nhl_records
# ===========================================================================


@pytest.mark.parametrize(
    "fixture,expected_min_rows",
    [
        ("records_franchise", 40),
        ("records_franchise_team_totals", 10),
        ("records_coach", 10),
        ("records_draft", 10),
        ("records_player_records", 10),
        ("records_attendance", 50),  # 80 years in capture
    ],
)
def test_parse_nhl_records_works_on_known_endpoints(fixture, expected_min_rows):
    from sportsdataverse.nhl import parse_nhl_records

    df = parse_nhl_records(_load(RECORDS_DIR, fixture))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= expected_min_rows, f"{fixture}: expected >= {expected_min_rows} rows, got {df.height}"


# ===========================================================================
# Empty-payload + pandas opt-in + registry contract
# ===========================================================================


@pytest.mark.parametrize(
    "parser_name,module_attr",
    [
        ("parse_nhl_stats_rest", "nhl_stats_rest_parsers"),
        ("parse_nhl_records", "nhl_records_parsers"),
    ],
)
def test_parser_handles_empty_payload(parser_name, module_attr):
    from sportsdataverse import nhl

    parser = getattr(nhl, parser_name)
    df = parser({})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_parse_nhl_stats_rest_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse.nhl import parse_nhl_stats_rest

    df = parse_nhl_stats_rest(
        _load(STATS_REST_DIR, "stats_rest_franchise"),
        return_as_pandas=True,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 40


def test_parse_nhl_records_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse.nhl import parse_nhl_records

    df = parse_nhl_records(
        _load(RECORDS_DIR, "records_franchise"),
        return_as_pandas=True,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 40


def test_nhl_stats_rest_endpoint_parsers_registry_references_real_wrappers():
    from sportsdataverse.nhl import NHL_STATS_REST_ENDPOINT_PARSERS, nhl_stats_rest

    for fn_name in NHL_STATS_REST_ENDPOINT_PARSERS:
        assert hasattr(nhl_stats_rest, fn_name), (
            f"NHL_STATS_REST_ENDPOINT_PARSERS references missing wrapper {fn_name!r}"
        )


def test_parser_for_nhl_stats_rest_always_returns_callable():
    from sportsdataverse.nhl import (
        parse_nhl_stats_rest,
        parser_for_nhl_stats_rest,
    )

    assert parser_for_nhl_stats_rest("nhl_stats_rest_season") is parse_nhl_stats_rest
    # Unknown name falls back to the same parser
    assert parser_for_nhl_stats_rest("nhl_stats_rest_does_not_exist") is parse_nhl_stats_rest


def test_parser_for_nhl_records_always_returns_callable():
    from sportsdataverse.nhl import parse_nhl_records, parser_for_nhl_records

    assert parser_for_nhl_records("nhl_records_franchise") is parse_nhl_records
    assert parser_for_nhl_records("nhl_records_does_not_exist") is parse_nhl_records
