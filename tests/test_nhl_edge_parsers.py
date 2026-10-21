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


@pytest.mark.parametrize(
    "board",
    [
        "skater_shot_speed_top_10",
        "skater_speed_top_10",
        "skater_distance_top_10",
        "skater_zone_time_top_10",
        "goalie_5v5_top_10",
        "team_skating_distance_top_10",
        "team_skating_speed_top_10",
        "team_zone_time_top_10",
    ],
)
def test_top10_parses_the_bare_list_the_live_boards_send(board):
    """Real captures (2026-10-07): every board answers with a bare JSON list, never a dict."""
    from sportsdataverse.nhl import parse_edge_top10

    raw = _load(board)
    assert isinstance(raw, list) and raw
    df = parse_edge_top10(raw)
    assert df.height == len(raw) and df.width > 0


# ===========================================================================
# Schema drift warns instead of reading as "no data" (task 4.9, A-L60)
# ===========================================================================


def _drifted(raw, key=None):
    """The real capture with the row list at ``key`` (or the bare list) turned into a list of strings."""
    if key is None:
        return [str(row) for row in raw]
    return {**raw, key: [str(row) for row in raw[key]]}


def _nulled(raw, key, keep):
    """The real capture with null rows added to the row list at ``key`` (``keep``), or only nulls left (4.13)."""
    rows = raw if key is None else raw[key]
    nulled = [None, *rows, None] if keep else [None] * len(rows)
    return nulled if key is None else {**raw, key: nulled}


@pytest.mark.parametrize(
    ("parser", "fixture", "key", "where"),
    [
        ("parse_edge_top10", "skater_speed_top_10", None, "parse_edge_top10: rows"),
        ("parse_edge_sog_details", "skater_detail", "sogDetails", "parse_edge_sog_details: sogDetails"),
        ("parse_edge_sog_summary", "skater_detail", "sogSummary", "parse_edge_sog_summary: sogSummary"),
        ("parse_edge_hardest_shots", "skater_shot_speed", "hardestShots", "parse_edge_hardest_shots: hardestShots"),
        (
            "parse_edge_shot_location",
            "team_shot_loc",
            "shotLocationDetails",
            "parse_edge_shot_location: shotLocationDetails",
        ),
        ("parse_edge_zone_time", "skater_zone_time", "zoneTimeDetails", "parse_edge_zone_time: zoneTimeDetails"),
    ],
)
def test_schema_drift_warns_instead_of_reading_as_no_data(parser, fixture, key, where, json_normalize_flavour):
    """A drifted row list still yields zero rows (the parser contract) but now SAYS so (A-L60)."""
    import sportsdataverse.nhl.nhl_edge_parsers as mod
    from sportsdataverse.errors import SchemaDriftWarning

    parse, raw = getattr(mod, parser), _load(fixture)
    assert parse(raw).height > 0  # the real capture parses to rows, with no warning (pytest.ini: warnings are errors)
    # 4.13: a null row is no data, dropped without a warning; a list of nulls is zero rows, never N x 0.
    assert parse(_nulled(raw, key, keep=True)).equals(parse(raw))  # a leading null too, order kept (R953-2/-3)
    assert parse(_nulled(raw, key, keep=False)).height == 0
    with pytest.warns(SchemaDriftWarning, match=where):
        out = parse(_drifted(raw, key))
    if parser != "parse_edge_zone_time":  # zone_time moves on to the next zone key (zoneStarts) after warning
        assert out.height == 0


def test_edge_top10_finds_a_wrapped_list_behind_a_leading_null():
    """R953-2: the last-ditch lookup (a list under a key top10 does not know) decides by the first NON-null row."""
    from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_top10

    rows = _load("skater_speed_top_10")
    expected = parse_edge_top10(rows)
    assert expected.height > 1
    assert parse_edge_top10({"wrapped": [None, *rows]}).equals(expected)


def test_edge_payload_later_row_drift_warns(json_normalize_flavour):
    """parse_edge_payload picks its row list by the FIRST row, so only a later row can drift: it warns and
    falls back to the single detail row (pandas 2 read the string rows as empty ones, silently; WB-1)."""
    from sportsdataverse.errors import SchemaDriftWarning
    from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_payload

    raw = _load("skater_skating_speed_detail")
    rows = raw["topSkatingSpeeds"]
    assert parse_edge_payload(raw).height == len(rows) > 1
    # 4.13 / R953-2: null rows are dropped wherever they sit; a leading one must not hide the list.
    assert parse_edge_payload({**raw, "topSkatingSpeeds": [None, *rows, None]}).equals(parse_edge_payload(raw))
    with pytest.warns(SchemaDriftWarning, match="parse_edge_payload: topSkatingSpeeds"):
        assert parse_edge_payload({**raw, "topSkatingSpeeds": [rows[0], *map(str, rows[1:])]}).height == 1


def test_edge_nested_shot_location_drift_warns(json_normalize_flavour):
    """The nested (dict-of-sections) shot-location shape: no committed capture has it, so the real
    team_shot_loc zone list is moved under one section. A later zone that is not an object warns (WB-1)."""
    from sportsdataverse.errors import SchemaDriftWarning
    from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_shot_location

    zones = _load("team_shot_loc")["shotLocationDetails"]
    assert parse_edge_shot_location({"all": {"shotLocationDetails": zones}}).height == len(zones)
    nested = parse_edge_shot_location({"all": {"shotLocationDetails": zones}})
    assert parse_edge_shot_location({"all": {"shotLocationDetails": [None, *zones, None]}}).equals(nested)  # 4.13
    drifted = {"all": {"shotLocationDetails": [zones[0], *map(str, zones[1:])]}}
    with pytest.warns(SchemaDriftWarning, match=r"parse_edge_shot_location: all\.shotLocationDetails"):
        assert parse_edge_shot_location(drifted).height == 0
