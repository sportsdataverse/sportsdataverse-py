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


@pytest.mark.parametrize(
    ("module", "parser", "directory", "stem"),
    [
        ("nhl_stats_rest_parsers", "parse_nhl_stats_rest", "nhl_stats_rest", "stats_rest_country"),
        ("nhl_records_parsers", "parse_nhl_records", "nhl_records", "records_allstar_coach_career"),
    ],
)
def test_schema_drift_warns_instead_of_reading_as_no_data(module, parser, directory, stem, json_normalize_flavour):
    """A ``data`` list that stops holding objects gives zero rows AND a SchemaDriftWarning (A-L60)."""
    import importlib

    from sportsdataverse.errors import SchemaDriftWarning

    parse, raw = getattr(importlib.import_module(f"sportsdataverse.nhl.{module}"), parser), _load(directory, stem)
    assert parse(raw).height > 0
    # 4.13: a null row is no data, dropped without a warning; a list of nulls is zero rows, never N x 0.
    assert parse({**raw, "data": [None, *raw["data"], None]}).equals(parse(raw))  # order kept (R953-3)
    assert parse({**raw, "data": [None] * len(raw["data"])}).height == 0
    with pytest.warns(SchemaDriftWarning, match=f"{parser}: data"):
        assert parse({**raw, "data": [str(row) for row in raw["data"]]}).height == 0


# ---------------------------------------------------------------------------
# Task 5.2 (B-H5): every NHL id is Int64 at the parser boundary
# ---------------------------------------------------------------------------
#: Id columns whose values are text on a capture, kept as they come: the stats-rest season label "1971-72" and
#: the country endpoint's ISO code "DEU".
_NHL_TEXT_IDS = {("stats_rest_country.json", "id")}
_NHL_TEXT_ID_COLUMNS = {"formatted_season_id"}


def _nhl_native_cases():
    import yaml

    from tools.codegen import generate, spec

    nat = yaml.safe_load((generate.ROOT / "tools" / "codegen" / "native_fixture_map.yaml").read_text("utf-8"))
    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    for api in ("nhl_api_web", "nhl_records", "nhl_stats_rest", "nhl_edge"):
        fa = spec.load_flat_api(generate.ENDPOINTS / f"{api}.yaml", params)
        by_short = {e.short: e for e in fa.endpoints}
        for fname, short in nat[api].items():
            ep = by_short.get(short)
            if ep is not None and ep.parser:
                yield pytest.param(api, fa.parser_module, ep.parser, fname, id=f"{api}/{fname}")


@pytest.mark.parametrize(("api", "module", "parser", "fname"), list(_nhl_native_cases()))
def test_nhl_ids_int64(api, module, parser, fname):
    """B-H5: ``details_player_id`` was Float64 on the full-game pbp, ``player_id`` Int64 / Float64 / String by records
    payload ('8445802.0' once stringified). One dtype per id, from the raw integer."""
    import importlib

    raw = load_fixture(api, fname.removesuffix(".json"))
    out = getattr(importlib.import_module(f"sportsdataverse.{module}"), parser)(raw)
    for sec, df in (out if isinstance(out, dict) else {"": out}).items():
        ids = {c: df.schema[c] for c in df.columns if c == "id" or c.endswith("_id")}
        text = {c for c in ids if c in _NHL_TEXT_ID_COLUMNS or (fname, c) in _NHL_TEXT_IDS}
        assert {c: t for c, t in ids.items() if c not in text and t != pl.Int64} == {}, sec
        assert all(ids[c] == pl.String for c in text), sec


@pytest.mark.parametrize(("api", "module", "parser", "fname"), list(_nhl_native_cases()))
def test_nhl_pandas_ids_are_nullable_int64(api, module, parser, fname):
    """R986-1: ``return_as_pandas=True`` carries the same ids as pandas' nullable ``Int64``. Before, the pandas frame
    skipped the cast: ``details_*_player_id`` on the scf g7 pbp were ``float64`` and records ids mixed float / object."""
    import importlib

    fn = getattr(importlib.import_module(f"sportsdataverse.{module}"), parser)
    raw = load_fixture(api, fname.removesuffix(".json"))
    out, pdf_out = fn(raw), fn(raw, return_as_pandas=True)
    frames = out if isinstance(out, dict) else {"": out}
    pdfs = pdf_out if isinstance(pdf_out, dict) else {"": pdf_out}
    for sec, df in frames.items():
        ids = [c for c, t in df.schema.items() if t == pl.Int64 and (c == "id" or c.endswith("_id"))]
        assert {c: str(pdfs[sec][c].dtype) for c in ids} == dict.fromkeys(ids, "Int64"), sec


def test_an_nhl_id_that_is_not_an_integer_stays_as_it_came_and_warns():
    """R986-2: one text id in a real payload keeps that column String (never nulled or forced) and warns naming it, as
    the ESPN parsers do; the known text ids (``formatted_season_id``, the country ``id``) stay silent (warnings are
    errors in this suite, so every other capture proves that)."""
    from sportsdataverse.errors import SchemaDriftWarning
    from sportsdataverse.nhl.nhl_api_web_parsers import parse_nhl_web_pbp

    raw = load_fixture("nhl_api_web", "pbp_2024_scf_g7")
    play = next(p for p in raw["plays"] if "eventOwnerTeamId" in (p.get("details") or {}))
    play["details"]["eventOwnerTeamId"] = "TBD"
    with pytest.warns(SchemaDriftWarning, match=r"NHL api-web parser: id columns \['details_event_owner_team_id'\]"):
        df = parse_nhl_web_pbp(raw)
    assert df.schema["details_event_owner_team_id"] == pl.String
    assert "TBD" in df["details_event_owner_team_id"].to_list()
    assert df.schema["details_player_id"] == pl.Int64


def test_an_edge_id_with_a_missing_value_is_int64_not_float():
    """R986-4: the EDGE captures' ids already arrive Int64, so nothing guarded the EDGE cast. The real team speed top-10
    with one team's ``id`` missing: the pandas hop floats ``team_id`` and the cast brings it back to Int64."""
    from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_top10

    raw = load_fixture("nhl_edge", "team_skating_speed_top_10")
    raw[0]["team"].pop("id")
    df = parse_edge_top10(raw)
    assert df.schema["team_id"] == pl.Int64 and df["team_id"].null_count() == 1
    assert df["team_id"].drop_nulls().to_list() == [r["team"]["id"] for r in raw[1:]]


def test_a_bare_id_with_a_missing_value_is_int64_not_float():
    """R986-4: no committed NHL capture has a bare ``id`` that needs the cast (the ``c == "id"`` half was unguarded).
    The real records vs-franchise capture with one row's ``id`` missing: ``id`` is Int64 with one null, not Float64."""
    from sportsdataverse.nhl.nhl_records_parsers import parse_nhl_records

    raw = load_fixture("nhl_records", "records_all_time_record_vs_franchise")
    raw["data"][0].pop("id")
    df = parse_nhl_records(raw)
    assert df.schema["id"] == pl.Int64 and df["id"].null_count() == 1
    assert df["id"].drop_nulls().to_list() == [r["id"] for r in raw["data"][1:]]


@pytest.mark.parametrize("label", [None, "19711972"], ids=["all_null", "all_digits"])
def test_a_text_by_design_id_is_never_cast_even_when_all_null(label):
    """R986-5 / R989-2: ``text_ids`` silenced the warning but still cast, so a payload whose ``formattedSeasonId`` is
    null on every row, or digits only on every row, flipped ``formatted_season_id`` to Int64 for that call. On the
    real season capture with every label replaced the column stays String; ``id`` (the integer season) is Int64."""
    from sportsdataverse.nhl.nhl_stats_rest_parsers import parse_nhl_stats_rest

    raw = load_fixture("nhl_stats_rest", "stats_rest_season")
    for row in raw["data"]:
        row["formattedSeasonId"] = label
    df = parse_nhl_stats_rest(raw)
    assert df.schema["formatted_season_id"] == pl.String
    assert df["formatted_season_id"].to_list() == [label] * df.height
    assert df.schema["id"] == pl.Int64
