"""Football-Data.co.uk parser (CSV bodies) + generated-YAML contract."""

import pathlib

import pandas as pd
import polars as pl
import yaml

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "football_data"
YAML_PATH = pathlib.Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "football_data.yaml"


def load(name):
    return (FIXTURES / f"{name}.csv").read_text(encoding="utf-8-sig")


def test_parser_reads_csv_text():
    """Review Focus 2: the body is text/csv -- a JSON parser would see nothing."""
    from sportsdataverse.soccer.football_data_parsers import parse_football_data

    df = parse_football_data(load("league_season"))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "div" in df.columns
    assert "home_team" in df.columns


def test_every_route_fixture_parses():
    from sportsdataverse.soccer.football_data_parsers import parse_football_data

    for name in ("league_season", "extra_league", "fixtures"):
        df = parse_football_data(load(name))
        assert df.height > 0, name
        assert all(c and not c.startswith("unnamed") for c in df.columns), name


def test_unnamed_trailing_columns_are_dropped():
    """Trailing commas in the source files create empty header cells."""
    from sportsdataverse.soccer.football_data_parsers import parse_football_data

    df = parse_football_data("Div,Date,HomeTeam,,\nE0,16/08/2025,Liverpool,,\n")
    assert df.columns == ["div", "date", "home_team"]


def test_empty_and_malformed_payloads_are_zero_rows():
    from sportsdataverse.soccer.football_data_parsers import parse_football_data

    for payload in (None, "", "   ", {}, 7, "Div,Date\n"):
        df = parse_football_data(payload)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


def test_pandas_round_trip():
    from sportsdataverse.soccer.football_data_parsers import parse_football_data

    assert isinstance(parse_football_data(load("fixtures"), return_as_pandas=True), pd.DataFrame)


def test_yaml_points_at_the_text_runtime():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert doc["getter_module"] == "sportsdataverse.soccer.football_data_runtime"
    assert len(doc["endpoints"]) == 3
    assert {e["short"] for e in doc["endpoints"]} == {"league_season", "extra_league", "fixtures"}


def test_notes_txt_is_not_wrapped():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert all(not e["path"].endswith(".txt") for e in doc["endpoints"])
