"""ESPN content.core v1 parser + generated-YAML contract."""

import json
import pathlib

import pandas as pd
import polars as pl
import yaml

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "espn_content"
YAML_PATH = pathlib.Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "espn_content.yaml"


def load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def test_headlines_become_rows():
    from sportsdataverse.espn_content.espn_content_parsers import parse_espn_content

    df = parse_espn_content(load("news"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == len(load("news")["headlines"])
    assert "id" in df.columns


def test_one_story_is_one_row():
    from sportsdataverse.espn_content.espn_content_parsers import parse_espn_content

    assert parse_espn_content(load("story")).height == 1


def test_empty_payload_is_zero_rows():
    from sportsdataverse.espn_content.espn_content_parsers import parse_espn_content

    for payload in (None, {}, [], {"headlines": []}, 7):
        df = parse_espn_content(payload)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


def test_pandas_round_trip():
    from sportsdataverse.espn_content.espn_content_parsers import parse_espn_content

    assert isinstance(parse_espn_content(load("news"), return_as_pandas=True), pd.DataFrame)


def test_id_columns_are_utf8():
    from sportsdataverse.espn_content.espn_content_parsers import parse_espn_content

    df = parse_espn_content(load("news"))
    for name, dtype in df.schema.items():
        if name == "id" or name.endswith("_id"):
            assert dtype == pl.String, name


def test_no_path_token_named_league_or_sport():
    """Review Focus 1: the module renderer substitutes {league}/{sport} as ESPN slugs."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for ep in doc["endpoints"]:
        assert "{league}" not in ep["path"], ep["short"]
        assert "{sport}" not in ep["path"], ep["short"]
        for prm in ep.get("path_params", []):
            assert prm["name"] not in {"league", "sport"}, ep["short"]


def test_league_news_path_uses_slug_params():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    ep = next(e for e in doc["endpoints"] if e["short"] == "league_news")
    assert ep["path"] == "/sports/{sport_slug}/{league_slug}/news"
    assert [p["name"] for p in ep["path_params"]] == ["sport_slug", "league_slug"]


def test_paging_params_are_caller_supplied():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    ep = next(e for e in doc["endpoints"] if e["short"] == "news")
    assert {p["name"] for p in ep["extra_params"]} == {"limit", "offset"}
