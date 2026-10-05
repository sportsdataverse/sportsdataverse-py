"""Offline tests for the men's Bart Torvik (T-Rank) wrappers (crosswalk prerequisites).

Parser assertions run against committed real captures (see
``tests/fixtures/torvik/README.md``) — no network. The ``team`` / ``conf``
pair is the minimum-viable surface the MBB crosswalk consumes.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv

FIX = Path(__file__).parents[1] / "fixtures" / "torvik"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def test_parse_ratings_mens():
    df = parse_torvik_csv(_read("2025_team_results_head.csv"))
    assert isinstance(df, pl.DataFrame)
    assert df.width == 45
    # the crosswalk-consumed pair
    assert df.schema["team"] == pl.Utf8
    assert df.schema["conf"] == pl.Utf8
    assert df["team"][0] == "Houston"
    assert df["conf"][0] == "B12"
    # duplicate source headers are de-duplicated, not clobbered
    assert "rank" in df.columns and "rank_2" in df.columns
    assert df.columns == sorted(set(df.columns), key=df.columns.index)


def test_parse_team_factors():
    df = parse_torvik_csv(_read("2025_fffinal_head.csv"))
    assert df.width == 41
    assert "team_name" in df.columns
    # janitor-style cleaning: % -> _percent, leading digit -> x prefix
    assert "e_fg_percent" in df.columns
    assert "x3p_percent" in df.columns
    # 18 per-stat rank columns survive de-duplication
    assert sum(c == "rk" or c.startswith("rk_") for c in df.columns) == 18


def test_parse_empty_payload_returns_zero_rows():
    for payload in ("", {}, None, "justoneline"):
        df = parse_torvik_csv(payload)
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0


def test_parse_malformed_csv_returns_zero_rows():
    # unterminated quoted field: polars raises ComputeError on read_csv, and the
    # parser's documented contract is a zero-row frame, not a raised exception.
    df = parse_torvik_csv('team,conf\nHouston,"B12\n')
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


def test_parse_html_outage_page_raises_rather_than_pretending_to_be_data():
    """An HTML body must not be read as a one-column CSV named after the DOCTYPE.

    barttorvik.com answers a transient outage with an HTML page and HTTP 200.
    Parsing it produced a frame whose only columns were the snake-cased
    DOCTYPE, so the failure surfaced far away as
    ``ColumnNotFoundError: unable to find column "team"`` inside
    ``wbb_team_crosswalk`` -- which is how the nightly wehoop-wbb-data build
    broke on 2026-08-24.
    """
    html = (
        '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"'
        ' "http://www.w3.org/TR/html4/loose.dtd">\n'
        "<html><body>Service Unavailable</body></html>\n"
    )
    with pytest.raises(ValueError, match="HTML document"):
        parse_torvik_csv(html)


def test_parse_leading_whitespace_html_also_raises():
    """The guard looks past leading whitespace, as a real body may have some."""
    with pytest.raises(ValueError, match="HTML document"):
        parse_torvik_csv("\n  <!DOCTYPE html>\n<html><body>nope</body></html>\n")


def test_parse_html_detection_does_not_swallow_a_csv_starting_with_a_bracket():
    """Only document markers count as HTML -- a bare "<" is not one.

    A CSV whose first header cell is ``<team>`` is still a CSV, and a lone
    ``<`` belongs on the documented zero-row path, not the raising one.
    """
    df = parse_torvik_csv("<team>,conf\nHouston,B12\n")
    assert df.columns == ["team", "conf"]
    assert df.height == 1
    assert parse_torvik_csv("<").height == 0


def test_parse_html_detection_sees_past_a_utf8_bom():
    """A BOM ahead of the DOCTYPE must not hide the markup."""
    with pytest.raises(ValueError, match="HTML document"):
        parse_torvik_csv("\ufeff<!DOCTYPE html>\n<html><body>x</body></html>\n")


def test_parse_xml_error_document_also_raises():
    with pytest.raises(ValueError, match="HTML document"):
        parse_torvik_csv('<?xml version="1.0"?>\n<error/>\n')


def test_wrapper_routes_through_parser(monkeypatch):
    import sportsdataverse.mbb.torvik as gen

    csv_text = _read("2025_team_results_head.csv")
    seen: dict = {}

    def fake_get(url: str, params=None, **kw):
        seen["url"] = url
        return csv_text

    monkeypatch.setattr(gen, "_get", fake_get)
    df = gen.torvik_ratings(year=2025)
    assert seen["url"] == "https://barttorvik.com/2025_team_results.csv"
    assert df["team"][0] == "Houston"


def test_raw_response_is_csv_text_not_dict(monkeypatch):
    """``return_parsed=False`` hands back the CSV body as ``str`` (not a JSON dict)."""
    import sportsdataverse.mbb.torvik as gen

    csv_text = _read("2025_team_results_head.csv")
    monkeypatch.setattr(gen, "_get", lambda url, params=None, **kw: csv_text)
    for fn in (gen.torvik_ratings, gen.torvik_team_factors):
        raw = fn(year=2025, return_parsed=False)
        assert isinstance(raw, str)
        assert raw == csv_text


# --- game_stats / player_stats / game_schedule (headerless positional files) ---


def test_parse_game_stats_real_capture():
    from sportsdataverse.mbb.torvik_parsers import GAME_STATS_COLS, parse_torvik_game_stats

    df = parse_torvik_game_stats(_read("2025_getgamestats_head.json"))
    assert df.columns == [*GAME_STATS_COLS, "game_date"]
    assert df.height == 17  # 15 head rows + the 2 rows with a null margin
    assert df["team"][0] == "Abilene Christian" and df["opp"][0] == "Baylor"
    assert df.schema["game_date"] == pl.Date and df["game_date"][0].isoformat() == "2024-12-09"
    assert df.schema["adj_oe"] == pl.Float64 and df.schema["year"] == pl.Int64
    assert df["game_date"].null_count() == 0
    # Torvik ships `game_stats` as a JSON string -- passed through verbatim
    assert df["game_stats"][0].startswith('["12/9/24"')
    # null margins in the capture survive as nulls, not 0
    assert df["margin"].null_count() == 2
    # both sides of one game: rows 0/1 are Abilene Christian vs Baylor on 12/9/24
    assert df["muid"][0] == df["muid"][1] == "Abilene ChristianBaylor12-9"
    assert df["team"][0] == df["opp"][1] and df["team"][1] == df["opp"][0]
    assert df["team"][0] != df["team"][1]


def test_parse_player_stats_real_capture():
    from sportsdataverse.mbb.torvik_parsers import PLAYER_STATS_COLS, parse_torvik_player_stats

    df = parse_torvik_player_stats(_read("2025_getadvstats_head.csv"))
    assert df.columns == PLAYER_STATS_COLS
    assert df.height == 15
    assert df["player_name"][0] == "Robby Carmody" and df["team"][0] == "Le Moyne"
    assert df.schema["player_id"] == pl.Int64 and df["player_id"][0] == 65443
    assert df["height"][0] == "6-4" and df["class"][0] == "Sr"
    assert df.schema["bpm"] == pl.Float64
    assert df["year"].unique().to_list() == [2025]


def test_parse_game_schedule_real_capture():
    from sportsdataverse._crosswalk_basketball_sources import SUPER_SKED_FIELDS
    from sportsdataverse.mbb.torvik_parsers import parse_torvik_game_schedule

    df = parse_torvik_game_schedule(_read("2025_super_sked_head.json"))
    assert df.columns == [*SUPER_SKED_FIELDS, "game_date", "year"]
    assert df.height == 15
    assert df["team1"][0] == "Northeastern St." and df["team2"][0] == "Tulsa"
    assert df["game_date"][0].isoformat() == "2024-11-04"
    # November 2024 belongs to the 2024-25 season (year = end year)
    assert df["year"].unique().to_list() == [2025]


def test_private_super_sked_helper_still_stamps_explicit_year():
    from sportsdataverse._crosswalk_basketball_sources import parse_super_sked

    df = parse_super_sked(_read("2025_super_sked_head.json"), 2025)
    assert df["year"].unique().to_list() == [2025] and df.height == 15


def test_headerless_parsers_empty_and_html():
    from sportsdataverse.mbb.torvik_parsers import (
        parse_torvik_game_schedule,
        parse_torvik_game_stats,
        parse_torvik_player_stats,
    )

    for fn in (parse_torvik_game_stats, parse_torvik_player_stats, parse_torvik_game_schedule):
        for payload in ("", None, {}, "[]" if fn is not parse_torvik_player_stats else ""):
            df = fn(payload)
            assert df.height == 0 and df.width > 30, (fn.__name__, payload)
        with pytest.raises(ValueError, match="HTML document"):
            fn("<!DOCTYPE html><html><body>down</body></html>")


def test_wrappers_hit_expected_urls(monkeypatch):
    import sportsdataverse.mbb.torvik as gen

    seen: list = []

    def fake_get(url, params=None, **kw):
        seen.append((url, params))
        return "[]" if url.endswith(".json") or "gamestats" in url else ""

    monkeypatch.setattr(gen, "_get", fake_get)
    gen.torvik_game_stats(year=2025)
    gen.torvik_player_stats(year=2025)
    gen.torvik_game_schedule(year=2025)
    assert seen[0][0] == "https://barttorvik.com/getgamestats.php"
    assert seen[0][1] == {"year": 2025, "json": 1}
    assert seen[1][0] == "https://barttorvik.com/getadvstats.php"
    assert seen[1][1] == {"year": 2025, "csv": 1}
    assert seen[2][0] == "https://barttorvik.com/2025_super_sked.json"


def test_game_schedule_year_inference_and_unparseable_date_rows():
    """year is inferred from the date (Jul+ -> next year, Jun -> same year, null -> null);
    a row with an unparseable date is KEPT (bart_super_sked drops it)."""
    import json

    from sportsdataverse.mbb.torvik_parsers import parse_torvik_game_schedule

    base = json.loads(_read("2025_super_sked_head.json"))[0]
    rows = []
    for d in ("7/1/24", "6/30/25", "11/4/24", "not-a-date"):
        r = list(base)
        r[1] = d
        rows.append(r)
    df = parse_torvik_game_schedule(json.dumps(rows))
    assert df.height == 4  # unparseable-date row kept
    assert df["year"].to_list() == [2025, 2025, 2025, None]
    assert df["game_date"].to_list()[3] is None
    assert df["game_date"].null_count() == 1
