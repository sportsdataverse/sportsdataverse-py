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
