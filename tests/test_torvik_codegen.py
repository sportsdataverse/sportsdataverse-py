"""Offline tests for the generated Bart Torvik wrappers (crosswalk prerequisites).

Parser assertions run against committed real captures (see
``tests/fixtures/torvik/README.md``) — no network. The ``team`` / ``conf``
pair is the minimum-viable surface the basketball crosswalks consume.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv

FIX = Path(__file__).parent / "fixtures" / "torvik"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def test_generated_wrappers_importable():
    from sportsdataverse.mbb import parse_torvik_csv, torvik_ratings, torvik_team_factors
    from sportsdataverse.wbb import bart_wbb_ratings

    assert all(callable(f) for f in (torvik_ratings, torvik_team_factors, bart_wbb_ratings, parse_torvik_csv))


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


def test_parse_ratings_womens_same_shape():
    m = parse_torvik_csv(_read("2025_team_results_head.csv"))
    w = parse_torvik_csv(_read("ncaaw_2025_team_results_head.csv"))
    assert w.columns == m.columns
    assert w["team"][0] == "Connecticut"
    assert w["conf"][0] == "BE"


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
    raw = gen.torvik_ratings(year=2025, return_parsed=False)
    assert raw == csv_text


def test_bart_wbb_wrapper_hits_ncaaw(monkeypatch):
    import sportsdataverse.wbb.bart_wbb as gen

    seen: dict = {}

    def fake_get(url: str, params=None, **kw):
        seen["url"] = url
        return _read("ncaaw_2025_team_results_head.csv")

    monkeypatch.setattr(gen, "_get", fake_get)
    df = gen.bart_wbb_ratings(year=2025)
    assert seen["url"] == "https://barttorvik.com/ncaaw/2025_team_results.csv"
    assert df["team"][0] == "Connecticut"
