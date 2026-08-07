"""Offline tests for the women's Bart Torvik (``/ncaaw`` T-Rank) wrappers.

Parser assertions run against committed real captures (see
``tests/fixtures/torvik/README.md``) — no network. The women's mirror is
byte-compatible with the men's file, so the shared parser must produce the
same column set.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv

FIX = Path(__file__).parents[1] / "fixtures" / "torvik"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def test_parse_ratings_womens_same_shape():
    m = parse_torvik_csv(_read("2025_team_results_head.csv"))
    w = parse_torvik_csv(_read("ncaaw_2025_team_results_head.csv"))
    assert isinstance(w, pl.DataFrame)
    assert w.columns == m.columns
    assert w["team"][0] == "Connecticut"
    assert w["conf"][0] == "BE"


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


def test_raw_response_is_csv_text_not_dict(monkeypatch):
    """``return_parsed=False`` hands back the CSV body as ``str`` (not a JSON dict)."""
    import sportsdataverse.wbb.bart_wbb as gen

    csv_text = _read("ncaaw_2025_team_results_head.csv")
    monkeypatch.setattr(gen, "_get", lambda url, params=None, **kw: csv_text)
    raw = gen.bart_wbb_ratings(year=2025, return_parsed=False)
    assert isinstance(raw, str)
    assert raw == csv_text
