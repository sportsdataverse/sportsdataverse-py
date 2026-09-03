"""Regression tests pinned to REAL KenPom / Her Hoop Stats captures.

Every assertion here encodes a bug that the hand-written fixtures in
``test_subscription_http.py`` could not have caught, and that a live run on
2026-09-02 did. No network and no credentials: the fixtures are trimmed captures
committed under ``tests/fixtures/``.
"""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

from sportsdataverse._html_tables import html_tables
from sportsdataverse.mbb import parse_kenpom_page

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def kenpom_html() -> str:
    return (FIXTURES / "kenpom" / "ratings_2025.trim.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def hhs_roster_html() -> str:
    return (FIXTURES / "herhoopstats" / "team_roster.trim.html").read_text(encoding="utf-8")


def test_kenpom_repeated_thead_does_not_multiply_column_names(kenpom_html):
    # KenPom re-renders its 2-row header every ~40 rows, which pandas reads as a
    # TWENTY-level MultiIndex. Collapsing only ADJACENT duplicate levels left names
    # like "strength_of_schedule_net_rtg" repeated ten times over.
    df = parse_kenpom_page(kenpom_html)["ratings_table"]
    assert "strength_of_schedule_net_rtg" in df.columns
    assert "strength_of_schedule_net_rtg_rk" in df.columns
    assert all(len(c) < 40 for c in df.columns), [c for c in df.columns if len(c) >= 40]
    assert len(set(df.columns)) == len(df.columns)


def test_kenpom_rank_twin_columns_are_named_rk(kenpom_html):
    # pandas suffixes a repeated LAST header level with ".1"; that column is the
    # unlabelled rank twin, not a second metric.
    df = parse_kenpom_page(kenpom_html)["ratings_table"]
    for metric in ("o_rtg", "d_rtg", "adj_t", "luck"):
        assert metric in df.columns
        assert f"{metric}_rk" in df.columns


def test_kenpom_in_table_header_repeats_are_dropped(kenpom_html):
    # The fixture deliberately carries a repeated header row inside <tbody>.
    df = parse_kenpom_page(kenpom_html)["ratings_table"]
    assert df.height > 0
    assert "Rk" not in df["team"].to_list()
    assert df["rk"].null_count() == 0


def test_kenpom_ncaa_seed_is_split_off_the_team_name(kenpom_html):
    # KenPom appends the tournament seed to the label: "Duke 1", not "Duke".
    # Leaving it glued on silently breaks every join on team name.
    df = parse_kenpom_page(kenpom_html)["ratings_table"]
    assert "ncaa_seed" in df.columns
    assert df["ncaa_seed"].dtype == pl.Int64
    assert "Duke" in df["team"].to_list()
    assert not any((t or "").rstrip().endswith(tuple("0123456789")) for t in df["team"].to_list())


def test_kenpom_numeric_columns_are_typed(kenpom_html):
    # KenPom serves everything as text and signs values with a leading "+".
    df = parse_kenpom_page(kenpom_html)["ratings_table"]
    assert df.schema["rk"] == pl.Int64
    assert df.schema["net_rtg"] == pl.Float64
    assert df.schema["team"] == pl.Utf8  # not everything is a number
    assert df.schema["w_l"] == pl.Utf8  # "35-4" must not be coerced


def test_herhoopstats_audio_widget_does_not_contaminate_player_names(hhs_roster_html):
    # Her Hoop Stats embeds a name-pronunciation <audio> in the player cell.
    # pandas.read_html concatenates all descendant text, so the element's fallback
    # copy was being glued onto the name ("Te-Hina Paopao  This HTML5 audio...").
    frames = html_tables(hhs_roster_html, min_rows=1)
    assert frames
    df = max(frames.values(), key=len)
    names = [n for n in df[df.columns[0]].to_list() if n]
    assert names
    assert not any("HTML5" in n or "audio" in n.lower() for n in names), names
