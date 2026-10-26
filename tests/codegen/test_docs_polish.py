"""Render-time polish of the generated docs: curated titles, polars dtypes, readable summaries, no internal text."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.codegen import generate, spec

ROOT = Path(__file__).resolve().parents[2]


def test_every_page_title_is_the_registry_label():
    """A league page's title, H1 and _category_.json read the same label as the sidebar and home grid."""
    registry = json.loads(generate.render_leagues_json())
    labels = {lg["prefix"]: lg["label"] for s in registry["sports"] for lg in s["leagues"]}
    assert {p: generate._doc_display_name(p) for p in labels} == labels
    assert generate._doc_display_name("nbasl_lv") == "NBA Summer League (Las Vegas)"
    assert generate._doc_display_name("mbb") == "Men's college basketball"
    assert generate._doc_display_name("nfl") == "NFL"
    index = (generate.DOCS / "thesportsdb" / "index.md").read_text(encoding="utf-8")
    assert "\ntitle: TheSportsDB" in index or '\ntitle: "TheSportsDB' in index


@pytest.mark.parametrize(
    ("stored", "shown"),
    [
        ("character", "String"),
        ("integer", "Int64"),
        ("numeric", "Float64"),
        ("double", "Float64"),
        ("logical", "Boolean"),
        ("list", "List"),
        ("Utf8", "String"),
        ("integer / character", "Int64 / String"),
        ("Int32", "Int32"),
        ("Datetime(time_unit='us', time_zone='UTC')", "Datetime(time_unit='us', time_zone='UTC')"),
        ("", ""),
    ],
)
def test_returns_types_render_as_polars_dtypes(stored, shown):
    assert generate._doc_dtype(stored) == shown


def test_no_committed_returns_table_shows_an_r_type():
    """The mapping runs at render time, so the committed pages carry polars names while schemas/** keep R's."""
    r_names = {"character", "integer", "numeric", "double", "logical"}
    for page in generate.DOCS.rglob("*.md"):
        intab = False
        for line in page.read_text(encoding="utf-8").splitlines():
            if line.startswith("| col_name | type |"):
                intab = True
            elif not line.startswith("|"):
                intab = False
            elif intab and line.startswith("| `"):
                assert line.split(" | ")[1].strip() not in r_names, f"{page}: {line[:80]}"


def test_a_path_only_summary_reads_as_the_humanized_short_name():
    ep = spec.Endpoint(
        short="boxscoreadvancedv3", path="/stats/boxscoreadvancedv3", summary="GET /stats/boxscoreadvancedv3"
    )
    assert generate._ep_summary(ep) == "Box score advanced v3."
    assert generate._humanize_slug("leaguedashplayerptshot") == "League dash player PT shot"
    assert generate._humanize_slug("coaches_history") == "Coaches history"
    assert generate._humanize_slug("zzqx") == "Zzqx"  # no split -> the slug itself, never a guess
    ep = spec.Endpoint(short="scoreboard", path="/x", summary="Daily scoreboard.")
    assert generate._ep_summary(ep) == "Daily scoreboard."


@pytest.mark.parametrize(
    "text",
    [
        "# ponytail: a shortcut",
        "see sdv-internal-refs/acb/README.md",
        "see `dev/phase5f-live-proof.md`",
        "built in Task 5e.6",
        "nothing in Phase 5e exercises it",
        "offline (C-L3): with",
        "(`PossessionUtils.scala:39-49`)",
        "``LineupUtils.scala\n    :1119-1144``",
        "C:/Users/someone/file",
    ],
)
def test_the_leak_gate_names_each_internal_pattern(text):
    assert generate._internal_leaks({"x.md": f"intro\n{text}\n"})


@pytest.mark.parametrize(
    "text",
    [
        "fitted by `tools/models/nba_prediction/fit_pregame.py`",  # a tracked fitting script: provenance, allowed
        "**Phase 1 -- shrink the candidate pool**",  # an algorithm's phase, not a plan id
        "`phase1_shot_event_enrichment`",
        "a USA U-18 roster, game RS-002, season S-1",
        "ported from `ExtractorUtils.scala`",
    ],
)
def test_the_leak_gate_allows_public_text(text):
    assert generate._internal_leaks({"x.md": text}) == []


def test_committed_docs_carry_no_internal_text():
    assert generate._internal_leaks() == []


def test_discover_league_table_is_current():
    """discover.py's generated span matches the registry, so function_count() can never skip a league."""
    path = ROOT / "sportsdataverse" / "discover.py"
    text = path.read_text(encoding="utf-8")
    assert (
        generate._patch_between_markers(text, *generate._DISCOVER_MARKERS, generate.render_discover_leagues()) == text
    )
    assert '"del": "hockey.del_",' in text
