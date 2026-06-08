"""Doc-parity: every generated reference page carries the full 8-section contract.

Iterates every league x API the generator knows about and asserts each rendered
reference page has the required headings. Catches a template regression (or a new
API family) that would otherwise ship a half-documented page.
"""

from __future__ import annotations

from tools.codegen import generate

_REQUIRED = ("Endpoint URL", "| API Parameter |", "### Returns", "### Example", "Last validated")


def test_every_reference_page_has_required_sections():
    for prefix in generate._doc_leagues():
        for api in generate._apis_for(prefix):
            md = generate.render_reference_page(prefix, api["name"])
            for needed in _REQUIRED:
                assert needed in md, f"{prefix}/{api['name']} reference page missing {needed!r}"


def test_every_documented_league_has_an_index():
    for prefix in generate._doc_leagues():
        md = generate.render_league_index(prefix)
        assert f"`sportsdataverse.{prefix}`" in md


def test_apis_for_resolves_flat_and_espn_families():
    """NHL exposes both the 3 ESPN APIs and its 4 native flat families."""
    names = {a["name"] for a in generate._apis_for("nhl")}
    assert {"espn_site_v2", "espn_web_v3", "espn_core_v2"} <= names
    assert {"nhl_api_web", "nhl_edge", "nhl_stats_rest", "nhl_records"} <= names
