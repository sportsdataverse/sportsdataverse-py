"""`sources.resolve`: tier order, exactly-one-match, and the stem lookups."""

from __future__ import annotations

import textwrap

import pytest

from tools.codegen import sources


@pytest.fixture(autouse=True)
def _clear_registry_cache():
    """Every test reads the registry its own body sets up, never the previous test's temp file.

    ``load()`` is ``lru_cache``d and ``monkeypatch`` restores ``SOURCES_FILE`` but not the cache,
    so without this the real-registry tests would read whichever temp file ran last.
    """
    sources.load.cache_clear()
    yield
    sources.load.cache_clear()


def _registry(tmp_path, body: str):
    p = tmp_path / "sources.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    sources.load.cache_clear()
    return p


def test_load_is_providers_then_categories_in_file_order():
    entries = sources.load()
    kinds = [e.kind for e in entries]
    assert kinds == sorted(kinds, key=lambda k: 0 if k == "provider" else 1)
    assert entries[0].key == "espn"
    assert [e.order for e in entries] == list(range(len(entries)))


def test_resolve_prefers_a_functions_glob_over_a_modules_glob(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              espn:
                label: ESPN
                modules: ["sportsdataverse.nba.*"]
            categories:
              dates:
                label: Dates and seasons
                functions: ["most_recent_*"]
            """,
        ),
    )
    assert sources.resolve("most_recent_nba_season", "sportsdataverse.nba.utils_date").key == "dates"
    assert sources.resolve("espn_nba_pbp", "sportsdataverse.nba.nba_pbp").key == "espn"


def test_resolve_raises_on_two_matching_rules(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              espn:
                label: ESPN
                modules: ["sportsdataverse.nba.*"]
              fox:
                label: Fox Sports
                modules: ["sportsdataverse.*.nba_fox_ext"]
            categories: {}
            """,
        ),
    )
    with pytest.raises(sources.AmbiguousSource) as e:
        sources.resolve("fox_nba_scores", "sportsdataverse.nba.nba_fox_ext")
    assert "espn" in str(e.value) and "fox" in str(e.value)
    assert "fox_nba_scores" in str(e.value)


def test_resolve_raises_on_no_matching_rule(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              espn:
                label: ESPN
                modules: ["sportsdataverse.nba.*"]
            categories: {}
            """,
        ),
    )
    with pytest.raises(sources.UnknownSource) as e:
        sources.resolve("brand_new_thing", "sportsdataverse.brandnew.mod")
    assert "brand_new_thing" in str(e.value)
    assert "sportsdataverse.brandnew.mod" in str(e.value)


def test_resolve_falls_back_to_the_api_stem_then_the_release_base(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              espn:
                label: ESPN
                espn_apis: [espn_site_v2]
              nflverse:
                label: nflverse data releases
                release_bases: [nflverse]
            categories: {}
            """,
        ),
    )
    assert sources.resolve("espn_nfl_scoreboard", "sportsdataverse.nfl.nfl_espn_ext", api="espn_site_v2").key == "espn"
    assert sources.resolve("load_nfl_pbp", "sportsdataverse.nfl.nfl_loaders", base="nflverse").key == "nflverse"


def test_by_api_and_by_base_look_up_the_real_registry():
    assert sources.by_api("espn_core_v2").key == "espn"
    assert sources.by_api("nba_stats").key == "nba_stats"
    assert sources.by_base("nflverse").key == "nflverse"
    assert sources.by_api("not_an_api") is None


def test_providers_and_categories_partition_the_registry():
    assert set(sources.providers()) | set(sources.categories()) == set(sources.load())
    assert not set(sources.providers()) & set(sources.categories())
