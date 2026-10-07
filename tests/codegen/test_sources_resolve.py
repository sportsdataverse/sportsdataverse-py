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
    """Two rules that pin down equally much is a registry bug, not a coin to flip."""
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
                modules: ["sportsdataverse.nba.*"]
            categories: {}
            """,
        ),
    )
    with pytest.raises(sources.AmbiguousSource) as e:
        sources.resolve("fox_nba_scores", "sportsdataverse.nba.nba_fox_ext")
    assert "espn" in str(e.value) and "fox" in str(e.value)
    assert "fox_nba_scores" in str(e.value)


def test_the_more_specific_glob_wins(tmp_path, monkeypatch):
    """A rule naming the exact module beats a rule that only names its package."""
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
    assert sources.resolve("fox_nba_scores", "sportsdataverse.nba.nba_fox_ext").key == "fox"
    assert sources.resolve("espn_nba_pbp", "sportsdataverse.nba.nba_pbp").key == "espn"


def test_module_rules_rank_providers_and_categories_together(tmp_path, monkeypatch):
    """A model built on a provider's data is a model when the registry says so by name; kind
    alone decides nothing, the more specific rule does."""
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              ncaa_stats:
                label: stats.ncaa.org
                modules: ["sportsdataverse.mbb.mbb_ncaa_*"]
            categories:
              models:
                label: Models and calculators
                modules: ["sportsdataverse.mbb.mbb_ncaa_models", "sportsdataverse.*.*_projection"]
            """,
        ),
    )
    assert sources.resolve("mbb_ncaa_fit", "sportsdataverse.mbb.mbb_ncaa_models").key == "models"
    assert sources.resolve("mbb_ncaa_box", "sportsdataverse.mbb.mbb_ncaa_box_stats").key == "ncaa_stats"
    assert sources.resolve("mbb_ncaa_proj", "sportsdataverse.mbb.mbb_ncaa_projection").key == "ncaa_stats"


def test_one_module_under_a_provider_and_a_category_is_ambiguous(tmp_path, monkeypatch):
    """nhl_edge_value was once listed under both NHL EDGE and Analytics; a tier order settled it
    silently. Two entries claiming the same module is a registry contradiction -- fail on it."""
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              nhl_edge:
                label: NHL EDGE
                modules: ["sportsdataverse.nhl.nhl_edge_value"]
            categories:
              analytics:
                label: Analytics
                modules: ["sportsdataverse.nhl.nhl_edge_value"]
            """,
        ),
    )
    with pytest.raises(sources.AmbiguousSource):
        sources.resolve("nhl_edge_skating_value", "sportsdataverse.nhl.nhl_edge_value")


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


def test_resolve_uses_the_api_stem_or_the_release_base(tmp_path, monkeypatch):
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


def test_a_generated_origin_beats_every_glob(tmp_path, monkeypatch):
    """A generated wrapper's source is the API it was generated from -- no name or module glob
    can move it. A college_baseball ESPN wrapper lives in a module an NCAA glob also matches."""
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
              ncaa_stats:
                label: stats.ncaa.org
                modules: ["sportsdataverse.baseball.*"]
              sdv_releases:
                label: SportsDataverse data releases
                release_bases: [sdv]
            categories:
              ids:
                label: IDs and crosswalks
                functions: ["*_team_ids"]
            """,
        ),
    )
    mod = "sportsdataverse.baseball.college_baseball_espn_ext"
    assert sources.resolve("espn_college_baseball_seasons", mod, api="espn_site_v2").key == "espn"
    assert (
        sources.resolve("load_ncaa_mbb_team_ids", "sportsdataverse.mbb.mbb_loaders", base="sdv").key == "sdv_releases"
    )


def test_a_generated_family_no_provider_owns_fails_instead_of_falling_through(tmp_path, monkeypatch):
    """Polymarket and Kalshi wrappers (stems with no provider yet) once resolved to The Odds API
    through its `sportsdataverse.odds.*` glob. A generated name's API is authoritative, so an
    unowned API is a registry gap, never a cue to guess from the module."""
    monkeypatch.setattr(
        sources,
        "SOURCES_FILE",
        _registry(
            tmp_path,
            """
            providers:
              odds_api:
                label: The Odds API
                modules: ["sportsdataverse.odds.*"]
            categories: {}
            """,
        ),
    )
    with pytest.raises(sources.UnknownSource) as e:
        sources.resolve("polymarket_clob_book", "sportsdataverse.odds.polymarket", api="polymarket")
    assert "polymarket" in str(e.value)
    with pytest.raises(sources.UnknownSource):
        sources.resolve("load_new_thing", "sportsdataverse.odds.odds_loaders", base="brand_new_base")


def test_by_api_and_by_base_look_up_the_real_registry():
    assert sources.by_api("espn_core_v2").key == "espn"
    assert sources.by_api("nba_stats").key == "nba_stats"
    assert sources.by_base("nflverse").key == "nflverse"
    assert sources.by_api("not_an_api") is None


def test_providers_and_categories_partition_the_registry():
    assert set(sources.providers()) | set(sources.categories()) == set(sources.load())
    assert not set(sources.providers()) & set(sources.categories())
