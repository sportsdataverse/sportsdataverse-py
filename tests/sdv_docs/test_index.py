from pathlib import Path

import pytest

from sdv_docs.index import Index, fts_query


@pytest.mark.parametrize("text", ['"', "-", "*", "NEAR(", "", "  ", "'; DROP TABLE functions; --", "AND OR NOT"])
def test_fts_query_never_emits_bare_syntax(text):
    q = fts_query(text)
    assert q == "" or all(part.startswith('"') and part.endswith('"') for part in q.split(" AND "))


def test_fts_query_quotes_words():
    assert fts_query("nhl pbp") == '"nhl" AND "pbp"'
    assert fts_query("nhl pbp", "OR") == '"nhl" OR "pbp"'


@pytest.mark.parametrize("text", ['"', "-", "*", "NEAR(", "", "'; DROP TABLE functions; --", "AND OR NOT"])
def test_search_survives_hostile_input(tiny_db: Path, text):
    with Index(tiny_db) as ix:
        assert isinstance(ix.search(text), list)


def test_search_ranks_and_filters(tiny_db: Path):
    with Index(tiny_db) as ix:
        hits = ix.search("nhl pbp")
        assert hits[0]["name"] == "load_nhl_pbp"
        assert {h["kind"] for h in ix.search("game id", kind="column")} == {"column"}
        assert all(h["lang"] == "r" for h in ix.search("play by play", lang="r"))


def test_search_falls_back_to_or(tiny_db: Path):
    with Index(tiny_db) as ix:
        assert any(h["name"] == "load_nhl_shifts" for h in ix.search("shift zamboni"))


def test_functions_accepts_name_variants(tiny_db: Path):
    with Index(tiny_db) as ix:
        assert [r["lang"] for r in ix.functions("load_nba_pbp")] == ["python", "r"]
        assert [r["lang"] for r in ix.functions(" load_nhl_pbp() ")] == ["python"]
        assert [r["package"] for r in ix.functions("hoopr::load_nba_pbp")] == ["hoopR"]
        assert [r["lang"] for r in ix.functions("load_nba_pbp", lang="R")] == ["r"]
        assert ix.functions("nope") == []


def test_columns_and_params(tiny_db: Path):
    with Index(tiny_db) as ix:
        assert [c["name"] for c in ix.columns("load_nhl_pbp")] == ["event_type", "period", "game_id"]
        assert [p["name"] for p in ix.params("load_nhl_pbp")] == ["seasons", "return_as_pandas"]
        rows = ix.columns_named("GAME_ID")
        assert {r["function"] for r in rows} == {"load_nhl_pbp", "load_nba_pbp"}
        assert {r["function"] for r in ix.columns_named("game_id", league="nba")} == {"load_nba_pbp"}
        assert len(ix.columns_named("game_id", limit=1)) == 1


def test_endpoints_join_and_api_filter(tiny_db: Path):
    with Index(tiny_db) as ix:
        assert ix.endpoints("athlete injuries")[0]["api"] == "espn_core_v2"
        assert [e["source"] for e in ix.endpoints("odds", api="ESPN Core API")] == ["openapi"]
        assert ix.endpoints("athlete injuries", api="espn_cdn") == []


def test_datasets_and_equivalents(tiny_db: Path):
    with Index(tiny_db) as ix:
        assert [d["loader"] for d in ix.datasets(league="nhl")] == ["load_nhl_pbp", "load_nhl_shifts"]
        assert [d["tag"] for d in ix.datasets(query="shifts")] == ["nhl_shifts"]
        assert ix.dataset_for("load_nhl_shifts")["min_season"] == 2025
        assert ix.dataset_for("nope") is None
        assert [(e["r_package"], e["r_function"]) for e in ix.equivalents("load_nba_pbp")] == [
            ("hoopR", "load_nba_pbp")
        ]


def test_meta_counts_names(tiny_db: Path):
    with Index(tiny_db) as ix:
        assert ix.meta()["sdv_py_commit"] == "abc1234"
        assert ix.counts()["functions"] == 6
        assert "load_nhl_pbp" in ix.names("functions")
