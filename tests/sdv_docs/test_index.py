import hashlib
import json
import os
import time
from pathlib import Path

import pytest

from sdv_docs import index as idx
from sdv_docs.index import Index, fts_query
from sdv_docs.schema import ASSET, MANIFEST, RELEASE_BASE
from tests.sdv_docs.conftest import make_tiny_db


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


def _published(tmp_path: Path, **meta: str) -> tuple[Path, dict]:
    db = make_tiny_db(tmp_path / "published.sqlite", **meta)
    return db, {"schema_version": 1, "sha256": idx.sha256(db), "size": db.stat().st_size}


def _fetcher(files: dict, calls: list):
    def fetch(url: str, timeout: float) -> bytes:
        calls.append(url)
        if url not in files:
            raise OSError(f"offline: {url}")
        return files[url]

    return fetch


def _serve(pub: Path, man: dict, asset_bytes: bytes = b"") -> dict:
    return {RELEASE_BASE + MANIFEST: json.dumps(man).encode(), RELEASE_BASE + ASSET: asset_bytes or pub.read_bytes()}


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setenv("SDV_PY_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.delenv("SDV_DOCS_DB", raising=False)
    return tmp_path / "cache" / "docs-index"


def test_env_override_is_used_and_never_fetches(tiny_db, monkeypatch):
    monkeypatch.setenv("SDV_DOCS_DB", str(tiny_db))
    calls: list = []
    assert idx.locate(_fetcher({}, calls)) == tiny_db
    assert calls == []


def test_env_override_missing_file_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("SDV_DOCS_DB", str(tmp_path / "nope.sqlite"))
    with pytest.raises(idx.IndexUnavailable, match="SDV_DOCS_DB"):
        idx.locate(_fetcher({}, []))


def test_first_use_downloads_verifies_and_installs(tmp_path, cache):
    pub, man = _published(tmp_path)
    path = idx.locate(_fetcher(_serve(pub, man), []), background=False)
    assert path == cache / ASSET
    assert idx.sha256(path) == man["sha256"]
    assert (cache / "last_check").exists()


def test_first_use_without_network_is_a_clear_error(cache):
    with pytest.raises(idx.IndexUnavailable, match="could not download"):
        idx.locate(_fetcher({}, []), background=False)


@pytest.mark.parametrize(
    ("tamper", "match"),
    [
        ("truncate", "size"),
        ("sha", "sha256"),
        ("schema", "schema"),
        ("garbage", "not a SQLite file"),
    ],
)
def test_bad_downloads_are_rejected_and_old_index_kept(tmp_path, cache, tamper, match):
    cache.mkdir(parents=True)
    old = make_tiny_db(cache / ASSET)
    old_sha = idx.sha256(old)
    pub, man = _published(
        tmp_path, built_at="2026-10-06T00:00:00Z", **({"schema_version": "999"} if tamper == "schema" else {})
    )
    if tamper == "schema":
        man = {**man, "sha256": idx.sha256(pub), "size": pub.stat().st_size}
    blob = pub.read_bytes()
    if tamper == "truncate":
        blob = blob[:1000]
    if tamper == "sha":
        man = {**man, "sha256": "0" * 64}
    if tamper == "garbage":
        blob = b"x" * 4096
        man = {**man, "sha256": hashlib.sha256(blob).hexdigest(), "size": len(blob)}
    with pytest.raises(idx.IndexUnavailable, match=match):
        idx.refresh(cache / ASSET, _fetcher(_serve(pub, man, blob), []))
    assert idx.sha256(cache / ASSET) == old_sha
    assert not (cache / (ASSET + ".tmp")).exists()


def test_unchanged_manifest_only_touches_the_stamp(tmp_path, cache):
    pub, man = _published(tmp_path)
    cache.mkdir(parents=True)
    (cache / ASSET).write_bytes(pub.read_bytes())
    calls: list = []
    assert idx.refresh(cache / ASSET, _fetcher(_serve(pub, man), calls)) is False
    assert calls == [RELEASE_BASE + MANIFEST]


def test_fresh_stamp_skips_the_network(tmp_path, cache):
    cache.mkdir(parents=True)
    make_tiny_db(cache / ASSET)
    (cache / "last_check").touch()
    calls: list = []
    assert idx.locate(_fetcher({}, calls), background=False) == cache / ASSET
    assert calls == []


def test_stale_stamp_installs_a_newer_index(tmp_path, cache):
    cache.mkdir(parents=True)
    make_tiny_db(cache / ASSET)
    (cache / "last_check").touch()
    os.utime(cache / "last_check", (time.time() - 2 * 86400,) * 2)
    pub, man = _published(tmp_path, built_at="2026-10-06T00:00:00Z")
    idx.locate(_fetcher(_serve(pub, man), []), background=False)
    assert idx.sha256(cache / ASSET) == man["sha256"]


def test_failed_replace_keeps_the_old_index(tmp_path, cache, monkeypatch):
    cache.mkdir(parents=True)
    old_sha = idx.sha256(make_tiny_db(cache / ASSET))
    pub, man = _published(tmp_path, built_at="2026-10-06T00:00:00Z")

    def locked(src, dst):
        raise PermissionError("file in use")  # what Windows does while the old index is open

    monkeypatch.setattr(idx.os, "replace", locked)
    assert idx.locate(_fetcher(_serve(pub, man), []), background=False) == cache / ASSET
    assert idx.sha256(cache / ASSET) == old_sha
    assert not (cache / (ASSET + ".tmp")).exists()
