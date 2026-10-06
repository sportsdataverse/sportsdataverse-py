"""Index schema shared by the builder (tools/codegen/build_docs_index.py) and the server."""

from __future__ import annotations

SCHEMA_VERSION = 1
ASSET = f"sdv_docs_v{SCHEMA_VERSION}.sqlite"  # the cached / local index (SDV_DOCS_DB)
RELEASE_ASSET = ASSET + ".gz"  # what the release serves; clients decompress it to ASSET
MANIFEST = f"manifest_v{SCHEMA_VERSION}.json"
RELEASE_TAG = "docs-index"
RELEASE_BASE = f"https://github.com/sportsdataverse/sportsdataverse-py/releases/download/{RELEASE_TAG}/"

SCHEMA_SQL = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE functions (
    name TEXT NOT NULL, lang TEXT NOT NULL, package TEXT NOT NULL, module TEXT, league TEXT,
    kind TEXT NOT NULL, category TEXT, summary TEXT, signature TEXT, doc_url TEXT
);
CREATE INDEX functions_name ON functions (name);
CREATE TABLE params (
    function TEXT NOT NULL, name TEXT NOT NULL, type TEXT, required INTEGER NOT NULL,
    default_value TEXT, description TEXT
);
CREATE INDEX params_function ON params (function);
CREATE TABLE columns (function TEXT NOT NULL, section TEXT, name TEXT NOT NULL, type TEXT, description TEXT);
CREATE INDEX columns_function ON columns (function);
CREATE INDEX columns_name ON columns (name);
CREATE TABLE endpoints (
    api TEXT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, summary TEXT, params_json TEXT,
    wrapper TEXT, notes TEXT, source TEXT NOT NULL, spec_url TEXT
);
CREATE TABLE datasets (
    loader TEXT PRIMARY KEY, league TEXT, tag TEXT, url_template TEXT, min_season INTEGER, release_url TEXT
);
CREATE TABLE equivalents (py_function TEXT NOT NULL, r_package TEXT NOT NULL, r_function TEXT NOT NULL, match TEXT NOT NULL);
CREATE VIRTUAL TABLE search USING fts5(
    kind UNINDEXED, name, title, body, url UNINDEXED, league UNINDEXED, lang UNINDEXED, ref UNINDEXED,
    tokenize = 'porter unicode61'
);
"""

# One search row per item. unicode61 treats "_" as a separator, so load_nhl_pbp
# indexes as load / nhl / pbp and a quoted "load_nhl_pbp" still matches as a phrase.
# A Python function's body also carries its return-section names (Shot_Chart_Detail):
# ~480 flat wrappers have only "GET /path" as a summary.
SEARCH_SQL = """
INSERT INTO search (kind, name, title, body, url, league, lang, ref)
SELECT 'function', f.name, f.summary,
       trim(coalesce(f.package, '') || ' ' || coalesce(f.kind, '') || ' ' || coalesce(f.category, '') || ' '
            || coalesce(f.signature, '') || ' '
            || coalesce((SELECT group_concat(p.name || ' ' || coalesce(p.description, ''), ' ')
                         FROM params p WHERE p.function = f.name AND f.lang = 'python'), '') || ' '
            || coalesce((SELECT group_concat(DISTINCT c.section)
                         FROM columns c WHERE c.function = f.name AND f.lang = 'python'), '')),
       f.doc_url, f.league, f.lang, f.rowid
FROM functions f;
INSERT INTO search (kind, name, title, body, url, league, lang, ref)
SELECT 'column', c.name, c.function,
       trim(coalesce(c.section, '') || ' ' || coalesce(c.type, '') || ' ' || coalesce(c.description, '')),
       f.doc_url, f.league, 'python', c.rowid
FROM columns c LEFT JOIN functions f ON f.name = c.function AND f.lang = 'python';
INSERT INTO search (kind, name, title, body, url, league, lang, ref)
SELECT 'endpoint', coalesce(nullif(e.wrapper, ''), e.path), e.summary,
       trim(e.api || ' ' || e.method || ' ' || e.path || ' ' || coalesce(e.params_json, '') || ' ' || coalesce(e.notes, '')),
       e.spec_url, NULL, NULL, e.rowid
FROM endpoints e;
INSERT INTO search (kind, name, title, body, url, league, lang, ref)
SELECT 'dataset', d.loader, d.tag,
       trim(coalesce(d.league, '') || ' ' || coalesce(d.url_template, '') || ' from ' || coalesce(d.min_season, '')),
       d.release_url, d.league, 'python', d.rowid
FROM datasets d;
"""
