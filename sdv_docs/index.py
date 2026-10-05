"""Locate, verify and query the sdv-docs SQLite index.

Stdlib only. Never import ``sportsdataverse`` here (see sdv_docs/__init__.py).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from sdv_docs.schema import ASSET, MANIFEST, RELEASE_BASE, SCHEMA_VERSION

_WORD = re.compile(r"\w+")
# bm25 weights per search column: kind, name, title, body, url, league, lang, ref
_WEIGHTS = "0, 10.0, 5.0, 1.0, 0, 0, 0, 0"


def fts_query(text: str, op: str = "AND") -> str:
    """Quote every word so user text can never be parsed as FTS5 syntax."""
    return f" {op} ".join(f'"{w}"' for w in _WORD.findall(text))


class Index:
    """Read-only view of one index file."""

    def __init__(self, path: Path) -> None:
        self.con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        self.con.row_factory = sqlite3.Row

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> Index:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def meta(self) -> dict[str, str]:
        return {r["key"]: r["value"] for r in self.con.execute("SELECT key, value FROM meta")}

    def counts(self) -> dict[str, int]:
        tables = ("functions", "params", "columns", "endpoints", "datasets", "equivalents")
        return {t: self.con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}  # noqa: S608 -- fixed names

    def search(
        self,
        query: str,
        kind: Optional[str] = None,
        league: Optional[str] = None,
        lang: Optional[str] = None,
        limit: int = 10,
    ) -> list[sqlite3.Row]:
        where, args = "", []
        for col, val in (("kind", kind), ("league", league), ("lang", lang)):
            if val:
                where += f" AND search.{col} = ?"
                args.append(val.lower() if col == "lang" else val)
        for op in ("AND", "OR"):
            q = fts_query(query, op)
            if not q:
                return []
            rows = self.con.execute(
                "SELECT kind, name, title, url, league, lang, ref FROM search "  # noqa: S608 -- fixed SQL
                f"WHERE search MATCH ?{where} ORDER BY bm25(search, {_WEIGHTS}) LIMIT ?",
                (q, *args, limit),
            ).fetchall()
            if rows:
                return rows
        return []

    def _ranked(self, table: str, kind: str, query: str, where: str, args: tuple, limit: int) -> list[sqlite3.Row]:
        for op in ("AND", "OR"):
            q = fts_query(query, op)
            if not q:
                return []
            rows = self.con.execute(
                f"SELECT t.* FROM search JOIN {table} t ON t.rowid = search.ref "  # noqa: S608 -- fixed table names
                f"WHERE search MATCH ? AND search.kind = ?{where} ORDER BY bm25(search, {_WEIGHTS}) LIMIT ?",
                (q, kind, *args, limit),
            ).fetchall()
            if rows:
                return rows
        return []

    def functions(self, name: str, lang: Optional[str] = None) -> list[sqlite3.Row]:
        pkg, _, fn = name.strip().rpartition("::")
        sql, args = "SELECT * FROM functions WHERE name = ?", [fn.strip().removesuffix("()")]
        if pkg:
            sql += " AND lower(package) = lower(?)"
            args.append(pkg.strip())
        if lang:
            sql += " AND lang = ?"
            args.append(lang.lower())
        return self.con.execute(sql + " ORDER BY lang, package", args).fetchall()

    def params(self, function: str) -> list[sqlite3.Row]:
        return self.con.execute("SELECT * FROM params WHERE function = ? ORDER BY rowid", (function,)).fetchall()

    def columns(self, function: str) -> list[sqlite3.Row]:
        return self.con.execute("SELECT * FROM columns WHERE function = ? ORDER BY rowid", (function,)).fetchall()

    def columns_named(
        self,
        column: str,
        league: Optional[str] = None,
        function: Optional[str] = None,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        sql = (
            "SELECT c.function, c.section, c.name, c.type, c.description, f.league, f.doc_url FROM columns c "
            "LEFT JOIN functions f ON f.name = c.function AND f.lang = 'python' WHERE lower(c.name) = lower(?)"
        )
        args: list[object] = [column.strip()]
        if league:
            sql += " AND f.league = ?"
            args.append(league)
        if function:
            sql += " AND c.function = ?"
            args.append(function.strip().removesuffix("()"))
        return self.con.execute(sql + " ORDER BY c.function LIMIT ?", (*args, limit)).fetchall()

    def equivalents(self, name: str) -> list[sqlite3.Row]:
        return self.con.execute(
            "SELECT * FROM equivalents WHERE py_function = ? OR r_function = ? ORDER BY r_package", (name, name)
        ).fetchall()

    def endpoints(self, query: str, api: Optional[str] = None, limit: int = 10) -> list[sqlite3.Row]:
        where, args = (" AND t.api = ?", (api,)) if api else ("", ())
        return self._ranked("endpoints", "endpoint", query, where, args, limit)

    def datasets(self, league: Optional[str] = None, query: Optional[str] = None, limit: int = 50) -> list[sqlite3.Row]:
        args: tuple = (league,) if league else ()
        if query is not None:  # an empty or punctuation-only query matches nothing, not everything
            return self._ranked("datasets", "dataset", query, " AND t.league = ?" if league else "", args, limit)
        sql = "SELECT * FROM datasets" + (" WHERE league = ?" if league else "") + " ORDER BY loader LIMIT ?"
        return self.con.execute(sql, (*args, limit)).fetchall()

    def dataset_for(self, loader: str) -> Optional[sqlite3.Row]:
        row: Optional[sqlite3.Row] = self.con.execute("SELECT * FROM datasets WHERE loader = ?", (loader,)).fetchone()
        return row

    def names(self, table: str, column: str = "name") -> list[str]:
        return [r[0] for r in self.con.execute(f"SELECT DISTINCT {column} FROM {table}")]  # noqa: S608 -- fixed identifiers


Fetch = Callable[[str, float], bytes]
CHECK_EVERY_S = 24 * 3600


class IndexUnavailable(RuntimeError):
    """No usable index: not cached, not downloadable, or SDV_DOCS_DB points nowhere."""


def cache_dir() -> Path:
    # ponytail: duplicates sportsdataverse.cache._cache_dir(); importing it would load every league.
    root = os.environ.get("SDV_PY_CACHE_DIR")
    return (Path(root) if root else Path.home() / ".cache" / "sportsdataverse") / "docs-index"


def http_fetch(url: str, timeout: float) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "sdv-docs"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 -- fixed https GitHub URLs
        data: bytes = resp.read()
    return data


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify(path: Path, manifest: dict) -> None:
    """Raise IndexUnavailable unless ``path`` matches the manifest and is a sound index of our schema."""
    if path.stat().st_size != manifest["size"]:
        raise IndexUnavailable("downloaded index has the wrong size")
    if sha256(path) != manifest["sha256"]:
        raise IndexUnavailable("downloaded index failed its sha256 check")
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            ok = con.execute("PRAGMA integrity_check").fetchone()[0]
            row = con.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
        finally:
            con.close()
    except sqlite3.DatabaseError as e:
        raise IndexUnavailable(f"downloaded index is not a SQLite file ({e})") from e
    if ok != "ok":
        raise IndexUnavailable(f"downloaded index failed integrity_check: {ok}")
    if row is None or str(row[0]) != str(SCHEMA_VERSION):
        raise IndexUnavailable(f"downloaded index has schema {row[0] if row else None}, expected {SCHEMA_VERSION}")


def refresh(db: Path, fetch: Fetch = http_fetch, timeout: float = 30.0) -> bool:
    """Install the published index at ``db`` if it differs from the local one. True when replaced."""
    manifest = json.loads(fetch(RELEASE_BASE + MANIFEST, timeout))
    db.parent.mkdir(parents=True, exist_ok=True)
    stamp = db.with_name("last_check")
    if db.is_file() and sha256(db) == manifest["sha256"]:
        stamp.touch()
        return False
    tmp = db.with_name(db.name + ".tmp")
    try:
        tmp.write_bytes(fetch(RELEASE_BASE + ASSET, timeout))
        verify(tmp, manifest)
        os.replace(tmp, db)
    finally:
        tmp.unlink(missing_ok=True)
    stamp.touch()
    return True


def _refresh_quietly(db: Path, fetch: Fetch) -> None:
    try:
        refresh(db, fetch)
    except Exception:  # noqa: BLE001 -- a refresh must never take the server down; the cached index keeps serving.
        return


def locate(fetch: Fetch = http_fetch, background: bool = True) -> Path:
    """The index to use: SDV_DOCS_DB, else the cache (re-checked at most daily), else a fresh download."""
    env = os.environ.get("SDV_DOCS_DB")
    if env:
        path = Path(env)
        if not path.is_file():
            raise IndexUnavailable(f"SDV_DOCS_DB={env} is not a file")
        return path
    db = cache_dir() / ASSET
    if db.is_file():
        stamp = db.with_name("last_check")
        if not stamp.exists() or time.time() - stamp.stat().st_mtime > CHECK_EVERY_S:
            stamp.touch()  # one attempt per day, even when it fails
            if background:
                threading.Thread(target=_refresh_quietly, args=(db, fetch), daemon=True).start()
            else:
                _refresh_quietly(db, fetch)
        return db
    try:
        refresh(db, fetch, timeout=10.0)
    except IndexUnavailable:
        raise
    except Exception as e:  # noqa: BLE001 -- network and JSON errors both mean "no index yet"
        raise IndexUnavailable(f"could not download the index ({e.__class__.__name__}: {e})") from e
    return db
