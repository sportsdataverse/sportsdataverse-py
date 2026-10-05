"""Locate, verify and query the sdv-docs SQLite index.

Stdlib only. Never import ``sportsdataverse`` here (see sdv_docs/__init__.py).
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Optional

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
