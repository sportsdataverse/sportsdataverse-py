"""sdv-docs MCP server: six read-only lookups over the SportsDataverse docs index.

Run as ``sdv-docs`` (stdio). Never import ``sportsdataverse`` here (see sdv_docs/__init__.py).
"""

from __future__ import annotations

import difflib
import json
import sys
from typing import Any, Callable, Optional

from sdv_docs.index import Index, IndexUnavailable, locate

INSTALL = "sdv-docs needs Python >= 3.10 and the mcp extra: pip install 'sportsdataverse[mcp]'"
USAGE = (
    "usage: sdv-docs [--help | --version]\n\n"
    "With no arguments, serve the sdv-docs MCP tools over stdio (Python >= 3.10, sportsdataverse[mcp])."
)
COLUMN_CAP = 50
WRAPPER_CAP = 6
FOUND_CAP = 10
LIMIT_MAX = 100
TABLE_BUDGET = 40_000  # characters of get_function's returns table; MCP clients cap tool output (~25k tokens)


def _with_index(run: Callable[[Index], str]) -> str:
    try:
        path = locate()
    except IndexUnavailable as e:
        return f"index unavailable: {e}; set SDV_DOCS_DB or check network"
    with Index(path) as ix:
        return run(ix)


def _clamp(limit: int) -> int:
    return max(1, min(int(limit), LIMIT_MAX))


def _elsewhere(filters: dict[str, Optional[str]], found: list[str]) -> str:
    """The tail of a message for a filtered lookup that missed but matches without its filters."""
    shown = " and ".join(f"{k} {v!r}" for k, v in filters.items() if v)
    more = ", …" if len(found) > FOUND_CAP else ""
    return f"not for {shown} (found in: {', '.join(found[:FOUND_CAP])}{more})."


def _partial(query: str) -> str:
    """Heading for OR-fallback hits: spec §5 says a miss is explicit, never dressed up as an answer."""
    return f"Nothing matches every word of {query!r}; partial matches:"


def _not_found(ix: Index, what: str, name: str, table: str) -> str:
    close = difflib.get_close_matches(name, ix.names(table), n=5, cutoff=0.6)
    hint = f" Close names: {', '.join(f'`{c}`' for c in close)}." if close else ""
    return f"`{name}`: {what} not in index.{hint}"


def search(
    query: str,
    kind: Optional[str] = None,
    league: Optional[str] = None,
    lang: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search the SportsDataverse index: Python and R functions, returned columns, provider API endpoints and released datasets.

    Use this to discover names; then call get_function / find_columns / find_endpoints / list_datasets for exact details.

    Args:
        query: Free text, e.g. "wnba shotchartdetail", "nhl shifts", "athlete injuries".
        kind: Optional filter: function, column, endpoint or dataset.
        league: Optional league prefix, e.g. nba, wnba, mbb, wbb, cfb, nfl, mlb, nhl.
        lang: Optional filter: python or r.
        limit: Maximum hits (default 10, at most 100).
    """

    def run(ix: Index) -> str:
        hits = ix.search(query, kind=kind, league=league, lang=lang, limit=_clamp(limit))
        if not hits:
            filters = {"kind": kind, "league": league, "lang": lang}
            anywhere = ix.search(query, limit=100) if any(filters.values()) else None
            if anywhere and anywhere.op == "AND":  # an OR hit is a partial match: it proves nothing exists elsewhere
                keys = [k for k, v in filters.items() if v]
                found = sorted({" · ".join(str(h[k] or "none") for k in keys) for h in anywhere})
                return f"Results for {query!r} exist, but " + _elsewhere(filters, found)
            return f"No results for {query!r}."
        lines = [_partial(query) if hits.op == "OR" else f"{len(hits)} result(s) for {query!r}:"]
        for h in hits:
            tags = " · ".join(t for t in (h["kind"], h["lang"], h["league"]) if t)
            title = f" — {h['title']}" if h["title"] else ""
            url = f" {h['url']}" if h["url"] else ""
            lines.append(f"- **{h['name']}** ({tags}){title}{url}")
        return "\n".join(lines)

    return _with_index(run)


def get_function(name: str, lang: Optional[str] = None, columns: bool = True) -> str:
    """Full reference for one function: signature, parameters, returned columns (type + meaning), release dataset and Python/R equivalents.

    Args:
        name: Function name, e.g. load_nhl_pbp or espn_nba_team_roster; R functions as pkg::fn, e.g. hoopR::load_nba_pbp.
        lang: Optional: python or r, when a name exists in both.
        columns: Include the returned-columns table (default True). Set False for very wide datasets.
    """

    def run(ix: Index) -> str:
        rows = ix.functions(name, lang)
        if not rows:
            pkg, _, bare = name.strip().rpartition("::")
            bare = bare.strip().removesuffix("()")
            anywhere = ix.functions(bare) if (pkg or lang) else []
            if anywhere:
                found = [
                    f"`{r['name']}` (python)" if r["lang"] == "python" else f"`{r['package']}::{r['name']}` (r)"
                    for r in anywhere
                ]
                return f"`{bare}` exists, but " + _elsewhere({"package": pkg.strip(), "lang": lang}, found)
            return _not_found(ix, "function", bare, "functions")
        return "\n\n---\n\n".join(_function_block(ix, f, columns) for f in rows)

    return _with_index(run)


def _function_block(ix: Index, f: Any, columns: bool) -> str:
    py = f["lang"] == "python"
    lines = [f"## {f['name']}" if py else f"## {f['package']}::{f['name']}"]
    lines.append(" · ".join(t for t in (f["lang"], f["kind"], f["league"], f["category"]) if t))
    if f["summary"]:
        lines.append(f["summary"])
    if f["signature"]:
        lines.append(f"```python\n{f['signature']}\n```")
    if py:
        params = ix.params(f["name"])
        if params:
            lines.append("**Parameters**")
            for p in params:
                typ = f" ({p['type']})" if p["type"] else ""
                dflt = f" = {p['default_value']}" if p["default_value"] else ""
                desc = f": {p['description']}" if p["description"] else ""
                lines.append(f"- `{p['name']}`{typ}{dflt}{desc}")
        ds = ix.dataset_for(f["name"])
        if ds is not None:
            lines.append(
                f"**Dataset**: release `{ds['tag']}`, seasons from {ds['min_season']}, "
                f"`{ds['url_template']}` ({ds['release_url']})"
            )
        eq = [e for e in ix.equivalents(f["name"]) if e["py_function"] == f["name"]]
        if eq:
            lines.append("**R equivalent**: " + ", ".join(f"`{e['r_package']}::{e['r_function']}`" for e in eq))
        cols = ix.columns(f["name"])
        if cols and not columns:
            lines.append(f"_{len(cols)} returned columns omitted; pass columns=True or use find_columns._")
        elif cols:
            lines += [f"**Returns** ({len(cols)} columns)", "| column | type | description |", "|---|---|---|"]
            used = 0
            for i, c in enumerate(cols):
                col = f"{c['section']}.{c['name']}" if c["section"] else c["name"]
                desc = (c["description"] or "").replace("|", "\\|")
                row = f"| `{col}` | {c['type'] or ''} | {desc} |"
                used += len(row) + 1
                if used > TABLE_BUDGET:  # 1,549-column functions reach 210 KB, over the MCP output limit
                    rest = f"full table: {f['doc_url']} (or use find_columns)" if f["doc_url"] else "use find_columns"
                    lines += ["", f"_first {i} of {len(cols)} columns; {rest}_"]
                    break
                lines.append(row)
    else:
        eq = [e for e in ix.equivalents(f["name"]) if e["r_function"] == f["name"] and e["r_package"] == f["package"]]
        if eq:
            lines.append("**Python equivalent**: " + ", ".join(f"`{e['py_function']}`" for e in eq))
    if f["doc_url"]:
        if lines[-1].startswith("|") or lines[-1].startswith("_first "):
            lines.append("")  # a GFM table swallows an adjacent line
        lines.append(f"Docs: {f['doc_url']}")
    return "\n".join(lines)


def find_columns(column: str, league: Optional[str] = None, function: Optional[str] = None) -> str:
    """Which functions return a column, with its type and meaning in each.

    Args:
        column: Column name, e.g. epa, drive_id, event_type (case-insensitive).
        league: Optional league prefix to narrow the list.
        function: Optional function name to get just that function's definition of the column.
    """

    def run(ix: Index) -> str:
        rows = ix.columns_named(column, league=league, function=function, limit=-1)  # -1: no LIMIT, to count
        if not rows:
            anywhere = ix.columns_named(column, limit=-1) if (league or function) else []  # -1: no LIMIT
            if anywhere:
                found = sorted(
                    {r["function"] for r in anywhere} if function else {r["league"] or "none" for r in anywhere}
                )
                return f"`{column.strip()}` exists, but " + _elsewhere({"league": league, "function": function}, found)
            return _not_found(ix, "column", column.strip(), "columns")
        n_fn = len({r["function"] for r in rows})  # a function can return the column in several result sets
        more = (
            f"; showing the first {COLUMN_CAP} of {len(rows)} definitions (narrow with league= or function=)"
            if len(rows) > COLUMN_CAP
            else ""
        )
        lines = [f"`{column.strip()}` is returned by {n_fn} function(s){more}:"]
        for r in rows[:COLUMN_CAP]:
            sec = f" [{r['section']}]" if r["section"] else ""
            url = f" {r['doc_url']}" if r["doc_url"] else ""
            lines.append(
                f"- **{r['function']}**{sec} ({r['type'] or '?'}): {r['description'] or '(no description)'}{url}"
            )
        return "\n".join(lines)

    return _with_index(run)


def find_endpoints(query: str, api: Optional[str] = None, limit: int = 10) -> str:
    """Provider API endpoints (ESPN, stats.nba.com, NHL, MLB, Fox, CBS, Yahoo, 247, ...) matching a description: method, URL, params (* = required), quirks and the sdv-py wrapper that calls it.

    Args:
        query: Free text, e.g. "athlete injuries", "event odds", "shotchartdetail".
        api: Optional exact API name, e.g. espn_core_v2, espn_site_v2, espn_cdn, nba_stats, nhl_api_web.
        limit: Maximum endpoints (default 10, at most 100).
    """

    def run(ix: Index) -> str:
        rows = ix.endpoints(query, api=api, limit=_clamp(limit))
        if not rows:
            anywhere = ix.endpoints(query, limit=100) if api else None
            if anywhere and anywhere.op == "AND":
                found = sorted({e["api"] for e in anywhere})
                return f"Endpoints matching {query!r} exist, but " + _elsewhere({"api": api}, found)
            return f"No endpoints match {query!r}" + (f" in api {api!r}" if api else "") + "."
        blocks = []
        for e in rows:
            lines = [f"### {e['method']} {e['path']}", f"api: {e['api']} · source: {e['source']}"]
            if e["summary"]:
                lines.append(e["summary"])
            params = json.loads(e["params_json"] or "[]")
            if params:
                lines.append(
                    "params: " + ", ".join(f"`{p['name']}`" + ("*" if p.get("required") else "") for p in params)
                )
            names = (e["wrapper"] or "").split()
            if names:
                extra = f" (+{len(names) - WRAPPER_CAP} more)" if len(names) > WRAPPER_CAP else ""
                lines.append("sdv-py: " + ", ".join(f"`{n}`" for n in names[:WRAPPER_CAP]) + extra)
            if e["notes"]:
                lines.append(f"notes: {e['notes'][:600]}")
            if e["spec_url"]:
                lines.append(f"spec: {e['spec_url']}")
            blocks.append("\n".join(lines))
        return (_partial(query) + "\n" if rows.op == "OR" else "") + "\n\n".join(blocks)

    return _with_index(run)


def list_datasets(league: Optional[str] = None, query: Optional[str] = None, limit: int = 50) -> str:
    """Released datasets behind the load_* loaders: release tag, first season and download URL template.

    Args:
        league: Optional league prefix, e.g. nhl, nfl, cfb, nba.
        query: Optional free text, e.g. "shifts", "ratings", "pbp".
        limit: Maximum rows (default 50, at most 100).
    """

    def run(ix: Index) -> str:
        limit_ = _clamp(limit)
        rows = ix.datasets(league=league, query=query, limit=-1)  # -1: no LIMIT, to count what is cut
        if not rows:
            anywhere = ix.datasets(query=query, limit=-1) if league else None  # -1: no LIMIT
            if anywhere and anywhere.op == "AND":
                what = f"Datasets matching {query!r} exist" if query else "Datasets exist"
                return f"{what}, but " + _elsewhere({"league": league}, sorted({d["league"] for d in anywhere}))
            return (
                "No datasets match"
                + (f" league {league!r}" if league else "")
                + (f" query {query!r}" if query else "")
                + "."
            )
        lines = [_partial(query or "")] if rows.op == "OR" else []
        if len(rows) > limit_:
            lines.append(f"Showing {limit_} of {len(rows)} datasets; narrow with league= or query=.")
        lines += ["| loader | league | release tag | from season | release |", "|---|---|---|---|---|"]
        lines += [
            f"| {d['loader']} | {d['league']} | {d['tag']} | {d['min_season']} | {d['release_url']} |"
            for d in rows[:limit_]
        ]
        return "\n".join(lines)

    return _with_index(run)


def index_info() -> str:
    """When the index was built, from which sdv-py commit and sources, and how many rows each table holds."""

    def run(ix: Index) -> str:
        meta = ix.meta()
        lines = [
            f"sdv-docs index (schema {meta.get('schema_version', '?')}), built {meta.get('built_at', '?')} "
            f"from sdv-py {meta.get('sdv_py_commit', '?')}"
        ]
        lines += [
            f"- {k}: {v}" for k, v in sorted(meta.items()) if k not in ("schema_version", "built_at", "sdv_py_commit")
        ]
        lines.append("rows: " + ", ".join(f"{k} {v:,}" for k, v in ix.counts().items()))
        return "\n".join(lines)

    return _with_index(run)


TOOLS: tuple[Callable[..., str], ...] = (search, get_function, find_columns, find_endpoints, list_datasets, index_info)


def build_server() -> Any:
    """The MCP server with every tool registered (imports mcp lazily so 3.9 can still import this module)."""
    from mcp.server import MCPServer

    server = MCPServer("sdv-docs")
    for tool in TOOLS:
        server.tool()(tool)
    return server


def _version() -> str:
    from importlib.metadata import PackageNotFoundError, version  # never import sportsdataverse itself

    try:
        return version("sportsdataverse")
    except PackageNotFoundError:
        return "unknown"


def main(argv: Optional[list[str]] = None) -> int:
    """Console entry point ``sdv-docs``: serve the tools over stdio until the client disconnects."""
    args = sys.argv[1:] if argv is None else argv
    if args in (["-h"], ["--help"]):
        print(USAGE)
        return 0
    if args == ["--version"]:
        print(f"sdv-docs {_version()}")
        return 0
    if args:
        print(USAGE, file=sys.stderr)
        return 2
    if sys.version_info < (3, 10):
        print(INSTALL, file=sys.stderr)
        return 2
    try:
        server = build_server()
    except ImportError:
        print(INSTALL, file=sys.stderr)
        return 2
    server.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
