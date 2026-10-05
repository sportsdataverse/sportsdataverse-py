"""Build the sdv-docs index: SportsDataverse functions, returned columns, provider endpoints,
released datasets and Python/R equivalents in one SQLite file.

    uv run python tools/codegen/build_docs_index.py --out build/docs-index [--offline]

Reads the same in-memory model codegen renders the reference docs from (spec.py +
generate.py), the committed generated docs tree (for doc URLs), and two public network
inputs that --offline skips: the sdv-swagger OpenAPI specs and the R packages' pkgdown
llms.txt function indexes. Writes sdv_docs.schema.ASSET and MANIFEST into --out.
"""

from __future__ import annotations

import importlib
import inspect
import io
import json
import re
import subprocess
import sys
import tarfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.codegen import generate as G  # noqa: E402
from tools.codegen import spec  # noqa: E402

DOCS_ROOT = G.ROOT / "docs" / "docs"
DOCS_URL = "https://py.sportsdataverse.org/docs/"
ENDPOINT_BLOB = (
    "https://github.com/sportsdataverse/sportsdataverse-py/blob/{commit}/tools/codegen/endpoints/{stem}.yaml"
)
SWAGGER_REPO = "saiemgilani/sdv-swagger"
# R packages whose pkgdown site serves llms.txt (probed 2026-10-05). baseballr's SDV
# domain 308-redirects to Bill Petti's site, so it is listed at its final URL.
R_LLMS = {
    "hoopR": "https://hoopr.sportsdataverse.org/llms.txt",
    "wehoop": "https://wehoop.sportsdataverse.org/llms.txt",
    "cfbfastR": "https://cfbfastr.sportsdataverse.org/llms.txt",
    "fastRhockey": "https://fastrhockey.sportsdataverse.org/llms.txt",
    "baseballr": "https://billpetti.github.io/baseballr/llms.txt",
    "sportsdataverse": "https://r.sportsdataverse.org/llms.txt",
    "sdvplotR": "https://sdvplotr.sportsdataverse.org/llms.txt",
    "cfbplotR": "https://cfbplotr.sportsdataverse.org/llms.txt",
    "cfb4th": "https://cfb4th.sportsdataverse.org/llms.txt",
    "oddsapiR": "https://oddsapir.sportsdataverse.org/llms.txt",
    "sportyR": "https://sportyr.sportsdataverse.org/llms.txt",
    "recruitR": "https://recruitr.sportsdataverse.org/llms.txt",
    "usfootballR": "https://usfootballr.sportsdataverse.org/llms.txt",
}


@dataclass
class Rows:
    """Rows per table, in sdv_docs.schema column order."""

    functions: list[tuple] = field(default_factory=list)
    params: list[tuple] = field(default_factory=list)
    columns: list[tuple] = field(default_factory=list)
    endpoints: list[tuple] = field(default_factory=list)
    datasets: list[tuple] = field(default_factory=list)
    equivalents: list[tuple] = field(default_factory=list)
    meta: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Wrapper:
    """One generated wrapper (ESPN cross-league or flat API) and what codegen knows about it."""

    name: str
    league: str
    kind: str  # "espn" | "flat"
    schema: Optional[str]
    summary: str
    params: tuple  # spec.Param, path params first


_HEADING = re.compile(r"^(?:## ([A-Za-z_][A-Za-z0-9_]*)|### ([A-Za-z_][A-Za-z0-9_]*) \{#([A-Za-z0-9_]+)\})\s*$")


def doc_anchors(docs_root: Path = DOCS_ROOT) -> dict[str, str]:
    """Function name -> live docs URL, from the headings of the committed (drift-gated) docs tree."""
    out: dict[str, str] = {}
    for md in sorted(docs_root.rglob("*.md")):
        rel = md.relative_to(docs_root).with_suffix("").as_posix()
        page = "" if rel == "index" else rel.removesuffix("/index")
        for line in md.read_text(encoding="utf-8").splitlines():
            m = _HEADING.match(line)
            if m:
                name = m.group(1) or m.group(2)
                anchor = m.group(1).lower() if m.group(1) else m.group(3)
                out.setdefault(name, f"{DOCS_URL}{page}#{anchor}")
    return out


def header_comments(path: Path) -> str:
    """YAML comment lines before the ``endpoints:`` key (e.g. espn_cdn's per-league probe matrix)."""
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("endpoints:"):
            break
        if line.startswith("#"):
            out.append(line.lstrip("#").rstrip())
    return "\n".join(out).strip()


def endpoint_row(api: str, path: str, ep: Any, wrapper: str, notes: str, spec_url: str) -> tuple:
    params = [
        {"name": p.python_name, "api": p.api, "type": p.type, "required": p.required, "description": p.description}
        for p in (*ep.path_params, *ep.query_params)
    ]
    return (api, "GET", path, ep.summary or "", json.dumps(params), wrapper, notes, "codegen", spec_url)


def codegen_wrappers(commit: str, rows: Rows) -> dict[str, Wrapper]:
    """Every generated wrapper by name, plus one endpoints row per endpoint-YAML entry."""
    params = spec.load_parameters(G.ENDPOINTS / "parameters.yaml")
    cfg = spec.load_leagues(G.ENDPOINTS / "leagues.yaml")
    apis = {stem: spec.load_espn_api(G.ENDPOINTS / f"{stem}.yaml", params) for stem in G.ESPN_APIS}
    out: dict[str, Wrapper] = {}
    found: dict[tuple[str, str], tuple[str, list[str]]] = {}  # (stem, short) -> (host_url, wrapper names)
    for lg in cfg.leagues:
        for v in G._espn_league_views(lg, list(apis.values()), cfg.hosts):
            ep = next(e for e in apis[v.api_name].endpoints if e.short == v.short)
            out.setdefault(
                v.fn_name,
                Wrapper(
                    v.fn_name, lg.prefix, "espn", ep.returns_schema, v.summary, (*ep.path_params, *ep.query_params)
                ),
            )
            found.setdefault((v.api_name, v.short), (v.host_url, []))[1].append(v.fn_name)
    for stem, api in apis.items():
        notes, url = header_comments(G.ENDPOINTS / f"{stem}.yaml"), ENDPOINT_BLOB.format(commit=commit, stem=stem)
        for ep in api.endpoints:
            host, names = found.get((stem, ep.short), (cfg.hosts[api.host], []))
            rows.endpoints.append(endpoint_row(stem, host + ep.path, ep, " ".join(names), notes, url))
    for stem, prefix in G.FLAT_APIS:
        path = G.ENDPOINTS / f"{stem}.yaml"
        if not path.exists():
            continue
        fa = spec.load_flat_api(path, params)
        views = {v.short: v for v in G._flat_views(fa, league_prefix=prefix)}
        notes, url = header_comments(path), ENDPOINT_BLOB.format(commit=commit, stem=stem)
        for ep in fa.endpoints:
            v = views.get(ep.short)
            if v is not None:
                out.setdefault(
                    v.fn_name,
                    Wrapper(
                        v.fn_name, prefix, "flat", ep.returns_schema, v.summary, (*ep.path_params, *ep.query_params)
                    ),
                )
            rows.endpoints.append(
                endpoint_row(stem, (v.host_url if v else fa.host) + ep.path, ep, v.fn_name if v else "", notes, url)
            )
    return out


def _add_python(
    rows: Rows,
    name: str,
    obj: Any,
    leaf: str,
    wrappers: dict[str, Wrapper],
    loaders: dict[str, Any],
    anchors: dict[str, str],
) -> str:
    w = wrappers.get(name)
    if w is not None:
        kind, league = w.kind, w.league
    elif name in loaders:
        kind, league = "loader", loaders[name].league
    else:
        kind, league = ("parser" if name.startswith("parse_") else "function"), leaf
    view = G._doc_view(obj)
    try:
        signature = f"{name}{inspect.signature(obj)}"
    except (TypeError, ValueError):
        signature = name
    rows.functions.append(
        (
            name,
            "python",
            "sportsdataverse",
            getattr(obj, "__module__", None),
            league,
            kind,
            None,
            view["short"] or None,
            signature,
            anchors.get(name),
        )
    )
    rows.params.extend(
        (name, p["name"], p["type"] or None, int(p["default"] == ""), p["default"] or None, p["description"] or None)
        for p in view["params"]
    )
    return league


def python_functions(
    wrappers: dict[str, Wrapper], loaders: dict[str, Any], anchors: dict[str, str], rows: Rows
) -> dict[str, str]:
    """One functions row (+ params) per public sdv-py callable. Returns name -> league."""
    import sportsdataverse
    from sportsdataverse import discover

    seen: dict[str, str] = {}
    for leaf, names in sorted(discover.list_functions().items()):
        mod = importlib.import_module(f"sportsdataverse.{discover._LEAF_TO_DOTTED.get(leaf, leaf)}")
        for name in names:
            obj = getattr(mod, name, None)
            if name not in seen and obj is not None:
                seen[name] = _add_python(rows, name, obj, leaf, wrappers, loaders, anchors)
    for name, ld in loaders.items():  # loader-only leagues (e.g. pwhl) surface at the package top level
        obj = getattr(sportsdataverse, name, None)
        if name not in seen and obj is not None:
            seen[name] = _add_python(rows, name, obj, ld.league, wrappers, loaders, anchors)
    for name, w in wrappers.items():  # generated, but not reachable through discover's leaves
        if name not in seen:
            rows.functions.append(
                (
                    name,
                    "python",
                    "sportsdataverse",
                    None,
                    w.league,
                    w.kind,
                    None,
                    w.summary or None,
                    None,
                    anchors.get(name),
                )
            )
            rows.params.extend(
                (
                    name,
                    p.python_name,
                    p.type,
                    int(p.required),
                    None if p.default is None else repr(p.default),
                    p.description or None,
                )
                for p in w.params
            )
            seen[name] = w.league
    return seen


def _clean(desc: str) -> Optional[str]:
    return desc.replace("\\|", "|").strip() or None


def python_columns(funcs: dict[str, str], wrappers: dict[str, Wrapper], loaders: dict[str, Any], rows: Rows) -> None:
    """Returned columns with the same descriptions the reference docs show (manual, then R back-fill)."""
    loader_schemas = G._loader_schemas()
    for name, league in funcs.items():
        w = wrappers.get(name)
        if w is not None:
            doc = G._schema_doc(w.schema, w.league) if w.schema else {}
            frames = doc.get("frames") or ([{"section": None, "columns": doc.get("columns") or []}] if doc else [])
            for fr in frames:
                for c in fr.get("columns") or []:
                    desc = G._table_cell_desc(c.get("description") or "", w.league, c["name"], schema=doc.get("schema"))
                    rows.columns.append((name, fr.get("section"), c["name"], c.get("type"), _clean(desc)))
        elif name in loaders:
            for c in loader_schemas.get(name, []):
                desc = G._table_cell_desc("", league, c["name"], schema=name)
                rows.columns.append((name, None, c["name"], c.get("type"), _clean(desc)))
        else:
            for scope in (league, "global"):
                cols = G._autodoc_return_columns(scope, name)
                for c in cols:
                    desc = G._table_cell_desc(c.get("description") or "", league, c["name"], schema=name)
                    rows.columns.append((name, None, c["name"], c.get("type"), _clean(desc)))
                if cols:
                    break


def dataset_rows(rel: Any, rows: Rows) -> None:
    for ld in rel.loaders:
        rows.datasets.append(
            (
                ld.fn,
                ld.league,
                ld.tag,
                f"{rel.bases[ld.base]}{ld.url}",
                ld.min_season,
                G._release_page_url(rel.bases, ld.base, ld.url, ld.tag),
            )
        )


def equivalent_rows(funcs: dict[str, str], rows: Rows) -> None:
    """The rule generate._r_parity_rows applies (identity or curated alias, R export must exist), computed directly."""
    exports = {pkg: set(fns) for pkg, fns in G._r_exports().items()}
    aliases = G._r_parity_aliases()
    for name, league in sorted(funcs.items()):
        pkg = G._R_PARITY_PACKAGE.get(league)
        if not pkg:
            continue
        alias = aliases.get(league, {}).get(name)
        r_fn = alias or name
        if r_fn in exports.get(pkg, set()):
            rows.equivalents.append((name, pkg, r_fn, "alias" if alias else "identity"))


def http_get(url: str, timeout: float = 60.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "sdv-docs-builder"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 -- fixed public https URLs
        data: bytes = resp.read()
    return data


def swagger_sha() -> str:
    """Current sdv-swagger main SHA, via git ls-remote (no GitHub API quota)."""
    out = subprocess.run(
        ["git", "ls-remote", f"https://github.com/{SWAGGER_REPO}.git", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return out.split()[0]


def fetch_swagger(sha: str) -> dict[str, dict]:
    return parse_swagger_tarball(http_get(f"https://codeload.github.com/{SWAGGER_REPO}/tar.gz/{sha}"))


def parse_swagger_tarball(data: bytes) -> dict[str, dict]:
    """Every top-level OpenAPI/Swagger document in a GitHub tarball, keyed by file name. Nothing touches disk."""
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    out: dict[str, dict] = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for m in tar.getmembers():
            name = m.name.split("/", 1)[-1]
            if not m.isfile() or "/" in name or not name.endswith((".yaml", ".yml", ".json")):
                continue
            f = tar.extractfile(m)
            if f is None:
                continue
            raw = f.read()
            try:
                doc = json.loads(raw) if name.endswith(".json") else yaml.load(raw, Loader=loader)  # noqa: S506 -- SafeLoader
            except (ValueError, yaml.YAMLError):
                continue
            if isinstance(doc, dict) and ("openapi" in doc or "swagger" in doc) and doc.get("paths"):
                out[name] = doc
    return out


def _server_url(doc: dict) -> str:
    servers = doc.get("servers") or []
    if servers and isinstance(servers[0], dict):
        return str(servers[0].get("url", "")).rstrip("/")
    if doc.get("host"):
        return f"https://{doc['host']}{doc.get('basePath', '')}".rstrip("/")
    return ""


def _param(doc: dict, p: Any) -> Optional[dict]:
    if isinstance(p, dict) and "$ref" in p:
        node: Any = doc
        for part in str(p["$ref"]).lstrip("#/").split("/"):
            node = node.get(part, {}) if isinstance(node, dict) else {}
        p = node
    if not isinstance(p, dict) or "name" not in p:
        return None
    return {
        "name": p["name"],
        "in": p.get("in"),
        "required": bool(p.get("required")),
        "description": str(p.get("description") or "")[:300],
    }


def openapi_rows(specs: dict[str, dict], sha: str, rows: Rows) -> None:
    for fname, doc in sorted(specs.items()):
        title = str((doc.get("info") or {}).get("title") or fname)
        base, url = _server_url(doc), f"https://github.com/{SWAGGER_REPO}/blob/{sha}/{fname}"
        for path, item in (doc.get("paths") or {}).items():
            if not isinstance(item, dict):
                continue
            shared = item.get("parameters") or []
            for method in ("get", "post", "put", "patch", "delete"):
                op = item.get(method)
                if not isinstance(op, dict):
                    continue
                params = [q for q in (_param(doc, p) for p in (*shared, *(op.get("parameters") or []))) if q]
                rows.endpoints.append(
                    (
                        title,
                        method.upper(),
                        base + path,
                        str(op.get("summary") or op.get("operationId") or ""),
                        json.dumps(params),
                        "",
                        str(op.get("description") or "")[:2000],
                        "openapi",
                        url,
                    )
                )


_R_ALIAS = re.compile(r"\[`([^`]+?)`\]\((\S+?)\)")
_R_TITLE = re.compile(r"^\s*\*\*(.+?)\*\*\s*$")


def parse_pkgdown_llms(package: str, text: str, rows: Rows) -> int:
    """R functions from the ``# Package index`` of a pkgdown llms.txt. One help page = aliases + a bold title."""
    _, _, index = text.partition("\n# Package index")
    category: Optional[str] = None
    pending: list[tuple[str, str]] = []
    added = 0
    for line in index.splitlines():
        if line.startswith("# "):
            break
        if line.startswith(("## ", "### ")):
            category, pending = line.lstrip("#").strip(), []
            continue
        if line.startswith("- "):
            pending = []
        pending += [(n.removesuffix("()"), u) for n, u in _R_ALIAS.findall(line)]
        m = _R_TITLE.match(line)
        if m and pending:
            for n, u in pending:
                rows.functions.append((n, "r", package, None, None, "function", category, m.group(1).strip(), None, u))
                added += 1
            pending = []
    return added
