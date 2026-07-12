"""Codegen CLI: render concrete ESPN league modules from YAML (build / --check)."""

from __future__ import annotations

import argparse
import functools
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlencode

# Make ``tools`` importable when run as a script (`python tools/codegen/generate.py`),
# where sys.path[0] is this file's dir rather than the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.codegen import render, spec  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"
OUT = ROOT / "tools" / "codegen" / "_generated"
LIVE = ROOT / "sportsdataverse"

ESPN_APIS = ["espn_site_v2", "espn_web_v3", "espn_core_v2"]

# Invoke ruff via the *current interpreter's* environment (``python -m ruff``)
# rather than a bare ``ruff`` on PATH. A bare ``ruff`` can resolve to a stale
# global install (e.g. a system 0.3.x) that ignores ``[tool.ruff.format]
# line-ending = "lf"`` and rewrites generated files with native CRLF on Windows
# -- which then trips ``--check`` against the all-LF committed blobs. ``-m ruff``
# pins to the venv's ruff, matching the pre-commit hook + CI byte-for-byte.
_RUFF = [sys.executable, "-m", "ruff"]

_PATH_TOKEN = re.compile(r"\{(\w+)\}")


def _path_token_first(s: str) -> str:
    m = _PATH_TOKEN.search(s)
    return m.group(1) if m else ""


def _sub_slugs(path: str, sport: str, league: str) -> str:
    """Substitute only the {sport}/{league} slugs, leaving path-param tokens intact.

    (``str.format`` would raise KeyError on the remaining ``{athlete_id}`` etc.)
    """
    return path.replace("{sport}", sport).replace("{league}", league)


def _example_url(host_url: str, ep: spec.Endpoint, sport: str, league: str) -> str:
    path = _sub_slugs(ep.path.replace("[", "").replace("]", ""), sport, league)
    for p in ep.path_params:
        val = ep.example_args.get(p.python_name)
        if val is not None:
            path = path.replace("{" + p.python_name + "}", str(val))
    # drop any unfilled (optional, trailing) path tokens from the example
    if "{" in path:
        path = path[: path.index("{")].rstrip("/")
    qs = {p.api: ep.example_args[p.python_name] for p in ep.query_params if p.python_name in ep.example_args}
    return f"{host_url}{path}" + (f"?{urlencode(qs)}" if qs else "")


def _example_call(ep: spec.Endpoint, fn_name: str) -> str:
    args = ", ".join(f"{k}={v!r}" for k, v in ep.example_args.items())
    return f"{fn_name}({args})"


def _build_docstring(
    ep: spec.Endpoint,
    sport: str,
    league: str,
    host_url: str,
    example_url: str,
    example_call: str,
    flat: bool = False,
    auth: bool = False,
    league_param: bool = False,
) -> str:
    """Build a function docstring as a 4-space-indented block (precise indentation).

    Shared renderer (Python, not a Jinja macro) so every generated family emits the
    same docstring contract without Jinja whitespace-control fragility. ``flat`` omits
    the sport/league binding line for the non-sport/league NHL/MLB APIs. ``auth``
    documents the extra ``headers`` arg the template adds for token-authed families
    (NFL.com), keeping the public ``Args`` block complete.
    """
    lines = [f'"""{ep.summary}', ""]
    if not flat:
        if league_param:
            lines.append(f"Bound to sport={sport!r}; ``league`` is a required argument (e.g. {league!r}).")
        else:
            lines.append(f"Bound to sport={sport!r}, league={league!r}.")
        lines.append("")
    lines.append(f"Endpoint: ``GET {host_url}{ep.path}``")
    if example_url:
        lines.append(f"Example URL: {example_url}")
    lines.append("")
    lines.append("Args:")
    for p in ep.path_params:
        lines.append(f"    {p.python_name}: {p.api} path parameter.")
    for p in ep.query_params:
        lines.append(f"    {p.python_name}: {p.api} query parameter.")
    if auth:
        lines.append(
            "    headers: optional pre-minted auth headers dict (e.g. from "
            "nfl_headers_gen()) to reuse across calls; a fresh anonymous token is "
            "minted when omitted."
        )
    if ep.parser:
        lines.append(
            f"    return_parsed: parse the payload through {ep.parser} -> polars DataFrame "
            "(default True). Pass return_parsed=False for the raw JSON Dict."
        )
        lines.append("    return_as_pandas: with return_parsed, return a pandas DataFrame instead of polars.")
    lines.append("")
    lines.append("Returns:")
    if ep.parser:
        lines.append("    A polars/pandas DataFrame by default; the raw JSON ``Dict`` when ``return_parsed=False``.")
    else:
        lines.append("    The raw JSON ``Dict``.")
    lines.append("")
    lines.append("Example:")
    lines.append("    Quick start::")
    lines.append("")
    lines.append(f"        {example_call}")
    lines.append('"""')
    return "\n".join(("    " + ln) if ln else "" for ln in lines)


@functools.lru_cache(maxsize=None)
def _docstring_param_descs(league_prefix: str, fn_name: str) -> dict[str, str]:
    """``{python_name: description}`` from the live wrapper's parsed docstring.

    Resolves the wrapper function from ``sportsdataverse.{league_prefix}`` (or
    directly from ``sportsdataverse.{league_prefix}`` for flat-API wrappers) and
    delegates to :func:`_doc_view` to extract the per-param descriptions.

    Returns an empty dict when the function is not importable (stub / not yet
    wired) so callers always get a safe fallback.  Results are cached per
    ``(league_prefix, fn_name)`` so repeated endpoint views inside the same
    codegen run share one import + parse.
    """
    import importlib

    try:
        mod = importlib.import_module(f"sportsdataverse.{league_prefix}") if league_prefix else None
    except Exception:  # noqa: BLE001
        return {}
    fn = getattr(mod, fn_name, None) if mod is not None else None
    if fn is None or not callable(fn):
        return {}
    try:
        view = _doc_view(fn)
    except Exception:  # noqa: BLE001
        return {}
    return {p["name"]: p["description"] for p in view["params"]}


def _param_rows(ep: spec.Endpoint, league_prefix: str = "", fn_name: str = "") -> list[dict]:
    """Merge path + query params into nba_api-style doc rows.

    ``required`` mirrors the spec; ``nullable`` is its inverse (an optional param
    may be omitted/``None``). Path params come first, matching the signature order
    used by the generated wrapper.

    Description resolution priority (highest wins):
    1. ``description`` field on the param definition (parameters.yaml or extra_params/path_params).
    2. The live wrapper's parsed docstring Args section (via :func:`_docstring_param_descs`).
    3. Empty string (silent fallback).

    Pipe characters in descriptions are escaped so they don't break the markdown
    table renderer.
    """
    doc_descs: dict[str, str] = {}
    if league_prefix and fn_name:
        doc_descs = _docstring_param_descs(league_prefix, fn_name)
    rows: list[dict] = []
    for p in (*ep.path_params, *ep.query_params):
        # Prefer the authored spec description; fall back to the parsed docstring.
        raw_desc = p.description or doc_descs.get(p.python_name, "")
        # Escape pipe chars and collapse newlines so the description is safe
        # inside a single markdown table cell.
        desc = raw_desc.replace("|", "\\|").replace("\n", " ").strip()
        rows.append(
            {
                "api": p.api,
                "python": p.python_name,
                "pattern": p.pattern,
                "required": p.required,
                "nullable": not p.required,
                "description": desc,
            },
        )
    return rows


# ---------------------------------------------------------------------------
# Render-time column-description fill from the mined SDV R-package dictionary.
#
# A prior codegen step mined per-column descriptions from the SDV R packages
# (cfbfastR / hoopR / wehoop / baseballr / fastRhockey) plus the nflverse data
# dictionaries (nflreadr CSVs + nflfastR variable list) into
# r_column_descriptions.yaml, shape:
#   {package: {col_name: description}, ..., _merged: {col_name: description}}
# At RENDER time (NOT capture) we backfill any blank return-table description
# cell from this dictionary, keyed by column name and league-aware package. A
# stored (hand-curated) description is never overwritten. Unmatched columns stay
# blank. This is purely cosmetic doc enrichment -- no schema YAML is mutated.
# ---------------------------------------------------------------------------

_R_DICT_FILE = ROOT / "tools" / "codegen" / "r_column_descriptions.yaml"

# League prefix -> R package whose column docs describe its columns. nfl maps to
# nflreadr (canonical nflverse dictionaries); nflfastR's variable list still
# contributes via the ``_merged`` fallback. Only pwhl has no package and
# resolves entirely via ``_merged``.
_LEAGUE_R_PACKAGE = {
    "cfb": "cfbfastR",
    "nba": "hoopR",
    "mbb": "hoopR",
    "wnba": "wehoop",
    "wbb": "wehoop",
    "mlb": "baseballr",
    "nhl": "fastRhockey",
    "nfl": "nflreadr",
    # Hockey junior leagues: use fastRhockey column descriptions (sport-appropriate)
    # so the _merged fallback (which has basketball-specific phrases like
    # "Las Vegas Aces" and "while on court") is never used for these leagues.
    "ahl": "fastRhockey",
    "ohl": "fastRhockey",
    "whl": "fastRhockey",
    "qmjhl": "fastRhockey",
    "pwhl": "fastRhockey",
}


@functools.lru_cache(maxsize=1)
def _r_col_descs() -> dict:
    """``{package: {col_name: description}}`` mined SDV R-package column docs.

    Cached loader of ``r_column_descriptions.yaml`` (committed + deterministic).
    Empty dict if the file is absent so the render-time fill is a silent no-op."""
    if not _R_DICT_FILE.exists():
        return {}
    import yaml

    return yaml.safe_load(_R_DICT_FILE.read_text(encoding="utf-8")) or {}


@functools.lru_cache(maxsize=None)
def _r_pkg_dict(league: str | None) -> dict:
    """The per-league lookup dict: the league's R package map (cached per league).

    ``None`` (or a league with no R package) yields ``{}`` so the caller falls
    back to ``_merged`` -- the union, most-frequent description per column name."""
    d = _r_col_descs()
    pkg = _LEAGUE_R_PACKAGE.get(league or "")
    return d.get(pkg, {}) if pkg else {}


def _r_col_desc(league: str | None, col: str) -> str:
    """Mined description for ``col`` for ``league``'s R package, else ``_merged``.

    Resolution: league package dict -> ``_merged`` union -> ``""``. ``league=None``
    (or a league with no package, e.g. pwhl) skips straight to ``_merged``."""
    if not col:
        return ""
    val = _r_pkg_dict(league).get(col)
    if val:
        return val
    return _r_col_descs().get("_merged", {}).get(col, "") or ""


_MANUAL_DESC_FILE = ROOT / "tools" / "codegen" / "manual_column_descriptions.yaml"


@functools.lru_cache(maxsize=None)
def _manual_col_descs() -> dict:
    """``{schema: {col: desc}, _global: {col: desc}}`` hand-curated column
    descriptions (committed, deterministic). Empty dict when the file is absent."""
    import yaml

    if not _MANUAL_DESC_FILE.exists():
        return {}
    return yaml.safe_load(_MANUAL_DESC_FILE.read_text(encoding="utf-8")) or {}


def _manual_col_desc(schema: str | None, col: str) -> str:
    """Hand-curated description for ``col``: schema-keyed first, then ``_global``.

    Resolution: ``manual[schema][col]`` -> ``manual["_global"][col]`` -> ``""``."""
    if not col:
        return ""
    d = _manual_col_descs()
    if schema:
        v = (d.get(schema) or {}).get(col)
        if v:
            return v
    return (d.get("_global") or {}).get(col, "") or ""


def _table_cell_desc(stored: str, league: str | None, col: str, schema: str | None = None) -> str:
    """A return-table description cell: stored value if non-empty, else the
    hand-curated manual dict (schema-keyed), else the R-dict fill.

    Never overwrites a non-empty (captured) stored description. The result is
    pipe/newline-escaped so it is safe inside a single markdown table cell.
    reST markup (double-backtick literals, Sphinx roles) is normalised to
    standard markdown inline code."""
    if (stored or "").strip():
        raw = stored
    else:
        raw = _manual_col_desc(schema, col) or _r_col_desc(league, col)
    normalized = _normalize_rst((raw or "").replace("\n", " ").strip())
    return normalized.replace("|", "\\|")


# ---------------------------------------------------------------------------
# Python <-> R function parity (per-league index table).
#
# The sdv-py wrappers mirror the sister R packages' names, so most Python
# functions have a same-named R function. We link each Python function to its
# equivalent R function's pkgdown reference -- but only when that R function
# actually exists (mined into r_exports.yaml, CI-offline-safe), so the link can't
# 404. r_parity_aliases.yaml supplies curated equivalents where the names diverge
# (e.g. nfl load_nfl_* -> nflreadr load_*, mlb_api_* -> baseballr mlb_*).
# ---------------------------------------------------------------------------

_R_EXPORTS_FILE = ROOT / "tools" / "codegen" / "r_exports.yaml"
_R_PARITY_ALIASES_FILE = ROOT / "tools" / "codegen" / "r_parity_aliases.yaml"

# League prefix -> R package for parity (pwhl is also fastRhockey; otherwise the
# same mapping used for column descriptions).
_R_PARITY_PACKAGE = {**_LEAGUE_R_PACKAGE, "pwhl": "fastRhockey"}

# R package -> pkgdown reference base URL (one <fn>.html per function topic).
_R_PKGDOWN_BASE = {
    "cfbfastR": "https://cfbfastR.sportsdataverse.org/reference",
    "hoopR": "https://hoopR.sportsdataverse.org/reference",
    "wehoop": "https://wehoop.sportsdataverse.org/reference",
    "baseballr": "https://billpetti.github.io/baseballr/reference",
    "fastRhockey": "https://fastRhockey.sportsdataverse.org/reference",
    "nflreadr": "https://nflreadr.nflverse.com/reference",
    "nflfastR": "https://www.nflfastr.com/reference",
}

# Level-2 endpoint/loader function header: ``## `fn` `` (name only, no signature).
_DOC_L2_FN = re.compile(r"(?m)^## `([A-Za-z_][A-Za-z0-9_]*)`\s*$")


@functools.lru_cache(maxsize=1)
def _r_exports() -> dict:
    """``{package: [exported fn, ...]}`` mined from the R NAMESPACE files.

    Cached loader of the committed ``r_exports.yaml``; empty dict if absent so the
    parity table is a silent no-op rather than a hard failure on a fresh checkout."""
    if not _R_EXPORTS_FILE.exists():
        return {}
    import yaml

    return yaml.safe_load(_R_EXPORTS_FILE.read_text(encoding="utf-8")) or {}


@functools.lru_cache(maxsize=1)
def _r_parity_aliases() -> dict:
    """``{league: {python_fn: r_fn}}`` curated cross-name equivalents (committed)."""
    if not _R_PARITY_ALIASES_FILE.exists():
        return {}
    import yaml

    return yaml.safe_load(_R_PARITY_ALIASES_FILE.read_text(encoding="utf-8")) or {}


def _r_parity_rows(prefix: str, ref_pages: dict, autodoc_names: list[str]) -> list[dict]:
    """``[{py, py_url, r, r_url}]`` linking each league function to its R equivalent.

    ``ref_pages`` is ``{slug: rendered_markdown}`` for the league's reference pages
    (used to find each endpoint/loader function's anchor); ``autodoc_names`` are the
    hand-written functions documented on ``reference/additional``. A row is emitted
    only when an R equivalent exists (curated alias first, else same-named export),
    so every link resolves. Sorted by Python function name."""
    pkg = _R_PARITY_PACKAGE.get(prefix)
    base = _R_PKGDOWN_BASE.get(pkg or "")
    if not pkg or not base:
        return []
    r_exports = set(_r_exports().get(pkg, []))
    aliases = _r_parity_aliases().get(prefix, {})

    # function name -> reference-page slug (for the Python doc link anchor)
    name_slug: dict[str, str] = {}
    for slug, content in ref_pages.items():
        for m in _DOC_L2_FN.finditer(content):
            name_slug.setdefault(m.group(1), slug)
    for name in autodoc_names:
        name_slug.setdefault(name, "additional")

    rows = []
    for name in sorted(name_slug):
        r_fn = aliases.get(name) or (name if name in r_exports else None)
        if not r_fn:
            continue
        rows.append(
            {
                "py": name,
                "py_url": f"reference/{name_slug[name]}#{name}",
                "r": r_fn,
                "r_url": f"{base}/{r_fn}.html",
            },
        )
    return rows


@functools.lru_cache(maxsize=None)
def _return_table(schema_name: str | None, league: str | None = None) -> str:
    """Markdown ``@return`` table(s) for a ``returns_schema``.

    Resolution order:
    1. ``schemas/{schema_name}/{league}.yaml`` -- per-league file (when ``league`` is given).
    2. ``schemas/{schema_name}.yaml`` -- generic fallback.

    Handles both on-disk shapes: ``kind: dataframe`` (top-level ``columns``) renders
    one table; ``kind: frames`` (``frames: [{section, columns}]``) renders one bolded
    table per non-empty sub-frame. Empty sub-frames (zero columns) are silently skipped
    so sport-specific sections that are vacant for another sport produce no noise.
    Empty string when no schema is registered/found or all columns are absent.
    """
    if not schema_name:
        return ""
    import yaml

    base = ROOT / "tools" / "codegen" / "schemas"
    p = (base / schema_name / f"{league}.yaml") if league else None
    if not (p and p.exists()):
        p = base / f"{schema_name}.yaml"  # fallback: generic
    if not p.exists():
        return ""
    d = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

    def tbl(cols) -> str:
        head = "| col_name | type | description |\n|---|---|---|\n"
        return head + "".join(
            f"| `{c['name']}` | {c.get('type', '')} | "
            f"{_table_cell_desc(c.get('description', ''), league, c.get('name', ''), d.get('schema'))} |\n"
            for c in cols
        )

    if d.get("kind") == "frames":
        blocks = [blk for blk in d.get("frames", []) if blk.get("columns")]
        return "\n".join(f"**{blk['section']}**\n\n{tbl(blk['columns'])}" for blk in blocks)
    cols = d.get("columns", [])
    return tbl(cols) if cols else ""


class _EndpointView:
    """Template-facing view of an Endpoint with computed render fields.

    Path handling has three shapes:
    - **No path params**: ``url_literal`` is a plain ``"https://..."`` string literal.
    - **Simple path params** (just ``{token}`` substitution): ``url_literal`` is an
      ``f"https://...{token}"`` literal; ``has_dynamic_path`` stays False.
    - **Dynamic** (``optional_segment`` / ``now_variant`` / ``default_from`` / ``transform``):
      ``has_dynamic_path`` is True, ``path_build_expr`` holds multi-line statements that
      assign ``__url``, and ``url_literal`` is the bare name ``__url``.
    """

    def __init__(
        self,
        ep: spec.Endpoint,
        fn_name: str,
        ep_host: str,
        league: spec.League,
        flat: bool = False,
        auth: bool = False,
    ):
        self.fn_name = fn_name
        self.short = ep.short
        self.summary = _normalize_rst(ep.summary or "")
        self.query_params = ep.query_params
        self.path_params = ep.path_params
        self.parser = ep.parser
        self.path = ep.path
        self.host_url = ep_host
        self.example_args = ep.example_args

        # signature order: required path (no default_from) -> required query ->
        # optional path -> optional query.
        req_path = [p for p in ep.path_params if p.required and p.default_from is None]
        opt_path = [p for p in ep.path_params if not p.required or p.default_from is not None]
        req_q = [p for p in ep.query_params if p.required]
        opt_q = [p for p in ep.query_params if not p.required]
        self.signature_params = req_path + req_q + opt_path + opt_q

        self.league_param = league.league_param
        # In param mode, keep {league} as a runtime f-string token (sport still baked).
        lg_slug = "{league}" if league.league_param else league.league
        bare = ep.path.replace("[", "").replace("]", "")
        url = f"{ep_host}{_sub_slugs(bare, league.sport, lg_slug)}"
        self.url_fstring = url
        self.full_url = url  # back-compat alias

        needs_build = (
            ep.now_variant is not None
            or any(p.optional_segment for p in ep.path_params)
            or any(p.default_from for p in ep.path_params)
            or any(p.transform for p in ep.path_params)
        )
        self.has_dynamic_path = needs_build
        if needs_build:
            self.path_build_expr = self._build_path_expr(ep, ep_host, league)
            self.url_literal = "__url"
        else:
            self.path_build_expr = ""
            # f-string whenever a runtime token is present: an endpoint path param OR
            # (param mode) the {league} token.
            needs_fstring = bool(ep.path_params) or league.league_param
            self.url_literal = ('f"' + url + '"') if needs_fstring else ('"' + url + '"')

        self.example_url = _example_url(ep_host, ep, league.sport, league.league)
        self.example_call = _example_call(ep, fn_name)
        self.docstring = _build_docstring(
            ep,
            league.sport,
            league.league,
            ep_host,
            self.example_url,
            self.example_call,
            flat=flat,
            auth=auth,
            league_param=league.league_param,
        )

        # ---- docs-rendering fields (consumed by _reference_block.jinja) ----
        # Every attribute below is read under StrictUndefined, so all must exist.
        self.endpoint_url = f"{ep_host}{_sub_slugs(ep.path, league.sport, league.league)}"
        self.valid_url = self.example_url
        self.param_rows = _param_rows(ep, league.prefix, fn_name)
        self.return_table = _return_table(ep.returns_schema, league.prefix)
        self.returns_prose = _normalize_rst(ep.summary or "")
        self.r_equivalent: dict[str, str] = {}  # reserved for a future R cross-ref map
        self.notebook: str | None = None
        self.last_validated: str | None = None
        self.api_name = ""  # set by _espn_league_views for per-API docs filtering

    @staticmethod
    def _build_path_expr(ep: spec.Endpoint, ep_host: str, league: spec.League) -> str:
        """Emit Python statements (newline+4-space joined) that assign ``__url``."""
        sport = league.sport
        lg = "{league}" if league.league_param else league.league
        lines: list[str] = []
        for p in ep.path_params:
            if p.default_from:
                lines.append(
                    f"{p.python_name} = {p.python_name} if {p.python_name} is not None else {p.default_from}",
                )
            if p.transform:
                lines.append(f"{p.python_name} = {p.transform}({p.python_name})")
        if "[" in ep.path:
            head, rest = ep.path.split("[", 1)
            seg, tail = rest.split("]", 1)  # seg e.g. "/{stat_type}" or "/groups/{group_id}"; tail may be ""
            seg_param = _path_token_first(seg)
            head_f = _sub_slugs(head, sport, lg)
            seg_f = _sub_slugs(seg, sport, lg)
            tail_f = _sub_slugs(tail, sport, lg)
            lines.append(f'__seg = f"{seg_f}" if {seg_param} is not None else ""')
            url_expr = f'f"{ep_host}{head_f}" + __seg'
            if tail_f:
                url_expr += f' + f"{tail_f}"'
            lines.append(f"__url = {url_expr}")
        elif ep.now_variant:
            # toggle = explicit now_toggle, else the first None-default path param,
            # else the last path param (back-compat).
            toggle = ep.now_toggle
            if toggle is None:
                none_default = [p.python_name for p in ep.path_params if not p.required and p.default is None]
                toggle = none_default[0] if none_default else ep.path_params[-1].python_name
            now_f = ep_host + _sub_slugs(ep.now_variant, sport, lg)
            full_f = ep_host + _sub_slugs(ep.path, sport, lg)
            # Emit the now-variant as an f-string only when it still has path-param
            # placeholders to interpolate (e.g. /club-schedule-season/{team}/now);
            # placeholder-free now-variants (e.g. /score/now) stay plain literals so
            # ruff F541 doesn't strip the prefix.
            now_literal = f'f"{now_f}"' if "{" in now_f else f'"{now_f}"'
            lines.append(f'__url = {now_literal} if {toggle} is None else f"{full_f}"')
        else:
            full_f = _sub_slugs(ep.path, sport, lg)
            lines.append(f'__url = f"{ep_host}{full_f}"')
        return "\n    ".join(lines)


def _ruff_format_dir(path: Path) -> None:
    """Format every ``.py`` under ``path`` with ruff *file-based*, so the repo's
    ``[tool.ruff.format]`` config (line-length=120, etc.) is discovered and applied --
    byte-for-byte identical to the pre-commit ruff hook. (Stdin formatting ignores the
    per-directory config and diverges from the hook.) No-op if ruff isn't installed.
    """
    try:
        subprocess.run([*_RUFF, "format", str(path)], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        pass


_ESPN_RENAME_FILE = ROOT / "tools" / "codegen" / "espn_rename_map.yaml"
_ESPN_RENAME_SKIPPED: dict[str, str] = {}  # old -> reason (collisions held back from the curated map)


def _load_espn_renames() -> dict:
    """Approved generated-name -> R-aligned-name overrides (espn_rename_map.yaml)."""
    if not _ESPN_RENAME_FILE.exists():
        return {}
    import yaml

    return yaml.safe_load(_ESPN_RENAME_FILE.read_text(encoding="utf-8")).get("rename", {}) or {}


def _load_espn_drops() -> set:
    """Generated full names NOT to emit (a hand-written sibling exposes the same endpoint)."""
    if not _ESPN_RENAME_FILE.exists():
        return set()
    import yaml

    return set(yaml.safe_load(_ESPN_RENAME_FILE.read_text(encoding="utf-8")).get("drop", []) or [])


_CONVENTION_TOKENS: dict[str, str] = {
    "athlete": "player",
    "athletes": "players",
    "event": "game",
    "events": "games",
}


def _convention_rename(short: str) -> str:
    """Universal ESPN-endpoint -> R-curated structural rename (all leagues).

    Aligns the raw ESPN endpoint taxonomy to the cfbfastR/hoopR/wehoop convention:
    an athlete is a player, an event is a game, a competitor is a game's team, a
    competition is a game competition. Applied to EVERY league.

    Two combined (token-merging) mappings run first, because they can't be
    expressed as a per-token swap:

    * ``event_competitor[s][...]`` -> ``game_team[s][...]`` (a competitor is the
      game's team)
    * ``event_competition`` -> ``game_competition`` and ``event_competition_X``
      -> ``game_X`` (the competition object / its flattened sub-resources)

    Everything else is a per-underscore-token swap (``athlete``->``player``,
    ``event``->``game``, plus plurals), so embedded / trailing / plural forms all
    convert: ``athlete_vs_athlete`` -> ``player_vs_player``, ``athletes_index`` ->
    ``players_index``, ``season_athletes`` -> ``season_players``, bare ``event`` ->
    ``game``, ``events`` -> ``games``, ``season_week_events`` ->
    ``season_week_games``. Compound tokens like ``eventlog`` are not bare tokens,
    so they are preserved (``athlete_eventlog`` -> ``player_eventlog``).
    """
    if short.startswith("event_competitor"):
        short = "game_team" + short[len("event_competitor") :]
    elif short.startswith("event_competition_"):
        short = "game_" + short[len("event_competition_") :]
    elif short == "event_competition":
        short = "game_competition"
    return "_".join(_CONVENTION_TOKENS.get(tok, tok) for tok in short.split("_"))


# Endpoints whose convention name should be version-qualified (rather than dropped)
# when it collides with an existing/hand-written bare sibling. The web-common-v3
# /athletes/{id}/stats is the comprehensive "v3" payload, so it sits as
# ``player_stats_v3`` alongside a bare ``player_stats`` -- but only WHEN that bare
# name is taken. When no bare sibling exists (most leagues) it keeps the bare
# ``player_stats`` name, so a lone endpoint is never left orphaned as ``*_v3``.
_ESPN_COLLISION_VERSIONED: dict[str, str] = {
    "athlete_stats": "player_stats_v3",
}

# Per-sport parser-name overrides: when rendering a league of sport S, endpoint short K
# has its baked parser swapped to the sport-specific parser.  The parser is imported
# from the sport's own parser module (see _SPORT_PARSER_MODULE) to avoid a circular
# import through sportsdataverse._common_espn_parsers.
_SPORT_PARSER_OVERRIDES: dict[str, dict[str, str]] = {
    "soccer": {
        "scoreboard": "parse_soccer_scoreboard",
        "standings": "parse_soccer_standings",
        "summary": "parse_soccer_summary",
        "teams_site": "parse_soccer_teams",
        "team_roster": "parse_soccer_team_roster",
    },
    "cricket": {
        "scoreboard": "parse_cricket_scoreboard",
        "standings": "parse_cricket_standings",
        "summary": "parse_cricket_summary",
    },
}

# Maps sport slug -> dotted import path for that sport's parser module.
# Used by _league_module_source to emit a second import line for sport-specific parsers.
_SPORT_PARSER_MODULE: dict[str, str] = {
    "soccer": "sportsdataverse.soccer.soccer_espn_parsers",
    "cricket": "sportsdataverse.cricket.cricket_espn_parsers",
}


def _versioned_on_collision(short: str, prefix: str) -> str | None:
    """Version-qualified ``espn_<prefix>_*`` name for ``short`` on collision, else None."""
    suffix = _ESPN_COLLISION_VERSIONED.get(short)
    return f"espn_{prefix}_{suffix}" if suffix else None


def _handwritten_espn_names(prefix: str) -> set[str]:
    """``espn_<prefix>_*`` public names NOT provided by the generated *_espn_ext module.

    Resolves each public name on the package and keeps those whose ``__module__`` is
    a sibling (or unknown, e.g. decorated/partial) -- robust to decorators that the
    per-module ``vars()`` scan misses (e.g. cached ``espn_cfb_teams``)."""
    import importlib

    try:
        pkg = importlib.import_module(f"sportsdataverse.{prefix}")
    except Exception:
        return set()
    out: set[str] = set()
    for n in dir(pkg):
        if not n.startswith(f"espn_{prefix}_"):
            continue
        mod = getattr(getattr(pkg, n, None), "__module__", "") or ""
        if not mod.endswith("_espn_ext"):
            out.add(n)
    return out


def _espn_league_views(league: spec.League, apis, hosts) -> list[_EndpointView]:
    """Resolve every in-scope ESPN endpoint for a league to its final wrapper name.

    Single source of truth for the generated name of each endpoint, shared by the
    module renderer (:func:`_league_module_source`) and the docs renderer
    (:func:`render_reference_page`) so a doc page can never disagree with the emitted
    wrapper. Each returned view is tagged with ``.api_name`` (the source API) for
    per-API docs filtering.

    Pass 1 collects in-scope endpoints + their base (pre-rename) names. Pass 2 applies
    the R-alignment renames with a collision guard (version-qualify the newer endpoint
    so one stays bare; record a skip only when even the versioned name is taken).
    """
    renames = _load_espn_renames()
    drops = _load_espn_drops()
    handwritten = _handwritten_espn_names(league.prefix)
    collected = []  # (ep, ep_host, base, api_name)
    for api in apis:
        host_url = hosts[api.host]
        for ep in api.endpoints:
            if ep.scope not in league.scopes or league.league in ep.exclude_leagues:
                continue
            base = api.name_pattern.format(prefix=league.prefix, short=ep.short)
            if base in drops:
                continue  # a hand-written sibling already exposes this exact endpoint
            ep_host = hosts[ep.host] if ep.host else host_url
            collected.append((ep, ep_host, base, api.api))
    base_names = {b for _, _, b, _ in collected}

    views: list[_EndpointView] = []
    used: set[str] = set()
    for ep, ep_host, base, api_name in collected:
        fn_name = base
        # static override wins; else the universal structural convention
        new = renames.get(base) or f"espn_{league.prefix}_{_convention_rename(ep.short)}"
        if new != base:
            if new in base_names or new in handwritten or new in used:
                versioned = _versioned_on_collision(ep.short, league.prefix)
                if versioned and versioned not in base_names and versioned not in handwritten and versioned not in used:
                    fn_name = versioned
                else:
                    _ESPN_RENAME_SKIPPED[base] = f"{new} (collision)"
            else:
                fn_name = new
        used.add(fn_name)
        view = _EndpointView(ep, fn_name, ep_host, league)
        view.api_name = api_name
        views.append(view)
    overrides = _SPORT_PARSER_OVERRIDES.get(league.sport, {})
    if overrides:
        for v in views:
            if v.short in overrides:
                v.parser = overrides[v.short]
    return views


def _league_module_source(league: spec.League, apis, hosts) -> str:
    """Render the (unformatted) module source; ruff formatting happens at write time."""
    views = _espn_league_views(league, apis, hosts)
    all_parser_names = {v.parser for v in views if v.parser}
    # Split parsers: sport-specific ones are imported from their own module (to avoid a
    # circular import through _common_espn_parsers); the rest come from _common_espn_parsers.
    sport_override_names = set((_SPORT_PARSER_OVERRIDES.get(league.sport) or {}).values())
    sport_parser_imports = sorted(all_parser_names & sport_override_names)
    common_parser_imports = sorted(all_parser_names - sport_override_names)
    sport_parser_module = _SPORT_PARSER_MODULE.get(league.sport, "") if sport_parser_imports else ""
    transforms: set[str] = set()
    for v in views:
        for p in (*v.path_params, *v.query_params):
            if p.transform:
                transforms.add(p.transform)
    runtime_imports = ["_get"] + sorted(transforms)
    template = render.ENV.get_template("espn_league_module.py.jinja")
    return template.render(
        prefix=league.prefix,
        sport=league.sport,
        league=league.league,
        endpoints=views,
        parser_imports=common_parser_imports,
        sport_parser_imports=sport_parser_imports,
        sport_parser_module=sport_parser_module,
        runtime_imports=runtime_imports,
    )


_FLAT_STUB_LEAGUE = spec.League(prefix="", sport="", league="", scopes=[])


def reserved_names(prefix: str, exclude_modules: tuple[str, ...] = ()) -> set[str]:
    """Public names already defined in ``sportsdataverse.{prefix}`` (hand-written
    composites, loaders, and submodule attributes) that a generated flat-API
    function must not shadow. Returns an empty set if the package can't import.

    ``exclude_modules`` drops names whose ``__module__`` ends with one of the
    given module names -- used so a flat module being regenerated does not treat
    its own (about-to-be-replaced) names as reserved (the bootstrapping case)."""
    import importlib

    try:
        mod = importlib.import_module(f"sportsdataverse.{prefix}")
    except Exception:
        return set()
    out: set[str] = set()
    for n in dir(mod):
        if n.startswith("_"):
            continue
        if exclude_modules:
            m = getattr(getattr(mod, n, None), "__module__", "") or ""
            if any(m.endswith(ex) for ex in exclude_modules):
                continue
        out.add(n)
    return out


def resolve_name(prefix: str, short: str, reserved: set, qualifier: str) -> str:
    """Clean ``{prefix}_{short}`` unless reserved, else ``{prefix}_{qualifier}_{short}``."""
    clean = f"{prefix}_{short}"
    if clean not in reserved:
        return clean
    return f"{prefix}_{qualifier}_{short}"


def _flat_views(api: spec.FlatApi, league_prefix: str = "") -> list[_EndpointView]:
    """Resolve a flat API's endpoints to their final wrapper names + views.

    Shared by the module renderer (:func:`render_flat_module`) and the docs renderer
    (:func:`render_reference_page`). When ``api.qualifier`` is set, each function gets
    a clean ``{prefix}_{short}`` name, qualified to ``{prefix}_{qualifier}_{short}``
    only on collision with a hand-written composite or another generated name.

    ``league_prefix`` is the *importable* league prefix under ``sportsdataverse``
    (e.g. ``"mlb"`` for the MLB Stats API).  When provided, docstring-based
    descriptions are resolved for each wrapper; when omitted the name-pattern
    prefix (``api.prefix``, which may be ``"mlb_api"`` etc.) is used as a
    best-effort fallback.
    """
    reserved = reserved_names(api.prefix, exclude_modules=(api.module,)) if api.qualifier else set()
    used: set[str] = set()
    views: list[_EndpointView] = []
    # Build a stub League that carries the real importable prefix so _EndpointView
    # can look up docstring descriptions via _docstring_param_descs.
    effective_prefix = league_prefix or api.prefix
    stub_league = spec.League(prefix=effective_prefix, sport="", league="", scopes=[])
    for ep in api.endpoints:
        if api.qualifier:
            fn_name = resolve_name(api.prefix, ep.short, reserved | used, api.qualifier)
        else:
            fn_name = api.name_pattern.format(short=ep.short)
        used.add(fn_name)
        ep_host = ep.host or api.host
        views.append(_EndpointView(ep, fn_name, ep_host, stub_league, flat=True, auth=api.auth))
    return views


def render_flat_module(api: spec.FlatApi, league_prefix: str = "") -> str:
    """Render a flat (non-sport/league) API module (NHL api-web/edge/..., MLB stats)."""
    views = _flat_views(api, league_prefix=league_prefix)
    parser_imports = {v.parser for v in views if v.parser}
    transforms: set[str] = set()
    for v in views:
        for p in (*v.path_params, *v.query_params):
            if p.transform:
                transforms.add(p.transform)
    # Also count transforms that appear in dynamic path build expressions
    # (e.g. format_nhl_season used in path_build_expr assignments).
    for v in views:
        if v.path_build_expr:
            for rt in api.runtime_imports:
                if rt != "_get" and rt in v.path_build_expr:
                    transforms.add(rt)
    # Always include _get; include YAML-declared extras only when actually used.
    runtime_imports = list(dict.fromkeys(["_get", *sorted(transforms)]))
    template = render.ENV.get_template("api_module.py.jinja")
    return template.render(
        api=api.api,
        host=api.host,
        module=api.module,
        parser_module=api.parser_module,
        endpoints=views,
        parser_imports=sorted(parser_imports),
        runtime_imports=runtime_imports,
        passthrough_query=api.passthrough_query,
        getter_module=api.getter_module,
        auth=api.auth,
    )


@functools.lru_cache(maxsize=1)
def _loader_schemas() -> dict:
    """``{fn: [{name, type}, ...]}`` introspected from the release parquet footers
    (tools/codegen/schemas/loader_schemas.yaml). Empty dict if absent."""
    p = ENDPOINTS.parent / "schemas" / "loader_schemas.yaml"
    if not p.exists():
        return {}
    import yaml

    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _build_loader_docstring(ld: spec.Loader) -> str:
    """4-space-indented docstring block for a generated dataset loader.

    Renders an ``@return``-style column table (name + polars dtype) when a
    schema was introspected for ``ld.fn``, so the generated docstrings carry the
    same column documentation the hand-written loaders lacked."""
    lines = [f'"""Load {ld.tag} (sportsdataverse-data release).', ""]
    lines.append(f"Source: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/{ld.tag}")
    lines.append("")
    lines.append("Args:")
    rng = f" (>= {ld.min_season})" if ld.min_season else ""
    lines.append(f"    seasons: an int or iterable of seasons{rng}.")
    lines.append("    return_as_pandas: return a pandas DataFrame instead of polars.")
    lines.append("")
    lines.append("Returns:")
    lines.append("    A polars (or pandas) DataFrame; seasons with no published asset are")
    lines.append("    skipped with a warning rather than raising (404-safe).")
    cols = _loader_schemas().get(ld.fn) or []
    if cols:
        width = max([len("col_name")] + [len(c["name"]) for c in cols])
        twidth = max([len("type")] + [len(c["type"]) for c in cols])
        lines.append("")
        lines.append(f"    |{'col_name'.ljust(width)} |{'type'.ljust(twidth)} |")
        lines.append(f"    |:{'-' * width}|:{'-' * twidth}|")
        for c in cols:
            lines.append(f"    |{c['name'].ljust(width)} |{c['type'].ljust(twidth)} |")
    lines.append("")
    lines.append("Example:")
    lines.append("    Quick start::")
    lines.append("")
    lines.append(f"        {ld.fn}(seasons={ld.example_args.get('seasons', 2024)!r})")
    lines.append('"""')
    return "\n".join(("    " + ln) if ln else "" for ln in lines)


class _LoaderView:
    """Template-facing view of a Loader (absolute URL + docstring)."""

    def __init__(self, ld: spec.Loader, bases: dict):
        self.fn = ld.fn
        self.tag = ld.tag
        self.min_season = ld.min_season
        self.example_args = ld.example_args
        self.stub = ld.stub
        self.stub_message = ld.stub_message
        self.abs_url = "" if ld.stub else f"{bases[ld.base]}{ld.url}"
        self.docstring = _build_loader_docstring(ld)


def render_loader_module(league: str, loaders, bases: dict) -> str:
    """Render a league's 404-safe dataset-loader module."""
    views = [_LoaderView(ld, bases) for ld in loaders]
    template = render.ENV.get_template("load_module.py.jinja")
    return template.render(league=league, loaders=views)


# Leagues whose {league}_loaders.py is GENERATED from releases.yaml. All season-loop
# loaders are generated; season-less loaders + module helpers that the loader template
# can't express are preserved hand-written in {league}_loaders_extra.py residuals
# (cfb: load_cfb_betting_lines + get_cfb_teams; nhl: nhl_teams).
_GENERATED_LOADER_LEAGUES = {"cfb", "mbb", "nba", "nhl", "pwhl", "wbb", "wnba"}


def _render_loaders_all() -> dict[str, str]:
    """{league: src} for each generated-loader league with manifest entries."""
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    out: dict[str, str] = {}
    for lg in sorted(_GENERATED_LOADER_LEAGUES):
        loaders = [ld for ld in rel.loaders if ld.league == lg]
        if loaders:
            out[lg] = render_loader_module(lg, loaders, rel.bases)
    return out


def build_loaders_live() -> list[Path]:
    """Write generated {league}_loaders.py modules + wire each league __init__."""
    written = []
    for lg, src in _render_loaders_all().items():
        pkg = LIVE / lg
        pkg.mkdir(parents=True, exist_ok=True)
        dest = pkg / f"{lg}_loaders.py"
        dest.write_text(src, encoding="utf-8", newline="\n")
        init = pkg / "__init__.py"
        line = f"from sportsdataverse.{lg}.{lg}_loaders import *\n"
        if not init.exists():
            init.write_text(
                f'"""sportsdataverse.{lg} -- {lg.upper()} data loaders."""\n\nfrom __future__ import annotations\n\n'
                + line,
                encoding="utf-8",
                newline="\n",
            )
        elif line not in init.read_text(encoding="utf-8"):
            init.write_text(init.read_text(encoding="utf-8").rstrip() + "\n" + line, encoding="utf-8", newline="\n")
        written.append(dest)
    if written:
        subprocess.run([*_RUFF, "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
    return sorted(written)


def _loaders_stale() -> list[str]:
    """Live generated {league}_loaders.py files that differ from a fresh render."""
    tmp = OUT / "_check_loaders_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        rendered = _render_loaders_all()
        for lg, src in rendered.items():
            (tmp / f"{lg}_loaders.py").write_text(src, encoding="utf-8", newline="\n")
        _ruff_format_dir(tmp)
        stale = []
        for lg in rendered:
            live_file = LIVE / lg / f"{lg}_loaders.py"
            if not live_file.exists() or live_file.read_text(encoding="utf-8") != (tmp / f"{lg}_loaders.py").read_text(
                encoding="utf-8",
            ):
                stale.append(str(live_file.relative_to(ROOT)))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return stale


# Legacy / display-name release tags that are not data-loader datasets (old ESPN
# display tags carry spaces; cfbfastR_cfb_pbp is the pre-cutover CFB pbp tag).
def _is_loader_tag(tag: str) -> bool:
    return " " not in tag and tag != "cfbfastR_cfb_pbp" and not tag.startswith("ESPN ")


def gh_release_tags(repo: str = "sportsdataverse/sportsdataverse-data", limit: int = 400) -> list[str]:
    """Live release tags for ``repo`` via the ``gh`` CLI (network).

    Defined here (not in the legacy runtime-capture ``extract.py``, which is
    import-broken post-factory-retirement) so the audit has no dead dependency.
    Raises ``subprocess.CalledProcessError`` if ``gh`` is missing/unauthenticated."""
    out = subprocess.run(
        ["gh", "release", "list", "-R", repo, "--limit", str(limit)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return _parse_release_tags(out)


def _parse_release_tags(text: str) -> list[str]:
    """Parse ``gh release list`` tab output -> sorted unique release **tags**.

    The columns are ``TITLE\\tTYPE\\tTAG\\tPUBLISHED``, so the tag is the
    second-to-last field. Reading the FIRST field grabs the *title* instead, which
    silently diverges whenever a release has a human-friendly title (e.g. title
    "ESPN NBA Draft" vs tag ``espn_nba_draft``) — making the audit falsely report
    that loader's tag as a dead release.
    """
    tags: set[str] = set()
    for line in text.splitlines():
        parts = line.split("\t")
        # gh release list columns: TITLE \t TYPE \t TAG \t PUBLISHED. Require the
        # full 4-column shape and read the explicit TAG column (index 2), so a
        # blank/truncated row can't misclassify TITLE or TYPE as a tag.
        if len(parts) >= 4 and parts[2].strip():
            tags.add(parts[2].strip())
    return sorted(tags)


# returns_schema name -> (parser callable, "frames" | "dataframe").
# parse_summary returns a {section: frame} dict; the rest return one frame.
def _return_schema_parsers():
    from sportsdataverse import _common_espn_parsers as P

    return {
        "scoreboard": (P.parse_scoreboard, "dataframe"),
        "teams": (P.parse_teams, "dataframe"),
        "team_roster": (P.parse_team_roster, "dataframe"),
        "standings": (P.parse_standings, "dataframe"),
        "leaders": (P.parse_leaders, "dataframe"),
        "summary": (P.parse_summary, "frames"),
        # deeper-tree endpoints (stable per-parser shapes; captured fixtures)
        "team_schedule": (P.parse_team_schedule, "dataframe"),
        "news": (P.parse_news, "dataframe"),
        "injuries": (P.parse_injuries, "dataframe"),
    }


def _desc_lookup(schema_name: str) -> dict:
    """{column_name: description} from the hand-curated generic schema, used to
    annotate the introspected per-league columns (which carry only name+type)."""
    import yaml

    p = ROOT / "tools" / "codegen" / "schemas" / f"{schema_name}.yaml"
    if not p.exists():
        return {}
    d = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    out = {}
    if d.get("kind") == "frames":
        for blk in d.get("frames", []):
            for c in blk.get("columns", []):
                out[c["name"]] = c.get("description", "")
    else:
        for c in d.get("columns", []):
            out[c["name"]] = c.get("description", "")
    return out


def _pl_to_doc_type(pl_type: str) -> str:
    s = pl_type.lower()
    if "int" in s:
        return "integer"
    if "float" in s:
        return "double"
    if "bool" in s:
        return "logical"
    return "character"


def _cols_from_frame(df, descs: dict) -> list:
    return [
        {"name": n, "type": _pl_to_doc_type(str(t)), "description": descs.get(n, "")}
        for n, t in zip(df.columns, df.dtypes)
    ]


_LEAGUES = ["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"]
_FIX = ROOT / "tests" / "fixtures" / "espn"


# Sport-specific ESPN parsers (soccer/cricket) emit different column shapes than
# the generic cross-league parsers, so they get their own per-prefix schema files
# introspected from the nested tests/fixtures/espn/{sport}/{league}/site-v2/ captures
# (unioned across captured leagues). Column descriptions are NOT written here -- like
# every captured schema they carry name+type only and are filled at render time from
# manual_column_descriptions.yaml (keyed by the `schema:` field). See the
# returns-table-descriptions design.

# (sport, prefix, [leagues], {endpoint(schema_key): (parser_attr, kind)})
_SPORT_SPECIFIC_SCHEMAS = [
    (
        "soccer",
        "soccer",
        ["eng.1", "usa.1", "uefa.champions"],
        {
            "scoreboard": ("parse_soccer_scoreboard", "dataframe"),
            "standings": ("parse_soccer_standings", "dataframe"),
            "teams": ("parse_soccer_teams", "dataframe"),
            "team_roster": ("parse_soccer_team_roster", "dataframe"),
            "summary": ("parse_soccer_summary", "frames"),
        },
        "sportsdataverse.soccer.soccer_espn_parsers",
    ),
    (
        "cricket",
        "cricket",
        ["8048"],
        {
            "scoreboard": ("parse_cricket_scoreboard", "dataframe"),
            "standings": ("parse_cricket_standings", "dataframe"),
            "summary": ("parse_cricket_summary", "frames"),
        },
        "sportsdataverse.cricket.cricket_espn_parsers",
    ),
]


def _refresh_sport_specific_return_schemas() -> int:
    """Emit per-prefix schemas for the soccer/cricket sport-specific parsers,
    unioning columns/sections across the captured leagues so the schema is
    representative (e.g. uefa.champions adds the summary `shootout` section)."""
    import importlib
    import json

    import yaml

    def cols(df) -> list:
        return [{"name": n, "type": _pl_to_doc_type(str(t)), "description": ""} for n, t in zip(df.columns, df.dtypes)]

    written = 0
    for sport, prefix, leagues, endpoints, modpath in _SPORT_SPECIFIC_SCHEMAS:
        pmod = importlib.import_module(modpath)
        for schema_key, (parser_attr, kind) in endpoints.items():
            parser = getattr(pmod, parser_attr)
            payloads = []
            for lg in leagues:
                fx = _FIX / sport / lg / "site-v2" / f"{schema_key}.json"
                if fx.exists():
                    payloads.append(json.loads(fx.read_text(encoding="utf-8")))
            if not payloads:
                continue
            if kind == "frames":
                sections: dict = {}  # section -> {col_name: col_dict}
                for p in payloads:
                    for sec, df in parser(p).items():
                        bucket = sections.setdefault(sec, {})
                        for c in cols(df):
                            bucket.setdefault(c["name"], c)
                doc = {
                    "schema": schema_key,
                    "kind": "frames",
                    "frames": [{"section": s, "columns": list(c.values())} for s, c in sections.items()],
                }
            else:
                seen: dict = {}
                for p in payloads:
                    for c in cols(parser(p)):
                        seen.setdefault(c["name"], c)
                doc = {"schema": schema_key, "kind": "dataframe", "columns": list(seen.values())}
            outdir = ROOT / "tools" / "codegen" / "schemas" / schema_key
            outdir.mkdir(parents=True, exist_ok=True)
            (outdir / f"{prefix}.yaml").write_text(
                yaml.safe_dump(doc, sort_keys=False, width=120, allow_unicode=True),
                encoding="utf-8",
                newline="\n",
            )
            written += 1
    return written


def refresh_return_schemas() -> int:
    """Run each parser on every per-league captured fixture and emit per-league
    column schemas under schemas/{name}/{league}.yaml. Descriptions are merged
    from the hand-curated generic schemas/{name}.yaml by column name."""
    import json

    import yaml

    written = skipped = 0
    for name, (parser, kind) in _return_schema_parsers().items():
        descs = _desc_lookup(name)
        outdir = ROOT / "tools" / "codegen" / "schemas" / name
        for league in _LEAGUES:
            fx = _FIX / f"{name}_{league}.json"
            if not fx.exists():
                skipped += 1
                continue
            payload = json.loads(fx.read_text(encoding="utf-8"))
            outdir.mkdir(parents=True, exist_ok=True)
            if kind == "frames":
                frames = parser(payload)  # {section: df}
                doc = {
                    "schema": name,
                    "kind": "frames",
                    "frames": [{"section": sec, "columns": _cols_from_frame(df, descs)} for sec, df in frames.items()],
                }
            else:
                df = parser(payload)
                cols = _cols_from_frame(df, descs)
                # Skip 0-column captures: an empty per-league schema file would
                # shadow (and suppress) the generic schemas/{name}.yaml fallback,
                # leaving the league with NO return table. No file => generic applies.
                if not cols:
                    skipped += 1
                    continue
                doc = {"schema": name, "kind": "dataframe", "columns": cols}
            (outdir / f"{league}.yaml").write_text(
                yaml.safe_dump(doc, sort_keys=False, width=120), encoding="utf-8", newline="\n"
            )
            written += 1

    # --- natives ---
    import importlib

    nat = yaml.safe_load((ROOT / "tools" / "codegen" / "native_fixture_map.yaml").read_text("utf-8")) or {}
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    for api, files in nat.items():
        fa = spec.load_flat_api(ENDPOINTS / f"{api}.yaml", params)
        by_short = {e.short: e for e in fa.endpoints}
        pmod = importlib.import_module(f"sportsdataverse.{fa.parser_module}")
        for fname, short in files.items():
            ep = by_short.get(short)
            if ep is None or not ep.parser:
                continue
            try:
                payload = json.loads((ROOT / "tests" / "fixtures" / api / fname).read_text("utf-8"))
                df = getattr(pmod, ep.parser)(payload)
                doc = {"schema": short, "kind": "dataframe", "columns": _cols_from_frame(df, {})}
            except Exception as e:  # noqa: BLE001
                print(f"  native skip {api}/{short} ({fname}): {e}")
                continue
            outdir = ROOT / "tools" / "codegen" / "schemas" / "native" / api
            outdir.mkdir(parents=True, exist_ok=True)
            (outdir / f"{short}.yaml").write_text(
                yaml.safe_dump(doc, sort_keys=False, width=120), encoding="utf-8", newline="\n"
            )
            written += 1

    # --- sport-specific ESPN parsers (soccer/cricket) ---
    sport_written = _refresh_sport_specific_return_schemas()
    written += sport_written

    print(f"return schemas: {written} written ({sport_written} sport-specific), {skipped} skipped (no fixture)")
    return 0


def refresh_loader_schemas() -> int:
    """Re-introspect every non-stub loader's release parquet footer and rewrite
    tools/codegen/schemas/loader_schemas.yaml (network; reads metadata only)."""
    import polars as pl
    import yaml

    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    out: dict = {}
    failed = []
    for ld in rel.loaders:
        if ld.stub:
            continue
        seasons = [ld.min_season or 2024, 2023, 2024, 2022, 2021]
        got = None
        for s in dict.fromkeys(seasons):
            try:
                sch = pl.read_parquet_schema(f"{rel.bases[ld.base]}{ld.url}".replace("{season}", str(s)))
                got = [{"name": k, "type": str(v)} for k, v in sch.items()]
                break
            except Exception:  # noqa: BLE001
                continue
        if got is None:
            failed.append(ld.fn)
        else:
            out[ld.fn] = got
    dest = ENDPOINTS.parent / "schemas" / "loader_schemas.yaml"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(yaml.safe_dump(out, sort_keys=True, width=120), encoding="utf-8", newline="\n")
    _loader_schemas.cache_clear()
    print(f"loader schemas: {len(out)} introspected" + (f"; {len(failed)} failed: {failed}" if failed else ""))
    return 0


def audit_releases() -> int:
    """Compare the releases.yaml manifest against the LIVE sportsdataverse-data
    release list (network, via ``gh``). Reports release tags with no loader
    (gaps) and manifest tags no longer published (orphans). Informational drift
    gate -- meant for a CI job, not the offline ``--check``."""
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    # Only loaders that pull from the sportsdataverse-data *releases* host are
    # comparable to the live release list. ``raw_data``-based loaders (e.g. cfb/nhl
    # read raw.githubusercontent.com/sportsdataverse/<repo>) carry the source repo
    # name as their "tag" (cfbfastR-data, fastRhockey-data), not a release tag, so
    # they must be excluded from both sides of the comparison to avoid bogus orphans.
    manifest = {ld.tag for ld in rel.loaders if ld.base == "sdv_releases"}
    try:
        live = {t for t in gh_release_tags() if _is_loader_tag(t)}
    except Exception as e:  # noqa: BLE001
        print(f"--audit-releases: could not query gh release list ({e})", file=sys.stderr)
        return 2
    missing = sorted(live - manifest)
    orphan = sorted(manifest - live)
    # "missing" (a published release with no loader yet) is purely informational --
    # adding a loader is a deliberate choice, and many tags intentionally never get
    # one (no season-partitioned parquet). "orphan" (a manifest tag no longer
    # published) is real drift: a loader now points at a dead release. Only the
    # latter is treated as a failure; the former is reported for awareness.
    if missing:
        print(f"release manifest: {len(missing)} published tag(s) without a loader (informational):", file=sys.stderr)
        print("  " + ", ".join(missing), file=sys.stderr)
    if orphan:
        print(
            f"release manifest drift: {len(orphan)} orphan manifest tag(s) (loader -> dead release):", file=sys.stderr
        )
        print("  " + ", ".join(orphan), file=sys.stderr)
        return 1
    print(f"release manifest OK ({len(manifest)} loader tags; {len(missing)} unmapped published tags, no orphans)")
    return 0


_PARSED_LEAGUES = ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl")


def _league_public_callables(league: str):
    """Public sdv callables exported by ``sportsdataverse.{league}``.

    Skips re-exported third-party names (pandas/polars/typing) by requiring
    ``__module__`` to live under ``sportsdataverse``."""
    import importlib

    mod = importlib.import_module(f"sportsdataverse.{league}")
    out = []
    for name in sorted(dir(mod)):
        if name.startswith("_"):
            continue
        attr = getattr(mod, name)
        if not callable(attr):
            continue
        if not getattr(attr, "__module__", "").startswith("sportsdataverse"):
            continue
        out.append((name, attr))
    return out


def render_parsed_module(league: str) -> str:
    """Render the concrete ``sportsdataverse/parsed/{league}.py`` mirror.

    Functions whose signature accepts ``return_parsed`` get a thin wrapper that
    defaults it to True; every other public callable passes through unchanged.
    Replaces the runtime ``types.ModuleType`` builder with a real file."""
    import inspect

    parser_fns, passthrough_fns = [], []
    for name, fn in _league_public_callables(league):
        try:
            has_rp = "return_parsed" in inspect.signature(fn).parameters
        except (TypeError, ValueError):
            has_rp = False
        (parser_fns if has_rp else passthrough_fns).append(name)
    parser_fns.sort()
    passthrough_fns.sort()
    template = render.ENV.get_template("parsed_module.py.jinja")
    return template.render(
        league=league,
        parser_fns=parser_fns,
        passthrough_fns=passthrough_fns,
        all_fns=sorted(parser_fns + passthrough_fns),
    )


def _render_parsed_all() -> dict[str, str]:
    return {f"{lg}.py": render_parsed_module(lg) for lg in _PARSED_LEAGUES}


def build_parsed_live() -> list[Path]:
    """Write the 8 concrete ``parsed/{league}.py`` modules into the live package."""
    written = []
    pdir = LIVE / "parsed"
    for name, src in _render_parsed_all().items():
        dest = pdir / name
        dest.write_text(src, encoding="utf-8", newline="\n")
        written.append(dest)
    if written:
        subprocess.run([*_RUFF, "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
    return sorted(written)


# Flat (non-sport/league) API families: (yaml_stem, league_prefix). Only those
# whose YAML exists under tools/codegen/endpoints/ are generated, so the NHL/MLB
# native cutover can land family-by-family.
FLAT_APIS = [
    ("nhl_api_web", "nhl"),
    ("nhl_edge", "nhl"),
    ("nhl_stats_rest", "nhl"),
    ("nhl_records", "nhl"),
    ("mlb_api", "mlb"),
    ("mlb_statcast", "mlb"),
    ("nfl_api", "nfl"),
    ("nba_stats", "nba"),
    ("wnba_stats", "wnba"),
    ("on3", "cfb"),
    ("sports247", "cfb"),
    ("sports247_site_pages", "cfb"),
    ("pff", "nfl"),
]


def _render_flat_all() -> dict[str, tuple[str, str]]:
    """{module_name: (league_prefix, src)} for each flat-API YAML that exists."""
    out: dict[str, tuple[str, str]] = {}
    for stem, prefix in FLAT_APIS:
        y = ENDPOINTS / f"{stem}.yaml"
        if not y.exists():
            continue
        api = spec.load_flat_api(y, {})
        out[api.module] = (prefix, render_flat_module(api, league_prefix=prefix))
    return out


def build_flat_live() -> list[Path]:
    """Write generated flat-API modules (NHL native, MLB stats) into the live package."""
    written = []
    for module, (prefix, src) in _render_flat_all().items():
        dest = LIVE / prefix / f"{module}.py"
        dest.write_text(src, encoding="utf-8", newline="\n")
        written.append(dest)
    if written:
        subprocess.run([*_RUFF, "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
    return sorted(written)


def _flat_stale() -> list[str]:
    """Live flat-API modules that differ from a fresh render."""
    tmp = OUT / "_check_flat_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        rendered = _render_flat_all()
        prefixes = {}
        for module, (prefix, src) in rendered.items():
            (tmp / f"{module}.py").write_text(src, encoding="utf-8", newline="\n")
            prefixes[module] = prefix
        _ruff_format_dir(tmp)
        stale = []
        for module, prefix in prefixes.items():
            live_file = LIVE / prefix / f"{module}.py"
            if not live_file.exists() or live_file.read_text(encoding="utf-8") != (tmp / f"{module}.py").read_text(
                encoding="utf-8",
            ):
                stale.append(str(live_file.relative_to(ROOT)))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return stale


def _parsed_stale() -> list[str]:
    """Live ``parsed/{league}.py`` files that differ from a fresh render."""
    tmp = OUT / "_check_parsed_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        rendered = _render_parsed_all()
        for name, src in rendered.items():
            (tmp / name).write_text(src, encoding="utf-8", newline="\n")
        _ruff_format_dir(tmp)
        stale = []
        for name in rendered:
            live_file = LIVE / "parsed" / name
            if not live_file.exists() or live_file.read_text(encoding="utf-8") != (tmp / name).read_text(
                encoding="utf-8",
            ):
                stale.append(str(live_file.relative_to(ROOT)))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return stale


def _render_all() -> dict[str, str]:
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    apis = [spec.load_espn_api(ENDPOINTS / f"{a}.yaml", params) for a in ESPN_APIS]
    return {f"{lg.prefix}_espn_ext.py": _league_module_source(lg, apis, cfg.hosts) for lg in cfg.leagues}


def build() -> list[Path]:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "__init__.py").write_text("", encoding="utf-8", newline="\n")
    for name, src in _render_all().items():
        (OUT / name).write_text(src, encoding="utf-8", newline="\n")
    _ruff_format_dir(OUT)
    return sorted(OUT.glob("*_espn_ext.py"))


def _ensure_init_import(pkg_dir: Path, prefix: str, dotted: str) -> None:
    """Idempotently make ``pkg_dir/__init__.py`` re-export the generated ext.

    ``dotted`` is the import path of the ext module, e.g.
    ``sportsdataverse.soccer.epl.epl_espn_ext`` (nested) or
    ``sportsdataverse.nba.nba_espn_ext`` (flat).
    """
    init = pkg_dir / "__init__.py"
    text = init.read_text(encoding="utf-8") if init.exists() else ""
    line = f"from {dotted} import *\n"
    if line.strip() in text:
        return
    init.write_text((text.rstrip() + "\n" + line).lstrip("\n"), encoding="utf-8", newline="\n")


def _container_init_body(group: str, members: list[str], has_ext: bool) -> str:
    """Return the deterministic body for a sport-group container ``__init__.py``.

    Args:
        group:    Sport-group name (e.g. ``"soccer"``, ``"football"``).
        members:  Sorted list of sub-league prefixes that live under this group.
        has_ext:  True when a ``{group}_espn_ext.py`` also lives in the container
                  (currently only ``soccer`` doubles as a top-level league).

    Returns:
        Source text for the container ``__init__.py``.
    """
    lines: list[str] = ["from __future__ import annotations", ""]
    if has_ext:
        lines.append(f"from sportsdataverse.{group}.{group}_espn_ext import *  # noqa: F401,F403")
        lines.append("")
    lines.append(f"# Sub-league packages — imported so ``sportsdataverse.{group}.<leaf>`` is reachable")
    lines.append("# as an attribute on this container module (0.0.65+).")
    # Emit at most 4 members per import line to stay within line-length limits.
    chunk_size = 4
    for i in range(0, len(members), chunk_size):
        chunk = members[i : i + chunk_size]
        lines.append(f"from sportsdataverse.{group} import {', '.join(chunk)}  # noqa: F401,E402")
    lines.append("")
    return "\n".join(lines)


# The hand-written HockeyTech junior/minor league modules under
# ``sportsdataverse.hockey.*`` (PWHL is separate — it lives at the top level with a
# richer loader surface). SINGLE SOURCE OF TRUTH: every codegen list that enumerates
# these leagues references this, so promoting a league (a hockeytech/_leagues.py entry
# + a hockey/<lg>/ module) only needs it added HERE — no per-list drift.
_HOCKEYTECH_MODULE_LEAGUES = [
    "ahl",
    "ohl",
    "whl",
    "qmjhl",
    "echl",
    "sphl",
    "chl",
    "ushl",
    "bchl",
    "ajhl",
    "sjhl",
    "ojhl",
    "cchl",
    "gojhl",
    "mhl",
    "nojhl",
    "vijhl",
    "kijhl",
    "mjhl",
]


def _container_groups(groups_map: dict[str, str]) -> dict[str, list[str]]:
    """Build group → sorted-members mapping from the prefix→group dict.

    Includes the hand-written HockeyTech modules (``_HOCKEYTECH_MODULE_LEAGUES``)
    under hockey.
    """
    _HOCKEYTECH = _HOCKEYTECH_MODULE_LEAGUES
    result: dict[str, list[str]] = {}
    for prefix, group in groups_map.items():
        if not group:
            continue
        result.setdefault(group, [])
        if prefix not in result[group]:
            result[group].append(prefix)
    # Add HockeyTech extras under hockey (they have no leagues.yaml entry).
    for ht in _HOCKEYTECH:
        result.setdefault("hockey", [])
        if ht not in result["hockey"]:
            result["hockey"].append(ht)
    return {g: sorted(members) for g, members in result.items()}


def build_live() -> list[Path]:
    """Write the concrete generated modules into the live package + wire __init__."""
    groups = {lg.prefix: lg.group for lg in spec.load_leagues(ENDPOINTS / "leagues.yaml").leagues}
    written = []
    for name, src in _render_all().items():
        prefix = name[: -len("_espn_ext.py")]
        group = groups.get(prefix, "")
        if group:
            container = LIVE / group
            container.mkdir(parents=True, exist_ok=True)
            pkg_dir = container / prefix
            dotted = f"sportsdataverse.{group}.{prefix}.{prefix}_espn_ext"
        else:
            pkg_dir = LIVE / prefix
            dotted = f"sportsdataverse.{prefix}.{prefix}_espn_ext"
        pkg_dir.mkdir(parents=True, exist_ok=True)
        dest = pkg_dir / f"{prefix}_espn_ext.py"
        dest.write_text(src, encoding="utf-8", newline="\n")
        _ensure_init_import(pkg_dir, prefix, dotted)
        written.append(dest)
    # Write deterministic container __init__.py for each sport-group (replaces bare .touch()).
    for group, members in _container_groups(groups).items():
        container = LIVE / group
        container.mkdir(parents=True, exist_ok=True)
        has_ext = (container / f"{group}_espn_ext.py").exists()
        body = _container_init_body(group, members, has_ext)
        (container / "__init__.py").write_text(body, encoding="utf-8", newline="\n")
    if written:
        subprocess.run([*_RUFF, "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
    return sorted(written)


def _live_stale() -> list[str]:
    """Live ``{prefix}_espn_ext.py`` and container ``__init__.py`` files that differ from a fresh render."""
    tmp = OUT / "_check_live_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        rendered = _render_all()
        for name, src in rendered.items():
            (tmp / name).write_text(src, encoding="utf-8", newline="\n")
        _ruff_format_dir(tmp)
        groups = {lg.prefix: lg.group for lg in spec.load_leagues(ENDPOINTS / "leagues.yaml").leagues}
        stale = []
        for name in rendered:
            prefix = name[: -len("_espn_ext.py")]
            group = groups.get(prefix, "")
            if group:
                live_file = LIVE / group / prefix / f"{prefix}_espn_ext.py"
            else:
                live_file = LIVE / prefix / f"{prefix}_espn_ext.py"
            if not live_file.exists() or live_file.read_text(encoding="utf-8") != (tmp / name).read_text(
                encoding="utf-8",
            ):
                stale.append(str(live_file.relative_to(ROOT)))
        # Also check each container __init__.py against its expected body.
        for group, members in _container_groups(groups).items():
            container = LIVE / group
            has_ext = (container / f"{group}_espn_ext.py").exists()
            expected = _container_init_body(group, members, has_ext)
            live_init = container / "__init__.py"
            if not live_init.exists() or live_init.read_text(encoding="utf-8") != expected:
                stale.append(str(live_init.relative_to(ROOT)))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return stale


def check() -> int:
    # Render + format in a temp subdir *inside* OUT so ruff discovers the same repo
    # config, then compare byte-for-byte against the committed generated files.
    tmp = OUT / "_check_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        rendered = _render_all()
        for name, src in rendered.items():
            (tmp / name).write_text(src, encoding="utf-8", newline="\n")
        _ruff_format_dir(tmp)
        stale = [
            name
            for name in rendered
            if not (OUT / name).exists()
            or (OUT / name).read_text(encoding="utf-8") != (tmp / name).read_text(encoding="utf-8")
        ]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    if stale:
        print("codegen --check: stale/missing generated files:", ", ".join(sorted(stale)), file=sys.stderr)
        return 1
    return 0


# ===========================================================================
# Docs-coverage gate (every user-facing function must reach the rendered docs)
#
# `generate.py --coverage` enumerates the public, callable, in-package functions
# each league exports, then proves each name's exact text appears somewhere in the
# rendered markdown corpus. It is enforcement + measurement: `--coverage` is the
# verbose human report, and `_coverage_gaps()` is wired into `--check`, which fails
# on any undocumented in-scope function (allowlist for cross-cutting internals).
# ===========================================================================

# The 8 documented sport leagues. (pwhl is loader-only and has no module of its
# own to import; its load_pwhl_* loaders surface at the package top level and are
# checked as package-level/global names against the whole docs corpus.)
_COVERAGE_LEAGUES = [
    "nba",
    "wnba",
    "mbb",
    "wbb",
    "cfb",
    "nfl",
    "mlb",
    "nhl",
    "pwhl",
    *_HOCKEYTECH_MODULE_LEAGUES,  # ahl/ohl/whl/qmjhl + the promoted junior/minor leagues
    "odds",
]

# Mapping from doc/coverage prefix to actual Python module path for leagues
# whose module moved under a sport-group container (Task 5+).
# All other leagues default to f"sportsdataverse.{prefix}".
_LEAGUE_MODULE: dict[str, str] = {lg: f"hockey.{lg}" for lg in _HOCKEYTECH_MODULE_LEAGUES}

_COVERAGE_ALLOWLIST_FILE = ROOT / "tools" / "codegen" / "coverage_allowlist.yaml"


def _coverage_in_scope(name: str, obj) -> bool:
    """True when ``name``/``obj`` is a user-facing function that must reach the docs.

    A name is IN SCOPE iff ALL of:
      * it is not private (no leading ``_``);
      * ``obj`` is callable;
      * ``obj.__module__`` is rooted in ``sportsdataverse`` (this excludes
        re-exported third-party / typing names like ``Any``, ``Literal``,
        ``lru_cache``, ``download``, ``DMatrix``, ``Booster``, ``datetime``,
        ``reduce``, ``StringIO`` -- their module is ``typing``/``functools``/...);
      * it is not an internal ``helper_*`` function; and
      * it is not a ``parse_*`` parser (parsers are covered generically by the
        shared parsers page, so they are out of scope for this gate).
    """
    if name.startswith("_"):
        return False
    if not callable(obj):
        return False
    if not getattr(obj, "__module__", "").startswith("sportsdataverse"):
        return False
    if name.startswith("helper_"):
        return False
    if name.startswith("parse_"):
        return False
    return True


def _coverage_scope_names() -> tuple[dict[str, set[str]], set[str]]:
    """Enumerate in-scope user-facing names per league + the package-level (global) set.

    Returns ``(per_league, global_names)`` where ``per_league`` maps each league
    prefix to its in-scope name set (introspected from ``sportsdataverse.{league}``),
    and ``global_names`` is the set of top-level (``import sportsdataverse``) in-scope
    names NOT already attributed to any league module -- e.g. ``find_team``,
    ``find_athlete``, ``find_event``, ``list_functions``, ``function_count``,
    ``clear_team_cache``, the cache-config getters, and the loader-only ``load_pwhl_*``
    functions."""
    import importlib

    per_league: dict[str, set[str]] = {}
    all_league: set[str] = set()
    for lg in _COVERAGE_LEAGUES:
        mod_path = _LEAGUE_MODULE.get(lg, lg)
        mod = importlib.import_module(f"sportsdataverse.{mod_path}")
        names = {n for n in dir(mod) if _coverage_in_scope(n, getattr(mod, n))}
        per_league[lg] = names
        all_league |= names

    top = importlib.import_module("sportsdataverse")
    top_names = {n for n in dir(top) if _coverage_in_scope(n, getattr(top, n))}
    global_names = top_names - all_league
    return per_league, global_names


def _coverage_allowlist() -> dict[str, set[str]]:
    """``{league|'global': {names...}}`` of intentionally-excluded names.

    Missing keys default to an empty set, so the gate works even before a key is
    added to the YAML."""
    if not _COVERAGE_ALLOWLIST_FILE.exists():
        return {}
    import yaml

    d = yaml.safe_load(_COVERAGE_ALLOWLIST_FILE.read_text(encoding="utf-8")) or {}
    return {k: set(v or []) for k, v in d.items()}


@functools.lru_cache(maxsize=None)
def _docs_corpus(league: str | None) -> str:
    """Concatenated rendered markdown for the coverage search.

    ``league`` selects ``docs/docs/{league}/**/*.md``; ``None`` selects the whole
    ``docs/docs/**/*.md`` corpus (for package-level names). Only the live
    ``docs/docs/`` tree is searched -- ``docs/versioned_docs/`` is deliberately
    excluded. Returns ``""`` when the directory does not exist."""
    base = DOCS / league if league else DOCS
    if not base.exists():
        return ""
    return "\n".join(f.read_text(encoding="utf-8") for f in sorted(base.rglob("*.md")))


def _is_documented(name: str, corpus: str) -> bool:
    """True when ``name`` appears as a whole word in ``corpus``.

    A word-boundary search (``_`` counts as a word character) so ``player_stats``
    is NOT considered documented merely because ``player_stats_v3`` appears, while
    surrounding punctuation/backticks/whitespace still count as a match."""
    return re.search(r"\b" + re.escape(name) + r"\b", corpus) is not None


def _coverage_gaps() -> list[tuple[str, list[str]]]:
    """Return ``[(league_label, [missing_names])]`` for any in-scope user-facing
    function that is neither documented in the rendered docs nor allowlisted.

    Used by the ``--check`` gate to fail the build when real documentation gaps
    are introduced. Returns an empty list when every function is accounted for."""
    per_league, global_names = _coverage_scope_names()
    allow = _coverage_allowlist()

    groups: list[tuple[str, set[str], str | None]] = [(lg, per_league[lg], lg) for lg in _COVERAGE_LEAGUES]
    groups.append(("global", global_names, None))

    gaps: list[tuple[str, list[str]]] = []
    for label, names, league_dir in groups:
        corpus = _docs_corpus(league_dir)
        allowed = allow.get(label, set())
        missing = sorted(names - {n for n in names if _is_documented(n, corpus)} - allowed)
        if missing:
            gaps.append((label, missing))
    return gaps


def coverage_report() -> int:
    """Report user-facing functions that never reach the rendered docs corpus.

    For each league (per-league names searched against ``docs/docs/{league}``) plus
    the package-level ``global`` group (searched against the whole ``docs/docs``
    corpus), compute in_scope / documented / missing (= in_scope - documented -
    allowlist). Prints a per-group table, the TOTAL missing, and the missing names
    grouped per group. Returns 0 when nothing is missing, else 1."""
    per_league, global_names = _coverage_scope_names()
    allow = _coverage_allowlist()

    groups: list[tuple[str, set[str], str | None]] = [(lg, per_league[lg], lg) for lg in _COVERAGE_LEAGUES]
    groups.append(("global", global_names, None))

    rows: list[tuple[str, int, int, int]] = []
    missing_by_group: dict[str, list[str]] = {}
    total_missing = 0
    for label, names, league_dir in groups:
        corpus = _docs_corpus(league_dir)
        allowed = allow.get(label, set())
        documented = {n for n in names if _is_documented(n, corpus)}
        missing = sorted(names - documented - allowed)
        rows.append((label, len(names), len(documented), len(missing)))
        missing_by_group[label] = missing
        total_missing += len(missing)

    w = max(len("league"), *(len(r[0]) for r in rows))
    print("docs-coverage report (user-facing functions reaching the rendered docs):\n")
    print(f"  {'league'.ljust(w)} | in_scope | documented | missing")
    print(f"  {'-' * w}-|----------|------------|--------")
    for label, n_scope, n_doc, n_miss in rows:
        print(f"  {label.ljust(w)} | {n_scope:>8} | {n_doc:>10} | {n_miss:>7}")
    print(f"  {'-' * w}-|----------|------------|--------")
    print(f"  {'TOTAL'.ljust(w)} | {sum(r[1] for r in rows):>8} | {sum(r[2] for r in rows):>10} | {total_missing:>7}")

    if total_missing:
        print(f"\nMISSING ({total_missing} user-facing function(s) not in docs, not allowlisted):")
        for label, _names, _ in groups:
            miss = missing_by_group[label]
            if miss:
                print(f"\n  {label} ({len(miss)}):")
                for n in miss:
                    print(f"    {n}")
        return 1

    print("\ndocs-coverage: every in-scope user-facing function reaches the docs.")
    return 0


# ===========================================================================
# Docs generation (live Docusaurus tree -> docs/docs/{league})
#
# `generate.py --docs` regenerates the per-league reference subtree directly into
# the Docusaurus "Next" surface (docs/docs/), full-clobbering each generated dir.
# Package-wide conceptual pages (intro, quality-of-life, architecture/, parsers/)
# live OUTSIDE the generated league/reference dirs and are preserved untouched.
# The drift gate (`--check`) only orphan-checks the fully-generated subtrees.
# ===========================================================================

DOCS = ROOT / "docs" / "docs"

# Top-level docs/docs subdir that is fully owned by the generator (shared
# reference pages like parameters.md). League dirs are the other generated roots.
_DOCS_REFERENCE_DIR = "reference"

# ESPN API -> (reference-file slug, human label) for the per-API doc pages.
_ESPN_API_DOC = {
    "espn_site_v2": ("site", "ESPN site API (v2)"),
    "espn_web_v3": ("web", "ESPN web API (v3)"),
    "espn_core_v2": ("core", "ESPN core API (v2)"),
}

# Flat/native API module -> human label for the per-API doc pages. Parallels
# _ESPN_API_DOC; without it the label falls back to the raw module name with
# underscores->spaces ("nhl api web"), which reads poorly in the nav.
_FLAT_API_DOC = {
    "nhl_api_web": "NHL Web API",
    "nhl_edge": "NHL EDGE API",
    "nhl_stats_rest": "NHL Stats REST API",
    "nhl_records": "NHL Records API",
    "mlb_api": "MLB Stats API",
    "mlb_statcast": "MLB Statcast (Baseball Savant)",
    "nfl_api": "NFL.com API",
    "nba_stats": "NBA Stats API (stats.nba.com)",
    "wnba_stats": "WNBA Stats API (stats.wnba.com)",
    "on3": "On3 Recruit Database (api.on3.com)",
    "sports247": "247Sports Recruit Database (ipa.247sports.com)",
    "sports247_site_pages": "247Sports Site Pages (247sports.com)",
    # keyed twice: "pff" matches the FLAT_APIS stem; "pff_core" matches the rendered
    # module name the docs renderer looks up (api.module).
    "pff": "PFF Premium Stats (premium.pff.com)",
    "pff_core": "PFF Premium Stats (premium.pff.com)",
}

# Friendly label per releases.yaml base key, for the "Dataset loaders" row of a
# league index (so NFL reads "nflverse data releases", not "sportsdataverse-data
# releases"). Unknown keys fall back to the raw key.
_LOADER_BASE_LABEL = {
    "sdv_releases": "sportsdataverse-data releases",
    "raw_data": "sportsdataverse raw data",
    "nflverse": "nflverse data releases",
}


def _loader_base_label(prefix: str) -> str:
    """Human label for a league's dataset-loader source(s) (distinct bases joined)."""
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    bases = sorted({ld.base for ld in rel.loaders if ld.league == prefix})
    return " / ".join(_LOADER_BASE_LABEL.get(b, b) for b in bases) or "sportsdataverse-data releases"


def _loader_schema_table(fn: str) -> str:
    """Markdown column table for a loader from the introspected footer schemas."""
    cols = _loader_schemas().get(fn) or []
    if not cols:
        return ""
    head = "| col_name | type |\n|---|---|\n"
    return head + "".join(f"| `{c['name']}` | {c['type']} |\n" for c in cols)


def _loader_doc_views(prefix: str) -> list[dict]:
    """Template-facing loader dicts for ``loaders_page.md.jinja`` (one per league loader).

    ``automation`` is normalized to always carry ``repo``/``workflow`` keys so the
    StrictUndefined template can test ``ld.automation.repo`` safely."""
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    out: list[dict] = []
    for ld in rel.loaders:
        if ld.league != prefix:
            continue
        auto = ld.automation or {}
        # Release-tag page URL: for GitHub-releases-hosted assets, derive it from
        # the SAME repo the asset download comes from (download -> tag) so an
        # nflverse-hosted loader links nflverse tags. Non-releases bases (e.g.
        # raw.githubusercontent) still tag their provenance in the
        # sportsdataverse-data releases repo, so fall back to that historical URL.
        base_dl = rel.bases.get(ld.base, "")
        if "/releases/download/" in base_dl:
            tag_base = base_dl.replace("/releases/download/", "/releases/tag/")
        else:
            tag_base = "https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/"
        out.append(
            {
                "fn": ld.fn,
                "tag": ld.tag,
                "tag_url": f"{tag_base}{ld.tag}",
                "url": "" if ld.stub else f"{rel.bases[ld.base]}{ld.url}",
                "automation": {"repo": auto.get("repo", ""), "workflow": auto.get("workflow", "")},
                "return_table": _return_table(ld.returns_schema) if ld.returns_schema else _loader_schema_table(ld.fn),
                "example_seasons": (ld.example_args or {}).get("seasons", 2024),
            },
        )
    return out


def _apis_for(prefix: str) -> list[dict]:
    """API descriptors (``name``/``slug``/``label``/``base``/``count``/``kind``) that
    have a reference page for ``prefix`` -- the in-scope ESPN APIs plus any flat API
    whose prefix matches. ``name`` is what :func:`render_reference_page` dispatches on
    (an ESPN API name, or a flat-API YAML stem)."""
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    out: list[dict] = []
    league = next((lg for lg in cfg.leagues if lg.prefix == prefix), None)
    if league is not None:
        espn_apis = [spec.load_espn_api(ENDPOINTS / f"{a}.yaml", params) for a in ESPN_APIS]
        views = _espn_league_views(league, espn_apis, cfg.hosts)
        for api_obj, name in zip(espn_apis, ESPN_APIS):
            count = sum(1 for v in views if v.api_name == name)
            if count:
                slug, label = _ESPN_API_DOC[name]
                out.append(
                    {
                        "name": name,
                        "slug": slug,
                        "label": label,
                        "base": cfg.hosts[api_obj.host],
                        "count": count,
                        "kind": "espn",
                    },
                )
    for stem, fprefix in FLAT_APIS:
        if fprefix != prefix:
            continue
        y = ENDPOINTS / f"{stem}.yaml"
        if not y.exists():
            continue
        fa = spec.load_flat_api(y, params)
        out.append(
            {
                "name": stem,
                "slug": fa.module,
                "label": _FLAT_API_DOC.get(fa.module, fa.module.replace("_", " ")),
                "base": fa.host,
                "count": len(fa.endpoints),
                "kind": "flat",
            },
        )
    return out


def render_reference_page(prefix: str, api: str, position: int = 1) -> str:
    """Render the per-API reference page (8-section block per function) as markdown.

    ``api`` is either an ESPN API name (``espn_site_v2``/``espn_web_v3``/
    ``espn_core_v2``) or a flat-API YAML stem (``nhl_api_web``/``mlb_api``/...).
    Names are resolved through the same view helpers the module codegen uses, so the
    page documents exactly the wrapper names that get emitted. ``position`` sets the
    page's ``sidebar_position`` so the league's Reference category orders Loaders
    first, then native APIs, then the ESPN APIs (site/web/core)."""
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    if api in ESPN_APIS:
        cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
        league = next(lg for lg in cfg.leagues if lg.prefix == prefix)
        espn_apis = [spec.load_espn_api(ENDPOINTS / f"{a}.yaml", params) for a in ESPN_APIS]
        endpoints = [v for v in _espn_league_views(league, espn_apis, cfg.hosts) if v.api_name == api]
        _slug, label = _ESPN_API_DOC[api]
    else:
        fa = spec.load_flat_api(ENDPOINTS / f"{api}.yaml", params)
        endpoints = _flat_views(fa, league_prefix=prefix)
        label = _FLAT_API_DOC.get(fa.module, fa.module.replace("_", " "))
    template = render.ENV.get_template("reference_page.md.jinja")
    return template.render(
        prefix=prefix,
        title=f"{prefix.upper()} — {label}",
        label=label,
        sidebar_position=position,
        count=len(endpoints),
        endpoints=endpoints,
    )


# Example notebooks are rendered (executed, with outputs) to on-site Tutorial pages
# under docs/docs/tutorials/ by tools/codegen/render_notebooks.py. Each league index
# links the general quickstart plus its sport-specific tutorial. Links are relative
# to a league index page (docs/docs/<lg>/index.md) -> ../tutorials/<stem>.
_QUICKSTART_NB = ("01_quickstart", "Quickstart")
_LEAGUE_NOTEBOOKS: dict[str, tuple[str, str]] = {
    "cfb": ("02_cfb_intro", "CFB tutorial"),
    "nfl": ("03_nfl_intro", "NFL tutorial"),
    "nba": ("04_nba_intro", "NBA tutorial"),
    "wbb": ("05_wbb_intro", "WBB tutorial"),
    "wnba": ("08_wnba_intro", "WNBA tutorial"),
    "mbb": ("06_mbb_intro", "MBB tutorial"),
    "nhl": ("07_nhl_intro", "NHL tutorial"),
    "mlb": ("09_mlb_intro", "MLB tutorial"),
    "pwhl": ("10_pwhl_intro", "PWHL tutorial"),
    # AHL + the three CHL major-junior loops share one combined HockeyTech tutorial.
    "ahl": ("11_junior_hockey_intro", "Junior & minor hockey tutorial"),
    "ohl": ("11_junior_hockey_intro", "Junior & minor hockey tutorial"),
    "whl": ("11_junior_hockey_intro", "Junior & minor hockey tutorial"),
    "qmjhl": ("11_junior_hockey_intro", "Junior & minor hockey tutorial"),
    "odds": ("12_odds_intro", "Betting odds tutorial"),
}


def _notebooks_for(prefix: str) -> list[dict]:
    """``[{label, url}]`` of example tutorials for a league index page.

    Always the general quickstart first, then the league's sport-specific tutorial
    when one exists. ``url`` is a doc-relative link to the rendered on-site Tutorial
    page; ready for the ``league_index.md.jinja`` ``## Examples`` block."""
    entries = [_QUICKSTART_NB]
    sport = _LEAGUE_NOTEBOOKS.get(prefix)
    if sport is not None:
        entries.append(sport)
    return [{"label": label, "url": f"../tutorials/{stem}.md"} for stem, label in entries]


def render_league_index(
    prefix: str,
    *,
    has_additional: bool = False,
    additional_count: int = 0,
    r_parity: list[dict] | None = None,
    r_pkg: str | None = None,
) -> str:
    """Render a league's ``index.md`` (reference table + optional loaders link).

    ``has_additional`` / ``additional_count`` are supplied by :func:`_render_docs_all`
    (and :func:`_autodoc_names_by_scope`) after they compute the autodoc set for this
    league, so the index table can link the ``reference/additional`` page with an
    accurate function count. ``r_parity`` / ``r_pkg`` (also from
    :func:`_render_docs_all`) drive the Python<->R parity table."""
    loaders = _loader_doc_views(prefix)
    template = render.ENV.get_template("league_index.md.jinja")
    return template.render(
        prefix=prefix,
        api_rows=_apis_for(prefix),
        has_loaders=bool(loaders),
        loader_count=len(loaders),
        loader_base=_loader_base_label(prefix),
        has_additional=has_additional,
        additional_count=additional_count,
        notebooks=_notebooks_for(prefix),
        r_parity=r_parity or [],
        r_pkg=r_pkg,
    )


def render_loaders_page(prefix: str, position: int = 1) -> str:
    """Render a league's ``reference/loaders.md`` (diagram + automation table + blocks).

    ``position`` defaults low so Loaders sorts first in the league's Reference
    category (ahead of the native and ESPN API pages)."""
    template = render.ENV.get_template("loaders_page.md.jinja")
    return template.render(prefix=prefix, sidebar_position=position, loaders=_loader_doc_views(prefix))


def render_parameters_page() -> str:
    """Render the shared ``reference/parameters.md`` from parameters.yaml."""
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    template = render.ENV.get_template("parameter_reference.md.jinja")
    return template.render(params=list(params.values()))


# ===========================================================================
# Autodoc: hand-written public functions the endpoint-YAML codegen never covers.
#
# ~150 user-facing functions live in hand-written modules (mlb_statcast,
# mlb_api_extra, nfl_loaders, nhl_records_extra, the espn_* league wrappers,
# ...). They are NOT codegen endpoints, so the generated reference pages never
# document them; adding them to YAML would collide with the existing defs. This
# step renders the *coverage gap* (in-scope user-facing names NOT already
# documented by the other generated pages and NOT allowlisted) into a per-league
# ``reference/additional.md`` page, grouped by family, from live signatures +
# docstrings. It reuses the coverage scope predicate -- one source of truth for
# "user-facing".
# ===========================================================================

# The single autodoc page filename (excluded from the "already documented"
# corpus so the gap computation is not circular).
_AUTODOC_PAGE = "additional.md"
_AUTODOC_GLOBAL_PAGE = "python-helpers.md"

# Committed Returns-schema artifacts for autodoc (hand-written) functions:
# schemas/autodoc/{league}/{fn}.yaml (``league`` is a league prefix or
# ``global``). Captured by ``--autodoc-schemas`` (network), READ offline by the
# renderer -- exactly like loader_schemas.yaml. The example call args used during
# capture live in autodoc_example_args.yaml.
_AUTODOC_SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "autodoc"
_AUTODOC_EXAMPLE_ARGS_FILE = ROOT / "tools" / "codegen" / "autodoc_example_args.yaml"


@functools.lru_cache(maxsize=1)
def _autodoc_example_args() -> dict:
    """``{league|'global': {fn: {kwargs}}}`` example call args for the capture step.

    Empty dict if the registry file is absent; a function with no entry falls
    back to a no-arg ``{}`` attempt during capture."""
    if not _AUTODOC_EXAMPLE_ARGS_FILE.exists():
        return {}
    import yaml

    d = yaml.safe_load(_AUTODOC_EXAMPLE_ARGS_FILE.read_text(encoding="utf-8")) or {}
    return {k: (v or {}) for k, v in d.items()}


@functools.lru_cache(maxsize=None)
def _autodoc_return_columns(scope: str, fn: str) -> tuple:
    """``({name,type,description}, ...)`` from the committed autodoc schema, or ``()``.

    ``scope`` is a league prefix or ``"global"``. Read offline by the renderer;
    returns an empty tuple when no schema was captured for ``fn`` (prose
    fallback). A tuple (not list) so the result is hashable under lru_cache."""
    p = _AUTODOC_SCHEMA_DIR / scope / f"{fn}.yaml"
    if not p.exists():
        return ()
    import yaml

    d = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return tuple(d.get("columns", []) or [])


# Modules whose name happens to end in "_parsers" but which are genuine
# user-facing NCAA HTML-parser modules (Phase 5e), not generic per-source
# dispatch-registry files -- exempted from the ``endswith("_parsers")`` leak
# heuristic below so their public functions/classes (``get_team_triples``,
# ``get_neutral_games``, ``ScheduleBuilders``, ...) get autodoc'd instead of
# silently vanishing from both the autodoc page AND the coverage allowlist.
_PARSER_SUFFIX_LEAK_EXCEPTIONS = frozenset(
    {
        "sportsdataverse.mbb.mbb_ncaa_team_parsers",
        "sportsdataverse.wbb.wbb_ncaa_team_parsers",
    },
)


# Modules that leak into a league's ``dir()`` via ``from ... import *`` but are
# NOT hand-written league functions: shared download/JSON utilities, the shared
# error types, and the generic per-source parsers (``parser_for_*``). These are
# genuine allowlist candidates, NOT autodoc material, so they are excluded from
# the autodoc set and remain visible to ``--coverage`` for a later allowlist task.
def _is_shared_leak(module: str) -> bool:
    if module in _PARSER_SUFFIX_LEAK_EXCEPTIONS:
        return False
    return module == "sportsdataverse.dl_utils" or module == "sportsdataverse.errors" or module.endswith("_parsers")


# Deterministic family order for the autodoc page (families not listed sort last,
# alphabetically). Functions within a family are always sorted alphabetically.
_AUTODOC_FAMILY_ORDER = [
    "Statcast",
    "MLB Stats API",
    "Play-by-play, schedule & rosters",
    "NHL native",
    "Dataset loaders",
    "Utilities & helpers",
    "Other",
]

_ESPN_PBP_FAMILY_TOKENS = (
    "_pbp",
    "_schedule",
    "_game_rosters",
    "_player_stats",
    "_play_participants",
    "_team_stats",
    "_game_officials",
)


def _autodoc_family(name: str) -> str:
    """Group key for an autodoc function name (see the family rules in Task D2)."""
    if name.startswith("statcast") or name == "mlb_statcast":
        return "Statcast"
    if name.startswith("load_"):
        return "Dataset loaders"
    if name.startswith("mlb_api"):
        return "MLB Stats API"
    if name.startswith("espn_") and any(tok in name for tok in _ESPN_PBP_FAMILY_TOKENS):
        return "Play-by-play, schedule & rosters"
    if name.startswith("nhl_"):
        return "NHL native"
    if name.endswith("PlayProcess") or name.startswith(("most_recent_", "get_current_")) or name == "year_to_season":
        return "Utilities & helpers"
    return "Other"


def _autodoc_signature(obj) -> str:
    """``str(inspect.signature(obj))`` with a ``"(...)"`` fallback for un-introspectable objects."""
    import inspect

    try:
        return str(inspect.signature(obj))
    except (TypeError, ValueError):
        return "(...)"


def _ann_str(annotation) -> str:
    """Stringify a signature annotation cleanly for the autodoc Parameters table.

    Annotations may be live types (``int``, ``pl.DataFrame``) or strings (PEP 563
    ``from __future__ import annotations`` makes every annotation a string). Strip
    surrounding quotes and module qualifiers (``pl.DataFrame`` -> ``DataFrame``,
    ``sportsdataverse.errors.NoEspnDataError`` -> ``NoEspnDataError``) so the table
    stays readable. Subscripted generics (``list[int]``) and unions keep their
    bracketed/`|`-joined form but with each component qualifier stripped."""
    import re as _re

    if isinstance(annotation, str):
        text = annotation.strip().strip("'\"")
    else:
        # Prefer a __name__ for plain classes; fall back to repr for typing forms.
        text = getattr(annotation, "__name__", None) or str(annotation)
        # ``typing.Optional[int]`` etc. render with a ``typing.`` prefix via str().
        text = text.replace("typing.", "")
    # Drop dotted module qualifiers on each identifier-ish token, keeping the final
    # component (e.g. ``pl.DataFrame`` -> ``DataFrame``). Leaves brackets, commas,
    # ``|`` and spaces intact so generics/unions survive.
    text = _re.sub(r"\b(?:[A-Za-z_][\w]*\.)+([A-Za-z_]\w*)", r"\1", text)
    return text.strip()


_PROSE_SECTION_RE = re.compile(
    r"^(See Also|Notes?|Returns?|Raises?|Warnings?|Warning|Todo|References?)\s*:?\s*$",
    re.IGNORECASE,
)
_RST_DIRECTIVE_RE = re.compile(r"^\.\.\s+\S")

# Any docstring SECTION HEADER -- a line that is JUST the header word (optionally
# followed by ``:``). Used by :func:`_clean_long` to truncate the prose wall the
# moment a section starts, and by :func:`_fallback_doc_sections` to split the
# raw docstring into sections. Kept broad (every Google/NumPy/reST heading we
# know) so no section body ever leaks into the rendered prose.
_DOC_SECTION_NAMES = (
    "Args",
    "Arguments",
    "Parameters",
    "Param",
    "Returns",
    "Return",
    "Yields",
    "Yield",
    "Raises",
    "Example",
    "Examples",
    "See Also",
    "Note",
    "Notes",
    "References",
    "Reference",
    "Warning",
    "Warnings",
    "Attributes",
    "Todo",
)
_DOC_SECTION_HEADER_RE = re.compile(
    r"^\s*(?:" + "|".join(re.escape(n) for n in _DOC_SECTION_NAMES) + r")\s*:?\s*$",
    re.IGNORECASE,
)
# Section header that may carry trailing content on the same line (``Args: foo``).
# Only the subset of headers we actively extract in the lenient fallback.
_FALLBACK_HEADER_RE = re.compile(
    r"^\s*(Args|Arguments|Parameters|Param|Returns|Return|Yields|Yield|Raises"
    r"|Examples?|See Also|Notes?)\s*:\s*(.*)$",
    re.IGNORECASE,
)
# A Google-style ``name (type): desc`` or ``name: desc`` arg-line OPENER. The
# name must be a bare identifier (no spaces), which is what distinguishes a new
# entry from a wrapped continuation line.
_ARG_LINE_RE = re.compile(
    r"^(?P<name>\*{0,2}[A-Za-z_]\w*)\s*(?:\((?P<type>[^)]*)\))?\s*:\s*(?P<desc>.*)$",
)
# reST inline link: ``text <url>``_  -> capture text + url.
_RST_LINK_RE = re.compile(r"`([^`<]+?)\s*<([^>]+)>`_+")
# Bare reST trailing-underscore / role artifacts left after link stripping, e.g.
# a dangling `` `_ `` or `` ` ``-wrapped fragment with no target.
_RST_BACKTICK_ARTIFACT_RE = re.compile(r"`_+")
# Sphinx cross-reference roles: :func:`X`, :class:`X`, etc. -> `X`
_RST_ROLE_RE = re.compile(r":(?:func|class|mod|meth|attr|data|ref|obj|exc):`([^`]+)`")
# reST double-backtick literals: ``X`` -> `X`
_RST_DBL_BACKTICK_RE = re.compile(r"``([^`]+)``")


def _strip_rst_links(text: str) -> str:
    """Convert reST inline links to plain markdown and drop stray reST artifacts.

    * `` `text <url>`_ `` -> ``[text](url)`` (markdown link).
    * Any remaining bare `` `_ `` / `` `__ `` trailing-underscore artifacts are
      removed so they do not render as literal backtick+underscore noise.

    Safe on text with no reST links (returns it unchanged); never raises."""
    if not text:
        return text
    out = _RST_LINK_RE.sub(lambda m: f"[{m.group(1).strip()}]({m.group(2).strip()})", text)
    out = _RST_BACKTICK_ARTIFACT_RE.sub("", out)
    return out


def _normalize_rst(text: str) -> str:
    """Normalize reST markup in docstring prose to standard markdown inline code.

    Applies in order (idempotent — running twice yields the same result):
    1. Sphinx cross-reference roles: ``:func:`X``` / ``:class:`X``` / etc.
       -> `` `X` `` (drop the ``:role:`` prefix, keep the backticked target).
    2. Double-backtick literals: ````X```` -> `` `X` ``.
    3. reST inline links: `` `text <url>`_ `` -> ``[text](url)``.
    4. Bare trailing-underscore artifacts: `` `_ `` -> removed.

    Safe on ``None`` / empty strings and on text that has already been
    normalised; never raises."""
    if not text:
        return text
    out = _RST_ROLE_RE.sub(r"`\1`", text)
    out = _RST_DBL_BACKTICK_RE.sub(r"`\1`", out)
    out = _RST_LINK_RE.sub(lambda m: f"[{m.group(1).strip()}]({m.group(2).strip()})", out)
    out = _RST_BACKTICK_ARTIFACT_RE.sub("", out)
    return out


def _returns_prose(ds_returns) -> str:
    """Reconstruct Returns prose from a ``docstring_parser`` DocstringReturns.

    ``docstring_parser`` splits a Google-style ``Returns:`` body on the first
    colon into ``(type_name, description)``. When the body is an inline code
    span like ``col: dtype, ...``, the colon INSIDE the span is mistaken for
    the type/description separator — leaving ``type_name`` a dangling ``col``
    fragment (with an unbalanced backtick) and ``description`` the remainder,
    which :func:`_normalize_rst` then cannot pair (emitting a stray trailing
    ``\\`\\``). Legit Google return types (``pl.DataFrame``, ``dict[str, X]``)
    never contain a backtick, so recombine ONLY in that mis-split case, then
    normalise reST -> markdown. Empty / ``None`` input yields ``""``.
    """
    if ds_returns is None or not getattr(ds_returns, "description", None):
        return ""
    desc = ds_returns.description
    tname = (getattr(ds_returns, "type_name", None) or "").strip()
    if "`" in tname:
        desc = f"{tname}: {desc}"
    return _normalize_rst(" ".join(desc.split()))


def _clean_long(text: str) -> str:
    """Defensive truncation of a docstring's long-description prose.

    ``docstring_parser`` occasionally fails to split a Google-style docstring
    (e.g. wrapped multi-line ``Args`` descriptions defeat the parser) and dumps
    the ENTIRE docstring -- Args/Returns/Example/See Also -- into
    ``long_description``. This helper truncates at the first line that is a
    section HEADER (:data:`_DOC_SECTION_HEADER_RE`) or a reST literal-block
    intro (a line ending in ``::``), so no section body ever leaks into the
    rendered prose wall. Returns the dedented, stripped prose before that point;
    reST links are normalised. Never raises."""
    if not text:
        return ""
    lines = text.splitlines()
    collected: list[str] = []
    for ln in lines:
        s = ln.strip()
        if _DOC_SECTION_HEADER_RE.match(ln):
            break
        if s.endswith("::"):  # reST literal-block intro (e.g. ``Quick start::``)
            break
        collected.append(ln)
    import textwrap

    block = textwrap.dedent("\n".join(collected)).strip()
    return _normalize_rst(block)


def _fallback_doc_sections(raw: str) -> dict:
    """Lenient Google-section extractor for docstrings the strict parser drops.

    When ``docstring_parser`` yields no params/returns/example but the raw
    docstring clearly has ``Args:``/``Returns:``/``Example(s):`` headers (the
    classic failure: a wrapped multi-line Arg description such as ``4 for
    all-star, 5 for off-season`` makes the strict Google parser raise), this
    recovers them by hand.

    Returns ``{"params": [...], "returns": str, "example": str}`` where each
    param is ``{"arg_name", "type_name", "description"}``. Continuation lines
    (lines that do NOT open a new ``name (...):`` / ``name:`` entry) are
    space-joined onto the previous param's description. Never raises; returns
    empty pieces when it cannot parse."""
    out = {"params": [], "returns": "", "example": ""}
    if not raw:
        return out
    try:
        lines = raw.splitlines()
        # Group lines into sections keyed by header name. Anything before the
        # first header is preamble (short/long description) and ignored here.
        sections: dict[str, list[str]] = {}
        current: str | None = None
        for ln in lines:
            m = _FALLBACK_HEADER_RE.match(ln)
            if m:
                current = m.group(1).strip().lower()
                sections.setdefault(current, [])
                trailing = m.group(2).strip()
                if trailing:
                    sections[current].append("    " + trailing)
                continue
            if current is not None:
                sections[current].append(ln)

        def section(*keys: str) -> list[str]:
            for k in keys:
                if k in sections:
                    return sections[k]
            return []

        # --- Args / Parameters ---------------------------------------------
        arg_lines = section("args", "arguments", "parameters", "param")
        params: list[dict] = []
        for raw_line in arg_lines:
            s = raw_line.strip()
            if not s:
                continue
            am = _ARG_LINE_RE.match(s)
            if am and (am.group("type") is not None or " " not in am.group("name")):
                # New entry. Guard: a continuation line like "4 for all-star: x"
                # would falsely match name="4"? No -- name must be an identifier
                # (\w starting with a letter/underscore), so leading-digit
                # continuations are treated as continuations below.
                params.append(
                    {
                        "arg_name": am.group("name"),
                        "type_name": (am.group("type") or "").strip(),
                        "description": am.group("desc").strip(),
                    },
                )
            elif params:
                # Continuation of the previous param's description.
                joined = (params[-1]["description"] + " " + s).strip()
                params[-1]["description"] = joined
            # else: stray line before any param -- ignore.
        for p in params:
            p["description"] = _normalize_rst(" ".join(p["description"].split()))
        out["params"] = params

        # --- Returns / Yields ----------------------------------------------
        ret_lines = section("returns", "return", "yields", "yield")
        if ret_lines:
            ret = textwrap_dedent_join(ret_lines)
            ret = " ".join(ret.split())
            # Strip a leading ``pl.DataFrame:`` / ``type:`` prefix if present.
            rm = re.match(r"^[A-Za-z_][\w.\[\], |]*\s*:\s*(.*)$", ret)
            if rm and rm.group(1):
                ret = rm.group(1).strip()
            out["returns"] = _normalize_rst(ret)

        # --- Example / Examples --------------------------------------------
        ex_lines = section("examples", "example")
        if ex_lines:
            out["example"] = textwrap_dedent_join(ex_lines).strip()
    except Exception:  # noqa: BLE001 -- never raise on odd docstrings.
        return {"params": [], "returns": "", "example": ""}
    return out


def textwrap_dedent_join(lines: list[str]) -> str:
    """Dedent a block of section lines and join into a single string."""
    import textwrap

    return textwrap.dedent("\n".join(lines))


def _clean_example(text: str) -> str:
    """Strip reST intro lines and trailing prose sections from a parsed example block.

    docstring_parser folds the entire ``Examples:`` section body into
    ``ds.examples[0].description``, which means the rendered text may contain:

    * A leading reST literal-block intro line (e.g. ``Quick start::``).
    * Trailing prose sections that docstring_parser did not split off
      (``See Also:``, ``Notes:``, RST ``.. _ref:`` footnotes, etc.).

    This helper:

    1. Skips leading blank lines then strips exactly ONE leading line whose
       stripped form ends with ``::`` (the reST literal-block marker).  Only
       a *single* intro is removed; if the first real content is already code
       it is kept intact.
    2. Collects lines until the first trailing-prose sentinel: a line whose
       stripped form matches a known prose section header (``See Also``,
       ``Notes``, ``Returns``, ``Raises``, ``Warnings``, ``Todo``,
       ``References`` -- optionally followed by ``:``) OR a bare RST
       ``.. directive`` line (``.. _ref:``, ``.. note::``, etc.).
    3. ``textwrap.dedent``-strips the collected block and ``.strip()``s it.
    4. Returns ``""`` when nothing real remains.
    """
    lines = text.splitlines()
    i = 0
    n = len(lines)

    # Step 1 — skip leading blank lines.
    while i < n and not lines[i].strip():
        i += 1

    # Step 2 — skip exactly ONE leading reST intro line (ends with "::").
    # A line is a "pure intro" when it is non-empty, ends with "::", and is
    # not itself indented code (indented lines are kept regardless).
    if i < n:
        stripped = lines[i].strip()
        if stripped.endswith("::") and stripped != "::":
            # Only skip when the line looks like prose, not code.  We treat a
            # line as prose when it has no leading whitespace (or very little)
            # AND it does not look like a Python statement.
            leading_spaces = len(lines[i]) - len(lines[i].lstrip())
            looks_like_code = leading_spaces >= 4 or stripped.startswith(("from ", "import ", ">>>"))
            if not looks_like_code:
                i += 1  # consume the intro line
                # Also skip the blank line(s) that follow the reST intro
                # so they don't defeat textwrap.dedent (blank lines have
                # zero leading spaces and would anchor the common indent to 0).
                while i < n and not lines[i].strip():
                    i += 1

    # Step 3 — collect lines until a prose-section sentinel.  Mid-block reST
    # literal-block intro lines (a prose line ending in ``::``, e.g.
    # ``Pull a specific date::``) are not code; left verbatim they render as a
    # broken statement inside the ```python``` fence.  Convert each to a ``# ``
    # comment so the example stays valid Python and reads as a labelled step.
    collected: list[str] = []
    for j in range(i, n):
        s = lines[j].strip()
        if _PROSE_SECTION_RE.match(s):
            break
        if _RST_DIRECTIVE_RE.match(lines[j]):
            break
        if s.endswith("::") and s != "::":
            leading_spaces = len(lines[j]) - len(lines[j].lstrip())
            looks_like_code = leading_spaces >= 4 or s.startswith(("from ", "import ", ">>>"))
            if not looks_like_code:
                # A reST literal-block intro can wrap across several prose lines,
                # only the LAST of which carries the ``::`` (e.g. "Construct a
                # fresh instance directly (rarely needed -- prefer\n``update_config``)::").
                # Absorb the preceding contiguous prose lines of that intro into
                # the label so they don't leak into the code fence as a broken
                # statement. Stop at a blank line, indented code (>=4), an
                # import/>>> line, or an already-emitted ``# `` label.
                parts = [s[:-2].rstrip().rstrip(":").strip()]
                while collected:
                    prev = collected[-1]
                    prev_s = prev.strip()
                    if not prev_s:
                        break
                    prev_indent = len(prev) - len(prev.lstrip())
                    if prev_indent >= 4 or prev_s.startswith(("from ", "import ", ">>>", "#")):
                        break
                    parts.insert(0, collected.pop().strip())
                label = " ".join(p for p in parts if p)
                pad = " " * leading_spaces
                collected.append(f"{pad}# {label}" if label else "")
                continue
        collected.append(_strip_rst_links(lines[j]))

    # Step 4 — dedent and strip.
    # textwrap.dedent uses the minimum common leading whitespace across ALL
    # non-blank lines.  When a collected block mixes 4-space-indented code with
    # 0-space section-header lines (``Section label::``), the common indent is
    # 0 and dedent is a no-op.  We instead compute the indent from the FIRST
    # non-blank collected line and strip that prefix from every line, which
    # matches what a human reader expects when the block starts with indented
    # code (after the intro was removed) but also contains 0-indent headers.
    stripped_lines = [ln for ln in collected if ln.strip()]
    if not stripped_lines:
        return ""
    first_indent = len(stripped_lines[0]) - len(stripped_lines[0].lstrip())
    if first_indent > 0:
        prefix = " " * first_indent
        collected = [ln[first_indent:] if ln.startswith(prefix) else ln for ln in collected]
    result = "\n".join(collected).strip()
    return result


# Default object.__init__ docstring -- carries no real constructor docs, so a
# class whose __init__ shows only this is treated as having no __init__ docstring.
_INIT_DOC_BOILERPLATE = "Initialize self."


def _parse_doc_params(raw: str) -> dict:
    """``{arg_name: {"description","type_name"}}`` parsed from a raw docstring.

    Strict ``docstring_parser`` first; the lenient ``_fallback_doc_sections``
    recovery only when the strict parse yields no params (wrapped multi-line
    Google ``Args`` defeat the strict parser)."""
    from docstring_parser import parse

    raw = raw or ""
    try:
        ds = parse(raw)
    except Exception:  # noqa: BLE001 -- AUTO parse can raise on malformed sections.
        ds = None
    out: dict[str, dict] = {}
    ds_params = list(ds.params) if ds is not None else []
    for p in ds_params:
        if p.arg_name:
            out[p.arg_name] = {"description": p.description or "", "type_name": p.type_name or ""}
    if not ds_params:
        for p in _fallback_doc_sections(raw)["params"]:
            out.setdefault(p["arg_name"], {"description": p["description"], "type_name": p["type_name"]})
    return out


def _method_signature(method) -> str:
    """``inspect.signature(method)`` as a call string with ``self``/``cls`` dropped."""
    import inspect

    try:
        sig = inspect.signature(method)
    except (ValueError, TypeError):
        return "(...)"
    params = [p for n, p in sig.parameters.items() if n not in ("self", "cls")]
    try:
        return str(sig.replace(parameters=params))
    except (ValueError, TypeError):
        return "(...)"


def _augment_class_view(cls, view: dict) -> None:
    """In-place: enrich a class autodoc ``view`` with constructor-doc backfill +
    a per-method doc-view list.

    A class commonly documents its constructor arguments on ``__init__`` rather
    than on the class object that ``inspect.getdoc`` reads (e.g. CFBPlayProcess),
    so blank constructor param cells are backfilled from ``__init__``. Each public
    (non-underscore) method gets its own :func:`_doc_view` so the template can
    render the method's signature, description, parameters, returns, and example."""
    import inspect

    init_raw = inspect.getdoc(cls.__init__) or ""
    if init_raw.startswith(_INIT_DOC_BOILERPLATE):
        init_raw = ""
    if init_raw:
        init_params = _parse_doc_params(init_raw)
        for p in view["params"]:
            if p["description"]:
                continue
            ip = init_params.get(p["name"])
            if ip and ip["description"]:
                p["description"] = _normalize_rst(" ".join(ip["description"].split()))
                if not p["type"] and ip["type_name"]:
                    p["type"] = ip["type_name"]

    methods = []
    for name, member in inspect.getmembers(cls, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        methods.append({"name": name, "signature": _method_signature(member), **_doc_view(member)})
    methods.sort(key=lambda m: m["name"])
    view["methods"] = methods


def _doc_view(obj) -> dict:
    """Parsed docstring + signature for rich autodoc rendering of ``obj``.

    Robust to missing docstrings, classes, builtins, and un-introspectable
    callables: every field falls back to an empty value rather than raising. Param
    descriptions come from the docstring; param *types* prefer the live annotation
    (docstrings frequently omit them) and fall back to the docstring's declared
    type. ``self``/``cls`` and ``*args``/``**kwargs`` are dropped from the table.

    For a class, ``inspect.signature``/``getdoc`` resolve the constructor; the
    returned view is then enriched by :func:`_augment_class_view` with a
    ``methods`` list (one doc-view per public method). Non-class objects carry an
    empty ``methods`` list."""
    import inspect

    from docstring_parser import parse

    raw = inspect.getdoc(obj) or ""
    try:
        ds = parse(raw)
    except Exception:  # noqa: BLE001 -- AUTO parse can raise on malformed sections.
        ds = None

    # Lenient recovery for docstrings the strict parser drops (wrapped multi-line
    # Google ``Args`` defeat it: 0 params, whole body dumped into long_description).
    ds_params = list(ds.params) if ds is not None else []
    ds_returns = ds.returns if ds is not None else None
    ds_examples = ds.examples if ds is not None else []
    ds_short = ds.short_description if ds is not None else ""
    ds_long = ds.long_description if ds is not None else ""

    need_fallback = (
        not ds_params
        and (ds_returns is None or not ds_returns.description)
        and not (ds_examples and ds_examples[0].description)
    )
    fb = _fallback_doc_sections(raw) if need_fallback else {"params": [], "returns": "", "example": ""}

    # Param descriptions: prefer the strict parser; fall back to the lenient one.
    doc_params = {p.arg_name: p for p in ds_params}
    fb_params = {p["arg_name"]: p for p in fb["params"]}

    try:
        sig = inspect.signature(obj)
    except (ValueError, TypeError):
        sig = None
    params: list[dict] = []
    if sig is not None:
        for name, sp in sig.parameters.items():
            if name in ("self", "cls") or sp.kind in (sp.VAR_POSITIONAL, sp.VAR_KEYWORD):
                continue
            dp = doc_params.get(name)
            fbp = fb_params.get(name)
            ann = "" if sp.annotation is sp.empty else _ann_str(sp.annotation)
            default = "" if sp.default is sp.empty else repr(sp.default)
            desc = ""
            if dp is not None and dp.description:
                desc = " ".join((dp.description or "").split())
            elif fbp is not None and fbp["description"]:
                desc = " ".join(fbp["description"].split())
            doc_type = (dp.type_name if dp is not None and dp.type_name else "") or (
                fbp["type_name"] if fbp is not None and fbp["type_name"] else ""
            )
            params.append(
                {
                    "name": name,
                    "type": (doc_type or ann),
                    "default": default,
                    "description": _normalize_rst(desc),
                },
            )

    long = _clean_long(ds_long or "")
    returns = ""
    if ds_returns is not None and ds_returns.description:
        returns = _returns_prose(ds_returns)
    elif fb["returns"]:
        returns = fb["returns"]
    raw_example = ds_examples[0].description if ds_examples and ds_examples[0].description else ""
    if not raw_example and fb["example"]:
        raw_example = fb["example"]
    example = _clean_example(raw_example)
    result = {
        "short": _normalize_rst((ds_short or "").strip()),
        "long": long,
        "params": params,
        "returns": returns,
        "example": example,
        "methods": [],
    }
    if inspect.isclass(obj):
        _augment_class_view(obj, result)
    return result


def _autodoc_names(league: str | None, corpus: str) -> list[str]:
    """In-scope user-facing names that the autodoc page should document for ``league``.

    The set is: in-scope names (per the shared coverage predicate) MINUS names
    already documented by the OTHER generated pages (``corpus`` -- the rendered
    markdown of every other in-scope page, EXCLUDING this league's autodoc page so
    the gap computation is not circular) MINUS the allowlist MINUS
    shared-utility/error/parser leaks (genuine allowlist candidates, handled by a
    later task). ``league=None`` is the package-level (global) set."""
    import importlib

    per_league, global_names = _coverage_scope_names()
    allow = _coverage_allowlist()
    if league is None:
        names = global_names
        mod = importlib.import_module("sportsdataverse")
        allowed = allow.get("global", set())
    elif league not in per_league:
        # Loader-only leagues (e.g. pwhl) have no module of their own and no
        # in-scope league names -- their loaders surface at the package top level
        # and are covered by the global autodoc/coverage path.
        return []
    else:
        names = per_league[league]
        mod_path = _LEAGUE_MODULE.get(league, league)
        mod = importlib.import_module(f"sportsdataverse.{mod_path}")
        allowed = allow.get(league, set())
    out = []
    for n in names:
        if n in allowed:
            continue
        if _is_documented(n, corpus):
            continue
        obj = getattr(mod, n, None)
        if obj is None:
            continue
        if _is_shared_leak(getattr(obj, "__module__", "")):
            continue
        out.append(n)
    return sorted(out)


def _autodoc_groups(league: str | None, names: list[str]) -> list[dict]:
    """``[{family, functions:[{name, signature, short, long, params, returns, example}]}]``.

    Families ordered by :data:`_AUTODOC_FAMILY_ORDER` (unknown families last,
    alphabetically); functions sorted alphabetically within each family. Each
    function carries the parsed-docstring view from :func:`_doc_view` so the
    template can render Parameters/Returns/Example sections."""
    import importlib

    _mod_path = _LEAGUE_MODULE.get(league, league) if league is not None else None
    mod = importlib.import_module("sportsdataverse" if _mod_path is None else f"sportsdataverse.{_mod_path}")
    scope = "global" if league is None else league
    by_family: dict[str, list[dict]] = {}
    for n in names:
        obj = getattr(mod, n)
        view = _doc_view(obj)
        # Pipe-escape param cells (table-rendered); short/long/returns are prose.
        for p in view["params"]:
            for k in ("type", "default", "description"):
                p[k] = p[k].replace("|", "\\|")
        # Class methods (classes only): escape each method's param cells too.
        for m in view.get("methods", []):
            for p in m["params"]:
                for k in ("type", "default", "description"):
                    p[k] = p[k].replace("|", "\\|")
        # Returns column table from the committed autodoc schema (offline read);
        # empty list -> the template falls back to the docstring Returns prose.
        # Blank description cells are backfilled from the mined SDV R-package dict
        # (league-aware; ``global`` scope -> ``_merged`` fallback via league=None);
        # a non-empty captured description is preserved.
        return_columns = [dict(c) for c in _autodoc_return_columns(scope, n)]
        for c in return_columns:
            raw_name = str(c.get("name", ""))
            c["description"] = _table_cell_desc(str(c.get("description", "")), league, raw_name, n)
            c["name"] = raw_name.replace("|", "\\|")
            c["type"] = str(c.get("type", "")).replace("|", "\\|")
        by_family.setdefault(_autodoc_family(n), []).append(
            {"name": n, "signature": _autodoc_signature(obj), "return_columns": return_columns, **view},
        )

    def fam_key(fam: str) -> tuple[int, str]:
        return (_AUTODOC_FAMILY_ORDER.index(fam) if fam in _AUTODOC_FAMILY_ORDER else len(_AUTODOC_FAMILY_ORDER), fam)

    groups = []
    for fam in sorted(by_family, key=fam_key):
        fns = sorted(by_family[fam], key=lambda f: f["name"])
        groups.append({"family": fam, "functions": fns})
    return groups


def render_autodoc_page(prefix: str | None, corpus: str) -> str | None:
    """Render a league's (or the package-level) ``additional.md`` autodoc page.

    ``prefix`` is a league prefix, or ``None`` for the package-level page.
    ``corpus`` is the rendered markdown of the OTHER in-scope pages (used to decide
    which names are already documented). Returns ``None`` (caller omits the page)
    when the autodoc set is empty."""
    names = _autodoc_names(prefix, corpus)
    if not names:
        return None
    groups = _autodoc_groups(prefix, names)
    if prefix is None:
        title = "Package — additional Python functions"
        module = "sportsdataverse"
    else:
        title = f"{prefix.upper()} — additional Python functions"
        module = f"sportsdataverse.{prefix}"
    template = render.ENV.get_template("autodoc_page.md.jinja")
    return template.render(title=title, module=module, sidebar_position=50, groups=groups)


def _autodoc_names_by_scope() -> dict[str | None, list[str]]:
    """``{league_prefix|None: [autodoc names]}`` -- the same in-scope autodoc set the
    docs generator documents on each ``additional.md`` / ``python-helpers.md`` page.

    Mirrors :func:`_render_docs_all`'s per-league corpus construction (the "already
    documented" judgment depends on the other rendered pages for that league), so the
    capture step (:func:`refresh_autodoc_schemas`) introspects EXACTLY the functions
    the autodoc pages render. ``None`` is the package-level (global) scope."""
    out: dict[str, str] = {}
    preserved = _preserved_docs_corpus()
    result: dict[str | None, list[str]] = {}
    for prefix in _doc_leagues():
        apis = _apis_for(prefix)
        loaders = _loader_doc_views(prefix)
        espn_n = flat_n = 0
        for a in apis:
            if a["kind"] == "espn":
                pos = 20 + espn_n
                espn_n += 1
            else:
                pos = 10 + flat_n
                flat_n += 1
            out[f"{prefix}/reference/{a['slug']}.md"] = render_reference_page(prefix, a["name"], pos)
        if loaders:
            out[f"{prefix}/reference/loaders.md"] = render_loaders_page(prefix, 1)
        # Compute autodoc names from the reference-pages corpus (without the index)
        # then store the index with the accurate additional-functions count.
        ref_corpus = "\n".join(c for rel, c in out.items() if rel.startswith(f"{prefix}/") and rel.endswith(".md"))
        names = _autodoc_names(prefix, ref_corpus)
        out[f"{prefix}/index.md"] = render_league_index(
            prefix,
            has_additional=bool(names),
            additional_count=len(names),
        )
        result[prefix] = names
    out["reference/parameters.md"] = render_parameters_page()
    global_corpus = "\n".join(c for rel, c in out.items() if rel.endswith(".md")) + "\n" + preserved
    result[None] = _autodoc_names(None, global_corpus)
    return result


def refresh_autodoc_schemas() -> int:
    """Call every in-scope autodoc DataFrame-returning function and capture its
    column schema to ``schemas/autodoc/{scope}/{fn}.yaml`` (network; best-effort).

    For each autodoc name (per :func:`_autodoc_names_by_scope`) the example call
    args are looked up in ``autodoc_example_args.yaml`` (else ``{}``), the live
    function is invoked, and -- when the result is a polars/pandas DataFrame with
    >0 columns -- a ``kind: dataframe`` schema is written via :func:`_cols_from_frame`
    (descriptions left blank; there is no per-column authored source). Any failure
    (call error, non-DataFrame return, empty frame) is logged as a skip and the
    function falls back to its docstring Returns prose at render time. Pre-existing
    schemas for names that no longer capture are removed so the committed set never
    goes stale."""
    import importlib

    import yaml

    captured = skipped = 0
    skip_reasons: list[str] = []
    written_paths: set = set()
    args_by_scope = _autodoc_example_args()
    for scope, names in _autodoc_names_by_scope().items():
        scope_key = "global" if scope is None else scope
        mod = importlib.import_module("sportsdataverse" if scope is None else f"sportsdataverse.{scope}")
        scope_args = args_by_scope.get(scope_key, {})
        for fn in names:
            obj = getattr(mod, fn, None)
            if obj is None or not callable(obj):
                continue
            kwargs = scope_args.get(fn, {})
            try:
                df = obj(**kwargs)
            except Exception as e:  # noqa: BLE001
                msg = str(e).splitlines()[0][:120] if str(e) else type(e).__name__
                print(f"  autodoc skip {scope_key}.{fn}: {msg}")
                skip_reasons.append(f"{scope_key}.{fn}: {msg}")
                skipped += 1
                continue
            cols = getattr(df, "columns", None)
            ncols = (df.width if hasattr(df, "width") else len(cols)) if cols is not None else 0
            is_frame = hasattr(df, "columns") and hasattr(df, "dtypes") and not isinstance(df, dict)
            if not is_frame or ncols == 0:
                print(f"  autodoc skip {scope_key}.{fn}: not a non-empty DataFrame ({type(df).__name__})")
                skip_reasons.append(f"{scope_key}.{fn}: non-DataFrame ({type(df).__name__})")
                skipped += 1
                continue
            doc = {"schema": fn, "kind": "dataframe", "columns": _cols_from_frame(df, {})}
            outdir = _AUTODOC_SCHEMA_DIR / scope_key
            outdir.mkdir(parents=True, exist_ok=True)
            dest = outdir / f"{fn}.yaml"
            dest.write_text(yaml.safe_dump(doc, sort_keys=False, width=120), encoding="utf-8", newline="\n")
            written_paths.add(dest.resolve())
            captured += 1
    # Prune stale schema files (a function that no longer captures) so the
    # committed artifact set stays in lockstep with what the renderer can read.
    pruned = 0
    if _AUTODOC_SCHEMA_DIR.exists():
        for f in _AUTODOC_SCHEMA_DIR.rglob("*.yaml"):
            if f.resolve() not in written_paths:
                # Hand-authored schemas (functions the capture step cannot call:
                # frame-valued args or dict-of-frames returns) are kept, not pruned.
                if (yaml.safe_load(f.read_text(encoding="utf-8")) or {}).get("hand_authored"):
                    continue
                f.unlink()
                pruned += 1
    _autodoc_return_columns.cache_clear()
    print(f"autodoc schemas: {captured} captured, {skipped} skipped" + (f", {pruned} pruned" if pruned else ""))
    if skip_reasons:
        print("  sample skips: " + "; ".join(skip_reasons[:5]))
    return 0


def render_category(label: str, position: int, collapsed: bool) -> str:
    """Render a Docusaurus ``_category_.json`` sidebar descriptor."""
    template = render.ENV.get_template("category_json.jinja")
    return template.render(label=label, position=position, collapsed=collapsed)


def render_packages_page() -> str | None:
    """Render ``packages.mdx`` from the committed packages.json snapshot.

    Returns None when no snapshot exists (run ``fetch_packages.py`` to create one);
    the page is then simply omitted so the offline drift gate never needs the network."""
    import json

    p = ROOT / "tools" / "codegen" / "packages.json"
    if not p.exists():
        return None
    template = render.ENV.get_template("packages_page.mdx.jinja")
    return template.render(packages=json.loads(p.read_text(encoding="utf-8")))


def _doc_leagues() -> list[str]:
    """League prefixes to document: every ESPN league + loader-only leagues (pwhl) + HockeyTech junior leagues."""
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    prefixes = [lg.prefix for lg in cfg.leagues]
    extra = sorted({ld.league for ld in rel.loaders} - set(prefixes))
    # HockeyTech junior leagues have hand-written modules but no ESPN/loader entries.
    _HOCKEYTECH_EXTRA = _HOCKEYTECH_MODULE_LEAGUES
    # Cross-sport hand-written modules that get their own docs scope but have no
    # ESPN/loader entries (e.g. the The Odds API wrappers in sportsdataverse.odds).
    _NONLEAGUE_EXTRA = ["odds"]
    known = set(prefixes) | set(extra)
    hockeytech = [lg for lg in _HOCKEYTECH_EXTRA if lg not in known]
    nonleague = [m for m in _NONLEAGUE_EXTRA if m not in known]
    return prefixes + extra + hockeytech + nonleague


def _preserved_docs_corpus() -> str:
    """Concatenated text of the on-disk docs pages the generator does NOT own.

    These are the conceptual pages outside the generated roots (``intro.md``,
    ``quality-of-life.md``, ``architecture/``, ``parsers/``, ...). They are stable
    across a generation run (never clobbered/rewritten), so reading them here is
    idempotent. The autodoc gap judgment unions this with the freshly-rendered
    generated pages so a name already covered by a conceptual page is not
    redundantly re-documented on an autodoc page."""
    if not DOCS.exists():
        return ""
    roots = _generated_docs_roots()
    parts = []
    for f in sorted(DOCS.rglob("*.md")):
        top = f.relative_to(DOCS).parts[0]
        if top in roots:
            continue
        parts.append(f.read_text(encoding="utf-8"))
    return "\n".join(parts)


def _render_docs_all() -> dict[str, str]:
    """{relpath: content} for the full generated docs staging tree."""
    out: dict[str, str] = {}
    preserved = _preserved_docs_corpus()
    for i, prefix in enumerate(_doc_leagues()):
        apis = _apis_for(prefix)
        loaders = _loader_doc_views(prefix)
        out[f"{prefix}/_category_.json"] = render_category(prefix.upper(), 10 + i, True)
        # Sidebar order within a league's Reference category: Loaders first (1),
        # then native/flat APIs (NHL/MLB API; 10+), then ESPN APIs (site/web/core;
        # 20+). _apis_for returns ESPN first then flat, so assign position by kind
        # rather than by iteration order.
        espn_n = flat_n = 0
        for a in apis:
            if a["kind"] == "espn":
                pos = 20 + espn_n
                espn_n += 1
            else:
                pos = 10 + flat_n
                flat_n += 1
            out[f"{prefix}/reference/{a['slug']}.md"] = render_reference_page(prefix, a["name"], pos)
        if loaders:
            out[f"{prefix}/reference/loaders.md"] = render_loaders_page(prefix, 1)
        # Autodoc page for hand-written functions not covered by the generated
        # endpoint/loader pages above. "Already documented" is judged against the
        # other rendered pages for THIS league only (everything emitted so far
        # under `{prefix}/`), never against the autodoc page itself -- avoids
        # circularity and matches the coverage gate's per-league corpus
        # (`_docs_corpus(league)` = `docs/docs/{league}/**` only). Conceptual pages
        # outside the league dir are deliberately NOT consulted here so the autodoc
        # set stays in lockstep with what `--coverage` counts as missing.
        # Compute autodoc names from the reference-pages corpus (index excluded) so
        # the function count is available for the index table link.  The index
        # contains only table links, not function names, so excluding it from this
        # corpus does not change which names are "already documented".
        ref_corpus = "\n".join(c for rel, c in out.items() if rel.startswith(f"{prefix}/") and rel.endswith(".md"))
        autodoc_names_list = _autodoc_names(prefix, ref_corpus)
        # {slug: content} for this league's reference pages (already in `out`), used
        # to anchor each Python function in the parity table to its doc page.
        ref_prefix = f"{prefix}/reference/"
        ref_pages = {
            rel[len(ref_prefix) : -3]: c for rel, c in out.items() if rel.startswith(ref_prefix) and rel.endswith(".md")
        }
        out[f"{prefix}/index.md"] = render_league_index(
            prefix,
            has_additional=bool(autodoc_names_list),
            additional_count=len(autodoc_names_list),
            r_parity=_r_parity_rows(prefix, ref_pages, autodoc_names_list),
            r_pkg=_R_PARITY_PACKAGE.get(prefix),
        )
        # Compute the autodoc page against the reference-pages corpus only -- the
        # SAME corpus as autodoc_names_list above -- NOT the index. The index now
        # carries the Python<->R parity table, which mentions autodoc function names;
        # including it here would make render_autodoc_page consider those functions
        # "already documented" and drop them from additional.md, breaking the parity
        # links that point at additional#<fn>. Excluding the index keeps the autodoc
        # page in lockstep with autodoc_names_list (the index has no function-name
        # headers of its own, so this doesn't lose any real documentation signal).
        autodoc = render_autodoc_page(prefix, ref_corpus)
        if autodoc is not None:
            out[f"{prefix}/reference/{_AUTODOC_PAGE}"] = autodoc
        if apis or loaders or autodoc is not None:
            out[f"{prefix}/reference/_category_.json"] = render_category("Reference", 1, True)
    out["reference/parameters.md"] = render_parameters_page()
    # Package-level (global) autodoc page for hand-written package-level helpers
    # not found anywhere in the corpus: every rendered .md plus the preserved
    # conceptual pages, minus the global autodoc page itself.
    global_corpus = "\n".join(c for rel, c in out.items() if rel.endswith(".md")) + "\n" + preserved
    global_autodoc = render_autodoc_page(None, global_corpus)
    if global_autodoc is not None:
        out[f"reference/{_AUTODOC_GLOBAL_PAGE}"] = global_autodoc
    pkgs = render_packages_page()
    if pkgs is not None:
        out["packages.mdx"] = pkgs
    # Normalize every file to exactly one trailing newline so the generic
    # end-of-file-fixer / trailing-whitespace pre-commit hooks are a no-op and
    # never fight this drift gate (template whitespace control leaves some pages
    # with a double trailing newline).
    return {rel: content.rstrip() + "\n" for rel, content in out.items()}


def _generated_docs_roots() -> set[str]:
    """Top-level docs/docs subdirs the generator fully owns (and may clobber): every
    documented league dir + the shared ``reference/`` dir. Conceptual top-level pages
    (intro.md, quality-of-life.md, architecture/, parsers/) are NOT here and survive."""
    return set(_doc_leagues()) | {_DOCS_REFERENCE_DIR}


def build_docs() -> list[Path]:
    """Regenerate the per-league reference subtree into the live Docusaurus tree
    (docs/docs/). Full-clobbers each generated root (league dirs + reference/) and
    rewrites it from metadata; conceptual top-level pages are left untouched."""
    rendered = _render_docs_all()
    for root in _generated_docs_roots():
        d = DOCS / root
        if d.exists():
            shutil.rmtree(d)
    written = []
    for rel, content in rendered.items():
        dest = DOCS / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8", newline="\n")
        written.append(dest)
    return sorted(written)


def _docs_stale() -> list[str]:
    """Live docs/docs generated files that differ from a fresh render. Orphan-checks
    ONLY the fully-generated roots (league dirs + reference/) so preserved conceptual
    pages outside them are never flagged."""
    rendered = _render_docs_all()
    stale = []
    for rel, content in rendered.items():
        f = DOCS / rel
        if not f.exists() or f.read_text(encoding="utf-8") != content:
            stale.append(rel)
    for root in _generated_docs_roots():
        d = DOCS / root
        if not d.exists():
            continue
        for f in d.rglob("*"):
            if f.is_file() and f.relative_to(DOCS).as_posix() not in rendered:
                stale.append(f"{f.relative_to(DOCS).as_posix()} (orphan)")
    return sorted(stale)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="generate.py")
    ap.add_argument("--check", action="store_true", help="fail if any generated (staging or live) file is stale")
    ap.add_argument(
        "--audit-releases",
        action="store_true",
        help="compare releases.yaml against the live sportsdataverse-data release list (network)",
    )
    ap.add_argument(
        "--loader-schemas",
        action="store_true",
        help="re-introspect release parquet footers -> schemas/loader_schemas.yaml (network)",
    )
    ap.add_argument(
        "--schemas",
        action="store_true",
        help="introspect parsers against captured fixtures -> per-league return schemas (offline)",
    )
    ap.add_argument(
        "--autodoc-schemas",
        action="store_true",
        help="call autodoc DataFrame functions live -> schemas/autodoc Returns col tables (network)",
    )
    ap.add_argument(
        "--docs",
        action="store_true",
        help="regenerate the per-league reference subtree into docs/docs/ and exit",
    )
    ap.add_argument(
        "--coverage",
        action="store_true",
        help="report user-facing functions that never reach the rendered docs (offline; report-only)",
    )
    args = ap.parse_args(argv)
    if args.coverage:
        return coverage_report()
    if args.autodoc_schemas:
        return refresh_autodoc_schemas()
    if args.schemas:
        return refresh_return_schemas()
    if args.loader_schemas:
        return refresh_loader_schemas()
    if args.audit_releases:
        return audit_releases()
    if args.docs:
        d = len(build_docs())
        print(f"codegen --docs: wrote {d} doc files to {DOCS}")
        return 0
    if args.check:
        rc = check()
        live = _live_stale()
        if live:
            print("codegen --check: stale live files:", ", ".join(sorted(live)), file=sys.stderr)
            rc = 1
        parsed = _parsed_stale()
        if parsed:
            print("codegen --check: stale parsed files:", ", ".join(sorted(parsed)), file=sys.stderr)
            rc = 1
        flat = _flat_stale()
        if flat:
            print("codegen --check: stale flat-API files:", ", ".join(sorted(flat)), file=sys.stderr)
            rc = 1
        loaders = _loaders_stale()
        if loaders:
            print("codegen --check: stale loader files:", ", ".join(sorted(loaders)), file=sys.stderr)
            rc = 1
        docs = _docs_stale()
        if docs:
            print("codegen --check: stale doc files:", ", ".join(sorted(docs)), file=sys.stderr)
            rc = 1
        gaps = _coverage_gaps()
        if gaps:
            missing_names = [name for _label, names in gaps for name in names]
            print(
                "codegen --check: undocumented user-facing functions:",
                ", ".join(missing_names),
                file=sys.stderr,
            )
            rc = 1
        if rc == 0:
            print("codegen --check: all generated files current")
        return rc
    build()
    n = len(build_live())
    p = len(build_parsed_live())
    f = len(build_flat_live())
    ldr = len(build_loaders_live())
    d = len(build_docs())
    print(
        f"codegen: wrote {n} live + {p} parsed + {f} flat-API + {ldr} loader modules "
        f"+ {d} doc files + refreshed staging at {OUT}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
