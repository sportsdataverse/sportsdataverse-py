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
) -> str:
    """Build a function docstring as a 4-space-indented block (precise indentation).

    Shared renderer (Python, not a Jinja macro) so every generated family emits the
    same docstring contract without Jinja whitespace-control fragility. ``flat`` omits
    the sport/league binding line for the non-sport/league NHL/MLB APIs.
    """
    lines = [f'"""{ep.summary}', ""]
    if not flat:
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
    if ep.parser:
        lines.append(f"    return_parsed: dispatch the raw payload through {ep.parser} -> polars DataFrame.")
        lines.append("    return_as_pandas: with return_parsed, return a pandas DataFrame instead of polars.")
    lines.append("")
    lines.append("Returns:")
    if ep.parser:
        lines.append("    polars/pandas DataFrame when ``return_parsed=True``, else the raw JSON ``Dict``.")
    else:
        lines.append("    The raw JSON ``Dict``.")
    lines.append("")
    lines.append("Example:")
    lines.append(f"    >>> {example_call}")
    lines.append('"""')
    return "\n".join(("    " + ln) if ln else "" for ln in lines)


def _param_rows(ep: spec.Endpoint) -> list[dict]:
    """Merge path + query params into nba_api-style doc rows.

    ``required`` mirrors the spec; ``nullable`` is its inverse (an optional param
    may be omitted/``None``). Path params come first, matching the signature order
    used by the generated wrapper.
    """
    rows: list[dict] = []
    for p in (*ep.path_params, *ep.query_params):
        rows.append(
            {
                "api": p.api,
                "python": p.python_name,
                "pattern": p.pattern,
                "required": p.required,
                "nullable": not p.required,
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
        return head + "".join(f"| `{c['name']}` | {c.get('type', '')} | {c.get('description', '')} |\n" for c in cols)

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

    def __init__(self, ep: spec.Endpoint, fn_name: str, ep_host: str, league: spec.League, flat: bool = False):
        self.fn_name = fn_name
        self.short = ep.short
        self.summary = ep.summary
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

        bare = ep.path.replace("[", "").replace("]", "")
        url = f"{ep_host}{_sub_slugs(bare, league.sport, league.league)}"
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
            self.url_literal = ('f"' + url + '"') if ep.path_params else ('"' + url + '"')

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
        )

        # ---- docs-rendering fields (consumed by _reference_block.jinja) ----
        # Every attribute below is read under StrictUndefined, so all must exist.
        self.endpoint_url = f"{ep_host}{_sub_slugs(ep.path, league.sport, league.league)}"
        self.valid_url = self.example_url
        self.param_rows = _param_rows(ep)
        self.return_table = _return_table(ep.returns_schema, league.prefix)
        self.returns_prose = ep.summary
        self.r_equivalent: dict[str, str] = {}  # reserved for a future R cross-ref map
        self.notebook: str | None = None
        self.last_validated: str | None = None
        self.api_name = ""  # set by _espn_league_views for per-API docs filtering

    @staticmethod
    def _build_path_expr(ep: spec.Endpoint, ep_host: str, league: spec.League) -> str:
        """Emit Python statements (newline+4-space joined) that assign ``__url``."""
        sport, lg = league.sport, league.league
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
            lines.append(f'__url = f"{now_f}" if {toggle} is None else f"{full_f}"')
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
        subprocess.run(["ruff", "format", str(path)], capture_output=True, text=True, check=False)
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
    return views


def _league_module_source(league: spec.League, apis, hosts) -> str:
    """Render the (unformatted) module source; ruff formatting happens at write time."""
    views = _espn_league_views(league, apis, hosts)
    parser_imports = {v.parser for v in views if v.parser}
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
        parser_imports=sorted(parser_imports),
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


def _flat_views(api: spec.FlatApi) -> list[_EndpointView]:
    """Resolve a flat API's endpoints to their final wrapper names + views.

    Shared by the module renderer (:func:`render_flat_module`) and the docs renderer
    (:func:`render_reference_page`). When ``api.qualifier`` is set, each function gets
    a clean ``{prefix}_{short}`` name, qualified to ``{prefix}_{qualifier}_{short}``
    only on collision with a hand-written composite or another generated name."""
    reserved = reserved_names(api.prefix, exclude_modules=(api.module,)) if api.qualifier else set()
    used: set[str] = set()
    views: list[_EndpointView] = []
    for ep in api.endpoints:
        if api.qualifier:
            fn_name = resolve_name(api.prefix, ep.short, reserved | used, api.qualifier)
        else:
            fn_name = api.name_pattern.format(short=ep.short)
        used.add(fn_name)
        ep_host = ep.host or api.host
        views.append(_EndpointView(ep, fn_name, ep_host, _FLAT_STUB_LEAGUE, flat=True))
    return views


def render_flat_module(api: spec.FlatApi) -> str:
    """Render a flat (non-sport/league) API module (NHL api-web/edge/..., MLB stats)."""
    views = _flat_views(api)
    parser_imports = {v.parser for v in views if v.parser}
    transforms: set[str] = set()
    for v in views:
        for p in (*v.path_params, *v.query_params):
            if p.transform:
                transforms.add(p.transform)
    runtime_imports = list(dict.fromkeys([*api.runtime_imports, *sorted(transforms)]))
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
    lines.append(f"    >>> {ld.fn}(seasons={ld.example_args.get('seasons', 2024)!r})")
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
        dest.write_text(src, encoding="utf-8")
        init = pkg / "__init__.py"
        line = f"from sportsdataverse.{lg}.{lg}_loaders import *\n"
        if not init.exists():
            init.write_text(
                f'"""sportsdataverse.{lg} -- {lg.upper()} data loaders."""\n\nfrom __future__ import annotations\n\n'
                + line,
                encoding="utf-8",
            )
        elif line not in init.read_text(encoding="utf-8"):
            init.write_text(init.read_text(encoding="utf-8").rstrip() + "\n" + line, encoding="utf-8")
        written.append(dest)
    if written:
        subprocess.run(["ruff", "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
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
            (tmp / f"{lg}_loaders.py").write_text(src, encoding="utf-8")
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
    return sorted({line.split("\t")[0].strip() for line in out.splitlines() if line.strip()})


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
                doc = {"schema": name, "kind": "dataframe", "columns": _cols_from_frame(df, descs)}
            (outdir / f"{league}.yaml").write_text(yaml.safe_dump(doc, sort_keys=False, width=120), encoding="utf-8")
            written += 1
    print(f"return schemas: {written} per-league written, {skipped} skipped (no fixture)")
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
    dest.write_text(yaml.safe_dump(out, sort_keys=True, width=120), encoding="utf-8")
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
        dest.write_text(src, encoding="utf-8")
        written.append(dest)
    if written:
        subprocess.run(["ruff", "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
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
]


def _render_flat_all() -> dict[str, tuple[str, str]]:
    """{module_name: (league_prefix, src)} for each flat-API YAML that exists."""
    out: dict[str, tuple[str, str]] = {}
    for stem, prefix in FLAT_APIS:
        y = ENDPOINTS / f"{stem}.yaml"
        if not y.exists():
            continue
        api = spec.load_flat_api(y, {})
        out[api.module] = (prefix, render_flat_module(api))
    return out


def build_flat_live() -> list[Path]:
    """Write generated flat-API modules (NHL native, MLB stats) into the live package."""
    written = []
    for module, (prefix, src) in _render_flat_all().items():
        dest = LIVE / prefix / f"{module}.py"
        dest.write_text(src, encoding="utf-8")
        written.append(dest)
    if written:
        subprocess.run(["ruff", "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
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
            (tmp / f"{module}.py").write_text(src, encoding="utf-8")
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
            (tmp / name).write_text(src, encoding="utf-8")
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
    (OUT / "__init__.py").write_text("", encoding="utf-8")
    for name, src in _render_all().items():
        (OUT / name).write_text(src, encoding="utf-8")
    _ruff_format_dir(OUT)
    return sorted(OUT.glob("*_espn_ext.py"))


def _ensure_init_import(prefix: str) -> None:
    """Idempotently make ``sportsdataverse/{prefix}/__init__.py`` re-export the module."""
    init = LIVE / prefix / "__init__.py"
    text = init.read_text(encoding="utf-8")
    if f"{prefix}_espn_ext import *" in text:
        return
    line = f"from sportsdataverse.{prefix}.{prefix}_espn_ext import *\n"
    init.write_text(text.rstrip() + "\n" + line, encoding="utf-8")


def build_live() -> list[Path]:
    """Write the concrete generated modules into the live package + wire __init__."""
    written = []
    for name, src in _render_all().items():
        prefix = name[: -len("_espn_ext.py")]
        dest = LIVE / prefix / f"{prefix}_espn_ext.py"
        dest.write_text(src, encoding="utf-8")
        _ensure_init_import(prefix)
        written.append(dest)
    if written:
        subprocess.run(["ruff", "format", *[str(p) for p in written]], capture_output=True, text=True, check=False)
    return sorted(written)


def _live_stale() -> list[str]:
    """Live ``{prefix}_espn_ext.py`` files that differ from a fresh render."""
    tmp = OUT / "_check_live_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        rendered = _render_all()
        for name, src in rendered.items():
            (tmp / name).write_text(src, encoding="utf-8")
        _ruff_format_dir(tmp)
        stale = []
        for name in rendered:
            prefix = name[: -len("_espn_ext.py")]
            live_file = LIVE / prefix / f"{prefix}_espn_ext.py"
            if not live_file.exists() or live_file.read_text(encoding="utf-8") != (tmp / name).read_text(
                encoding="utf-8",
            ):
                stale.append(str(live_file.relative_to(ROOT)))
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
            (tmp / name).write_text(src, encoding="utf-8")
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
    print("codegen --check: all generated files current")
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
}


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
        out.append(
            {
                "fn": ld.fn,
                "tag": ld.tag,
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
        endpoints = _flat_views(fa)
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


def render_league_index(prefix: str) -> str:
    """Render a league's ``index.md`` (reference table + optional loaders link)."""
    loaders = _loader_doc_views(prefix)
    template = render.ENV.get_template("league_index.md.jinja")
    return template.render(
        prefix=prefix,
        api_rows=_apis_for(prefix),
        has_loaders=bool(loaders),
        loader_count=len(loaders),
        notebooks=[],
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
    """League prefixes to document: every ESPN league + any loader-only league (pwhl)."""
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    prefixes = [lg.prefix for lg in cfg.leagues]
    extra = sorted({ld.league for ld in rel.loaders} - set(prefixes))
    return prefixes + extra


def _render_docs_all() -> dict[str, str]:
    """{relpath: content} for the full generated docs staging tree."""
    out: dict[str, str] = {}
    for i, prefix in enumerate(_doc_leagues()):
        apis = _apis_for(prefix)
        loaders = _loader_doc_views(prefix)
        out[f"{prefix}/index.md"] = render_league_index(prefix)
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
        if apis or loaders:
            out[f"{prefix}/reference/_category_.json"] = render_category("Reference", 1, True)
    out["reference/parameters.md"] = render_parameters_page()
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
        "--docs",
        action="store_true",
        help="regenerate the per-league reference subtree into docs/docs/ and exit",
    )
    args = ap.parse_args(argv)
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
