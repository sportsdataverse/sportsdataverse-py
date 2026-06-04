"""Codegen CLI: render concrete ESPN league modules from YAML (build / --check)."""

from __future__ import annotations

import argparse
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


def _league_module_source(league: spec.League, apis, hosts) -> str:
    """Render the (unformatted) module source; ruff formatting happens at write time."""
    renames = _load_espn_renames()
    drops = _load_espn_drops()
    handwritten = _handwritten_espn_names(league.prefix)
    # pass 1: collect endpoints + their base (pre-rename) names for this league
    collected = []  # (ep, ep_host, base_name)
    for api in apis:
        host_url = hosts[api.host]
        for ep in api.endpoints:
            if ep.scope not in league.scopes or league.league in ep.exclude_leagues:
                continue
            base = api.name_pattern.format(prefix=league.prefix, short=ep.short)
            if base in drops:
                continue  # a hand-written sibling already exposes this exact endpoint
            ep_host = hosts[ep.host] if ep.host else host_url
            collected.append((ep, ep_host, base))
    base_names = {b for _, _, b in collected}

    # pass 2: apply the R-alignment renames with a collision guard (skip + record any
    # rename that would clash with an existing generated name, a hand-written sibling,
    # or another already-assigned rename).
    endpoints = []
    parser_imports = set()
    transforms = set()
    used: set[str] = set()
    for ep, ep_host, base in collected:
        fn_name = base
        # static override wins; else the universal structural convention
        new = renames.get(base) or f"espn_{league.prefix}_{_convention_rename(ep.short)}"
        if new != base:
            if new in base_names or new in handwritten or new in used:
                # Collision: keep BOTH by version-qualifying the larger/newer endpoint
                # (one stays bare) instead of dropping the rename. Falls back to a
                # recorded skip only when even the versioned name is unavailable.
                versioned = _versioned_on_collision(ep.short, league.prefix)
                if versioned and versioned not in base_names and versioned not in handwritten and versioned not in used:
                    fn_name = versioned
                else:
                    _ESPN_RENAME_SKIPPED[base] = f"{new} (collision)"
            else:
                fn_name = new
        used.add(fn_name)
        if ep.parser:
            parser_imports.add(ep.parser)
        for p in (*ep.path_params, *ep.query_params):
            if p.transform:
                transforms.add(p.transform)
        endpoints.append(_EndpointView(ep, fn_name, ep_host, league))
    runtime_imports = ["_get"] + sorted(transforms)
    template = render.ENV.get_template("espn_league_module.py.jinja")
    return template.render(
        prefix=league.prefix,
        sport=league.sport,
        league=league.league,
        endpoints=endpoints,
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


def render_flat_module(api: spec.FlatApi) -> str:
    """Render a flat (non-sport/league) API module (NHL api-web/edge/..., MLB stats).

    When ``api.qualifier`` is set, each function gets a clean ``{prefix}_{short}``
    name, qualified to ``{prefix}_{qualifier}_{short}`` only on collision with a
    hand-written composite or another generated name in this module."""
    reserved = reserved_names(api.prefix, exclude_modules=(api.module,)) if api.qualifier else set()
    used: set[str] = set()
    endpoints = []
    parser_imports = set()
    transforms = set()
    for ep in api.endpoints:
        if api.qualifier:
            fn_name = resolve_name(api.prefix, ep.short, reserved | used, api.qualifier)
        else:
            fn_name = api.name_pattern.format(short=ep.short)
        used.add(fn_name)
        if ep.parser:
            parser_imports.add(ep.parser)
        for p in (*ep.path_params, *ep.query_params):
            if p.transform:
                transforms.add(p.transform)
        ep_host = ep.host or api.host
        endpoints.append(_EndpointView(ep, fn_name, ep_host, _FLAT_STUB_LEAGUE, flat=True))
    runtime_imports = list(dict.fromkeys([*api.runtime_imports, *sorted(transforms)]))
    template = render.ENV.get_template("api_module.py.jinja")
    return template.render(
        api=api.api,
        host=api.host,
        module=api.module,
        parser_module=api.parser_module,
        endpoints=endpoints,
        parser_imports=sorted(parser_imports),
        runtime_imports=runtime_imports,
        passthrough_query=api.passthrough_query,
    )


def _build_loader_docstring(ld: spec.Loader) -> str:
    """4-space-indented docstring block for a generated dataset loader."""
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


def audit_releases() -> int:
    """Compare the releases.yaml manifest against the LIVE sportsdataverse-data
    release list (network, via ``gh``). Reports release tags with no loader
    (gaps) and manifest tags no longer published (orphans). Informational drift
    gate -- meant for a CI job, not the offline ``--check``."""
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    manifest = {ld.tag for ld in rel.loaders}
    try:
        live = {t for t in gh_release_tags() if _is_loader_tag(t)}
    except Exception as e:  # noqa: BLE001
        print(f"--audit-releases: could not query gh release list ({e})", file=sys.stderr)
        return 2
    missing = sorted(live - manifest)
    orphan = sorted(manifest - live)
    if missing or orphan:
        print(f"release manifest drift: {len(missing)} tag(s) without a loader, {len(orphan)} orphan(s)", file=sys.stderr)
        if missing:
            print("  missing loaders for:", ", ".join(missing), file=sys.stderr)
        if orphan:
            print("  orphan manifest tags:", ", ".join(orphan), file=sys.stderr)
        return 1
    print(f"release manifest matches live release list ({len(manifest)} tags)")
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="generate.py")
    ap.add_argument("--check", action="store_true", help="fail if any generated (staging or live) file is stale")
    ap.add_argument(
        "--audit-releases",
        action="store_true",
        help="compare releases.yaml against the live sportsdataverse-data release list (network)",
    )
    args = ap.parse_args(argv)
    if args.audit_releases:
        return audit_releases()
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
        return rc
    build()
    n = len(build_live())
    p = len(build_parsed_live())
    f = len(build_flat_live())
    print(f"codegen: wrote {n} live + {p} parsed + {f} flat-API modules + refreshed staging at {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
