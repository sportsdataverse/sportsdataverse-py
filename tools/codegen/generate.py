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

ESPN_APIS = ["espn_site_v2"]  # extended in later plans

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
) -> str:
    """Build a function docstring as a 4-space-indented block (precise indentation).

    Shared renderer (Python, not a Jinja macro) so every generated family emits the
    same docstring contract without Jinja whitespace-control fragility.
    """
    lines = [f'"""{ep.summary}', ""]
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

    def __init__(self, ep: spec.Endpoint, fn_name: str, ep_host: str, league: spec.League):
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
        self.docstring = _build_docstring(ep, league.sport, league.league, ep_host, self.example_url, self.example_call)

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
            head, tail = ep.path.split("[", 1)
            tail = tail.rstrip("]")  # e.g. "/{stat_type}"
            seg_param = _path_token_first(tail)
            head_f = _sub_slugs(head, sport, lg)
            lines.append(f'__suffix = f"{tail}" if {seg_param} is not None else ""')
            lines.append(f'__url = f"{ep_host}{head_f}" + __suffix')
        elif ep.now_variant:
            toggle = ep.path_params[-1].python_name
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


def _league_module_source(league: spec.League, apis, hosts) -> str:
    """Render the (unformatted) module source; ruff formatting happens at write time."""
    endpoints = []
    parser_imports = set()
    transforms = set()
    for api in apis:
        host_url = hosts[api.host]
        for ep in api.endpoints:
            if ep.scope not in league.scopes:
                continue
            if league.league in ep.exclude_leagues:
                continue
            ep_host = hosts[ep.host] if ep.host else host_url
            fn_name = api.name_pattern.format(prefix=league.prefix, short=ep.short)
            if ep.parser:
                parser_imports.add(ep.parser)
            for p in ep.path_params:
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
    ap.add_argument("--check", action="store_true", help="fail if any generated file is stale")
    args = ap.parse_args(argv)
    if args.check:
        return check()
    build()
    print(f"codegen: wrote {len(list(OUT.glob('*_espn_ext.py')))} modules to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
