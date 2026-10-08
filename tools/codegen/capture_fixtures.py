"""Capture one real payload per schema-less endpoint into tests/fixtures/.

``generate.py --schemas`` is offline: it turns a COMMITTED capture into a returns
schema. This CLI gets the capture. It calls the generated wrapper with the same
``example_args`` the docs show and ``return_parsed=False``, and writes the raw
payload where ``refresh_return_schemas`` reads it:

* an ESPN endpoint -> ``tests/fixtures/espn/<short>_<league>.json``, captured for ONE
  representative league (the parser fixes the shape, not the league);
* a flat-API endpoint -> ``tests/fixtures/<api>/<short>.json``, registered in
  ``native_fixture_map.yaml``.

A failed or empty capture NEVER writes a file: it prints a ``skip`` line naming the
cause. An empty returns table would silently claim the endpoint has no columns.

    uv run python tools/codegen/capture_fixtures.py --api espn_core_v2 --all-missing
    uv run python tools/codegen/capture_fixtures.py --api fox_api --all-missing --sleep 1.5
"""

from __future__ import annotations

import argparse
import functools
import importlib
import inspect
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.codegen import generate, spec  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
NATIVE_MAP = ROOT / "tools" / "codegen" / "native_fixture_map.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas"

# Representative league for an ESPN capture: the first of these that the endpoint reaches.
_PREFERRED = ("nba", "nfl", "mlb", "nhl", "wnba", "cfb", "mbb", "wbb")


@functools.lru_cache(maxsize=None)
def _views(api: str) -> dict[str, dict[str, object]]:
    """``{short: {league_prefix: view}}`` for every wrapper of ``api`` (one ``""`` key for a flat API)."""
    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    out: dict[str, dict[str, object]] = {}
    if api in generate.ESPN_APIS:
        cfg = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
        apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
        for league in cfg.leagues:
            for v in generate._espn_league_views(league, apis, cfg.hosts):
                if v.api_name == api:
                    out.setdefault(v.short, {})[league.prefix] = v
        return out
    prefix = dict(generate.FLAT_APIS)[api]
    fa = spec.load_flat_api(generate.ENDPOINTS / f"{api}.yaml", params)
    for v in generate._flat_views(fa, league_prefix=prefix):
        out.setdefault(v.short, {})[""] = v
    return out


def _endpoints(api: str) -> list:
    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    y = generate.ENDPOINTS / f"{api}.yaml"
    loaded = spec.load_espn_api(y, params) if api in generate.ESPN_APIS else spec.load_flat_api(y, params)
    return list(loaded.endpoints)


def _flat_schema_exists(api: str, short: str) -> bool:
    import yaml

    p = SCHEMAS / "native" / api / f"{short}.yaml"
    if not p.exists():
        return False
    doc = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return bool(doc.get("columns") or doc.get("frames") or "unverified" in doc)


def missing_schema_endpoints(api: str) -> list[str]:
    """Endpoint shorts of ``api`` that have no returns schema and no ``unverified:`` reason."""
    if api in generate.ESPN_APIS:
        return [ep.short for ep in _endpoints(api) if ep.returns_schema is None]
    return [ep.short for ep in _endpoints(api) if ep.returns_schema is None and not _flat_schema_exists(api, ep.short)]


def representative_league(api: str, short: str) -> str:
    """The league an ESPN endpoint is captured for: a preferred one it reaches, else its first."""
    if api not in generate.ESPN_APIS:
        return ""
    reach = _views(api).get(short, {})
    return next((lg for lg in _PREFERRED if lg in reach), next(iter(reach), ""))


def _candidate_leagues(api: str, short: str, limit: int = 8) -> list[str]:
    """The representative league first, then up to ``limit - 1`` more that reach the endpoint."""
    if api not in generate.ESPN_APIS:
        return [""]
    reach = list(_views(api).get(short, {}))
    ordered = [lg for lg in _PREFERRED if lg in reach] + [lg for lg in reach if lg not in _PREFERRED]
    return ordered[:limit] or [""]


def fixture_path(api: str, short: str, league: str = "") -> Path:
    """Where ``refresh_return_schemas`` reads this endpoint's capture."""
    if api in generate.ESPN_APIS:
        return FIXTURES / "espn" / f"{short}_{league}.json"
    return FIXTURES / api / f"{short}.json"


def _call(api: str, short: str, league: str):
    """Invoke the generated wrapper for one endpoint with its documented example and return the RAW payload."""
    view = _views(api)[short][league]
    if api in generate.ESPN_APIS:
        # The generated module itself: a hand-written function of the same name (espn_nba_calendar)
        # shadows the wrapper on the league package and takes different arguments.
        mod = importlib.import_module(
            f"sportsdataverse.{generate._LEAGUE_MODULE.get(league, league)}.{league}_espn_ext"
        )
    else:
        prefix = dict(generate.FLAT_APIS)[api]
        fa_module = spec.load_flat_api(
            generate.ENDPOINTS / f"{api}.yaml", spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
        ).module
        mod = importlib.import_module(f"sportsdataverse.{generate._LEAGUE_MODULE.get(prefix, prefix)}.{fa_module}")
    fn = getattr(mod, view.fn_name)
    # A wrapper with no parser returns the raw payload already and takes no return_parsed.
    raw = {"return_parsed": False} if "return_parsed" in inspect.signature(fn).parameters else {}
    return fn(**dict(view.example_args), **raw)


def _register_native(api: str, short: str, fname: str) -> None:
    """Add ``fname: short`` under ``api`` in native_fixture_map.yaml, keeping its comments."""
    text = NATIVE_MAP.read_text(encoding="utf-8")
    line = f"  {fname}: {short}\n"
    if line in text:
        return
    head = f"{api}:\n"
    if head not in text:
        NATIVE_MAP.write_text(text.rstrip("\n") + f"\n{head}{line}", encoding="utf-8", newline="\n")
        return
    i = text.index(head) + len(head)
    NATIVE_MAP.write_text(text[:i] + line + text[i:], encoding="utf-8", newline="\n")


def capture(api: str, short: str, league: str = "", *, dry_run: bool = False) -> tuple[bool, str]:
    """Capture one endpoint's payload. Returns ``(ok, reason)`` and writes nothing on failure."""
    dest = fixture_path(api, short, league)
    where = f"{api}/{short}" + (f" [{league}]" if league else "")
    if dry_run:
        return True, f"dry-run: would capture {where} -> {dest.relative_to(dest.parents[2]).as_posix()}"
    try:
        payload = _call(api, short, league)
    except Exception as e:  # noqa: BLE001 -- every failure is reported, none is fatal
        return False, f"{type(e).__name__}: {(str(e).splitlines() or [''])[0][:200]}"
    if not payload:
        return False, "the live call returned an empty payload"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        json.dumps(payload, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n"
    )
    if api not in generate.ESPN_APIS:
        _register_native(api, short, dest.name)
    return True, f"captured {dest.relative_to(ROOT).as_posix() if dest.is_relative_to(ROOT) else dest}"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Always exits 0: a skipped endpoint is reported, not an error."""
    ap = argparse.ArgumentParser(prog="capture_fixtures.py")
    ap.add_argument("--api", required=True, help="an ESPN API name or a flat-API YAML stem")
    ap.add_argument("--league", default="", help="ESPN league prefix (default: each endpoint's representative league)")
    ap.add_argument("--endpoints", default="", help="comma-separated endpoint shorts")
    ap.add_argument("--all-missing", action="store_true", help="every endpoint with no returns schema")
    ap.add_argument("--dry-run", action="store_true", help="list what would be captured; write nothing")
    ap.add_argument(
        "--sleep", type=float, default=1.5, help="seconds between live calls (ESPN Core v2 403s when hurried)"
    )
    args = ap.parse_args(argv)
    shorts = [s for s in args.endpoints.split(",") if s] or (
        missing_schema_endpoints(args.api) if args.all_missing else []
    )
    ok = bad = 0
    for short in shorts:
        # An endpoint's base example args can belong to one league (group 80 is college football), so a
        # failed ESPN capture is retried in the next leagues that reach it before it is reported.
        leagues = [args.league] if args.league else _candidate_leagues(args.api, short)
        for i, league in enumerate(leagues):
            good, reason = capture(args.api, short, league, dry_run=args.dry_run)
            if not args.dry_run:
                time.sleep(args.sleep)
            if good or args.dry_run or i == len(leagues) - 1:
                break
        print(f"  {'ok  ' if good else 'skip'} {args.api}/{short}: {reason}", flush=True)
        ok, bad = (ok + 1, bad) if good else (ok, bad + 1)
    print(f"capture_fixtures: {ok} captured, {bad} skipped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
