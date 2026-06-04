"""Full URL+params parity: every generated ESPN fn builds the same request the
live core fn does, for both the minimal (required-only) and maximal (all-optional)
argument sets. This is stronger than signature parity -- it catches wrong paths,
wrong wire-keys, and missing value transforms that a name-only check would miss.

Covered via two leagues that together exercise every scope:
* cfb  -> universal + ncaa + football
* mlb  -> universal + mlb
"""

import importlib.util
import inspect
from pathlib import Path

import pytest
import yaml

from sportsdataverse import _common_espn as ce

from tools.codegen import extract, generate

OUT = Path("tools/codegen/_generated")
_ALL_SHORTS = set(extract._table().keys())
_RENAME = yaml.safe_load((Path("tools/codegen/rename_map.yaml")).read_text(encoding="utf-8")) or {}
# nhl is additive (new espn_nhl_* surface); the other 7 must reproduce the factory exactly
_EXISTING_PREFIXES = ["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb"]

# (prefix, sport, league, [tables...]) — tables whose shorts apply to this league
_LEAGUES = [
    ("cfb", "football", "college-football", ["universal", "ncaa", "football"]),
    ("mlb", "baseball", "mlb", ["universal", "mlb"]),
]
_TABLE_BY_SCOPE = {scope: tbl for scope, tbl in extract._TABLES}


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _args(core_fn, *, maximal: bool):
    sig = inspect.signature(core_fn)
    out = {}
    for p in sig.parameters.values():
        if p.name in ("sport", "league") or p.kind == p.VAR_KEYWORD:
            continue
        required = p.default is inspect.Parameter.empty
        if required or maximal:
            out[p.name] = extract._EXAMPLE.get(p.name, "1")
    return out


def _cap_core(core_fn, sport, league, args):
    box = {}

    def fake(url, params=None, **_kw):
        box["url"] = url
        box["params"] = {k: v for k, v in (params or {}).items() if v is not None}
        return {}

    orig = ce._get
    ce._get = fake
    try:
        core_fn(sport, league, **args)
    finally:
        ce._get = orig
    return box


def _cap_gen(gen_fn, gen_mod, args):
    box = {}

    def fake(url, params=None, **_kw):
        box["url"] = url
        box["params"] = {k: v for k, v in (params or {}).items() if v is not None}
        return {}

    orig = gen_mod._get
    gen_mod._get = fake
    try:
        gen_fn(**args)
    finally:
        gen_mod._get = orig
    return box


@pytest.fixture(scope="module", autouse=True)
def _build():
    generate.build()


@pytest.mark.parametrize("prefix,sport,league,scopes", _LEAGUES)
def test_generated_requests_match_core(prefix, sport, league, scopes):
    gen = _load(OUT / f"{prefix}_espn_ext.py", f"_gen_{prefix}")
    mismatches = []
    for scope in scopes:
        for short, core_fn in _TABLE_BY_SCOPE[scope]:
            clean = extract._clean_name(short, _ALL_SHORTS)
            gen_fn = getattr(gen, f"espn_{prefix}_{clean}", None)
            if gen_fn is None:
                mismatches.append(f"{short}: no generated espn_{prefix}_{clean}")
                continue
            for maximal in (False, True):
                args = _args(core_fn, maximal=maximal)
                want = _cap_core(core_fn, sport, league, args)
                got = _cap_gen(gen_fn, gen, args)
                if got.get("url") != want.get("url"):
                    mismatches.append(
                        f"{short} (max={maximal}) URL:\n   gen:  {got.get('url')}\n   core: {want.get('url')}",
                    )
                elif got.get("params") != want.get("params"):
                    mismatches.append(
                        f"{short} (max={maximal}) PARAMS: gen={got.get('params')} core={want.get('params')}",
                    )
    assert not mismatches, f"{len(mismatches)} parity mismatch(es) for {prefix}:\n" + "\n".join(mismatches[:40])


@pytest.mark.parametrize("prefix", _EXISTING_PREFIXES)
def test_no_factory_name_lost(prefix):
    """Every live *factory* espn_<prefix>_* name maps (via rename_map) to a generated fn.

    Scoped to the ``{prefix}_espn_ext`` module (the factory surface) so hand-written
    league functions in sibling modules (e.g. espn_mlb_pbp) aren't falsely counted.
    """
    gen = _load(OUT / f"{prefix}_espn_ext.py", f"_inv_{prefix}")
    live_ext = __import__(f"sportsdataverse.{prefix}.{prefix}_espn_ext", fromlist=["x"])
    live_names = [n for n in getattr(live_ext, "__all__", []) if n.startswith(f"espn_{prefix}_")]
    assert live_names, f"no factory names found on sportsdataverse.{prefix}.{prefix}_espn_ext"
    missing = [f"{n} -> {_RENAME.get(n, n)}" for n in live_names if not hasattr(gen, _RENAME.get(n, n))]
    assert not missing, f"{prefix}: {len(missing)} factory name(s) absent from generated: {missing[:15]}"
