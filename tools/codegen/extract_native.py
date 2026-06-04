"""Faithful extraction of the hand-written NHL/MLB flat modules -> codegen YAML.

Unlike ``openapi_to_endpoints`` (which derives endpoints from OpenAPI specs that
diverge from the curated hand-written API), this extracts from the **hand-written
functions themselves** -- the source of truth. Each public function is runtime-
captured (minimal + maximal real-valued args) under a patched ``download``; the
captured URL is reverse-mapped to a ``{token}`` path template, and now_variant /
now_toggle / season transform / defaults are recovered by diffing the two calls.

Behavior is preserved byte-for-byte (guarded by ``test_parity_native``); only the
codegen metadata is derived. This is a build-time tool, run against the
pre-regeneration modules.
"""

from __future__ import annotations

import importlib
import inspect
from typing import Dict, List, Optional


from sportsdataverse._codegen_runtime import format_nhl_season

# representative real values per param name (must satisfy the hand-written
# functions' own validation, e.g. format_nhl_season requires 4/8-digit seasons)
_VALUES = {
    "game_id": 2024020001,
    "player_id": 8480801,
    "team": "TOR",
    "team_id": 10,
    "season": 2025,
    "game_type": 2,
    "date": "2024-01-01",
    "year": 2024,
    "month": "2024-01",
    "series_letter": "a",
    "round_number": 1,
    "sort_by": "points",
    "categories": "points",
    "limit": 50,
    "cayenne_exp": "seasonId=20242025",
    "lang": "en",
    "franchise_id": 1,
    "trophy_id": 1,
    "report": "summary",
    "game_pk": 716390,
    "person_id": 660271,
    "sport_id": 1,
    "league_id": 103,
    "division_id": 200,
    "venue_id": 15,
    "award_id": "MLBHOF",
    "meta_type": "leagueLeaderTypes",
    "stat_type": "season",
    "group": "hitting",
    "hydrate": "team",
    "fields": "dates",
}


def _val(name: str):
    return _VALUES.get(name, "X")


def _capture(module, fn, args: dict):
    """Call ``fn(**args)`` under a patched ``module.download``; return (url, params)."""
    box: dict = {}

    def fake_download(url=None, params=None, **kw):
        box["url"] = url
        box["params"] = params
        return None  # fn's _fetch/_get handles None resp -> {}

    orig = module.download
    module.download = fake_download
    try:
        fn(**args)
    finally:
        module.download = orig
    return box.get("url"), box.get("params")


def _to_template(url: str, base: str, params_in_url: Dict[str, object]) -> str:
    """Strip the base host and replace each path *segment* that equals a param value
    with ``{name}`` (segment-aware, so short values like ``2``/``a`` don't corrupt
    literal segments such as ``play-by-play`` or ``season``)."""
    path = url[len(base) :] if url.startswith(base) else url
    head, sep, query = path.partition("?")
    val_to_name = {str(v): n for n, v in params_in_url.items()}
    out = ["{" + val_to_name[seg] + "}" if seg in val_to_name else seg for seg in head.split("/")]
    return "/".join(out) + (sep + query if sep else "")


def describe_fn(module, name: str, fn, base: str, strip_prefix: str) -> Optional[dict]:
    """Describe one hand-written function as a flat endpoint dict (or None to skip)."""
    sig = inspect.signature(fn)
    params = [p for p in sig.parameters.values() if p.name != "kwargs" and p.kind != p.VAR_KEYWORD]
    required = [p for p in params if p.default is inspect.Parameter.empty]
    none_default = [p for p in params if p.default is None]
    set_default = [p for p in params if p.default is not inspect.Parameter.empty and p.default is not None]

    args_min = {p.name: _val(p.name) for p in required}
    args_min.update({p.name: p.default for p in set_default})
    args_max = {p.name: _val(p.name) for p in params}
    try:
        url_min, q_min = _capture(module, fn, args_min)
        url_max, q_max = _capture(module, fn, args_max)
    except Exception:
        return None
    if not url_max:
        return None

    # values that appear in the maximal URL, keyed by param name (transform-aware).
    # Check the *transformed* season first ("20242025") so the raw "2025" substring
    # doesn't claim only part of the segment.
    in_url_max: Dict[str, object] = {}
    transforms: Dict[str, str] = {}
    url_segs_max = set(url_max.split("/"))
    for p in params:
        v = args_max[p.name]
        if p.name == "season":
            tv = str(format_nhl_season(v))
            if tv in url_segs_max:
                in_url_max[p.name] = tv
                transforms[p.name] = "format_nhl_season"
                continue
        if str(v) in url_segs_max:
            in_url_max[p.name] = v

    full_path = _to_template(url_max, base, in_url_max)

    # now_variant: only meaningful when a None-default param's absence changes the URL
    now_variant = None
    now_toggle = None
    if none_default and url_min and url_min != url_max:
        in_url_min = {n: v for n, v in in_url_max.items() if str(v) in set(url_min.split("/"))}
        min_path = _to_template(url_min, base, in_url_min)
        if min_path != full_path:
            now_variant = min_path
            for p in none_default:
                if p.name in in_url_max and p.name not in in_url_min:
                    now_toggle = p.name
                    break

    path_param_names = {p.name for p in params if ("{" + p.name + "}") in full_path}
    path_params, query_params = [], []
    for p in params:
        if p.name in path_param_names:
            entry = {"name": p.name, "type": _ptype(p), "required": p.default is inspect.Parameter.empty}
            if p.name in transforms:
                entry["transform"] = transforms[p.name]
            if p.default is not inspect.Parameter.empty and p.default is not None:
                entry["default"] = p.default
            path_params.append(entry)
        else:
            # query param (appears in captured params dict, or just not in path)
            wire = _wire_key(q_max, p.name)
            entry = {"name": p.name, "query_key": wire, "type": _ptype(p)}
            if p.default is not inspect.Parameter.empty and p.default is not None:
                entry["default"] = p.default
            if p.name in transforms:
                entry["transform"] = transforms[p.name]
            query_params.append(entry)

    short = name[len(strip_prefix) :] if name.startswith(strip_prefix) else name
    ep = {"short": short, "summary": (fn.__doc__ or "").strip().split("\n")[0], "path": full_path}
    if now_variant:
        ep["now_variant"] = now_variant
        if now_toggle:
            ep["now_toggle"] = now_toggle
    if path_params:
        ep["path_params"] = path_params
    if query_params:
        ep["extra_params"] = query_params
    ex = {p["name"]: _val(p["name"]) for p in path_params if p.get("required")}
    if ex:
        ep["example_args"] = ex
    return ep


def _ptype(p) -> str:
    ann = p.annotation
    if ann is inspect.Parameter.empty:
        return "str"
    s = str(ann).replace("typing.", "")
    if s.startswith("Optional[") and s.endswith("]"):
        s = s[len("Optional[") : -1]
    if s.startswith("Union[") and s.endswith("]"):
        parts = [x.strip() for x in s[len("Union[") : -1].split(",") if "None" not in x and x.strip()]
        return "|".join(parts) or "str"
    return s


def _wire_key(params: Optional[dict], name: str) -> str:
    """Recover the wire key for a query param from a captured params dict."""
    if not params:
        return name
    val = _val(name)
    for k, v in params.items():
        if v == val or str(v) == str(val):
            return k
    return name


def extract_module(dotted: str, prefix: str, base: str, name_pattern: str) -> List[dict]:
    """Extract all public functions of a module into endpoint dicts."""
    module = importlib.import_module(dotted)
    eps = []
    for name, fn in vars(module).items():
        if not name.startswith(prefix) or not callable(fn) or name.startswith("_"):
            continue
        ep = describe_fn(module, name, fn, base, strip_prefix=prefix)
        if ep is not None:
            eps.append(ep)
    return eps
