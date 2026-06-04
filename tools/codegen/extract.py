"""Runtime extraction: the live ESPN factory -> codegen YAML + rename_map.

Rather than statically parsing each core fn's source (fragile against the
``if x is not None`` path branches and ``c = cid or event_id`` fallbacks),
this *executes* each core fn under a mocked ``_get`` with sentinel arguments and
reads back the URL it built. Two calls per fn -- one with only the required
params set, one with every optional param set -- plus a segment-level diff
recover, faithfully:

* the path template (``{sport}``/``{league}`` + path-param tokens),
* which optional params are path segments vs query params,
* ``optional_segment`` (a segment present only in the all-optional call),
* ``default_from`` (a segment whose value falls back to another param), and
* the exact query wire-keys.

Because it runs the real branching logic, every conditional shape is handled
without bespoke source parsing.

This module imports the pre-retirement factory (``_UNIVERSAL_WRAPPERS`` etc.).
It is a build-time tool, runnable only while the factory still exists; after the
factory is retired (Plan 2 Task 8) re-extraction requires the factory git ref.
"""

from __future__ import annotations

import difflib
import inspect
from pathlib import Path
from typing import Dict, List

import yaml

from sportsdataverse import _common_espn as ce
from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"

# host constant value -> codegen host key (longest-prefix match at extract time)
_HOSTS = {
    ce._SITE_V2: "site_v2",
    ce._SITE_V2_ALT: "site_v2_alt",
    ce._WEB_V3: "web_v3",
    ce._CORE_V2: "core_v2",
}
# which espn_*.yaml file each host belongs to
_API_OF_HOST = {
    "site_v2": "espn_site_v2",
    "site_v2_alt": "espn_site_v2",
    "web_v3": "espn_web_v3",
    "core_v2": "espn_core_v2",
}
_API_HOST = {"espn_site_v2": "site_v2", "espn_web_v3": "web_v3", "espn_core_v2": "core_v2"}

# scope per source table
_TABLES = [
    ("universal", ce._UNIVERSAL_WRAPPERS),
    ("ncaa", ce._NCAA_WRAPPERS),
    ("football", ce._FOOTBALL_WRAPPERS),
    ("mlb", ce._MLB_WRAPPERS),
]

# clean-name policy: drop these public-name suffixes when the bare name is free.
# ``_core`` is kept (it disambiguates from the site/web variant).
_DROP_SUFFIX = ("_site", "_alt")

_SENT = "@@{}@@"  # sentinel marker; reverse-mapped to {name}

# representative ids so example URLs render and the parity test has live-ish args
_EXAMPLE = {
    "event_id": "401584793",
    "athlete_id": "4239",
    "team_id": "4",
    "opp_id": "5",
    "season": 2024,
    "week": 1,
    "group_id": 80,
    "venue_id": "3663",
    "franchise_id": "2",
    "coach_id": "1",
    "position_id": "1",
    "award_id": "1",
    "play_id": "1",
    "official_id": "1",
    "round_id": "1",
    "pick_id": "1",
    "stat_type": 0,
    "season_type": 2,
    "split": 0,
    "cid": "1",
    "dates": "20240115",
    "limit": 500,
}


def _table() -> Dict[str, object]:
    out: Dict[str, object] = {}
    for _scope, tbl in _TABLES:
        for short, fn in tbl:
            out[short] = fn
    return out


def _type_str(ann) -> str:
    """Annotation -> spec type string ('int|str', 'int', 'bool', 'str')."""
    if ann is inspect.Parameter.empty:
        return "str"
    s = str(ann).replace("typing.", "")
    if s.startswith("Optional[") and s.endswith("]"):
        s = s[len("Optional[") : -1]
    if s.startswith("Union[") and s.endswith("]"):
        inner = s[len("Union[") : -1]
        parts = [p.strip() for p in inner.split(",") if "NoneType" not in p and p.strip()]
        return "|".join(parts)
    return s


def _capture(fn, kwargs) -> tuple:
    """Call ``fn`` with sport/league sentinels + kwargs under a mocked _get.

    Returns ``(path, params)`` where path is host-stripped with sentinels intact.
    """
    seen: List[tuple] = []
    orig = ce._get

    def fake_get(url, params=None, **_kw):
        seen.append((url, dict(params or {})))
        return {}

    ce._get = fake_get
    try:
        fn(_SENT.format("sport"), _SENT.format("league"), **kwargs)
    finally:
        ce._get = orig
    url, params = seen[-1]
    host_key = max(
        (h for h, _ in _HOSTS.items() if url.startswith(h)),
        key=len,
        default="",
    )
    path = url[len(host_key) :]
    return host_key, path, params


def _to_tokens(path: str) -> str:
    """Replace @@sport@@/@@league@@/... sentinels with {name} tokens."""
    out = path
    while "@@" in out:
        i = out.index("@@")
        j = out.index("@@", i + 2)
        name = out[i + 2 : j]
        out = out[:i] + "{" + name + "}" + out[j + 2 :]
    return out


def describe_core_fn(short: str, fn=None) -> Dict:
    """Describe a core fn's endpoint shape via runtime capture."""
    fn = fn or _table()[short]
    sig = inspect.signature(fn)
    params = [p for p in sig.parameters.values() if p.name not in ("sport", "league") and p.kind != p.VAR_KEYWORD]
    set_default = [p for p in params if p.default is not inspect.Parameter.empty and p.default is not None]
    required = [p for p in params if p.default is inspect.Parameter.empty]

    # call A: required + non-None-default params as sentinels; None-default left unset
    args_a = {p.name: _SENT.format(p.name) for p in required + set_default}
    host_key, path_a, _q_a = _capture(fn, args_a)
    # call B: everything as sentinels
    args_b = {p.name: _SENT.format(p.name) for p in params}
    _hb, path_b, q_b = _capture(fn, args_b)

    full_path = _to_tokens(path_b)
    segs_a = _to_tokens(path_a).strip("/").split("/")
    segs_b = full_path.strip("/").split("/")

    # which None-default params are path params (appear as a token in path_b)
    path_param_names = {p.name for p in params if ("{" + p.name + "}") in full_path}

    optional_segments: set = set()
    default_from: Dict[str, str] = {}
    sm = difflib.SequenceMatcher(a=segs_a, b=segs_b, autojunk=False)
    for tag, _i1, _i2, j1, j2 in sm.get_opcodes():
        if tag == "insert":
            for seg in segs_b[j1:j2]:
                for nm in path_param_names:
                    if "{" + nm + "}" == seg or "{" + nm + "}" in seg:
                        optional_segments.add(nm)
        elif tag == "replace":
            # B token {cid} vs A token {event_id} at the same slot => cid defaults from event_id
            for off in range(min(_i2 - _i1, j2 - j1)):
                a_seg = segs_a[_i1 + off]
                b_seg = segs_b[j1 + off]
                if a_seg != b_seg and b_seg.startswith("{") and a_seg.startswith("{"):
                    default_from[b_seg.strip("{}")] = a_seg.strip("{}")

    # build the bracketed path: wrap each optional-segment param's inserted run in [ ]
    bracket_path = _bracket_optional_segments(full_path, optional_segments)

    # path params, in signature order
    path_params = []
    for p in params:
        if p.name not in path_param_names:
            continue
        entry = {"name": p.name, "type": _type_str(p.annotation), "required": p.default is inspect.Parameter.empty}
        if p.name in optional_segments:
            entry["optional_segment"] = True
        if p.name in default_from:
            entry["default_from"] = default_from[p.name]
        if p.default is not inspect.Parameter.empty and p.default is not None:
            entry["default"] = p.default
        path_params.append(entry)

    # query params: wire-key -> python name, recovered from the captured params dict
    query_params: Dict[str, Dict] = {}
    sentinel_to_name = {_SENT.format(p.name): p.name for p in params}
    for wire, val in q_b.items():
        name = sentinel_to_name.get(val if isinstance(val, str) else "")
        transform = None
        if name is None:
            # value was transformed (e.g. bool -> "true"/"false"); map by elimination
            leftovers = [
                p.name
                for p in params
                if p.name not in path_param_names and p.name not in {q["name"] for q in query_params.values()}
            ]
            name = leftovers[0] if leftovers else wire
            transform = (
                "bool_str"
                if _type_str(next((p.annotation for p in params if p.name == name), None)) == "bool"
                else None
            )
        p = next((pp for pp in params if pp.name == name), None)
        query_params[name] = {
            "name": name,
            "query_key": wire,
            "type": _type_str(p.annotation) if p else "str",
            "default": (p.default if p and p.default is not inspect.Parameter.empty else None),
        }
        if transform:
            query_params[name]["transform"] = transform

    return {
        "short": short,
        "host": host_key and _HOSTS[host_key],
        "path": bracket_path,
        "path_params": path_params,
        "query_params": query_params,
        "parser": getattr(ENDPOINT_PARSERS.get(short), "__name__", None)
        or (ENDPOINT_PARSERS.get(short) if isinstance(ENDPOINT_PARSERS.get(short), str) else None),
    }


def _bracket_optional_segments(full_path: str, optional: set) -> str:
    """Wrap the path run owning each optional-segment param in ``[ ]``."""
    if not optional:
        return full_path
    parts = full_path.strip("/").split("/")
    # find, for each optional param, the contiguous run of segments to bracket:
    # the segment containing {param} plus any immediately-preceding literal segments
    # that belong only to the optional branch (e.g. "groups" before "{group_id}").
    # mark the index of each optional param's own token segment
    token_idx = {}
    for idx, seg in enumerate(parts):
        for nm in optional:
            if seg == "{" + nm + "}":
                token_idx[nm] = idx
    # For each optional param, decide how many preceding literal segments to absorb.
    # Heuristic: absorb a single preceding *literal* (non-token) segment if it is not
    # shared with the required path (i.e. it directly precedes the token and is a word).
    absorb = {}
    for nm, idx in token_idx.items():
        start = idx
        if idx - 1 >= 0 and not parts[idx - 1].startswith("{"):
            # preceding literal like "groups" -> part of the optional branch
            # (only when the token is NOT the very last path element OR there is a tail)
            if idx != len(parts) - 1:
                start = idx - 1
        absorb[nm] = (start, idx)
    open_at = {v[0]: v[1] for v in absorb.values()}
    close_after = {v[1] for v in absorb.values()}
    rebuilt = ""
    for idx, seg in enumerate(parts):
        opened = idx in open_at
        # open the bracket *before* the slash so the slash is part of the optional
        # segment: ".../powerindex[/{team_id}]" (no trailing slash when omitted).
        rebuilt += ("[/" if opened else "/") + seg
        if idx in close_after:
            rebuilt += "]"
    return rebuilt


def _clean_name(short: str, all_shorts: set) -> str:
    for suf in _DROP_SUFFIX:
        if short.endswith(suf):
            base = short[: -len(suf)]
            if base not in all_shorts:
                return base
    return short


def build_rename_map(prefixes=("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl")) -> Dict[str, str]:
    """old public name -> clean public name (only entries that actually change)."""
    shorts = set(_table().keys())
    rename: Dict[str, str] = {}
    for short in shorts:
        new = _clean_name(short, shorts)
        if new != short:
            for pfx in prefixes:
                rename[f"espn_{pfx}_{short}"] = f"espn_{pfx}_{new}"
    return rename


def extract_all() -> Dict:
    """Build the per-API endpoint dicts + rename map."""
    shorts = set(_table().keys())
    apis: Dict[str, List[Dict]] = {"espn_site_v2": [], "espn_web_v3": [], "espn_core_v2": []}
    for scope, tbl in _TABLES:
        for short, fn in tbl:
            info = describe_core_fn(short, fn)
            api = _API_OF_HOST[info["host"]]
            ep = {"short": _clean_name(short, shorts), "scope": scope, "path": info["path"]}
            if info["host"] == "site_v2_alt":
                ep["host"] = "site_v2_alt"
            if info["path_params"]:
                ep["path_params"] = info["path_params"]
            if info["query_params"]:
                ep["extra_params"] = [
                    {
                        "name": q["name"],
                        "query_key": q["query_key"],
                        "type": q["type"],
                        **({"default": q["default"]} if q["default"] is not None else {}),
                        **({"transform": q["transform"]} if q.get("transform") else {}),
                    }
                    for q in info["query_params"].values()
                ]
            if info["parser"]:
                ep["parser"] = info["parser"]
                if info["parser"] in _SCHEMA_FOR_PARSER:
                    ep["returns_schema"] = _SCHEMA_FOR_PARSER[info["parser"]]
            # example args for required path params
            ex = {pp["name"]: _EXAMPLE.get(pp["name"], "1") for pp in info["path_params"] if pp.get("required")}
            if ep["short"] == "scoreboard":
                ex["dates"] = "20240115"
            if ex:
                ep["example_args"] = ex
            apis[api].append(ep)
    return apis


# parser -> schema key (drives schemas/<key>.yaml + endpoint returns_schema linkage)
_SCHEMA_FOR_PARSER = {
    "parse_scoreboard": "scoreboard",
    "parse_teams": "teams",
    "parse_standings": "standings",
    "parse_team_roster": "team_roster",
    "parse_leaders": "leaders",
    "parse_summary": "summary",
}

# polars dtype repr -> friendly (R-roxygen-style) type label, matching scoreboard.yaml
_PL_TYPE = {
    "Int64": "integer",
    "Int32": "integer",
    "Float64": "double",
    "Float32": "double",
    "Utf8": "character",
    "String": "character",
    "Boolean": "logical",
}


def _friendly_type(dtype) -> str:
    return _PL_TYPE.get(str(dtype), str(dtype).lower())


def _humanize(col: str) -> str:
    return col.replace("_", " ").strip().capitalize() + "."


def schema_from_parser(parser_name: str, payload: dict, description: str = "") -> dict:
    """Run a registered parser on a payload and emit a {name,type,description} schema."""
    import sportsdataverse._common_espn_parsers as parsers

    df = getattr(parsers, parser_name)(payload)
    cols = [{"name": c, "type": _friendly_type(df.schema[c]), "description": _humanize(c)} for c in df.columns]
    return {
        "schema": _SCHEMA_FOR_PARSER.get(parser_name, parser_name),
        "kind": "dataframe",
        "description": description,
        "columns": cols,
    }


def write_yaml(obj, path: Path) -> None:
    Path(path).write_text(yaml.safe_dump(obj, sort_keys=False, width=120, allow_unicode=True), encoding="utf-8")


def main() -> int:
    apis = extract_all()
    for api, eps in apis.items():
        doc = {"api": api, "host": _API_HOST[api], "name_pattern": "espn_{prefix}_{short}", "endpoints": eps}
        write_yaml(doc, ENDPOINTS / f"{api}.yaml")
    rename = build_rename_map()
    write_yaml(rename, ROOT / "tools" / "codegen" / "rename_map.yaml")
    print(
        f"extract: wrote {sum(len(v) for v in apis.values())} endpoints across {len(apis)} APIs; {len(rename)} renames",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
