"""YAML endpoint specs -> typed dataclasses (the codegen data model)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml


class SpecError(ValueError):
    """Raised when a YAML spec is malformed or internally inconsistent."""


@dataclass(frozen=True)
class Param:
    python_name: str
    api: str  # wire/query key
    type: str = "str"  # e.g. "int", "int|str"
    required: bool = False
    default: object = None
    pattern: Optional[str] = None  # regex for docs/validation
    is_query: bool = True  # query vs path
    optional_segment: bool = False  # pairs with [/{token}] in path
    default_from: Optional[str] = None  # use another arg's value when None
    transform: Optional[str] = None  # named runtime transform (e.g. format_nhl_season, _csv)
    description: str = ""  # authored human-readable description for docs


@dataclass(frozen=True)
class Endpoint:
    short: str
    path: str
    summary: str = ""
    scope: str = "universal"
    host: Optional[str] = None
    parser: Optional[str] = None
    returns_schema: Optional[str] = None
    query_params: List[Param] = field(default_factory=list)
    path_params: List[Param] = field(default_factory=list)
    example_args: Dict[str, object] = field(default_factory=dict)
    now_variant: Optional[str] = None  # alternate path when the now_toggle param is None
    now_toggle: Optional[str] = None  # the path param whose None selects now_variant
    exclude_leagues: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class EspnApi:
    api: str
    host: str
    name_pattern: str
    endpoints: List[Endpoint]


@dataclass(frozen=True)
class FlatApi:
    """A non-sport/league API: a literal base host + endpoints (NHL api-web/edge/
    stats-rest/records, MLB stats). ``name_pattern`` is e.g. ``"nhl_{short}"``."""

    api: str
    host: str
    name_pattern: str
    module: str  # destination module name, e.g. "nhl_api_web"
    endpoints: List[Endpoint]
    parser_module: Optional[str] = None  # dotted, e.g. "nhl.nhl_api_web_parsers"
    runtime_imports: List[str] = field(default_factory=lambda: ["_get"])
    qualifier: str = ""  # collision qualifier, e.g. "web"/"edge"/"stats_rest"/"records"/"api"
    # When True, the catch-all ``**kwargs`` is forwarded as (None-filtered) QUERY
    # params rather than to the HTTP layer -- reproduces the hand-written
    # ``**filters`` power feature of the NHL stats-REST / records / MLB families
    # (cayenneExp/sort/hydrate/fields/...).
    passthrough_query: bool = False
    # Dotted module the runtime helpers (``_get`` + transforms) are imported from.
    # Defaults to the shared no-auth runtime; auth'd families (NFL.com) point this
    # at a league runtime module that mints + threads a bearer token.
    getter_module: str = "sportsdataverse._codegen_runtime"
    # When True, each wrapper gains an optional ``headers`` arg threaded into the
    # getter so callers can reuse a minted-token dict across calls (NFL.com auth).
    auth: bool = False
    # Raw (``return_parsed=False``) response types this family's getter can return.
    # Defaults to JSON-only. A content-type-aware getter that hands back CSV/HTML
    # bodies as text declares ``raw_types: [Dict, str]`` so the generated wrappers
    # annotate + document the union they actually return (Torvik data files).
    raw_types: List[str] = field(default_factory=lambda: ["Dict"])
    # Optional per-family docstring extras merged into every generated wrapper:
    # ``raw_doc`` (prose for the return_parsed=False payload), ``raises`` (list of
    # "Exception: description"), ``see_also`` (list of {name, url, note}) and
    # ``example_import`` (bool -- prepend the import line to the Example block).
    docstring: Dict[str, object] = field(default_factory=dict)

    @property
    def prefix(self) -> str:
        """League prefix from ``name_pattern`` (``"nhl_{short}"`` -> ``"nhl"``)."""
        return self.name_pattern.split("_{", 1)[0]


SEASON_TOKEN = re.compile(r"\{season(?:\s*\+\s*(\d+))?\}")


def fill_season(url: str, season: int) -> str:
    """Resolve a loader asset URL's ``{season}`` / ``{season + N}`` token.

    The ``+ N`` form is the START-year -> END-year translation used by leagues whose
    published assets are keyed by the season's END year (NBA: ``1996`` = 1996-97 =
    ``nba_play_by_play_1997.parquet``). The public ``seasons`` argument stays the
    START year -- the offset lives only in the asset path.
    """
    return SEASON_TOKEN.sub(lambda m: str(season + int(m.group(1) or 0)), url)


@dataclass(frozen=True)
class Loader:
    """A 404-safe dataset loader over a sportsdataverse-data release asset."""

    fn: str
    league: str
    base: str  # key into ReleasesConfig.bases
    url: str  # asset path with a {season} token, relative to base
    tag: str  # release tag (provenance + audit key)
    min_season: Optional[int] = None
    returns_schema: Optional[str] = None
    example_args: Dict[str, object] = field(default_factory=dict)
    automation: Dict[str, str] = field(default_factory=dict)
    notebook: Optional[str] = None
    stub: bool = False
    stub_message: Optional[str] = None
    # Id columns to canonicalize to Int64 at the loader boundary. Producers have
    # shipped the same ESPN id as String, Int32 and Int64 across releases, which
    # makes a cross-dataset join on that id silently match nothing. Declaring the
    # column here normalizes it on read without touching the published asset.
    id_int64: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReleasesConfig:
    bases: Dict[str, str]
    loaders: List[Loader]


@dataclass(frozen=True)
class League:
    prefix: str
    sport: str
    league: str
    scopes: List[str]
    league_param: bool = False
    group: str = ""


@dataclass(frozen=True)
class LeaguesConfig:
    hosts: Dict[str, str]
    leagues: List[League]


_PATH_TOKEN = re.compile(r"\{(\w+)\}")


def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def load_parameters(path: Path) -> Dict[str, Param]:
    raw = _read_yaml(path)["params"]
    out: Dict[str, Param] = {}
    for key, v in raw.items():
        out[key] = Param(
            python_name=key,
            api=v.get("api", key),
            type=v.get("type", "str"),
            required=v.get("required", False),
            default=v.get("default"),
            pattern=v.get("pattern"),
            is_query=v.get("is_query", True),
            description=v.get("description", ""),
        )
    return out


def load_leagues(path: Path) -> LeaguesConfig:
    raw = _read_yaml(path)
    leagues = [
        League(
            prefix=lg["prefix"],
            sport=lg["sport"],
            league=lg["league"],
            scopes=list(lg["scopes"]),
            league_param=bool(lg.get("league_param", False)),
            group=str(lg.get("group", "")),
        )
        for lg in raw["leagues"]
    ]
    return LeaguesConfig(hosts=dict(raw["hosts"]), leagues=leagues)


def _resolve_param(name: str, registry: Dict[str, Param], src: Path) -> Param:
    if name not in registry:
        raise SpecError(f"{src}: endpoint references unknown parameter key {name!r}")
    return registry[name]


def _parse_endpoint(e: dict, registry: Dict[str, Param], path: Path) -> Endpoint:
    """Parse one endpoint dict (shared by ESPN + flat-API loaders)."""
    qps = [_resolve_param(k, registry, path) for k in e.get("params", [])]
    for extra in e.get("extra_params", []):
        # Inherit description from the shared registry when not set inline.
        reg_desc = registry.get(extra["name"], None)
        inherited_desc = reg_desc.description if reg_desc is not None else ""
        qps.append(
            Param(
                python_name=extra["name"],
                api=extra.get("query_key", extra["name"]),
                type=extra.get("type", "str"),
                required=extra.get("required", False),
                default=extra.get("default"),
                transform=extra.get("transform"),
                description=extra.get("description", "") or inherited_desc,
            ),
        )
    pps = []
    for pp in e.get("path_params", []):
        pps.append(
            Param(
                python_name=pp["name"],
                api=pp["name"],
                type=pp.get("type", "str"),
                required=pp.get("required", True),
                default=pp.get("default"),
                is_query=False,
                optional_segment=pp.get("optional_segment", False),
                default_from=pp.get("default_from"),
                transform=pp.get("transform"),
                description=pp.get("description", ""),
            ),
        )
    ep = Endpoint(
        short=e["short"],
        path=e["path"],
        summary=e.get("summary", ""),
        scope=e.get("scope", "universal"),
        host=e.get("host"),
        parser=e.get("parser"),
        returns_schema=e.get("returns_schema"),
        query_params=qps,
        path_params=pps,
        example_args=e.get("example_args", {}) or {},
        now_variant=e.get("now_variant"),
        now_toggle=e.get("now_toggle"),
        exclude_leagues=list(e.get("exclude_leagues", [])),
    )
    # validate path tokens (excluding the {sport}/{league} slugs) have a known param;
    # strip optional-segment brackets first so "[/{token}]" tokens are seen.
    bare_path = ep.path.replace("[", "").replace("]", "")
    tokens = set(_PATH_TOKEN.findall(bare_path)) - {"sport", "league"}
    known = {p.python_name for p in ep.path_params} | set(registry)
    missing = tokens - known
    if missing:
        raise SpecError(f"{path}: endpoint {ep.short!r} path token(s) {missing} have no param")
    return ep


def load_espn_api(path: Path, registry: Dict[str, Param]) -> EspnApi:
    raw = _read_yaml(path)
    endpoints = [_parse_endpoint(e, registry, path) for e in raw["endpoints"]]
    return EspnApi(api=raw["api"], host=raw["host"], name_pattern=raw["name_pattern"], endpoints=endpoints)


def load_releases(path: Path) -> ReleasesConfig:
    """Load the dataset-loader manifest (releases.yaml)."""
    raw = _read_yaml(path)
    loaders = [
        Loader(
            fn=ld["fn"],
            league=ld["league"],
            base=ld["base"],
            url=ld["url"],
            tag=ld["tag"],
            min_season=ld.get("min_season"),
            returns_schema=ld.get("returns_schema"),
            example_args=ld.get("example_args", {}) or {},
            automation=ld.get("automation", {}) or {},
            notebook=ld.get("notebook"),
            stub=ld.get("stub", False),
            stub_message=ld.get("stub_message"),
            id_int64=list(ld.get("id_int64", []) or []),
        )
        for ld in raw["loaders"]
    ]
    return ReleasesConfig(bases=dict(raw["bases"]), loaders=loaders)


def load_flat_api(path: Path, registry: Dict[str, Param]) -> FlatApi:
    """Load a flat (non-sport/league) API spec: NHL api-web/edge/stats-rest/records, MLB stats."""
    raw = _read_yaml(path)
    endpoints = [_parse_endpoint(e, registry, path) for e in raw["endpoints"]]
    return FlatApi(
        api=raw["api"],
        host=raw["host"],
        name_pattern=raw["name_pattern"],
        module=raw["module"],
        endpoints=endpoints,
        parser_module=raw.get("parser_module"),
        runtime_imports=list(raw.get("runtime_imports", ["_get"])),
        qualifier=raw.get("qualifier", ""),
        passthrough_query=bool(raw.get("passthrough_query", False)),
        getter_module=raw.get("getter_module", "sportsdataverse._codegen_runtime"),
        auth=bool(raw.get("auth", False)),
        raw_types=list(raw.get("raw_types") or ["Dict"]),
        docstring=dict(raw.get("docstring") or {}),
    )
