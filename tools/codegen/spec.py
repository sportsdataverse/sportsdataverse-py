"""YAML endpoint specs -> typed dataclasses (the codegen data model)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    # Closed value set; the wrapper raises ValueError otherwise. Typed by ``type`` (see :func:`typed_choices`):
    # ints for ``int``, ISO-date strings for ``date``, strings for everything else.
    choices: List[object] = field(default_factory=list)
    # Render after ``*`` so a param added to an existing wrapper never shifts the
    # positional slot of ``headers`` (or any later arg) for existing callers.
    kw_only: bool = False
    # Travels in the request body (``Endpoint.body`` form / json) instead of the query string.
    # Body fields ride in ``query_params`` (signature, choices, docs rows are shared); the template splits
    # them out by this flag.
    in_body: bool = False
    # Runtime function that maps the public value to the one sent on the wire; used only to render the
    # curl example of a body endpoint (the wrapper passes the public value, the runtime converts it).
    wire_transform: Optional[str] = None
    # Extra arguments (OTHER params of the same endpoint, by python name) passed after the value to the
    # named ``transform`` -- ``transform: {fn: shl_ssgt_uuid, with: [game_type]}`` renders
    # ``shl_ssgt_uuid(season, game_type)``. They are read as the wrapper's variables are at that point.
    transform_with: Tuple[str, ...] = ()
    # Declarative ``value + offset`` (``transform: {offset: -1}``: a season END year -> START year), None-safe.
    transform_offset: Optional[int] = None

    @property
    def has_transform(self) -> bool:
        return bool(self.transform) or self.transform_offset is not None

    def transform_call(self, arg: str) -> str:
        """The wrapper expression applying this param's transform to the variable ``arg``."""
        if self.transform_offset is not None:
            sign = "-" if self.transform_offset < 0 else "+"
            return f"None if {arg} is None else int({arg}) {sign} {abs(self.transform_offset)}"
        if self.transform == "bool_str":  # its ValueError names the parameter (A-M17)
            return f'bool_str({arg}, name="{self.python_name}")'
        return f"{self.transform}({', '.join((arg, *self.transform_with))})"


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
    # Allowlist of league PREFIXES (``nba``, ``cfb``, ``epl``) this endpoint is emitted
    # for, on top of ``scope``. Empty = every in-scope league. For a family whose
    # per-league availability was live-probed (ESPN CDN): a league no probe verified
    # gets no wrapper. Prefixes, not slugs, so the soccer param-mode catch-all
    # (slug ``eng.1``, prefix ``soccer``) is not swept in alongside ``epl``.
    include_prefixes: List[str] = field(default_factory=list)
    # Constant query params sent on every request and never exposed as arguments
    # (ESPN CDN ``xhr=1``: without it the page is served as HTML). Rendered before
    # the caller's params, so ``params=`` can still override. A family-level
    # ``fixed_params`` in an ESPN API YAML is merged into each of its endpoints.
    fixed_params: Dict[str, object] = field(default_factory=dict)
    # Same shape as FlatApi.docstring, scoped to ONE endpoint. Declared per
    # endpoint when a family is too large to opt in wholesale: the family-level
    # block rewrites every wrapper in the family, this one changes only its own.
    # Takes precedence over the family block when both are present.
    docstring: Dict[str, object] = field(default_factory=dict)
    # Per-league override of ``example_args`` for the generated docs/examples, keyed
    # by league prefix (``{"cfb": {"week": 12, ...}}``). A league listed here uses
    # that dict INSTEAD of ``example_args`` (not merged), so a league that ignores a
    # default example param (football and ``date``) can swap in ones it honours.
    league_example_args: Dict[str, Dict[str, object]] = field(default_factory=dict)
    # HTTP method the family's getter sends (``get`` | ``post``). Docs only: the
    # rendered call is the same ``_get(url, params=...)`` either way, and a POST
    # family's runtime ``_get`` sends ``params`` as the request body. ``post`` makes
    # the docstring say ``POST`` and call the arguments body fields.
    method: str = "get"
    # Where the params go on a ``post``: ``query`` (default, the getter gets ``params=`` only),
    # ``form`` (``data=``) or ``json`` (``json=``). Non-query kinds send every param and every
    # ``fixed_params`` entry in the body unless the param says ``in: query`` (paging keys).
    body: str = "query"
    # ``fixed_params`` keys that stay in the query string of a body endpoint (CBA ``startNum``).
    fixed_in_query: List[str] = field(default_factory=list)
    # A body endpoint's runtime callable the wrapper passes its flat body dict through before sending,
    # e.g. to nest fields into the JSON the API expects. Resolved from the family's ``getter_module``,
    # else ``sportsdataverse._codegen_runtime`` (the generated import follows). Only top-level None
    # values are stripped from its output: the builder owns None handling inside what it nests.
    body_builder: Optional[str] = None
    # Per-route getter: the callable this route's wrapper calls instead of the family ``_get``. A bare name
    # is looked up in the family ``getter_module``; ``pkg.module:name`` imports it from that module (a
    # multi-host family whose routes need different transports). Its module supplies the route's
    # ``_build_query`` for the example replay and decides the error-vocabulary docstring; transforms still
    # come from the family ``getter_module``.
    getter: Optional[str] = None

    @property
    def url_fixed(self) -> Dict[str, object]:
        return {k: v for k, v in self.fixed_params.items() if self.body == "query" or k in self.fixed_in_query}

    @property
    def body_fixed(self) -> Dict[str, object]:
        return {k: v for k, v in self.fixed_params.items() if k not in self.url_fixed}

    @property
    def body_params(self) -> List[Param]:
        return [p for p in self.query_params if p.in_body]

    @property
    def url_params(self) -> List[Param]:
        """The query-string params: every query param that is not a body field."""
        return [p for p in self.query_params if not p.in_body]


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
    # When True, every wrapper passes its returns-schema column names to the parser as
    # ``columns=[...]`` (and the module carries them as ``_PARSER_COLUMNS``), so an empty
    # payload parses to a zero-row frame WITH the documented schema (f1).
    parser_columns: bool = False
    # Prefix of the ``schema:`` key of this family's native returns-schemas, which is what
    # manual_column_descriptions.yaml is keyed by. Empty = the bare endpoint short; set it when a
    # bare short already names another family's descriptions (sihf: ``standings`` -> ``sihf_standings``).
    schema_prefix: str = ""
    # No R-package column descriptions for this family's returns tables (player / team AGGREGATES whose
    # sport's R dictionaries describe play-by-play; On3 recruiting tables that are not cfbfastR's): manual
    # text only. Replaces a hard-coded list of family names in generate.py.
    no_r_fallback: bool = False

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
    # Free-text caveat for this dataset -- coverage boundaries, partial
    # seasons, fields that only populate after some date. Rendered into the
    # generated docstring AND the loaders reference page, because a caveat
    # only a maintainer can see does not stop anyone averaging a
    # partly-empty column.
    notes: Optional[str] = None
    # Companion links rendered as the docstring's ``See Also:`` block, each
    # ``{name, note, url}`` -- the same shape as an endpoint's ``docstring.see_also``.
    see_also: List[Dict[str, str]] = field(default_factory=list)
    stub: bool = False
    stub_message: Optional[str] = None
    # Name of the loader that supersedes this one. When set, the generated body is
    # a DeprecationWarning shim that forwards to that loader instead of reading
    # ``url`` itself -- used when a release tag is retired and its content is
    # already carried by another (superset) tag. Both loaders must expose the same
    # public ``seasons`` convention so forwarding needs no year arithmetic; the
    # ``{season + N}`` offset in ``url`` is what keeps the two aligned.
    deprecated_for: Optional[str] = None
    # Id columns to canonicalize to Int64 at the loader boundary. Producers have
    # shipped the same ESPN id as String, Int32 and Int64 across releases, which
    # makes a cross-dataset join on that id silently match nothing. Declaring the
    # column here normalizes it on read without touching the published asset.
    id_int64: List[str] = field(default_factory=list)
    # Id columns to cast to Int32 instead: a String id in a release family whose every
    # other dataset ships that id as Int32 (ESPN basketball game_rosters.game_id,
    # rosters.athlete_id, the crosswalk / NCAA bridge espn_* ids), so the join keys
    # are schema-equal, not merely joinable.
    id_int32: List[str] = field(default_factory=list)
    # What the sdv-py loader does when a requested season has no published asset:
    # ``skip`` (default; the generated loaders -- the season is dropped) or ``raise``
    # (the hand-written nfl_loaders -- ``NoDataError``). Machine-readable so ports
    # that generate every entry (sdv-js) can honour the difference; sdv-py's own
    # generated output does not read it.
    on_missing: str = "skip"


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


def typed_choices(raw: object, ptype: str, where: str) -> List[object]:
    """A param's ``choices:`` as values of its declared ``type`` (the wrapper compares them as typed).

    * ``int``: ints (a digit string is read as its int); ``True`` / ``2.5`` / ``"x"`` are a spec error.
    * ``date``: ISO ``YYYY-MM-DD`` strings, each a real calendar date.
    * anything else (``str``, ``int|str``, ...): strings, as before.

    Raises:
        SpecError: ``choices`` is not a list, or an entry does not fit the type.
    """
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise SpecError(f"{where}: choices must be a list; got {raw!r}")
    out: List[object] = []
    for c in raw:
        if ptype == "int":
            if isinstance(c, bool) or not (isinstance(c, int) or (isinstance(c, str) and re.fullmatch(r"-?\d+", c))):
                raise SpecError(f"{where}: choice {c!r} is not an int (the param is type: int)")
            out.append(int(c))
        elif ptype == "date":
            text = str(c)  # YAML reads an unquoted 2026-01-01 as a date; str() is its ISO form
            try:
                ok = bool(re.fullmatch(r"\d{4}-\d\d-\d\d", text)) and date.fromisoformat(text).isoformat() == text
            except ValueError:
                ok = False
            if not ok:
                raise SpecError(f"{where}: choice {c!r} is not an ISO date YYYY-MM-DD (the param is type: date)")
            out.append(text)
        else:
            out.append(str(c))
    if len(set(out)) != len(out):
        raise SpecError(f"{where}: choices repeat a value: {raw!r}")
    return out


def _parse_transform(raw: object, where: str) -> dict:
    """``transform:`` of a param -> ``Param`` kwargs. A bare name is a runtime callable of the value;
    ``{fn: name, with: [other params]}`` also passes those params; ``{offset: N}`` is ``int(value) + N``."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        return {"transform": raw}
    if isinstance(raw, dict):
        if set(raw) == {"offset"} and isinstance(raw["offset"], int) and not isinstance(raw["offset"], bool):
            return {"transform_offset": raw["offset"]}
        if isinstance(raw.get("fn"), str) and set(raw) <= {"fn", "with"}:
            extra = raw.get("with") or []
            if isinstance(extra, list) and all(isinstance(x, str) for x in extra):
                return {"transform": raw["fn"], "transform_with": tuple(extra)}
    raise SpecError(f"{where}: transform must be a name, {{fn: name, with: [params]}} or {{offset: int}}; got {raw!r}")


def _resolve_param(name: str, registry: Dict[str, Param], src: Path) -> Param:
    if name not in registry:
        raise SpecError(f"{src}: endpoint references unknown parameter key {name!r}")
    return registry[name]


def _parse_endpoint(e: dict, registry: Dict[str, Param], path: Path) -> Endpoint:
    """Parse one endpoint dict (shared by ESPN + flat-API loaders)."""
    body = str(e.get("body") or "query").lower()
    qps = [_resolve_param(k, registry, path) for k in e.get("params", [])]
    if body != "query":
        qps = [replace(p, in_body=True) for p in qps]
    for extra in e.get("extra_params", []):
        where = extra.get("in")
        if where not in (None, "query", "body"):
            raise SpecError(
                f"{path}: endpoint {e.get('short')!r} param {extra['name']!r} in {where!r} is not query or body"
            )
        if where == "body" and body == "query":
            raise SpecError(
                f"{path}: endpoint {e.get('short')!r} param {extra['name']!r} is in: body but the endpoint has no body kind"
            )
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
                description=extra.get("description", "") or inherited_desc,
                choices=typed_choices(
                    extra.get("choices"), extra.get("type", "str"), f"{path}: {e.get('short')}.{extra['name']}"
                ),
                kw_only=bool(extra.get("kw_only", False)),
                in_body=body != "query" and where != "query",
                wire_transform=extra.get("wire_transform"),
                **_parse_transform(extra.get("transform"), f"{path}: {e.get('short')}.{extra['name']}"),
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
                description=pp.get("description", ""),
                choices=typed_choices(
                    pp.get("choices"), pp.get("type", "str"), f"{path}: {e.get('short')}.{pp['name']}"
                ),
                **_parse_transform(pp.get("transform"), f"{path}: {e.get('short')}.{pp['name']}"),
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
        include_prefixes=list(e.get("include_prefixes", [])),
        fixed_params=dict(e.get("fixed_params") or {}),
        docstring=dict(e.get("docstring") or {}),
        league_example_args={k: dict(v or {}) for k, v in (e.get("league_example_args") or {}).items()},
        method=str(e.get("method") or "get").lower(),
        body=body,
        fixed_in_query=list(e.get("fixed_in_query") or []),
        body_builder=e.get("body_builder"),
        getter=e.get("getter"),
    )
    if ep.method not in ("get", "post"):
        raise SpecError(f"{path}: endpoint {ep.short!r} method {ep.method!r} is not get or post")
    unknown = sorted(set(ep.fixed_in_query) - set(ep.fixed_params))
    if unknown:
        raise SpecError(f"{path}: endpoint {ep.short!r} fixed_in_query names {unknown} not in fixed_params")
    if ep.body not in ("query", "form", "json"):
        raise SpecError(f"{path}: endpoint {ep.short!r} body {ep.body!r} is not query, form or json")
    if ep.body != "query" and ep.method != "post":
        raise SpecError(f"{path}: endpoint {ep.short!r} sets body {ep.body!r} but is not a post")
    if ep.body_builder and ep.body == "query":
        raise SpecError(f"{path}: endpoint {ep.short!r} sets body_builder but has no body kind (form or json)")
    names = [p.python_name for p in (*ep.query_params, *ep.path_params)]
    if len(names) != len(set(names)):
        raise SpecError(f"{path}: endpoint {ep.short!r} repeats a parameter name: {sorted(names)}")
    for p in (*ep.query_params, *ep.path_params):
        if set(p.transform_with) - set(names):
            raise SpecError(
                f"{path}: endpoint {ep.short!r} param {p.python_name!r} transform uses unknown param(s) "
                f"{sorted(set(p.transform_with) - set(names))}"
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
    if any(e.get("getter") for e in raw["endpoints"]):
        raise SpecError(f"{path}: getter: is for flat APIs; ESPN endpoints share one getter")
    fam = raw.get("fixed_params") or {}
    endpoints = [
        _parse_endpoint({**e, "fixed_params": {**fam, **(e.get("fixed_params") or {})}}, registry, path)
        for e in raw["endpoints"]
    ]
    return EspnApi(api=raw["api"], host=raw["host"], name_pattern=raw["name_pattern"], endpoints=endpoints)


def validate_league_keys(apis: List[EspnApi], cfg: "LeaguesConfig") -> None:
    """Reject an ``include_prefixes`` / ``league_example_args`` entry that names no league
    in ``leagues.yaml`` -- a typo would otherwise silently drop the endpoint (or its
    example) for that league."""
    known = {lg.prefix for lg in cfg.leagues}
    for api in apis:
        for ep in api.endpoints:
            bad = (set(ep.include_prefixes) | set(ep.league_example_args)) - known
            if bad:
                raise SpecError(f"{api.api}.{ep.short}: unknown league prefix(es) {sorted(bad)}")


def _on_missing(ld: dict) -> str:
    value = ld.get("on_missing", "skip")
    if value not in ("skip", "raise"):
        raise ValueError(f"{ld['fn']}: on_missing must be 'skip' or 'raise', got {value!r}")
    return value


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
            notes=ld.get("notes"),
            see_also=list(ld.get("see_also", []) or []),
            stub=ld.get("stub", False),
            stub_message=ld.get("stub_message"),
            deprecated_for=ld.get("deprecated_for"),
            id_int64=list(ld.get("id_int64", []) or []),
            id_int32=list(ld.get("id_int32", []) or []),
            on_missing=_on_missing(ld),
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
        parser_columns=bool(raw.get("parser_columns", False)),
        schema_prefix=str(raw.get("schema_prefix") or ""),
        no_r_fallback=bool(raw.get("no_r_fallback", False)),
    )
