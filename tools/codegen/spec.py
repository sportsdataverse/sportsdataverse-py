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


@dataclass(frozen=True)
class EspnApi:
    api: str
    host: str
    name_pattern: str
    endpoints: List[Endpoint]


@dataclass(frozen=True)
class League:
    prefix: str
    sport: str
    league: str
    scopes: List[str]


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
        )
    return out


def load_leagues(path: Path) -> LeaguesConfig:
    raw = _read_yaml(path)
    leagues = [
        League(prefix=lg["prefix"], sport=lg["sport"], league=lg["league"], scopes=list(lg["scopes"]))
        for lg in raw["leagues"]
    ]
    return LeaguesConfig(hosts=dict(raw["hosts"]), leagues=leagues)


def _resolve_param(name: str, registry: Dict[str, Param], src: Path) -> Param:
    if name not in registry:
        raise SpecError(f"{src}: endpoint references unknown parameter key {name!r}")
    return registry[name]


def load_espn_api(path: Path, registry: Dict[str, Param]) -> EspnApi:
    raw = _read_yaml(path)
    endpoints: List[Endpoint] = []
    for e in raw["endpoints"]:
        qps = [_resolve_param(k, registry, path) for k in e.get("params", [])]
        for extra in e.get("extra_params", []):
            qps.append(
                Param(
                    python_name=extra["name"],
                    api=extra.get("query_key", extra["name"]),
                    type=extra.get("type", "str"),
                    required=extra.get("required", False),
                    default=extra.get("default"),
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
            example_args=e.get("example_args", {}) or {},
        )
        # validate path tokens (excluding the {sport}/{league} slugs) have a known param
        tokens = set(_PATH_TOKEN.findall(ep.path)) - {"sport", "league"}
        known = {p.python_name for p in ep.path_params} | set(registry)
        missing = tokens - known
        if missing:
            raise SpecError(f"{path}: endpoint {ep.short!r} path token(s) {missing} have no param")
        endpoints.append(ep)
    return EspnApi(api=raw["api"], host=raw["host"], name_pattern=raw["name_pattern"], endpoints=endpoints)
