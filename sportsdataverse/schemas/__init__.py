"""Declared shapes for the raw provider payloads committed in the SDV ``-raw`` repos.

The ``-raw`` repos archive provider payloads verbatim and the ``-data`` repos
reshape them into released datasets. Between those two steps there was nothing
that said what a payload is *supposed* to look like, so a provider dropping a
key surfaced as a silently-null column several datasets downstream.

**Strict on types, permissive on extras.** Two independent knobs that pull
opposite ways:

* Unknown keys are allowed (``additional_properties: true``). ESPN adds fields
  routinely; forbidding them would red a daily cron for a non-event.
* ``required`` lists only the keys some parser actually depends on, so a
  *missing* ``boxscore`` or ``plays`` is a red test on the file at scrape time.
* Types are **not** coerced. A numeric-string id stays a string here. Lax
  validation would quietly turn ``"401811123"`` into an int and ``5`` into
  ``5.0`` -- the float-origin-id and Int-width join-key failures this project
  keeps hitting. Where a payload genuinely ships two shapes (ESPN's
  ``winprobability`` is a list in most games and a dict in some), the schema
  declares the union explicitly rather than widening to "any".

**One provider can ship several envelopes.** ESPN is one family per endpoint
group, but stats.nba.com has *five*, and which one an endpoint uses is a
property of the endpoint rather than of the season or league: a classic
``resultSets`` list, a singular ``resultSet`` dict, a ``resultSets`` dict whose
``headers`` are column-group objects, the modern v3 ``{<entity>, meta}`` tree,
and a period-number map of v3 payloads. Classify before validating -- see
:func:`validate_payload` usage in the ``-raw`` repos.

Every schema here was derived from real committed captures. Do not edit one to
make a test pass without re-sampling the payloads it describes. The nba_stats
families were checked against 8,608 payloads spanning 54 endpoint directories
of ``hoopR-nba-stats-raw``; every one matched exactly one family.

Example:
    Validate a captured payload::

        import json
        from sportsdataverse.schemas import validate_payload

        payload = json.loads(open("wbb/json/final/401811123.json").read())
        problems = validate_payload("espn_summary", payload)
        if problems:
            raise AssertionError("\\n".join(problems))

    Inspect a declared shape::

        from sportsdataverse.schemas import RAW_SCHEMAS, load_raw_schema

        print(sorted(RAW_SCHEMAS))
        print(load_raw_schema("espn_officials")["required"])
"""

from __future__ import annotations

from functools import lru_cache
from importlib.resources import files
from typing import Any

__all__ = ["RAW_SCHEMAS", "load_raw_schema", "validate_payload"]

#: Every payload family with a declared shape. Keys are schema names; values
#: are one-line descriptions of the tree location the payload is archived at.
RAW_SCHEMAS: dict[str, str] = {
    "espn_summary": "<lg>/json/{raw,final}/<game_id>.json",
    "espn_game_rosters": "<lg>/game_rosters/json/<game_id>.json",
    "espn_officials": "<lg>/officials/json/<game_id>.json",
    "espn_player_core": "<lg>/player_core/json/<athlete_id>.json",
    "espn_standings": "<lg>/standings/json/<season>.json",
    "espn_team_stats": "<lg>/team_stats/json/<season>/<team_id>.json",
    "espn_player_season_stats": "<lg>/player_season_stats/json/<season>/<athlete_id>.json",
    # stats.nba.com / stats.wnba.com. FIVE envelope families, not one -- see
    # each schema's description. Which family an endpoint uses is a property of
    # the endpoint, not of the season or league.
    "nba_stats_result_sets": "nba_stats/json/<endpoint>/<season>/<slug>.json",
    "nba_stats_result_set": "nba_stats/json/<endpoint>/<season>/<slug>.json",
    "nba_stats_result_sets_grouped": "nba_stats/json/<endpoint>/<season>/<slug>.json",
    "nba_stats_v3": "nba_stats/json/<endpoint>/<season>/<game_id>.json",
    "nba_stats_v3_period": "nba_stats/json/boxscoretraditionalv3_period/<season>/<game_id>.json",
}

_TYPES: dict[str, type | tuple[type, ...]] = {
    "dict": dict,
    "list": list,
    "str": str,
    "int": int,
    "float": (int, float),
    "bool": bool,
}


@lru_cache(maxsize=None)
def load_raw_schema(name: str) -> dict[str, Any]:
    """Load a raw-payload schema by name.

    Args:
        name: A key of :data:`RAW_SCHEMAS`, e.g. ``"espn_summary"``.

    Returns:
        The parsed schema: ``{name, description, additional_properties,
        required, optional}``.

    Raises:
        KeyError: If ``name`` is not a declared payload family.
        ImportError: If PyYAML is not installed.
        TypeError: If the schema file does not parse to a mapping.

    Example:
        Quick start::

            from sportsdataverse.schemas import load_raw_schema

            schema = load_raw_schema("espn_summary")
            print(sorted(schema["required"]))

        Inspect what a family tolerates::

            print(schema["additional_properties"])   # True -- unknown keys allowed

        See Also:
            * `wehoop-wbb-raw`_ -- the archive whose payloads these describe
            * `wehoop`_ -- the R client that consumes the reshaped output

        .. _wehoop-wbb-raw: https://github.com/sportsdataverse/wehoop-wbb-raw
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if name not in RAW_SCHEMAS:
        raise KeyError(f"unknown raw payload family {name!r}; expected one of {sorted(RAW_SCHEMAS)}")
    # PyYAML is a build/CI-time dependency here, not a runtime one, so it is
    # imported lazily. The base package must always import cleanly (same
    # precedent as curl_cffi and psutil); only callers that actually read a
    # schema need the parser.
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - guarded by test_yaml_guidance
        raise ImportError(
            "Reading raw payload schemas requires PyYAML. Install it with "
            "`pip install pyyaml` or `pip install sportsdataverse[all]`."
        ) from exc
    path = files("sportsdataverse.schemas").joinpath("raw", f"{name}.yaml")
    schema = yaml.safe_load(path.read_text(encoding="utf-8"))
    # safe_load returns whatever the document is. A schema that parsed to a
    # list or a bare string would otherwise fail much later, inside .get() on
    # a caller's behalf, with an error that points nowhere near the cause.
    if not isinstance(schema, dict):
        raise TypeError(f"raw schema {name!r} parsed to {type(schema).__name__}, expected a mapping")
    return schema


def validate_payload(name: str, payload: Any) -> list[str]:
    """Check a payload against its declared shape.

    Args:
        name: A key of :data:`RAW_SCHEMAS`.
        payload: The parsed JSON payload.

    Returns:
        A list of human-readable problems. An empty list means the payload
        matches: every required key is present with a declared type, and every
        present optional key has a declared type. Unknown keys are ignored by
        design.

    Raises:
        KeyError: If ``name`` is not a declared payload family.

    Example:
        Quick start::

            import json
            from sportsdataverse.schemas import validate_payload

            payload = json.loads(open("wbb/json/final/401811123.json").read())
            problems = validate_payload("espn_summary", payload)
            print(problems)   # [] when the payload matches

        Fail a scrape on a bad capture::

            if validate_payload("espn_team_stats", payload):
                raise AssertionError("refusing to persist a non-conforming payload")

        Scan an archive for damage::

            bad = [p for p in paths if validate_payload("espn_team_stats", load(p))]

        See Also:
            * `wehoop-wbb-raw`_ -- the archive whose payloads these describe
            * `hoopR`_ -- the men's-basketball sibling with the same tree shape

        .. _wehoop-wbb-raw: https://github.com/sportsdataverse/wehoop-wbb-raw
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    schema = load_raw_schema(name)
    if not isinstance(payload, dict):
        return [f"{name}: payload is {type(payload).__name__}, expected dict"]

    problems: list[str] = []
    for key, declared in (schema.get("required") or {}).items():
        if key not in payload:
            problems.append(f"{name}: missing required key {key!r}")
            continue
        problems.extend(_check(name, key, payload[key], declared))
    # `required_any`: at least one of these keys must be present. The
    # stats.nba.com v3 endpoints all ship {<entityKey>, meta} where the entity
    # key is named per endpoint (boxScoreTraditional, game, leagueSchedule,
    # scoreboard, ...). Requiring only `meta` would let a payload whose body
    # was dropped validate clean, and one schema per entity key would be a
    # dozen near-identical files.
    for group in schema.get("required_any") or []:
        # A bare string is a schema-authoring slip (`required_any: [foo]` rather
        # than `[[foo]]`). Iterating it would test the payload for single
        # CHARACTERS and report nonsense, so treat it as a one-key group.
        keys = [group] if isinstance(group, str) else list(group)
        if not any(key in payload for key in keys):
            problems.append(f"{name}: payload has none of {sorted(keys)!r}; expected one")
    for key, declared in (schema.get("optional") or {}).items():
        # An absent optional key and an explicit null are both "not provided".
        if payload.get(key) is not None:
            problems.extend(_check(name, key, payload[key], declared))

    # `values_of`: every present value must itself match another declared
    # family. Without it a period map validates as long as its four keys hold
    # *some* dict, so `{"1": {}, "2": {}, "3": {}, "4": {}}` -- a capture that
    # lost every payload body -- passes as a complete game.
    nested = schema.get("values_of")
    if nested:
        if nested == name:
            raise ValueError(f"raw schema {name!r} declares itself as values_of")
        # Only DECLARED keys. Unknown keys stay ignored, as everywhere else --
        # `additional_properties: true` must keep meaning the same thing here,
        # or a new top-level field would be reported as a malformed period.
        declared_keys = [
            key for key in list(schema.get("required") or {}) + list(schema.get("optional") or {}) if key in payload
        ]
        for key in declared_keys:
            value = payload[key]
            if not isinstance(value, dict):
                problems.append(f"{name}: {key!r} is {type(value).__name__}, expected dict")
                continue
            problems.extend(f"{name}[{key}] -> {p}" for p in validate_payload(nested, value))
    return problems


def _check(name: str, key: str, value: Any, declared: str) -> list[str]:
    """Type-check one key. ``declared`` may be a union like ``"list|dict"``."""
    alternatives = [alt.strip() for alt in str(declared).split("|")]
    unknown = [alt for alt in alternatives if alt not in _TYPES]
    if unknown:
        return [f"{name}: schema declares unknown type {unknown[0]!r} for {key!r}"]

    # bool subclasses int in Python, so `isinstance(True, int)` is True. A
    # boolean where a number was declared is a defect, not a narrow int.
    if isinstance(value, bool) and "bool" not in alternatives:
        return [f"{name}: {key!r} is bool, expected {declared}"]

    if any(isinstance(value, _TYPES[alt]) for alt in alternatives):
        return []
    return [f"{name}: {key!r} is {type(value).__name__}, expected {declared}"]
