"""Generate the ``yahoo_shangrila`` flat-API endpoint YAML + returns-schemas from
the frozen Yahoo Sports OpenAPI specs in ``sdv-internal-refs/yahoo/``.

Spec-driven, NOT reverse engineering: every wrapper, parameter and returns-column
below is read out of a committed spec. Idempotent -- the same specs yield
byte-identical output. Modeled on :mod:`gen_on3`.

Two of the three specs are folded into ONE stem (``yahoo_shangrila``), the way
``on3.yaml`` folds its single ``/rdb/v2`` route in, via an endpoint-level
``host:`` override:

* ``yahoo-sports-shangrila.openapi.yaml`` -- 105 persisted GraphQL queries on
  ``graphite-secure.sports.yahoo.com/v1/query/shangrila``. Family host.
* ``yahoo-sports-editorial.openapi.yaml`` -- 2 routes on
  ``api-secure.sports.yahoo.com/v1/editorial/s`` (scoreboard + boxscore), carried
  as ``editorial_*`` endpoints with a ``host:`` override.

The third spec (``yahoo-sports-ncp.openapi.yaml``, 1 path) is deliberately NOT
wrapped: it is marked ``deprecated: true`` in its own spec and resolves only
inside an authenticated page context (needs consent cookies + a crumb), returning
401/404 to any programmatic request. Its one payload -- the league team list -- is
already reachable unauthenticated through ``editorial_scoreboard``.

Auth: none. ``graphite-secure`` and ``api-secure`` only require
``Origin``/``Referer`` headers, which
:mod:`sportsdataverse.yahoo.yahoo_shangrila_runtime` supplies -- so the YAML sets
``getter_module`` but NOT ``auth: true``.

The three locale parameters every path declares (``lang``/``region``/``tz``, all
optional with spec defaults) are NOT emitted as wrapper arguments: the runtime
sends the spec defaults and a caller overrides them with ``params={"lang": ...}``.
Emitting them would add three no-op arguments to all 107 wrappers.

Returns-schemas:

* shangrila -- resolved from the spec's 200-response component by descending
  through single-key wrapper levels (``data.leagues[].leaders[]`` -> the
  ``leaders`` item), then flattening nested objects the way
  :func:`pandas.json_normalize` does, so the ``@return`` table matches the
  columns the parser actually emits. Endpoints whose ``data`` object carries more
  than one collection get ``kind: frames`` (one section per collection).
* editorial -- the spec's two response components enumerate concrete captured
  game ids as property names, so they carry no reusable column list. Those two
  schemas are instead derived by running the real parser over the committed
  captures in ``yahoo/discovery/responses/``.

Run: ``python tools/codegen/gen_yahoo.py``
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from sportsdataverse.dl_utils import underscore

ROOT = Path(__file__).resolve().parents[2]

HOST = "https://graphite-secure.sports.yahoo.com/v1/query/shangrila"
EDITORIAL_HOST = "https://api-secure.sports.yahoo.com/v1/editorial/s"

_SHANGRILA_PREFIX = "/v1/query/shangrila/"
_EDITORIAL_PREFIX = "/v1/editorial/s/"

# Locale params carried by every path; handled by the runtime, not the signature.
_LOCALE_PARAMS = {"lang", "region", "tz"}

# JSON-schema type -> R-style returns-schema type (mirrors gen_on3._DTYPE).
_DTYPE = {
    "integer": "integer",
    "number": "numeric",
    "string": "character",
    "boolean": "logical",
    "object": "character",
    "array": "character",
    "unknown": "character",
}
# JSON-schema type -> python arg annotation for query/path params.
_PYTYPE = {"integer": "int", "string": "str", "boolean": "bool", "number": "float"}

_TOKEN = re.compile(r"\{([^}]+)\}")


def _refs_root() -> Path:
    """Locate the ``sdv-internal-refs`` checkout.

    ``$SDV_INTERNAL_REFS_REPO`` wins; otherwise the Windows workspace default is
    tried, then a sibling checkout next to this repo (the Linux droplet layout).
    """
    env = os.environ.get("SDV_INTERNAL_REFS_REPO")
    if env:
        return Path(env)
    for cand in (Path("C:/Users/saiem/Documents/sdv-internal-refs"), ROOT.parent / "sdv-internal-refs"):
        if cand.exists():
            return cand
    return ROOT.parent / "sdv-internal-refs"


def _load(name: str) -> dict:
    return yaml.safe_load((_refs_root() / "yahoo" / name).read_text(encoding="utf-8"))


def _short(tail: str) -> str:
    """snake_case slug from a path tail (``OlyMedalCount`` -> ``oly_medal_count``).

    Non-alphanumerics collapse to ``_`` first so ``common/pills`` and
    ``consensus-rankings.php`` become ``common_pills`` / ``consensus_rankings_php``.
    """
    slug = underscore(re.sub(r"[^A-Za-z0-9]+", "_", tail))
    return re.sub(r"_+", "_", slug).strip("_").lower()


# ---------------------------------------------------------------------------
# response-schema resolution
# ---------------------------------------------------------------------------


def _descend(key: str, node: dict) -> Tuple[str, dict, List[str]]:
    """Walk down single-key wrapper levels to the schema of one output ROW.

    Yahoo wraps its rows in one or more single-key envelopes:
    ``data.leagues[0].leaders[]`` carries a ``leagues`` array whose only property
    is ``leaders``. Descending only through levels that have EXACTLY one property
    (and only into a nested collection/object, never a scalar leaf) lands on the
    row schema without guessing. The parser applies the identical rule to the live
    payload, which is what keeps the ``@return`` table honest.

    Args:
        key: name of the collection ``node`` was reached under.
        node: JSON-schema node for that collection.

    Returns:
        ``(final_key, row_schema, path)`` -- the key the rows sit under, the
        schema of one row, and the descended key path.
    """
    path = [key]
    while isinstance(node, dict):
        if node.get("type") == "array":
            node = node.get("items") or {}
            continue
        props = node.get("properties") or {}
        if len(props) == 1:
            only_key, only_val = next(iter(props.items()))
            if isinstance(only_val, dict) and (only_val.get("type") == "array" or only_val.get("properties")):
                key, node, path = only_key, only_val, [*path, only_key]
                continue
        break
    return key, node if isinstance(node, dict) else {}, path


def _flat_cols(obj: dict, prefix: str = "") -> List[Dict[str, str]]:
    """Flatten a row schema's properties the way ``json_normalize(sep="_")`` does.

    Nested OBJECTS with declared properties are flattened into ``parent_child``
    columns (matching the parser); arrays and untyped nodes stay one stringified
    ``character`` column.
    """
    cols: List[Dict[str, str]] = []
    seen: set = set()
    for name, pv in (obj or {}).get("properties", {}).items():
        pv = pv if isinstance(pv, dict) else {}
        key = f"{prefix}{name}"
        if pv.get("type") == "object" and pv.get("properties"):
            cols.extend(_flat_cols(pv, f"{key}_"))
            continue
        col = underscore(key)
        if col in seen:
            continue
        seen.add(col)
        cols.append({"name": col, "type": _DTYPE.get(pv.get("type") or "unknown", "character"), "description": ""})
    # de-duplicate across recursion levels, first occurrence wins
    out: List[Dict[str, str]] = []
    taken: set = set()
    for c in cols:
        if c["name"] in taken:
            continue
        taken.add(c["name"])
        out.append(c)
    return out


def _data_props(op: dict, spec: dict) -> Optional[Dict[str, Any]]:
    """The ``data`` object's properties for an op's 200 response, or ``None``.

    ``None`` means the spec declares a bare ``{"type": "object"}`` (five queries
    whose body was never captured), so no columns can be resolved.
    """
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    ref = schema.get("$ref")
    if not ref:
        return None
    resp = spec["components"]["schemas"][ref.rsplit("/", 1)[-1]]
    data = (resp.get("properties") or {}).get("data") or {}
    return data.get("properties") or None


def _schema_doc(short: str, props: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Build the returns-schema document for one shangrila endpoint."""
    if not props:
        return {"schema": short, "kind": "dataframe", "columns": []}
    if len(props) == 1:
        key, node = next(iter(props.items()))
        _, row, _ = _descend(key, node)
        return {"schema": short, "kind": "dataframe", "columns": _flat_cols(row)}
    frames = []
    for key, node in props.items():
        _, row, _ = _descend(key, node)
        frames.append({"section": underscore(key), "columns": _flat_cols(row)})
    return {"schema": short, "kind": "frames", "frames": frames}


# ---------------------------------------------------------------------------
# endpoint entries
# ---------------------------------------------------------------------------


def _query_params(op: dict, skip: Optional[set] = None) -> List[Dict[str, Any]]:
    skip = skip or set()
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if "$ref" in prm or prm.get("in") != "query":
            continue
        wire = prm["name"]
        name = underscore(wire)
        if wire in _LOCALE_PARAMS or name in skip:
            continue
        jtype = (prm.get("schema") or {}).get("type") or "string"
        out.append({"name": name, "query_key": wire, "type": _PYTYPE.get(jtype, "str")})
    return out


def _summary(op: dict, qname: str, props: Optional[Dict[str, Any]]) -> str:
    """Prefer the spec summary; replace the bare ``shangrila: X`` stub with the
    query name plus the collection(s) the parser will surface."""
    spec_summary = op.get("summary") or ""
    if not spec_summary.startswith("shangrila: "):
        return spec_summary
    if props is None:
        return f"Yahoo shangrila persisted query `{qname}` (response body not captured; shape unknown)"
    if len(props) == 1:
        key, node = next(iter(props.items()))
        final, _, path = _descend(key, node)
        return f"Yahoo shangrila persisted query `{qname}` -> one row per `{'.'.join(path)}` entry"
    tables = ", ".join(underscore(k) for k in props)
    return f"Yahoo shangrila persisted query `{qname}` -> tables: {tables}"


def _shangrila_entries(spec: dict) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for path in sorted(spec["paths"]):
        op = spec["paths"][path]["get"]
        qname = path[len(_SHANGRILA_PREFIX) :]
        short = _short(qname)
        props = _data_props(op, spec)
        multi = bool(props) and len(props) > 1
        entry: Dict[str, Any] = {
            "short": short,
            "summary": _summary(op, qname, props),
            "path": f"/{qname}",
            "parser": "parse_yahoo_shangrila_tables" if multi else "parse_yahoo_shangrila",
            "returns_schema": f"native/yahoo_shangrila/{short}",
        }
        if multi:
            entry["docstring"] = {
                "parsed_doc": "A dict of polars/pandas DataFrames keyed by the payload's ``data`` collections",
            }
        qps = _query_params(op)
        if qps:
            entry["extra_params"] = qps
        entries.append(entry)
    return entries


def _editorial_entries(spec: dict) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for path in sorted(spec["paths"]):
        op = spec["paths"][path]["get"]
        tail = path[len(_EDITORIAL_PREFIX) :]
        short = f"editorial_{_short(_TOKEN.sub('', tail))}"
        pps = [{"name": underscore(tok), "type": "str", "required": True} for tok in _TOKEN.findall(tail)]
        entry: Dict[str, Any] = {
            "short": short,
            "summary": op.get("summary") or f"GET {path}",
            "host": EDITORIAL_HOST,
            "path": "/" + _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", tail),
            "parser": "parse_yahoo_editorial",
            "returns_schema": f"native/yahoo_shangrila/{short}",
            "docstring": {
                "parsed_doc": "A dict of polars/pandas DataFrames keyed by the feed's id-keyed collections",
            },
        }
        if pps:
            entry["path_params"] = pps
            entry["example_args"] = {"game_id": "ncaaf.g.202509200023"}
        qps = _query_params(op, skip={p["name"] for p in pps})
        if qps:
            entry["extra_params"] = qps
        entries.append(entry)
    return entries


# ---------------------------------------------------------------------------
# editorial returns-schemas (derived from the committed captures)
# ---------------------------------------------------------------------------

_CAPTURES = {
    "editorial_scoreboard": "editorial_scoreboard_ncaaf.json",
    "editorial_boxscore": "editorial_boxscore_ncaaf.json",
}


def _pl_doc_type(dtype: Any) -> str:
    s = str(dtype).lower()
    if "int" in s:
        return "integer"
    if "float" in s or "decimal" in s:
        return "numeric"
    if "bool" in s:
        return "logical"
    return "character"


def _editorial_schema_docs() -> Dict[str, Dict[str, Any]]:
    """``kind: frames`` schemas for the two editorial endpoints.

    Built by running the real parser over the committed NCAAF captures, so the
    documented columns are exactly the ones a caller gets back. Returns an empty
    mapping when the captures are unavailable (the endpoints then keep an empty
    returns-schema rather than a fabricated one).
    """
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_editorial

    resp_dir = _refs_root() / "yahoo" / "discovery" / "responses"
    docs: Dict[str, Dict[str, Any]] = {}
    for short, fname in _CAPTURES.items():
        src = resp_dir / fname
        if not src.exists():
            continue
        tables = parse_yahoo_editorial(json.loads(src.read_text(encoding="utf-8")))
        docs[short] = {
            "schema": short,
            "kind": "frames",
            "frames": [
                {
                    "section": name,
                    "columns": [{"name": c, "type": _pl_doc_type(df.schema[c]), "description": ""} for c in df.columns],
                }
                for name, df in tables.items()
            ],
        }
    return docs


# ---------------------------------------------------------------------------


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def main() -> None:
    shangrila = _load("yahoo-sports-shangrila.openapi.yaml")
    editorial = _load("yahoo-sports-editorial.openapi.yaml")

    entries = [*_shangrila_entries(shangrila), *_editorial_entries(editorial)]
    shorts = [e["short"] for e in entries]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise ValueError(f"duplicate yahoo shorts: {dupes}")

    doc = {
        "api": "yahoo_shangrila",
        "host": HOST,
        "name_pattern": "yahoo_{short}",
        "module": "yahoo_shangrila",
        "parser_module": "yahoo.yahoo_shangrila_parsers",
        "getter_module": "sportsdataverse.yahoo.yahoo_shangrila_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "example_import_from": "sportsdataverse.yahoo.yahoo_shangrila",
            "raises": [
                "requests.exceptions.RequestException: Connection-level failure after "
                "``dl_utils.download`` exhausts its retries.",
            ],
            "see_also": [
                {
                    "name": "cfbfastR",
                    "url": "https://cfbfastR.sportsdataverse.org",
                    "note": "R sister package for college football",
                },
                {
                    "name": "Yahoo Sports",
                    "url": "https://sports.yahoo.com",
                    "note": "data origin (unofficial, undocumented endpoints)",
                },
            ],
        },
        "endpoints": entries,
    }
    _write_yaml(ROOT / "tools/codegen/endpoints/yahoo_shangrila.yaml", doc)

    schema_dir = ROOT / "tools/codegen/schemas/native/yahoo_shangrila"
    schema_dir.mkdir(parents=True, exist_ok=True)
    for stale in schema_dir.glob("*.yaml"):
        stale.unlink()
    for path in sorted(shangrila["paths"]):
        short = _short(path[len(_SHANGRILA_PREFIX) :])
        props = _data_props(shangrila["paths"][path]["get"], shangrila)
        _write_yaml(schema_dir / f"{short}.yaml", _schema_doc(short, props))
    editorial_docs = _editorial_schema_docs()
    for short in _CAPTURES:
        _write_yaml(
            schema_dir / f"{short}.yaml",
            editorial_docs.get(short) or {"schema": short, "kind": "dataframe", "columns": []},
        )

    print(
        f"yahoo_shangrila: {len(entries)} endpoints ({len(shangrila['paths'])} shangrila + {len(_CAPTURES)} editorial)"
    )


if __name__ == "__main__":
    main()
