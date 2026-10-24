"""The one OpenAPI query-parameter reader every flat-API generator shares.

Sport-neutral on purpose: Yahoo, CBS, On3, Euroleague and F1 read their specs through it as well as
the soccer generators, so it lives beside them rather than in ``gen_soccer_common`` (review R858-1).
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any, Callable

from sportsdataverse.dl_utils import underscore


def _spec_param_description(prm: dict) -> str:
    """A spec parameter's own ``description``, stripped (``""`` when absent)."""
    return str(prm.get("description") or "").strip()


def _query_params(
    op: dict,
    *,
    path_names: Collection[str] = (),
    on_clash: str = "skip",
    names: dict[str, str] | None = None,
    pytypes: dict[str, str] | None = None,
    skip: Collection[str] = (),
    description: Callable[[dict], str] | None = _spec_param_description,
    keep_empty_description: bool = False,
    required: str = "ignore",
) -> list[dict[str, Any]]:
    """An operation's ``in: query`` parameters as endpoint-YAML ``extra_params`` entries.

    The one query-parameter reader shared by every flat-API generator that walks an
    OpenAPI ``parameters`` list (``gen_asa``, ``gen_cbs``, ``gen_euroleague``, ``gen_f1``,
    ``gen_fotmob``, ``gen_mls``, ``gen_nwsl``, ``gen_on3``, ``gen_uefa``, ``gen_yahoo``).
    Each caller names its policy in keywords instead of carrying its own copy.

    Args:
        op: The OpenAPI operation object.
        path_names: Python names already taken by path parameters.
        on_clash: What to do when a query name equals a path name: ``"skip"`` drops the
            query param (the path token wins), ``"prefix"`` renames it ``filter_<name>``.
        names: Wire key -> python name overrides; other keys are snake-cased.
        pytypes: JSON-schema type -> python type; ``None`` types every param ``str``.
        skip: Wire keys or python names to leave out.
        description: Reads a parameter's description; ``None`` writes no description.
        keep_empty_description: Write ``description: ""`` instead of omitting the key.
        required: How the spec's ``required: true`` is carried. ``"flag"`` writes
            ``required: true`` (a positional, non-Optional argument), ``"note"`` prefixes
            the description with ``Required.``, ``"both"`` does both, ``"ignore"`` drops it.
            Yahoo and NWSL pass ``"ignore"`` until the owner decides on their signature
            change (A-M23): flagging them makes 19 Yahoo and one NWSL argument required.

    Returns:
        One dict per query parameter (``name``, ``query_key``, ``type`` and, by policy,
        ``description`` / ``required``), in spec order.

    Raises:
        ValueError: ``on_clash`` or ``required`` is not one of the documented values.
    """
    if on_clash not in ("skip", "prefix"):
        raise ValueError(f"on_clash must be 'skip' or 'prefix'; got {on_clash!r}")
    if required not in ("flag", "note", "both", "ignore"):
        raise ValueError(f"required must be 'flag', 'note', 'both' or 'ignore'; got {required!r}")
    out: list[dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if "$ref" in prm or prm.get("in") != "query":
            continue
        wire = prm["name"]
        name = (names or {}).get(wire) or underscore(wire)
        if wire in skip or name in skip:
            continue
        if name in path_names:
            if on_clash == "skip":
                continue
            name = f"filter_{name}"
        jtype = (prm.get("schema") or {}).get("type") or "string"
        entry: dict[str, Any] = {
            "name": name,
            "query_key": wire,
            "type": pytypes.get(jtype, "str") if pytypes is not None else "str",
        }
        is_required = bool(prm.get("required"))
        if description is not None:
            desc = description(prm)
            if is_required and required in ("note", "both"):
                desc = f"Required. {desc}".strip()
            if desc or keep_empty_description:
                entry["description"] = desc
        if is_required and required in ("flag", "both"):
            entry["required"] = True
        out.append(entry)
    return out
