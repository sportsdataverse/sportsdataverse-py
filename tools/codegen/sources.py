"""Source registry loader: resolve a public function to exactly one provider or category.

``sources.yaml`` is the single source of truth for "where does this data come
from". Resolution per function, first tier that matches wins:

1. the ESPN API name / flat-API stem / release base the codegen already knows --
   authoritative, because a generated wrapper or loader comes from exactly that API
   and no glob can know better
2. ``functions:`` glob on the function name
3. ``modules:`` glob on ``obj.__module__`` -- providers and categories ranked
   together, most specific glob first

Module rules are literal module paths wherever they can be: a cross-package
suffix glob (``sportsdataverse.*.*_schedule``) silently claimed every future
module with that suffix (``mbb_strength_of_schedule`` once resolved to ESPN), so
the registry fails closed and a new module needs its own line.

Exactly one entry must match within the winning tier: zero raises
:class:`UnknownSource`, two or more raises :class:`AmbiguousSource`. Both are
failures of ``generate.py --check`` -- there is no allowlist, because an
unmapped function is a docs gap and two matching rules is a registry bug.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import functools
from pathlib import Path

SOURCES_FILE = Path(__file__).resolve().parent / "sources.yaml"

_SECTIONS = (("providers", "provider"), ("categories", "category"))


class UnknownSource(Exception):
    """No sources.yaml rule matches a public function."""


class AmbiguousSource(Exception):
    """More than one sources.yaml rule matches a public function."""


@dataclasses.dataclass(frozen=True)
class Entry:
    """One provider or helper category, with the rules that match functions to it."""

    key: str
    label: str
    kind: str
    order: int
    home: str = ""
    auth: str = ""
    espn_apis: tuple[str, ...] = ()
    flat_apis: tuple[str, ...] = ()
    release_bases: tuple[str, ...] = ()
    modules: tuple[str, ...] = ()
    functions: tuple[str, ...] = ()

    @property
    def apis(self) -> tuple[str, ...]:
        """Every API name/stem this entry owns, ESPN first then flat."""
        return (*self.espn_apis, *self.flat_apis)


@functools.lru_cache(maxsize=1)
def load() -> tuple[Entry, ...]:
    """Every registry entry in file order: providers first, then categories."""
    import yaml

    doc = yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8")) or {}
    out: list[Entry] = []
    for section, kind in _SECTIONS:
        for key, rule in (doc.get(section) or {}).items():
            rule = rule or {}
            out.append(
                Entry(
                    key=key,
                    label=rule.get("label", key),
                    kind=kind,
                    order=len(out),
                    home=rule.get("home", ""),
                    auth=rule.get("auth", ""),
                    espn_apis=tuple(rule.get("espn_apis") or ()),
                    flat_apis=tuple(rule.get("flat_apis") or ()),
                    release_bases=tuple(rule.get("release_bases") or ()),
                    modules=tuple(rule.get("modules") or ()),
                    functions=tuple(rule.get("functions") or ()),
                ),
            )
    return tuple(out)


def providers() -> tuple[Entry, ...]:
    """Registry entries that are data sources, in registry order."""
    return tuple(e for e in load() if e.kind == "provider")


def categories() -> tuple[Entry, ...]:
    """Registry entries that are helper categories, in registry order."""
    return tuple(e for e in load() if e.kind == "category")


def by_api(api: str) -> Entry | None:
    """The provider that owns an ESPN API name or flat-API stem, or ``None``."""
    return next((e for e in load() if api in e.apis), None)


def by_base(base: str) -> Entry | None:
    """The provider that owns a ``releases.yaml`` base key, or ``None``."""
    return next((e for e in load() if base in e.release_bases), None)


def _specificity(glob: str) -> tuple[int, int]:
    """Sort key for "most specific glob wins": most literal characters, then fewest wildcards.

    Specificity is how much of the name a glob actually pins down, so
    ``sportsdataverse.*.nba_fox_ext`` (28 literal chars) beats ``sportsdataverse.nba.*``
    (20), and a category's literal ``sportsdataverse.mbb.mbb_ncaa_models`` beats a
    provider's ``sportsdataverse.mbb.mbb_ncaa_*``. A true tie (two rules pinning down
    equally much, such as one module listed under two entries) stays ambiguous on
    purpose: that is a registry bug to fix, not a coin to flip.
    """
    wild = glob.count("*") + glob.count("?")
    return (len(glob) - wild, -wild)


def _match(entries: tuple[Entry, ...], value: str, field: str) -> list[Entry]:
    """Entries whose ``field`` globs match ``value``, keeping only the most specific tie group."""
    hits: list[tuple[tuple[int, int], Entry]] = []
    for e in entries:
        best = max(
            (_specificity(g) for g in getattr(e, field) if fnmatch.fnmatchcase(value, g)),
            default=None,
        )
        if best is not None:
            hits.append((best, e))
    if not hits:
        return []
    top = max(k for k, _ in hits)
    return [e for k, e in hits if k == top]


def _one(hits: list[Entry], name: str, module: str) -> Entry:
    if len(hits) > 1:
        raise AmbiguousSource(
            f"{name} ({module}) matches {len(hits)} sources.yaml rules: " + ", ".join(e.key for e in hits),
        )
    return hits[0]


def resolve(name: str, module: str, *, api: str = "", base: str = "") -> Entry:
    """The one registry entry for a public function, or raise.

    Args:
        name: The public function name (``espn_nba_pbp``).
        module: The function's ``__module__`` (``sportsdataverse.nba.nba_pbp``).
        api: For a generated wrapper, the ESPN API name or flat-API stem it came from.
        base: For a generated loader, its ``releases.yaml`` base key.

    Returns:
        The matching :class:`Entry`.

    Raises:
        AmbiguousSource: Two or more rules match in the winning tier.
        UnknownSource: No rule matches in any tier.
    """
    if api and (e := by_api(api)) is not None:
        return e
    if base and (e := by_base(base)) is not None:
        return e
    if hits := _match(load(), name, "functions"):
        return _one(hits, name, module)
    if hits := _match(load(), module, "modules"):
        return _one(hits, name, module)
    raise UnknownSource(f"{name} ({module}) matches no sources.yaml rule -- add one to tools/codegen/sources.yaml")
