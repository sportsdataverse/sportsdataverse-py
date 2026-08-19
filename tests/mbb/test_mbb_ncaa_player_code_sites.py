"""Guard: no new `build_player_code` call site may bypass the box roster.

This test exists because the fix for sibling player codes shipped THREE
times before it was complete. `validate_box_score` widens colliding codes
(`MarkieffMorris` / `MarcusMorris`), and any path that later re-derives a
code from a name with `build_player_code` silently un-widens it -- the two
siblings collapse back onto one code, one wins the roster match, and the
other DISAPPEARS from the output.

The first pass fixed 4 sites in two modules. It missed 8 more in three other
modules, and the miss was invisible: games parsed, counts looked healthy, and
NIU 2014-15 (the Armstead brothers) produced lineups for 3 of 30 games.

Greping two modules is not a check. This is.

`code_from_box` is the roster-resolving entry point; use it anywhere a box
lineup is reachable.

**The allowlist is keyed per CALL SITE, not per module, and pins an exact
count.** A module-level allowlist would let a new unguarded call inside an
already-listed module pass -- which is this very bug at a finer grain, and
several allowlisted modules (`mbb_ncaa_shot_parser`, `mbb_ncaa_stints`) hold
BOTH a legitimate no-roster path and roster-aware paths. Identity is
`(module, enclosing qualname)` rather than a line number, so the guard
survives reformatting but still fails on a genuinely new site -- and the
pinned count fails on a second call added to an already-listed function.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

_PKG = Path(__file__).resolve().parents[2] / "sportsdataverse"

#: (module, enclosing qualname) -> (expected number of calls, why no roster).
#: Adding an entry is a deliberate act: state why a roster is unreachable there.
_ALLOWED: "dict[tuple[str, str], tuple[int, str]]" = {
    ("mbb/mbb_ncaa_boxscore_parser.py", "validate_box_score"): (
        1,
        "THE site that derives and widens the roster codes",
    ),
    ("mbb/mbb_ncaa_names.py", "code_from_box"): (1, "fallback for a name absent from the roster"),
    ("mbb/mbb_ncaa_names.py", "tidy_player"): (1, "builds a lookup key, not an emitted code"),
    ("mbb/mbb_ncaa_pbp_glue.py", "extract_player_from_ev"): (1, "opponent path -- team is None, no roster"),
    ("mbb/mbb_ncaa_roster_parser.py", "parse_roster"): (1, "parses the roster PAGE; it is the source"),
    ("mbb/mbb_ncaa_shot_parser.py", "parse_shot_html"): (1, "opponent path -- team is None, no roster"),
    ("mbb/mbb_ncaa_stints.py", "_code"): (1, "fallback only when the caller has no box lineup"),
    ("scrape/ncaa/identity.py", "load_roster_index"): (1, "runs off a name list, not a box lineup"),
}


class _Collector(ast.NodeVisitor):
    """Records every `build_player_code` call with its enclosing qualname."""

    def __init__(self) -> None:
        self.stack: "list[str]" = []
        self.sites: "list[str]" = []

    def _scoped(self, node: "ast.AST") -> None:
        self.stack.append(getattr(node, "name", "?"))
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = _scoped
    visit_AsyncFunctionDef = _scoped
    visit_ClassDef = _scoped

    def visit_Call(self, node: ast.Call) -> None:
        fn = node.func
        name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", None)
        if name == "build_player_code":
            self.sites.append(".".join(self.stack) if self.stack else "<module>")
        self.generic_visit(node)


def _call_sites() -> "Counter[tuple[str, str]]":
    found: "Counter[tuple[str, str]]" = Counter()
    for path in sorted(_PKG.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - not expected in-tree
            continue
        c = _Collector()
        c.visit(tree)
        mod = path.relative_to(_PKG).as_posix()
        for qual in c.sites:
            found[(mod, qual)] += 1
    return found


def test_call_sites_match_the_allowlist_exactly() -> None:
    """New sites fail; extra calls in an allowlisted function fail too."""
    found = _call_sites()
    expected = {k: v[0] for k, v in _ALLOWED.items()}
    added = {k: n for k, n in found.items() if k not in expected}
    grown = {k: (expected[k], n) for k, n in found.items() if k in expected and n != expected[k]}
    assert not added, (
        f"build_player_code called at un-allowlisted site(s): {sorted(added)}. "
        "Use code_from_box(name, box_lineup, team) so a widened sibling code is not "
        "silently re-derived; if the roster is genuinely unreachable, add the site "
        "to _ALLOWED with the reason."
    )
    assert not grown, f"call count changed at allowlisted site(s) (expected, found): {grown}"


def test_allowlist_has_no_stale_entries() -> None:
    """An exemption that outlives its call site stops being a guard."""
    found = _call_sites()
    stale = sorted(k for k in _ALLOWED if k not in found)
    assert not stale, f"_ALLOWED lists sites with no build_player_code call: {stale}"


def test_the_guard_can_actually_fail() -> None:
    """The detector finds real calls -- a guard matching nothing passes vacuously."""
    found = _call_sites()
    assert sum(found.values()) >= 8, f"expected the known call sites, found {sum(found.values())}"
    assert ("mbb/mbb_ncaa_names.py", "code_from_box") in found
