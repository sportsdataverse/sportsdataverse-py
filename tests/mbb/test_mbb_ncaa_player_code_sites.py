"""Guard: no new `build_player_code` call site may bypass the box roster.

This test exists because the fix for sibling player codes shipped THREE
times before it was complete. `validate_box_score` widens colliding codes
(`MarkieffMorris` / `MarcusMorris`), and any code path that later re-derives
a code from a name with `build_player_code` silently un-widens it -- the two
siblings collapse back onto one code, one wins the roster match, and the
other DISAPPEARS from the output.

The first pass fixed 4 sites in two modules. It missed 8 more in three other
modules, and the miss was invisible: games parsed, counts looked healthy, and
NIU 2015 (the Armstead brothers) produced lineups for 3 of 30 games.

Greping two modules is not a check. This is.

`code_from_box` is the roster-resolving entry point; use it anywhere a box
lineup is reachable. The allowlist below is every legitimate exception, each
with the reason it cannot consult a roster.
"""

from __future__ import annotations

import ast
from pathlib import Path

_PKG = Path(__file__).resolve().parents[2] / "sportsdataverse"

#: module -> {line-anchored reason}. A call site is allowed ONLY if the module
#: is listed AND the reason still holds. Adding an entry is a deliberate act:
#: state why the roster is genuinely unavailable there.
_ALLOWED: "dict[str, str]" = {
    # The definition itself, plus the box parser that establishes the codes.
    "mbb/mbb_ncaa_stints.py": "defines build_player_code; _code() falls back only when no box lineup exists",
    "mbb/mbb_ncaa_boxscore_parser.py": "validate_box_score IS the site that derives + widens the roster codes",
    "mbb/mbb_ncaa_names.py": "code_from_box's own fallback for a name absent from the roster",
    # Opponent-side and roster-page paths: no box lineup for that team here.
    "mbb/mbb_ncaa_pbp_glue.py": "opponent path -- team is None, no roster available",
    "mbb/mbb_ncaa_shot_parser.py": "opponent path -- team is None, no roster available",
    "mbb/mbb_ncaa_roster_parser.py": "parses the roster PAGE; it is the source, not a consumer",
    "scrape/ncaa/identity.py": "identity enrichment runs off a name list, not a box lineup",
}


def _call_sites() -> "list[tuple[str, int]]":
    out: "list[tuple[str, int]]" = []
    for path in sorted(_PKG.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - not expected in-tree
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", None)
            if name == "build_player_code":
                out.append((path.relative_to(_PKG).as_posix(), node.lineno))
    return out


def test_no_unguarded_build_player_code_call_sites() -> None:
    """Every real call lives in an allowlisted module with a stated reason."""
    offenders = sorted({mod for mod, _ in _call_sites() if mod not in _ALLOWED})
    assert not offenders, (
        "build_player_code called outside the allowlist: "
        f"{offenders}. Use code_from_box(name, box_lineup, team) so a widened "
        "sibling code is not silently re-derived; if the roster is genuinely "
        "unreachable, add the module to _ALLOWED with the reason."
    )


def test_allowlist_has_no_stale_entries() -> None:
    """An allowlist that outlives its call sites stops being a guard.

    If a module is cleaned up, its exemption must go too -- otherwise the next
    re-derivation added there is silently permitted.
    """
    live = {mod for mod, _ in _call_sites()}
    stale = sorted(set(_ALLOWED) - live)
    assert not stale, f"_ALLOWED lists modules with no build_player_code call: {stale}"


def test_the_guard_can_actually_fail() -> None:
    """The detector finds real calls -- a guard that matches nothing passes vacuously."""
    sites = _call_sites()
    assert len(sites) >= 8, f"expected the known call sites, found {len(sites)}"
