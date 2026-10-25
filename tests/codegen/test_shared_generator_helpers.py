"""One copy of the generator helpers (task 6.1 (c): A-L7, A-L22, A-L23, A-L24, A-N18).

The flat-API generators share ``tools/codegen/gen_soccer_common.py``. Static checks only (the
generators need the private sdv-internal-refs checkout to run):

* every generator that carried its own copy now imports the shared helper and defines no copy;
* no module under ``tools/codegen`` holds a body equal to a shared helper's, so a new copy fails here, with or
  without type hints and whatever its parameters are called (PRE-1008 B3);
* each shared body's canonical key (``tests/_body_key.py``, the same on Python 3.10-3.14) equals the deleted
  copies' at the merge commit (re-pinned under that key in PRE-1008 M-3).
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests._body_key import body_key

CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
COMMON = "gen_soccer_common.py"

# generator -> (helpers it must import from gen_soccer_common, private copies it must not define)
FORMER_COPIES = {
    "gen_cbs.py": (
        {"write_yaml", "rewrite_schema_dir", "refs_dir", "load_spec"},
        {"_write_yaml", "_clean_generated_schema_dir", "_refs_dir", "_load_spec"},
    ),
    "gen_nba_stats.py": ({"write_yaml", "rewrite_schema_dir"}, {"_write_yaml", "_clean_generated_schema_dir"}),
    "gen_on3.py": ({"write_yaml", "refs_root"}, {"_write_yaml"}),
    "gen_pff.py": (
        {"write_yaml", "rewrite_schema_dir", "refs_root"},
        {"_write_yaml", "_clean_generated_schema_dir"},
    ),
    "gen_pff_api.py": ({"write_yaml", "refs_root"}, {"_write_yaml"}),
    "gen_sports247_site_pages.py": (
        {"write_yaml", "rewrite_schema_dir", "refs_root"},
        {"_write_yaml", "_clean_generated_schema_dir"},
    ),
    "gen_yahoo.py": ({"write_yaml", "refs_root"}, {"_write_yaml", "_refs_root"}),
    "gen_sports247.py": ({"refs_root"}, set()),
    "gen_espn_content.py": ({"capture_for"}, {"_capture_for"}),
    "gen_kalshi.py": ({"capture_for"}, {"_capture_for"}),
    "gen_openligadb.py": ({"capture_for"}, {"_capture_for"}),
    "gen_sleeper.py": ({"capture_for"}, {"_capture_for"}),
    "gen_easl.py": ({"snake_path"}, {"_snake_path"}),
    "gen_kbl.py": ({"snake_path"}, {"_snake_path"}),
    "gen_kzs.py": ({"snake_path"}, {"_snake_path"}),
    "gen_wjbl.py": (set(), {"_snake_path"}),
    "gen_f1.py": ({"snake_path"}, {"_snake_path"}),
    "gen_uefa.py": ({"snake_path"}, {"_snake_path"}),
    "gen_euroleague.py": ({"snake_path"}, {"_snake_path"}),
}

# shared helper -> canonical key of the copies it replaced, except where marked "current body"
PINS = {
    "write_yaml": "d2c0fd81daca3498",
    "rewrite_schema_dir": "186076d5a47055b1",
    "capture_for": "63b9a789159e1082",
    # current body, deliberately changed in #994 when the variants were unified (env, then a sibling checkout,
    # then the default); the deleted gen_yahoo copy keyed f631060855bea9a5. #1000 touched only the module docstring
    "refs_root": "cc3e9ecd474dc205",
}


def _tree(name: str) -> ast.Module:
    return ast.parse((CODEGEN / name).read_text(encoding="utf-8"))


def _functions(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    return {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}


def _shared_imports(tree: ast.Module) -> set[str]:
    return {
        a.asname or a.name
        for n in ast.walk(tree)
        if isinstance(n, ast.ImportFrom) and (n.module or "").endswith("gen_soccer_common")
        for a in n.names
    }


@pytest.mark.parametrize("gen", sorted(FORMER_COPIES))
def test_a_former_copy_imports_the_shared_helper(gen: str) -> None:
    must_import, must_not_define = FORMER_COPIES[gen]
    tree = _tree(gen)
    assert must_import <= _shared_imports(tree), f"{gen} does not import {must_import - _shared_imports(tree)}"
    assert not (must_not_define & set(_functions(tree))), (
        f"{gen} defines its own {must_not_define & set(_functions(tree))}"
    )


def _copies_in(paths) -> list[str]:
    """Every function in ``paths`` whose canonical body key equals a shared helper's."""
    shared = {
        body_key(fn): name
        for name, fn in _functions(_tree(COMMON)).items()
        if name in PINS or name in {"refs_root", "refs_dir", "load_spec", "snake_path"}
    }
    return [
        f"{path.name}:{fn.name} == gen_soccer_common.{shared[body_key(fn)]}"
        for path in sorted(paths)
        if path.name != COMMON
        for fn in _functions(ast.parse(path.read_text(encoding="utf-8"))).values()
        if body_key(fn) in shared
    ]


def test_no_generator_carries_a_copy_of_a_shared_helper() -> None:
    assert _copies_in(CODEGEN.glob("*.py")) == []


def _pasted_copy(name: str, *, strip_hints: bool, rename: bool) -> str:
    """The shared helper ``name`` as someone might paste it into a generator: private name, hints dropped, and/or
    every parameter renamed."""
    fn = _functions(_tree(COMMON))[name]
    params = [a for a in fn.args.posonlyargs + fn.args.args + fn.args.kwonlyargs]
    names = {a.arg: f"{a.arg}_x" for a in params} if rename else {}
    for a in params:
        a.arg = names.get(a.arg, a.arg)
        if strip_hints:
            a.annotation = None
    if strip_hints:
        fn.returns = None
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and node.id in names:
            node.id = names[node.id]
    fn.name = f"_{name}"
    return ast.unparse(fn)


@pytest.mark.parametrize(
    ("strip_hints", "rename"), [(True, False), (False, True), (True, True)], ids=["no_hints", "renamed", "both"]
)
def test_a_pasted_copy_without_hints_or_with_renamed_parameters_is_caught(tmp_path, strip_hints, rename) -> None:
    """PRE-1008 B3: the detector compared exact copies only, so a pasted helper with its type hints dropped or its
    parameters renamed passed. The canonical key ignores both."""
    gen = tmp_path / "gen_pasted.py"
    gen.write_text(
        "import yaml\n\n\n" + _pasted_copy("write_yaml", strip_hints=strip_hints, rename=rename) + "\n",
        encoding="utf-8",
    )
    assert _copies_in([gen]) == ["gen_pasted.py:_write_yaml == gen_soccer_common.write_yaml"]


def test_a_real_body_change_turns_the_key_red() -> None:
    """PRE-1008 M-3: the canonical key ignores names, hints and docstrings, not behaviour: one changed argument in
    the shared body gives a different key, so its pin fails."""
    fn = _functions(_tree(COMMON))["write_yaml"]
    pinned = body_key(fn)
    changed = ast.parse(ast.unparse(fn).replace("sort_keys=True", "sort_keys=False")).body[0]
    assert body_key(changed) != pinned == PINS["write_yaml"]


@pytest.mark.parametrize(("name", "pin"), sorted(PINS.items()))
def test_the_shared_body_equals_the_copies_it_replaced(name: str, pin: str) -> None:
    assert body_key(_functions(_tree(COMMON))[name]) == pin, f"gen_soccer_common.{name} changed; update the pin"


def test_no_generator_inlines_snake_path() -> None:
    """R61c-2: an inline re-copy of ``snake_path`` (``"{" + underscore(m.group(1)) + "}"`` in a token
    ``sub``) is not AST-identical to the shared body, so the body-hash check above misses it."""
    copies = [
        f"{path.name}:{i}"
        for path in sorted(CODEGEN.glob("*.py"))
        if path.name != COMMON
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if re.search(r'"\{"\s*\+\s*underscore\(', line)
    ]
    assert copies == [], f"use gen_soccer_common.snake_path: {copies}"


def test_no_generator_reads_the_refs_env_itself() -> None:
    """R61c-2: an inline re-copy of ``refs_root`` (a generator reading ``$SDV_INTERNAL_REFS_REPO`` itself, in any
    function or at module level) differs from the shared body's AST, so the body-hash check above misses it."""
    copies = [
        f"{path.name}:{node.lineno}"
        for path in sorted(CODEGEN.glob("*.py"))
        if path.name != COMMON
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call)
        and any(isinstance(a, ast.Constant) and a.value == "SDV_INTERNAL_REFS_REPO" for a in node.args)
    ]
    assert copies == [], f"use gen_soccer_common.refs_root / refs_dir: {copies}"
