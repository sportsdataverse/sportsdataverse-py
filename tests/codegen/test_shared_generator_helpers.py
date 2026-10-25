"""One copy of the generator helpers (task 6.1 (c): A-L7, A-L22, A-L23, A-L24, A-N18).

The flat-API generators share ``tools/codegen/gen_soccer_common.py``. Static checks only (the
generators need the private sdv-internal-refs checkout to run):

* every generator that carried its own copy now imports the shared helper and defines no copy;
* no module under ``tools/codegen`` holds a body equal to a shared helper's, so a new copy fails here;
* each shared body hashes to the deleted copies' (docstring and name excluded) at the merge commit.
"""

from __future__ import annotations

import ast
import hashlib
import re
from pathlib import Path

import pytest

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

# shared helper -> hash of the copies it replaced
PINS = {
    "write_yaml": "cde26123dbd25114",
    "rewrite_schema_dir": "4811f2004dfacda7",
    "capture_for": "4802852069df1789",
    "refs_root": "8091b40d284bc12d",
}


def _tree(name: str) -> ast.Module:
    return ast.parse((CODEGEN / name).read_text(encoding="utf-8"))


def _body_key(node: ast.FunctionDef) -> str:
    body = node.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    sig = ast.dump(node.args) + (ast.dump(node.returns) if node.returns else "")
    return hashlib.sha256((sig + "".join(ast.dump(b) for b in body)).encode()).hexdigest()[:16]


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


def test_no_generator_carries_a_copy_of_a_shared_helper() -> None:
    shared = {
        _body_key(fn): name
        for name, fn in _functions(_tree(COMMON)).items()
        if name in PINS or name in {"refs_root", "refs_dir", "load_spec", "snake_path"}
    }
    copies = [
        f"{path.name}:{fn.name} == gen_soccer_common.{shared[_body_key(fn)]}"
        for path in sorted(CODEGEN.glob("*.py"))
        if path.name != COMMON
        for fn in _functions(ast.parse(path.read_text(encoding="utf-8"))).values()
        if _body_key(fn) in shared
    ]
    assert copies == []


@pytest.mark.parametrize(("name", "pin"), sorted(PINS.items()))
def test_the_shared_body_equals_the_copies_it_replaced(name: str, pin: str) -> None:
    assert _body_key(_functions(_tree(COMMON))[name]) == pin, f"gen_soccer_common.{name} changed; update the pin"


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
