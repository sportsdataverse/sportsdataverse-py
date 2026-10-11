"""Assert recipe/meta.yaml mirrors pyproject.toml: same version, every runtime dependency, same bounds.

    uv run python tools/check_recipe_mirror.py [--pyproject PATH] [--recipe PATH]

Prints ``RECIPE OK: version X matches, N deps mirrored`` and exits 0, or names every
mismatch and exits 1. The recipe is hand-mirrored (no generator), so this is the only
thing that notices when a dependency change in pyproject.toml is not carried over.

Rules: a pyproject dependency with a ``sys_platform`` marker is exempt (a ``noarch:
python`` recipe cannot carry platform selectors); every other one must appear in the
recipe's ``run:`` list, under its conda name, with the same version specifier. A
python-version marker (``tomli; python_version < '3.11'``) is listed unconditionally in
the recipe, so only the specifier is compared.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # the 3.10 floor: tomli is declared for python_version < '3.11' (same split as scrape/ncaa/canary.py)
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]

# PyPI name -> conda-forge name where they differ.
CONDA_NAMES = {"matplotlib": "matplotlib-base"}

_REQ = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(\[[^\]]*\])?\s*([^;]*?)\s*(?:;\s*(.*))?$")
# The block ends at the next top-level key or the next sibling under requirements:,
# which includes `run_constrained:` (optional pins; never a runtime dependency).
_RECIPE_RUN = re.compile(r"^  run:\n(.*?)(?=^\S|^  [a-z_]+:)", re.M | re.S)
_RECIPE_DEP = re.compile(r"^\s*-\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(.*?)\s*$", re.M)
_RECIPE_VERSION = re.compile(r'\{%\s*set\s+version\s*=\s*"([^"]+)"\s*%\}')


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _spec(spec: str) -> str:
    return spec.replace(" ", "")


def pyproject_deps(text: str) -> tuple[str, dict[str, str]]:
    """Return (version, {conda_name: specifier}) for the deps the recipe must carry."""
    project = tomllib.loads(text)["project"]
    deps: dict[str, str] = {}
    for raw in project["dependencies"]:
        m = _REQ.match(raw)
        if m is None:
            raise ValueError(f"unparseable requirement {raw!r}")
        name, _extras, spec, marker = m.groups()
        if marker and "sys_platform" in marker:
            continue
        name = _norm(name)
        deps[CONDA_NAMES.get(name, name)] = _spec(spec)
    return project["version"], deps


def recipe_deps(text: str) -> tuple[str, dict[str, str]]:
    """Return (version, {name: specifier}) from the recipe's ``run:`` block, ``python`` excluded."""
    vm = _RECIPE_VERSION.search(text)
    rm = _RECIPE_RUN.search(text)
    if vm is None or rm is None:
        raise ValueError("recipe is missing the version line or the run: block")
    deps = {}
    for line in rm.group(1).splitlines():
        line = line.split("#", 1)[0]
        m = _RECIPE_DEP.match(line)
        if m is None:
            continue
        name, spec = m.groups()
        if _norm(name) != "python":
            deps[_norm(name)] = _spec(spec)
    return vm.group(1), deps


def check(pyproject: Path, recipe: Path) -> list[str]:
    """Return the mismatches (empty when the recipe mirrors pyproject)."""
    version, want = pyproject_deps(pyproject.read_text(encoding="utf-8"))
    recipe_version, have = recipe_deps(recipe.read_text(encoding="utf-8"))
    problems = []
    if recipe_version != version:
        problems.append(f"version: recipe says {recipe_version}, pyproject says {version}")
    for name, spec in want.items():
        if name not in have:
            problems.append(f"missing from recipe: {name} {spec}")
        elif have[name] != spec:
            problems.append(f"bounds differ for {name}: recipe {have[name]!r}, pyproject {spec!r}")
    for name in have:
        if name not in want:
            problems.append(f"in recipe but not pyproject: {name}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pyproject", type=Path, default=ROOT / "pyproject.toml")
    parser.add_argument("--recipe", type=Path, default=ROOT / "recipe" / "meta.yaml")
    args = parser.parse_args(argv)
    problems = check(args.pyproject, args.recipe)
    if problems:
        print("RECIPE MISMATCH:")
        for p in problems:
            print(f"  - {p}")
        return 1
    version, deps = pyproject_deps(args.pyproject.read_text(encoding="utf-8"))
    print(f"RECIPE OK: version {version} matches, {len(deps)} deps mirrored")
    return 0


if __name__ == "__main__":
    sys.exit(main())
