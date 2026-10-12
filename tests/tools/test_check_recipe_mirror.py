"""The conda recipe is hand-mirrored from pyproject.toml; this is what notices when it drifts (A-M24).

Before #751 the recipe said version 0.0.51 against a 0.1.5 pyproject and omitted scikit-learn, which
12 package modules import, so a conda install broke on every sklearn-using module and nothing diffed
the two lists.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tools import check_recipe_mirror as mirror

ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"
RECIPE = ROOT / "recipe" / "meta.yaml"


def test_committed_recipe_mirrors_pyproject(capsys):
    assert mirror.check(PYPROJECT, RECIPE) == []
    assert mirror.main(["--pyproject", str(PYPROJECT), "--recipe", str(RECIPE)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("RECIPE OK: version ")
    assert "deps mirrored" in out


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda s: s.replace("    - scikit-learn >=1.0.0\n", ""), "missing from recipe: scikit-learn >=1.0.0"),
        (lambda s: s.replace('set version = "', 'set version = "0.0.51', 1), "version: recipe says 0.0.51"),
        (lambda s: s.replace("    - urllib3 >=2.8.0\n", "    - urllib3 >=2.7.0\n"), "bounds differ for urllib3"),
        (
            lambda s: s.replace("    - urllib3 >=2.8.0\n", "    - urllib3 >=2.8.0\n    - flask >=1.0\n"),
            "in recipe but not pyproject: flask",
        ),
        (
            # run_constrained: is an OPTIONAL pin; a dependency moved there is missing from the install
            lambda s: s.replace("    - scikit-learn >=1.0.0\n", "").replace(
                "\ntest:\n", "  run_constrained:\n    - scikit-learn >=1.0.0\n\ntest:\n"
            ),
            "missing from recipe: scikit-learn >=1.0.0",
        ),
    ],
    ids=["missing-dep", "stale-version", "lower-bound-drift", "extra-dep", "moved-to-run-constrained"],
)
def test_drift_is_named_and_exits_1(tmp_path, capsys, mutation, expected):
    original = RECIPE.read_text(encoding="utf-8")
    mutated = mutation(original)
    assert mutated != original, "the mutation did not apply; the fixture no longer matches"
    recipe = tmp_path / "meta.yaml"
    recipe.write_text(mutated, encoding="utf-8")
    assert mirror.main(["--pyproject", str(PYPROJECT), "--recipe", str(recipe)]) == 1
    assert expected in capsys.readouterr().out


def test_platform_marked_dependency_is_exempt():
    version, deps = mirror.pyproject_deps(PYPROJECT.read_text(encoding="utf-8"))
    assert "tzdata" not in deps  # sys_platform marker: a noarch recipe cannot carry it
    assert deps["tomli"] == ">=2.0"  # python_version marker: listed unconditionally, bounds still compared
