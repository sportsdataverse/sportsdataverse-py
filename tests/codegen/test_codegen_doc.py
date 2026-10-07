"""The codegen architecture page exists, covers the real pipeline, and the drift hooks see generate.py."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "docs" / "architecture" / "codegen.md"
PRECOMMIT = ROOT / ".pre-commit-config.yaml"

_REQUIRED_HEADINGS = [
    "## Inputs",
    "## API generation",
    "## Docs-site generation",
    "## How the two differ",
    "## The drift gate",
    "## Downstream",
]


def test_the_page_exists():
    assert DOC.exists()


@pytest.mark.parametrize("heading", _REQUIRED_HEADINGS)
def test_required_heading(heading):
    assert heading in DOC.read_text(encoding="utf-8")


def test_the_retired_sphinx_instructions_are_gone():
    assert not (ROOT / "docs_instructions.md").exists()


def test_the_page_names_the_six_build_stages():
    text = DOC.read_text(encoding="utf-8")
    for stage in (
        "build()",
        "build_live()",
        "build_parsed_live()",
        "build_flat_live()",
        "build_loaders_live()",
        "build_docs()",
    ):
        assert stage in text


def test_the_page_names_the_four_schema_source_shapes():
    text = DOC.read_text(encoding="utf-8")
    for shape in (
        "schemas/<name>.yaml",
        "schemas/<name>/<league>.yaml",
        "schemas/native/<stem>/",
        "schemas/autodoc/<league>/",
    ):
        assert shape in text


def test_every_count_the_page_states_matches_the_code():
    """The page is a replacement for stale prose, so its own numbers are gated."""
    import json

    from tools.codegen import generate, spec

    text = DOC.read_text(encoding="utf-8")
    cfg = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    registry = json.loads(generate.render_leagues_json())
    facts = {
        "leagues.yaml league count": f"({len(cfg.leagues)} leagues)",
        "flat API count": f"the {len(generate.FLAT_APIS)} flat-API YAMLs",
        "registry league count": f"{sum(len(s['leagues']) for s in registry['sports'])} leagues across",
        "page budget": f"{generate._PAGE_MD_BUDGET // 1000} KB of",
    }
    missing = {k: v for k, v in facts.items() if v not in text}
    assert not missing, f"the page states a stale number: {missing}"


def _hook(hook_id: str) -> dict:
    doc = yaml.safe_load(PRECOMMIT.read_text(encoding="utf-8"))
    for repo in doc["repos"]:
        for hook in repo.get("hooks") or []:
            if hook.get("id") == hook_id:
                return hook
    raise AssertionError(f"no hook {hook_id}")


@pytest.mark.parametrize("hook_id", ["sdv-codegen", "codegen-drift-prepush"])
@pytest.mark.parametrize("path", ["tools/codegen/generate.py", "tools/codegen/sources.py", "CHANGELOG.md"])
def test_the_drift_hooks_match_generate_py_and_the_changelog(hook_id, path):
    assert re.search(_hook(hook_id)["files"], path), f"{hook_id} does not fire on {path}"


@pytest.mark.parametrize("hook_id", ["sdv-codegen", "codegen-drift-prepush"])
def test_the_drift_hooks_still_match_their_original_inputs(hook_id):
    pattern = _hook(hook_id)["files"]
    for path in (
        "tools/codegen/endpoints/leagues.yaml",
        "tools/codegen/schemas/autodoc/nfl/x.yaml",
        "tools/codegen/templates/league_index.md.jinja",
    ):
        assert re.search(pattern, path)


def test_the_prepush_hook_still_matches_the_docs_tree():
    assert re.search(_hook("codegen-drift-prepush")["files"], "docs/docs/nfl/index.md")
