"""Drift guard: the validation-harness workflow's hardcoded dataset/lint-target
lists must stay in sync with the Python registry.

The Workflow runtime is JS and can't import the Python registry, so
``.claude/workflows/validation-harness.js`` hardcodes ``DATASETS`` /
``LINT_TARGETS``. When those drift from ``tools/validation/registry.py`` the
workflow silently skips the missing target (this is exactly how the ``cfb_data_r``
lint went unrun). These tests fail CI on any divergence.
"""

from __future__ import annotations

import re
from pathlib import Path

from tools.validation.registry import DATASETS, LINT_TARGETS

_WORKFLOW = Path(__file__).resolve().parents[2] / ".claude" / "workflows" / "validation-harness.js"


def _js_string_array(const_name: str) -> set[str]:
    """Extract the quoted entries of a ``const <name> = [...]`` JS array."""
    text = _WORKFLOW.read_text(encoding="utf-8")
    m = re.search(rf"const\s+{const_name}\s*=\s*\[([^\]]*)\]", text)
    assert m, f"{const_name} array not found in {_WORKFLOW.name}"
    return set(re.findall(r"['\"]([^'\"]+)['\"]", m.group(1)))


def test_workflow_file_exists():
    assert _WORKFLOW.is_file(), f"workflow not found at {_WORKFLOW}"


def test_workflow_datasets_match_registry():
    assert _js_string_array("DATASETS") == set(DATASETS), (
        "validation-harness.js DATASETS drifted from tools/validation/registry.py DATASETS"
    )


def test_workflow_lint_targets_match_registry():
    assert _js_string_array("LINT_TARGETS") == set(LINT_TARGETS), (
        "validation-harness.js LINT_TARGETS drifted from tools/validation/registry.py LINT_TARGETS"
    )
