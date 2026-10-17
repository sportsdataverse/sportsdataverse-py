"""Docs front matter must be valid YAML: Docusaurus fails the whole site build on one bad page (sdv-py #859 post-merge)."""

from pathlib import Path

import pytest
import yaml

from tools.codegen import render

DOCS = Path(__file__).resolve().parents[2] / "docs" / "docs"


@pytest.mark.parametrize(
    "text",
    [
        "ABA — ABA League sites (Adriatic: ABA League and ABA League 2)",
        "TBF app API (Turkey: BSL, TBL, KBSL)",
        "a #b",
        "- x",
        "'q'",
    ],
)
def test_yaml_scalar_round_trips_hazardous_labels(text):
    assert yaml.safe_load(f"k: {render.yaml_scalar(text)}") == {"k": text}


def test_yaml_scalar_leaves_safe_labels_plain():
    assert render.yaml_scalar("ESPN NBA (site v2)") == "ESPN NBA (site v2)"


def test_every_docs_page_front_matter_parses():
    bad = []
    for page in sorted(DOCS.rglob("*.md")):
        text = page.read_text(encoding="utf-8")
        if not text.startswith("---\n"):
            continue
        front = text.split("\n---\n", 1)[0][4:]
        try:
            yaml.safe_load(front)
        except yaml.YAMLError as exc:
            bad.append(f"{page.relative_to(DOCS)}: {str(exc).splitlines()[0]}")
    assert not bad, bad


def test_every_category_file_is_valid_json():
    import json

    bad = []
    for cat in sorted(DOCS.rglob("_category_.json")):
        try:
            json.loads(cat.read_text(encoding="utf-8"))
        except ValueError as exc:
            bad.append(f"{cat.relative_to(DOCS)}: {exc}")
    assert not bad, bad
