"""Jinja environment + filters for code generation."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

_TEMPLATES = Path(__file__).parent / "templates"


def type_hint(t: str) -> str:
    """Map a spec type string to a Python annotation. 'int|str' -> 'Union[int, str]'."""
    parts = [p.strip() for p in t.split("|")]
    if len(parts) == 1:
        return parts[0]
    return f"Union[{', '.join(parts)}]"


def py_repr(value) -> str:
    """Render a default value as a Python literal."""
    return repr(value)


def yaml_scalar(value) -> str:
    """Render a string as a YAML front-matter scalar, quoting it only when plain YAML would not read it back unchanged.

    A label such as ``Turkey: BSL`` has a colon-space, which a bare ``title:`` line parses as a nested mapping
    (Docusaurus then fails the whole build). Strings that are already safe stay plain so existing pages do not change.
    """
    text = str(value)
    try:
        if yaml.safe_load(f"k: {text}") == {"k": text}:
            return text
    except yaml.YAMLError:
        pass
    return json.dumps(text, ensure_ascii=False)


ENV = Environment(
    loader=FileSystemLoader(str(_TEMPLATES)),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
    undefined=StrictUndefined,
)
ENV.filters["type_hint"] = type_hint
ENV.filters["py_repr"] = py_repr
ENV.filters["yaml_scalar"] = yaml_scalar
