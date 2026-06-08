"""Jinja environment + filters for code generation."""

from __future__ import annotations

from pathlib import Path

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


ENV = Environment(
    loader=FileSystemLoader(str(_TEMPLATES)),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
    undefined=StrictUndefined,
)
ENV.filters["type_hint"] = type_hint
ENV.filters["py_repr"] = py_repr
