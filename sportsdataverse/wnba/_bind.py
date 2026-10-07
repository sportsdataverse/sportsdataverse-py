"""Docstrings for the WNBA ``league_id="10"`` bindings of league-agnostic NBA functions."""

from __future__ import annotations

import inspect
import re
from typing import Callable


def wnba_doc(summary: str, core: Callable) -> str:
    """``summary`` followed by ``core``'s ``Returns:`` section.

    A binding is ``functools.partial(core, league_id="10")``, so it returns exactly what
    ``core`` returns; repeating that section keeps the binding's return documented.

    Args:
        summary: The binding's one-line description.
        core: The NBA function the binding wraps.

    Returns:
        str: The docstring for the binding.
    """
    match = re.search(r"(?ms)^Returns:\n.*?(?=^\S|\Z)", inspect.getdoc(core) or "")
    return f"{summary}\n\n{match.group(0).rstrip()}" if match else summary
