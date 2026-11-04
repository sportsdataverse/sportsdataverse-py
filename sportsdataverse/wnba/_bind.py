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


def wnba_signature(core: Callable) -> inspect.Signature:
    """``core``'s signature with ``league_id`` defaulting to ``"10"``.

    ``functools.update_wrapper`` sets ``__wrapped__``, so ``inspect.signature`` (and the generated
    reference docs) would otherwise read ``core``'s NBA default ``"00"`` through the binding.
    Assign the result to the binding's ``__signature__``.

    Args:
        core: The NBA function the binding wraps.

    Returns:
        inspect.Signature: ``core``'s signature with the WNBA ``league_id`` default.
    """
    sig = inspect.signature(core)
    return sig.replace(
        parameters=[p.replace(default="10") if p.name == "league_id" else p for p in sig.parameters.values()]
    )
