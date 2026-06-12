"""Centralized deprecation policy + helpers.

Deprecation policy
------------------
A public API is **never removed without warning**. When something is
deprecated:

1. It keeps working, unchanged, for a deprecation window of **at least two
   minor releases** (e.g. deprecated in ``0.0.57`` → removable no earlier than
   ``0.1.0``).
2. For that whole window every call emits a :class:`DeprecationWarning` that
   names *both* the replacement API *and* the version it will be removed in, so
   downstream users get an actionable, time-boxed migration path.
3. Removal happens only in the named release, and is called out in the
   changelog.

This module is the single source of truth for that contract. Emit warnings via
:func:`warn_deprecated` (in-body) or the :func:`deprecated` decorator (for a
whole callable) rather than hand-rolling ``warnings.warn(...)`` so the message
format and ``stacklevel`` stay consistent across the package.

Examples:
    In-body (use when the function has custom legacy behavior to preserve)::

        from sportsdataverse._deprecation import warn_deprecated

        def load_nfl_ngs_passing(...):
            warn_deprecated(
                "load_nfl_ngs_passing",
                replacement="load_nfl_nextgen_stats(stat_type='passing')",
                removed_in="0.1.0",
            )
            ...

    Decorator (use for a plain alias that just forwards)::

        from sportsdataverse._deprecation import deprecated

        @deprecated(replacement="new_fn", removed_in="0.1.0")
        def old_fn(*args, **kwargs):
            return new_fn(*args, **kwargs)
"""

from __future__ import annotations

import functools
import warnings
from typing import Any, Callable, Optional, TypeVar

_F = TypeVar("_F", bound=Callable[..., Any])

__all__ = ["build_deprecation_message", "warn_deprecated", "deprecated"]


def build_deprecation_message(
    name: str,
    *,
    replacement: Optional[str] = None,
    since: Optional[str] = None,
    removed_in: Optional[str] = None,
    extra: Optional[str] = None,
) -> str:
    """Compose the canonical one-line deprecation message.

    Args:
        name: The deprecated API's name (e.g. ``"load_nfl_ngs_passing"``).
        replacement: What to use instead (rendered as ``use <replacement>
            instead``). Omit when there is no drop-in successor.
        since: Version the deprecation started (e.g. ``"0.0.57"``).
        removed_in: Version the API will be removed in (e.g. ``"0.1.0"``).
            Per policy this should be set for every deprecation.
        extra: Free-form trailing sentence appended verbatim.

    Returns:
        A single sentence, e.g. ``"load_nfl_ngs_passing is deprecated and will
        be removed in 0.1.0; use load_nfl_nextgen_stats(stat_type='passing')
        instead."``
    """
    msg = f"{name} is deprecated"
    if since:
        msg += f" since {since}"
    if removed_in:
        msg += f" and will be removed in {removed_in}"
    if replacement:
        msg += f"; use {replacement} instead"
    msg += "."
    if extra:
        msg += f" {extra}"
    return msg


def warn_deprecated(
    name: str,
    *,
    replacement: Optional[str] = None,
    since: Optional[str] = None,
    removed_in: Optional[str] = None,
    extra: Optional[str] = None,
    stacklevel: int = 2,
) -> None:
    """Emit a standardized :class:`DeprecationWarning`.

    Drop-in replacement for a hand-written ``warnings.warn(msg,
    DeprecationWarning, stacklevel=2)`` inside a deprecated function body.

    Args:
        name: The deprecated API's name.
        replacement: Successor API (see :func:`build_deprecation_message`).
        since: Version the deprecation started.
        removed_in: Version the API will be removed in.
        extra: Free-form trailing sentence.
        stacklevel: How many frames *above this helper's caller* to attribute
            the warning to. Defaults to ``2`` -- matching a direct
            ``warnings.warn(..., stacklevel=2)`` written in the caller's body
            (the extra hop through this helper is added internally).
    """
    warnings.warn(
        build_deprecation_message(name, replacement=replacement, since=since, removed_in=removed_in, extra=extra),
        DeprecationWarning,
        stacklevel=stacklevel + 1,
    )


def deprecated(
    *,
    replacement: Optional[str] = None,
    since: Optional[str] = None,
    removed_in: Optional[str] = None,
    extra: Optional[str] = None,
    name: Optional[str] = None,
) -> Callable[[_F], _F]:
    """Decorator that fires a :class:`DeprecationWarning` on every call.

    Use for plain forwarding aliases. The wrapped callable runs unchanged after
    the warning. Sets ``wrapper.__deprecated__ = True`` for introspection.

    Args:
        replacement: Successor API.
        since: Version the deprecation started.
        removed_in: Version the API will be removed in.
        extra: Free-form trailing sentence.
        name: Override the reported name (defaults to the function's
            ``__name__``).

    Returns:
        A decorator that wraps the target callable.
    """

    def decorator(func: _F) -> _F:
        reported = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warn_deprecated(
                reported,
                replacement=replacement,
                since=since,
                removed_in=removed_in,
                extra=extra,
                stacklevel=2,
            )
            return func(*args, **kwargs)

        wrapper.__deprecated__ = True  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator
