"""Tests for the centralized deprecation helpers (``sportsdataverse._deprecation``)."""

from __future__ import annotations

import warnings

import pytest

from sportsdataverse._deprecation import (
    build_deprecation_message,
    deprecated,
    warn_deprecated,
)


# ===========================================================================
# Message composition (pure)
# ===========================================================================


def test_message_full_form() -> None:
    msg = build_deprecation_message(
        "load_nfl_ngs_passing",
        replacement="load_nfl_nextgen_stats(stat_type='passing')",
        removed_in="0.1.0",
    )
    assert msg == (
        "load_nfl_ngs_passing is deprecated and will be removed in 0.1.0; "
        "use load_nfl_nextgen_stats(stat_type='passing') instead."
    )


def test_message_with_since_and_extra() -> None:
    msg = build_deprecation_message(
        "sportsdataverse.parsed.cfb",
        replacement="sportsdataverse.cfb",
        since="0.0.54",
        removed_in="0.1.0",
        extra="Pass return_parsed=False for the raw Dict.",
    )
    assert msg == (
        "sportsdataverse.parsed.cfb is deprecated since 0.0.54 and will be removed in 0.1.0; "
        "use sportsdataverse.cfb instead. Pass return_parsed=False for the raw Dict."
    )


def test_message_minimal() -> None:
    # No replacement / version -> still a clean sentence.
    assert build_deprecation_message("old_thing") == "old_thing is deprecated."


# ===========================================================================
# warn_deprecated
# ===========================================================================


def test_warn_deprecated_emits_deprecationwarning() -> None:
    with pytest.warns(DeprecationWarning, match=r"old_fn .*removed in 0\.1\.0.*use new_fn instead"):
        warn_deprecated("old_fn", replacement="new_fn", removed_in="0.1.0")


def test_warn_deprecated_stacklevel_points_at_callers_caller() -> None:
    # Contract: warn_deprecated(stacklevel=2) inside a function F attributes the
    # warning to F's *caller* -- same as an inline warnings.warn(stacklevel=2).
    # So a deprecated API's warning lands on the user's call site, never on
    # _deprecation.py internals.
    def deprecated_api() -> None:
        warn_deprecated("deprecated_api", replacement="new", removed_in="0.1.0")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        deprecated_api()  # the warning should be attributed to THIS line
    assert len(caught) == 1
    assert caught[0].filename == __file__  # not _deprecation.py


# ===========================================================================
# @deprecated decorator
# ===========================================================================


def test_deprecated_decorator_warns_and_forwards() -> None:
    @deprecated(replacement="new_fn", removed_in="0.1.0")
    def old_fn(a: int, b: int) -> int:
        return a + b

    with pytest.warns(DeprecationWarning, match="old_fn"):
        result = old_fn(2, 3)
    assert result == 5  # body still runs + return value forwarded


def test_deprecated_decorator_sets_marker_and_preserves_metadata() -> None:
    @deprecated(replacement="new_fn")
    def old_fn() -> str:
        """Original docstring."""
        return "ok"

    assert old_fn.__deprecated__ is True
    assert old_fn.__name__ == "old_fn"  # functools.wraps preserved identity
    assert old_fn.__doc__ == "Original docstring."


def test_deprecated_decorator_name_override() -> None:
    @deprecated(replacement="new_fn", name="public_alias")
    def _impl() -> None:
        return None

    with pytest.warns(DeprecationWarning, match="public_alias"):
        _impl()
