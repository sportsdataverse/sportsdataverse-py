"""Unit tests for the cricket format-constants table (T7.3 Task 0.3)."""

from __future__ import annotations

import pytest

from sportsdataverse.cricket.cricket_model_constants import FORMAT_TABLE, FormatConstants, get_format


def test_t20_resolves() -> None:
    fc = get_format("t20")
    assert isinstance(fc, FormatConstants)
    assert fc.balls_total == 120
    assert fc.max_wickets == 10


def test_odi_resolves() -> None:
    fc = get_format("odi")
    assert fc.balls_total == 300
    assert fc.max_wickets == 10


def test_get_format_is_case_insensitive() -> None:
    assert get_format("T20").name == "t20"
    assert get_format(" ODI ").name == "odi"


def test_test_cricket_raises() -> None:
    with pytest.raises(ValueError, match="Test cricket deferred"):
        get_format("test")


def test_unknown_format_raises() -> None:
    with pytest.raises(ValueError, match="Unknown cricket format"):
        get_format("mlb")


def test_table_keys() -> None:
    assert set(FORMAT_TABLE) == {"t20", "odi"}
    for fc in FORMAT_TABLE.values():
        assert fc.par_score > 0.0
        assert fc.sigma_set > 0.0
        assert fc.sigma_chase > 0.0
        assert fc.resource_surface_path.endswith(".parquet")
