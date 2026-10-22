"""``_codegen_runtime._pin_documented`` never raises and never changes a value (task 5.1, R979-4)."""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse import _codegen_runtime as runtime


@pytest.fixture
def table(monkeypatch):
    from sportsdataverse import _returns_schemas

    schema = {"i": pl.Int64, "f": pl.Float64, "n": pl.Int32, "s": pl.String}
    monkeypatch.setitem(_returns_schemas.SCHEMAS, "test/pin", schema)
    return "test/pin"


@pytest.mark.parametrize(
    ("col", "values", "dtype", "pinned"),
    [
        ("i", [1.0, None, 3.0], pl.Float64, True),  # whole numbers the pandas hop widened: back to Int64
        ("i", [1.0, 1.5], pl.Float64, False),  # a fraction would truncate
        ("i", [1.0, float("nan")], pl.Float64, False),  # NaN has no integer
        ("i", [1.0, float("inf")], pl.Float64, False),
        ("i", [1e30], pl.Float64, False),  # outside Int64
        ("i", [2**63], pl.UInt64, False),  # UInt64 overflow
        ("n", [2**40], pl.Int64, False),  # Int32 overflow
        ("f", [2**60 + 1], pl.Int64, False),  # Float64 would round it
        ("f", [5, None], pl.Int64, True),
        ("s", [None, None], pl.Float64, True),  # all null: only a reader's guess
        ("s", [1.0], pl.Float64, False),  # not numeric on both sides: a parser question
    ],
)
def test_pin_documented_casts_only_what_survives(table, col, values, dtype, pinned):
    df = pl.DataFrame({col: pl.Series(values, dtype=dtype)})
    out = runtime._pin_documented(table, df)
    want = runtime._empty_from_schema(table).schema[col]
    assert out.schema[col] == (want if pinned else dtype)
    assert out[col].cast(dtype).equals(df[col]) if pinned else out.equals(df)  # values never change
