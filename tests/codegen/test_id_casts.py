"""Loader-boundary id canonicalization (``_cast_ids_int64`` + the ``id_int64`` manifest).

The same ESPN id shipped as String on one release family and Int64 on another, so a
cross-dataset join matched **nothing** -- silently, with no error and a structurally
valid frame. These tests pin both halves of the fix: the cast is lossless-or-refused,
and every loader that declares ``id_int64`` actually emits the call.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._codegen_runtime import _cast_ids_int64

ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------
# _cast_ids_int64: converts what is safe, refuses what is not
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("label", "values", "dtype", "expected_dtype", "expected"),
    [
        ("string integral", ["103", "2649", None], pl.Utf8, pl.Int64, [103, 2649, None]),
        ("int32 widens", [1, 2], pl.Int32, pl.Int64, [1, 2]),
        ("float-origin id", [123.0, 456.0], pl.Float64, pl.Int64, [123, 456]),
        ("already int64", [7, 8], pl.Int64, pl.Int64, [7, 8]),
        ("all-null text", [None, None], pl.Utf8, pl.Int64, [None, None]),
    ],
)
def test_cast_converts_integral_ids(label, values, dtype, expected_dtype, expected):
    df = pl.DataFrame({"team_id": pl.Series(values, dtype=dtype)})
    out = _cast_ids_int64(df, ["team_id"])
    assert out.schema["team_id"] == expected_dtype, label
    assert out["team_id"].to_list() == expected, label


@pytest.mark.parametrize(
    ("label", "values"),
    [
        # Not an integer id at all -- casting would null it out.
        ("non-numeric", ["ABC", "103"]),
        # Casts cleanly to 7 but SILENTLY changes the id. "no new nulls" is
        # necessary but not sufficient; the round-trip check is what catches this.
        ("zero-padded", ["007", "103"]),
        ("float with a real fraction", None),
    ],
)
def test_cast_refuses_when_it_would_change_the_id(label, values):
    if values is None:  # float that does not round-trip to an integer
        df = pl.DataFrame({"team_id": [1.5, 2.0]})
        before = df.schema["team_id"]
    else:
        df = pl.DataFrame({"team_id": values})
        before = pl.Utf8
    out = _cast_ids_int64(df, ["team_id"])
    assert out.schema["team_id"] == before, f"{label}: should have been left untouched"
    assert out["team_id"].to_list() == df["team_id"].to_list(), label


def test_cast_is_a_noop_for_missing_columns_and_empty_frames():
    assert _cast_ids_int64(pl.DataFrame({"x": [1]}), ["team_id"]).columns == ["x"]
    empty = pl.DataFrame({"team_id": []}, schema={"team_id": pl.Utf8})
    assert _cast_ids_int64(empty, ["team_id"]).height == 0


def test_cast_leaves_other_columns_alone():
    df = pl.DataFrame({"team_id": ["1"], "other_id": ["2"], "name": ["x"]})
    out = _cast_ids_int64(df, ["team_id"])
    assert out.schema["team_id"] == pl.Int64
    assert out.schema["other_id"] == pl.Utf8  # not declared -> untouched
    assert out.schema["name"] == pl.Utf8


# --------------------------------------------------------------------------
# manifest -> generated code
# --------------------------------------------------------------------------
def _declared_id_int64() -> dict[str, list[str]]:
    import yaml

    raw = yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml").read_text(encoding="utf-8"))
    return {ld["fn"]: ld["id_int64"] for ld in raw["loaders"] if ld.get("id_int64")}


def test_every_declared_loader_emits_the_cast():
    """A loader declaring ``id_int64`` must actually call it, or the manifest lies."""
    declared = _declared_id_int64()
    assert declared, "expected at least one loader to declare id_int64"
    for fn, cols in declared.items():
        league = fn.split("_")[1]
        src = (ROOT / "sportsdataverse" / league / f"{league}_loaders.py").read_text(encoding="utf-8")
        body = src.split(f"def {fn}(")[1].split("\ndef ")[0]
        assert "_cast_ids_int64(" in body, f"{fn} declares id_int64={cols} but emits no cast"


def test_declared_schema_agrees_with_the_cast():
    """A column canonicalized to Int64 must be DECLARED Int64.

    The declared schema drives the published returns table; if it still said String
    the docs would contradict what the loader hands back.
    """
    import yaml

    schemas = yaml.safe_load(
        (ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8")
    )
    for fn, cols in _declared_id_int64().items():
        declared = {e["name"]: e["type"] for e in schemas.get(fn, [])}
        for col in cols:
            if col in declared:
                assert declared[col] == "Int64", f"{fn}.{col} cast to Int64 but declared {declared[col]}"
