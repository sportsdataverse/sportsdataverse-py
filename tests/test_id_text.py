"""``_codegen_runtime._id_text``: the one id coercer the football adapters read id-map rows through (B-GB-22)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from sportsdataverse._codegen_runtime import _id_text


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (401671775.0, "401671775"),
        (np.float64(401671775), "401671775"),
        (np.int64(401671775), "401671775"),
        (401671775, "401671775"),
        ("401671775", "401671775"),
        (None, ""),
        (math.nan, ""),
        (pd.NA, ""),
        ("", ""),
    ],
    ids=["float", "np_float", "np_int", "int", "text", "none", "nan", "pd_na", "blank"],
)
def test_id_text(value, text):
    assert _id_text(value) == text
