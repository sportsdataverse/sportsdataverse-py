"""A missing id cell stays null, never the string "nan" (the Python 3.10 floor lock, pandas 2.3).

pandas 2.3 fills a missing JSON key with NaN in an object column. The mixed-kind stringify in
``_parsers._to_frame`` used to turn that NaN into "nan", so the id cast refused the column: it stayed String
and warned instead of casting to Int64 (71 HockeyTech tests on the floor lock).
"""

from __future__ import annotations

import math
import warnings

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _str_or_null
from sportsdataverse.hockeytech._parsers import _to_frame


def test_a_nan_team_id_beside_a_text_one_still_casts_to_int64():
    # int, str and a missing value: the mix the feed ships (0 in one row, "1" in the next), which stays an
    # object column on pandas 3 as well as on 2.3, so the bug shows on CI's Python and on the floor alike.
    records = [
        {"player_team_id": 12, "team_id": 1},
        {"player_team_id": "3", "team_id": "3"},
        {"player_team_id": float("nan"), "team_id": 5},
    ]
    assert pd.DataFrame(records)["player_team_id"].dtype == object  # the input reaches the stringify
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # a refused id column warns SchemaDriftWarning
        out = _to_frame(records, False, parser="test")
    assert out.schema["player_team_id"] == pl.Int64
    assert out["player_team_id"].to_list() == [12, 3, None]
    assert out.schema["team_id"] == pl.Int64
    assert out["team_id"].to_list() == [1, 3, 5]


def test_str_or_null_keeps_missing_values_null():
    assert [_str_or_null(v) for v in (None, math.nan, pd.NA, pd.NaT)] == [None] * 4
    assert [_str_or_null(v) for v in (12, 1.5, "x", True)] == ["12", "1.5", "x", "True"]
