"""The last-resort stringify in each parser's ``_to_output`` keeps a missing cell null, never ``"nan"``.

An object column that mixes an int, a str and a missing value stays ``object`` on pandas 2.3 and pandas 3,
so ``pl.from_pandas`` rejects it and the fallback runs on both. ``astype(str)`` there turned the NaN into
``"nan"``, which made the id cast refuse the column (String + ``SchemaDriftWarning``); on the 3.10 floor it
changed 15 columns of the committed ``right_rail_2024_scf_g7.json`` leaders frame.

pandas 3's default string dtype keeps a missing value missing under ``astype(str)``, but its opt-out
``future.infer_string = False`` (pandas 2.3's behaviour) does not, so the test runs under that option: the bug
then shows on CI's pandas 3 as well as on the floor.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from sportsdataverse import _common_espn_parsers
from sportsdataverse.nhl import nhl_api_web_parsers, nhl_edge_parsers, nhl_records_parsers, nhl_stats_rest_parsers

MODULES = [_common_espn_parsers, nhl_api_web_parsers, nhl_edge_parsers, nhl_records_parsers, nhl_stats_rest_parsers]


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.__name__.rsplit(".", 1)[-1])
def test_the_stringify_fallback_keeps_a_missing_cell_null(module):
    with pd.option_context("future.infer_string", False):
        df = pd.DataFrame({"team_id": [12, "3", float("nan")], "label": ["a", 1, None]})
        assert df["team_id"].dtype == object  # the input reaches the fallback on this pandas
        out = module._to_output(df, False)
    assert out.schema["team_id"] == pl.Int64
    assert out["team_id"].to_list() == [12, 3, None]
    assert out["label"].to_list() == ["a", "1", None]
