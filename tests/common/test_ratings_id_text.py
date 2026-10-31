"""id_text: float-origin ids read as integers, other ids as they are."""

import polars as pl

from sportsdataverse._common.ratings import id_text


def test_id_text_float_int_and_string_ids():
    df = pl.DataFrame({"f": [401700174.0, None], "i": [401700174, 7], "s": ["A", "007"]})
    out = df.select(id_text(df, "f"), id_text(df, "i"), id_text(df, "s"))
    assert out.schema == pl.Schema({"f": pl.Utf8, "i": pl.Utf8, "s": pl.Utf8})
    assert out["f"].to_list() == ["401700174", None]
    assert out["i"].to_list() == ["401700174", "7"]
    assert out["s"].to_list() == ["A", "007"]
