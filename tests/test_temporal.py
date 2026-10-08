import datetime as dt

import polars as pl

from sportsdataverse._temporal import as_date


def test_as_date_parses_strings_and_casts_temporals():
    d = dt.date(2024, 9, 5)
    frames = {
        "string": pl.DataFrame({"x": ["2024-09-05", None]}),
        "date": pl.DataFrame({"x": [d, None]}),
        "datetime": pl.DataFrame({"x": [dt.datetime(2024, 9, 5, 20, 15), None]}),
    }
    for name, df in frames.items():
        out = df.select(as_date(pl.col("x")))
        assert out.schema["x"] == pl.Date, name
        assert out["x"].to_list() == [d, None], name
        # lazy (streaming by default in polars 2.0) gives the same answer
        assert df.lazy().select(as_date(pl.col("x"))).collect()["x"].to_list() == [d, None], name
