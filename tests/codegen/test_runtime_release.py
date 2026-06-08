"""404-safe release-parquet helper + season-list normalization (generated loaders)."""

from unittest.mock import patch

import polars as pl

from sportsdataverse import _codegen_runtime as rt


def test_as_season_list_normalizes():
    assert rt._as_season_list(2024) == [2024]
    assert rt._as_season_list(range(2022, 2024)) == [2022, 2023]
    assert rt._as_season_list([2021, 2022]) == [2021, 2022]
    assert rt._as_season_list("2024") == [2024]


def test_read_release_parquet_returns_df_on_success():
    df = pl.DataFrame({"a": [1]})
    with patch("sportsdataverse._codegen_runtime.pl.read_parquet", return_value=df):
        out = rt._read_release_parquet("https://x/ok.parquet")
    assert out is not None and out.shape == (1, 1)


def test_read_release_parquet_returns_none_on_404():
    def boom(*a, **k):
        raise FileNotFoundError("404 Not Found")

    with patch("sportsdataverse._codegen_runtime.pl.read_parquet", side_effect=boom):
        assert rt._read_release_parquet("https://x/missing.parquet") is None


def test_read_release_parquet_reraises_unexpected():
    def boom(*a, **k):
        raise ValueError("schema mismatch")

    with patch("sportsdataverse._codegen_runtime.pl.read_parquet", side_effect=boom):
        try:
            rt._read_release_parquet("https://x/bad.parquet")
            raise AssertionError("expected ValueError")
        except ValueError:
            pass
