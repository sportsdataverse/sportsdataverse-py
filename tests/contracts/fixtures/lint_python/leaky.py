import polars as pl

df.with_columns(prev=pl.col("ep").shift(1))  # LEAK: no .over grouping  # noqa: F821
