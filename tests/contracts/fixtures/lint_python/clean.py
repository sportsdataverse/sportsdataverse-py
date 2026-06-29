import polars as pl

df.with_columns(prev=pl.col("ep").shift(1).over("game_id"))  # grouped — clean  # noqa: F821
