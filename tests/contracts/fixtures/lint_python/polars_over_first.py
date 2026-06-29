import polars as pl

df.with_columns(prev=pl.col("ep").over("game_id").shift(1))  # noqa: F821 -- polars grouped, clean
