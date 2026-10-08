"""R's arrow tags classed vectors as ``arrow.r.vctrs``; polars 2.0 would load them as an
Extension column that no str op, comparison or cast accepts. Importing sportsdataverse
registers the type as storage, so those columns read as on polars 1.x."""

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import sportsdataverse  # noqa: F401  (registers arrow.r.vctrs on import)


def test_r_vctrs_columns_read_as_their_storage_type(tmp_path):
    # The field metadata R's arrow writes for a glue string, as on the published
    # nhl_schedules / pwhl_schedules game_json_url column.
    field = pa.field(
        "game_json_url",
        pa.string(),
        metadata={b"ARROW:extension:name": b"arrow.r.vctrs", b"ARROW:extension:metadata": b"glue"},
    )
    path = tmp_path / "schedule.parquet"
    pq.write_table(pa.table({"game_json_url": ["https://x/1", "https://y/2"]}, schema=pa.schema([field])), path)

    for df in (pl.read_parquet(path), pl.read_parquet(path, use_pyarrow=True), pl.scan_parquet(path).collect()):
        assert df.schema["game_json_url"] == pl.String
        assert df.filter(pl.col("game_json_url").str.contains("x/")).height == 1
