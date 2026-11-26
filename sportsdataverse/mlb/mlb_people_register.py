"""Chadwick Baseball Bureau person register: the cross-provider baseball person id table."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64, _fetch_csv
from sportsdataverse.errors import SchemaDriftWarning

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = ["load_mlb_chadwick_register"]

CHADWICK_REGISTER_PEOPLE_URL = (
    "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people-{shard}.csv"
)
# The register splits ``people`` into 16 shards by the first hex digit of ``key_person``.
_SHARDS = "0123456789abcdef"

# Every column is read as Utf8 (infer_schema_length=0), then these are cast to Int64 through
# _cast_ids_int64, which refuses (keeps as text) any column a value would not survive. So
# ``key_mlbam`` joins straight onto the Int64 MLBAM ids of load_mlb_pbp (batter_id /
# pitcher_id), load_mlb_statcast and the model loaders. ``key_npb`` stays text on purpose: NPB
# ids are zero-padded ("01405134") and an integer would drop the padding.
_INT_COLUMNS = (
    "key_mlbam",
    "key_fangraphs",
    "birth_year",
    "birth_month",
    "birth_day",
    "death_year",
    "death_month",
    "death_day",
    "pro_played_first",
    "pro_played_last",
    "mlb_played_first",
    "mlb_played_last",
    "col_played_first",
    "col_played_last",
    "pro_managed_first",
    "pro_managed_last",
    "mlb_managed_first",
    "mlb_managed_last",
    "col_managed_first",
    "col_managed_last",
    "pro_umpired_first",
    "pro_umpired_last",
    "mlb_umpired_first",
    "mlb_umpired_last",
)


def load_mlb_chadwick_register(return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Load the Chadwick Baseball Bureau person register (every baseball person id crosswalk).

    One row per person (players, managers, umpires, coaches in any league) with their id in each
    provider the register tracks, names, birth/death dates and first/last seasons. The 16
    ``people`` shards are read from the register's GitHub repository on every call. Licence:
    ODC-By 1.0; credit the Chadwick Baseball Bureau when you redistribute derived data.

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe, else polars (default).

    Returns:
        pl.DataFrame: One row per person (about 528,000; ``key_person`` is unique). ``key_mlbam``
        is Int64 and joins the Int64 MLBAM ids of ``load_mlb_pbp`` (``batter_id``, ``pitcher_id``)
        and ``load_mlb_statcast``; ``key_npb`` keeps its zero padding as Utf8. An Int64 column
        holding a value upstream that is not an integer comes back as Utf8 with a ``SchemaDriftWarning``.

        | col_name | type | description |
        |:---|:---|:---|
        | key_person | Utf8 | Register primary person key (8 hex characters) |
        | key_uuid | Utf8 | Register UUID for the person |
        | key_mlbam | Int64 | MLB Advanced Media (MLB Stats API / Statcast) person id |
        | key_retro | Utf8 | Retrosheet person id |
        | key_bbref | Utf8 | Baseball-Reference major-league player id |
        | key_bbref_minors | Utf8 | Baseball-Reference minor-league / register id |
        | key_fangraphs | Int64 | FanGraphs player id |
        | key_npb | Utf8 | Nippon Professional Baseball player id (zero-padded, 8 digits) |
        | key_sr_nfl | Utf8 | Sports-Reference pro football id (multi-sport athletes) |
        | key_sr_nba | Utf8 | Sports-Reference pro basketball id (multi-sport athletes) |
        | key_sr_nhl | Utf8 | Sports-Reference hockey id (multi-sport athletes) |
        | key_wikidata | Utf8 | Wikidata entity id |
        | name_last | Utf8 | Last name |
        | name_first | Utf8 | First name (as commonly used) |
        | name_given | Utf8 | Given (full first and middle) name |
        | name_suffix | Utf8 | Name suffix (Jr., III, ...) |
        | name_matrilineal | Utf8 | Matrilineal surname |
        | name_nick | Utf8 | Nickname |
        | birth_year | Int64 | Birth year |
        | birth_month | Int64 | Birth month |
        | birth_day | Int64 | Birth day |
        | death_year | Int64 | Death year |
        | death_month | Int64 | Death month |
        | death_day | Int64 | Death day |
        | pro_played_first | Int64 | First season played professionally |
        | pro_played_last | Int64 | Last season played professionally |
        | mlb_played_first | Int64 | First season played in the major leagues |
        | mlb_played_last | Int64 | Last season played in the major leagues |
        | col_played_first | Int64 | First season played in college |
        | col_played_last | Int64 | Last season played in college |
        | pro_managed_first | Int64 | First season managed professionally |
        | pro_managed_last | Int64 | Last season managed professionally |
        | mlb_managed_first | Int64 | First season managed in the major leagues |
        | mlb_managed_last | Int64 | Last season managed in the major leagues |
        | col_managed_first | Int64 | First season managed in college |
        | col_managed_last | Int64 | Last season managed in college |
        | pro_umpired_first | Int64 | First season umpired professionally |
        | pro_umpired_last | Int64 | Last season umpired professionally |
        | mlb_umpired_first | Int64 | First season umpired in the major leagues |
        | mlb_umpired_last | Int64 | Last season umpired in the major leagues |

    Raises:
        NoDataError: a shard answered 404 (the register moved or renamed its files).
        AssetFetchError: a shard fetch failed (429, 5xx, a dropped connection) or returned
            something other than CSV. A partial register is never returned.
        ValueError: a shard answered 400 / 422.

    Example:
        Quick start::

            from sportsdataverse.mlb import load_mlb_chadwick_register
            people = load_mlb_chadwick_register()

        MLBAM to Baseball-Reference / FanGraphs / Retrosheet ids for major leaguers::

            import polars as pl
            mlb_ids = (
                load_mlb_chadwick_register()
                .filter(pl.col("key_mlbam").is_not_null() & pl.col("mlb_played_first").is_not_null())
                .select("key_mlbam", "key_bbref", "key_fangraphs", "key_retro", "name_first", "name_last")
            )

    See Also:
        * `Chadwick register`_ -- the upstream register and its licence
        * `baseballr`_ -- R sibling package (``chadwick_player_lu()`` reads the same shards)

        .. _Chadwick register: https://github.com/chadwickbureau/register
        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    frames = [_fetch_csv(CHADWICK_REGISTER_PEOPLE_URL.format(shard=shard), infer_schema_length=0) for shard in _SHARDS]
    out = _cast_ids_int64(pl.concat(frames, how="vertical"), list(_INT_COLUMNS))
    refused = [c for c in _INT_COLUMNS if c in out.columns and out.schema[c] != pl.Int64]
    if refused:
        warnings.warn(
            f"load_mlb_chadwick_register: columns {refused} hold values that are not integers; kept as text",
            SchemaDriftWarning,
            stacklevel=2,
        )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
