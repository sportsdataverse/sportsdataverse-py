---
title: "MLB — additional Python functions — Chadwick Baseball Bureau register"
sidebar_label: "Chadwick Baseball Bureau register"
sidebar_position: 4
description: "MLB — additional Python functions — Chadwick Baseball Bureau register — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Chadwick Baseball Bureau register

### load_mlb_chadwick_register {#load_mlb_chadwick_register}

`load_mlb_chadwick_register(return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the Chadwick Baseball Bureau person register (every baseball person id crosswalk).

One row per person (players, managers, umpires, coaches in any league) with their id in each
provider the register tracks, names, birth/death dates and first/last seasons. The 16
`people` shards are read from the register's GitHub repository on every call. Licence:
ODC-By 1.0; credit the Chadwick Baseball Bureau when you redistribute derived data.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe, else polars (default). |

**Returns**

One row per person (about 528,000; `key_person` is unique). `key_mlbam` is Int64 and joins the Int64 MLBAM ids of `load_mlb_pbp` (`batter_id`, `pitcher_id`) and `load_mlb_statcast`; `key_npb` keeps its zero padding as Utf8. An Int64 column holding a value upstream that is not an integer comes back as Utf8 with a `SchemaDriftWarning`. | col_name | type | description | |:---|:---|:---| | key_person | Utf8 | Register primary person key (8 hex characters) | | key_uuid | Utf8 | Register UUID for the person | | key_mlbam | Int64 | MLB Advanced Media (MLB Stats API / Statcast) person id | | key_retro | Utf8 | Retrosheet person id | | key_bbref | Utf8 | Baseball-Reference major-league player id | | key_bbref_minors | Utf8 | Baseball-Reference minor-league / register id | | key_fangraphs | Int64 | FanGraphs player id | | key_npb | Utf8 | Nippon Professional Baseball player id (zero-padded, 8 digits) | | key_sr_nfl | Utf8 | Sports-Reference pro football id (multi-sport athletes) | | key_sr_nba | Utf8 | Sports-Reference pro basketball id (multi-sport athletes) | | key_sr_nhl | Utf8 | Sports-Reference hockey id (multi-sport athletes) | | key_wikidata | Utf8 | Wikidata entity id | | name_last | Utf8 | Last name | | name_first | Utf8 | First name (as commonly used) | | name_given | Utf8 | Given (full first and middle) name | | name_suffix | Utf8 | Name suffix (Jr., III, ...) | | name_matrilineal | Utf8 | Matrilineal surname | | name_nick | Utf8 | Nickname | | birth_year | Int64 | Birth year | | birth_month | Int64 | Birth month | | birth_day | Int64 | Birth day | | death_year | Int64 | Death year | | death_month | Int64 | Death month | | death_day | Int64 | Death day | | pro_played_first | Int64 | First season played professionally | | pro_played_last | Int64 | Last season played professionally | | mlb_played_first | Int64 | First season played in the major leagues | | mlb_played_last | Int64 | Last season played in the major leagues | | col_played_first | Int64 | First season played in college | | col_played_last | Int64 | Last season played in college | | pro_managed_first | Int64 | First season managed professionally | | pro_managed_last | Int64 | Last season managed professionally | | mlb_managed_first | Int64 | First season managed in the major leagues | | mlb_managed_last | Int64 | Last season managed in the major leagues | | col_managed_first | Int64 | First season managed in college | | col_managed_last | Int64 | Last season managed in college | | pro_umpired_first | Int64 | First season umpired professionally | | pro_umpired_last | Int64 | Last season umpired professionally | | mlb_umpired_first | Int64 | First season umpired in the major leagues | | mlb_umpired_last | Int64 | Last season umpired in the major leagues |

| col_name | type | description |
|---|---|---|
| `key_person` | String | Register primary person key (8 hex characters), unique per row. |
| `key_uuid` | String | Register UUID for the person. |
| `key_mlbam` | Int64 | MLB Advanced Media (MLB Stats API / Statcast) person id, Int64. |
| `key_retro` | String | Retrosheet's id for the person (player, manager, coach or umpire). |
| `key_bbref` | String | Baseball-Reference major-league player id. |
| `key_bbref_minors` | String | Baseball-Reference minor-league / register id. |
| `key_fangraphs` | Int64 | FanGraphs player id, Int64. |
| `key_npb` | String | Nippon Professional Baseball player id, zero-padded text (8 digits). |
| `key_sr_nfl` | String | Sports-Reference pro football id, for multi-sport athletes. |
| `key_sr_nba` | String | Sports-Reference pro basketball id, for multi-sport athletes. |
| `key_sr_nhl` | String | Sports-Reference hockey id, for multi-sport athletes. |
| `key_wikidata` | String | Wikidata entity id of the person. |
| `name_last` | String | Last name as the register records it. |
| `name_first` | String | First name as commonly used. |
| `name_given` | String | Given (full first and middle) name. |
| `name_suffix` | String | Name suffix such as Jr. or III. |
| `name_matrilineal` | String | Matrilineal surname, where recorded. |
| `name_nick` | String | Nickname, where recorded. |
| `birth_year` | Int64 | Calendar year of birth. |
| `birth_month` | Int64 | Month of birth (1-12). |
| `birth_day` | Int64 | Day of the month the person was born. |
| `death_year` | Int64 | Calendar year of death; null for the living. |
| `death_month` | Int64 | Month of death (1-12); null for the living. |
| `death_day` | Int64 | Day of the month of death; null for the living. |
| `pro_played_first` | Int64 | First season played professionally. |
| `pro_played_last` | Int64 | Last season played professionally. |
| `mlb_played_first` | Int64 | First season played in the major leagues. |
| `mlb_played_last` | Int64 | Last season played in the major leagues. |
| `col_played_first` | Int64 | First season played in college. |
| `col_played_last` | Int64 | Last season played in college. |
| `pro_managed_first` | Int64 | First season managed professionally. |
| `pro_managed_last` | Int64 | Last season managed professionally. |
| `mlb_managed_first` | Int64 | First season managed in the major leagues. |
| `mlb_managed_last` | Int64 | Last season managed in the major leagues. |
| `col_managed_first` | Int64 | First season managed in college. |
| `col_managed_last` | Int64 | Last season managed in college. |
| `pro_umpired_first` | Int64 | First season umpired professionally. |
| `pro_umpired_last` | Int64 | Last season umpired professionally. |
| `mlb_umpired_first` | Int64 | First season umpired in the major leagues. |
| `mlb_umpired_last` | Int64 | Last season umpired in the major leagues. |
| `col_umpired_first` | Int64 | First season umpired in college. |
| `col_umpired_last` | Int64 | Last season umpired in college. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | A shard answered 404 (the register moved or renamed its files). |
| `AssetFetchError` | A shard fetch failed (429, 5xx, a dropped connection) or returned something other than CSV. A partial register is never returned. |
| `ValueError` | A shard answered 400 / 422. |

**Example**

```python
from sportsdataverse.mlb import load_mlb_chadwick_register
people = load_mlb_chadwick_register()

# MLBAM to Baseball-Reference / FanGraphs / Retrosheet ids for major leaguers

import polars as pl
mlb_ids = (
    load_mlb_chadwick_register()
    .filter(pl.col("key_mlbam").is_not_null() & pl.col("mlb_played_first").is_not_null())
    .select("key_mlbam", "key_bbref", "key_fangraphs", "key_retro", "name_first", "name_last")
)
```
