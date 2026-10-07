---
title: "NFL — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 16
description: "NFL — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — IDs and crosswalks

### build_nfl_players {#build_nfl_players}

`build_nfl_players(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build an SDV-native NFL players frame from ESPN's public athletes endpoint.

Walks ESPN's public NFL athletes index
(`sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes`),
resolves each athlete's detail resource, flattens it onto the SDV-native
players schema, **dedups to the highest numeric `espn_id` per
`(full_name, birth_date)`** (the ESPN ~2007 4-digit -> 7-digit id
migration left some players with two ids), and enriches `gsis_id` +
other cross-IDs by a best-effort join against
`sportsdataverse.nfl.load_nfl_players`.

This is the **public ESPN-athletes tier only** — a partial mirror of
nflverse's full seven-source `players.parquet` (three of those sources,
PFR / OTC / PFF, require private credentials). ESPN-native rows with no
nflverse match keep only their ESPN fields (cross-IDs left null). For the
full identity master prefer `sportsdataverse.nfl.load_nfl_players`;
use `build_nfl_players` when you need an SDV-native frame that depends
only on the live public ESPN API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-player `DataFrame` with the documented schema (`espn_id`, `full_name`, `first_name`, `last_name`, `position`, `team`, `jersey`, `height`, `weight`, `birth_date`, `status`, `headshot_url`, `gsis_id`, `esb_id`, `pfr_id`, `pff_id`, `smart_id`, `college`). An empty fetch yields a zero-row frame carrying the same column set.

**Example**

```python
from sportsdataverse.nfl import build_nfl_players
players = build_nfl_players()
print(players.shape)

# Pandas output

df = build_nfl_players(return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
build_nfl_players().filter(pl.col("position") == "QB").head()
```

### nfl_players_crosswalk {#nfl_players_crosswalk}

`nfl_players_crosswalk(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Pure-consumer ID crosswalk sliced from `load_nfl_players`.

Reads nflverse's published players master and projects it down to just the
cross-system identifier columns it carries (`gsis_id`, `esb_id`,
`espn_id`, `pfr_id`, `pff_id`, `otc_id`, `nfl_id`, `smart_id` —
whichever the parquet exposes) plus `full_name` and `position`, deduped
on `gsis_id`. The players master has no Yahoo or CBS ids, so `yahoo_id`
and `cbs_id` are joined on `gsis_id` from
`load_nfl_ff_playerids` (DynastyProcess). A `gsis_id` that
DynastyProcess lists twice is ambiguous upstream and gets null provider ids.
It is a convenience for joining identity IDs onto PBP / rosters / stats
frames without carrying the full ~40-column master.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-`gsis_id` `DataFrame` of cross-system IDs (all `Utf8`) + `full_name` / `position`, with `yahoo_id` / `cbs_id` null where DynastyProcess has no unambiguous match (or its load fails). A failed / empty players load yields a zero-row frame carrying the same column set (never a raise).

**Example**

```python
from sportsdataverse.nfl import nfl_players_crosswalk
xwalk = nfl_players_crosswalk()
print(xwalk.columns)

# Join nflverse IDs onto a PBP frame (one line)

pbp.join(nfl_players_crosswalk(), left_on="passer_player_id", right_on="gsis_id", how="left")
```
