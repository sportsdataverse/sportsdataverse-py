---
title: "NFL — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 27
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

| col_name | type | description |
|---|---|---|
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `jersey` | character |  |
| `height` | double | Official height, in inches |
| `weight` | double | Official weight, in pounds |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `status` | character |  |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `college` | character | Official college (usually the last one attended) |

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

| col_name | type | description |
|---|---|---|
| `full_name` | character | Full name as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `otc_id` | character | Over the Cap ID for player |
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |

**Example**

```python
from sportsdataverse.nfl import nfl_players_crosswalk
xwalk = nfl_players_crosswalk()
print(xwalk.columns)

# Join nflverse IDs onto a PBP frame (one line)

pbp.join(nfl_players_crosswalk(), left_on="passer_player_id", right_on="gsis_id", how="left")
```
