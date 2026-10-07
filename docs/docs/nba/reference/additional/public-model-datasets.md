---
title: "NBA — additional Python functions — Public model datasets"
sidebar_label: "Public model datasets"
sidebar_position: 8
description: "NBA — additional Python functions — Public model datasets — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Public model datasets

### load_darko_dpm {#load_darko_dpm}

`load_darko_dpm(path: 'str') -> 'pl.DataFrame'`

Parse a DARKO DPM leaderboard CSV (e.g. `2026-darko-dpm-leaderboard.csv`).

Name-keyed only (no shared player id with the model zoo) -- this is the
family `~sportsdataverse.nba.nba_model_validation.external_validity`
joins with `join="name"`. Handles two real-file quirks: a leading UTF-8
BOM (read with `encoding="utf-8-sig"`, which strips it) and
sign-prefixed integer columns (`"+7"`, not `"7"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a DARKO DPM leaderboard CSV. |

**Returns**

Frame with schema `DARKO_DPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_darko_dpm
oracle = load_darko_dpm(f"{oracle_dir}/2026-darko-dpm-leaderboard.csv")
print(oracle.sort("dpm", descending=True).head())
```

### load_dunks_threes_stats {#load_dunks_threes_stats}

`load_dunks_threes_stats(path: 'str') -> 'pl.DataFrame'`

Parse a Dunks & Threes counting-stats CSV (e.g. `2025_Dunks_&_Threes_Stats.csv`).

Only `ewins` (estimated wins) is kept -- the WAR-layer oracle target
the spec pairs with LEBRON's `WAR` column. WP4's `nba_war` doesn't
exist yet, so this loader is built and tested standalone (see the
plan's "WP4/WP2 dependency notes").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T counting-stats CSV. |

**Returns**

Frame with schema `DT_STATS_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_dunks_threes_stats
oracle = load_dunks_threes_stats(f"{oracle_dir}/2025_Dunks_&_Threes_Stats.csv")
```

### load_epm {#load_epm}

`load_epm(path: 'str') -> 'pl.DataFrame'`

Parse a Dunks & Threes EPM CSV (`{season}_EPM_data.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T EPM CSV. |

**Returns**

Frame with schema `EPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_epm
oracle = load_epm(f"{oracle_dir}/2025_EPM_data.csv")
```

### load_lebron_daily {#load_lebron_daily}

`load_lebron_daily(path: 'str') -> 'pl.DataFrame'`

Parse a LEBRON daily-snapshot CSV (e.g. `lebron_daily_2026-07-02.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON daily-snapshot CSV. |

**Returns**

Frame with schema `LEBRON_DAILY_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import glob
from sportsdataverse.nba.nba_oracle_data import load_lebron_daily
latest = sorted(glob.glob(f"{oracle_dir}/lebron_daily_*.csv"))[-1]
oracle = load_lebron_daily(latest)
```

### load_lebron_season {#load_lebron_season}

`load_lebron_season(path: 'str') -> 'pl.DataFrame'`

Parse a LEBRON season-file CSV (e.g. `lebron-data-2026.csv`).

`seasons` is passed through as a raw string -- per-season files carry a
single year (`"2026"`); the combined all-years file carries a
multi-year window (`"2010-2013"`). Both parse with this one function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON season CSV. |

**Returns**

Frame with schema `LEBRON_SEASON_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_lebron_season
oracle = load_lebron_season(f"{oracle_dir}/lebron-data-2026.csv")
```

### load_rapm_ryan_davis {#load_rapm_ryan_davis}

`load_rapm_ryan_davis(path: 'str') -> 'pl.DataFrame'`

Parse a Ryan Davis published RAPM CSV (single-season or multi-year window).

Serves BOTH real files -- `rapm_ryan_davis.csv` (`season` like
`"2009-10"`) and `rapm_multi_ryan_davis.csv` (`season` like
`"2011-16"`, a multi-year decay window) -- since they share an
identical header. Only the combined (not per-side Off`/Def`)
rating columns are kept, matching the model zoo's combined-rating
convention (`nba_rapm`'s `rapm` column, not separate offense/defense).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a Ryan Davis RAPM CSV. |

**Returns**

Frame with schema `RAPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv")
season = oracle.filter(pl.col("season") == "2022-23")
```

### normalize_player_name {#normalize_player_name}

`normalize_player_name(name: 'str') -> 'str'`

Fold a player display name to a join-safe key.

Lower-cases, strips diacritics (`"Jokić"` -> `"jokic"` -- the real
stats.nba.com feed spells Nikola Jokic's name with the Serbian `ć`,
while the DARKO/D&T CSVs use plain ASCII), drops periods/apostrophes/
hyphens, collapses internal whitespace, and strips a trailing
Jr./Sr./II/III/IV suffix. Two names normalize equal iff they refer to
the same join key under this scheme -- it is NOT guaranteed globally
unique (rare true duplicate full names are a known, accepted residual;
`external_validity`'s `coverage_pct` surfaces the effect rather
than hiding it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | A raw display name, e.g. `"Nikola Jokić"` or `"A.J. Green"`. |

**Returns**

The normalized key, e.g. `"nikola jokic"`, `"aj green"`. Empty string in, empty string out (never raises).

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import normalize_player_name
assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
```
