---
title: "NFL — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 14
description: "NFL — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Other

### NflConfig {#NflConfig}

`NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None`

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy's `NflreadpyConfig` so users can swap engines
without changing call sites. The defaults are conservative: in-memory
caching with a 24-hour TTL, verbose progress bars on, 30-second
HTTP timeout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_mode` | `CacheMode` | `'memory'` |  |
| `cache_dir` | `Optional[Path]` | `None` |  |
| `cache_duration` | `int` | `86400` |  |
| `verbose` | `bool` | `True` |  |
| `timeout` | `int` | `30` |  |
| `user_agent` | `str` | `'sportsdataverse-py-nfl'` |  |

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()  # NflConfig instance
cfg.cache_mode      # "memory"
cfg.cache_duration  # 86400 (24h)
cfg.timeout         # 30 (seconds)

# Construct a fresh instance directly (rarely needed -- prefer ``update_config``)

from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

### cached_loader {#cached_loader}

`cached_loader(func: 'F') -> 'F'`

Decorator that adds caching to a `load_nfl_*` function.

Honors the active `NflConfig.cache_mode`:

- `memory`: dict-based per-process cache.
- `filesystem`: parquet-based cross-process cache under `cache_dir`.
- `off`: no caching, function runs every time.

The cache key is the hash of `(qualified_name, args, kwargs)` with
`return_as_pandas` excluded so memory / disk hits work regardless of
which return shape the caller asked for. The cache always stores the
polars frame internally and converts to pandas on read when requested.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `func` | `F` |  |  |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.cache import cached_loader

@cached_loader
def load_my_thing(season: int, return_as_pandas: bool = False):
    # ... fetch parquet, build a polars frame ...
    return pl.DataFrame({"season": [season]})

df1 = load_my_thing(2024)            # network hit, populates cache
df2 = load_my_thing(2024)            # served from cache
df_pd = load_my_thing(2024, return_as_pandas=True)
# `return_as_pandas` is excluded from the cache key, so the
# polars hit is reused and converted to pandas on the way out.

# Switch caching modes at runtime

from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

### clear_cache {#clear_cache}

`clear_cache() -> 'None'`

Clear both memory and filesystem caches.

Memory: empties the in-process dict.
Filesystem: removes all entries under `config.cache_dir`. The
directory itself is preserved so subsequent writes succeed without
needing `mkdir`.

The `models/` subdirectory is **deliberately preserved** — it holds
download-on-demand model artifacts (e.g. the ~34 MB `xyac_model.ubj`)
that are expensive to re-fetch. Clearing the *data* cache should not force
a model re-download; delete `<cache_dir>/models/` by hand to drop those.

**Example**

```python
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])

# Pair with a cache-mode switch

from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

### get_config {#get_config}

`get_config() -> 'NflConfig'`

Return the live `NflConfig` singleton.

The same object is returned on every call; mutate via `update_config`
rather than reassigning fields directly so future hooks (e.g. logging
on config change) have a single choke point.

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

# Pair with ``update_config`` to verify a change took effect

from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### reset_config {#reset_config}

`reset_config() -> 'NflConfig'`

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain
of `update_config()` calls without restarting the interpreter.

**Example**

```python
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### update_config {#update_config}

`update_config(**kwargs: 'object') -> 'NflConfig'`

Update the active config in place.

**Returns**

The (mutated) global config object, for chaining or inspection.

**Example**

```python
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)

# Disable caching for development

update_config(cache_mode="off")

# Point cache at a custom directory

update_config(cache_dir="~/sdv-cache")
```

### get_current_nfl_season {#get_current_nfl_season}

`get_current_nfl_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### get_current_nfl_week {#get_current_nfl_week}

`get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### get_current_season {#get_current_season}

`get_current_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### get_current_week {#get_current_week}

`get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### most_recent_nfl_season {#most_recent_nfl_season}

`most_recent_nfl_season(roster: 'bool' = False) -> 'int'`

Alias for `get_current_nfl_season()` mirroring nflreadr's

`most_recent_season()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()

# Roster-year flavor

roster_year = most_recent_nfl_season(roster=True)
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
