---
title: "NFL — additional Python functions — Cache and configuration"
sidebar_label: "Cache and configuration"
sidebar_position: 14
description: "NFL — additional Python functions — Cache and configuration — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Cache and configuration

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
