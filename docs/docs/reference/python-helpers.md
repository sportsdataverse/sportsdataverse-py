---
title: Package — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# Package — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse`
not covered by the generated API-endpoint reference above.

## Dataset loaders

### `load_pwhl_games(return_as_pandas: 'bool' = False)`

Load the PWHL games-in-data-repo manifest (no ``seasons`` argument).

Mirrors fastRhockey (R) ``load_pwhl_games()`` which reads a manifest of every
PWHL game that has processed data in the data repository.

Tries the sportsdataverse-data release asset first; falls back to the raw
fastRhockey-data GitHub path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of all games in the data repository.

**Example**

```python
>>> load_pwhl_games()
```

### `load_pwhl_goalie_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_player_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_player_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_schedule(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_schedules() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_skater_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_pwhl_team_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_pwhl_team_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

## Other

### `cache_stats() -> 'Dict[str, Any]'`

Return a snapshot of the cache for debugging / inspection.

Returns a dict with ``mode``, ``entries``, and ``disk_bytes`` (only
populated when mode=filesystem). Cheap — doesn't read the cached
bodies, just counts + sizes.

### `get_cache_mode() -> 'str'`

Return the current cache mode.

### `set_cache_mode(mode: 'str') -> 'None'`

Switch the global cache mode.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mode` | `str` |  | One of ``"off"``, ``"memory"``, ``"filesystem"``. |

### `set_default_ttl(ttl: 'Optional[Union[timedelta, int]]') -> 'None'`

Override the default TTL for endpoints not matched by the tier rules.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ttl` | `Optional[Union[timedelta, int]]` |  | A ``timedelta``, an integer (interpreted as seconds), or ``None`` to reset to the built-in :data:`DEFAULT_TTL` (``MODERATE`` = 1 hour). |
