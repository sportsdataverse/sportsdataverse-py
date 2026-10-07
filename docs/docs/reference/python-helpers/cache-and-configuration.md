---
title: "Package — additional Python functions — Cache and configuration"
sidebar_label: "Cache and configuration"
sidebar_position: 11
description: "Package — additional Python functions — Cache and configuration — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Cache and configuration

### cache_stats {#cache_stats}

`cache_stats() -> 'Dict[str, Any]'`

Return a snapshot of the cache for debugging / inspection.

Returns a dict with `mode`, `entries`, and `disk_bytes` (only
populated when mode=filesystem). Cheap — doesn't read the cached
bodies, just counts + sizes.

### get_cache_mode {#get_cache_mode}

`get_cache_mode() -> 'str'`

Return the current cache mode.

### set_cache_mode {#set_cache_mode}

`set_cache_mode(mode: 'str') -> 'None'`

Switch the global cache mode.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mode` | `str` |  | One of `"off"`, `"memory"`, `"filesystem"`. |

### set_default_ttl {#set_default_ttl}

`set_default_ttl(ttl: 'Optional[Union[timedelta, int]]') -> 'None'`

Override the default TTL for endpoints not matched by the tier rules.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ttl` | `Optional[Union[timedelta, int]]` |  | A `timedelta`, an integer (interpreted as seconds), or `None` to reset to the built-in `DEFAULT_TTL` (`MODERATE` = 1 hour). |
