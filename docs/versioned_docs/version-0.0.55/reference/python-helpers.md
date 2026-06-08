---
title: Package — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# Package — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse`
not covered by the generated API-endpoint reference above.

## Other

### `cache_stats() -> 'Dict[str, Any]'`

Return a snapshot of the cache for debugging / inspection.

Returns a dict with ``mode``, ``entries``, and ``disk_bytes`` (only populated when mode=filesystem). Cheap — doesn't read the cached bodies, just counts + sizes.

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
