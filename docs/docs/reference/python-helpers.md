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

### `get_cache_mode() -> 'str'`

Return the current cache mode.

### `set_cache_mode(mode: 'str') -> 'None'`

Switch the global cache mode.

### `set_default_ttl(ttl: 'Optional[Union[timedelta, int]]') -> 'None'`

Override the default TTL for endpoints not matched by the tier rules.
