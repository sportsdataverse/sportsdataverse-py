- **`nfl_players_crosswalk()`** raises `AssetFetchError` when the nflverse players master cannot be
  fetched; it used to return a zero-row frame ("never a raise"), which nulled every id in a join. An
  absent asset still gives zero rows, and a failed DynastyProcess load now warns (yahoo/cbs ids null).
- **`nfl.update_config(cache_mode=...)`** raises `ValueError` for a mode other than `"memory"`,
  `"filesystem"` or `"off"` (an unknown mode used to switch caching off silently).
