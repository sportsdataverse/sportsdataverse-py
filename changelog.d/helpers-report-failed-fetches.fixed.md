- **Helpers:** `find_athlete` raises `AssetFetchError` when a roster fetch fails (403, rate limit, 5xx)
  instead of reporting "no such player", and skips only a 404 roster; `find_athlete` / `find_event` raise
  `ValueError` (not `KeyError`) for an unknown league. The `sdv` CLI prints `sdv: <msg>` and exits 2 on a
  `NoDataError` / `AssetFetchError` instead of a traceback. `sportsdataverse_upload` stops retrying a
  permanent `gh` failure (HTTP 400/401/404/422, a missing release) instead of sleeping ~645 s.
- **Validation:** `validate_game` no longer crashes under polars 2.0 when the score columns are Float64,
  and a failure building the standing-scrimmage frame is reported as `box.standing_scrimmage_built`
  (warning) instead of silently lowering coverage.
- **Caches:** a memory-cache hit returns a copy (NFL loaders and the HTTP response cache), so editing a
  returned frame or payload in place no longer changes what the next call gets; `clear_cache()` (both)
  deletes only the entries the cache wrote, never other files in the cache directory; a corrupt NFL
  cache entry that cannot be unlinked no longer escapes as `PermissionError`.
