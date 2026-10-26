- **Helpers:** `find_athlete` raises `AssetFetchError` when a roster fetch fails (403, rate limit, 5xx) instead of
  reporting "no such player", skipping only a 404 roster; `find_athlete` / `find_event` raise `ValueError` (not
  `KeyError`) for an unknown league. (#899)
- **CLI:** `sdv` prints `sdv: <msg>` and exits 2 on a `NoDataError` / `AssetFetchError` instead of a traceback. (#899)
- **Release utilities:** `sportsdataverse_upload` stops retrying a permanent `gh` failure (HTTP 400/401/404, a 403
  that is not a rate limit, a 422 other than `already_exists`, a missing release), judged on gh's stderr, instead of
  sleeping ~645 s. (#899)
- **Validation:** `validate_game` no longer crashes under polars 2.0 when the score columns are Float64, and a failure
  building the standing-scrimmage frame is reported as `box.standing_scrimmage_built` (warning) instead of silently
  lowering coverage. (#899)
- **Caches:** a memory-cache hit (NFL loaders and the HTTP response cache) returns a copy, so editing a returned frame
  or payload in place no longer changes what the next call gets. (#899)
- **Caches:** `clear_cache()` (both) deletes only the entries the cache wrote, never other files in the cache
  directory, and a corrupt NFL cache entry that cannot be unlinked no longer escapes as `PermissionError`. (#899)
