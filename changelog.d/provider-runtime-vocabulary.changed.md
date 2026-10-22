- **Runtimes:** The Odds API raises `AssetFetchError` on a dead connection (was `ConnectionError`) or a scalar 2xx body;
  the deprecated `on3_*_rankings` shims raise `ValueError` on a malformed `year` / `sport_slug` (was `{}`); LEGACY `pff_*`
  raises `AssetFetchError` on a JSON array (was `{}`); `pff_*` and the 247Sports getters honour `SDV_PY_HTTP_TIMEOUT`. (#921)
