- **stats.ncaa.org cache:** a cached bm-verify shell, sub-1 KB stub or Access Denied page is re-fetched; it was served
  forever, so every parser read zero rows for that game until `force=True`. (#920)
- **`ncaa_mbb_date_games` / `ncaa_wbb_date_games`** close the browser they open; a lingering one made the next
  browser fetch in the process fail. (#920)
- **`load_proxybonanza_pool`** raises `AssetFetchError` on a 200 that is not the package JSON, not `KeyError`. (#920)
- **stats.ncaa.org / ProxyBonanza:** an HTTP 400 or 422 (without a WAF ban or challenge page) raises `ValueError` after
  one request; it rotated the whole proxy pool and ended in `AssetFetchError`. (#920)
