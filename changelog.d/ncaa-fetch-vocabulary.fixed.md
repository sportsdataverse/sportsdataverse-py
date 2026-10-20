- **stats.ncaa.org cache:** a cached bm-verify shell, sub-1 KB stub or Access Denied page is re-fetched; it was served
  forever, so every parser read zero rows for that game until `force=True`. (#TBD)
- **`ncaa_mbb_date_games` / `ncaa_wbb_date_games`** close the browser they open; a lingering one made the next
  browser fetch in the process fail. (#TBD)
- **`load_proxybonanza_pool`** raises `AssetFetchError` on a 200 that is not the package JSON, not `KeyError`. (#TBD)
