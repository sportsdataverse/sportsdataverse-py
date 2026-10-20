- **stats.ncaa.org fetch layer:** a real 404 raises `NoDataError` after one request; a banned or exhausted proxy pool,
  an uncleared bm-verify challenge or the Terms gate raises `AssetFetchError` naming which. Both were `RuntimeError`;
  catch `AssetFetchError` instead (a missing proxy configuration is still `RuntimeError`). (#TBD)
