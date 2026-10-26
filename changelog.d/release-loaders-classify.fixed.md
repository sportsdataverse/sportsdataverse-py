- **Release loaders:** a refused or reset connection raises `AssetFetchError` (it leaked `requests.ConnectionError`); a
  failed read is checked with a one-byte ranged GET instead of a full re-download, and a reachable asset whose read
  failed in transit is read again, a second failure classified too (404 `NoDataError`, else `AssetFetchError`). (#898)
- **Release loaders:** a body that arrived whole but does not parse is re-raised at once, without a second fetch, and
  `download()` no longer decodes an `application/octet-stream` body as JSON. (#898)
- **NFL CSV loaders:** `load_nfl_teams`, `load_nfl_ff_playerids` and `load_nfl_ff_rankings` fetch through the HTTP
  gateway: 404 `NoDataError`, 400 `ValueError`, a rate limit retried then `AssetFetchError`, a challenge page refused
  (on polars 2.0 every failure was a bare `OSError`). (#898)
