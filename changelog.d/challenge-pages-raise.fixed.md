- **RealGM:** every `realgm_*` function raises `AssetFetchError` when the page is still Cloudflare's
  "Just a moment..." challenge; it returned a zero-row frame. (#TBD)
- **KenPom / Her Hoop Stats:** a member page answered with the login form or Her Hoop Stats' free-tier
  teaser (expired, replaced or unsubscribed session) raises `AssetFetchError` and drops the cached session. (#TBD)
- **KenPom / Her Hoop Stats:** a failed login request raises the package's error vocabulary
  (`AssetFetchError` / `NoDataError` / `ValueError`) instead of a bare `requests` error. (#TBD)
- **Her Hoop Stats:** `herhoopstats_team_roster` answers a zero-row frame with the roster columns when the
  page has no roster; it returned the page's tallest table. (#TBD)
- **TheSportsDB:** the default free key is `123` (key `3` now answers HTTP 403), and a free-key list that
  reaches the key's row cap warns that it is probably truncated. (#TBD)
- **CBS NAPI:** an unknown id (HTTP 200 with a `warnings` entry of code 404) raises `NoDataError`; it
  parsed to a zero-row frame. (#TBD)
- **Statcast search:** a 200 chunk that is not a Statcast CSV (an HTML or error page) raises
  `AssetFetchError` instead of adding a junk row or reading as "no games"; a dropped connection does too. (#TBD)
