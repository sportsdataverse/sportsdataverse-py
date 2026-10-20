- **RealGM:** every `realgm_*` function raises `AssetFetchError` when the page is still Cloudflare's
  "Just a moment..." challenge; it returned a zero-row frame. (#TBD)
- **KenPom / Her Hoop Stats:** a member page answered with the login form (an expired session, or one
  another login replaced) raises `AssetFetchError` and drops the cached session; KenPom parsed it to `{}`. (#TBD)
- **CBS NAPI:** an unknown id (HTTP 200 with a `warnings` entry of code 404) raises `NoDataError`; it
  parsed to a zero-row frame. (#TBD)
- **Statcast search:** a 200 chunk that is not a Statcast CSV (an HTML or error page) raises
  `AssetFetchError` instead of adding a junk row or reading as "no games"; a dropped connection does too. (#TBD)
