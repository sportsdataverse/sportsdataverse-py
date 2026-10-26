- **Response cache:** a request that authenticates outside its URL (an `Authorization`, `Cookie`, `X-Api-Key` or other
  credential header, or a `session=` holding cookies) is never cached, so a second credential in the same process or
  cache dir is not served the first one's body (PFF Premium, `api.nfl.com`, NFL Pro, KenPom, Her Hoop Stats). (#897)
- **Response cache:** `cache_ttl=0` given as an `int` no longer raises `AttributeError` when a cache mode is on, and a
  body that cannot be cached, or a binary one (`application/octet-stream`, images, parquet), is not JSON-decoded. (#897)
- **Redaction:** error messages, retry log lines and cached URLs also hide a URL's `user:password@`, `appkey=`,
  `access_key=`, `subscription-key=`, `sig=`, `signature=`, `auth=`, `pass=` and `Authorization` values. (#897)
- **Redaction:** a `Bearer` / `Basic` credential and a whole `Cookie:` header line are hidden, and a failed request's
  `X-Api-Key` / `x-api-secret` / `X-Authorization` headers are dropped with `Authorization` and `Cookie`. (#897)
- **Subscription logins:** the KenPom / Her Hoop Stats session cache tells passwords apart (a changed password logs in
  again), threads that miss it together log in once, and the 247Sports token and On3 buildId are minted under a lock.
  (#897)
- **Cached bodies:** only the request's own credential values are scrubbed from a stored body (now including
  `api_token=` / `user_key=`), so a signed media URL in the body (`X-Amz-Signature=`) is served as the live call gave
  it. (#897)
