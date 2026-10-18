- **Response cache:** a request that authenticates outside its URL (an `Authorization`, `Cookie`,
  `X-Api-Key` or other credential header, or a `session=` holding cookies) is never cached, so a second
  credential in the same process or cache dir is no longer served the first one's body (legacy PFF
  Premium, `api.nfl.com`, NFL Pro, the Data API id map, KenPom / Her Hoop Stats sessions). `cache_ttl=0`
  given as an `int` no longer raises `AttributeError` when a cache mode is on (every basketball runtime
  passes it). A body that cannot be cached, or a binary one (`application/octet-stream`, images,
  parquet, ...), is no longer JSON-decoded on every download.
- **Redaction:** error messages, retry log lines and cached URLs now also hide a URL's `user:password@`,
  `appkey=`, `access_key=`, `subscription-key=`, `sig=`, `signature=`, `auth=`, `pass=` and `Authorization`
  values, a `Bearer` / `Basic` credential and a whole `Cookie:` header line; a failed request's `X-Api-Key` / `x-api-secret` / `X-Authorization`
  headers are dropped with `Authorization` and `Cookie`.
- **Subscription logins:** the KenPom / Her Hoop Stats session cache tells passwords apart (a changed
  password logs in again instead of reusing the old session for 30 minutes), and threads that miss it
  together log in once. The 247Sports guest token and the On3 buildId are minted once under a lock.
- **Cached bodies:** only the request's own credential values are scrubbed from a stored body (now
  including `api_token=` / `user_key=` params), so a signed media URL in the body (`X-Amz-Signature=`,
  `Signature=`) is served from the cache exactly as the live call returned it.
