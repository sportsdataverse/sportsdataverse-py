- **Kalshi:** the `kalshi_*` wrappers retry a request on `external-api.kalshi.com` when `api.elections.kalshi.com`
  fails with a connection error or a 5xx. A 4xx (bad ticker, bad parameter, rate limit) never fails over. (#1349)
