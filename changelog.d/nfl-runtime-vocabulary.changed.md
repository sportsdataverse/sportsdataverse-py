- **NFL:** `nfl_pro_*`, the `api.nfl.com` token mint, `nfl_game_schedule` / `nfl_game_details` / `nfl_game_pbp` and
  `nfl_ngs_*` raise `NoDataError` (404), `ValueError` (400/422) or `AssetFetchError` (429/5xx after retries, a dead
  connection, an empty/HTML/scalar 2xx) instead of `HTTPError` / `KeyError` / `JSONDecodeError` / `ValueError`. (#918)
- **NFL Pro:** `NFLProAuthError` is an `AssetFetchError` (still a `RuntimeError`); pro.nfl.com's empty-200 rejection,
  a GraphQL `errors` envelope or a body without `data.viewer.gameDetail` raises instead of giving an empty pbp. (#918)
- **NFL:** NGS retries 408/429/5xx but not its 403 deny, the schedule / details GETs retry, the getters honour
  `SDV_PY_HTTP_TIMEOUT`, a string pro.nfl.com `limit` pages correctly, and an undecodable token is logged. (#918)
