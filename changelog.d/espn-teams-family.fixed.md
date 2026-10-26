- **ESPN teams:** the eight `espn_<lg>_teams` (mbb, wbb, nba, wnba, nfl, nhl, cfb, mlb) drop their process-lifetime
  `functools.lru_cache`, which raised `TypeError` on `headers={...}`, shared one frame object across callers and never
  expired. Each call returns its own frame; `sportsdataverse.cache` is the opt-in TTL cache. (#888)
- **ESPN teams:** a payload without `sports[0].leagues[0].teams` returns a zero-row frame with the documented columns
  instead of raising `TypeError` / `IndexError`, and the docstrings no longer name a `clear_cache()` that never existed.
  (#888)
