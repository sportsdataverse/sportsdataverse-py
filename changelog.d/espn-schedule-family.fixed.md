- **ESPN schedules:** all eight `espn_<lg>_schedule` return a zero-row frame with the documented columns on an empty
  slate; one malformed event no longer discards the day; `Canceled` games get null linescores like `Postponed`; NFL
  `*_current_rank` is a null integer (was the CFB default 99); parsing the same payload twice no longer raises. (#855)
- **ESPN schedules:** `espn_mbb_schedule(dates=2024)` returns the whole 2023-24 season, November through the title
  game, sorted by date (one request per game day); it returned Jan 1-13 2024 only. A failed day, or one that fills its
  `limit`, raises `AssetFetchError`. (#855)
- **ESPN calendars:** `espn_mbb_calendar` / `espn_wbb_calendar` accept `season=None` (current season) and a float
  `2024.0`. (#855)
