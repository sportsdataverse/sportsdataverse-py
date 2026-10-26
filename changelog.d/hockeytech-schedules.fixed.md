- **HockeyTech:** `<league>_schedule(season=...)` (the 19 league families and `pwhl_schedule`) returns that season's
  games; it read `modulekit/scorebar`, which ignores `season_id` and stops at 10,000 rows oldest-first, so
  `ahl_schedule(season=2026)` returned AHL games from 1995-2012 and PWHL every season's. (#727)
- **HockeyTech:** the schedules read `modulekit/schedule&season_id=<id>`, which the feed scopes to one season, and
  `parse_schedule(..., season_id=)` drops any row from another season. (#727)
- **HockeyTech:** with neither `season` nor `season_id`, a schedule is the newest regular season, as in the other
  season-aware functions; it used to be the feed's whole window. (#727)
- **HockeyTech:** `game_status` reads the schedule view's strings: `Final OT` where scorebar said `Final 1st OT`, and
  the start time (`7:00 pm EST`) for an unplayed game. (#727)
- **HockeyTech:** a season's playoffs and preseason are separate HockeyTech season ids, so pass `season_id` for them;
  an empty reply, or a season filter that keeps nothing, is zero rows with the 12 columns. (#727)
