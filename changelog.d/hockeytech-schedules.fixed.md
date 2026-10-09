- **HockeyTech schedules returned other seasons' games.** `<league>_schedule(season=...)` for
  the 19 HockeyTech league families, and `pwhl_schedule`, read `modulekit/scorebar`. That view
  ignores the `season_id` it is sent, returns games oldest-first and stops at its 10,000-row
  limit, so `ahl_schedule(season=2026)` returned 10,000 AHL games from 1995 to 2012 and none
  from 2025-26; OHL and ECHL failed the same way, and PWHL returned every season's games. The
  schedule functions now read `modulekit/schedule&season_id=<id>`, which the feed scopes to one
  season (live 2026-10-08: AHL 2025-26 = 1,152 games, OHL = 682, PWHL = 120, each in that season
  alone), and `parse_schedule(..., season_id=)` drops any row from another season (#727).
  Behaviour changes:
  - With neither `season` nor `season_id`, the schedule is the newest regular season, as in the
    other season-aware functions. It used to be the feed's whole window.
  - `game_status` now reads the schedule view's strings: `Final OT` where scorebar said
    `Final 1st OT`, and the start time (`7:00 pm EST`) for an unplayed game.
  - A season's playoffs and preseason are separate HockeyTech season ids; pass `season_id` for
    them.
  - An empty reply, or a season filter that keeps nothing, is zero rows with the 12 columns.
