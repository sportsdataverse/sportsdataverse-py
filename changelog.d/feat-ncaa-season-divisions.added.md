- **stats.ncaa.org:** `ncaa_season_division_id(sport, season, division)` resolves the scoreboard id for all
  20 stats.ncaa.org sports from a bundled table (live once for a new season); `ncaa_season_divisions()` lists
  it, and `ncaa_scoreboard_contests(sport, dates)` lists a date's contests from one page. (#1066)
- **NCAA MBB/WBB:** capture stores the `team_stats` and `officials` tabs, re-fetching officials posted late, and
  parse emits `linescore` (every bundle), `team_stats` and `officials`; `discover --scoreboard` is the
  nightly discovery mode. (#1066)
