- **NBA/WNBA:** `nba_shot_value_lineups` reads the endpoint's zone table, `ShotChartLineupLeagueAverage`; it looked
  for `LeagueAverages`, found nothing, and left every zone-scored lineup shot's `base_fg_pct` and `xpoints` null.
  (#1312)
