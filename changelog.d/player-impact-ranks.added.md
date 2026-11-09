- **NBA/WNBA:** `nba_player_impact_context` (`league_id="00"` / `"10"`) appends `<m>_n`, `<m>_rank` and `<m>_pct` to a
  `player_impact` table for its 13 metrics. The ranks use the on/off half-signal gates of the same side.
  `nba_player_impact_league_averages` adds the league-average rows. (#1137)
