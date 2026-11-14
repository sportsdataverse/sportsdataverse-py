- **NBA/WNBA:** `load_{nba,wnba}_stats_metric_curves` and `load_{nba,wnba}_stats_rolling_windows` read the published
  shot-distance curves and rolling-window shooting form (NBA 1996-97 on, WNBA 1997 on; `entity_id` / `team_id` Int64),
  and `load_nba_depth_charts` reads the daily ESPN NBA depth-chart snapshots (2026-27 on). (#1188)
- **MBB/WBB:** `ncaa_espn_schools(league=)` and `ncaa_espn_school_seasons(league=)`, exported from `sportsdataverse.mbb`
  and `sportsdataverse.wbb`, read the static NCAA school and school-season tables of the `{mbb,wbb}_crosswalk` releases.
  (#1188)
