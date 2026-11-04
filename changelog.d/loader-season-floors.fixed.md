- **Release loaders:** 25 loaders no longer raise `SeasonNotFoundError` for seasons published on
  sportsdataverse-data. Each floor is now the first published season: `load_mbb_officials` 2003 and
  `load_mbb_game_rosters` 2002 (were 2025), `load_mbb_schedule_crosswalk` 2008 (2025),
  `load_nba_schedule_crosswalk` / `load_nba_team_crosswalk` 2002 (2026), the six `load_ncaa_baseball_*` stat
  loaders 2016 (2024) and `load_ncaa_baseball_pbp` / `_games` 2016 (2017), `load_cfb_power_index` 2005 (2015),
  `load_cfb_returning_production` 2004 (2005), `load_cfb_teams_crosswalk` / `load_cfb_schedule_crosswalk` 2004
  (2013), the ESPN NBA schedule, box score, player core and season stats loaders 1994 (2002), `load_wbb_schedule`
  2001 (2002), `load_wbb_team_crosswalk` 2002 (2014) and `load_wnba_schedule` 1997 (2002). Seasons missing between
  the floor and the latest season are skipped, as before. A live test checks every floor against its release. (#1091)
