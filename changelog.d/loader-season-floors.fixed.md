- **Release loaders:** 25 loaders no longer raise `SeasonNotFoundError` for published seasons; each floor is the first
  published season. Seasons missing between the floor and the latest season are skipped, as before, and a live test
  checks every floor against its release. Fixes #1091. (#1098)
- **Release loaders:** the new floors are MBB officials 2003, game rosters 2002 and schedule crosswalk 2008; NBA
  schedule / team crosswalks 2002; NCAA baseball stat loaders, pbp and games 2016; CFB power index 2005, returning
  production 2004 and team / schedule crosswalks 2004; ESPN NBA schedule, box and player loaders 1994. (#1098)
- **Release loaders:** `load_wbb_schedule` 2001, `load_wbb_team_crosswalk` 2002 and `load_wnba_schedule` 1997. (#1098)
