- **WBB / WNBA:** `find_athlete(..., league="wnba" | "wbb")` no longer raises `TypeError` (`download()` got
  `return_parsed`): `espn_wbb_team_roster` / `espn_wnba_team_roster` now accept `return_parsed=False`, the generated
  wrappers' spelling of `raw=True`. (#1017)
