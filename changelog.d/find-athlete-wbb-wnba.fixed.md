- `find_athlete(..., league="wnba" | "wbb")` raised `TypeError: download() got an unexpected keyword argument
  'return_parsed'`: the hand-written WBB/WNBA rosters took only `raw=`, so the `return_parsed=False` that
  `find_athlete` sends every league fell through to `download()`. `espn_wbb_team_roster` / `espn_wnba_team_roster`
  now accept `return_parsed=False` as the generated wrappers' spelling of `raw=True`.
