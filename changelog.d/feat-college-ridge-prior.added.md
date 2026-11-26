- **College basketball:** `mbb_team_strength` / `wbb_team_strength` seed the possession ridge from each team's
  previous season-end rating by default (`prior="last_season"`; `prior=None` keeps the old fit), cutting the
  as-of game-margin error by 0.25 (men's) and 0.34 (women's) points. (#1291)
