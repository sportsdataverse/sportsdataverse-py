- **NFL:** an ESPN game with no venue id (every game before 2006) or an unknown one takes its home
  team's roof that season (`model_vars.home_team_roof`), so 2002-2005 dome games no longer score EP/CP
  as outdoors; a neutral site stays outdoors. `nfl_build.PIPELINE_VERSION` is 3. (#1029)
- **NFL:** the Shield source sets the header's `neutralSite` from the id map's `neutral_site`, so a
  London or Super Bowl game is not given the home team's dome. (#1029)
