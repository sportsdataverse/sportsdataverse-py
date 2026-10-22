- **MBB / WBB:** `sportsdataverse.scrape.ncaa.league_config` (`NcaaLeagueConfig`, `MBB`, `WBB`, `by_league`)
  warns `DeprecationWarning` on access and goes in 0.2.0. Nothing read it, and `WBB.periods = 4` is wrong
  before 2015-16; pass the league slug and use `scrape.ncaa.parse.wbb_period_model(season)`. (#853)
