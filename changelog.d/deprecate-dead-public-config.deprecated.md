- **Config:** 29 unread `sportsdataverse.config` release URLs warn `DeprecationWarning` and are removed in 0.4.0; use
  the generated `load_<league>_*` loaders. They are `CFB_{ROSTER,TEAM_SCHEDULE,TEAM_INFO}_URL`, `NFL_PLAYER_STATS_URL`,
  `NFL_PLAYER_KICKING_STATS_URL` and `{NHL,PHF,MBB,NBA,WBB,WNBA}_{BASE,PLAYER_BOX,TEAM_BOX,TEAM_SCHEDULE}_URL`. (#987)
- **Config:** the warning fires on `from sportsdataverse.config import NBA_BASE_URL` and on
  `importlib.import_module("sportsdataverse.config")`. `sdv.config` is `sportsdataverse.nfl.config` (bound by
  `sportsdataverse.nfl`'s star import), so `sdv.config.NBA_BASE_URL` raises `AttributeError` with no warning. (#987)
