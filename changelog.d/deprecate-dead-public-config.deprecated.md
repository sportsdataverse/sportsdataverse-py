- **Config:** 29 unread `sportsdataverse.config` release URLs warn `DeprecationWarning` and go in 0.2.0 (use the generated
  `load_<league>_*` loaders): `CFB_{ROSTER,TEAM_SCHEDULE,TEAM_INFO}_URL`, `NFL_PLAYER_STATS_URL`,
  `NFL_PLAYER_KICKING_STATS_URL` and `{NHL,PHF,MBB,NBA,WBB,WNBA}_{BASE,PLAYER_BOX,TEAM_BOX,TEAM_SCHEDULE}_URL`. The
  warning fires on the module spellings, `from sportsdataverse.config import NBA_BASE_URL` and
  `importlib.import_module("sportsdataverse.config").NBA_BASE_URL`. The package attribute (`sdv.config.NBA_BASE_URL`,
  `from sportsdataverse import config`) is `sportsdataverse.nfl.config`, which `sportsdataverse.nfl`'s star import binds
  there (as on main), so it raises `AttributeError` without the warning. (#987)
