- **NFL:** `load_nfl_model_pbp()` documents `receiver` / `receiver_id`, nflfastR `clean_pbp`'s target columns, which
  `nfl_model_pbp` carries from its SCHEMA_REV 11 republish (sportsdataverse/nfl-data#95). On 2003-2008, whose NFL.com stats
  name no target on an incompletion, `receiver_id` names one on all but ~300-400 throws a season (2003: 7,103 nulls -> 399).
