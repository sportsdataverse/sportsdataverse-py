- **NFL:** `load_nfl_model_pbp()` documents nflfastR `clean_pbp`'s player columns `passer` / `passer_id` / `rusher` /
  `rusher_id` / `receiver` / `receiver_id`, which `nfl_model_pbp` carries from its SCHEMA_REV 11 republish
  (sportsdataverse/nfl-data#95). On 2003-2008, whose NFL.com stats name no target on an incompletion, `receiver_id` names
  one on all but ~300-400 throws a season (2003: 7,103 nulls -> 399); on 1999-2005 charted scrambles the QB is the `passer`
  (nflfastR #536), where nflverse's older 1999-2004 release still has him as the `rusher`.
