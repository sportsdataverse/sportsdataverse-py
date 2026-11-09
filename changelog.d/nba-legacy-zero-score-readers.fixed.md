- **NBA/WNBA legacy pbp:** `flag_garbage_time` (the CTG `is_garbage_time_ctg` flag) and `nba_v3_to_v2_pbp`'s
  `score` / `score_margin` / `team_leading` no longer fall back to 0-0 on the `"0"` score strings that
  pre-2017-18 feeds (WNBA: pre-2017) write on every non-scoring row; they keep the running score. (#1124)
