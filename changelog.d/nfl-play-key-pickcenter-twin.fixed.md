- **NFL / CFB:** `clean_nfl_pbp` now matches nflfastR's 15 `fix_weird_pass_plays` keys on nflverse pbp, whose `play_id`
  is Float64: the key cast goes through Int64, so it reads `..._1611`, not `..._1611.0`. On the real
  nflverse 1999-2020 files the 15 keyed rows now read `pass` 0, as nflverse's own column does (C-NF-8).
  The NFL processor's summary-pickcenter odds are Python scalars (they were shape-(1,) arrays), so a
  payload's odds round-trip through `odds_override`, and an all-null pickcenter spread takes the
  default path instead of raising IndexError (the CFB C-M19 / C-NF-10(b) twin; play frames unchanged
  on 84 nfl-raw games). `CFBPlayProcess.cfb_pbp_disk()` documents that an offline rebuild needs
  `join_participants=False` and `odds_override` (C-L3). (#852)
