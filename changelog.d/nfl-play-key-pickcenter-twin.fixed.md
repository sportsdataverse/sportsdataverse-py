- **NFL:** `clean_nfl_pbp` matches nflfastR's 15 `fix_weird_pass_plays` keys on nflverse pbp, whose `play_id` is
  Float64: the key cast goes through Int64 (`..._1611`, not `..._1611.0`), so those rows read `pass` 0, as in nflverse.
  (#852)
- **NFL:** the processor's summary-pickcenter odds are Python scalars (they were shape-(1,) arrays), so they round-trip
  through `odds_override`, and an all-null pickcenter spread takes the default path instead of raising `IndexError`.
  (#852)
- **CFB:** `CFBPlayProcess.cfb_pbp_disk()` documents that an offline rebuild needs `join_participants=False` and
  `odds_override`. (#852)
