- **NFL Shield PBP `play_type`:** a Shield `UNSPECIFIED` play is `no_play` when it carries a penalty or no
  pass / rush / punt / field-goal attempt, as in nflfastR's `translate_play_type_nfl` (it was a null `play_type`).
  On 2025 this labels 121 rows, all `no_play` in nflverse. Re-enriched, `ep` / `wp` / `wpa` / `vegas_wp` move on no
  row; the 121 gain an `xpass`, and one end-of-game no-play takes nflfastR's `epa = -ep`.
