- **NBA / WNBA:** the Cleaning the Glass flags from `add_play_context`, `add_transition`, `flag_garbage_time` and
  `wnba_play_context` are now `is_transition_ctg` and `is_garbage_time_ctg`. The old `is_transition` and
  `is_garbage_time` columns stay as deprecated copies for now; move reads to `*_ctg`. (#1132)
