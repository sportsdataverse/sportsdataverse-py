- **NCAA:** stage 03 parse re-runs the identity join on an already-parsed contest that is older than its
  season's newest roster file and still has a null `shooter_player_id`, so games parsed before stage 04
  captured the rosters heal on the next run instead of keeping null shooter ids (6,837 shots of Nov 1-2 2026). (#1166)
