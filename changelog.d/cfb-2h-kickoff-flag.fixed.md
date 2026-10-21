- **CFB:** `firstHalfKickoffTeamId` is the opening kicker and `pos_team_receives_2H_kickoff` true only on its first-half possessions (inverted on every first-half row of home-kicked games, and set after halftime).
  The WP boosters learned that flag as `1 - is_home` and are still fed that, so published WP stays as it was (each game's first row aside);
  WP keeps the as-trained flag (the `_as_trained_2h_flag` shim) until the WP boosters are retrained on the corrected flag. No retrain has shipped (task 3.8 changed no model bytes); whether to retrain is an open owner decision. (#781)
