- **CFB:** `firstHalfKickoffTeamId` is the opening kicker and `pos_team_receives_2H_kickoff` true only on its first-half possessions (inverted on every first-half row of home-kicked games, and set after halftime).
  The WP boosters learned that flag as `1 - is_home` and are still fed that, so published WP stays as it was (each game's first row aside);
  WP/WPA move only with the task 3.8 retrain, and the CFB WP reprocess waits for it. (#781)
