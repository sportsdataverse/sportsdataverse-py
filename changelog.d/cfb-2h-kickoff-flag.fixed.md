- **CFB:** `firstHalfKickoffTeamId` is the opening kicker and `pos_team_receives_2H_kickoff` is true only on its
  first-half possessions; it was inverted on every first-half row of home-kicked games, and set after halftime. (#781)
- **CFB:** the WP boosters learned that flag as `1 - is_home` and are still fed that (the `_as_trained_2h_flag` shim),
  so published WP is unchanged (each game's first row aside) until the boosters are retrained; no model bytes changed.
  (#781)
