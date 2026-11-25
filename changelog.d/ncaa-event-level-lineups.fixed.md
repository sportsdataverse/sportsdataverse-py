- **MBB / WBB:** `parse_ncaa_bb_game_pbp` lineups are now the five on the floor at each row: a sub applies at its own
  dead ball, not the next possession, and a placeholder sub no longer nulls a game's lineups (2026: 1.4% of points
  were off their scorer's five, now 0.004%). `event_lineups=False` keeps bigballR's. (#1271)
- **MBB / WBB:** the lineup walk follows NCAA Rule 3-6.2: a player's own sub moves past his own acts in one clock
  reading, each team is balanced to five, and an unlogged sub-in goes on at the last dead ball. (#1271)
