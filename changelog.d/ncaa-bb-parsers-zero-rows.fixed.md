- **MBB / WBB:** the bigballR-port parsers (`parse_ncaa_bb_box`, `_team_schedule`, `_team_roster`,
  `_game_pbp`, `_shots`) return a zero-row frame for a non-HTML payload, and `parse_ncaa_bb_box` for another
  stats.ncaa.org page type, instead of raising. (#789)
