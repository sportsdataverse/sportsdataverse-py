- **ESPN game rosters:** the seven Core v2 `espn_<lg>_game_rosters` (mbb, wbb, nba, wnba, nfl, nhl, cfb) keep
  `position_id` (integer), `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf`
  (football and hockey also `position_parent_href`), as hoopR and wehoop do. (#878)
- **ESPN game rosters:** an athlete `$ref` answering 404 drops that athlete with a `UserWarning` instead of failing the
  game; a single team logo is kept as `logo_href`, and an unreadable logo shape warns instead of writing `""`. (#878)
- **ESPN game rosters:** `espn_mlb_game_rosters` returns a zero-row frame with its columns when ESPN lists no
  competitor roster; it returned `None`. (#878)
- **WBB / WNBA release helpers:** `helper_wbb_officials` / `helper_wnba_officials` skip a null official, return a
  zero-row frame with their columns when there are none, and no longer merge two id-less officials with equal fields.
  (#878)
- **WBB release helpers:** `helper_wbb_game_rosters` reads `"false"` / `"F"` as false (R `as.logical`), and the ESPN
  stat pivot's float coercion maps NaN to null. (#878)
