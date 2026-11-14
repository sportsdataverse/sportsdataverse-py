- **MBB / WBB:** `load_mbb_player_crosswalk` starts at 2004 and `load_wbb_player_crosswalk` at 2007, the first seasons
  the `mbb_crosswalk` / `wbb_crosswalk` tags publish (both contiguous to 2027). (#1194)
- **MBB / WBB:** the player and schedule crosswalk returns tables gain `ncaa_player_id` / `ncaa_contest_id` (String;
  null before the 2009-10 season), the stats.ncaa.org ids the producers now publish. (#1194)
- **NBA:** `build_nba_player_identity_lookup` orders `player_box` by `game_date` before taking each athlete's last row.
  The published box is newest game first, so a traded player kept their first team (82 of 106 traded players in
  2024); they now keep their latest one. (#1194)
