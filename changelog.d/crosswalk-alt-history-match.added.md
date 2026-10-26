- **NBA / WNBA crosswalks:** `nba_crosswalks_from_archive()` / `wnba_crosswalks_from_archive()` take `alt_root=`, an
  `*-alt-data` checkout whose parsed Fox / Yahoo history fills the player crosswalk's null `fox_athlete_id` /
  `fox_player` / `yahoo_player_id` / `yahoo_player_name` offline. (#817)
- **Crosswalks:** the private `sportsdataverse._crosswalk_alt` parses stored Fox / Yahoo / CBS payloads into per-season
  `games` / `game_rosters` parquet and matches them to ESPN: teams by (date, side), games by (US-Eastern date, team
  pair), players within each game and team, voted across the season. (#817)
