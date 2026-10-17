- `nba_crosswalks_from_archive()` / `wnba_crosswalks_from_archive()` take `alt_root=`: the
  `*-alt-data` checkout whose parsed Fox / Yahoo history fills the player crosswalk's
  `fox_athlete_id` / `fox_player` and `yahoo_player_id` / `yahoo_player_name` offline, only
  where they are null. The new private `sportsdataverse._crosswalk_alt` parses the stored
  Fox / Yahoo / CBS payloads into per-season `games` and `game_rosters` parquet
  (`python -m sportsdataverse._crosswalk_alt`) and matches them to ESPN: teams by
  (date, side) co-occurrence, games on (US-Eastern date, team pair), players inside each
  game and team, voted across the season.
