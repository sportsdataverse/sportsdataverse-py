- **ESPN:** `parse_athlete_gamelog` named every stat `stat_0..stat_13` and left `event_date`, `home_away` and the
  opponent columns null on every row: Web v3 keeps the stat names and each game's metadata at the payload's top
  level. (#1219)
