- **NBA/WNBA:** `nba_player_props(as_of_date=)` uses only box lines and ratings games dated strictly
  before the cut; the default is the target game's own date, so a played game is no longer
  projected from its own or later lines. Logs without `game_date` raise `ValueError`. (#850)
- **NBA/WNBA:** `nba_availability` imputes missing GP% with the train-season median stored in the
  bundled artifact, not a median over the seasons being scored. (#850)
