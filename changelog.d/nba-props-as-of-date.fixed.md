- **NBA/WNBA:** `nba_player_props(as_of_date=)` uses only box lines and ratings games dated strictly
  before the cut; the default is the target game's date (box logs, else schedule; else a new
  `errors.AsOfCutWarning`), so a played game never sees its own or later lines. (#850)
- **NBA/WNBA:** `nba_availability` imputes missing GP% with the train-season median stored in the
  bundled artifact, not a median over the seasons being scored. (#850)
