- **NBA:** `add_shot_make_prob` gives each field goal's make chance from the play-by-play alone (location, value,
  type), from a booster (`load_shot_make_model`) gated walk-forward in `tools/models/nba_shot_value/`: log loss
  3.0% to 4.0% under the zone table on every season since 2018-19, nearly all at the rim. (#1311)
