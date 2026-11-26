- **NBA:** `add_shot_make_prob` gives each field goal's make chance from the play-by-play alone (location, value,
  type), from a booster (`load_shot_make_model`) gated walk-forward in `tools/models/nba_shot_value/`: log loss
  3.0% to 3.9% under the zone table on every season since 2018-19. (#1311)
- **NBA:** `score_shot_xpoints(model="location")` scores shot-chart shots with it (opt-in; `"zone"` stays the
  default while three seasons' calibration misses await a ruling), and takes `league_id=`. (#1311)
