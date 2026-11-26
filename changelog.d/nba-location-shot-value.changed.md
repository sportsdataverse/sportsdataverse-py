- **NBA:** `score_shot_xpoints` (so `nba_shot_value`, its talent, selection and zone maps) scores NBA twos with that
  location model by default, threes with the zone value; `model="zone"` keeps the old table. The talent shrinkage
  `k` is refit for it (58.1, was 70.1; the G League keeps 70.1). (#1311)
- **WNBA:** `wnba_shot_value`'s `score_shot_xpoints` and `shooter_talent` are bound to `league_id="10"`: the
  zone table and the WNBA's `k`, never the NBA booster or the NBA's `k`. (#1311)
