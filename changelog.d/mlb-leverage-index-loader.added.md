- **MLB:** `load_mlb_leverage_index` reads the `mlb_game_state` leverage index, one row per win-expectancy state
  bucket (the `load_mlb_we_table` key) for 2020 on. The 2020-2026 `mlb_game_state` tables were rebuilt so every
  extra half-inning opens with the automatic runner on 2nd. (#1354)
