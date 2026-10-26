- **WBB NCAA:** `ncaa_wbb_game_pbp` / `ncaa_wbb_play_by_play` use the period model of the game's era (halves through
  2014-15, quarters from 2015-16), not quarters always, so a pre-2016 game no longer parses to zero rows. A new
  `season=` pins the era; without it `infer_wbb_period_model` reads it from the first-period clock. (#738)
