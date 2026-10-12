- **CFB play-by-play:** `lag_half` and `game_play_number` follow the final row order (15 captures moved); a play
  filed under an id outside the feed's numbering is placed by its period and clock (401425418's third-quarter
  kickoff was the game's first row). (#837)
- **CFB play-by-play:** a feed whose plays all lack the `end` block or the scores processes instead of raising
  `ColumnNotFoundError` (NFL too, for `start`, `end`, `clock` and the scores); a pickcenter with no spread on any
  provider takes the no-pickcenter path instead of raising `IndexError`. (#837)
- **CFB play-by-play:** a missing pregame total is 55.5 on every odds path (the pickcenter path used 55.0) and warns
  `errors.DefaultOddsWarning`; ESPN's literal play text "null" is no longer a player name. (#837)
