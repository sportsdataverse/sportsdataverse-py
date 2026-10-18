- **WNBA crosswalks:** `wnba_crosswalks_from_archive(..., yahoo=True)` fills `yahoo_team_*` and
  `yahoo_game_id` for a past season through the live builders' Yahoo code; a failed Yahoo fetch
  leaves the columns null with a warning. (#803)
