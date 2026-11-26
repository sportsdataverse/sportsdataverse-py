- **CFB:** `cfb_standings()` leaves a same-conference game out of the conference standings when its new optional
  `conf_game` is `False`, which `cfb_games_from_schedule()` carries from `conference_game`, so the Pac-12's 2026
  Flex Week and Mountain West non-conference meetings stay out of the conference record. (#1315)
