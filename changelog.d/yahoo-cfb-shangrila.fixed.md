- **CFB / Yahoo:** `yahoo_cfb_teams`, `yahoo_cfb_scoreboard` and `yahoo_cfb_boxscore` read shangrila (Yahoo's
  editorial feed 404s since 2026-11-20): new scoreboard columns, lineup-only box, null `yahoo_global_game_id`.
  `cfb_schedule_crosswalk` raises when every Yahoo week is empty or 404. (#1250)
