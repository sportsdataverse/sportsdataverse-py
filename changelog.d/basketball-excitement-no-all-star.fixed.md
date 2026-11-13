- **NBA / WNBA game excitement:** `season_game_excitement(..., schedule=)` leaves out the All-Star games and
  Rising Stars the schedule marks (`type_abbreviation` `ALLSTAR` or a `notes_headline` naming one), so they
  no longer appear or take a `gei_rank`, and `gei_n` counts real games only. (#1172)
