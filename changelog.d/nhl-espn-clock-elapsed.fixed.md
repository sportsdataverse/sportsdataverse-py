- **NHL:** `espn_nhl_pbp` reads ESPN's NHL clock as elapsed time, so `start/end.*_seconds_remaining`
  count down within each period (playoff OT 20 min, regular-season 5, shootout 0; in OT the game clock is
  the OT period clock, as in fastRhockey) and no row ends after it starts. (#857)
