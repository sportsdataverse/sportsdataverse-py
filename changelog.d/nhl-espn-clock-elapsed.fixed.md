- **NHL:** `espn_nhl_pbp` reads ESPN's NHL clock as elapsed time, so `start/end.*_seconds_remaining`
  count down (playoff overtime 20 min, regular-season 5 min, shootout 0) and no row ends after it
  starts; it added elapsed seconds as if remaining and rose through every period. (#PR)
