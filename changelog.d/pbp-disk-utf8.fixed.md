- **`*_pbp_disk` / `CFBPlayProcess.cfb_pbp_disk` / `NFLPlayProcess.nfl_pbp_disk`:** saved ESPN payloads
  are read as UTF-8. On Windows they were decoded with the platform default (cp1252), which garbled
  every non-ASCII player and team name in an offline rebuild.
