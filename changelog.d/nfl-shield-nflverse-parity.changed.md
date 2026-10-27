- **NFL Shield play-by-play (`shield_pbp`, `nfl_model_pbp`):** the `drive_*` columns come from Shield's own drive
  records, as nflfastR's do (text yard lines such as `"BAL 20"`, GSIS transition codes, a new `drive`); relocated clubs
  take their current abbreviation; playoff weeks continue the season; `qb_dropback` follows nflfastR. (#PR)
