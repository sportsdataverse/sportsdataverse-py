- **Release (all sports):** `write_rds` stores an integer column outside int32 as character, not a double that
  loses precision (87% of 2026 WBB play ids collided). From the next rebuild, hoopR / wehoop / cfbfastR read
  such ids (NBA / MBB / WBB / WNBA play `id`, 18-digit football ids, epoch-ms values) as character. (#1047)
- **WBB/WNBA:** `r_exports.yaml` carries wehoop's 750 exports (the 289 international ones were missing) and
  `r_parity_aliases.yaml` maps 123 renamed WBB / WNBA functions (stats.wnba.com, box/stats loaders, `hhs_*`). (#1047)
