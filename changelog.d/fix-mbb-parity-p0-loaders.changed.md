- **MBB / NBA loaders:** ids are Int32 on every ESPN basketball release loader, so they join without a cast:
  `load_{mbb,nba}_game_rosters` `game_id` and `load_{mbb,nba}_rosters` `athlete_id` (String before), the
  `load_{mbb,nba}_*_crosswalk` `espn_*` ids (Int64 before) and the `load_ncaa_mbb_*` `espn_*` ids (String). (#1044)
- **Codegen:** `releases.yaml` gains an `id_int32:` key (cast at the loader boundary, like `id_int64:`);
  `build_r_exports.py` takes package names to re-mine only those. `r_exports.yaml` carries hoopR's 1,316 exports and
  `r_parity_aliases.yaml` maps 176 renamed MBB / NBA functions (KenPom, stats.nba.com, the box/stats loaders). (#1044)
