- **Basketball:** per-competition convenience families over FIBA, Genius LiveStats and Sportradar Atrium,
  minted from a registry (`tools/codegen/basketball_competitions.yaml`): `fiba_wc_*`, `fiba_wwc_*`,
  `fiba_oly_*`, `fiba_woly_*`, `fiba_bcl_*`, `fiba_elw_*`, `fiba_afrobasket_*`, `fiba_weurobasket_*`
  (`editions`, `schedule`, `standings`, `teams`, `player_box`, `team_box`, `pbp`, `shots`); `wbbl_*`,
  `slb_*`, `bal_*`, `nznbl_*`, `esake_*`, `lfb_*`, `lnbm_*`, `lub_*` (`competitions`, `schedule`, box,
  `pbp`, `shots`); `lnb_*`, `lnbp_*`, `nbl1_*`, `lmb_*`, `lpb_*` (`seasons`, `schedule`, `standings`,
  `teams`, box, `pbp`, `shots`). Each binds the competition (`comp` / `org` / `website_id`) and resolves
  an END-year `season` to the edition; `comp=` / `comp_id=` override.
  The reference pages show each minted function's returns table, read from its base wrapper's native
  schema (`__sdv_schema__`), with the base's column descriptions. (#TBD)
