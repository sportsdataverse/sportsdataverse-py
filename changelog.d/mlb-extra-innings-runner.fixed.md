- **MLB:** `pbp_base_out_states` starts each 2020+ extra half-inning with the automatic runner on 2nd (`"_2_"`)
  instead of bases empty, so RE24 and win expectancy stop counting those plate appearances as bases-empty; it is
  detected from the half's first plate appearance, and frames without those columns are unchanged. (#1262)
