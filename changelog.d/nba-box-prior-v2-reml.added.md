- **NBA:** `career_rapm_target` fits an age-adjusted, time-weighted career RAPM, the target of box prior v2
  (`box_prior_features`, `train_box_prior`, `apply_box_prior`): padded league-relative box rates plus age,
  height, position and lineup context, walk-forward and cross-fitted in five player folds (BB-D38). (#1165)
- **NBA:** `nba_reml_rapm` shrinks RAPM toward that prior at the fixed league penalty (3,728) by default;
  on held-out games it beats `nba_adj_rapm` by 1.64 bp of MSE in all 29 seasons 1998-2026. (#1165)
- **NBA:** `penalty="reml"` chooses offense and defense penalties per season by REML instead; it scores
  0.91 bp worse than the fixed default (27 of 29 seasons), see `tools/fit_box_prior_v2.py`. (#1165)
