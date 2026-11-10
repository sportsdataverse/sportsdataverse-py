- **NBA:** `career_rapm_target` fits an age-adjusted, time-weighted career RAPM, the target of box prior v2
  (`box_prior_features`, `train_box_prior`, `apply_box_prior`): padded league-relative box rates plus age,
  height, position and lineup context, walk-forward and cross-fitted in five player folds (BB-D38). (#1165)
- **NBA:** `nba_reml_rapm` shrinks RAPM toward that prior, offense and defense penalties chosen per season by
  REML; it beats `nba_adj_rapm` on held-out games by 0.73 bp of MSE (22 of 29 seasons). (#1165)
- **NBA:** caveat: the same prior at `nba_adj_rapm`'s frozen penalty beats the REML pick by 0.91 bp
  (27 of 29 seasons); `tools/fit_box_prior_v2.py` fits and gates both. Existing outputs are unchanged. (#1165)
