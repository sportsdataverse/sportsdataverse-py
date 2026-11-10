- **NBA:** box prior v2 and a REML-penalised season impact rating (BB-D38); existing outputs are unchanged.
  `career_rapm_target` fits an age-adjusted, time-weighted career RAPM over many seasons;
  `box_prior_features` builds league-relative per-100 rates padded toward average at low possessions,
  plus age, height, position and on-court teammate / opponent context; `train_box_prior` /
  `apply_box_prior` fit it walk-forward by season (leave-one-season-out through 2005) and cross-fitted in
  five player folds. `nba_reml_rapm` shrinks RAPM toward that prior, with the offense and defense
  penalties chosen per season by REML, and reports each player's prior weight. On held-out games
  1998-2026 it beats `nba_adj_rapm` by 0.73 bp of MSE (SE 0.17, 22 of 29 seasons), and by 1.03 bp
  (SE 0.49) after a mid-season cut date in 2006-2026. Against a long-window 8-season RAPM, the prior ranks
  players at Spearman 0.67 (SPM 0.54) with a standard deviation of 1.33 (SPM 0.41). Caveats: the same prior at
  `nba_adj_rapm`'s frozen penalty scores 0.91 bp better than the REML pick; the prior does not beat BPM
  against that oracle (0.68); and the season rating ranks below adjusted RAPM there (0.80 vs 0.91).
  `tools/fit_box_prior_v2.py` fits and gates it.
