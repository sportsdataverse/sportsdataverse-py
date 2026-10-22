- **Model lineage:** a new docs page, *Model lineage*, records the fitting script of every bundled booster
  (26 `.ubj` files) and fitted constant, which bundled boosters are byte-identical to their release asset,
  and the cited fitting scripts that are lost or untracked. The NCAA RAPM ridge-lambda and NBA shot-talent
  shrinkage fitters move into `tools/` (`fit_ncaa_rapm_lambda.py`, `fit_nba_shot_shrinkage_k.py`);
  `TALENT_SHRINKAGE_K["10"]` (WNBA) is now documented as a seeded placeholder, not a fit, and the NFL
  prediction constants say their fitting scripts are lost.
