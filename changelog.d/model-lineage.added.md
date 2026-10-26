- **Model lineage:** a new *Model lineage* docs page records the fitting script of every bundled booster (26 `.ubj`
  files) and fitted constant, which boosters are byte-identical to their release asset, and which cited fitting
  scripts are lost or untracked. (#984)
- **Model lineage:** the NCAA RAPM ridge-lambda and NBA shot-talent shrinkage fitters move into `tools/`
  (`fit_ncaa_rapm_lambda.py`, `fit_nba_shot_shrinkage_k.py`); `TALENT_SHRINKAGE_K["10"]` (WNBA) is documented as a
  seeded placeholder, and the NFL prediction constants say their fitting scripts are lost. (#984)
