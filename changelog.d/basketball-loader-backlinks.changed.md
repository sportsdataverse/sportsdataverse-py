- **MBB/WBB/WNBA:** the release loaders whose producer publishes a dataset page (`hoopR-mbb-data`, `wehoop-wbb-data`,
  `wehoop-wnba-data` `docs/datasets/*.md`) link it in their docstring (`Dataset page:`) and on the league's loaders
  reference page, from a new `dataset_page` key in `releases.yaml`. (#1187)
- **NBA/WNBA:** the hand-written `load_nba_stats_*` / `load_wnba_stats_*` loaders in `nba_loaders_extra` /
  `wnba_loaders_extra` now carry the `Source:` release-tag line the generated loaders have. (#1187)
