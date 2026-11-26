- **WNBA:** `wnba_season_sim`'s per-team rating draw now carries the preseason prior's fitted miss (`PRIOR_MISS_POSS`,
  1,060); its preseason 80% win bands covered 0.845 against 0.839 on unseen 1998-2009 seasons, against 0.991. (#1328)
- **NBA:** the same candidate failed its gate (preseason bands too narrow), so `nba_season_sim` is unchanged;
  expansion teams keep the ridge posterior in both leagues. (#1328)
