- **Basketball:** plain RAPM (`nba_rapm`, the `player_impact` RAPM and the factor split) fits at one ridge penalty per
  league in every season (`RAPM_PENALTY`: NBA 2,683, WNBA 1,931); RidgeCV's per-season pick made the RAPM scale jump
  between seasons, so values fit before are not comparable. `nba_rapm(alphas=)` becomes `alpha=` / `league_id=`. (#1121)
