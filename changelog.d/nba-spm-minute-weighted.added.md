- **NBA/WNBA:** `train_spm(..., weight="min", alpha=SPM_ALPHA_WEIGHTED)` is an opt-in minute-weighted SPM (prior scale
  `ADJ_RAPM_PRIOR_SCALE_WEIGHTED` 1.5). Unshipped: it fails the NBA `r_rapm_adj_min` floor. The default stays the
  published equal-weight fit (`SPM_ALPHA` 100, prior scale 2), so rebuilds reproduce the release. (#1309)
