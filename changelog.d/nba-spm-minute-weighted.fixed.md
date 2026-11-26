- **NBA/WNBA:** `train_spm` weights each player by his minutes (`weight="min"`) at a refit default penalty
  (`SPM_ALPHA` 30, was 100); low-minute players no longer hold the SPM to a quarter of its target's spread.
  `train_spm(..., weight=None, alpha=100.0)` is the old fit. (#1309)
