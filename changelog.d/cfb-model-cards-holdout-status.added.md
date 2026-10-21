- **CFB:** every bundled model card states its holdout status: `qbr_model` names its disjoint holdout
  (2026 regular season, weeks 1-4), `wp_ot_reach` already did, and the other nine say `in_sample: true`
  with the reason (fit on every season they score). `fg_model` / `two_pt_model` / `xpass_model` now
  record the rows actually fitted (42,617 / 1,628 / 1,902,481) instead of the training frame's height.
  Each booster now ships `<model>.importance.json` (gain / cover / split counts read off the booster by
  `tools/cfb_model_importance.py`), and `cfb_cp_model_air_yards` gains the card it shipped without.
  No model bytes changed.
