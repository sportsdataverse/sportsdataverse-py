- **CFB:** every bundled model card states its holdout status: `qbr_model` names its disjoint holdout
  (2026 weeks 1-4), `wp_ot_reach` already did, and the other nine say `in_sample: true` with the
  reason (fit on every season they score). Each booster now ships `<model>.importance.json`
  (gain / cover / split counts read off the booster by `tools/cfb_model_importance.py`), and
  `cfb_cp_model_air_yards` gains the card it shipped without. No model bytes changed.
