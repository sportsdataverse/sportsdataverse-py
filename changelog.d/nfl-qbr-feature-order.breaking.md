- **NFL:** `sportsdataverse.nfl.model_vars.qbr_vars` is in the QBR trainer's order (`qbr_epa`, `pass_epa`, `rush_epa`,
  `sack_epa`, `pen_epa`, `spread`), and `nfl/models/qbr_model.ubj` is re-saved with those feature names (same trees), so
  a frame in the old column order raises instead of scoring permuted inputs. (#795)
