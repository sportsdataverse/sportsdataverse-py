- **NFL:** `exp_qbr` feeds the bundled QBR booster in its trainer's order; it scored sack EPA as pass EPA,
  pass as rush and rush as sack. `nfl/models/qbr_model.ubj` is re-saved with those feature names (same trees),
  so a permuted input raises.
