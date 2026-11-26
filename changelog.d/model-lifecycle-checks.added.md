- **Models:** `tools/models/model_ledger.json` (schema 1.1) gives every row `baseline`, `primary_metric`,
  `adoption_margin`, `status` and `lifecycle_note`, filled from committed evidence, and adds the NBA / WNBA season sim.
  New `tools/models/lifecycle/` checks: retire rule, season-sim win-total coverage, coefficient sign stability. (#1306)
