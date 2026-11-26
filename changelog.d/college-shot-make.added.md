- **Models:** a college (men's and women's) location shot-make model was gated against `mbb_shot_quality_model`
  and did not ship: a small gain (0.5% to 1.3% log loss), and calibration failures (men's slope 0.80 on 2025,
  1.21 on the 2026 holdout; women's 0.88 on 2024). The fitter and reports are in `tools/models/mbb_shot_value/`. (#1317)
