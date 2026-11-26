- **Models:** the model ledger accepts `status: "baseline_tied"` for a shipped model that ties its baseline by
  construction (the NBA draft / rookie pick curves); the retire rule runs only its margin trigger on it. (#1322)
- **Models:** a ledger row's optional `reserved_seasons` (WNBA `pro_season_sim`: 2022, 2023, 2026) are refused by
  the retire rule and by `coverage.py`. (#1322)
