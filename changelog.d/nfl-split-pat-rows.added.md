- **NFL:** `split_pat_rows(plays)` gives each extra point / two-point try that ESPN folds into its touchdown row
  (about 2014 on) its own nflverse-style `XP_KICK` / `PAT2` row, id = touchdown id + 1 (`pat_of_id`); opt-in,
  `NFLPlayProcess` rows are unchanged. 99.8-100% of PAT results match nflverse on 2016, 2019 and 2024. (#1255)
