- **NFL models:** when the download-on-demand nfl4th models (`fd_model.ubj`, `wp_model.ubj`) cannot be obtained, the
  fourth-down step warns once per artifact; it returned all-null `go_wp` / `fg_wp` / `punt_wp` silently. (#899)
- **NFL models:** `enrich_nfl_pbp(strict=True)` or `SDV_PY_MODELS_STRICT=1` raises `FileNotFoundError` naming a missing
  model instead of nulling xYAC, xpass or fourth-down columns; the ESPN `NFLPlayProcess` path honours the variable.
  (#899)
- **NFL fourth-down getters:** `get_go_wp`, `get_fg_wp`, `get_punt_wp`, `get_2pt_wp`, `get_2pt_probs` and
  `get_4th_down_probs` treat only `FileNotFoundError` as a missing model; a corrupt cached booster (`XGBoostError`) or a
  disagreeing model card (`ValueError`) propagates instead of yielding NaN. (#899)
- **NFL models:** `enrich_nfl_pbp` and `NFLPlayProcess` still catch such an error and skip the step with a
  `RuntimeWarning` unless strict mode is on. (#899)
