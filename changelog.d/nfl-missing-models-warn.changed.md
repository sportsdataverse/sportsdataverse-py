- **NFL models:** when the download-on-demand nfl4th models (`fd_model.ubj`, `wp_model.ubj`) cannot be
  obtained, the fourth-down step warns once per artifact (it returned all-null `go_wp` / `fg_wp` /
  `punt_wp` silently). `enrich_nfl_pbp(strict=True)` or `SDV_PY_MODELS_STRICT=1` raises
  `FileNotFoundError` naming the missing model instead of nulling its columns (xYAC, xpass and the
  fourth-down step; the ESPN `NFLPlayProcess` path honours the environment variable).
- **NFL fourth-down getters** (`get_go_wp`, `get_fg_wp`, `get_punt_wp`, `get_2pt_wp`, `get_2pt_probs`,
  `get_4th_down_probs`) treat only a model that cannot be obtained (`FileNotFoundError`) as missing. A
  corrupt cached booster (`XGBoostError`) or a model card that disagrees with the applier (`ValueError`)
  now propagates instead of silently yielding NaN; `enrich_nfl_pbp` and `NFLPlayProcess` still catch it
  and skip the step with a `RuntimeWarning` unless strict mode is on.
