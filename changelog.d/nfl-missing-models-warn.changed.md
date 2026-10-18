- **NFL models:** when the download-on-demand nfl4th models (`fd_model.ubj`, `wp_model.ubj`) cannot be
  obtained, the fourth-down step warns once per artifact (it returned all-null `go_wp` / `fg_wp` /
  `punt_wp` silently). `enrich_nfl_pbp(strict=True)` or `SDV_PY_MODELS_STRICT=1` raises
  `FileNotFoundError` naming the missing model instead of nulling its columns (xYAC, xpass and the
  fourth-down step; the ESPN `NFLPlayProcess` path honours the environment variable).
