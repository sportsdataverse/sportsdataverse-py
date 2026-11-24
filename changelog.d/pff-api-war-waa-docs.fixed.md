- **PFF:** the `pff_api_position_report` and `pff_api_team_leaders` returns tables now list and describe PFF's
  `war` / `waa` win-value columns and their `_rank` / `_rank_of` (and, on leaders, `_percentile`) companions,
  which the wrappers already returned. Use `waa` for college; NCAA rows also carry an undocumented `war`. (#1256)
