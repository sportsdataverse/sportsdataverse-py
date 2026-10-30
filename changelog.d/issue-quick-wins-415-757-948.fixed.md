- **NBA game page:** `team_game_rates` returns a null `orb_pct` when either team's offensive plus defensive
  rebounds differ from its total, as ESPN's 2002-2012 NBA boxes do. (#757, #1041)
- **Validation:** the `nfl_model_pbp` schema snapshot follows the current release (`drive`, string drive yard
  lines, `tackle_with_assist_2_*`), and `kick_distance` allows a backwards punt (-11 floor). (#415, #1041)
- **NFL:** an `api.nfl.com` 2xx answer that carries a GraphQL `errors` list raises `AssetFetchError`. (#948, #1041)
- **NCAA MBB/WBB:** under `errors="skip"`, a rejected 2018-format page is `unsupported_layout: <format>`, not
  `empty`. (#948, #1041)
- **NHL:** `nhl_unit_ratings` breaks `unit_value` ties by team and unit, so its row order is reproducible.
  (#948, #1041)
