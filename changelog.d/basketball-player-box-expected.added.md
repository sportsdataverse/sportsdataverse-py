- **NBA/WNBA/MBB/WBB:** `{nba,wnba,mbb,wbb}_player_season_box_rates` give each player-season's points, rebounds and
  assists per 36 minutes with `_n` / `_rank` / `_pct` (minutes gates fitted on split halves), and
  `{nba,wnba,mbb,wbb}_player_game_expected_box` give each player-game the season-without-this-game per-minute
  expectation (`<s>_base`) and a usage-adjusted one (`<s>_exp`, a Poisson model fitted on earlier seasons by
  `tools/fit_player_box_expected.py`), ESPN-keyed. On the held-out seasons the usage-adjusted points cut the per-minute
  baseline's mean absolute error by 20-25% in every league; rebounds by about 1%, assists not at all.
