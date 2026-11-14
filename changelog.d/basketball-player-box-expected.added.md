- **NBA/WNBA/MBB/WBB:** `{nba,wnba,mbb,wbb}_player_season_box_rates` give points, rebounds and assists per 36 minutes
  ranked with `_n` / `_rank` / `_pct`; `{nba,wnba,mbb,wbb}_player_game_expected_box` give each player-game a per-minute
  and a usage-adjusted expectation from his season without that game, ESPN-keyed. (#1191)
- **NBA/WNBA/MBB/WBB:** the usage-adjusted points cut the per-minute MAE by 20-25% on held-out seasons in every league;
  rebounds by about 1%, assists not at all. Fitted by `tools/fit_player_box_expected.py`. (#1191)
