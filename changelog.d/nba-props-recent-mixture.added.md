- **NBA:** `nba_player_props` adds a last-10 line per stat (`stat_<stat>_recent_exp`) and `recent_weight`, and
  `prop_distribution` / `prob_over` price the recent-window mixture (`recent_value=`, `recent_weight=`); on held-out
  2018-19 it improves points CRPS by 0.035 (z 8.3). WNBA and G-League are unchanged. (#1316)
