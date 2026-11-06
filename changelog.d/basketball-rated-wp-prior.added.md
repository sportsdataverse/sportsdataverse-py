- **Basketball:** `nba_rated_priors` rates every NBA / WNBA game from the as-of team ratings; `home_prior`,
  `score_win_prob` and `game_page_tables` take it as `rated_home_prob`, the prior when there is no line
  (BB-D25). (#1116)
- **Basketball:** the NBA / WNBA in-game WP boosters ship model cards (`<lg>_in_game_wp.card.json`) from
  `tools/models/nba_prediction/wp_model_cards.py`: holdout Brier and calibration by period, horn behaviour. (#1116)
