- **Basketball:** `nba_pregame_projections` carries the in-game WP prior (BB-D25, `nba_rated_priors`) beside the
  team_strength projection: `wp_prior_home_prob`, `wp_prior_exp_home_margin`, the two game-model ratings and ranks,
  `wp_prior_poss`, `wp_prior_hfa` (points, 0 at a neutral site) and `wp_prior_margin_sd`, so a page can show the
  number the win-probability chart starts from. `rated_home_probs` also returns the ranks, `poss` and `margin_sd`.
