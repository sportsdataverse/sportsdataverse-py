- **NFL fourth down:** the PAT make probability (`get_2pt_wp` / `get_2pt_probs`, `conv_1pt`) scores the FG model at
  nflfastR's pre-2015 spot (`yardline_100 = 3`) before 2015, not the modern 15 for every season; new constants
  `PAT_YARDLINE_PRE_2015` / `PAT_YARDLINE_POST_2015` / `PAT_FIRST_SEASON_FROM_15` in `nfl.model_vars`. (#742)
