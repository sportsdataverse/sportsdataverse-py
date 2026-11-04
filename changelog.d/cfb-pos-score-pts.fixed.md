- **CFB play-by-play:** `pos_score_pts` is the points scored on the play, signed for the possessing team (a defensive
  touchdown or a safety is negative). It was the move in the running margin, so a feed score that dipped to 0 and came
  back read values such as 14, 21 or -70. `pos_score_diff_end` and EPA/WPA are unchanged. Fixes #1089. (#1096)
