- **CFB play-by-play:** `pos_score_pts` is the points scored on the play, signed for the possessing team
  (a defensive touchdown or a safety is negative). It was the move in the running margin, so a feed score
  that fell to 0 for a few rows and came back read values such as 14, 21 or -70. Each team's points are now
  what its best score so far gained on the row. `pos_score_diff_end` and EPA/WPA are unchanged. (#1089)
