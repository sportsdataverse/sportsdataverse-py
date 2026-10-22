- **NFL:** released `home_`/`away_wp_after_naive` were flipped on every change of possession (3,671 rows, ~14 a game, in
  268 held-out games), and `wp_after_naive`/`wpa_naive` read the next row (a Timeout, or a half's end), not the next real play (1,855 rows in
  256 games, max 0.38). Naive home WP is now continuous into the next real play (breaks 5,438 -> 0), as nflfastR. (#973)
