- **NFL:** `home_`/`away_wp_after_naive` were flipped on every change of possession (about 14 rows a game), and
  `wp_after_naive` / `wpa_naive` read the next row (a Timeout, or a half's end), not the next real play (up to 0.38).
  Naive home WP now runs continuously into the next real play, as in nflfastR. (#973)
