- **CFB:** a half-ending play keys `wp_after` on the next real play's team (cfbfastR's rule), not its own possession change. A play that kept the ball took the model's end
  state when the other team snaps next; it now takes 1 - that play's `wp_before` (401858224's tying FG 0.476 -> 0.498). Mostly first-half endings: 64 rows in 62/132 captures, 230 in
  222/580 held-out 2014-23 games, max |Δ| 0.161, in `wp_after`, `wpa`, `def/home/away_wp_after` and their `_naive` twins. (#966)
