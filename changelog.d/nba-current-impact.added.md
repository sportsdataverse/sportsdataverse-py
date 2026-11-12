- **NBA:** `nba_current_impact` rates each player's expected impact as of a date from the games before it:
  the box prior v2 on the 365 days of box lines before the date (`current_impact_prior`) plus a joint
  Kalman-filtered deviation, with entry, new-season and team-change shocks and daily drift (BB-D39).
- **NBA:** on 28-day forward error over 2014-2026 it beats the season-to-date `nba_reml_rapm` by 5.53 bp of
  MSE (season-cluster bootstrap SE 0.35) in all 13 seasons, and its strongest tuned variant by 1.91 bp (SE
  0.20); the SDs are fitted on 2007-2013 by `tools/fit_current_impact.py`.
