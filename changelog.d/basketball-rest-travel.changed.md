- **NBA season sim:** each scheduled game's expected margin now carries the fitted effect of rest, back-to-backs,
  three-in-fours, travel and time zones crossed, read off the schedule and a bundled, sourced table of arenas and
  neutral sites (`sportsdataverse/nba/data/game_sites.csv`). Walk-forward, it lowers per-game log loss, Brier and
  margin MAE on each of the 2023-24, 2024-25 and 2025-26 held-out seasons (pooled log loss -0.0030 +- 0.0013);
  nearly all of it is the back-to-back (about 2 points). Season totals are unchanged within noise. The WNBA fit
  failed its held-out gate, so the WNBA sim is unchanged. (BB-D23)
