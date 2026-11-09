- **NBA/WNBA:** `nba_bpm` applies the BPM 2.0 team adjustment at its published size (team rating / 5 per
  player) and splits it: `obpm` takes the team's offensive rating relative to the league, `dbpm = bpm - obpm`
  the defense. League obpm / dbpm now average 0; they read -2.94 / +2.90 before. (#1148)
