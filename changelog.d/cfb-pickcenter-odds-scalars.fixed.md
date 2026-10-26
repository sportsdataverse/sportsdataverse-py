- **CFB:** the payload's `pickcenter` odds are plain numbers (they were one-element arrays) and include `homeFavorite`,
  so they feed straight back into `CFBPlayProcess(odds_override=...)`; values and play columns are unchanged. (#828)
- **CFB:** a game with no pregame spread (`gameSpreadAvailable` False, rebuilds included) warns the new
  `errors.DefaultOddsWarning`. (#828)
