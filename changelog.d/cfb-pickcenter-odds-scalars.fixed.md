- **CFB:** the payload's `pickcenter` odds are plain numbers (they were one-element arrays) and include `homeFavorite`,
  so they feed straight back into `CFBPlayProcess(odds_override=...)`; values and play columns are unchanged. A game that
  falls to the default spread now logs a warning on the `sdv.cfb_pbp` logger. (#828)
