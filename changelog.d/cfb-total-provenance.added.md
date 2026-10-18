- **CFB:** an `overUnderAvailable` play column and payload key say whether `overUnder` is a market total or the 55.5
  default (a pickcenter or ESPN BET line with a spread and no total); `odds_override` accepts it, so a rebuild keeps the
  flag and warns `DefaultOddsWarning` again instead of relabelling the default as injected. (R837-2, R837-3)
