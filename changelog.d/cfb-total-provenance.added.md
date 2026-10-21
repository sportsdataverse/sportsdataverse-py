- **CFB:** an `overUnderAvailable` play column and payload key say whether `overUnder` is a market total or the 55.5
  default (a line with a spread and no total). `odds_override` accepts it (a missing key is unknown: a null flag), so a
  rebuild keeps it and warns `DefaultOddsWarning` again instead of relabelling the default as injected. (#895)
