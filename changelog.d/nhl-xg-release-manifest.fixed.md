- **NHL:** `ensure_xg_models` checks cached xG boosters against the `nhl_xg_models` release
  manifest on every load and replaces any that no longer match, so a retrain reaches an old
  cache. A download that does not match is refused. (#1181)
