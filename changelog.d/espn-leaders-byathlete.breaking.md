- **ESPN:** `espn_<league>_leaders()` drops its one-row envelope columns (`pagination_*`, `league_*`, `current_season_*`,
  the `athletes` / `glossary` / `categories` cells) for one row per athlete; leagues without a leaderboard give zero
  rows. Page counts and ESPN's per-stat ranks stay in `return_parsed=False`. (#1018)
