- **ESPN:** `espn_<league>_leaders()` returns one row per athlete with every stat as `{category}_{stat}` (Float64)
  and a `rank`; it flattened ESPN's envelope into one row. Football `category=` takes `offense:passing` (a bare
  `passing` is an HTTP 400), and a league with no leaderboard gives zero rows. (#1018)
