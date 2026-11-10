- **Basketball:** `{nba,wnba,mbb,wbb}_game_stints` give every stint a player spent on the floor: period,
  clock in and out, points for and against and possessions. Each team's stint +/- sums to five times its
  final margin wherever the source lineup lists five players (BB-D32). (#1156)
- **Basketball:** `{nba,wnba,mbb,wbb}_game_units` give every 1- to 5-man set of teammates in a game, with
  ORtg/DRtg/net, the four factors and the rim / mid-range / three shares for and against, and
  `*_game_on_off` each player's in-game on and off splits (BB-D32). (#1156)
- **Basketball:** `nba_lineup_count_rows` takes an optional `pbp` for the pro shot-zone counts (a rim
  attempt is a two under 4 feet) (BB-D32). (#1156)
