- **WNBA:** `wnba_epv_plays` gives EPV and EPV added on every WNBA play-by-play event, from a booster fitted on
  2022-24 and gated on 2025 and 2026, its scoring level read from each game's date (BB-D24). (#1120)
- **WNBA:** one 2026 gate-3 state bucket is an owner-accepted exception, not a pass; the model card and
  `epv_wnba_holdout.json` list it (BB-D24). (#1120)
- **WNBA:** `wnba_epv_player_games` / `_lineup_games` / `_team_games` roll EPV added up to players, units and
  team-games, as the NBA and college tables do (BB-D24). (#1120)
- **Basketball:** `nba_epv.epv_level` now requires each game's date (`game_order`) (BB-D24). (#1120)
