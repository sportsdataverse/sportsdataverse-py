---
title: MBB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# MBB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mbb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_mbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_game_rosters() - Pull the game by id.

### `espn_mbb_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_mbb_pbp() - Pull the game by id. Data from API endpoints: `mens-college-basketball/playbyplay`, `mens-college-basketball/summary`

### `espn_mbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a men's-college-basketball athlete's ESPN **season** stat line.

### `espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_schedule - look up the men's college basketball scheduler for a given season

## Utilities & helpers

### `most_recent_mbb_season()`

Return the most recent men's college basketball season year.

## Other

### `espn_mbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_teams - look up the men's college basketball teams

### `mbb_pbp_disk(game_id, path_to_json)`



### `scoreboard_event_parsing(event)`
