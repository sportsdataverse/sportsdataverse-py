---
title: WBB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WBB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wbb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wbb_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull the officials assigned to a women's-college-basketball game.

### `espn_wbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_game_rosters() - Pull the game by id.

### `espn_wbb_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_wbb_pbp() - Pull the game by id. Data from API endpoints - `womens-college-basketball/playbyplay`, `womens-college-basketball/summary`

### `espn_wbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a women's-college-basketball athlete's ESPN **season** stat line.

### `espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_schedule - look up the women's college basketball schedule for a given season

### `espn_wbb_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'`

Pull ESPN team season stats for a women's-college-basketball team.

## Utilities & helpers

### `most_recent_wbb_season()`

Return the most recent women's college basketball season year.

## Other

### `espn_wbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_teams - look up the women's college basketball teams

### `scoreboard_event_parsing(event)`



### `wbb_pbp_disk(game_id, path_to_json)`
