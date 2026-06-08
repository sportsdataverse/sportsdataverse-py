---
title: WNBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# WNBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wnba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_wnba_game_officials(game_id: 'int', season: 'int | None' = None, *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull the officials assigned to a WNBA game.

### `espn_wnba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a WNBA athlete's ESPN **season** stat line.

### `espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_schedule - look up the WNBA schedule for a given season

### `espn_wnba_team_stats(team_id: 'int', season: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]'`

Pull ESPN team season stats for a WNBA team.

## Utilities & helpers

### `most_recent_wnba_season()`

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

## Other

### `espn_wnba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_teams - look up WNBA teams

### `scoreboard_event_parsing(event)`



### `wnba_pbp_disk(game_id, path_to_json)`
