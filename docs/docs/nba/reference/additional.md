---
title: NBA — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nba`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NBA athlete's ESPN **season** stat line as one wide row.

### `espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_schedule - look up the NBA schedule for a given date from ESPN

## Utilities & helpers

### `most_recent_nba_season()`

Return the most recent NBA season year based on today's date.

### `year_to_season(year)`

Convert a season-end year (e.g. 2024) to the NBA's hyphenated label (e.g. ``"2023-24"``).

## Other

### `espn_nba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_teams - look up NBA teams

### `nba_pbp_disk(game_id, path_to_json)`

Load a previously cached ESPN NBA summary JSON for a game from disk.

### `scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN NBA scoreboard event dict into a shape suitable for ``pd.json_normalize``.
