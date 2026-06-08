---
title: NHL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nhl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nhl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_game_rosters() - Pull the game by id.

### `espn_nhl_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

### `espn_nhl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NHL athlete's ESPN **season** stat line as one wide row.

### `espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_schedule - look up the NHL schedule for a given date

## NHL native

### `nhl_pbp_disk(game_id, path_to_json)`



### `nhl_records_coach_milestone_wins(wins: 'int', playoffs: 'bool' = False, **filters) -> 'Dict'`

Coaches who reached a wins milestone in fewest games.

### `nhl_records_comeback_wins(scope: 'str' = 'league', **filters) -> 'Dict'`

Comeback wins from a multi-goal deficit.

### `nhl_records_consecutive_goal_seasons(goals: 'int' = 50, **filters) -> 'Dict'`

Skaters with the most consecutive N-goal seasons.

### `nhl_records_fastest_goals(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals by one team in a single game.

### `nhl_records_fastest_goals_both_teams(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals combined (both teams) in a single game.

### `nhl_records_games_played_streak_skaters(active_only: 'bool' = False, **filters) -> 'Dict'`

Consecutive games-played streaks for skaters.

### `nhl_scoreboard(date: 'Optional[str]' = None, team: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Dict'`

In-game scoreboard payload (renamed from ``nhl_web_scoreboard``).

## Utilities & helpers

### `most_recent_nhl_season()`

most_recent_nhl_season - return the season year for "today".

### `year_to_season(year)`

year_to_season - format a starting year as the canonical ``YYYY-YY`` season string.

## Other

### `espn_nhl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_teams - look up NHL teams

### `scoreboard_event_parsing(event)`
