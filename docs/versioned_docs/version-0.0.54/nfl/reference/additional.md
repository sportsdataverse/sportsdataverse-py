---
title: NFL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nfl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_game_rosters() - Pull the game by id.

### `espn_nfl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NFL athlete's ESPN **season** stat line as one wide row.

### `espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_schedule - look up the NFL schedule for a given season

## Dataset loaders

### `load_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

### `load_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

### `load_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Depth Chart data for selected seasons

### `load_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

### `load_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL fantasy football opportunity data from ffverse/ffopportunity

### `load_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

### `load_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

### `load_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL FTN charting data going back to 2022

### `load_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL injuries data for selected seasons

### `load_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

### `load_nfl_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

### `load_nfl_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

### `load_nfl_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Depth Chart data for selected seasons

### `load_nfl_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

### `load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL fantasy football opportunity data from ffverse/ffopportunity

### `load_nfl_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

### `load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

### `load_nfl_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL FTN charting data going back to 2022

### `load_nfl_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL injuries data for selected seasons

### `load_nfl_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

### `load_nfl_ngs_passing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='passing')``.

### `load_nfl_ngs_receiving(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='receiving')``.

### `load_nfl_ngs_rushing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='rushing')``.

### `load_nfl_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

### `load_nfl_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play by play data going back to 1999

### `load_nfl_pbp_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play-by-play participation data for selected seasons

### `load_nfl_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

### `load_nfl_pfr_def(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='season')``.

### `load_nfl_pfr_pass(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='season')``.

### `load_nfl_pfr_rec(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='season')``.

### `load_nfl_pfr_rush(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='season')``.

### `load_nfl_pfr_weekly_def(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='week')``.

### `load_nfl_pfr_weekly_pass(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='week')``.

### `load_nfl_pfr_weekly_rec(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='week')``.

### `load_nfl_pfr_weekly_rush(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='week')``.

### `load_nfl_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL player stats data

### `load_nfl_players(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Player ID information

### `load_nfl_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL roster data for all seasons

### `load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL schedule data

### `load_nfl_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL snap counts data for selected seasons

### `load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team stats data going back to 1999

### `load_nfl_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

### `load_nfl_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

### `load_nfl_weekly_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL weekly roster data for selected seasons

### `load_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

### `load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play-by-play participation data for selected seasons

### `load_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play by play data going back to 1999

### `load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

### `load_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL player stats data

### `load_players(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Player ID information

### `load_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL roster data for all seasons

### `load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL weekly roster data for selected seasons

### `load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL schedule data

### `load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL snap counts data for selected seasons

### `load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team stats data going back to 1999

### `load_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

### `load_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

## Utilities & helpers

### `NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)`

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

### `get_current_nfl_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

### `get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

### `get_current_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

### `get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

### `most_recent_nfl_season(roster: 'bool' = False) -> 'int'`

Alias for `get_current_nfl_season()` mirroring nflreadr's `most_recent_season()`.

## Other

### `NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None`

Runtime configuration for sdv-py NFL loaders.

### `cached_loader(func: 'F') -> 'F'`

Decorator that adds caching to a ``load_nfl_*`` function.

### `clear_cache() -> 'None'`

Clear both memory and filesystem caches.

### `espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_teams - look up NFL teams

### `get_config() -> 'NflConfig'`

Return the live ``NflConfig`` singleton.

### `nfl_game_details(game_id=None, headers=None, raw=False) -> 'Dict'`

nfl_game_details() -- pull full ``api.nfl.com`` game details by game id.

### `nfl_game_schedule(season=2021, season_type='REG', week=1, headers=None, raw=False) -> 'Dict'`

nfl_game_schedule() -- list ``api.nfl.com`` games for a season/week slice.

### `nfl_headers_gen()`

Build the full request-header dict expected by ``api.nfl.com``.

### `nfl_token_gen()`

Mint a fresh ``api.nfl.com`` access token via the public reroute endpoint.

### `reset_config() -> 'NflConfig'`

Reset the active config to its env-var-derived defaults.

### `scoreboard_event_parsing(event)`

Normalize one ESPN scoreboard ``event`` into a flatter shape.

### `update_config(**kwargs: 'object') -> 'NflConfig'`

Update the active config in place.
