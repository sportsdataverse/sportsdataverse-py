---
title: MLB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# MLB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mlb`
not covered by the generated API-endpoint reference above.

## Statcast

### `statcast_gamefeed(game_pk: 'int', at_bat_number: 'Optional[int]' = None, **kwargs) -> 'Dict'`

GET /gf?game_pk=... — Savant per-game JSON feed (richer than the Stats API live feed).

### `statcast_leaderboard_arm_strength(year: 'Union[int, str]', pos: 'Optional[str]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/arm-strength — outfielder + infielder arm-strength leaders.

### `statcast_leaderboard_bat_tracking(year: 'Union[int, str]', type_: 'str' = 'batter-swings', min_: 'Optional[Union[int, str]]' = 'q', attack_zone: 'Optional[str]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/bat-tracking — swing speed / attack angle (2024+).

### `statcast_leaderboard_catch_probability(year: 'Union[int, str]', csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/catch_probability — outfielder catch-probability leaderboard.

### `statcast_leaderboard_custom(year: 'Union[int, str]', type_: 'str', selections: 'str', filter_: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', sort: 'Optional[str]' = None, sort_dir: 'str' = 'desc', csv: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/custom — build-your-own metric leaderboard.

### `statcast_leaderboard_expected_statistics(year: 'Union[int, str]', type_: 'str' = 'batter', position: 'Optional[str]' = None, team: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/expected_statistics — xBA / xSLG / xwOBA / xISO leaders.

### `statcast_leaderboard_outs_above_average(year: 'Union[int, str]', pos: 'Optional[str]' = None, team: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/outs_above_average — OAA fielding leaderboard.

### `statcast_leaderboard_pitch_arsenal(year: 'Union[int, str]', team: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', pitch_hand: 'Optional[str]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/pitch-arsenal-stats — per-pitch outcome stats by pitcher.

### `statcast_leaderboard_poptime(year: 'Union[int, str]', min2b: 'Optional[int]' = None, min3b: 'Optional[int]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/poptime — catcher pop-time leaders.

### `statcast_leaderboard_sprint_speed(year: 'Union[int, str]', position: 'Optional[str]' = None, team: 'Optional[str]' = None, min_opp: 'Optional[int]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/sprint_speed — sprint-speed (ft/sec) leaders.

### `statcast_player_page(player_id: 'int', stats: 'Optional[str]' = None, **kwargs) -> 'str'`

GET /savant-player/{playerId} — Savant player profile page (HTML with embedded JSON).

### `statcast_search(start_date: 'str', end_date: 'str', *, player_type: 'str' = 'batter', season: 'Optional[Union[str, Iterable[str]]]' = None, game_type: 'Optional[Union[str, Iterable[str]]]' = None, batters_lookup: 'Optional[Union[int, Iterable[int]]]' = None, pitchers_lookup: 'Optional[Union[int, Iterable[int]]]' = None, team: 'Optional[str]' = None, opponent: 'Optional[str]' = None, home_road: 'Optional[str]' = None, stadium: 'Optional[Union[int, str]]' = None, pitcher_throws: 'Optional[str]' = None, batter_stands: 'Optional[str]' = None, position: 'Optional[Union[str, Iterable[str]]]' = None, pitch_type: 'Optional[Union[str, Iterable[str]]]' = None, count: 'Optional[Union[str, Iterable[str]]]' = None, at_bat_result: 'Optional[Union[str, Iterable[str]]]' = None, batted_ball_type: 'Optional[Union[str, Iterable[str]]]' = None, pitch_result: 'Optional[Union[str, Iterable[str]]]' = None, zone: 'Optional[Union[str, Iterable[str]]]' = None, outs: 'Optional[Union[int, Iterable[int]]]' = None, inning: 'Optional[Union[int, Iterable[int]]]' = None, runners_on: 'Optional[Union[str, Iterable[str]]]' = None, flag: 'Optional[Union[str, Iterable[str]]]' = None, return_as_pandas: 'bool' = False, raise_on_truncation: 'bool' = True, **kwargs)`

GET /statcast_search/csv — pitch-by-pitch Statcast search.

### `statcast_search_chunked(start_date: 'str', end_date: 'str', *, chunk_days: 'int' = 5, return_as_pandas: 'bool' = False, **kwargs)`

Auto-chunk a date range into ``chunk_days``-day windows and concatenate.

## MLB Stats API

### `mlb_api_attendance(team_id: 'Optional[int]' = None, league_id: 'Optional[Union[int, str]]' = None, season: 'Optional[Union[int, str]]' = None, league_list_id: 'Optional[str]' = None, game_type: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/attendance — game attendance figures.

### `mlb_api_divisions(sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, division_id: 'Optional[int]' = None, **kwargs) -> 'Dict'`

GET /api/v1/divisions — list divisions.

### `mlb_api_draft_prospects(year: 'Union[int, str]', scouting_report: 'Optional[bool]' = None, limit: 'int' = 100, **kwargs) -> 'Dict'`

GET /api/v1/draft/prospects/{year} — draft prospect list for a year.

### `mlb_api_pbp_diff(game_pk: 'int', start_timecode: 'str', end_timecode: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed.

### `mlb_api_pbp_live(game_pk: 'int', language: 'Optional[str]' = None, timecode: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

### `mlb_api_person_stats(person_id: 'int', stats: 'str', group: 'str' = 'hitting', season: 'Optional[Union[int, str]]' = None, season_type: 'Optional[str]' = None, sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/people/{personId}/stats — player aggregate stats.

### `mlb_api_schedule(date: 'Optional[str]' = None, start_date: 'Optional[str]' = None, end_date: 'Optional[str]' = None, team_id: 'Optional[int]' = None, opponent_id: 'Optional[int]' = None, season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, game_type: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/schedule — schedule of games for a date, range, team, or season.

### `mlb_api_seasons(sport_id: 'int' = 1, season: 'Optional[Union[int, str]]' = None, all_seasons: 'bool' = False, **kwargs) -> 'Dict'`

GET /api/v1/seasons — list of seasons for a sport.

### `mlb_api_standings(league_id: 'Union[int, str, List[int]]' = '103,104', season: 'Optional[Union[int, str]]' = None, date: 'Optional[str]' = None, standings_types: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/standings — league standings.

### `mlb_api_stats(stats: 'str', group: 'str', season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, team_id: 'Optional[int]' = None, player_pool: 'Optional[str]' = None, game_type: 'Optional[str]' = None, limit: 'int' = 50, offset: 'int' = 0, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/stats — generic stats query.

### `mlb_api_stats_leaders(leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, stat_group: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, limit: 'int' = 10, **kwargs) -> 'Dict'`

GET /api/v1/stats/leaders — top-N leaders for a stat category.

### `mlb_api_stats_streaks(streak_type: 'str', streak_threshold: 'int' = 1, season: 'Optional[Union[int, str]]' = None, stat_group: 'Optional[str]' = None, active_streak: 'Optional[bool]' = None, sport_id: 'int' = 1, **kwargs) -> 'Dict'`

GET /api/v1/stats/streaks — active or historical streaks.

### `mlb_api_team_leaders(team_id: 'int', leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, limit: 'int' = 10, **kwargs) -> 'Dict'`

GET /api/v1/teams/{teamId}/leaders — team leaders.

### `mlb_api_team_stats(team_id: 'int', season: 'Union[int, str]', stats: 'str' = 'season', group: 'str' = 'hitting', sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/teams/{teamId}/stats — team-level stats.

### `mlb_api_teams(season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_ids: 'Optional[Union[int, List[int], str]]' = None, active_status: 'Optional[str]' = None, all_star_statuses: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/teams — list teams. ``sport_id=1`` = MLB.

## Play-by-play, schedule & rosters

### `espn_mlb_game_rosters(game_id: 'int', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)`

espn_mlb_game_rosters - pull the active game rosters for both teams.

### `espn_mlb_pbp(game_id: 'int', raw: 'bool' = False, **kwargs) -> 'Dict'`

espn_mlb_pbp - pull the full ESPN game-summary payload for one MLB game.

### `espn_mlb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an MLB athlete's ESPN **season** stat line as one wide row.

### `espn_mlb_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_schedule - look up the MLB schedule for a given date or season-year.

## Dataset loaders

### `load_mlb_pbp(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_pbp - planned: load pre-built season-level MLB play-by-play.

### `load_mlb_player_boxscore(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_player_boxscore - planned: load pre-built season-level MLB player boxscores.

### `load_mlb_rosters(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_rosters - planned: load pre-built season-level MLB rosters.

### `load_mlb_schedule(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_schedule - planned: load pre-built season-level MLB schedule.

### `load_mlb_team_boxscore(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_team_boxscore - planned: load pre-built season-level MLB team boxscores.

## Utilities & helpers

### `most_recent_mlb_season() -> 'int'`

most_recent_mlb_season - return the most recent / current MLB season year.

## Other

### `espn_mlb_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.
