---
title: MLB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# MLB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mlb`
not covered by the generated API-endpoint reference above.

## MLB Stats API

### `mlb_api_attendance(team_id: 'Optional[int]' = None, league_id: 'Optional[Union[int, str]]' = None, season: 'Optional[Union[int, str]]' = None, league_list_id: 'Optional[str]' = None, game_type: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_attendance}

GET /api/v1/attendance — game attendance figures.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `league_list_id` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |

### `mlb_api_divisions(sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, division_id: 'Optional[int]' = None, **kwargs) -> 'Dict'` {#mlb_api_divisions}

GET /api/v1/divisions — list divisions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `division_id` | `Optional[int]` | `None` |  |

### `mlb_api_draft_prospects(year: 'Union[int, str]', scouting_report: 'Optional[bool]' = None, limit: 'int' = 100, **kwargs) -> 'Dict'` {#mlb_api_draft_prospects}

GET /api/v1/draft/prospects/{year} — draft prospect list for a year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `scouting_report` | `Optional[bool]` | `None` |  |
| `limit` | `int` | `100` |  |

### `mlb_api_pbp_diff(game_pk: 'int', start_timecode: 'str', end_timecode: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_pbp_diff}

GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed.

Replays of in-game state for low-bandwidth clients.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `start_timecode` | `str` |  |  |
| `end_timecode` | `Optional[str]` | `None` |  |

### `mlb_api_pbp_live(game_pk: 'int', language: 'Optional[str]' = None, timecode: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_pbp_live}

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

Top-level keys: `copyright, gamePk, link, metaData, gameData, liveData`.
Includes Statcast metrics where available. The historical name
`mlb_api_pbp` is preserved as an alias.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `language` | `Optional[str]` | `None` |  |
| `timecode` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_person_stats(person_id: 'int', stats: 'str', group: 'str' = 'hitting', season: 'Optional[Union[int, str]]' = None, season_type: 'Optional[str]' = None, sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_person_stats}

GET /api/v1/people/{personId}/stats — player aggregate stats.

`stats`: `season`, `career`, `yearByYear`, `vsTeam`, `vsPlayer`,
`byMonth`, `byDayOfWeek`, `homeAndAway`, `gameLog`, `lastXGames`, …

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `person_id` | `int` |  |  |
| `stats` | `str` |  |  |
| `group` | `str` | `'hitting'` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `season_type` | `Optional[str]` | `None` |  |
| `sport_ids` | `Optional[Union[int, List[int]]]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_schedule(date: 'Optional[str]' = None, start_date: 'Optional[str]' = None, end_date: 'Optional[str]' = None, team_id: 'Optional[int]' = None, opponent_id: 'Optional[int]' = None, season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, game_type: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_schedule}

GET /api/v1/schedule — schedule of games for a date, range, team, or season.

Response: `dates[].games[]`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` |  |
| `start_date` | `Optional[str]` | `None` |  |
| `end_date` | `Optional[str]` | `None` |  |
| `team_id` | `Optional[int]` | `None` |  |
| `opponent_id` | `Optional[int]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_seasons(sport_id: 'int' = 1, season: 'Optional[Union[int, str]]' = None, all_seasons: 'bool' = False, **kwargs) -> 'Dict'` {#mlb_api_seasons}

GET /api/v1/seasons — list of seasons for a sport.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `all_seasons` | `bool` | `False` |  |

### `mlb_api_standings(league_id: 'Union[int, str, List[int]]' = '103,104', season: 'Optional[Union[int, str]]' = None, date: 'Optional[str]' = None, standings_types: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_standings}

GET /api/v1/standings — league standings.

`league_id`: `103` AL, `104` NL (comma-separated for both, the default).
`standings_types` e.g. `regularSeason`, `wildCard`, `divisionLeaders`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `Union[int, str, List[int]]` | `'103,104'` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `date` | `Optional[str]` | `None` |  |
| `standings_types` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_stats(stats: 'str', group: 'str', season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, team_id: 'Optional[int]' = None, player_pool: 'Optional[str]' = None, game_type: 'Optional[str]' = None, limit: 'int' = 50, offset: 'int' = 0, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_stats}

GET /api/v1/stats — generic stats query.

`stats` selects the slice (`season`, `career`, `yearByYear`, …) and
`group` selects the stat group (`hitting`, `pitching`, `fielding`).
Filters: `season`, `team_id`, `league_id`, `game_type`, `player_pool`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stats` | `str` |  |  |
| `group` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `team_id` | `Optional[int]` | `None` |  |
| `player_pool` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `limit` | `int` | `50` |  |
| `offset` | `int` | `0` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_stats_leaders(leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, stat_group: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, limit: 'int' = 10, **kwargs) -> 'Dict'` {#mlb_api_stats_leaders}

GET /api/v1/stats/leaders — top-N leaders for a stat category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `leader_categories` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `leader_game_types` | `Optional[str]` | `None` |  |
| `stat_group` | `Optional[str]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `limit` | `int` | `10` |  |

### `mlb_api_stats_streaks(streak_type: 'str', streak_threshold: 'int' = 1, season: 'Optional[Union[int, str]]' = None, stat_group: 'Optional[str]' = None, active_streak: 'Optional[bool]' = None, sport_id: 'int' = 1, **kwargs) -> 'Dict'` {#mlb_api_stats_streaks}

GET /api/v1/stats/streaks — active or historical streaks.

`streak_type` e.g. `hittingStreakOverall`, `onBaseOverall`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `streak_type` | `str` |  |  |
| `streak_threshold` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `stat_group` | `Optional[str]` | `None` |  |
| `active_streak` | `Optional[bool]` | `None` |  |
| `sport_id` | `int` | `1` |  |

### `mlb_api_team_leaders(team_id: 'int', leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, limit: 'int' = 10, **kwargs) -> 'Dict'` {#mlb_api_team_leaders}

GET /api/v1/teams/{teamId}/leaders — team leaders.

`leader_categories` e.g. `homeRuns`, `battingAverage`, `wins`,
`earnedRunAverage` (comma-separated for multi).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `leader_categories` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `leader_game_types` | `Optional[str]` | `None` |  |
| `limit` | `int` | `10` |  |

### `mlb_api_team_stats(team_id: 'int', season: 'Union[int, str]', stats: 'str' = 'season', group: 'str' = 'hitting', sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_team_stats}

GET /api/v1/teams/{teamId}/stats — team-level stats.

`stats`: `season`, `career`, `yearByYear`, `byMonth`, `byDayOfWeek`, …
`group`: `hitting`, `pitching`, `fielding`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Union[int, str]` |  |  |
| `stats` | `str` | `'season'` |  |
| `group` | `str` | `'hitting'` |  |
| `sport_ids` | `Optional[Union[int, List[int]]]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_teams(season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_ids: 'Optional[Union[int, List[int], str]]' = None, active_status: 'Optional[str]' = None, all_star_statuses: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_api_teams}

GET /api/v1/teams — list teams. `sport_id=1` = MLB.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `league_ids` | `Optional[Union[int, List[int], str]]` | `None` |  |
| `active_status` | `Optional[str]` | `None` |  |
| `all_star_statuses` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

## Play-by-play, schedule & rosters

### `espn_mlb_game_rosters(game_id: 'int', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)` {#espn_mlb_game_rosters}

espn_mlb_game_rosters - pull the active game rosters for both teams.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game id. |
| `raw` | `bool` | `False` | When True, returns the merged competitor + roster payload dict. |
| `return_as_pandas` | `bool` | `False` | When True, returns a pandas dataframe; otherwise polars. |

**Returns**

One row per (game × team × athlete) with columns `game_id, team_id, home_away, athlete_id, athlete_full_name, athlete_jersey, athlete_position_id, athlete_position_abbreviation, athlete_starter`.

**Example**

```python
from sportsdataverse.mlb import espn_mlb_game_rosters
ros = espn_mlb_game_rosters(game_id=401569461)
print(ros.shape)
ros.group_by("home_away").len()
```

### `espn_mlb_pbp(game_id: 'int', raw: 'bool' = False, **kwargs) -> 'Dict'` {#espn_mlb_pbp}

espn_mlb_pbp - pull the full ESPN game-summary payload for one MLB game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game id (the "event id"). Obtainable from `espn_mlb_schedule`. |
| `raw` | `bool` | `False` | When True, returns the full nested payload unchanged. When False (default), the same payload is returned for now — full parsing into a tidy plays / boxscore dict is **not yet implemented**; see the TODO below. |

**Returns**

The Site v2 summary payload. Top-level keys typically include `header`, `boxscore`, `plays`, `leaders`, `scoringPlays`, `gameInfo`, `winprobability`, `pickcenter`, `news`, `videos`, `standings`, `article`, `seasonseries`, `broadcasts`, `predictor`.

**Example**

```python
from sportsdataverse.mlb import espn_mlb_pbp
game = espn_mlb_pbp(game_id=401569461, raw=True)
sorted(game.keys())
print(game.get("header", {}).get("competitions", [{}])[0].get("date"))

# Iterate the plays array

plays = game.get("plays") or []
print(f"{len(plays)} plays")
for p in plays[:3]:
    print(p.get("text"))
```

### `espn_mlb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_mlb_player_stats}

Pull an MLB athlete's ESPN **season** stat line as one wide row.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns (for baseball: `batting_*`, `pitching_*`, `fielding_*`),
the athlete / team metadata blocks, and the `season_type` / `total`
parameters. For the richer multi-category web-v3 payload use
`sportsdataverse.mlb.espn_mlb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN MLB athlete identifier (e.g. `33192` for Aaron Judge). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `season_type` | character | Season-type id. |
| `total` | logical | Total. |
| `athlete_id` | integer | Unique ESPN athlete identifier. |
| `athlete_uid` | character | Athlete uid. |
| `athlete_guid` | character | Athlete guid. |
| `athlete_type` | character | Athlete type. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `full_name` | character | Player's full name. |
| `display_name` | character | Display name. |
| `short_name` | character | Short display name. |
| `weight` | double | Weight in pounds. |
| `display_weight` | character | Display weight. |
| `height` | double | Height (feet and inches). |
| `display_height` | character | Display height. |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Date of birth. |
| `jersey` | character | Jersey number worn by the player. |
| `slug` | character | URL-safe identifier. |
| `active` | logical | Whether the player is currently active. |
| `position_id` | integer | Unique position identifier. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation. |
| `college_name` | character | College name. |
| `status_id` | integer | Status id. |
| `status_name` | character | Game status (e.g. 'STATUS_FINAL'). |
| `batting_games_played` | double | Team batting: batting games played. |
| `batting_team_games_played` | double | Team batting: batting team games played. |
| `batting_hit_by_pitch` | double | Team batting: batting hit by pitch. |
| `batting_ground_balls` | double | Team batting: batting ground balls. |
| `batting_strikeouts` | double | Team batting: batting strikeouts. |
| `batting_rb_is` | double | Team batting: batting rb is. |
| `batting_sac_hits` | double | Team batting: batting sac hits. |
| `batting_hits` | double | Team batting: batting hits. |
| `batting_stolen_bases` | double | Team batting: batting stolen bases. |
| `batting_walks` | double | Team batting: batting walks. |
| `batting_catcher_interference` | double | Team batting: batting catcher interference. |
| `batting_runs` | double | Team batting: batting runs. |
| `batting_gid_ps` | double | Team batting: batting gid ps. |
| `batting_sac_flies` | double | Team batting: batting sac flies. |
| `batting_at_bats` | double | Team batting: batting at bats. |
| `batting_home_runs` | double | Team batting: batting home runs. |
| `batting_grand_slam_home_runs` | double | Team batting: batting grand slam home runs. |
| `batting_runners_left_on_base` | double | Team batting: batting runners left on base. |
| `batting_triples` | double | Team batting: batting triples. |
| `batting_game_winning_rb_is` | double | Team batting: batting game winning rb is. |
| `batting_intentional_walks` | double | Team batting: batting intentional walks. |
| `batting_doubles` | double | Team batting: batting doubles. |
| `batting_fly_balls` | double | Team batting: batting fly balls. |
| `batting_caught_stealing` | double | Team batting: batting caught stealing. |
| `batting_pitches` | double | Team batting: batting pitches. |
| `batting_games_started` | double | Team batting: batting games started. |
| `batting_pinch_at_bats` | double | Team batting: batting pinch at bats. |
| `batting_pinch_hits` | double | Team batting: batting pinch hits. |
| `batting_player_rating` | double | Team batting: batting player rating. |
| `batting_is_qualified` | double | Team batting: batting is qualified. |
| `batting_is_qualified_steals` | double | Team batting: batting is qualified steals. |
| `batting_total_bases` | double | Team batting: batting total bases. |
| `batting_plate_appearances` | double | Team batting: batting plate appearances. |
| `batting_projected_home_runs` | double | Team batting: batting projected home runs. |
| `batting_extra_base_hits` | double | Team batting: batting extra base hits. |
| `batting_runs_created` | double | Team batting: batting runs created. |
| `batting_avg` | double | Team batting: batting average. |
| `batting_pinch_avg` | double | Team batting: batting pinch avg. |
| `batting_slug_avg` | double | Team batting: batting slug avg. |
| `batting_secondary_avg` | double | Team batting: batting secondary avg. |
| `batting_on_base_pct` | double | Team batting: batting on base pct. |
| `batting_ops` | double | Team batting: batting ops. |
| `batting_ground_to_fly_ratio` | double | Team batting: batting ground to fly ratio. |
| `batting_runs_created_per27_outs` | double |  |
| `batting_batter_rating` | double | Team batting: batting batter rating. |
| `batting_at_bats_per_home_run` | double | Team batting: batting at bats per home run. |
| `batting_stolen_base_pct` | double | Team batting: batting stolen base pct. |
| `batting_pitches_per_plate_appearance` | double | Team batting: batting pitches per plate appearance. |
| `batting_isolated_power` | double | Team batting: batting isolated power. |
| `batting_walk_to_strikeout_ratio` | double | Team batting: batting walk to strikeout ratio. |
| `batting_walks_per_plate_appearance` | double | Team batting: batting walks per plate appearance. |
| `batting_secondary_avg_minus_ba` | double | Team batting: batting secondary avg minus ba. |
| `batting_runs_produced` | double | Team batting: batting runs produced. |
| `batting_runs_ratio` | double | Team batting: batting runs ratio. |
| `batting_patience_ratio` | double |  |
| `batting_bipa` | double |  |
| `batting_mlb_rating` | double |  |
| `batting_off_warbr` | double |  |
| `batting_warbr` | double |  |
| `fielding_games_played` | double |  |
| `fielding_team_games_played` | double |  |
| `fielding_double_plays` | double |  |
| `fielding_opportunities` | double |  |
| `fielding_errors` | double |  |
| `fielding_passed_balls` | double |  |
| `fielding_assists` | double |  |
| `fielding_outfield_assists` | double |  |
| `fielding_pickoffs` | double |  |
| `fielding_putouts` | double |  |
| `fielding_outs_on_field` | double |  |
| `fielding_triple_plays` | double |  |
| `fielding_balls_in_zone` | double |  |
| `fielding_extra_bases` | double |  |
| `fielding_outs_made` | double |  |
| `fielding_hits` | double |  |
| `fielding_total_bases` | double |  |
| `fielding_games_started` | double |  |
| `fielding_catcher_third_innings_played` | double |  |
| `fielding_catcher_caught_stealing` | double |  |
| `fielding_catcher_stolen_bases_allowed` | double |  |
| `fielding_catcher_earned_runs` | double |  |
| `fielding_is_qualified` | double |  |
| `fielding_is_qualified_catcher` | double |  |
| `fielding_is_qualified_pitcher` | double |  |
| `fielding_successful_chances` | double |  |
| `fielding_total_chances` | double |  |
| `fielding_full_innings_played` | double |  |
| `fielding_part_innings_played` | double |  |
| `fielding_fielding_pct` | double |  |
| `fielding_range_factor` | double |  |
| `fielding_zone_rating` | double |  |
| `fielding_catcher_caught_stealing_pct` | double |  |
| `fielding_catcher_era` | double |  |
| `fielding_def_warbr` | double |  |
| `team_id` | integer | Unique ESPN team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID). |
| `team_guid` | character | ESPN team GUID. |
| `team_slug` | character | URL-safe team identifier. |
| `team_location` | character | Team city / location. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'NYY'). |
| `team_display_name` | character | Full team display name (e.g. 'New York Yankees'). |
| `team_short_display_name` | character | Short team display name. |
| `team_color` | character | Team primary color (hex, no leading '#'). |
| `team_alternate_color` | character | Team alternate color (hex). |
| `team_is_active` | logical | Team is active. |
| `team_logo_href` | character | Default team logo URL; `team_detail = TRUE` only. |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_player_stats
df = espn_mlb_player_stats(athlete_id=33192, season=2023)
df.select(["full_name", "team_display_name", "batting_home_runs"])
```

### `espn_mlb_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_mlb_schedule}

espn_mlb_schedule - look up the MLB schedule for a given date or season-year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Date filter. Either a calendar date as YYYYMMDD or a season-year (e.g. 2024). When a 4-digit year is passed, the call returns the full season slate (paginated by `limit`). |
| `season_type` | `int` | `None` | Season type — 1 = spring training, 2 = regular, 3 = postseason, 4 = all-star. |
| `limit` | `int` | `500` | Number of records to return. Default 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule. Returns `None` if no games.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique ESPN game/event identifier. |
| `date` | character | Date in YYYY-MM-DD format. |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | Season-type id. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `venue_id` | character | MLBAM venue ID. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / province. |
| `home_id` | character | Unique identifier for home. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_score` | character | Home team run total after the play. |
| `home_winner` | logical | Whether the home team won. |
| `away_id` | character | Unique identifier for away. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_score` | character | Away team run total after the play. |
| `away_winner` | logical | Whether the away team won. |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_schedule
sched = espn_mlb_schedule(dates=20240328)
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

# Pull a regular-season slate from a season-year

reg = espn_mlb_schedule(dates=2024, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)

# Pandas round-trip for one date

espn_mlb_schedule(dates=20240328, return_as_pandas=True).head()
```

## Dataset loaders

### `load_mlb_pbp(seasons: 'List[int]', return_as_pandas: 'bool' = False)` {#load_mlb_pbp}

load_mlb_pbp - planned: load pre-built season-level MLB play-by-play.

TODO: Implement once an MLB-data release pipeline is in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_player_boxscore(seasons: 'List[int]', return_as_pandas: 'bool' = False)` {#load_mlb_player_boxscore}

load_mlb_player_boxscore - planned: load pre-built season-level MLB player boxscores.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_rosters(seasons: 'List[int]', return_as_pandas: 'bool' = False)` {#load_mlb_rosters}

load_mlb_rosters - planned: load pre-built season-level MLB rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_schedule(seasons: 'List[int]', return_as_pandas: 'bool' = False)` {#load_mlb_schedule}

load_mlb_schedule - planned: load pre-built season-level MLB schedule.

TODO: Implement once an MLB-data release pipeline is in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_team_boxscore(seasons: 'List[int]', return_as_pandas: 'bool' = False)` {#load_mlb_team_boxscore}

load_mlb_team_boxscore - planned: load pre-built season-level MLB team boxscores.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

## Utilities & helpers

### `most_recent_mlb_season() -> 'int'` {#most_recent_mlb_season}

most_recent_mlb_season - return the most recent / current MLB season year.

MLB seasons run calendar-year. Before April we still consider the *previous* year
the "most recent" season (since spring training only starts in late February).

**Returns**

The most recent MLB season year (e.g. `2024`).

## Other

### `espn_mlb_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_mlb_teams}

espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for MLB. This function caches by default, so if you want to refresh the data, use `sportsdataverse.mlb.espn_mlb_teams.cache_clear()`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. 'NYY'). |
| `team_alternate_color` | character | Team alternate color (hex). |
| `team_color` | character | Team primary color (hex, no leading '#'). |
| `team_display_name` | character | Full team display name (e.g. 'New York Yankees'). |
| `team_id` | character | Unique ESPN team identifier. |
| `team_is_active` | logical | Team is active. |
| `team_is_all_star` | logical | Team is all star. |
| `team_location` | character | Team city / location. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team name. |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Short team display name. |
| `team_slug` | character | URL-safe team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID). |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_teams
teams = espn_mlb_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Los Angeles Dodgers (team_id 19)

import polars as pl
teams.filter(pl.col("team_id") == "19").to_dicts()

# Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas

espn_mlb_teams.cache_clear()
teams_pd = espn_mlb_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```

### `fox_mlb_league_leaders(category: 'str' = 'batting', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_mlb_league_leaders}

MLB statistical leaders (`stats-con`); who=player|team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'batting'` | Stat category. Defaults to `"batting"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_leaders
df = fox_mlb_league_leaders("batting")
```

### `fox_mlb_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_mlb_odds}

MLB game odds six-pack (run line / to-win / total per team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_odds
df = fox_mlb_odds("...")
```

### `fox_mlb_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_mlb_standings}

MLB standings for a team's division/league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_standings
df = fox_mlb_standings("...")
```

### `fox_mlb_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_mlb_team_gamelog}

MLB team game log (long: one row per game-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_team_gamelog
df = fox_mlb_team_gamelog("...")
```

### `fox_mlb_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_mlb_team_roster}

MLB team roster (one row per player).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the position-group tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_team_roster
df = fox_mlb_team_roster("...")
```

### `fox_mlb_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_mlb_team_stats}

MLB team stat leaders by category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_team_stats
df = fox_mlb_team_stats("...")
```

### `mlb_statcast_search(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters) -> "'Union[pl.DataFrame, object]'"` {#mlb_statcast_search}

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` |  |
| `chunk_days` | `int` | `7` |  |
| `return_as_pandas` | `bool` | `False` |  |
