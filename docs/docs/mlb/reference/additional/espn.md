---
title: "MLB — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 1
description: "MLB — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — ESPN

### espn_mlb_calendar {#espn_mlb_calendar}

`espn_mlb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_calendar - look up the MLB game-day calendar for a season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `None` | Season year (e.g. 2024). |
| `ondays` | `bool` | `None` | When True, returns the date-only list ESPN exposes; when False, returns a one-row-per-date frame with `eventDate` plus parsed year/month/day. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

Polars dataframe of valid game dates.

| col_name | type | description |
|---|---|---|
| `event_date` | String | Event date-time in ISO 8601 (e.g. '2017-07-11T00:00:00Z'). |
| `year` | Int64 | Season (4-digit year). |
| `month` | Int64 | Month of the game day, as an integer (1-12). |
| `day` | Int64 | Day of the month of the game day, as an integer. |
| `season` | Int64 | Season year. |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_calendar
cal = espn_mlb_calendar(season=2024)
print(cal.shape)
cal.head()
```

### espn_mlb_game_rosters {#espn_mlb_game_rosters}

`espn_mlb_game_rosters(game_id: 'int', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)`

espn_mlb_game_rosters - pull the active game rosters for both teams.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game id. |
| `raw` | `bool` | `False` | When True, returns `{"competitors": [...], "rows": [...]}`: the competitor items and one dict per roster entry with the documented columns, read from the entry only (no athlete fetch; `athlete_id` is the entry's `playerId`, else the id in its athlete `$ref`; it and `game_position_id` are ints). |
| `return_as_pandas` | `bool` | `False` | When True, returns a pandas dataframe; otherwise polars. |

**Returns**

One row per (game × team × athlete) with columns `game_id, team_id, home_away, athlete_id, athlete_full_name, athlete_jersey, athlete_position_id, athlete_position_abbreviation, athlete_position_name, game_position_id, athlete_starter`; zero rows with those columns when ESPN lists no competitor roster entry.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game (event) id the roster belongs to. |
| `team_id` | Int64 | ESPN team id of the competitor whose roster the row comes from. |
| `home_away` | String | Whether the row's team is the home or the away side of the game ("home" / "away"). |
| `athlete_id` | Int64 | ESPN athlete id, from the roster entry's playerId (else the id in its athlete $ref). |
| `athlete_full_name` | String | The athlete's full name, from his ESPN athlete record (the roster entry carries only a short name). |
| `athlete_jersey` | String | The athlete's jersey number as text, from his ESPN athlete record. |
| `athlete_position_id` | Int64 | ESPN position id of the athlete's listed position on his athlete record, i.e. his season position, not necessarily where he played in this game (see game_position_id). |
| `athlete_position_abbreviation` | String | Abbreviation of the athlete's listed (season) position on his athlete record, e.g. RF or SS. |
| `athlete_position_name` | String | Name of the athlete's listed (season) position on his athlete record, e.g. Right Field. |
| `game_position_id` | Int64 | ESPN position id the player entered this game at, from the roster entry's own position $ref (e.g. 8 for a center fielder; 11 is a pinch hitter and 12 a pinch runner). It is the entry position only, not any later move in the game; differs from athlete_position_id when he played out of his listed position. |
| `athlete_starter` | Boolean | Whether the athlete started this game, from the roster entry. |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | A competitors, roster or athlete fetch failed (not a 404). |
| `NoDataError` | The competitors resource answered 404, or roster entries exist but no athlete record could be read (every athlete `$ref` 404'd, or no entry carries one). |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_game_rosters
ros = espn_mlb_game_rosters(game_id=401569461)
print(ros.shape)
ros.group_by("home_away").len()
```

### espn_mlb_pbp {#espn_mlb_pbp}

`espn_mlb_pbp(game_id: 'int', raw: 'bool' = False, **kwargs) -> 'Dict'`

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

### espn_mlb_player_stats {#espn_mlb_player_stats}

`espn_mlb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

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
| `season` | Int64 | Season year. |
| `season_type` | String | Season-type id. |
| `total` | Boolean | Total. |
| `athlete_id` | Int64 | Unique ESPN athlete identifier. |
| `athlete_uid` | String | Athlete uid. |
| `athlete_guid` | String | Athlete guid. |
| `athlete_type` | String | Athlete type. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `weight` | Float64 | Weight in pounds. |
| `display_weight` | String | Display weight. |
| `height` | Float64 | Height (feet and inches). |
| `display_height` | String | Display height. |
| `age` | Int64 | Player age (in years). |
| `date_of_birth` | String | Date of birth. |
| `jersey` | String | Jersey number worn by the player. |
| `slug` | String | URL-safe identifier. |
| `active` | Boolean | Whether the player is currently active. |
| `position_id` | Int64 | Unique position identifier. |
| `position_name` | String | Position name. |
| `position_display_name` | String | Position display name. |
| `position_abbreviation` | String | Position abbreviation. |
| `college_name` | String |  |
| `status_id` | Int64 | Status id. |
| `status_name` | String | Game status (e.g. 'STATUS_FINAL'). |
| `batting_games_played` | Float64 | Team batting: batting games played. |
| `batting_team_games_played` | Float64 | Team batting: batting team games played. |
| `batting_hit_by_pitch` | Float64 | Team batting: batting hit by pitch. |
| `batting_ground_balls` | Float64 | Team batting: batting ground balls. |
| `batting_strikeouts` | Float64 | Team batting: batting strikeouts. |
| `batting_rb_is` | Float64 | Team batting: batting rb is. |
| `batting_sac_hits` | Float64 | Team batting: batting sac hits. |
| `batting_hits` | Float64 | Team batting: batting hits. |
| `batting_stolen_bases` | Float64 | Team batting: batting stolen bases. |
| `batting_walks` | Float64 | Team batting: batting walks. |
| `batting_catcher_interference` | Float64 | Team batting: batting catcher interference. |
| `batting_runs` | Float64 | Team batting: batting runs. |
| `batting_gid_ps` | Float64 | Team batting: batting gid ps. |
| `batting_sac_flies` | Float64 | Team batting: batting sac flies. |
| `batting_at_bats` | Float64 | Team batting: batting at bats. |
| `batting_home_runs` | Float64 | Team batting: batting home runs. |
| `batting_grand_slam_home_runs` | Float64 | Team batting: batting grand slam home runs. |
| `batting_runners_left_on_base` | Float64 | Team batting: batting runners left on base. |
| `batting_triples` | Float64 | Team batting: batting triples. |
| `batting_game_winning_rb_is` | Float64 | Team batting: batting game winning rb is. |
| `batting_intentional_walks` | Float64 | Team batting: batting intentional walks. |
| `batting_doubles` | Float64 | Team batting: batting doubles. |
| `batting_fly_balls` | Float64 | Team batting: batting fly balls. |
| `batting_caught_stealing` | Float64 | Team batting: batting caught stealing. |
| `batting_pitches` | Float64 | Team batting: batting pitches. |
| `batting_games_started` | Float64 | Team batting: batting games started. |
| `batting_pinch_at_bats` | Float64 | Team batting: batting pinch at bats. |
| `batting_pinch_hits` | Float64 | Team batting: batting pinch hits. |
| `batting_player_rating` | Float64 | Team batting: batting player rating. |
| `batting_is_qualified` | Float64 | Team batting: batting is qualified. |
| `batting_is_qualified_steals` | Float64 | Team batting: batting is qualified steals. |
| `batting_total_bases` | Float64 | Team batting: batting total bases. |
| `batting_plate_appearances` | Float64 | Team batting: batting plate appearances. |
| `batting_projected_home_runs` | Float64 | Team batting: batting projected home runs. |
| `batting_extra_base_hits` | Float64 | Team batting: batting extra base hits. |
| `batting_runs_created` | Float64 | Team batting: batting runs created. |
| `batting_avg` | Float64 | Team batting: batting average. |
| `batting_pinch_avg` | Float64 | Team batting: batting pinch avg. |
| `batting_slug_avg` | Float64 | Team batting: batting slug avg. |
| `batting_secondary_avg` | Float64 | Team batting: batting secondary avg. |
| `batting_on_base_pct` | Float64 | Team batting: batting on base pct. |
| `batting_ops` | Float64 | Team batting: batting ops. |
| `batting_ground_to_fly_ratio` | Float64 | Team batting: batting ground to fly ratio. |
| `batting_runs_created_per27_outs` | Float64 | Bill James Runs Created per 27 outs, estimating how many runs a lineup of this batter would score per game. |
| `batting_batter_rating` | Float64 | Team batting: batting batter rating. |
| `batting_at_bats_per_home_run` | Float64 | Team batting: batting at bats per home run. |
| `batting_stolen_base_pct` | Float64 | Team batting: batting stolen base pct. |
| `batting_pitches_per_plate_appearance` | Float64 | Team batting: batting pitches per plate appearance. |
| `batting_isolated_power` | Float64 | Team batting: batting isolated power. |
| `batting_walk_to_strikeout_ratio` | Float64 | Team batting: batting walk to strikeout ratio. |
| `batting_walks_per_plate_appearance` | Float64 | Team batting: batting walks per plate appearance. |
| `batting_secondary_avg_minus_ba` | Float64 | Team batting: batting secondary avg minus ba. |
| `batting_runs_produced` | Float64 | Team batting: batting runs produced. |
| `batting_runs_ratio` | Float64 | Team batting: batting runs ratio. |
| `batting_patience_ratio` | Float64 | Ratio of walks to strikeouts, measuring a batter's plate discipline and ability to work counts. |
| `batting_bipa` | Float64 | Batting average on balls in the air (fly balls and line drives), measuring in-play success on airborne contact. |
| `batting_mlb_rating` | Float64 | ESPN's composite MLB rating for the batter reflecting overall offensive performance. |
| `batting_off_warbr` | Float64 | Offensive Wins Above Replacement (Baseball Reference methodology) attributable to the batter's hitting contributions. |
| `batting_warbr` | Float64 | Total Wins Above Replacement (Baseball Reference methodology) for the batter including offense and baserunning. |
| `fielding_games_played` | Float64 | Number of games in which the player appeared defensively at their position. |
| `fielding_team_games_played` | Float64 | Number of games the player's team played while the player was on the active roster. |
| `fielding_double_plays` | Float64 | Number of double plays the fielder participated in during the season. |
| `fielding_opportunities` | Float64 | Total fielding opportunities defined as putouts plus assists plus errors for the player. |
| `fielding_errors` | Float64 | Number of fielding errors charged to the player during the season. |
| `fielding_passed_balls` | Float64 | Number of pitches ruled as passed balls charged to the catcher during the season. |
| `fielding_assists` | Float64 | Number of assists recorded by the fielder when a thrown ball contributes to an out. |
| `fielding_outfield_assists` | Float64 | Number of outfield assists, recorded when an outfielder throws out a runner. |
| `fielding_pickoffs` | Float64 | Number of baserunners picked off by pitchers while this catcher was behind the plate or this fielder was at their position. |
| `fielding_putouts` | Float64 | Number of putouts recorded by the fielder where they made the final play to retire a batter or runner. |
| `fielding_outs_on_field` | Float64 | Total outs recorded across all innings the player was present on the field. |
| `fielding_triple_plays` | Float64 | Number of triple plays in which the fielder participated during the season. |
| `fielding_balls_in_zone` | Float64 | Number of batted balls that entered the fielder's defined defensive zone. |
| `fielding_extra_bases` | Float64 | Extra bases allowed by the outfielder due to errors or misplays on balls hit into their zone. |
| `fielding_outs_made` | Float64 | Total outs the fielder was directly responsible for recording during the season. |
| `fielding_hits` | Float64 | Number of hits recorded while this fielder was positioned, relevant for zone-rating calculations. |
| `fielding_total_bases` | Float64 | Total bases allowed by the outfielder on balls hit into their zone, used in advanced defensive metrics. |
| `fielding_games_started` | Float64 | Number of games the player started at their primary defensive position. |
| `fielding_catcher_third_innings_played` | Float64 | Total one-third innings played behind the plate by the catcher, expressed in thirds. |
| `fielding_catcher_caught_stealing` | Float64 | Number of opposing baserunners thrown out attempting to steal a base by this catcher. |
| `fielding_catcher_stolen_bases_allowed` | Float64 | Number of successful stolen bases allowed by the catcher during the season. |
| `fielding_catcher_earned_runs` | Float64 | Earned runs allowed while this catcher was behind the plate, used in catcher ERA calculations. |
| `fielding_is_qualified` | Float64 | Indicator flag for whether the player meets minimum innings requirements to qualify for fielding rate stats. |
| `fielding_is_qualified_catcher` | Float64 | Indicator flag for whether the catcher meets the minimum innings threshold to qualify for catcher-specific rate stats. |
| `fielding_is_qualified_pitcher` | Float64 | Indicator flag for whether the pitcher qualifies for pitcher fielding rate statistics. |
| `fielding_successful_chances` | Float64 | Total successful fielding chances defined as putouts plus assists (errors excluded). |
| `fielding_total_chances` | Float64 | Total fielding chances (putouts + assists + errors) offered to the player during the season. |
| `fielding_full_innings_played` | Float64 | Number of complete innings (three outs per side) the player appeared in the field. |
| `fielding_part_innings_played` | Float64 | Number of fractional (partial) innings the player appeared in the field, expressed as a count of thirds. |
| `fielding_fielding_pct` | Float64 | Fielding percentage calculated as successful chances divided by total chances (putouts + assists + errors). |
| `fielding_range_factor` | Float64 | Range factor per nine innings (putouts + assists) / innings * 9, measuring a fielder's defensive range. |
| `fielding_zone_rating` | Float64 | Percentage of batted balls in the fielder's defined zone that were successfully converted into outs. |
| `fielding_catcher_caught_stealing_pct` | Float64 | Percentage of opposing stolen base attempts that resulted in the catcher throwing out the runner. |
| `fielding_catcher_era` | Float64 | ERA of pitchers while this specific catcher was behind the plate over the season. |
| `fielding_def_warbr` | Float64 | Defensive Wins Above Replacement (Baseball Reference methodology) attributable to the player's fielding. |
| `team_id` | Int64 | Unique ESPN team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID). |
| `team_guid` | String | ESPN team GUID. |
| `team_slug` | String | URL-safe team identifier. |
| `team_location` | String | Team city / location. |
| `team_name` | String | Team name. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'NYY'). |
| `team_display_name` | String | Full team display name (e.g. 'New York Yankees'). |
| `team_short_display_name` | String | Short team display name. |
| `team_color` | String | Team primary color (hex, no leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex). |
| `team_is_active` | Boolean | Team is active. |
| `team_logo_href` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season_type` is not `"regular"`/`"postseason"`. |
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `sportsdataverse.errors.AssetFetchError` | The statistics, athlete or team fetch failed (403, 429, 5xx, a dead connection); only a 404 on the athlete or team node leaves those columns null. |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_player_stats
df = espn_mlb_player_stats(athlete_id=33192, season=2023)
df.select(["full_name", "team_display_name", "batting_home_runs"])
```

### espn_mlb_schedule {#espn_mlb_schedule}

`espn_mlb_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_schedule - look up the MLB schedule for a given date or season-year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Date filter. Either a calendar date as YYYYMMDD or a season-year (e.g. 2024). When a 4-digit year is passed, the call returns the full season slate (paginated by `limit`). |
| `season_type` | `int` | `None` | Season type — 1 = spring training, 2 = regular, 3 = postseason, 4 = all-star. |
| `limit` | `int` | `500` | Number of records to return. Default 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule. A day with no games returns a zero-row frame carrying the documented columns.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique ESPN game/event identifier. |
| `date` | String | Date in YYYY-MM-DD format. |
| `season_year` | Int64 |  |
| `season_type` | Int64 | Season-type id. |
| `status_type_state` | String |  |
| `status_type_completed` | Boolean |  |
| `status_type_description` | String |  |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_full_name` | String |  |
| `venue_city` | String | Venue city. |
| `venue_state` | String | Venue state / province. |
| `home_id` | Int64 |  |
| `home_name` | String |  |
| `home_abbreviation` | String |  |
| `home_display_name` | String |  |
| `home_score` | String | Home team score from the ESPN scoreboard: the final score for a completed game, the live score for one in progress (string). |
| `home_winner` | Boolean |  |
| `away_id` | Int64 |  |
| `away_name` | String |  |
| `away_abbreviation` | String |  |
| `away_display_name` | String |  |
| `away_score` | String | Away team score from the ESPN scoreboard: the final score for a completed game, the live score for one in progress (string). |
| `away_winner` | Boolean |  |

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

### espn_mlb_teams {#espn_mlb_teams}

`espn_mlb_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe of the MLB teams, one row per team, with the columns in the returns table. A payload without teams returns a zero-row frame with those columns. The result is not cached: every call fetches (through the package's opt-in TTL response cache, `sportsdataverse.cache`, when it is on).

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. 'NYY'). |
| `team_alternate_color` | String | Team alternate color (hex). |
| `team_color` | String | Team primary color (hex, no leading '#'). |
| `team_display_name` | String | Full team display name (e.g. 'New York Yankees'). |
| `team_id` | Int64 | Unique ESPN team identifier. |
| `team_is_active` | Boolean | Team is active. |
| `team_is_all_star` | Boolean | Team is all star. |
| `team_location` | String | Team city / location. |
| `team_logos` | Int64 |  |
| `team_name` | String | Team name. |
| `team_nickname` | String |  |
| `team_short_display_name` | String | Short team display name. |
| `team_slug` | String | URL-safe team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN answered 404 for the teams resource. |
| `AssetFetchError` | The fetch failed (403, 429, 5xx, a dropped connection); never returned as an empty frame. |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_teams
teams = espn_mlb_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Los Angeles Dodgers (team_id 19)

import polars as pl
teams.filter(pl.col("team_id") == 19).to_dicts()

# Pandas round-trip

teams_pd = espn_mlb_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```
