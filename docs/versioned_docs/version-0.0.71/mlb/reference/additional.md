---
title: MLB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# MLB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mlb`
not covered by the generated API-endpoint reference above.

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
| `batting_runs_created_per27_outs` | double | Bill James Runs Created per 27 outs, estimating how many runs a lineup of this batter would score per game. |
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
| `batting_patience_ratio` | double | Ratio of walks to strikeouts, measuring a batter's plate discipline and ability to work counts. |
| `batting_bipa` | double | Batting average on balls in the air (fly balls and line drives), measuring in-play success on airborne contact. |
| `batting_mlb_rating` | double | ESPN's composite MLB rating for the batter reflecting overall offensive performance. |
| `batting_off_warbr` | double | Offensive Wins Above Replacement (Baseball Reference methodology) attributable to the batter's hitting contributions. |
| `batting_warbr` | double | Total Wins Above Replacement (Baseball Reference methodology) for the batter including offense and baserunning. |
| `fielding_games_played` | double | Number of games in which the player appeared defensively at their position. |
| `fielding_team_games_played` | double | Number of games the player's team played while the player was on the active roster. |
| `fielding_double_plays` | double | Number of double plays the fielder participated in during the season. |
| `fielding_opportunities` | double | Total fielding opportunities defined as putouts plus assists plus errors for the player. |
| `fielding_errors` | double | Number of fielding errors charged to the player during the season. |
| `fielding_passed_balls` | double | Number of pitches ruled as passed balls charged to the catcher during the season. |
| `fielding_assists` | double | Number of assists recorded by the fielder when a thrown ball contributes to an out. |
| `fielding_outfield_assists` | double | Number of outfield assists, recorded when an outfielder throws out a runner. |
| `fielding_pickoffs` | double | Number of baserunners picked off by pitchers while this catcher was behind the plate or this fielder was at their position. |
| `fielding_putouts` | double | Number of putouts recorded by the fielder where they made the final play to retire a batter or runner. |
| `fielding_outs_on_field` | double | Total outs recorded across all innings the player was present on the field. |
| `fielding_triple_plays` | double | Number of triple plays in which the fielder participated during the season. |
| `fielding_balls_in_zone` | double | Number of batted balls that entered the fielder's defined defensive zone. |
| `fielding_extra_bases` | double | Extra bases allowed by the outfielder due to errors or misplays on balls hit into their zone. |
| `fielding_outs_made` | double | Total outs the fielder was directly responsible for recording during the season. |
| `fielding_hits` | double | Number of hits recorded while this fielder was positioned, relevant for zone-rating calculations. |
| `fielding_total_bases` | double | Total bases allowed by the outfielder on balls hit into their zone, used in advanced defensive metrics. |
| `fielding_games_started` | double | Number of games the player started at their primary defensive position. |
| `fielding_catcher_third_innings_played` | double | Total one-third innings played behind the plate by the catcher, expressed in thirds. |
| `fielding_catcher_caught_stealing` | double | Number of opposing baserunners thrown out attempting to steal a base by this catcher. |
| `fielding_catcher_stolen_bases_allowed` | double | Number of successful stolen bases allowed by the catcher during the season. |
| `fielding_catcher_earned_runs` | double | Earned runs allowed while this catcher was behind the plate, used in catcher ERA calculations. |
| `fielding_is_qualified` | double | Indicator flag for whether the player meets minimum innings requirements to qualify for fielding rate stats. |
| `fielding_is_qualified_catcher` | double | Indicator flag for whether the catcher meets the minimum innings threshold to qualify for catcher-specific rate stats. |
| `fielding_is_qualified_pitcher` | double | Indicator flag for whether the pitcher qualifies for pitcher fielding rate statistics. |
| `fielding_successful_chances` | double | Total successful fielding chances defined as putouts plus assists (errors excluded). |
| `fielding_total_chances` | double | Total fielding chances (putouts + assists + errors) offered to the player during the season. |
| `fielding_full_innings_played` | double | Number of complete innings (three outs per side) the player appeared in the field. |
| `fielding_part_innings_played` | double | Number of fractional (partial) innings the player appeared in the field, expressed as a count of thirds. |
| `fielding_fielding_pct` | double | Fielding percentage calculated as successful chances divided by total chances (putouts + assists + errors). |
| `fielding_range_factor` | double | Range factor per nine innings (putouts + assists) / innings * 9, measuring a fielder's defensive range. |
| `fielding_zone_rating` | double | Percentage of batted balls in the fielder's defined zone that were successfully converted into outs. |
| `fielding_catcher_caught_stealing_pct` | double | Percentage of opposing stolen base attempts that resulted in the catcher throwing out the runner. |
| `fielding_catcher_era` | double | ERA of pitchers while this specific catcher was behind the plate over the season. |
| `fielding_def_warbr` | double | Defensive Wins Above Replacement (Baseball Reference methodology) attributable to the player's fielding. |
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

### `mlb_attendance(team_id: 'Optional[int]' = None, league_id: 'Optional[Union[int, str]]' = None, season: 'Optional[Union[int, str]]' = None, league_list_id: 'Optional[str]' = None, game_type: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_attendance}

GET /api/v1/attendance — game attendance figures.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `league_list_id` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |

### `mlb_divisions(sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, division_id: 'Optional[int]' = None, **kwargs) -> 'Dict'` {#mlb_divisions}

GET /api/v1/divisions — list divisions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `division_id` | `Optional[int]` | `None` |  |

### `mlb_draft_prospects(year: 'Union[int, str]', scouting_report: 'Optional[bool]' = None, limit: 'int' = 100, **kwargs) -> 'Dict'` {#mlb_draft_prospects}

GET /api/v1/draft/prospects/{year} — draft prospect list for a year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `scouting_report` | `Optional[bool]` | `None` |  |
| `limit` | `int` | `100` |  |

### `mlb_pbp_diff(game_pk: 'int', start_timecode: 'str', end_timecode: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_pbp_diff}

GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed.

Replays of in-game state for low-bandwidth clients.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `start_timecode` | `str` |  |  |
| `end_timecode` | `Optional[str]` | `None` |  |

### `mlb_pbp_live(game_pk: 'int', language: 'Optional[str]' = None, timecode: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_pbp_live}

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

Top-level keys: `copyright, gamePk, link, metaData, gameData, liveData`.
Includes Statcast metrics where available. The historical name
`mlb_pbp` is preserved as an alias in the generated module.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `language` | `Optional[str]` | `None` |  |
| `timecode` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_person_stats(person_id: 'int', stats: 'str', group: 'str' = 'hitting', season: 'Optional[Union[int, str]]' = None, season_type: 'Optional[str]' = None, sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_person_stats}

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

### `mlb_schedule(date: 'Optional[str]' = None, start_date: 'Optional[str]' = None, end_date: 'Optional[str]' = None, team_id: 'Optional[int]' = None, opponent_id: 'Optional[int]' = None, season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, game_type: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_schedule}

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

### `mlb_seasons(sport_id: 'int' = 1, season: 'Optional[Union[int, str]]' = None, all_seasons: 'bool' = False, **kwargs) -> 'Dict'` {#mlb_seasons}

GET /api/v1/seasons — list of seasons for a sport.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `all_seasons` | `bool` | `False` |  |

### `mlb_standings(league_id: 'Union[int, str, List[int]]' = '103,104', season: 'Optional[Union[int, str]]' = None, date: 'Optional[str]' = None, standings_types: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_standings}

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

### `mlb_statcast_player(player_id: 'int', stats: 'Optional[str]' = None, *, section: 'str' = 'statcast', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame, str]'"` {#mlb_statcast_player}

GET /savant-player/{player_id} and parse one embedded table into a tidy frame.

Returns a tidy frame **by default** (the parsed Statcast page); pass
`raw=True` to get the underlying HTML string instead (the page embeds ~12
other tables you can mine yourself, or feed to
`sportsdataverse.mlb.parse_mlb_statcast_player` with a different
`section`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | MLBAM player id (shared with the Stats API `personId`). |
| `stats` | `Optional[str]` | `None` | optional `stats` query value to scope the embedded payload. |
| `section` | `str` | `'statcast'` | which embedded `serverVals` table to flatten (default `"statcast"`, the seasonal aggregate; e.g. `"statcastGameLogs"`). |
| `raw` | `bool` | `False` | return the raw page HTML string instead of a parsed frame. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of the player's Statcast metrics by default (zero rows when the page/section is absent); the raw HTML `str` when `raw=True`.

| col_name | type | description |
|---|---|---|
| `aggregate` | integer | Aggregate. |
| `year` | integer | Season year. |
| `yearhidden` | integer | Yearhidden. |
| `player_id` | integer | MLBAM player id. |
| `age` | integer | Player age. |
| `bat_side` | character | Batter side (R/L/S). |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `month` | character | Month. |
| `grouping_code` | character | Grouping code. |
| `grouping_cat` | character | Grouping cat. |
| `pitch_count` | integer | Pitch count. |
| `in_zone_percent` | numeric | In zone rate. |
| `out_zone_percent` | numeric | Out zone rate. |
| `edge_percent` | numeric | Edge rate. |
| `z_swing_percent` | numeric | Z swing rate. |
| `oz_swing_percent` | numeric | Oz swing rate. |
| `iz_contact_percent` | numeric | Iz contact rate. |
| `oz_contact_percent` | numeric | Oz contact rate. |
| `whiff_percent` | numeric | Whiff rate (swings and misses / swings). |
| `f_strike_percent` | numeric | F strike rate. |
| `f_swing_percent` | numeric | F swing rate. |
| `swing_percent` | numeric | Swing rate. |
| `meatball_swing_percent` | integer | Meatball swing rate. |
| `meatball_percent` | numeric | Meatball rate. |
| `z_swing_miss_percent` | numeric | Z swing miss rate. |
| `oz_swing_miss_percent` | numeric | Oz swing miss rate. |
| `in_zone` | integer | In zone. |
| `out_zone` | integer | Out zone. |
| `edge` | integer | Edge. |
| `popups` | integer | Popups. |
| `flyballs` | integer | Flyballs. |
| `linedrives` | integer | Linedrives. |
| `groundballs` | integer | Groundballs. |
| `airballs` | integer | Airballs. |
| `popups_percent` | numeric | Popups rate. |
| `flyballs_percent` | numeric | Flyballs rate. |
| `linedrives_percent` | numeric | Linedrives rate. |
| `groundballs_percent` | numeric | Groundballs rate. |
| `airballs_percent` | numeric | Airballs rate. |
| `pull_percent` | numeric | Pull rate. |
| `straightaway_percent` | numeric | Straightaway rate. |
| `opposite_percent` | numeric | Opposite rate. |
| `pull_percent_airballs` | numeric | Pull percent airballs. |
| `straightaway_percent_airballs` | numeric | Straightaway percent airballs. |
| `opposite_percent_airballs` | integer | Opposite percent airballs. |
| `pull_percent_groundballs` | numeric | Pull percent groundballs. |
| `straightaway_percent_groundballs` | numeric | Straightaway percent groundballs. |
| `opposite_percent_groundballs` | numeric | Opposite percent groundballs. |
| `pull_percent_popups` | numeric | Pull percent popups. |
| `straightaway_percent_popups` | numeric | Straightaway percent popups. |
| `opposite_percent_popups` | numeric | Opposite percent popups. |
| `pull_percent_flyballs` | numeric | Pull percent flyballs. |
| `straightaway_percent_flyballs` | numeric | Straightaway percent flyballs. |
| `opposite_percent_flyballs` | numeric | Opposite percent flyballs. |
| `pull_percent_linedrives` | integer | Pull percent linedrives. |
| `straightaway_percent_linedrives` | integer | Straightaway percent linedrives. |
| `opposite_percent_linedrives` | integer | Opposite percent linedrives. |
| `poorlyweak_percent` | integer | Poorlyweak rate. |
| `poorlytopped_percent` | numeric | Poorlytopped rate. |
| `poorlyunder_percent` | numeric | Poorlyunder rate. |
| `flareburner_percent` | numeric | Flareburner rate. |
| `solidcontact_percent` | numeric | Solidcontact rate. |
| `hr_flyballs_percent` | numeric | Hr flyballs rate. |
| `in_zone_swing` | integer | In zone swing. |
| `out_zone_swing` | integer | Out zone swing. |
| `in_zone_swing_miss` | integer | In zone swing miss. |
| `out_zone_swing_miss` | integer | Out zone swing miss. |
| `pitch_count_fastball` | integer | Pitch count fastball. |
| `pitch_count_offspeed` | integer | Pitch count offspeed. |
| `pitch_count_breaking` | integer | Pitch count breaking. |
| `pa` | integer | Plate appearances. |
| `ab` | integer | At-bats. |
| `hit` | integer | Hit. |
| `single` | integer | Singles. |
| `double` | integer | Doubles. |
| `triple` | integer | Triples. |
| `home_run` | integer | Home run. |
| `walk` | integer | Walk. |
| `strikeout` | integer | Strikeout. |
| `hbp` | integer | Hbp. |
| `k_percent` | numeric | Strikeout rate. |
| `bb_percent` | numeric | Walk rate. |
| `sz_judge` | numeric | Sz judge. |
| `batted_ball` | integer | Batted ball. |
| `barrel` | integer | Barrel. |
| `barrel_batted_rate` | numeric | Barrels per batted ball. |
| `barrels_per_pa` | numeric | Barrels per pa. |
| `launch_angle_avg` | numeric | Launch angle avg. |
| `exit_velocity_avg` | numeric | Exit velocity avg. |
| `exit_velocity_max` | numeric | Exit velocity max. |
| `hard_hit_percent` | numeric | Hard-hit rate (95+ mph EV). |
| `sweet_spot_percent` | numeric | Sweet-spot rate (8-32 deg launch angle). |
| `ba` | numeric | Batting average. |
| `xba` | numeric | Expected batting average. |
| `bacon` | numeric | Bacon. |
| `xbacon` | numeric | Expected batting average on contact. |
| `babip` | numeric | BABIP. |
| `obp` | numeric | On-base percentage. |
| `slg` | numeric | Slugging percentage. |
| `xobp` | numeric | Expected on-base percentage. |
| `xslg` | numeric | Expected slugging. |
| `iso` | numeric | Isolated power. |
| `xiso` | numeric | Expected isolated power. |
| `woba` | numeric | Weighted on-base average. |
| `xwoba` | numeric | Expected wOBA. |
| `wobacon` | numeric | Wobacon. |
| `xwobacon` | numeric | Xwobacon. |
| `xbadiff` | numeric | Xbadiff. |
| `xslgdiff` | numeric | Xslgdiff. |
| `wobadiff` | numeric | Wobadiff. |
| `player_type` | character | Player type. |
| `era` | character | Era. |
| `xera` | character | Expected ERA. |
| `avg_hyper_speed` | numeric | Avg hyper speed. |
| `avg_best_speed` | numeric | Avg best speed. |
| `distance_hr_avg` | integer | Distance hr avg. |
| `sprint_speed` | numeric | Sprint speed (ft/sec, top 50% of competitive runs). |
| `pop_2b` | character | Pop 2b. |
| `arm_cs_2b` | character | Arm cs 2b. |
| `strike_rate` | character | Called-strike rate. |
| `outs_above_average` | integer | Outs Above Average. |
| `jump_v_avg` | integer | Jump v avg. |
| `max_arm_strength` | character | Max arm strength (mph). |
| `arm_overall` | character | Arm overall. |
| `xhr` | numeric | Xhr. |
| `swing_take_run_value` | integer | Swing take run value. |
| `blocks_above_average` | character | Blocks above average. |
| `cs_above_average` | character | Cs above average. |
| `fastball_velo` | character | Fastball velo. |
| `fastball_spin` | character | Fastball spin. |
| `fastball_extension` | character | Fastball extension. |
| `curveball_spin` | character | Curveball spin. |
| `pitch_run_value_fastball` | numeric | Pitch run value fastball. |
| `pitch_run_value_breaking` | numeric | Pitch run value breaking. |
| `pitch_run_value_offspeed` | numeric | Pitch run value offspeed. |
| `group_fastball_velo` | numeric | Group fastball velo. |
| `group_breaking_velo` | numeric | Group breaking velo. |
| `group_offspeed_velo` | numeric | Group offspeed velo. |
| `pitch_usage_fastball` | numeric | Pitch usage fastball. |
| `pitch_usage_breaking` | numeric | Pitch usage breaking. |
| `pitch_usage_offspeed` | numeric | Pitch usage offspeed. |
| `fielding_run_value` | integer | Fielding run value. |
| `runner_run_value` | integer | Runner run value. |
| `fielding_run_value_arm` | integer | Fielding run value arm. |
| `fielding_run_value_framing` | character | Fielding run value framing. |
| `runner_runs_sb` | integer | Runner runs sb. |
| `runner_runs_xb` | integer | Runner runs xb. |
| `net_bases_runner` | integer | Net bases runner. |
| `net_bases_pitcher` | character | Net bases pitcher. |
| `fast_swing_rate` | character | Fast-swing rate (>=75 mph). |
| `squared_up_contact` | character | Squared up contact. |
| `squared_up_swing` | character | Squared up swing. |
| `blasts_contact` | character | Blasts contact. |
| `blasts_swing` | character | Blasts swing. |
| `swords` | character | Swords. |
| `avg_swing_speed` | character | Avg swing speed. |
| `avg_swing_length` | character | Avg swing length. |
| `attack_angle` | character | Attack angle (deg, bat path at contact). |
| `vertical_swing_path` | character | Vertical swing path. |
| `acceleration` | character | Acceleration. |
| `horizontal_swing_path` | character | Horizontal swing path. |
| `attack_direction` | character | Attack direction (deg, pull/oppo). |
| `ideal_angle_rate` | character | Ideal angle rate. |
| `n_squared_up` | character | Number of squared up. |
| `n_blasts` | character | Number of blasts. |
| `arm_angle` | character | Arm angle. |
| `is_qualified` | integer | Is qualified. |
| `percent_rank_barrel_unrounded` | character | Percent rank barrel unrounded. |
| `percent_rank_barrel_batted_rate_unrounded` | character | Percent rank barrel batted rate unrounded. |
| `percent_rank_exit_velocity_avg_unrounded` | character | Percent rank exit velocity avg unrounded. |
| `percent_rank_exit_velocity_max_unrounded` | numeric | Percent rank exit velocity max unrounded. |
| `percent_rank_launch_angle_avg_unrounded` | character | Percent rank launch angle avg unrounded. |
| `percent_rank_xba_unrounded` | character | Percent rank xba unrounded. |
| `percent_rank_xslg_unrounded` | character | Percent rank xslg unrounded. |
| `percent_rank_xwoba_unrounded` | character | Percent rank xwoba unrounded. |
| `percent_rank_woba_unrounded` | character | Percent rank woba unrounded. |
| `percent_rank_hard_hit_percent_unrounded` | character | Percent rank hard hit percent unrounded. |
| `percent_rank_xwobacon_unrounded` | character | Percent rank xwobacon unrounded. |
| `percent_rank_wobacon_unrounded` | character | Percent rank wobacon unrounded. |
| `percent_rank_k_percent_unrounded` | character | Percent rank k percent unrounded. |
| `percent_rank_bb_percent_unrounded` | character | Percent rank bb percent unrounded. |
| `percent_rank_sz_judge_unrounded` | character | Percent rank sz judge unrounded. |
| `percent_rank_whiff_percent_unrounded` | character | Percent rank whiff percent unrounded. |
| `percent_rank_chase_percent_unrounded` | character | Percent rank chase percent unrounded. |
| `percent_rank_ba_unrounded` | character | Percent rank ba unrounded. |
| `percent_rank_bacon_unrounded` | character | Percent rank bacon unrounded. |
| `percent_rank_xbacon_unrounded` | character | Percent rank xbacon unrounded. |
| `percent_rank_babip_unrounded` | character | Percent rank babip unrounded. |
| `percent_rank_obp_unrounded` | character | Percent rank obp unrounded. |
| `percent_rank_slg_unrounded` | character | Percent rank slg unrounded. |
| `percent_rank_xobp_unrounded` | character | Percent rank xobp unrounded. |
| `percent_rank_iso_unrounded` | character | Percent rank iso unrounded. |
| `percent_rank_xiso_unrounded` | character | Percent rank xiso unrounded. |
| `percent_rank_sweet_spot_percent_unrounded` | character | Percent rank sweet spot percent unrounded. |
| `percent_rank_distance_hr_avg_unrounded` | character | Percent rank distance hr avg unrounded. |
| `percent_rank_groundballs_percent_unrounded` | character | Percent rank groundballs percent unrounded. |
| `percent_rank_airballs_percent_unrounded` | character | Percent rank airballs percent unrounded. |
| `percent_rank_avg_hyper_speed_unrounded` | character | Percent rank avg hyper speed unrounded. |
| `percent_rank_avg_best_speed_unrounded` | character | Percent rank avg best speed unrounded. |
| `percent_rank_pitch_run_value_fastball_unrounded` | character | Percent rank pitch run value fastball unrounded. |
| `percent_rank_pitch_run_value_breaking_unrounded` | character | Percent rank pitch run value breaking unrounded. |
| `percent_rank_pitch_run_value_offspeed_unrounded` | character | Percent rank pitch run value offspeed unrounded. |
| `percent_rank_barrel` | character | Percent rank barrel. |
| `percent_rank_barrel_batted_rate` | character | Percent rank barrel batted rate. |
| `percent_rank_exit_velocity_avg` | character | Percent rank exit velocity avg. |
| `percent_rank_exit_velocity_max` | integer | Percent rank exit velocity max. |
| `percent_rank_launch_angle_avg` | character | Percent rank launch angle avg. |
| `percent_rank_xba` | character | Percent rank xba. |
| `percent_rank_xslg` | character | Percent rank xslg. |
| `percent_rank_xwoba` | character | Percent rank xwoba. |
| `percent_rank_woba` | character | Percent rank woba. |
| `percent_rank_hard_hit_percent` | character | Percent rank hard hit rate. |
| `percent_rank_xwobacon` | character | Percent rank xwobacon. |
| `percent_rank_wobacon` | character | Percent rank wobacon. |
| `percent_rank_k_percent` | character | Percent rank k rate. |
| `percent_rank_bb_percent` | character | Percent rank bb rate. |
| `percent_rank_sz_judge` | character | Percent rank sz judge. |
| `percent_rank_whiff_percent` | character | Percent rank whiff rate. |
| `percent_rank_chase_percent` | character | Percent rank chase rate. |
| `percent_rank_ba` | character | Percent rank ba. |
| `percent_rank_bacon` | character | Percent rank bacon. |
| `percent_rank_xbacon` | character | Percent rank xbacon. |
| `percent_rank_babip` | character | Percent rank babip. |
| `percent_rank_obp` | character | Percent rank obp. |
| `percent_rank_slg` | character | Percent rank slg. |
| `percent_rank_xobp` | character | Percent rank xobp. |
| `percent_rank_iso` | character | Percent rank iso. |
| `percent_rank_xiso` | character | Percent rank xiso. |
| `percent_rank_sweet_spot_percent` | character | Percent rank sweet spot rate. |
| `percent_rank_distance_hr_avg` | character | Percent rank distance hr avg. |
| `percent_rank_groundballs_percent` | character | Percent rank groundballs rate. |
| `percent_rank_airballs_percent` | character | Percent rank airballs rate. |
| `percent_rank_avg_hyper_speed` | character | Percent rank avg hyper speed. |
| `percent_rank_avg_best_speed` | character | Percent rank avg best speed. |
| `percent_rank_pitch_run_value_fastball` | character | Percent rank pitch run value fastball. |
| `percent_rank_pitch_run_value_breaking` | character | Percent rank pitch run value breaking. |
| `percent_rank_pitch_run_value_offspeed` | character | Percent rank pitch run value offspeed. |
| `percent_speed_order` | integer | Percent speed order. |
| `percent_rank_speed_order` | integer | Percent rank speed order. |
| `percent_rank_pop_2b` | character | Percent rank pop 2b. |
| `percent_rank_arm_cs_2b` | character | Percent rank arm cs 2b. |
| `percent_rank_oaa` | character | Percent rank oaa. |
| `percent_rank_framing` | character | Percent rank framing. |
| `percent_rank_jump` | character | Percent rank jump. |
| `percent_rank_fastball_velo` | character | Percent rank fastball velo. |
| `percent_rank_fastball_spin` | character | Percent rank fastball spin. |
| `percent_rank_fastball_extension` | character | Percent rank fastball extension. |
| `percent_rank_cu_spin` | character | Percent rank cu spin. |
| `percent_rank_xera` | character | Percent rank xera. |
| `percent_rank_arm_max` | character | Percent rank arm max. |
| `percent_rank_arm_overall` | character | Percent rank arm overall. |
| `percent_rank_xhr` | character | Percent rank xhr. |
| `percent_rank_swing_take_run_value` | character | Percent rank swing take run value. |
| `percent_rank_blocks_above_average` | character | Percent rank blocks above average. |
| `percent_rank_cs_above_average` | character | Percent rank cs above average. |
| `percent_rank_fielding_run_value` | character | Percent rank fielding run value. |
| `percent_rank_runner_run_value` | character | Percent rank runner run value. |
| `percent_rank_fielding_run_value_arm` | character | Percent rank fielding run value arm. |
| `percent_rank_fielding_run_value_framing` | character | Percent rank fielding run value framing. |
| `percent_rank_swing_speed` | character | Percent rank swing speed. |
| `percent_rank_swing_length` | character | Percent rank swing length. |
| `percent_rank_squared_up_swing` | character | Percent rank squared up swing. |
| `percent_rank_attack_angle` | character | Percent rank attack angle. |
| `percent_rank_vertical_swing_path` | character | Percent rank vertical swing path. |
| `percent_rank_acceleration` | character | Percent rank acceleration. |
| `percent_rank_ideal_angle_rate` | character | Percent rank ideal angle rate. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_player
df = mlb_statcast_player(592450)
html = mlb_statcast_player(592450, raw=True)
```

### `mlb_statcast_search(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_statcast_search}

Pitch-by-pitch MLB Statcast search (`/statcast_search/csv`), date-chunked.

Savant caps a single `/statcast_search/csv` response at **25,000 rows with
no pagination**. This splits the date range into `chunk_days` windows,
halving any window that hits the cap, and stitches the chunks back together.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` | `"batter"` (default) or `"pitcher"`. |
| `chunk_days` | `int` | `7` | initial window size in days. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per pitch.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character | Pitch type code. |
| `game_date` | character | Game date. |
| `release_speed` | numeric | Release speed. |
| `release_pos_x` | numeric | Release pos x. |
| `release_pos_z` | numeric | Release pos z. |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM id of the batter. |
| `pitcher` | integer | MLBAM id of the pitcher. |
| `events` | character | Events. |
| `description` | character | Description. |
| `spin_dir` | character | Spin dir. |
| `spin_rate_deprecated` | character | Spin rate deprecated. |
| `break_angle_deprecated` | character | Break angle deprecated. |
| `break_length_deprecated` | character | Break length deprecated. |
| `zone` | integer | Zone. |
| `des` | character | Des. |
| `game_type` | character | Game type. |
| `stand` | character | Batter stance side (R/L). |
| `p_throws` | character | Pitcher throwing hand (R/L). |
| `home_team` | character | Home team. |
| `away_team` | character | Away team. |
| `type` | character | Record/pitch type. |
| `hit_location` | integer | Hit location. |
| `bb_type` | character | Bb type. |
| `balls` | integer | Balls. |
| `strikes` | integer | Strikes. |
| `game_year` | integer | Game year. |
| `pfx_x` | numeric | Horizontal movement (in, pitcher perspective). |
| `pfx_z` | numeric | Induced vertical movement (in). |
| `plate_x` | numeric | Plate x. |
| `plate_z` | numeric | Plate z. |
| `on_3b` | character | On 3b. |
| `on_2b` | character | On 2b. |
| `on_1b` | character | On 1b. |
| `outs_when_up` | integer | Outs when up. |
| `inning` | integer | Inning. |
| `inning_topbot` | character | Inning topbot. |
| `hc_x` | numeric | Hc x. |
| `hc_y` | numeric | Hc y. |
| `tfs_deprecated` | character | Tfs deprecated. |
| `tfs_zulu_deprecated` | character | Tfs zulu deprecated. |
| `umpire` | character | Umpire. |
| `sv_id` | character | Sv id. |
| `vx0` | numeric | Vx0. |
| `vy0` | numeric | Vy0. |
| `vz0` | numeric | Vz0. |
| `ax` | numeric | Ax. |
| `ay` | numeric | Ay. |
| `az` | numeric | Az. |
| `sz_top` | numeric | Sz top. |
| `sz_bot` | numeric | Sz bot. |
| `hit_distance_sc` | integer | Hit distance sc. |
| `launch_speed` | numeric | Exit velocity of the batted ball (mph). |
| `launch_angle` | integer | Launch angle (deg). |
| `effective_speed` | integer | Effective speed. |
| `release_spin_rate` | integer | Release spin rate. |
| `release_extension` | numeric | Release extension. |
| `game_pk` | integer | MLBAM game id. |
| `fielder_2` | integer | Fielder 2. |
| `fielder_3` | integer | Fielder 3. |
| `fielder_4` | integer | Fielder 4. |
| `fielder_5` | integer | Fielder 5. |
| `fielder_6` | integer | Fielder 6. |
| `fielder_7` | integer | Fielder 7. |
| `fielder_8` | integer | Fielder 8. |
| `fielder_9` | integer | Fielder 9. |
| `release_pos_y` | numeric | Release pos y. |
| `estimated_ba_using_speedangle` | numeric | Estimated ba using speedangle. |
| `estimated_woba_using_speedangle` | numeric | Estimated woba using speedangle. |
| `woba_value` | integer | Woba value. |
| `woba_denom` | integer | Woba denom. |
| `babip_value` | integer | Babip value. |
| `iso_value` | integer | Iso value. |
| `launch_speed_angle` | integer | Launch speed angle. |
| `at_bat_number` | integer | At bat number. |
| `pitch_number` | integer | Pitch number. |
| `pitch_name` | character | Pitch type name. |
| `home_score` | integer | Home score. |
| `away_score` | integer | Away score. |
| `bat_score` | integer | Bat score. |
| `fld_score` | integer | Fld score. |
| `post_away_score` | integer | Post away score. |
| `post_home_score` | integer | Post home score. |
| `post_bat_score` | integer | Post bat score. |
| `post_fld_score` | integer | Post fld score. |
| `if_fielding_alignment` | character | If fielding alignment. |
| `of_fielding_alignment` | character | Of fielding alignment. |
| `spin_axis` | integer | Spin axis. |
| `delta_home_win_exp` | numeric | Delta home win exp. |
| `delta_run_exp` | numeric | Delta run exp. |
| `bat_speed` | numeric | Bat speed (mph). |
| `swing_length` | numeric | Swing length (ft, head travel). |
| `miss_distance` | character | Average miss distance (in) on swings. |
| `estimated_slg_using_speedangle` | numeric | Estimated slg using speedangle. |
| `delta_pitcher_run_exp` | numeric | Delta pitcher run exp. |
| `hyper_speed` | numeric | Hyper speed. |
| `home_score_diff` | integer | Home score diff. |
| `bat_score_diff` | integer | Bat score diff. |
| `home_win_exp` | numeric | Home win exp. |
| `bat_win_exp` | numeric | Bat win exp. |
| `age_pit_legacy` | integer | Age pit legacy. |
| `age_bat_legacy` | integer | Age bat legacy. |
| `age_pit` | integer | Age pit. |
| `age_bat` | integer | Age bat. |
| `n_thruorder_pitcher` | integer | Number of thruorder pitcher. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of priorpa thisgame player at bat. |
| `pitcher_days_since_prev_game` | integer | Pitcher days since prev game. |
| `batter_days_since_prev_game` | integer | Batter days since prev game. |
| `pitcher_days_until_next_game` | integer | Pitcher days until next game. |
| `batter_days_until_next_game` | integer | Batter days until next game. |
| `api_break_z_with_gravity` | numeric | Api break z with gravity. |
| `api_break_x_arm` | numeric | Api break x arm. |
| `api_break_x_batter_in` | numeric | Api break x batter in. |
| `arm_angle` | numeric | Arm angle. |
| `attack_angle` | numeric | Attack angle (deg, bat path at contact). |
| `attack_direction` | numeric | Attack direction (deg, pull/oppo). |
| `swing_path_tilt` | numeric | Swing-path tilt (deg). |
| `intercept_ball_minus_batter_pos_x_inches` | numeric | Intercept ball minus batter pos x inches. |
| `intercept_ball_minus_batter_pos_y_inches` | numeric | Intercept ball minus batter pos y inches. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search
df = mlb_statcast_search("2024-06-15", "2024-06-16", batters_lookup=592450)
```

### `mlb_statcast_search_minors(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_statcast_search_minors}

Minor-league Statcast search (`/statcast-search-minors/csv`), date-chunked.

Same shape, columns, and 25,000-row chunking as `mlb_statcast_search`,
but against the MiLB CSV route. Scope with `hfLevel` (Triple-A/Double-A/…)
and `hfSea` filters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` | `"batter"` (default) or `"pitcher"`. |
| `chunk_days` | `int` | `7` | initial window size in days. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per minor-league pitch.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character | Pitch type code. |
| `game_date` | character | Game date. |
| `release_speed` | numeric | Release speed. |
| `release_pos_x` | numeric | Release pos x. |
| `release_pos_z` | numeric | Release pos z. |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM id of the batter. |
| `pitcher` | integer | MLBAM id of the pitcher. |
| `events` | character | Events. |
| `description` | character | Description. |
| `spin_dir` | character | Spin dir. |
| `spin_rate_deprecated` | character | Spin rate deprecated. |
| `break_angle_deprecated` | character | Break angle deprecated. |
| `break_length_deprecated` | character | Break length deprecated. |
| `zone` | integer | Zone. |
| `des` | character | Des. |
| `game_type` | character | Game type. |
| `stand` | character | Batter stance side (R/L). |
| `p_throws` | character | Pitcher throwing hand (R/L). |
| `home_team` | character | Home team. |
| `away_team` | character | Away team. |
| `type` | character | Record/pitch type. |
| `hit_location` | integer | Hit location. |
| `bb_type` | character | Bb type. |
| `balls` | integer | Balls. |
| `strikes` | integer | Strikes. |
| `game_year` | integer | Game year. |
| `pfx_x` | numeric | Horizontal movement (in, pitcher perspective). |
| `pfx_z` | numeric | Induced vertical movement (in). |
| `plate_x` | numeric | Plate x. |
| `plate_z` | numeric | Plate z. |
| `on_3b` | character | On 3b. |
| `on_2b` | character | On 2b. |
| `on_1b` | character | On 1b. |
| `outs_when_up` | integer | Outs when up. |
| `inning` | integer | Inning. |
| `inning_topbot` | character | Inning topbot. |
| `hc_x` | numeric | Hc x. |
| `hc_y` | numeric | Hc y. |
| `tfs_deprecated` | character | Tfs deprecated. |
| `tfs_zulu_deprecated` | character | Tfs zulu deprecated. |
| `umpire` | character | Umpire. |
| `sv_id` | character | Sv id. |
| `vx0` | numeric | Vx0. |
| `vy0` | numeric | Vy0. |
| `vz0` | numeric | Vz0. |
| `ax` | numeric | Ax. |
| `ay` | numeric | Ay. |
| `az` | numeric | Az. |
| `sz_top` | numeric | Sz top. |
| `sz_bot` | numeric | Sz bot. |
| `hit_distance_sc` | integer | Hit distance sc. |
| `launch_speed` | numeric | Exit velocity of the batted ball (mph). |
| `launch_angle` | integer | Launch angle (deg). |
| `effective_speed` | integer | Effective speed. |
| `release_spin_rate` | integer | Release spin rate. |
| `release_extension` | numeric | Release extension. |
| `game_pk` | integer | MLBAM game id. |
| `fielder_2` | integer | Fielder 2. |
| `fielder_3` | integer | Fielder 3. |
| `fielder_4` | integer | Fielder 4. |
| `fielder_5` | integer | Fielder 5. |
| `fielder_6` | integer | Fielder 6. |
| `fielder_7` | integer | Fielder 7. |
| `fielder_8` | integer | Fielder 8. |
| `fielder_9` | integer | Fielder 9. |
| `release_pos_y` | numeric | Release pos y. |
| `estimated_ba_using_speedangle` | numeric | Estimated ba using speedangle. |
| `estimated_woba_using_speedangle` | numeric | Estimated woba using speedangle. |
| `woba_value` | integer | Woba value. |
| `woba_denom` | integer | Woba denom. |
| `babip_value` | integer | Babip value. |
| `iso_value` | integer | Iso value. |
| `launch_speed_angle` | integer | Launch speed angle. |
| `at_bat_number` | integer | At bat number. |
| `pitch_number` | integer | Pitch number. |
| `pitch_name` | character | Pitch type name. |
| `home_score` | integer | Home score. |
| `away_score` | integer | Away score. |
| `bat_score` | integer | Bat score. |
| `fld_score` | integer | Fld score. |
| `post_away_score` | integer | Post away score. |
| `post_home_score` | integer | Post home score. |
| `post_bat_score` | integer | Post bat score. |
| `post_fld_score` | integer | Post fld score. |
| `if_fielding_alignment` | character | If fielding alignment. |
| `of_fielding_alignment` | character | Of fielding alignment. |
| `spin_axis` | integer | Spin axis. |
| `delta_home_win_exp` | numeric | Delta home win exp. |
| `delta_run_exp` | numeric | Delta run exp. |
| `bat_speed` | numeric | Bat speed (mph). |
| `swing_length` | numeric | Swing length (ft, head travel). |
| `miss_distance` | character | Average miss distance (in) on swings. |
| `estimated_slg_using_speedangle` | numeric | Estimated slg using speedangle. |
| `delta_pitcher_run_exp` | numeric | Delta pitcher run exp. |
| `hyper_speed` | numeric | Hyper speed. |
| `home_score_diff` | integer | Home score diff. |
| `bat_score_diff` | integer | Bat score diff. |
| `home_win_exp` | numeric | Home win exp. |
| `bat_win_exp` | numeric | Bat win exp. |
| `age_pit_legacy` | integer | Age pit legacy. |
| `age_bat_legacy` | integer | Age bat legacy. |
| `age_pit` | integer | Age pit. |
| `age_bat` | integer | Age bat. |
| `n_thruorder_pitcher` | integer | Number of thruorder pitcher. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of priorpa thisgame player at bat. |
| `pitcher_days_since_prev_game` | integer | Pitcher days since prev game. |
| `batter_days_since_prev_game` | integer | Batter days since prev game. |
| `pitcher_days_until_next_game` | integer | Pitcher days until next game. |
| `batter_days_until_next_game` | integer | Batter days until next game. |
| `api_break_z_with_gravity` | numeric | Api break z with gravity. |
| `api_break_x_arm` | numeric | Api break x arm. |
| `api_break_x_batter_in` | numeric | Api break x batter in. |
| `arm_angle` | numeric | Arm angle. |
| `attack_angle` | numeric | Attack angle (deg, bat path at contact). |
| `attack_direction` | numeric | Attack direction (deg, pull/oppo). |
| `swing_path_tilt` | numeric | Swing-path tilt (deg). |
| `intercept_ball_minus_batter_pos_x_inches` | numeric | Intercept ball minus batter pos x inches. |
| `intercept_ball_minus_batter_pos_y_inches` | numeric | Intercept ball minus batter pos y inches. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search_minors
df = mlb_statcast_search_minors("2024-06-01", "2024-06-02")
```

### `mlb_statcast_search_wbc(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_statcast_search_wbc}

World Baseball Classic Statcast search (`/statcast-search-world-baseball-classic/csv`).

Same shape, columns, and 25,000-row chunking as `mlb_statcast_search`,
against the WBC CSV route. Pass WBC date windows (e.g. March of a WBC year).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` | `"batter"` (default) or `"pitcher"`. |
| `chunk_days` | `int` | `7` | initial window size in days. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per WBC pitch.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character | Pitch type code. |
| `game_date` | character | Game date. |
| `release_speed` | numeric | Release speed. |
| `release_pos_x` | numeric | Release pos x. |
| `release_pos_z` | numeric | Release pos z. |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM id of the batter. |
| `pitcher` | integer | MLBAM id of the pitcher. |
| `events` | character | Events. |
| `description` | character | Description. |
| `spin_dir` | character | Spin dir. |
| `spin_rate_deprecated` | character | Spin rate deprecated. |
| `break_angle_deprecated` | character | Break angle deprecated. |
| `break_length_deprecated` | character | Break length deprecated. |
| `zone` | integer | Zone. |
| `des` | character | Des. |
| `game_type` | character | Game type. |
| `stand` | character | Batter stance side (R/L). |
| `p_throws` | character | Pitcher throwing hand (R/L). |
| `home_team` | character | Home team. |
| `away_team` | character | Away team. |
| `type` | character | Record/pitch type. |
| `hit_location` | integer | Hit location. |
| `bb_type` | character | Bb type. |
| `balls` | integer | Balls. |
| `strikes` | integer | Strikes. |
| `game_year` | integer | Game year. |
| `pfx_x` | numeric | Horizontal movement (in, pitcher perspective). |
| `pfx_z` | numeric | Induced vertical movement (in). |
| `plate_x` | numeric | Plate x. |
| `plate_z` | numeric | Plate z. |
| `on_3b` | character | On 3b. |
| `on_2b` | character | On 2b. |
| `on_1b` | character | On 1b. |
| `outs_when_up` | integer | Outs when up. |
| `inning` | integer | Inning. |
| `inning_topbot` | character | Inning topbot. |
| `hc_x` | numeric | Hc x. |
| `hc_y` | numeric | Hc y. |
| `tfs_deprecated` | character | Tfs deprecated. |
| `tfs_zulu_deprecated` | character | Tfs zulu deprecated. |
| `umpire` | character | Umpire. |
| `sv_id` | character | Sv id. |
| `vx0` | numeric | Vx0. |
| `vy0` | numeric | Vy0. |
| `vz0` | numeric | Vz0. |
| `ax` | numeric | Ax. |
| `ay` | numeric | Ay. |
| `az` | numeric | Az. |
| `sz_top` | numeric | Sz top. |
| `sz_bot` | numeric | Sz bot. |
| `hit_distance_sc` | integer | Hit distance sc. |
| `launch_speed` | numeric | Exit velocity of the batted ball (mph). |
| `launch_angle` | integer | Launch angle (deg). |
| `effective_speed` | integer | Effective speed. |
| `release_spin_rate` | integer | Release spin rate. |
| `release_extension` | numeric | Release extension. |
| `game_pk` | integer | MLBAM game id. |
| `fielder_2` | integer | Fielder 2. |
| `fielder_3` | integer | Fielder 3. |
| `fielder_4` | integer | Fielder 4. |
| `fielder_5` | integer | Fielder 5. |
| `fielder_6` | integer | Fielder 6. |
| `fielder_7` | integer | Fielder 7. |
| `fielder_8` | integer | Fielder 8. |
| `fielder_9` | integer | Fielder 9. |
| `release_pos_y` | numeric | Release pos y. |
| `estimated_ba_using_speedangle` | numeric | Estimated ba using speedangle. |
| `estimated_woba_using_speedangle` | numeric | Estimated woba using speedangle. |
| `woba_value` | integer | Woba value. |
| `woba_denom` | integer | Woba denom. |
| `babip_value` | integer | Babip value. |
| `iso_value` | integer | Iso value. |
| `launch_speed_angle` | integer | Launch speed angle. |
| `at_bat_number` | integer | At bat number. |
| `pitch_number` | integer | Pitch number. |
| `pitch_name` | character | Pitch type name. |
| `home_score` | integer | Home score. |
| `away_score` | integer | Away score. |
| `bat_score` | integer | Bat score. |
| `fld_score` | integer | Fld score. |
| `post_away_score` | integer | Post away score. |
| `post_home_score` | integer | Post home score. |
| `post_bat_score` | integer | Post bat score. |
| `post_fld_score` | integer | Post fld score. |
| `if_fielding_alignment` | character | If fielding alignment. |
| `of_fielding_alignment` | character | Of fielding alignment. |
| `spin_axis` | integer | Spin axis. |
| `delta_home_win_exp` | numeric | Delta home win exp. |
| `delta_run_exp` | numeric | Delta run exp. |
| `bat_speed` | numeric | Bat speed (mph). |
| `swing_length` | numeric | Swing length (ft, head travel). |
| `miss_distance` | character | Average miss distance (in) on swings. |
| `estimated_slg_using_speedangle` | numeric | Estimated slg using speedangle. |
| `delta_pitcher_run_exp` | numeric | Delta pitcher run exp. |
| `hyper_speed` | numeric | Hyper speed. |
| `home_score_diff` | integer | Home score diff. |
| `bat_score_diff` | integer | Bat score diff. |
| `home_win_exp` | numeric | Home win exp. |
| `bat_win_exp` | numeric | Bat win exp. |
| `age_pit_legacy` | integer | Age pit legacy. |
| `age_bat_legacy` | integer | Age bat legacy. |
| `age_pit` | integer | Age pit. |
| `age_bat` | integer | Age bat. |
| `n_thruorder_pitcher` | integer | Number of thruorder pitcher. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of priorpa thisgame player at bat. |
| `pitcher_days_since_prev_game` | integer | Pitcher days since prev game. |
| `batter_days_since_prev_game` | integer | Batter days since prev game. |
| `pitcher_days_until_next_game` | integer | Pitcher days until next game. |
| `batter_days_until_next_game` | integer | Batter days until next game. |
| `api_break_z_with_gravity` | numeric | Api break z with gravity. |
| `api_break_x_arm` | numeric | Api break x arm. |
| `api_break_x_batter_in` | numeric | Api break x batter in. |
| `arm_angle` | numeric | Arm angle. |
| `attack_angle` | numeric | Attack angle (deg, bat path at contact). |
| `attack_direction` | numeric | Attack direction (deg, pull/oppo). |
| `swing_path_tilt` | numeric | Swing-path tilt (deg). |
| `intercept_ball_minus_batter_pos_x_inches` | numeric | Intercept ball minus batter pos x inches. |
| `intercept_ball_minus_batter_pos_y_inches` | numeric | Intercept ball minus batter pos y inches. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search_wbc
df = mlb_statcast_search_wbc("2023-03-08", "2023-03-22")
```

### `mlb_stats(stats: 'str', group: 'str', season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, team_id: 'Optional[int]' = None, player_pool: 'Optional[str]' = None, game_type: 'Optional[str]' = None, limit: 'int' = 50, offset: 'int' = 0, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_stats}

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

### `mlb_stats_leaders(leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, stat_group: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, limit: 'int' = 10, **kwargs) -> 'Dict'` {#mlb_stats_leaders}

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

### `mlb_stats_streaks(streak_type: 'str', streak_threshold: 'int' = 1, season: 'Optional[Union[int, str]]' = None, stat_group: 'Optional[str]' = None, active_streak: 'Optional[bool]' = None, sport_id: 'int' = 1, **kwargs) -> 'Dict'` {#mlb_stats_streaks}

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

### `mlb_team_leaders(team_id: 'int', leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, limit: 'int' = 10, **kwargs) -> 'Dict'` {#mlb_team_leaders}

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

### `mlb_team_stats(team_id: 'int', season: 'Union[int, str]', stats: 'str' = 'season', group: 'str' = 'hitting', sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_team_stats}

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

### `mlb_teams(season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_ids: 'Optional[Union[int, List[int], str]]' = None, active_status: 'Optional[str]' = None, all_star_statuses: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'` {#mlb_teams}

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
