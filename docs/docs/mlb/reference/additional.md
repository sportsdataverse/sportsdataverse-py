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

### `add_sequence_features(feats: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#add_sequence_features}

Add within-game sequence, times-through-order, and workload features.

Consumes the output of `pitch_features`. Within each plate
appearance (`game_pk`, `pitcher`, `at_bat_number`, sorted by
`pitch_number`), adds `prev_pitch_type`/`prev_release_pos_x`/
`prev_release_pos_z`/`prev_plate_x`/`prev_plate_z` via
`shift(1)`. Within each game (`game_pk`, `pitcher`, sorted by
`at_bat_number` then `pitch_number`), adds `cum_pitches_game`
(running pitch count), `batter_faced_index` (distinct-`at_bat_number`
rank), and `times_through_order` (`min(3, (batter_faced_index-1)//9+1)`).
Every lag/rank is `.over(...)` scoped to avoid cross-game leakage.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `feats` | `DataFrame` |  | Output of `pitch_features`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`feats` plus the sequence/TTO/workload columns described above. Empty input returns a zero-row frame carrying the full schema.

| col_name | type | description |
|---|---|---|
| `prev_pitch_type` | character | Pitch type of the previous pitch in the same plate appearance (null on the first pitch). |
| `prev_release_pos_x` | double | Horizontal release position of the previous pitch in the same plate appearance. |
| `prev_release_pos_z` | double | Vertical release position of the previous pitch in the same plate appearance. |
| `prev_plate_x` | double | Horizontal plate location of the previous pitch in the same plate appearance. |
| `prev_plate_z` | double | Vertical plate location of the previous pitch in the same plate appearance. |
| `cum_pitches_game` | integer | Running count of pitches thrown by this pitcher so far in this game (inclusive of the current pitch). |
| `batter_faced_index` | integer | Dense rank of this plate appearance's at_bat_number within the game (1 = first batter faced). |
| `times_through_order` | integer | Times through the batting order, min(3, (batter_faced_index-1)//9 + 1). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
feats = add_sequence_features(pitch_features(raw))
print(feats.select("times_through_order", "cum_pitches_game").tail())
```

### `build_we_table(states: 'pl.DataFrame', results: 'pl.DataFrame', *, laplace: 'float' = 1.0) -> 'pl.DataFrame'` {#build_we_table}

Empirical, Laplace-smoothed home win-expectancy table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `states` | `DataFrame` |  | Output of `pbp_base_out_states`. |
| `results` | `DataFrame` |  | Game-level results with `game_id` (same dtype as `states`), `home_score`, `away_score`. |
| `laplace` | `float` | `1.0` | Additive smoothing constant (default 1.0). |

**Returns**

one row per observed state bucket. | Column | Type | Description | |---|---|---| | inning_capped | Int64 | Inning, capped at 9 | | half | Utf8 | `"top"` or `"bottom"` | | base_state | Utf8 | 3-char base occupancy | | outs_start | Int64 | Outs before the play (0-2) | | score_diff_bucket | Int64 | home - away score, clipped to [-6, 6] | | home_win_exp | Float64 | Laplace-smoothed P(home wins \| state) | | n | Int64 | Plate appearances observed in this bucket |

| col_name | type | description |
|---|---|---|
| `inning_capped` | integer | Inning number, capped at 9 (extra innings pooled with the 9th). |
| `half` | character | Half-inning ("top" or "bottom"). |
| `base_state` | character | 3-char base occupancy code. |
| `outs_start` | integer | Outs before the play (0-2). |
| `score_diff_bucket` | integer | home minus away score, clipped to [-6, 6]. |
| `home_win_exp` | double | Laplace-smoothed empirical P(home team wins \| state bucket). |
| `n` | integer | Plate appearances observed in this state bucket. |

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
from sportsdataverse.mlb.mlb_win_expectancy import build_we_table
states = pbp_base_out_states(pbp)
table = build_we_table(states, results)
```

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

### `fit_zone_model(pitches: 'pl.DataFrame') -> 'Dict[str, Any]'` {#fit_zone_model}

Fit a logistic P(called strike | zone coordinates) on called pitches.

Compute-on-demand -- **no artifact is bundled or cached to disk.**
L2-regularized (`1e-4`) mean log-loss, minimized via
`scipy.optimize.minimize(method="L-BFGS-B")`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Frame of pitches with `description` (filtered to `{"called_strike", "ball"}`), `plate_x`, `plate_z`, `sz_top`, `sz_bot`. |

**Returns**

`{"coef": list[float] (7,), "intercept": float, "features": list[str]}`. `{"coef": [], "intercept": 0.0, "features": [...]}` if fewer than 2 called pitches are available (degenerate fit).

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import fit_zone_model
model = fit_zone_model(pitches)
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

### `mlb_batter_projection(target_season: 'int', *, history: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_batter_projection}

Next-season xwOBA projection (Marcel + delta-method aging) for every batter.

If `history` is `None`, builds player-season xwOBA history via
`sportsdataverse.mlb.mlb_expected_stats.mlb_expected_stats` across
the three seasons before `target_season` (ages must already be present
on a supplied `history` frame -- this convenience path is intended for
callers who already maintain an age-joined roster history).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int` |  | The season being projected. |
| `history` | `Optional[DataFrame]` | `None` | Pre-built player-season history (`batter`, `season`, `age`, `xwoba`, `pa`). If `None`, uses `mlb_expected_stats` over `target_season - 3 .. target_season - 1`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `batter`: `age`, `proj_xwoba`, `proj_pa`. Empty history returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id. |
| `age` | integer | Batter's projected age in target_season (last known age plus one). |
| `proj_xwoba` | double | Marcel-style weighted, PA-regressed, aging-curve-adjusted xwOBA projection for target_season. |
| `proj_pa` | double | Sum of plate appearances across the weighted lookback seasons used to build the projection. |

**Example**

```python
from sportsdataverse.mlb.mlb_batter_projection import mlb_batter_projection

proj = mlb_batter_projection(2024, history=player_season_history)
print(proj.shape)

# Pipeline next step (one line)

proj.sort("proj_xwoba", descending=True).head()
```

### `mlb_command_plus(pitches: 'pl.DataFrame', *, level: 'str' = 'pitch', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_command_plus}

Score pitches with the bundled Command+/Location+ (②) run-value model.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `plate_x_abs`, `plate_z_norm`, `in_zone`, `dist_from_heart`, `balls`, `strikes`, `stand`, `p_throws`, `pitch_type`). |
| `level` | `str` | `'pitch'` | `"pitch"` (default) for per-pitch output, or `"pitcher"` for a per-pitcher mean. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `pitch_type`, `location_rv_hat`, `command_plus`. `level="pitcher"`: `pitcher`, `location_rv_hat`, `command_plus` (per-pitcher mean). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `pitch_type` | character | Statcast pitch-type abbreviation. |
| `location_rv_hat` | double | Predicted per-pitch run value from the bundled Command+/Location+ xgboost model (location + count/handedness/pitch-type features only). |
| `command_plus` | double | Plus-scale Command+/Location+ score, 100 = league average, higher = better. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_command_plus import mlb_command_plus
feats = pitch_features(raw_pitches)
out = mlb_command_plus(feats)
print(out.select("pitcher", "command_plus").head())

# Pipeline next step

out.group_by("pitcher").agg(pl.col("command_plus").mean()).sort("command_plus", descending=True)
```

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

### `mlb_expected_home_runs(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, park_factors: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_expected_home_runs}

Per player-season park-neutral xHR, park-adjusted xHR, and HR-above-expected.

Pulls batted balls via `puller(start_dt, end_dt, player_type="batter")`,
builds the EV x LA x spray HR-probability grid from the pull's own batted
balls (season-agnostic algorithm, per-pull empirical constants), predicts
each ball's HR probability with the EV x LA-marginal fallback, park-adjusts
via `hr_factor` (index 100 = neutral, joined on the Statcast
`home_team` abbreviation -> MLBAM team id), then aggregates per batter.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `park_factors` | `Optional[DataFrame]` | `None` | Pre-fetched park-factors frame (`team_id`, `hr_factor`); if `None`, fetched via `sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_park_factors`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `hr`, `xhr_neutral`, `xhr_park_adj`, `hr_above_expected` (`hr - xhr_neutral`). Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's home-runs leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `hr` | integer | Realized home runs in the pulled window. |
| `xhr_neutral` | double | Park-neutral expected home runs from the EV x LA x spray probability grid. |
| `xhr_park_adj` | double | Park-adjusted expected home runs (xhr_neutral cell probabilities scaled by each batted ball's home-park HR factor / 100). |
| `hr_above_expected` | double | hr minus xhr_neutral -- positive means the batter over-performed the park-neutral HR model. |

**Example**

```python
from sportsdataverse.mlb.mlb_expected_home_runs import mlb_expected_home_runs

df = mlb_expected_home_runs("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("hr_above_expected", descending=True).head()
```

### `mlb_expected_stats(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_expected_stats}

Per player-season xwOBA/xBA/xSLG from an on-the-fly EV x LA empirical grid.

Pulls pitches via `puller(start_dt, end_dt, player_type="batter")`, builds
the outcome grid from the pull's own batted balls (season-agnostic
algorithm, per-pull empirical constants -- see `CLAUDE.md`), predicts
contact `woba`/`ba`/`slg` per batted ball with the launch-angle-
marginal fallback, then aggregates:

* `xwoba = (sum(predicted_woba over balls in play) + sum(woba_value over
  non-batted-ball PA outcomes)) / sum(woba_denom)`
* `xba = sum(predicted_ba over balls in play) / ab`
* `xslg = sum(predicted_tb over balls in play) / ab`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `pa`, `ab`, `xwoba`, `xba`, `xslg`. Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's expected-stats leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `pa` | integer | Plate appearances in the pulled window. |
| `ab` | integer | At-bats (PA minus walks, HBP, and sacrifices) in the pulled window. |
| `xwoba` | double | Expected wOBA from the EV x LA empirical grid (contact) plus realized non-contact outcome value, divided by wOBA denominator. |
| `xba` | double | Expected batting average from the EV x LA empirical grid's hit-indicator cell means. |
| `xslg` | double | Expected slugging percentage from the EV x LA empirical grid's total-bases cell means. |

**Example**

```python
from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats

df = mlb_expected_stats("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("xwoba", descending=True).head()
```

### `mlb_injury_risk(pitches: 'pl.DataFrame', *, as_of_date: 'Optional[dt.date]' = None, window: 'int' = 5, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_injury_risk}

Composite pitcher injury-risk index from leakage-safe trailing trends.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw pitch frame (from `sportsdataverse.mlb.mlb_statcast_search`). |
| `as_of_date` | `Optional[date]` | `None` | When given, restricts input to `game_date < as_of_date` via `sportsdataverse.mlb.mlb_pitching_constants.as_of_split` before computing trends (an additional leakage boundary on top of the per-appearance trailing-window logic). |
| `window` | `int` | `5` | Trailing-window size passed to `pitcher_appearance_trends`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `game_date`, `injury_risk_index` — equal-weighted sum of standardized adverse features (`-velo_trend`, `-velo_drop`, `trailing_workload`, `-days_rest`; higher = more risk). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `game_pk` | integer | Game identifier. |
| `game_date` | date | Game date. |
| `injury_risk_index` | double | Equal-weighted composite of standardized adverse trailing features (higher = more risk). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_injury import mlb_injury_risk
out = mlb_injury_risk(raw_pitches)
print(out.sort("injury_risk_index", descending=True).head())
```

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

### `mlb_pitch_classify(pitches: 'pl.DataFrame', *, max_components: 'int' = 6, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_pitch_classify}

Per-pitcher Gaussian-mixture pitch reclassification.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`). |
| `max_components` | `int` | `6` | Cap on GMM components considered per pitcher (BIC picks the best `1..min(max_components, n_pitch_types)`). |
| `seed` | `int` | `0` | Random seed for reproducible cluster labels. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `pitch_type`, `pitch_type_reclass`, `reclass_confidence` (max posterior cluster responsibility). Pitchers with fewer than `MIN_PITCHES_FOR_CLUSTERING` pitches pass through the Savant label unchanged with `reclass_confidence = 1.0`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `pitch_type` | character | Savant-reported pitch-type abbreviation. |
| `pitch_type_reclass` | character | Reclassified pitch-type label from the per-pitcher GMM clustering (may differ from pitch_type). |
| `reclass_confidence` | double | Max posterior cluster responsibility (1.0 for low-volume pitchers passed through unchanged). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_pitch_classify import mlb_pitch_classify
feats = pitch_features(raw_pitches)
out = mlb_pitch_classify(feats, seed=0)
print(out.filter(out["pitch_type"] != out["pitch_type_reclass"]).head())
```

### `mlb_pitch_era(pitches: 'pl.DataFrame', seasons: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_pitch_era}

Combined xERA + SIERA-like estimator (model ③).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw pitch frame (from `sportsdataverse.mlb.mlb_statcast_search`). |
| `seasons` | `int` |  | Season year. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `x_woba`, `x_era`, `k_pct`, `bb_pct`, `gb_pct`, `siera_like`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `season` | integer | Season year. |
| `x_woba` | double | Mean estimated_woba_using_speedangle allowed on batted balls. |
| `x_era` | double | Parametric xERA converted from x_woba via the season's league baselines. |
| `k_pct` | double | Strikeout rate (strikeouts / batters faced). |
| `bb_pct` | double | Walk rate (walks + HBP / batters faced). |
| `gb_pct` | double | Ground-ball rate among batted balls. |
| `siera_like` | double | SIERA-like ERA estimate from the fitted K%/BB%/GB% OLS coefficients. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import mlb_pitch_era
out = mlb_pitch_era(raw_pitches, 2024)
print(out.select("pitcher", "x_era", "siera_like").head())
```

### `mlb_pitch_tunneling(pitches: 'pl.DataFrame', *, eps: 'float' = 0.01, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_pitch_tunneling}

Per-pitch release/plate distance from the previous pitch + tunnel ratio.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `release_pos_x`/`release_pos_z`, `prev_release_pos_x`/ `prev_release_pos_z`, `plate_x`/`plate_z`, `prev_plate_x`/`prev_plate_z`). |
| `eps` | `float` | `0.01` | Minimum `release_dist` denominator (avoids divide-by-zero). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `at_bat_number`, `pitch_number`, `release_dist`, `plate_dist`, `tunnel_ratio`. First pitch of a plate appearance (no previous pitch) has null geometry. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `game_pk` | integer | Game identifier. |
| `at_bat_number` | integer | Game-level plate-appearance sequence number. |
| `pitch_number` | integer | Pitch sequence number within the plate appearance. |
| `release_dist` | double | Euclidean distance between this pitch's release point and the previous pitch's release point. |
| `plate_dist` | double | Euclidean distance between this pitch's plate location and the previous pitch's plate location. |
| `tunnel_ratio` | double | plate_dist / max(release_dist, eps) -- higher means better tunneling (similar release, different result). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_pitch_tunneling
feats = add_sequence_features(pitch_features(raw_pitches))
out = mlb_pitch_tunneling(feats)
print(out.select("tunnel_ratio").describe())
```

### `mlb_prop_strikeouts(team_k9: 'float', opp_k_rate: 'float', lg_k_rate: 'float', *, innings: 'float' = 9.0) -> 'float'` {#mlb_prop_strikeouts}

Expected pitcher/team strikeouts via a K/9-and-opponent-K-rate blend.

`team_k9 / 9 * innings * (opp_k_rate / lg_k_rate)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_k9` | `float` |  | Team/pitcher strikeouts per 9 innings pitched. |
| `opp_k_rate` | `float` |  | Opponent's own strikeout rate (K per PA). |
| `lg_k_rate` | `float` |  | League-average strikeout rate. |
| `innings` | `float` | `9.0` | Innings pitched in this outing (default 9.0). |

**Returns**

expected strikeouts.

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_strikeouts
mlb_prop_strikeouts(9.0, 0.22, 0.22)
```

### `mlb_prop_team_runs(home_off: 'float', away_def: 'float', lg_rpg: 'float', *, park_factor: 'float' = 1.0) -> 'float'` {#mlb_prop_team_runs}

Expected team runs via a log5-style rate blend.

`lg_rpg * (home_off / lg_rpg) * (away_def / lg_rpg) * park_factor`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  | Team's own runs-scored-per-game rate. |
| `away_def` | `float` |  | Opponent's runs-allowed-per-game rate. |
| `lg_rpg` | `float` |  | League-average runs-per-game rate. |
| `park_factor` | `float` | `1.0` | Park run-scoring multiplier (default neutral 1.0; a real park-factor table is a documented follow-on). |

**Returns**

expected runs for the team in this matchup.

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_team_runs
mlb_prop_team_runs(5.5, 5.0, 4.5)
```

### `mlb_props(matchups: 'pl.DataFrame', ratings: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_props}

Expected team runs + strikeouts for a slate of matchups.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `matchups` | `DataFrame` |  | One row per game: `game_id`, `home_team_id`, `away_team_id`. |
| `ratings` | `DataFrame` |  | Per-team as-of-date rate table: `team_id`, `off_rpg` (runs scored/game), `def_rpg` (runs allowed/game), and optionally `k9` + `k_rate` (see the module docstring -- strikeout columns are null without them). `team_id` must share a dtype with `matchups`' team-id columns. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per matchup. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | home_team_id | Utf8 | Home team identifier | | away_team_id | Utf8 | Away team identifier | | exp_runs_home | Float64 | Expected home-team runs | | exp_runs_away | Float64 | Expected away-team runs | | exp_strikeouts_home | Float64 | Expected home-pitcher strikeouts (null if `ratings` lacks k9/k_rate) | | exp_strikeouts_away | Float64 | Expected away-pitcher strikeouts (null if `ratings` lacks k9/k_rate) |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier. |
| `home_team_id` | character | Home team identifier. |
| `away_team_id` | character | Away team identifier. |
| `exp_runs_home` | double | Expected home-team runs (log5-style rate blend). |
| `exp_runs_away` | double | Expected away-team runs (log5-style rate blend). |
| `exp_strikeouts_home` | double | Expected home-pitcher strikeouts (null when the ratings input lacks k9/k_rate). |
| `exp_strikeouts_away` | double | Expected away-pitcher strikeouts (null when the ratings input lacks k9/k_rate). |

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import mlb_props
props = mlb_props(matchups, ratings)
```

### `mlb_pythagenpat(runs_scored: 'float', runs_allowed: 'float', games: 'int', *, exponent: 'float' = 0.287) -> 'float'` {#mlb_pythagenpat}

Pythagenpat expected win percentage (Smyth-Patriot, run-environment adaptive exponent).

`x = ((runs_scored + runs_allowed) / games) ** exponent`;
`win_pct = runs_scored**x / (runs_scored**x + runs_allowed**x)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `runs_scored` | `float` |  | Total runs scored. |
| `runs_allowed` | `float` |  | Total runs allowed. |
| `games` | `int` |  | Games played. |
| `exponent` | `float` | `0.287` | Run-environment exponent (default the published 0.287). |

**Returns**

expected win percentage in `[0, 1]`. Returns `0.5` when `games == 0` or `runs_scored + runs_allowed == 0` (guard against a zero-division/degenerate input).

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat
mlb_pythagenpat(800, 600, 162)
```

### `mlb_pythagenpat_table(results: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_pythagenpat_table}

Per-(season, team) pythagenpat table from game-level results.

This is a **same-window estimator**, not a forward-looking prediction:
pythagenpat smooths a team's *already-known* run differential into an
implied "true-talent" win rate over that same window (the classic
Bill James validation is exactly "does the formula's win% track the
actual win% over the same season"). To use it predictively for a
future game, pre-filter `results` to games strictly before that date
with `sportsdataverse.mlb.mlb_game_state_constants.as_of_split`
first -- this function does not do that filtering itself.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Game-level results (`season`, `home_team_id`, `away_team_id`, `home_score`, `away_score`). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per (season, team). | Column | Type | Description | |---|---|---| | season | Int64 | Season | | team_id | Utf8 | Team identifier | | runs_scored | Int64 | Total runs scored | | runs_allowed | Int64 | Total runs allowed | | games | Int64 | Games played | | win_pct | Float64 | Realized win percentage | | pythag_win_pct | Float64 | Pythagenpat expected win percentage |

| col_name | type | description |
|---|---|---|
| `season` | integer | Season. |
| `team_id` | character | Team identifier (statsapi team id, stringified). |
| `runs_scored` | integer | Total runs scored across the covered games. |
| `runs_allowed` | integer | Total runs allowed across the covered games. |
| `games` | integer | Games played. |
| `win_pct` | double | Realized win percentage. |
| `pythag_win_pct` | double | Pythagenpat expected win percentage (exponent 0.287). |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat_table
table = mlb_pythagenpat_table(results)
```

### `mlb_run_expectancy_matrix(seasons: 'Union[int, List[int], None]' = None, *, pbp: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_run_expectancy_matrix}

Empirical RE24 run-expectancy matrix by base-out state.

`re[base_state, outs] = mean(runs_rest_of_inning)` over all plate
appearances starting in that state, excluding the bottom of the 9th
inning and beyond (the standard RE24 exclusion -- those half-innings
are only played while the home team trails or is tied, a
score-differential selection bias that would otherwise distort the
matrix). Computed on demand from statsapi play-by-play; **no bundled
artifact**.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | One season (int) or a list of seasons to collect via `sportsdataverse.mlb.mlb_api_extra.mlb_schedule`. Ignored when `pbp` is supplied. |
| `pbp` | `Optional[DataFrame]` | `None` | Pre-collected parsed play-by-play frame (skips the network collector -- primarily for tests / offline reuse). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

up to 24 rows (base_state x outs). | Column | Type | Description | |---|---|---| | base_state | Utf8 | 3-char base occupancy (e.g. `"1_3"`) | | outs | Int64 | Outs at the start of the state (0-2) | | re | Float64 | Mean runs scored through the end of the half-inning | | n | Int64 | Number of plate appearances observed in this state |

| col_name | type | description |
|---|---|---|
| `base_state` | character | 3-char base occupancy code ("_" = empty, "1"/"2"/"3" = occupied), e.g. "1_3" for runners on first and third. |
| `outs` | integer | Outs at the start of the base-out state (0-2). |
| `re` | double | Empirical mean runs scored from this state through the end of the half-inning (RE24). |
| `n` | integer | Number of plate appearances observed starting in this base-out state. |

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import mlb_run_expectancy_matrix
matrix = mlb_run_expectancy_matrix(pbp=pbp)

# Pipeline next step (one line)

matrix.filter(pl.col("base_state") == "___").sort("outs")
```

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

### `mlb_sequence_run_value(pitches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_sequence_run_value}

Mean run value grouped by the ordered `(prev_pitch_type, pitch_type)` sequence.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `prev_pitch_type`, `pitch_type`, `run_value` / `delta_run_exp`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`prev_pitch_type`, `pitch_type`, `mean_run_value`, `n` — one row per observed ordered pair (rows with a null `prev_pitch_type`, i.e. the first pitch of a PA, are dropped). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `prev_pitch_type` | character | Pitch type of the preceding pitch in the sequence. |
| `pitch_type` | character | Pitch type of the current pitch. |
| `mean_run_value` | double | Mean run value of the current pitch, grouped by the ordered (prev_pitch_type, pitch_type) pair. |
| `n` | integer | Number of pitches observed for this ordered pair. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_sequencing import mlb_sequence_run_value
out = mlb_sequence_run_value(feats)
print(out.sort("mean_run_value").head())
```

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

### `mlb_stuff_plus(pitches: 'pl.DataFrame', *, level: 'str' = 'pitch', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_stuff_plus}

Score pitches with the bundled Stuff+ (①) run-value model.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`, `release_pos_x_z`, `release_pos_z_z`, `extension_z`). |
| `level` | `str` | `'pitch'` | `"pitch"` (default) for per-pitch output, or `"arsenal"` for a per `(pitcher, pitch_type)` mean. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `pitch_type`, `stuff_rv_hat`, `stuff_plus` — one row per pitch (`level="pitch"`) or per pitcher-pitchtype (`level="arsenal"`). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `pitch_type` | character | Statcast pitch-type abbreviation. |
| `stuff_rv_hat` | double | Predicted per-pitch run value from the bundled Stuff+ xgboost model (physics + fastball-relative features only). |
| `stuff_plus` | double | Plus-scale Stuff+ score, 100 = league average, higher = better (sign-inverted from stuff_rv_hat). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_stuff_plus import mlb_stuff_plus
feats = pitch_features(raw_pitches)
out = mlb_stuff_plus(feats, level="arsenal")
print(out.sort("stuff_plus", descending=True).head())

# Pipeline next step

out.filter(pl.col("pitch_type") == "FF").sort("stuff_plus", descending=True)
```

### `mlb_swing_decision(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_swing_decision}

Per player-season swing/take run value + selective-aggression (SEAGER analog).

Pulls pitches via `puller(start_dt, end_dt, player_type="batter")`,
builds the RV(swing)/RV(take) zone x count surfaces (and the league
swing-rate table) from the pull itself, then per batter:

* `swing_take_runs` = sum of the **actual** per-pitch `delta_run_exp`
  credited to the batter's swing/take decisions (matching Savant's
  swing/take run-value definition -- the run value of what actually
  happened on each pitch, not a league-average lookup).
* `selective_agg` = sum of `rv_chosen - rv_neutral`, where
  `rv_neutral = swing_rate * rv_swing + (1 - swing_rate) * rv_take` uses
  the **league** swing rate for that zone x count cell -- positive means
  the batter swings at hittable pitches and takes bad ones more than a
  league-average decision-maker would.
* `chase_rate` = swings / pitches seen in the waste/chase zones
  (`zone in {11,12,13,14}`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `pitches`, `swing_take_runs`, `selective_agg`, `chase_rate`, `n_swings`. Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's swing/take leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `pitches` | integer | Total pitches seen with a non-null zone/decision in the pulled window. |
| `swing_take_runs` | double | Sum of the run value of the batter's actual swing/take decisions (delta_run_exp of the chosen decision at that zone x count). |
| `selective_agg` | double | SEAGER-analog selective-aggression score -- sum of (chosen run value minus the league-neutral-rate run value) per pitch. |
| `chase_rate` | double | Share of pitches in the waste/chase attack zones (11-14) that the batter swung at. |
| `n_swings` | integer | Count of pitches the batter swung at. |

**Example**

```python
from sportsdataverse.mlb.mlb_swing_decision import mlb_swing_decision

df = mlb_swing_decision("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("selective_agg", descending=True).head()
```

### `mlb_team_elo(results: 'pl.DataFrame', *, k: 'float' = 4.0, hfa: 'float' = 24.0, init: 'float' = 1500.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_team_elo}

As-of-date iterative Elo run-differential rating.

Games are folded **in date order** (ties broken by `game_id`); each
team's rating updates only *after* its game is scored, so the
`home_rating`/`away_rating` columns are strictly as-of-date (no
leakage from later games). `home_win_prob_elo` uses the standard
logistic Elo formula with a home-field-advantage offset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Game-level results (`game_id`, `date`, `home_team_id`, `away_team_id`, `home_score`, `away_score`). |
| `k` | `float` | `4.0` | Elo K-factor (rating-update step size). |
| `hfa` | `float` | `24.0` | Home-field-advantage Elo-point offset. |
| `init` | `float` | `1500.0` | Initial rating for a team with no prior games. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per game, in date order. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | date | Date | Game date | | home_team_id | Utf8 | Home team identifier | | away_team_id | Utf8 | Away team identifier | | home_rating | Float64 | Home team's rating **before** this game | | away_rating | Float64 | Away team's rating **before** this game | | home_win_prob_elo | Float64 | Elo-implied P(home wins) before this game | | home_rating_post | Float64 | Home team's rating **after** this game | | away_rating_post | Float64 | Away team's rating **after** this game |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `date` | date | Game date. |
| `home_team_id` | character | Home team identifier. |
| `away_team_id` | character | Away team identifier. |
| `home_rating` | double | Home team's Elo rating before this game (as-of-date). |
| `away_rating` | double | Away team's Elo rating before this game (as-of-date). |
| `home_win_prob_elo` | double | Elo-implied P(home team wins) before this game. |
| `home_rating_post` | double | Home team's Elo rating after this game. |
| `away_rating_post` | double | Away team's Elo rating after this game. |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_team_elo
elo = mlb_team_elo(results)

# Pipeline next step (one line)

elo.group_by("home_team_id").agg(pl.col("home_rating_post").last())
```

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

### `mlb_team_projection(seasons: 'Union[int, List[int], None]' = None, *, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_team_projection}

Combined pythagenpat + Elo team projection.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | Reserved for a future network-collector path (currently unused -- pass `results` directly; see `sportsdataverse.mlb.mlb_run_expectancy.mlb_run_expectancy_matrix` for the collector pattern this will follow once wired). |
| `results` | `Optional[DataFrame]` | `None` | Game-level results (see `mlb_pythagenpat_table` and `mlb_team_elo` for the required columns). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per (season, team). | Column | Type | Description | |---|---|---| | season | Int64 | Season | | team_id | Utf8 | Team identifier | | win_pct | Float64 | Realized win percentage | | pythag_win_pct | Float64 | Pythagenpat expected win percentage | | rating | Float64 | Final (as of the last observed game) Elo rating | | exp_margin | Float64 | Elo-implied expected run margin vs a league-average opponent |

| col_name | type | description |
|---|---|---|
| `season` | integer | Season. |
| `team_id` | character | Team identifier (statsapi team id, stringified). |
| `win_pct` | double | Realized win percentage. |
| `pythag_win_pct` | double | Pythagenpat expected win percentage. |
| `rating` | double | Final (as of the last observed game) Elo rating. |
| `exp_margin` | double | Elo-implied expected run margin vs a league-average opponent. |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_team_projection
projection = mlb_team_projection(results=results)
```

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

### `mlb_times_through_order(pitches: 'pl.DataFrame', *, season: 'int' = 2024, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#mlb_times_through_order}

Per-pitch fitted times-through-order fatigue adjustment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `times_through_order`). |
| `season` | `int` | `2024` | Season year, selects the fitted `tto_penalty` coefficients via `sportsdataverse.mlb.mlb_pitching_constants.get_baselines`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `at_bat_number`, `pitch_number`, `times_through_order`, `fatigue_rv_adj` (the fitted marginal penalty for that TTO level). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `game_pk` | integer | Game identifier. |
| `at_bat_number` | integer | Game-level plate-appearance sequence number. |
| `pitch_number` | integer | Pitch sequence number within the plate appearance. |
| `times_through_order` | integer | Times through the batting order (1-3). |
| `fatigue_rv_adj` | double | Fitted per-TTO run-value marginal (mlb_pitching_constants.tto_penalty) for this pitch's TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import mlb_times_through_order
feats = add_sequence_features(pitch_features(raw_pitches))
out = mlb_times_through_order(feats, season=2024)
print(out.select("times_through_order", "fatigue_rv_adj").unique())
```

### `mlb_umpire_bias(pitches: 'pl.DataFrame', *, model: 'Optional[Dict[str, Any]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_umpire_bias}

Per-umpire called-strike bias residual (observed minus expected).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Called pitches with `umpire_id`, `description`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`. |
| `model` | `Optional[Dict[str, Any]]` | `None` | Pre-fit model dict from `fit_zone_model`; fits on `pitches` itself when `None`. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per umpire. | Column | Type | Description | |---|---|---| | umpire_id | Utf8 | Umpire identifier | | n_called | Int64 | Called pitches observed for this umpire | | obs_strike_rate | Float64 | Realized called-strike rate | | exp_strike_rate | Float64 | Mean model-predicted called-strike probability | | bias | Float64 | obs_strike_rate - exp_strike_rate (positive = strike-generous) |

| col_name | type | description |
|---|---|---|
| `umpire_id` | character | Umpire identifier (statsapi people id, stringified). |
| `n_called` | integer | Called pitches (strike or ball) observed for this umpire. |
| `obs_strike_rate` | double | Realized called-strike rate for this umpire. |
| `exp_strike_rate` | double | Mean model-predicted called-strike probability for this umpire's pitches. |
| `bias` | double | obs_strike_rate minus exp_strike_rate (positive = strike-generous). |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_bias
bias = mlb_umpire_bias(pitches)
```

### `mlb_umpire_called_strike_prob(pitches: 'pl.DataFrame', *, model: 'Optional[Dict[str, Any]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_umpire_called_strike_prob}

P(called strike) per pitch from the zone logistic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Frame with `plate_x`, `plate_z`, `sz_top`, `sz_bot` (one row per pitch, not required to be called pitches only). |
| `model` | `Optional[Dict[str, Any]]` | `None` | Pre-fit model dict from `fit_zone_model`; fits on `pitches` itself when `None` (using only its called pitches). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per input pitch. | Column | Type | Description | |---|---|---| | called_strike_prob | Float64 | P(called strike \| pitch location) |

| col_name | type | description |
|---|---|---|
| `called_strike_prob` | double | P(called strike \| pitch location) from the standardized zone-coordinate logistic. |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_called_strike_prob
prob = mlb_umpire_called_strike_prob(pitches)
```

### `mlb_win_expectancy(pbp: 'pl.DataFrame', results: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_win_expectancy}

Per-play home win expectancy from the empirical state table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed `mlb_play_by_play` frame (see `sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`). |
| `results` | `DataFrame` |  | Game-level results (`game_id`, `home_score`, `away_score`). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per plate appearance, **plus one terminal "game over" row per game** (`at_bat_index` = last real PA's index + 1, `home_win_exp` pinned to the actual final outcome: 1.0 if home won, 0.0 otherwise). Without this anchor, the last real play's own WPA swing (e.g. a walk-off) would never be captured by `mlb_win_probability_added`'s per-game diff, and the game-level WPA sum would not telescope to the exact +-0.5 identity. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | at_bat_index | Int64 | Game-global sequential PA index (last row is a synthetic terminal marker) | | half | Utf8 | `"top"` or `"bottom"` (offense side); the terminal row repeats the last real half | | home_win_exp | Float64 | P(home team wins \| state before the play); 1.0/0.0 on the terminal row |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `at_bat_index` | integer | Game-global sequential plate-appearance index. |
| `half` | character | Half-inning ("top" or "bottom") -- which side is on offense. |
| `home_win_exp` | double | Empirical P(home team wins \| base-out-score-inning state before the play). |

**Example**

```python
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_expectancy
we = mlb_win_expectancy(pbp, results)

# Pipeline next step (one line)

we.filter(pl.col("game_id") == "716390").sort("at_bat_index")
```

### `mlb_win_probability_added(we: 'pl.DataFrame', *, perspective: 'str' = 'home', return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#mlb_win_probability_added}

Per-play win-probability-added from a `mlb_win_expectancy` frame.

`wpa_i = home_win_exp_i - home_win_exp_{i-1}` within each game (the
first play of a game is measured against the neutral 0.5 baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `we` | `DataFrame` |  | Output of `mlb_win_expectancy` (needs `game_id`, `at_bat_index`, `home_win_exp`). |
| `perspective` | `str` | `'home'` | `"home"` (default) returns home-team WPA; any other value (e.g. `"away"`) returns the sign-flipped (away-team) WPA. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per plate appearance. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | at_bat_index | Int64 | Game-global sequential PA index | | wpa | Float64 | Win-probability added, from `perspective` |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `at_bat_index` | integer | Game-global sequential plate-appearance index. |
| `wpa` | double | Win-probability added on this play, from the requested perspective. |

**Example**

```python
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_probability_added
wpa = mlb_win_probability_added(we)
```

### `pbp_base_out_states(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#pbp_base_out_states}

Reconstruct pre-play base-out state from statsapi play-by-play.

Within each `(game_id, inning, half)` half-inning, ordered by the
game-global `at_bat_index`: `base_state`/`outs_start` before PA
*i* are the post-occupancy / out-count of PA *i-1* (empty/0 at the
half's first PA -- occupancy and outs both genuinely reset at every
half-inning boundary). `runs_on_play` is the score delta since the
*previous PA in the game* (`over("game_id")`, **not** reset per
half-inning -- the score itself carries across the half-inning
boundary even though outs/bases do not).  `runs_rest_of_inning` is
the suffix-sum of `runs_on_play` within the half.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed `mlb_play_by_play` frame (optionally concatenated across games), carrying `game_id`, `about_inning`, `about_half_inning`, `about_at_bat_index`, `count_outs`, `result_home_score`, `result_away_score`, `matchup_post_on_{first,second,third}_id`. |

**Returns**

one row per plate appearance. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | inning | Int64 | Inning number | | half | Utf8 | `"top"` or `"bottom"` | | at_bat_index | Int64 | Game-global sequential PA index | | base_state | Utf8 | 3-char occupancy before the PA (`"1_3"` etc.) | | outs_start | Int64 | Outs before the PA (0-2) | | runs_on_play | Int64 | Runs scored on this PA | | runs_rest_of_inning | Int64 | Runs scored from this PA through the half's end | | score_diff | Int64 | home - away score at the start of the PA |

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
states = pbp_base_out_states(pbp)
```

### `pitch_features(pitches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#pitch_features}

Build the per-pitch feature substrate every pitching model consumes.

Standardizes physics (velocity/spin/movement/release/extension) within
`pitcher`, derives strike-zone-relative location features, pins id
columns to `Int64`, and passes Savant's per-pitch `delta_run_exp`
through unchanged as `run_value` (the single run-value label used by
Stuff+/Command+/TTO/tunneling).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw Savant pitch frame (e.g. from `sportsdataverse.mlb.mlb_statcast_search`), one row per pitch, carrying `pitcher`, `release_speed`, `release_spin_rate`, `pfx_x`, `pfx_z`, `release_pos_x`, `release_pos_z`, `release_extension`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`, `delta_run_exp`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per pitch with the input columns plus `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`, `release_pos_x_z`, `release_pos_z_z`, `extension_z` (standardized within `pitcher`), `plate_z_norm`, `plate_x_abs`, `in_zone`, `dist_from_heart`, and `run_value`. Empty/malformed input returns a zero-row frame carrying the added schema.

| col_name | type | description |
|---|---|---|
| `velo_z` | double | Release speed standardized (z-score) within pitcher. |
| `spin_z` | double | Release spin rate standardized (z-score) within pitcher. |
| `pfx_x_z` | double | Horizontal movement (pfx_x) standardized (z-score) within pitcher. |
| `pfx_z_z` | double | Vertical movement (pfx_z) standardized (z-score) within pitcher. |
| `release_pos_x_z` | double | Horizontal release position standardized (z-score) within pitcher. |
| `release_pos_z_z` | double | Vertical release position standardized (z-score) within pitcher. |
| `extension_z` | double | Release extension standardized (z-score) within pitcher. |
| `run_value` | double | Savant per-pitch delta_run_exp, passed through unchanged as the spine's single run-value label. |
| `plate_x_abs` | double | Absolute horizontal plate location (distance from the center of the zone). |
| `plate_z_norm` | double | Vertical plate location normalized to the batter's own strike zone, 0 = bottom, 1 = top. |
| `in_zone` | integer | 1 if the pitch crossed the strike zone (normalized location + horizontal bound), else 0. |
| `dist_from_heart` | double | Euclidean distance from the normalized zone center (0, 0.5) -- lower is more hittable. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
raw = mlb_statcast_search("2024-06-15", "2024-06-15", player_type="pitcher")
feats = pitch_features(raw)
print(feats.select("pitch_type", "in_zone", "run_value").head())

# Pipeline next step

feats.filter(pl.col("in_zone") == 1).group_by("pitch_type").agg(pl.col("run_value").mean())
```

### `pitcher_appearance_trends(pitches: 'pl.DataFrame', *, window: 'int' = 5, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#pitcher_appearance_trends}

Leakage-safe per-appearance trailing velocity/workload trends.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw (or feature-substrate) pitch frame carrying `pitcher`, `game_pk`, `game_date`, `pitch_type`, `release_speed`. |
| `window` | `int` | `5` | Number of trailing PRIOR appearances used for the rolling statistics (never includes the current appearance). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(pitcher, game_pk, game_date)`: `fb_velo` (this game's mean fastball `release_speed`), `velo_trend` (OLS slope of `fb_velo` over the trailing `window` prior appearances), `velo_drop` (trailing-baseline mean minus this game's `fb_velo`), `pitches_game`, `trailing_workload` (mean `pitches_game` over the trailing window), `days_rest`. The first appearance for a pitcher has null trailing stats (no prior data). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `game_pk` | integer | Game identifier. |
| `game_date` | date | Game date. |
| `fb_velo` | double | Mean fastball release speed for this appearance. |
| `velo_trend` | double | OLS slope of fb_velo over the trailing prior appearances (leakage-safe). |
| `velo_drop` | double | Trailing-baseline mean fb_velo minus this appearance's fb_velo. |
| `pitches_game` | integer | Pitches thrown in this appearance. |
| `trailing_workload` | double | Mean pitches_game over the trailing prior appearances (leakage-safe). |
| `days_rest` | double | Days since the pitcher's previous appearance. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_injury import pitcher_appearance_trends
out = pitcher_appearance_trends(raw_pitches, window=5)
print(out.select("game_date", "velo_drop", "days_rest").tail())
```

### `prop_over_prob(line: 'float', expected: 'float') -> 'float'` {#prop_over_prob}

P(realized count > line) under a Poisson(expected) model.

`1 - poisson.cdf(floor(line), expected)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `line` | `float` |  | The prop betting line (e.g. 8.5 runs). |
| `expected` | `float` |  | The Poisson mean (expected runs/strikeouts/etc.). |

**Returns**

P(over), in `[0, 1]`.

**Example**

```python
from sportsdataverse.mlb.mlb_prop_projection import prop_over_prob
prop_over_prob(3.5, 4.5)
```

### `siera_like(pitches: 'pl.DataFrame', season: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#siera_like}

SIERA-like ERA estimator from K%/BB%/GB% (**experimental / provisional**).

Evaluates the published SIERA functional form with
`mlb_pitching_constants.siera_coef`, which are **SEEDED literature
placeholders** (not yet OLS-fitted — the Task-4.2 next-season-ERA fit has
not landed). Treat the output as directionally indicative, not a calibrated
ERA; use `x_era` (oracle-gated vs Savant's xERA) for a fitted number.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw pitch frame carrying `pitcher`, `events`, and (optionally) `bb_type`. |
| `season` | `int` |  | Season year (unused in the formula itself, carried through for join convenience with `x_era`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `k_pct`, `bb_pct`, `gb_pct`, `siera_like`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `season` | integer | Season year (carried through for join convenience with x_era). |
| `k_pct` | double | Strikeout rate (strikeouts / batters faced). |
| `bb_pct` | double | Walk rate (walks + HBP / batters faced). |
| `gb_pct` | double | Ground-ball rate among batted balls. |
| `siera_like` | double | SIERA-like ERA estimate from the fitted K%/BB%/GB% OLS coefficients. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import siera_like
out = siera_like(raw_pitches, 2024)
print(out.sort("siera_like").head())
```

### `tto_penalty_table(feats: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#tto_penalty_table}

Observed mean run value by times-through-order, with the penalty vs TTO=1.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `feats` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `times_through_order` and `run_value`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`times_through_order`, `mean_run_value`, `penalty_vs_first` (`mean_run_value` minus the TTO=1 mean run value), `n`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `times_through_order` | integer | Times through the batting order (1-3). |
| `mean_run_value` | double | Mean observed run value for pitches at this TTO level. |
| `penalty_vs_first` | double | mean_run_value minus the TTO=1 mean run value. |
| `n` | integer | Number of pitches observed at this TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import tto_penalty_table
feats = add_sequence_features(pitch_features(raw_pitches))
out = tto_penalty_table(feats)
print(out.sort("times_through_order"))
```

### `x_era(pitches: 'pl.DataFrame', season: 'int', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#x_era}

Parametric xERA from Statcast expected wOBA allowed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Raw (or feature-substrate) pitch frame carrying `pitcher` and `estimated_woba_using_speedangle`. |
| `season` | `int` |  | Season year (selects the league baseline via `sportsdataverse.mlb.mlb_pitching_constants.get_baselines`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `season`, `x_woba`, `x_era`. Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLBAM pitcher id. |
| `season` | integer | Season year. |
| `x_woba` | double | Mean estimated_woba_using_speedangle allowed on batted balls. |
| `x_era` | double | Parametric xERA converted from x_woba via the season's league baselines. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_era import x_era
out = x_era(raw_pitches, 2024)
print(out.sort("x_era").head())
```
