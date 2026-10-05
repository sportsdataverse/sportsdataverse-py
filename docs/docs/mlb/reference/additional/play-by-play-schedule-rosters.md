---
title: "MLB — additional Python functions — Play-by-play, schedule & rosters"
sidebar_label: "Play-by-play, schedule & rosters"
sidebar_position: 4
description: "MLB — additional Python functions — Play-by-play, schedule & rosters — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Play-by-play, schedule & rosters

### espn_mlb_game_rosters {#espn_mlb_game_rosters}

`espn_mlb_game_rosters(game_id: 'int', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)`

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
