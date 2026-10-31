---
title: "Men's college basketball — additional Python functions — Highlights"
sidebar_label: "Highlights"
sidebar_position: 1
description: "Men's college basketball — additional Python functions — Highlights — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Highlights

### espn_mbb_game_rosters {#espn_mbb_game_rosters}

`espn_mbb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame | pd.DataFrame | dict'`

espn_mbb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from mbb_schedule(). |
| `raw` | `bool` | `False` | If True, returns the Core v2 competitors payload (dict) as ESPN sent it; its roster, team and athlete `$ref` links are not followed. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id' plus the athlete's `position_id`, `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf`. With `raw=True`, the competitors payload (dict).

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `athlete_display_name` | String | Athlete display name (full). |
| `short_name` | String | Short display name. |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `age` | Int64 | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `slug` | String | URL-safe identifier. |
| `jersey` | String | Jersey number worn by the player. |
| `linked` | Boolean | TRUE if the record is linked to a related entity. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `alternate_ids_sdr` | String |  |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `birth_country_alternate_id` | Int64 | Alternate identifier for the player's birth country as provided by ESPN. |
| `birth_country_abbreviation` | String |  |
| `headshot_href` | String | Headshot image URL. |
| `headshot_alt` | String | Alternative-text label for the headshot. |
| `flag_href` | String | URL to the player's country flag image as supplied by ESPN. |
| `flag_alt` | String | Alt-text string for the player's country flag image, typically the country name. |
| `flag_rel` | String | Relationship descriptor for the country flag link, indicating the link type (e.g., 'country-flag'). |
| `experience_years` | Int64 | Experience years. |
| `experience_display_value` | String | Experience display value. |
| `experience_abbreviation` | String | Experience abbreviation. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `position_id` | Int64 | ESPN's numeric identifier of the athlete's listed position (the Core v2 position resource id). |
| `position_name` | String | Name of the athlete's listed position from ESPN's position resource, e.g. Guard or Forward. |
| `position_display_name` | String | Display form of the athlete's listed position as ESPN renders it; usually equal to position_name. |
| `position_abbreviation` | String | Short code of the athlete's listed position (e.g. G, F, RW, OL), from ESPN's position resource. |
| `position_leaf` | Boolean | Whether the position is a leaf of ESPN's position tree; False marks a grouping, not a specific role. |
| `hand_type` | String | Hand type. |
| `hand_abbreviation` | String | Hand abbreviation. |
| `hand_display_value` | String | Hand display value. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `jersey_right` | String | Right-justified or secondary jersey number display string for the player. |
| `valid` | Boolean | Valid. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `display_name` | String | Display name. |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `athlete_href` | String | ESPN API URL pointing to the full athlete resource for this player. |
| `position_href` | String | ESPN API URL pointing to the full position resource for this player's role. |
| `statistics_href` | String | ESPN API URL pointing to the player's statistics resource for this game. |
| `team_id` | Int64 | Unique team identifier. |
| `order` | Int64 | Display order within the result set. |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `winner` | Boolean | Winner. |
| `team_guid` | String | ESPN team GUID. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | String | Team nickname. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `is_active` | Boolean | Is active. |
| `is_all_star` | Boolean | Is all star. |
| `team_alternate_ids_sdr` | String | Alternate team identifier from ESPN's SDR (Sports Data Repository) system. |
| `logo_href` | String | Team or league logo URL. |
| `logo_dark_href` | String | Logo URL for dark backgrounds. |
| `game_id` | Int64 | Unique game identifier. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN has no competitors, roster or athlete resource for the game (404), or every rostered athlete's `$ref` answered 404. A single 404 athlete is left out with a `UserWarning` and the rest of the roster is returned. |
| `AssetFetchError` | A fetch failed (403, 429, 5xx, a dropped connection); never treated as a missing athlete. |

**Example**

```python
from sportsdataverse.mbb import espn_mbb_game_rosters
roster = espn_mbb_game_rosters(game_id=401638637)
print(roster.shape)

# Identify starters

import polars as pl
starters = roster.filter(pl.col("starter") == True).select(
    ["full_name", "jersey", "team_display_name"]
)

# Pandas round-trip

roster_pd = espn_mbb_game_rosters(game_id=401638637, return_as_pandas=True)
roster_pd.head()
```

### espn_mbb_pbp {#espn_mbb_pbp}

`espn_mbb_pbp(game_id: 'int', raw: 'bool' = False, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

espn_mbb_pbp() - Pull one game's play-by-play from ESPN's `mens-college-basketball/summary` endpoint.

By default the plays come back as the frame the `espn_mens_college_basketball_pbp` release
publishes (what `~sportsdataverse.mbb.load_mbb_pbp` reads): snake_case columns, Int32
team / athlete / game ids, the Int64 play `id`, `game_date` / `game_date_time`, the
hoopR shot coordinates, `athlete_name_{1,2,3}`, and `home_score` / `away_score`
rebuilt from the scoring plays when that reproduces the final (`score_source` says which).
The release's season-level `pregame_home_prob` / `home_win_prob` are not added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from mbb_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint (wins over `return_parsed`). |
| `return_parsed` | `bool` | `True` | If True (default), returns the release play-by-play frame. If False, returns the game dictionary (the shape before 0.2.0, which `helper_mbb_play_by_play` / `helper_mbb_team_box` / `helper_mbb_player_box` and `game_page_frames` read). |
| `return_as_pandas` | `bool` | `False` | If True, returns the frame as pandas instead of polars. |

**Returns**

One row per play, columns and dtypes as the release (`load_mbb_pbp`) for that season. Empty (no columns) when ESPN has no play-by-play for the game (`playByPlaySource == "none"`) or 10 or fewer plays, as hoopR returns NULL. A game with no betting line has null spread, home-favorite and home-team-spread values on every play and the spread-available flag False (hoopR's NA), never a default line. With `return_parsed=False`, a dictionary of game data with keys: "gameId", "plays", "winprobability", "boxscore", "header", "format", "broadcasts", "videos", "playByPlaySource", "standings", "article", "leaders", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "teamInfo", "season".

**Example**

```python
from sportsdataverse.mbb import espn_mbb_pbp
plays = espn_mbb_pbp(game_id=401638637)
print(plays.shape)

# Filter shooting plays for a basic shot chart

import polars as pl
shots = plays.filter(pl.col("shooting_play") == True)
shots.select(
    [
        "period_number",
        "clock_display_value",
        "team_id",
        "coordinate_x",
        "coordinate_y",
        "score_value",
        "text",
    ]
).head()

# As pandas

plays_pd = espn_mbb_pbp(game_id=401638637, return_as_pandas=True)
plays_pd[plays_pd["shooting_play"] == True].head()

# The game dictionary (box score, header, pickcenter, ...)

game = espn_mbb_pbp(game_id=401638637, return_parsed=False)
print(game["gameId"], len(game["plays"]))

# Raw payload (skip the cleaning pipeline) for debugging

raw = espn_mbb_pbp(game_id=401638637, raw=True)
sorted(raw.keys())
```

### espn_mbb_player_stats {#espn_mbb_player_stats}

`espn_mbb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a men's-college-basketball athlete's ESPN **season** stat line.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns, the athlete / team metadata blocks, and the `season_type` /
`total` parameters. For the richer web-v3 payload use
`sportsdataverse.mbb.espn_mbb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN men's-college-basketball athlete identifier. |
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
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `total` | Boolean | Total. |
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `age` | String | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `jersey` | String | Jersey number worn by the player. |
| `slug` | String | URL-safe identifier. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `position_id` | Int64 | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `college_name` | String | College / pre-draft team. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status label. |
| `defensive_blocks` | Float64 | Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |
| `defensive_defensive_rebounds` | Float64 | The number of times when the defense obtains the possession of the ball after a missed shot by the offense. |
| `defensive_steals` | Float64 | The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_turnover_points` | Float64 | The amount of points resulting from the possession following a turnover. |
| `defensive_avg_defensive_rebounds` | Float64 | The average defensive rebounds per game. |
| `defensive_avg_blocks` | Float64 | The average blocks per game. |
| `defensive_avg_steals` | Float64 | The average steals per game. |
| `general_disqualifications` | Float64 | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | Float64 | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | Float64 | The number of times a player had illegal contact with the opponent. |
| `general_per` | Float64 | A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_ejections` | Float64 | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | Float64 | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | Float64 | The total number of rebounds (offensive and defensive). |
| `general_minutes` | Float64 | The total number of minutes played. |
| `general_avg_minutes` | Float64 | The average number of minutes per game. |
| `general_fantasy_rating` | Float64 | The Fantasy Rating of a player. |
| `general_plus_minus` | Float64 | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_avg_rebounds` | Float64 | The average rebounds per game. |
| `general_avg_fouls` | Float64 | The average fouls committed per game. |
| `general_avg_flagrant_fouls` | Float64 | The average number of flagrant fouls per game. |
| `general_avg_technical_fouls` | Float64 | The average number of technical fouls per game. |
| `general_avg_ejections` | Float64 | The average ejections per game. |
| `general_avg_disqualifications` | Float64 | The average number of disqualifications per game. |
| `general_assist_turnover_ratio` | Float64 | The average number of assists a player or team records per turnover. |
| `general_steal_foul_ratio` | Float64 | The average number of steals a player or team records per foul committed. |
| `general_block_foul_ratio` | Float64 | The average number of blocks a player or record per foul committed. |
| `general_avg_team_rebounds` | Float64 | The average number of rebounds for a team per game. |
| `general_total_rebounds` | Float64 | The total number of rebounds for a team or player. |
| `general_total_technical_fouls` | Float64 | The total number of technical fouls for a team or player. |
| `general_steal_turnover_ratio` | Float64 | The number of steals per turnover. |
| `general_games_played` | Float64 | Games Played. |
| `general_games_started` | Float64 | The number of games started by an athlete. |
| `general_double_double` | Float64 | The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | Float64 | The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `offensive_assists` | Float64 | The number of times a player who passes the ball to a teammate in a way that leads to a score by field goal, meaning that he or she was "assisting" in the basket. There is some judgment involved in deciding whether a passer should be credited with an assist. |
| `offensive_field_goals` | Float64 | Field Goal makes and attempts. |
| `offensive_field_goals_attempted` | Float64 | The number of times a 2pt field goal was attempted. |
| `offensive_field_goals_made` | Float64 | The number of times a 2pt field goal was made. |
| `offensive_field_goal_pct` | Float64 | The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_free_throws` | Float64 | Free Throw makes and attempts. |
| `offensive_free_throw_pct` | Float64 | The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_free_throws_attempted` | Float64 | The number of times a free throw was attempted. |
| `offensive_free_throws_made` | Float64 | The number of times a free throw was made. |
| `offensive_offensive_rebounds` | Float64 | The number of times when the offense obtains the possession of the ball after a missed shot. |
| `offensive_points` | Float64 | The number of points scored. |
| `offensive_turnovers` | Float64 | The number of times a player loses possession to the other team. |
| `offensive_three_point_field_goals_attempted` | Float64 | The number of times a 3pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | Float64 | The number of times a 3pt field goal was made. |
| `offensive_total_turnovers` | Float64 | The number of turnovers plus team turnovers for the team. |
| `offensive_points_in_paint` | Float64 | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_second_chance_points` | Float64 | Total points scored by the player on second-chance opportunities following an offensive rebound. |
| `offensive_fast_break_points` | Float64 | The number of points scored on fast breaks. |
| `offensive_avg_field_goals_made` | Float64 | The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | Float64 | The average field goals attempted per game. |
| `offensive_avg_three_point_field_goals_made` | Float64 | The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | Float64 | The average three point field goals attempted per game. |
| `offensive_avg_free_throws_made` | Float64 | The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | Float64 | The average free throw shots attempted per game. |
| `offensive_avg_points` | Float64 | The average number of points scored per game. |
| `offensive_avg_offensive_rebounds` | Float64 | The average offensive rebounds per game. |
| `offensive_avg_assists` | Float64 | The average assists per game. |
| `offensive_avg_turnovers` | Float64 | The average turnovers committed per game. |
| `offensive_offensive_rebound_pct` | Float64 | The percentage of the number of times they obtain the possession of the ball after a missed shot. |
| `offensive_estimated_possessions` | Float64 | An estimation of the number of possessions for a team or player. |
| `offensive_avg_estimated_possessions` | Float64 | The average number of estimated possessions per game for a team or player. |
| `offensive_points_per_estimated_possessions` | Float64 | The number of points per estimated possession for a team or player. |
| `offensive_avg_team_turnovers` | Float64 | The average number of turnovers for a team per game. |
| `offensive_avg_total_turnovers` | Float64 | The average number of total turnovers for a team per game. |
| `offensive_three_point_field_goal_pct` | Float64 | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_two_point_field_goals_made` | Float64 | The number of 2-point field goals made for a team or player. |
| `offensive_two_point_field_goals_attempted` | Float64 | The number of 2-point field goals attempted for a team or player. |
| `offensive_avg_two_point_field_goals_made` | Float64 | The number of 2-point field goals made per game for a team or player. |
| `offensive_avg_two_point_field_goals_attempted` | Float64 | The number of 2-point field goals attempted per game for a team or player. |
| `offensive_two_point_field_goal_pct` | Float64 | The percentage of 2-points fields goals made by a team or player. |
| `offensive_shooting_efficiency` | Float64 | The efficiency with which a team or player shoots the basketball. |
| `offensive_scoring_efficiency` | Float64 | The efficiency with which a team or player scores the basketball. |
| `team_id` | Int64 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | String | ESPN team GUID. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_is_active` | Boolean | TRUE if the team is currently active. |
| `team_logo_href` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season_type` is not `"regular"`/`"postseason"`. |
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `sportsdataverse.errors.AssetFetchError` | The statistics, athlete or team fetch failed (403, 429, 5xx, a dead connection); only a 404 on the athlete or team node leaves those columns null. |

**Example**

```python
from sportsdataverse.mbb import espn_mbb_player_stats
df = espn_mbb_player_stats(athlete_id=4395624, season=2023)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### espn_mbb_schedule {#espn_mbb_schedule}

`espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_schedule - look up the men's college basketball scheduler for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | A date (`YYYYMMDD`) or a season (`YYYY`, 2002 or later). A season reads that season's game days from ESPN's scoreboard calendar and requests each day in turn (one calendar request plus ~150 day requests), keeping the games ESPN files under the season, sorted by date. A day that fails raises `AssetFetchError`. |
| `groups` | `int` | `50` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `limit` | `int` | `500` | number of records to return, default: 500. A season pull sends it per day, capped at 1000 (ESPN answers 25 events above that), and raises `AssetFetchError` for a day that fills it. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. A day or season with no games returns a zero-row frame carrying the documented columns.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `uid` | String | ESPN UID string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `attendance` | Int64 | Reported attendance. |
| `time_valid` | Boolean | Time valid. |
| `neutral_site` | Boolean | Neutral site. |
| `conference_competition` | Boolean | Conference competition. |
| `play_by_play_available` | Boolean |  |
| `recent` | Boolean | Recent. |
| `tournament_id` | Int64 | ESPN tournament identifier. |
| `start_date` | String | Start date (YYYY-MM-DD). |
| `broadcast` | String | Broadcast information string. |
| `highlights` | String |  |
| `notes_type` | String | Notes type. |
| `notes_headline` | String | Notes headline. |
| `broadcast_market` | String | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | String | Broadcast name. |
| `type_id` | Int64 | Type identifier (numeric). |
| `type_abbreviation` | String | Type abbreviation. |
| `venue_id` | Int64 | Unique venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `status_clock` | Float64 | Status clock. |
| `status_display_clock` | String | Status display clock. |
| `status_period` | Int64 | Status period. |
| `status_type_id` | Int64 | Unique identifier for status type. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status type state. |
| `status_type_completed` | Boolean | Status type completed. |
| `status_type_description` | String | Status type description. |
| `status_type_detail` | String | Status type detail. |
| `status_type_short_detail` | String | Status type short detail. |
| `format_regulation_periods` | Int64 | Format regulation periods. |
| `home_id` | Int64 | Unique identifier for home. |
| `home_uid` | String | Home team's uid. |
| `home_location` | String | Home team's location. |
| `home_name` | String | Home name. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home display name. |
| `home_short_display_name` | String | Home short display name. |
| `home_color` | String | Color code (hex) for home. |
| `home_alternate_color` | String | Color code (hex) for home alternate. |
| `home_is_active` | Boolean | Home team's is active. |
| `home_venue_id` | Int64 | Unique identifier for home venue. |
| `home_logo` | String | Home team logo URL. |
| `home_conference_id` | Int64 | Unique identifier for home conference. |
| `home_score` | String | Home team score at the time of the play. |
| `home_winner` | Boolean | Home team's winner. |
| `home_current_rank` | Int64 | Current national poll ranking of the home team at the time of the game, or null if unranked. |
| `home_linescores` | List | Period-by-period scoring breakdown for the home team, serialized as a packed integer sequence. |
| `home_records` | String | Win-loss record string(s) for the home team across applicable record categories (e.g., overall, conference). |
| `away_id` | Int64 | Unique identifier for away. |
| `away_uid` | String | Away team's uid. |
| `away_location` | String | Away team's location. |
| `away_name` | String | Away name. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away display name. |
| `away_short_display_name` | String | Away short display name. |
| `away_color` | String | Color code (hex) for away. |
| `away_alternate_color` | String | Color code (hex) for away alternate. |
| `away_is_active` | Boolean | Away team's is active. |
| `away_venue_id` | Int64 | Unique identifier for away venue. |
| `away_logo` | String | Away team logo URL. |
| `away_conference_id` | Int64 | Unique identifier for away conference. |
| `away_score` | String | Away team score at the time of the play. |
| `away_winner` | Boolean | Away team's winner. |
| `away_current_rank` | Int64 | Current national poll ranking of the away team at the time of the game, or null if unranked. |
| `away_linescores` | List | Period-by-period scoring breakdown for the away team, serialized as a packed integer sequence. |
| `away_records` | String | Win-loss record string(s) for the away team across applicable record categories (e.g., overall, conference). |
| `game_id` | Int64 | Unique game identifier. |
| `season` | Int64 | Season year. |
| `season_type` | Int64 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `home_logo_dark` | String |  |
| `away_logo_dark` | String |  |

**Example**

```python
from sportsdataverse.mbb import espn_mbb_schedule
day = espn_mbb_schedule(dates=20240408)
print(day.shape)

# Season-level pull (2024 season)

season = espn_mbb_schedule(dates=2024)
print(season.shape)

# Filter to a specific team (Duke ``team_id=150``)

import polars as pl
duke = season.filter(
    (pl.col("home_id") == 150) | (pl.col("away_id") == 150)
)

# Pandas round-trip

season_pd = espn_mbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

### espn_mbb_teams {#espn_mbb_teams}

`espn_mbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_teams - look up the men's college basketball teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of the men's college basketball teams, one row per team, with the columns in the returns table. A payload without teams returns a zero-row frame with those columns. The result is not cached: every call fetches (through the package's opt-in TTL response cache, `sportsdataverse.cache`, when it is on).

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_display_name` | String | Full team display name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_is_active` | Boolean | TRUE if the team is currently active. |
| `team_is_all_star` | Boolean | TRUE if the row represents an All-Star team. |
| `team_location` | String | Team city or location string. |
| `team_logos` | Int64 |  |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | String | Team nickname. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN answered 404 for the teams resource. |
| `AssetFetchError` | The fetch failed (403, 429, 5xx, a dropped connection); never returned as an empty frame. |

**Example**

```python
from sportsdataverse.mbb import espn_mbb_teams
teams = espn_mbb_teams()
print(teams.shape)
print(teams.columns[:8])

# Walk every team-id (handy for batched scrapes)

team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")

# Pandas round-trip + Division II/III

d2_d3 = espn_mbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

### most_recent_mbb_season {#most_recent_mbb_season}

`most_recent_mbb_season()`

Return the most recent men's college basketball season year.

The men's college basketball season spans early November through early
April; for any month October-December the "current season" is the
following calendar year (e.g. October 2025 returns `2026`).

**Returns**

The most recent / current season year.

**Example**

```python
from sportsdataverse.mbb import most_recent_mbb_season, espn_mbb_schedule
season = most_recent_mbb_season()
sched = espn_mbb_schedule(dates=season)
```

### ncaa_mbb_play_by_play {#ncaa_mbb_play_by_play}

`ncaa_mbb_play_by_play(game_ids: "'Sequence[object]'", *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False, errors: "Literal['raise', 'skip']" = 'raise') -> "'Union[pl.DataFrame, Any]'"`

Scrape many MBB games' play-by-play (bigballR `get_play_by_play`).

Ports `bigballR/R/all_functions.R:1857-1897`: drops missing ids, shares
one fetcher session across games, retries each empty/failed game once
(except a page the format guard rejected: its layout cannot change on a
re-fetch), row-binds the survivors, and logs the removed ids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries and non-integral floats (`6470186.5`) are dropped, an integral float (`6470186.0`) is read as `"6470186"`. |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp`. Defaults to one shared `NcaaFetcher.with_browser()` context. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |
| `errors` | `Literal['raise', 'skip']` | `'raise'` | `"raise"` (default): a failed fetch (a ban, an exhausted proxy pool, an uncleared challenge) raises `AssetFetchError`. `"skip"`: it is dropped like a missing game, and the frame gains a `scrape_status` column -- `"ok"` on every play row, plus one row per dropped game (`game_id` and why: `fetch_failed: ...`, `no_data: ...`, `request_error: ...`, `parse_failed: ...`, `unsupported_layout: <format>` for a page in a layout the parser does not read, or `empty`). |

**Returns**

Row-bound play-by-play for every game that scraped successfully (zero-row contract frame when none did). A missing game or a page the parser cannot read is dropped and logged (bigballR parity).

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home` | String | Home. |
| `away` | String | Away record. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | String | Game clock value. |
| `game_time` | String | Game start time. |
| `game_seconds` | Int64 |  |
| `home_score` | Int64 | Home team score at the time of the play. |
| `away_score` | Int64 | Away team score at the time of the play. |
| `event_team` | String |  |
| `event_description` | String |  |
| `player_1` | String |  |
| `player_2` | String |  |
| `event_type` | String | Event / play type code (V2 PBP). |
| `event_result` | String |  |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `event_length` | Int64 |  |
| `poss_num` | Int64 |  |
| `poss_team` | String |  |
| `poss_length` | Int64 |  |
| `is_transition` | Boolean |  |
| `home_1` | String |  |
| `home_2` | String |  |
| `home_3` | String |  |
| `home_4` | String |  |
| `home_5` | String |  |
| `away_1` | String |  |
| `away_2` | String |  |
| `away_3` | String |  |
| `away_4` | String |  |
| `away_5` | String |  |
| `status` | String | Status label. |
| `is_garbage_time` | Boolean |  |
| `sub_deviate` | Int64 |  |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | A fetch failed and `errors="raise"`. |
| `ValueError` | `errors` is not `"raise"` or `"skip"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
df = ncaa_mbb_play_by_play(["6470186", "6479639"])
print(df.shape)

# Pipeline next step (one line)

df.group_by("game_id").len()
```

### ncaa_mbb_team_roster {#ncaa_mbb_team_roster}

`ncaa_mbb_team_roster(team_id: 'Optional[int]' = None, *, team: 'Optional[str]' = None, season: 'Optional[str]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape a men's team roster from stats.ncaa.org.

Port of bigballR `get_team_roster`. The `player` column is the
normalized `FIRST.LAST` key that byte-matches the play-by-play name
normalization, so roster<->pbp joins line up.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` | stats.ncaa.org team id (changes every season). |
| `team` | `Optional[str]` | `None` | School name, e.g. `"Illinois"`. |
| `season` | `Optional[str]` | `None` | Season string, e.g. `"2025-26"`; required with `team`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher`; defaults to a fresh browser-transport fetcher. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player -- see `parse_ncaa_bb_team_roster` for the column contract.

| col_name | type | description |
|---|---|---|
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `jersey` | String | Jersey number worn by the player. |
| `name` | String | Display name. |
| `class` | String | College class / draft eligibility note. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `hometown` | String | Player hometown. |
| `high_school` | String |  |
| `player` | String | Player name. |
| `clean_name` | String |  |
| `ht_inches` | Int64 |  |
| `player_id` | String | Unique player identifier. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | Neither `team_id` nor a resolvable `team`/`season` pair was given. |

**Example**

```python
from sportsdataverse.mbb import ncaa_mbb_team_roster
df = ncaa_mbb_team_roster(team="Illinois", season="2025-26")
print(df.select("jersey", "player", "ht_inches").head())
```

### ncaa_mbb_team_schedule {#ncaa_mbb_team_schedule}

`ncaa_mbb_team_schedule(team_id: 'Optional[int]' = None, *, team: 'Optional[str]' = None, season: 'Optional[str]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape a men's team's season schedule from stats.ncaa.org.

Port of bigballR `get_team_schedule`. Give either the season-specific
`team_id` or a `team` + `season` pair (resolved through the bundled
men's crosswalk).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` | stats.ncaa.org team id (changes every season) -- the number in the team page URL. |
| `team` | `Optional[str]` | `None` | School name, e.g. `"Illinois"` (not the mascot form). |
| `season` | `Optional[str]` | `None` | Season string, e.g. `"2025-26"`; required with `team`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher`; defaults to a fresh browser-transport fetcher (stats.ncaa.org blocks plain HTTP clients). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per scheduled game -- see `parse_ncaa_bb_team_schedule` for the column contract.

| col_name | type | description |
|---|---|---|
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home` | String | Home. |
| `home_score` | Int64 | Home team score at the time of the play. |
| `away` | String | Away record. |
| `away_score` | Int64 | Away team score at the time of the play. |
| `box_id` | String |  |
| `game_id` | String | Unique game identifier. |
| `is_neutral` | Boolean |  |
| `detail` | String | Detail. |
| `attendance` | Int64 | Reported attendance. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | Neither `team_id` nor a resolvable `team`/`season` pair was given. |

**Example**

```python
import polars as pl
from sportsdataverse.mbb import ncaa_mbb_team_schedule
df = ncaa_mbb_team_schedule(team="Illinois", season="2025-26")
print(df.shape)

# Pipeline next step (one line)

df.filter(pl.col("is_neutral") == True).head()
```
