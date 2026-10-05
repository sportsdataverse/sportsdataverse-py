---
title: "NFL — additional Python functions — Dataset loaders"
sidebar_label: "Dataset loaders"
sidebar_position: 3
description: "NFL — additional Python functions — Dataset loaders — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Dataset loaders

### load_combine {#load_combine}

`load_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | double | Year that player was drafted |
| `draft_team` | character | Team that drafted player |
| `draft_round` | double | Round that player was drafted in |
| `draft_ovr` | double | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfb_id` | character | Sports Reference (CFB) ID for player |
| `player_name` | character | Full name of player |
| `pos` | character | Position as tracked by FP |
| `school` | character | College of player |
| `ht` | character | Height of player (feet and inches) |
| `wt` | double | Weight of player (lbs) |
| `forty` | double | Player's 40 yard dash time at combine (seconds) |
| `bench` | double | Reps benched by player at combine |
| `vertical` | double | Player's vertical jump at combine (inches) |
| `broad_jump` | double | Player's broad jump at combine (inches) |
| `cone` | double | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | double | Player's shuttle run time at combine (seconds) |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

# Filter by draft year and position

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### load_contracts {#load_contracts}

`load_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | logical | Active contract |
| `year_signed` | integer | Year the contract was signed |
| `years` | integer | Contract length |
| `value` | double | Total contract value |
| `apy` | double | Average money per contract year |
| `guaranteed` | double | Total guaranteed money |
| `apy_cap_pct` | double | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | double | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | double | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | double | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | character | Player's OverTheCap url |
| `otc_id` | integer | Over the Cap ID for player |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | character | Player date of birth (if published). |
| `height` | character | Official height, in inches |
| `weight` | character | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_overall` | integer | Overall draft selection number. |
| `draft_team` | character | Team that drafted player |
| `cols` | double | Placeholder column retained in the contracts loader output schema; contains no meaningful data in this context. |
| `season_history` | double | List of structs, one per league year covered by the contract (year as a string, team, base_salary, prorated_bonus, option_bonus, roster_bonus, guaranteed_salary, cap_number, cap_percent, cash_paid, workout_bonus, per_game_roster_bonus, other_bonus), money in millions of dollars and a final 'Total' row per nflreadr. |
| `contract_history` | integer | List of structs, one per contract in the player's OverTheCap contract history (team, contract_type, status, year_signed, yrs, total, apy, guarantees, amount_earned, percent_earned, effective_apy), with money fields in millions of dollars. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

# Pandas round-trip with sort by APY

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### load_depth_charts {#load_depth_charts}

`load_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Depth Chart data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `club_code` | character | Three-letter team abbreviation identifying the NFL club on the depth chart row. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `depth_team` | character | Numeric depth rank indicating whether the player is listed as the starter (1), backup (2), or further reserve on the depth chart. |
| `last_name` | character | Last name of player |
| `first_name` | character | First name of player |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `formation` | character | Offensive or defensive formation context in which the depth chart position applies (e.g., 'Shotgun', 'Nickel'). |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `elias_id` | character | Elias Sports Bureau identifier for the player, used by the NFL for official statistical tracking. |
| `depth_position` | character | Positional grouping label used to place the player on the team's official depth chart (e.g., 'QB', 'WR1', 'ILB'). |
| `full_name` | character | Full name as per NFL.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

# Multi-season range

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### load_draft_picks {#load_draft_picks}

`load_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | integer | Draft round |
| `pick` | integer | Draft overall pick |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `cfb_player_id` | character | ID from College Football Reference |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `hof` | logical | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | character | Primary position as reported by NFL.com |
| `category` | character | Broader category of player positions |
| `side` | character | O for offense, D for defense, S for special teams |
| `college` | character | Official college (usually the last one attended) |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | integer | Final season played in NFL |
| `allpro` | integer | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | integer | Number of Pro Bowls |
| `seasons_started` | integer | Number of seasons recorded as primary starter for position |
| `w_av` | integer | Weighted Approximate Value |
| `car_av` | logical | Career Approximate Value |
| `dr_av` | integer | Draft Approximate Value |
| `games` | integer | Games played in career |
| `pass_completions` | integer | Number of successful completions for a given game |
| `pass_attempts` | integer | Career pass attempts |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_tds` | integer | Career pass touchdowns thrown |
| `pass_ints` | integer | Career pass interceptions thrown |
| `rush_atts` | integer | Career rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `rush_tds` | integer | Career rushing touchdowns |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | integer | Career receiving yards |
| `rec_tds` | integer | Career receiving touchdowns |
| `def_solo_tackles` | integer | Career solo tackles |
| `def_ints` | integer | Career interceptions |
| `def_sacks` | double | Number of sacks form this player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

# Filter to a single year and round

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### load_espn_qbr {#load_espn_qbr}

`load_espn_qbr(seasons: 'List[int]', summary_type: 'str' = 'season', return_as_pandas: 'bool' = False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load ESPN Total QBR (Quarterback Rating) data going back to 2006.

Mirrors nflreadpy / nflreadr `load_espn_qbr` -- the lone nflreadpy dataset
that previously had no sdv-py loader. ESPN publishes Total QBR only from 2006
onward, so 2006 is the earliest available season (unlike the 1999 floor on
play-by-play). nflverse republishes ESPN's QBR through the `espn_data`
release as two combined files (one per `summary_type`), each covering all
seasons; this loader reads the requested file once and post-filters by
`season` (the same access pattern as `load_nfl_schedule`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to return. 2006 is the earliest available season. |
| `summary_type` | `str` | `'season'` | Aggregation level. `"season"` (default) returns one row per quarterback-season; `"week"` returns one row per quarterback-game. Any other value raises `ValueError`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which QBR release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse `espn_data` release. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_espn_qbr` release (built by `nfl-data` from ESPN's QBR web endpoint -- the same source nflverse's espnscrapeR uses). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing ESPN Total QBR for the requested seasons, summarized per `summary_type`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `game_week` | character | Season week |
| `team_abb` | character | Abbreviation of Team of Player |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name_short` | character | Short name of player (First Initial, Last Name) |
| `rank` | double | QBR Rank in specified timeframe |
| `qbr_total` | double | Adjusted Total QBR, which adjusts quarterback play on 0-100 scale adjusted for strength of opposing defenses played. |
| `pts_added` | double | Number of points contributed by a quarterback above the average level QB |
| `qb_plays` | double | Total dropbacks for the quarterback (excludes handoffs) |
| `epa_total` | double | Total Expected Points Added by quarterback, calculated by ESPN Win Probability Model |
| `pass` | double | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `run` | double | Expected Points Added on run plays |
| `exp_sack` | double | Expected EPA Added on Sacks |
| `penalty` | double | Binary indicator for whether or not a penalty occurred. |
| `qbr_raw` | double | Raw total QBR, does not adjust for strength of opposing defenses played. |
| `sack` | double | Binary indicator for if the play ended in a sack. |
| `name_first` | character | First Name of Quarterback |
| `name_last` | character | Last Name of Quarterback |
| `name_display` | character | Full Name of Quarterback |
| `headshot_href` | character | Link to ESPN Headshot of Player |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `qualified` | logical | True/False indicator of whether or not player meets minimum play requirement |

**Example**

```python
from sportsdataverse.nfl import load_nfl_espn_qbr
qbr = load_nfl_espn_qbr(seasons=[2024])
qbr.shape

# Week-level QBR

qbr_week = load_nfl_espn_qbr(seasons=[2024], summary_type="week")

# Multi-season range

qbr = load_nfl_espn_qbr(seasons=range(2020, 2025))

# Pandas round-trip

qbr_pd = load_nfl_espn_qbr(seasons=[2024], return_as_pandas=True)
qbr_pd[["season", "team_abb", "qbr_total"]].head()
```

### load_ff_opportunity {#load_ff_opportunity}

`load_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `posteam` | character | String abbreviation for the team with possession. |
| `week` | double | Season week. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `full_name` | character | Full name as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `rec_attempt` | double | Total number of targets for a given game |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_air_yards` | double | Total air yards thrown for a given game |
| `rec_air_yards` | double | Total air yards on receiving attempts for a given game |
| `pass_completions` | double | Number of successful completions for a given game |
| `receptions` | double | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `pass_completions_exp` | double | Expected number of pass_completions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `receptions_exp` | double | Expected number of receptions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_yards_gained` | double | Total passing yards gained for a given game |
| `rec_yards_gained` | double | Total receiving yards gained for a given game |
| `rush_yards_gained` | double | Total rushing yards gained for a given game |
| `pass_yards_gained_exp` | double | Expected number of pass_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_yards_gained_exp` | double | Expected number of rec_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_yards_gained_exp` | double | Expected number of rush_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rec_touchdown` | double | Total receiving touchdowns |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `pass_touchdown_exp` | double | Expected number of pass_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_touchdown_exp` | double | Expected number of rec_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_touchdown_exp` | double | Expected number of rush_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_two_point_conv` | double | Number of successful passing two point conversions |
| `rec_two_point_conv` | double | Number of successful receiving two point conversions |
| `rush_two_point_conv` | double | Number of successful rushing two point conversions |
| `pass_two_point_conv_exp` | double | Expected number of pass_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_two_point_conv_exp` | double | Expected number of rec_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_two_point_conv_exp` | double | Expected number of rush_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_first_down` | double | Number of passing first downs |
| `rec_first_down` | double | Number of receiving first downs |
| `rush_first_down` | double | Number of rushing first downs |
| `pass_first_down_exp` | double | Expected number of pass_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_first_down_exp` | double | Expected number of rec_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_first_down_exp` | double | Expected number of rush_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_interception` | double | Number of interceptions thrown |
| `rec_interception` | double | Number of interceptions on targets |
| `pass_interception_exp` | double | Expected number of pass_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_interception_exp` | double | Expected number of rec_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fumble_lost` | double | Number of fumbles on receiving attempts |
| `rush_fumble_lost` | double | Number of fumbles on rushing attempts |
| `pass_fantasy_points_exp` | double | Expected number of pass_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fantasy_points_exp` | double | Expected number of rec_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_fantasy_points_exp` | double | Expected number of rush_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_fantasy_points` | double | Total fantasy points from passing, assuming 0.04 points per pass yard, 4 points per pass TD, -2 points per interception |
| `rec_fantasy_points` | double | Total fantasy points from receiving, assuming PPR scoring |
| `rush_fantasy_points` | double | Total fantasy points from rushing, assuming PPR scoring |
| `total_yards_gained` | double | Total scrimmage yards (sum of pass, rush, and receiving yards) |
| `total_yards_gained_exp` | double | Expected number of total_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_touchdown` | double | Total touchdowns (sum of pass, rush, and receiving touchdowns) |
| `total_touchdown_exp` | double | Expected number of total_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_first_down` | double | Total first downs (sum of pass, rush, and receiving first downs) |
| `total_first_down_exp` | double | Expected number of total_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_fantasy_points` | double | Total fantasy points (sum of pass, rush, and receiving fantasy points) |
| `total_fantasy_points_exp` | double | Expected number of total_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_completions_diff` | double | Difference between actual and expected number of pass_completions - often interpreted as efficiency for a given play/game |
| `receptions_diff` | double | Difference between actual and expected number of receptions - often interpreted as efficiency for a given play/game |
| `pass_yards_gained_diff` | double | Difference between actual and expected number of pass_yards_gained - often interpreted as efficiency for a given play/game |
| `rec_yards_gained_diff` | double | Difference between actual and expected number of rec_yards_gained - often interpreted as efficiency for a given play/game |
| `rush_yards_gained_diff` | double | Difference between actual and expected number of rush_yards_gained - often interpreted as efficiency for a given play/game |
| `pass_touchdown_diff` | double | Difference between actual and expected number of pass_touchdown - often interpreted as efficiency for a given play/game |
| `rec_touchdown_diff` | double | Difference between actual and expected number of rec_touchdown - often interpreted as efficiency for a given play/game |
| `rush_touchdown_diff` | double | Difference between actual and expected number of rush_touchdown - often interpreted as efficiency for a given play/game |
| `pass_two_point_conv_diff` | double | Difference between actual and expected number of pass_two_point_conv - often interpreted as efficiency for a given play/game |
| `rec_two_point_conv_diff` | double | Difference between actual and expected number of rec_two_point_conv - often interpreted as efficiency for a given play/game |
| `rush_two_point_conv_diff` | double | Difference between actual and expected number of rush_two_point_conv - often interpreted as efficiency for a given play/game |
| `pass_first_down_diff` | double | Difference between actual and expected number of pass_first_down - often interpreted as efficiency for a given play/game |
| `rec_first_down_diff` | double | Difference between actual and expected number of rec_first_down - often interpreted as efficiency for a given play/game |
| `rush_first_down_diff` | double | Difference between actual and expected number of rush_first_down - often interpreted as efficiency for a given play/game |
| `pass_interception_diff` | double | Difference between actual and expected number of pass_interception - often interpreted as efficiency for a given play/game |
| `rec_interception_diff` | double | Difference between actual and expected number of rec_interception - often interpreted as efficiency for a given play/game |
| `pass_fantasy_points_diff` | double | Difference between actual and expected number of pass_fantasy_points - often interpreted as efficiency for a given play/game |
| `rec_fantasy_points_diff` | double | Difference between actual and expected number of rec_fantasy_points - often interpreted as efficiency for a given play/game |
| `rush_fantasy_points_diff` | double | Difference between actual and expected number of rush_fantasy_points - often interpreted as efficiency for a given play/game |
| `total_yards_gained_diff` | double | Difference between actual and expected number of total_yards_gained - often interpreted as efficiency for a given play/game |
| `total_touchdown_diff` | double | Difference between actual and expected number of total_touchdown - often interpreted as efficiency for a given play/game |
| `total_first_down_diff` | double | Difference between actual and expected number of total_first_down - often interpreted as efficiency for a given play/game |
| `total_fantasy_points_diff` | double | Difference between actual and expected number of total_fantasy_points - often interpreted as efficiency for a given play/game |
| `pass_attempt_team` | double | Team-level total pass_attempt for a game, summed across all plays/players for that team. |
| `rec_attempt_team` | double | Team-level total rec_attempt for a game, summed across all plays/players for that team. |
| `rush_attempt_team` | double | Team-level total rush_attempt for a game, summed across all plays/players for that team. |
| `pass_air_yards_team` | double | Team-level total pass_air_yards for a game, summed across all plays/players for that team. |
| `rec_air_yards_team` | double | Team-level total rec_air_yards for a game, summed across all plays/players for that team. |
| `pass_completions_team` | double | Team-level total pass_completions for a game, summed across all plays/players for that team. |
| `receptions_team` | double | Team-level total receptions for a game, summed across all plays/players for that team. |
| `pass_completions_exp_team` | double | Team-level total expected pass_completions_exp for a game, summed across all plays & players for that team. |
| `receptions_exp_team` | double | Team-level total expected receptions_exp for a game, summed across all plays & players for that team. |
| `pass_yards_gained_team` | double | Team-level total pass_yards_gained for a game, summed across all plays/players for that team. |
| `rec_yards_gained_team` | double | Team-level total rec_yards_gained for a game, summed across all plays/players for that team. |
| `rush_yards_gained_team` | double | Team-level total rush_yards_gained for a game, summed across all plays/players for that team. |
| `pass_yards_gained_exp_team` | double | Team-level total expected pass_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rec_yards_gained_exp_team` | double | Team-level total expected rec_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rush_yards_gained_exp_team` | double | Team-level total expected rush_yards_gained_exp for a game, summed across all plays & players for that team. |
| `pass_touchdown_team` | double | Team-level total pass_touchdown for a game, summed across all plays/players for that team. |
| `rec_touchdown_team` | double | Team-level total rec_touchdown for a game, summed across all plays/players for that team. |
| `rush_touchdown_team` | double | Team-level total rush_touchdown for a game, summed across all plays/players for that team. |
| `pass_touchdown_exp_team` | double | Team-level total expected pass_touchdown_exp for a game, summed across all plays & players for that team. |
| `rec_touchdown_exp_team` | double | Team-level total expected rec_touchdown_exp for a game, summed across all plays & players for that team. |
| `rush_touchdown_exp_team` | double | Team-level total expected rush_touchdown_exp for a game, summed across all plays & players for that team. |
| `pass_two_point_conv_team` | double | Team-level total pass_two_point_conv for a game, summed across all plays/players for that team. |
| `rec_two_point_conv_team` | double | Team-level total rec_two_point_conv for a game, summed across all plays/players for that team. |
| `rush_two_point_conv_team` | double | Team-level total rush_two_point_conv for a game, summed across all plays/players for that team. |
| `pass_two_point_conv_exp_team` | double | Team-level total expected pass_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rec_two_point_conv_exp_team` | double | Team-level total expected rec_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rush_two_point_conv_exp_team` | double | Team-level total expected rush_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `pass_first_down_team` | double | Team-level total pass_first_down for a game, summed across all plays/players for that team. |
| `rec_first_down_team` | double | Team-level total rec_first_down for a game, summed across all plays/players for that team. |
| `rush_first_down_team` | double | Team-level total rush_first_down for a game, summed across all plays/players for that team. |
| `pass_first_down_exp_team` | double | Team-level total expected pass_first_down_exp for a game, summed across all plays & players for that team. |
| `rec_first_down_exp_team` | double | Team-level total expected rec_first_down_exp for a game, summed across all plays & players for that team. |
| `rush_first_down_exp_team` | double | Team-level total expected rush_first_down_exp for a game, summed across all plays & players for that team. |
| `pass_interception_team` | double | Team-level total pass_interception for a game, summed across all plays/players for that team. |
| `rec_interception_team` | double | Team-level total rec_interception for a game, summed across all plays/players for that team. |
| `pass_interception_exp_team` | double | Team-level total expected pass_interception_exp for a game, summed across all plays & players for that team. |
| `rec_interception_exp_team` | double | Team-level total expected rec_interception_exp for a game, summed across all plays & players for that team. |
| `rec_fumble_lost_team` | double | Team-level total rec_fumble_lost for a game, summed across all plays/players for that team. |
| `rush_fumble_lost_team` | double | Team-level total rush_fumble_lost for a game, summed across all plays/players for that team. |
| `pass_fantasy_points_exp_team` | double | Team-level total expected pass_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rec_fantasy_points_exp_team` | double | Team-level total expected rec_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rush_fantasy_points_exp_team` | double | Team-level total expected rush_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `pass_fantasy_points_team` | double | Team-level total pass_fantasy_points for a game, summed across all plays/players for that team. |
| `rec_fantasy_points_team` | double | Team-level total rec_fantasy_points for a game, summed across all plays/players for that team. |
| `rush_fantasy_points_team` | double | Team-level total rush_fantasy_points for a game, summed across all plays/players for that team. |
| `pass_completions_diff_team` | double | Team-level difference between actual and expected number of pass_completions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `receptions_diff_team` | double | Team-level difference between actual and expected number of receptions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_yards_gained_diff_team` | double | Team-level difference between actual and expected number of pass_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rec_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rush_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_touchdown_diff_team` | double | Team-level difference between actual and expected number of pass_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_touchdown_diff_team` | double | Team-level difference between actual and expected number of rec_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_touchdown_diff_team` | double | Team-level difference between actual and expected number of rush_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of pass_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rec_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rush_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_first_down_diff_team` | double | Team-level difference between actual and expected number of pass_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_first_down_diff_team` | double | Team-level difference between actual and expected number of rec_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_first_down_diff_team` | double | Team-level difference between actual and expected number of rush_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_interception_diff_team` | double | Team-level difference between actual and expected number of pass_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_interception_diff_team` | double | Team-level difference between actual and expected number of rec_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of pass_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rec_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rush_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_yards_gained_team` | double | Team-level total total_yards_gained for a game, summed across all plays/players for that team. |
| `total_yards_gained_exp_team` | double | Team-level total expected total_yards_gained_exp for a game, summed across all plays & players for that team. |
| `total_yards_gained_diff_team` | double | Team-level difference between actual and expected number of total_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_touchdown_team` | double | Team-level total total_touchdown for a game, summed across all plays/players for that team. |
| `total_touchdown_exp_team` | double | Team-level total expected total_touchdown_exp for a game, summed across all plays & players for that team. |
| `total_touchdown_diff_team` | double | Team-level difference between actual and expected number of total_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_first_down_team` | double | Team-level total total_first_down for a game, summed across all plays/players for that team. |
| `total_first_down_exp_team` | double | Team-level total expected total_first_down_exp for a game, summed across all plays & players for that team. |
| `total_first_down_diff_team` | double | Team-level difference between actual and expected number of total_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_fantasy_points_team` | double | Team-level total total_fantasy_points for a game, summed across all plays/players for that team. |
| `total_fantasy_points_exp_team` | double | Team-level total expected total_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `total_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of total_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

# Pass play-by-play opportunity stats

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

# Rush play-by-play opportunity stats with pinned model version

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### load_ff_playerids {#load_ff_playerids}

`load_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | character | MyFantasyLeague.com ID - this is the primary key for this table and is unique and complete. Usually an integer of 5 digits. |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `fantasypros_id` | character | FantasyPros.com ID - usually an integer of 5 digits. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `fleaflicker_id` | character | Fleaflicker ID - usual format is an integer with ~4 digits. Fleaflicker API also has sportradar and that's generally preferred. |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfbref_id` | character | College Football Reference ID - usual format is firstname-lastname-integer |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `rotoworld_id` | character | Rotoworld ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `ktc_id` | character | KeepTradeCut ID - usual format is an integer with ~four digits. |
| `stats_id` | character | Stats ID - usual format is five digit integer |
| `stats_global_id` | character | Stats Global ID - usual format is a six digit integer |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `swish_id` | character | Player ID for Swish Analytics |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `merge_name` | character | Name but formatted for name joins via ffscrapr::dp_cleannames() - coerced to lowercase, stripped of punctuation and suffixes, and common substitutions performed. |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `birthdate` | character | Birthdate |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_pick` | integer | Draft pick within round, i.e. 32nd pick of second round. |
| `draft_ovr` | integer | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `twitter_username` | character | Official twitter handle, if known |
| `height` | integer | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `db_season` | integer | Year of database build. Previous years may also be available via dynastyprocess. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

# Filter to active QBs

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### load_ff_rankings {#load_ff_rankings}

`load_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of `"draft"` (current draft rankings), `"week"` (weekly rankings), or `"all"` (full historical rankings). Defaults to `"draft"`. Kept for nflreadpy parity since its parameter is also called `type`; the forward-going preferred name is `kind`. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as `type`. If both are supplied, `kind` wins. If neither is supplied, defaults to `"draft"` via `type`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character | The relative url that the data was scraped from (add the prefix https://www.fantasypros.com/ to visit the page) |
| `page_type` | character | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `ecr_type` | character | A two letter identifier combining the ranking type (b = bestball; d = dynasty; r = redraft) and position type (o = overall; p = positional; sf = superflex; rk = rookie) |
| `player` | character | Player name |
| `id` | character | ID of the player in the 'name' column. |
| `pos` | character | Position as tracked by FP |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `ecr` | double | Average (mean) expert ranking for this player |
| `sd` | double | Standard deviation of expert rankings for this player |
| `best` | integer | The highest ranking given for this player by any one expert |
| `worst` | integer | The lowest ranking given for this player by any one expert |
| `sportsdata_id` | character | ID - also known as sportradar_id (they are equivalent!) |
| `player_filename` | character | base URL for this player on fantasypros.com |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |
| `player_owned_avg` | double | The average percentage this player is rostered across ESPN and Yahoo |
| `player_owned_espn` | character | The percentage that this player is rostered in ESPN leagues |
| `player_owned_yahoo` | character | The percentage that this player is rostered in Yahoo leagues |
| `player_image_url` | character | An image of the player |
| `player_square_image_url` | character | An square image of the player |
| `rank_delta` | integer | Change in ranks over a recent period |
| `bye` | integer | NFL bye week |
| `mergename` | character | Player name after being cleaned by dp_cleannames - generally strips punctuation and suffixes as well as performing common name substitutions. |
| `scrape_date` | character | Date this dataframe was last updated |
| `tm` | character | Team ID as used on MyFantasyLeague.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

# Weekly rankings

weekly = load_nfl_ff_rankings(kind="week")

# Full historical rankings (parquet)

history = load_nfl_ff_rankings(kind="all")

# nflreadpy-parity ``type=`` parameter (still supported)

draft = load_nfl_ff_rankings(type="draft")
```

### load_ftn_charting {#load_ftn_charting}

`load_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL FTN charting data going back to 2022

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `ftn_game_id` | integer | FTN game ID |
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `ftn_play_id` | integer | FTN play ID |
| `nflverse_play_id` | integer | Play ID used by nflverse, corresponds to GSIS play ID |
| `starting_hash` | character | hash the ball was place(L = left, M = middle, R = right) |
| `qb_location` | character | pre-snap position of quarterback(U = under center, S = shotgun, P = pistol) |
| `n_offense_backfield` | integer | number of players in the backfield at the snap |
| `n_defense_box` | integer | Number of defenders positioned in the box at the snap, as charted by FTN Data. |
| `is_no_huddle` | logical | no huddle |
| `is_motion` | logical | motion occurred on the play before or at the time of the snap |
| `is_play_action` | logical | play-action pass |
| `is_screen_pass` | logical | screen pass |
| `is_rpo` | logical | play is considered run-pass option |
| `is_trick_play` | logical | trick play |
| `is_qb_out_of_pocket` | logical | quarterback moved out of pocket |
| `is_interception_worthy` | logical | interception worthy pass |
| `is_throw_away` | logical | quarterback thrown away |
| `read_thrown` | character | read the ball was thrown |
| `is_catchable_ball` | logical | catchable ball(defined by throws that are generally on target that are not defended away) |
| `is_contested_ball` | logical | contested ball(defined by whether or not the receiver is facing physical contact at the time of the catch) |
| `is_created_reception` | logical | created reception(defined by a reception that only occurs due to an exceptional play by the receiver) |
| `is_drop` | logical | receiver drop |
| `is_qb_sneak` | logical | quarterback sneak |
| `n_blitzers` | integer | number of blitzers |
| `n_pass_rushers` | integer | number of pass rushers |
| `is_qb_fault_sack` | logical | sack that is the fault of the quarterback |
| `date_pulled` | character | Date the data was retrieved from the FTN Data API by nflverse jobs |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

# Multi-season range

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

# Filter to plays with motion

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### load_injuries {#load_injuries}

`load_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL injuries data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `week` | integer | Season week. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `position` | character | Primary position as reported by NFL.com |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `report_primary_injury` | character | Primary injury listed on official injury report |
| `report_secondary_injury` | character | Secondary injury listed on official injury report |
| `report_status` | character | Player's status for game on official injury report |
| `practice_primary_injury` | character | Primary injury listed on practice injury report |
| `practice_secondary_injury` | character | Secondary injury listed on practice injury report |
| `practice_status` | character | Player's participation in practice |
| `date_modified` | character | Date and time that injury information was updated |

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

# Multi-season range with team filter

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### load_nextgen_stats {#load_nextgen_stats}

`load_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats
accessors. Mirrors the API surface of nflreadpy's
`load_nextgen_stats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — `seasons` is applied as a post-filter on the `season` column. |
| `stat_type` | `str` | `'passing'` | One of `"passing"`, `"rushing"`, `"receiving"`. Defaults to `"passing"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested `stat_type` and `seasons`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

# Rushing NextGen stats

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

# Receiving NextGen stats with a follow-up filter

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

# Pandas round-trip

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### load_nfl_combine {#load_nfl_combine}

`load_nfl_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | double | Year that player was drafted |
| `draft_team` | character | Team that drafted player |
| `draft_round` | double | Round that player was drafted in |
| `draft_ovr` | double | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfb_id` | character | Sports Reference (CFB) ID for player |
| `player_name` | character | Full name of player |
| `pos` | character | Position as tracked by FP |
| `school` | character | College of player |
| `ht` | character | Height of player (feet and inches) |
| `wt` | double | Weight of player (lbs) |
| `forty` | double | Player's 40 yard dash time at combine (seconds) |
| `bench` | double | Reps benched by player at combine |
| `vertical` | double | Player's vertical jump at combine (inches) |
| `broad_jump` | double | Player's broad jump at combine (inches) |
| `cone` | double | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | double | Player's shuttle run time at combine (seconds) |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

# Filter by draft year and position

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### load_nfl_contracts {#load_nfl_contracts}

`load_nfl_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | logical | Active contract |
| `year_signed` | integer | Year the contract was signed |
| `years` | integer | Contract length |
| `value` | double | Total contract value |
| `apy` | double | Average money per contract year |
| `guaranteed` | double | Total guaranteed money |
| `apy_cap_pct` | double | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | double | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | double | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | double | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | character | Player's OverTheCap url |
| `otc_id` | integer | Over the Cap ID for player |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | character | Player date of birth (if published). |
| `height` | character | Official height, in inches |
| `weight` | character | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_overall` | integer | Overall draft selection number. |
| `draft_team` | character | Team that drafted player |
| `cols` | double | Number of contract columns returned in the contracts dataset (metadata artifact from the loader). |
| `season_history` | double | List of structs, one per league year covered by the contract (year as a string, team, base_salary, prorated_bonus, option_bonus, roster_bonus, guaranteed_salary, cap_number, cap_percent, cash_paid, workout_bonus, per_game_roster_bonus, other_bonus), money in millions of dollars and a final 'Total' row per nflreadr. |
| `contract_history` | integer | List of structs, one per contract in the player's OverTheCap contract history (team, contract_type, status, year_signed, yrs, total, apy, guarantees, amount_earned, percent_earned, effective_apy), with money fields in millions of dollars. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

# Pandas round-trip with sort by APY

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### load_nfl_draft_picks {#load_nfl_draft_picks}

`load_nfl_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | integer | Draft round |
| `pick` | integer | Draft overall pick |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `cfb_player_id` | character | ID from College Football Reference |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `hof` | logical | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | character | Primary position as reported by NFL.com |
| `category` | character | Broader category of player positions |
| `side` | character | O for offense, D for defense, S for special teams |
| `college` | character | Official college (usually the last one attended) |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | integer | Final season played in NFL |
| `allpro` | integer | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | integer | Number of Pro Bowls |
| `seasons_started` | integer | Number of seasons recorded as primary starter for position |
| `w_av` | integer | Weighted Approximate Value |
| `car_av` | logical | Career Approximate Value |
| `dr_av` | integer | Draft Approximate Value |
| `games` | integer | Games played in career |
| `pass_completions` | integer | Number of successful completions for a given game |
| `pass_attempts` | integer | Career pass attempts |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_tds` | integer | Career pass touchdowns thrown |
| `pass_ints` | integer | Career pass interceptions thrown |
| `rush_atts` | integer | Career rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `rush_tds` | integer | Career rushing touchdowns |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | integer | Career receiving yards |
| `rec_tds` | integer | Career receiving touchdowns |
| `def_solo_tackles` | integer | Career solo tackles |
| `def_ints` | integer | Career interceptions |
| `def_sacks` | double | Number of sacks form this player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

# Filter to a single year and round

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### load_nfl_espn_qbr {#load_nfl_espn_qbr}

`load_nfl_espn_qbr(seasons: 'List[int]', summary_type: 'str' = 'season', return_as_pandas: 'bool' = False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load ESPN Total QBR (Quarterback Rating) data going back to 2006.

Mirrors nflreadpy / nflreadr `load_espn_qbr` -- the lone nflreadpy dataset
that previously had no sdv-py loader. ESPN publishes Total QBR only from 2006
onward, so 2006 is the earliest available season (unlike the 1999 floor on
play-by-play). nflverse republishes ESPN's QBR through the `espn_data`
release as two combined files (one per `summary_type`), each covering all
seasons; this loader reads the requested file once and post-filters by
`season` (the same access pattern as `load_nfl_schedule`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to return. 2006 is the earliest available season. |
| `summary_type` | `str` | `'season'` | Aggregation level. `"season"` (default) returns one row per quarterback-season; `"week"` returns one row per quarterback-game. Any other value raises `ValueError`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which QBR release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse `espn_data` release. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_espn_qbr` release (built by `nfl-data` from ESPN's QBR web endpoint -- the same source nflverse's espnscrapeR uses). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing ESPN Total QBR for the requested seasons, summarized per `summary_type`.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (year) the Total QBR record covers. |
| `season_type` | character | Season segment for the record -- regular season or postseason. |
| `game_week` | character | Week scope of the QBR aggregation; for season-level rows this is the season-summary tag. |
| `team_abb` | character | Team abbreviation for the quarterback's team during the period. |
| `player_id` | character | ESPN athlete identifier for the quarterback. |
| `name_short` | character | Abbreviated display name of the quarterback (e.g. 'P. Mahomes'). |
| `rank` | double | Quarterback's rank by Total QBR among qualified passers for the period. |
| `qbr_total` | double | ESPN Total QBR on a 0-100 scale -- the headline opponent-adjusted quarterback rating. |
| `pts_added` | double | Points the quarterback added versus a league-average passer (ESPN QBR points-added component). |
| `qb_plays` | double | Count of qualifying quarterback action plays used to compute QBR. |
| `epa_total` | double | Total expected points added across the quarterback's plays (ESPN QBR EPA component). |
| `pass` | double | QBR points contribution from pass plays. |
| `run` | double | QBR points contribution from designed runs and scrambles. |
| `exp_sack` | double | QBR points contribution adjustment from expected sacks. |
| `penalty` | double | QBR points contribution from penalties attributed to the quarterback. |
| `qbr_raw` | double | Raw (non-opponent-adjusted) QBR for the period. |
| `sack` | double | QBR points contribution from sacks taken. |
| `name_first` | character | Quarterback's first name. |
| `name_last` | character | Quarterback's last name. |
| `name_display` | character | Quarterback's full display name. |
| `headshot_href` | character | URL of the quarterback's ESPN headshot image. |
| `team` | character | Full team name for the quarterback's team during the period. |
| `qualified` | logical | Whether the quarterback met ESPN's minimum action-play threshold to qualify for ranking. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_espn_qbr
qbr = load_nfl_espn_qbr(seasons=[2024])
qbr.shape

# Week-level QBR

qbr_week = load_nfl_espn_qbr(seasons=[2024], summary_type="week")

# Multi-season range

qbr = load_nfl_espn_qbr(seasons=range(2020, 2025))

# Pandas round-trip

qbr_pd = load_nfl_espn_qbr(seasons=[2024], return_as_pandas=True)
qbr_pd[["season", "team_abb", "qbr_total"]].head()
```
