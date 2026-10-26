---
title: "NFL — additional Python functions — nflverse data releases: combine–nfl_draft"
sidebar_label: "nflverse data releases: combine–nfl_draft"
sidebar_position: 5
description: "NFL — additional Python functions — nflverse data releases: combine–nfl_draft — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: combine–nfl_draft

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
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | Float64 | Year that player was drafted |
| `draft_team` | String | Team that drafted player |
| `draft_round` | Float64 | Round that player was drafted in |
| `draft_ovr` | Float64 | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | String | Pro-Football-Reference ID for player |
| `cfb_id` | String | Sports Reference (CFB) ID for player |
| `player_name` | String | Full name of player |
| `pos` | String | Position as tracked by FP |
| `school` | String | College of player |
| `ht` | String | Height of player (feet and inches) |
| `wt` | Float64 | Weight of player (lbs) |
| `forty` | Float64 | Player's 40 yard dash time at combine (seconds) |
| `bench` | Float64 | Reps benched by player at combine |
| `vertical` | Float64 | Player's vertical jump at combine (inches) |
| `broad_jump` | Float64 | Player's broad jump at combine (inches) |
| `cone` | Float64 | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | Float64 | Player's shuttle run time at combine (seconds) |

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
| `player` | String | Player name |
| `position` | String | Primary position as reported by NFL.com |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | Boolean | Active contract |
| `year_signed` | Int64 | Year the contract was signed |
| `years` | Int64 | Contract length |
| `value` | Float64 | Total contract value |
| `apy` | Float64 | Average money per contract year |
| `guaranteed` | Float64 | Total guaranteed money |
| `apy_cap_pct` | Float64 | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | Float64 | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | Float64 | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | Float64 | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | String | Player's OverTheCap url |
| `otc_id` | Int64 | Over the Cap ID for player |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | String |  |
| `height` | String | Official height, in inches |
| `weight` | String | Official weight, in pounds |
| `college` | String | Official college (usually the last one attended) |
| `draft_year` | Int64 | Year that player was drafted |
| `draft_round` | Int64 | Round that player was drafted in |
| `draft_overall` | Int64 |  |
| `draft_team` | String | Team that drafted player |
| `cols` | Float64 | Placeholder column retained in the contracts loader output schema; contains no meaningful data in this context. |
| `season_history` | Float64 | List of structs, one per league year covered by the contract (year as a string, team, base_salary, prorated_bonus, option_bonus, roster_bonus, guaranteed_salary, cap_number, cap_percent, cash_paid, workout_bonus, per_game_roster_bonus, other_bonus), money in millions of dollars and a final 'Total' row per nflreadr. |
| `contract_history` | Int64 | List of structs, one per contract in the player's OverTheCap contract history (team, contract_type, status, year_signed, yrs, total, apy, guarantees, amount_earned, percent_earned, effective_apy), with money fields in millions of dollars. |

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
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. `None` loads the current roster year (it turns on March 15), `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `club_code` | String | Three-letter team abbreviation identifying the NFL club on the depth chart row. |
| `week` | Int64 | Season week. |
| `game_type` | String | The most recent game type of that season that a player appeared on the roster. |
| `depth_team` | String | Numeric depth rank indicating whether the player is listed as the starter (1), backup (2), or further reserve on the depth chart. |
| `last_name` | String | Last name of player |
| `first_name` | String | First name of player |
| `football_name` | String | Common player name (i.e. in most cases common_first_name last_name) |
| `formation` | String | Offensive or defensive formation context in which the depth chart position applies (e.g., 'Shotgun', 'Nickel'). |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `jersey_number` | String | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | String | Primary position as reported by NFL.com |
| `elias_id` | String | Elias Sports Bureau identifier for the player, used by the NFL for official statistical tracking. |
| `depth_position` | String | Positional grouping label used to place the player on the team's official depth chart (e.g., 'QB', 'WR1', 'ILB'). |
| `full_name` | String | Full name as per NFL.com |

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
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | Int64 | Draft round |
| `pick` | Int64 | Draft overall pick |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | String | ID from Pro Football Reference |
| `cfb_player_id` | String | ID from College Football Reference |
| `pfr_player_name` | String | Player's name as recorded by PFR |
| `hof` | Boolean | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | String | Primary position as reported by NFL.com |
| `category` | String | Broader category of player positions |
| `side` | String | O for offense, D for defense, S for special teams |
| `college` | String | Official college (usually the last one attended) |
| `age` | Int64 | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | Int64 | Final season played in NFL |
| `allpro` | Int64 | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | Int64 | Number of Pro Bowls |
| `seasons_started` | Int64 | Number of seasons recorded as primary starter for position |
| `w_av` | Int64 | Weighted Approximate Value |
| `car_av` | Boolean | Career Approximate Value |
| `dr_av` | Int64 | Draft Approximate Value |
| `games` | Int64 | Games played in career |
| `pass_completions` | Int64 | Number of successful completions for a given game |
| `pass_attempts` | Int64 | Career pass attempts |
| `pass_yards` | Int64 | Number of yards gained on pass plays |
| `pass_tds` | Int64 | Career pass touchdowns thrown |
| `pass_ints` | Int64 | Career pass interceptions thrown |
| `rush_atts` | Int64 | Career rushing attempts |
| `rush_yards` | Int64 | The number of rushing yards gained |
| `rush_tds` | Int64 | Career rushing touchdowns |
| `receptions` | Int64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | Int64 | Career receiving yards |
| `rec_tds` | Int64 | Career receiving touchdowns |
| `def_solo_tackles` | Int64 | Career solo tackles |
| `def_ints` | Int64 | Career interceptions |
| `def_sacks` | Float64 | Number of sacks form this player |

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
| `seasons` | `list` |  | Seasons to return. 2006 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `summary_type` | `str` | `'season'` | Aggregation level. `"season"` (default) returns one row per quarterback-season; `"week"` returns one row per quarterback-game. Any other value raises `ValueError`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which QBR release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse `espn_data` release. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_espn_qbr` release (built by `nfl-data` from ESPN's QBR web endpoint -- the same source nflverse's espnscrapeR uses). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing ESPN Total QBR for the requested seasons, summarized per `summary_type`. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `game_week` | String | Season week |
| `team_abb` | String | Abbreviation of Team of Player |
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name_short` | String | Short name of player (First Initial, Last Name) |
| `rank` | Float64 | QBR Rank in specified timeframe |
| `qbr_total` | Float64 | Adjusted Total QBR, which adjusts quarterback play on 0-100 scale adjusted for strength of opposing defenses played. |
| `pts_added` | Float64 | Number of points contributed by a quarterback above the average level QB |
| `qb_plays` | Float64 | Total dropbacks for the quarterback (excludes handoffs) |
| `epa_total` | Float64 | Total Expected Points Added by quarterback, calculated by ESPN Win Probability Model |
| `pass` | Float64 | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `run` | Float64 | Expected Points Added on run plays |
| `exp_sack` | Float64 | Expected EPA Added on Sacks |
| `penalty` | Float64 | Binary indicator for whether or not a penalty occurred. |
| `qbr_raw` | Float64 | Raw total QBR, does not adjust for strength of opposing defenses played. |
| `sack` | Float64 | Binary indicator for if the play ended in a sack. |
| `name_first` | String | First Name of Quarterback |
| `name_last` | String | Last Name of Quarterback |
| `name_display` | String | Full Name of Quarterback |
| `headshot_href` | String | Link to ESPN Headshot of Player |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `qualified` | Boolean | True/False indicator of whether or not player meets minimum play requirement |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `summary_type` is not `"season"` / `"week"`, or if `source` is not one of `"nflverse"`, `None`, `"sportsdataverse"`, or `"sdv"`. |
| `SeasonNotFoundError` | If any requested season is before 2006. |

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
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `posteam` | String | String abbreviation for the team with possession. |
| `week` | Float64 | Season week. |
| `game_id` | String | Ten digit identifier for NFL game. |
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `full_name` | String | Full name as per NFL.com |
| `position` | String | Primary position as reported by NFL.com |
| `pass_attempt` | Float64 | Binary indicator for if the play was a pass attempt (includes sacks). |
| `rec_attempt` | Float64 | Total number of targets for a given game |
| `rush_attempt` | Float64 | Binary indicator for if the play was a run. |
| `pass_air_yards` | Float64 | Total air yards thrown for a given game |
| `rec_air_yards` | Float64 | Total air yards on receiving attempts for a given game |
| `pass_completions` | Float64 | Number of successful completions for a given game |
| `receptions` | Float64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `pass_completions_exp` | Float64 | Expected number of pass_completions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `receptions_exp` | Float64 | Expected number of receptions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_yards_gained` | Float64 | Total passing yards gained for a given game |
| `rec_yards_gained` | Float64 | Total receiving yards gained for a given game |
| `rush_yards_gained` | Float64 | Total rushing yards gained for a given game |
| `pass_yards_gained_exp` | Float64 | Expected number of pass_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_yards_gained_exp` | Float64 | Expected number of rec_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_yards_gained_exp` | Float64 | Expected number of rush_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_touchdown` | Float64 | Binary indicator for if the play resulted in a passing TD. |
| `rec_touchdown` | Float64 | Total receiving touchdowns |
| `rush_touchdown` | Float64 | Binary indicator for if the play resulted in a rushing TD. |
| `pass_touchdown_exp` | Float64 | Expected number of pass_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_touchdown_exp` | Float64 | Expected number of rec_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_touchdown_exp` | Float64 | Expected number of rush_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_two_point_conv` | Float64 | Number of successful passing two point conversions |
| `rec_two_point_conv` | Float64 | Number of successful receiving two point conversions |
| `rush_two_point_conv` | Float64 | Number of successful rushing two point conversions |
| `pass_two_point_conv_exp` | Float64 | Expected number of pass_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_two_point_conv_exp` | Float64 | Expected number of rec_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_two_point_conv_exp` | Float64 | Expected number of rush_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_first_down` | Float64 | Number of passing first downs |
| `rec_first_down` | Float64 | Number of receiving first downs |
| `rush_first_down` | Float64 | Number of rushing first downs |
| `pass_first_down_exp` | Float64 | Expected number of pass_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_first_down_exp` | Float64 | Expected number of rec_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_first_down_exp` | Float64 | Expected number of rush_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_interception` | Float64 | Number of interceptions thrown |
| `rec_interception` | Float64 | Number of interceptions on targets |
| `pass_interception_exp` | Float64 | Expected number of pass_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_interception_exp` | Float64 | Expected number of rec_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fumble_lost` | Float64 | Number of fumbles on receiving attempts |
| `rush_fumble_lost` | Float64 | Number of fumbles on rushing attempts |
| `pass_fantasy_points_exp` | Float64 | Expected number of pass_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fantasy_points_exp` | Float64 | Expected number of rec_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_fantasy_points_exp` | Float64 | Expected number of rush_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_fantasy_points` | Float64 | Total fantasy points from passing, assuming 0.04 points per pass yard, 4 points per pass TD, -2 points per interception |
| `rec_fantasy_points` | Float64 | Total fantasy points from receiving, assuming PPR scoring |
| `rush_fantasy_points` | Float64 | Total fantasy points from rushing, assuming PPR scoring |
| `total_yards_gained` | Float64 | Total scrimmage yards (sum of pass, rush, and receiving yards) |
| `total_yards_gained_exp` | Float64 | Expected number of total_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_touchdown` | Float64 | Total touchdowns (sum of pass, rush, and receiving touchdowns) |
| `total_touchdown_exp` | Float64 | Expected number of total_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_first_down` | Float64 | Total first downs (sum of pass, rush, and receiving first downs) |
| `total_first_down_exp` | Float64 | Expected number of total_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_fantasy_points` | Float64 | Total fantasy points (sum of pass, rush, and receiving fantasy points) |
| `total_fantasy_points_exp` | Float64 | Expected number of total_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_completions_diff` | Float64 | Difference between actual and expected number of pass_completions - often interpreted as efficiency for a given play/game |
| `receptions_diff` | Float64 | Difference between actual and expected number of receptions - often interpreted as efficiency for a given play/game |
| `pass_yards_gained_diff` | Float64 | Difference between actual and expected number of pass_yards_gained - often interpreted as efficiency for a given play/game |
| `rec_yards_gained_diff` | Float64 | Difference between actual and expected number of rec_yards_gained - often interpreted as efficiency for a given play/game |
| `rush_yards_gained_diff` | Float64 | Difference between actual and expected number of rush_yards_gained - often interpreted as efficiency for a given play/game |
| `pass_touchdown_diff` | Float64 | Difference between actual and expected number of pass_touchdown - often interpreted as efficiency for a given play/game |
| `rec_touchdown_diff` | Float64 | Difference between actual and expected number of rec_touchdown - often interpreted as efficiency for a given play/game |
| `rush_touchdown_diff` | Float64 | Difference between actual and expected number of rush_touchdown - often interpreted as efficiency for a given play/game |
| `pass_two_point_conv_diff` | Float64 | Difference between actual and expected number of pass_two_point_conv - often interpreted as efficiency for a given play/game |
| `rec_two_point_conv_diff` | Float64 | Difference between actual and expected number of rec_two_point_conv - often interpreted as efficiency for a given play/game |
| `rush_two_point_conv_diff` | Float64 | Difference between actual and expected number of rush_two_point_conv - often interpreted as efficiency for a given play/game |
| `pass_first_down_diff` | Float64 | Difference between actual and expected number of pass_first_down - often interpreted as efficiency for a given play/game |
| `rec_first_down_diff` | Float64 | Difference between actual and expected number of rec_first_down - often interpreted as efficiency for a given play/game |
| `rush_first_down_diff` | Float64 | Difference between actual and expected number of rush_first_down - often interpreted as efficiency for a given play/game |
| `pass_interception_diff` | Float64 | Difference between actual and expected number of pass_interception - often interpreted as efficiency for a given play/game |
| `rec_interception_diff` | Float64 | Difference between actual and expected number of rec_interception - often interpreted as efficiency for a given play/game |
| `pass_fantasy_points_diff` | Float64 | Difference between actual and expected number of pass_fantasy_points - often interpreted as efficiency for a given play/game |
| `rec_fantasy_points_diff` | Float64 | Difference between actual and expected number of rec_fantasy_points - often interpreted as efficiency for a given play/game |
| `rush_fantasy_points_diff` | Float64 | Difference between actual and expected number of rush_fantasy_points - often interpreted as efficiency for a given play/game |
| `total_yards_gained_diff` | Float64 | Difference between actual and expected number of total_yards_gained - often interpreted as efficiency for a given play/game |
| `total_touchdown_diff` | Float64 | Difference between actual and expected number of total_touchdown - often interpreted as efficiency for a given play/game |
| `total_first_down_diff` | Float64 | Difference between actual and expected number of total_first_down - often interpreted as efficiency for a given play/game |
| `total_fantasy_points_diff` | Float64 | Difference between actual and expected number of total_fantasy_points - often interpreted as efficiency for a given play/game |
| `pass_attempt_team` | Float64 | Team-level total pass_attempt for a game, summed across all plays/players for that team. |
| `rec_attempt_team` | Float64 | Team-level total rec_attempt for a game, summed across all plays/players for that team. |
| `rush_attempt_team` | Float64 | Team-level total rush_attempt for a game, summed across all plays/players for that team. |
| `pass_air_yards_team` | Float64 | Team-level total pass_air_yards for a game, summed across all plays/players for that team. |
| `rec_air_yards_team` | Float64 | Team-level total rec_air_yards for a game, summed across all plays/players for that team. |
| `pass_completions_team` | Float64 | Team-level total pass_completions for a game, summed across all plays/players for that team. |
| `receptions_team` | Float64 | Team-level total receptions for a game, summed across all plays/players for that team. |
| `pass_completions_exp_team` | Float64 | Team-level total expected pass_completions_exp for a game, summed across all plays & players for that team. |
| `receptions_exp_team` | Float64 | Team-level total expected receptions_exp for a game, summed across all plays & players for that team. |
| `pass_yards_gained_team` | Float64 | Team-level total pass_yards_gained for a game, summed across all plays/players for that team. |
| `rec_yards_gained_team` | Float64 | Team-level total rec_yards_gained for a game, summed across all plays/players for that team. |
| `rush_yards_gained_team` | Float64 | Team-level total rush_yards_gained for a game, summed across all plays/players for that team. |
| `pass_yards_gained_exp_team` | Float64 | Team-level total expected pass_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rec_yards_gained_exp_team` | Float64 | Team-level total expected rec_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rush_yards_gained_exp_team` | Float64 | Team-level total expected rush_yards_gained_exp for a game, summed across all plays & players for that team. |
| `pass_touchdown_team` | Float64 | Team-level total pass_touchdown for a game, summed across all plays/players for that team. |
| `rec_touchdown_team` | Float64 | Team-level total rec_touchdown for a game, summed across all plays/players for that team. |
| `rush_touchdown_team` | Float64 | Team-level total rush_touchdown for a game, summed across all plays/players for that team. |
| `pass_touchdown_exp_team` | Float64 | Team-level total expected pass_touchdown_exp for a game, summed across all plays & players for that team. |
| `rec_touchdown_exp_team` | Float64 | Team-level total expected rec_touchdown_exp for a game, summed across all plays & players for that team. |
| `rush_touchdown_exp_team` | Float64 | Team-level total expected rush_touchdown_exp for a game, summed across all plays & players for that team. |
| `pass_two_point_conv_team` | Float64 | Team-level total pass_two_point_conv for a game, summed across all plays/players for that team. |
| `rec_two_point_conv_team` | Float64 | Team-level total rec_two_point_conv for a game, summed across all plays/players for that team. |
| `rush_two_point_conv_team` | Float64 | Team-level total rush_two_point_conv for a game, summed across all plays/players for that team. |
| `pass_two_point_conv_exp_team` | Float64 | Team-level total expected pass_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rec_two_point_conv_exp_team` | Float64 | Team-level total expected rec_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rush_two_point_conv_exp_team` | Float64 | Team-level total expected rush_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `pass_first_down_team` | Float64 | Team-level total pass_first_down for a game, summed across all plays/players for that team. |
| `rec_first_down_team` | Float64 | Team-level total rec_first_down for a game, summed across all plays/players for that team. |
| `rush_first_down_team` | Float64 | Team-level total rush_first_down for a game, summed across all plays/players for that team. |
| `pass_first_down_exp_team` | Float64 | Team-level total expected pass_first_down_exp for a game, summed across all plays & players for that team. |
| `rec_first_down_exp_team` | Float64 | Team-level total expected rec_first_down_exp for a game, summed across all plays & players for that team. |
| `rush_first_down_exp_team` | Float64 | Team-level total expected rush_first_down_exp for a game, summed across all plays & players for that team. |
| `pass_interception_team` | Float64 | Team-level total pass_interception for a game, summed across all plays/players for that team. |
| `rec_interception_team` | Float64 | Team-level total rec_interception for a game, summed across all plays/players for that team. |
| `pass_interception_exp_team` | Float64 | Team-level total expected pass_interception_exp for a game, summed across all plays & players for that team. |
| `rec_interception_exp_team` | Float64 | Team-level total expected rec_interception_exp for a game, summed across all plays & players for that team. |
| `rec_fumble_lost_team` | Float64 | Team-level total rec_fumble_lost for a game, summed across all plays/players for that team. |
| `rush_fumble_lost_team` | Float64 | Team-level total rush_fumble_lost for a game, summed across all plays/players for that team. |
| `pass_fantasy_points_exp_team` | Float64 | Team-level total expected pass_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rec_fantasy_points_exp_team` | Float64 | Team-level total expected rec_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rush_fantasy_points_exp_team` | Float64 | Team-level total expected rush_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `pass_fantasy_points_team` | Float64 | Team-level total pass_fantasy_points for a game, summed across all plays/players for that team. |
| `rec_fantasy_points_team` | Float64 | Team-level total rec_fantasy_points for a game, summed across all plays/players for that team. |
| `rush_fantasy_points_team` | Float64 | Team-level total rush_fantasy_points for a game, summed across all plays/players for that team. |
| `pass_completions_diff_team` | Float64 | Team-level difference between actual and expected number of pass_completions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `receptions_diff_team` | Float64 | Team-level difference between actual and expected number of receptions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_yards_gained_diff_team` | Float64 | Team-level difference between actual and expected number of pass_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_yards_gained_diff_team` | Float64 | Team-level difference between actual and expected number of rec_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_yards_gained_diff_team` | Float64 | Team-level difference between actual and expected number of rush_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_touchdown_diff_team` | Float64 | Team-level difference between actual and expected number of pass_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_touchdown_diff_team` | Float64 | Team-level difference between actual and expected number of rec_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_touchdown_diff_team` | Float64 | Team-level difference between actual and expected number of rush_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_two_point_conv_diff_team` | Float64 | Team-level difference between actual and expected number of pass_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_two_point_conv_diff_team` | Float64 | Team-level difference between actual and expected number of rec_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_two_point_conv_diff_team` | Float64 | Team-level difference between actual and expected number of rush_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_first_down_diff_team` | Float64 | Team-level difference between actual and expected number of pass_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_first_down_diff_team` | Float64 | Team-level difference between actual and expected number of rec_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_first_down_diff_team` | Float64 | Team-level difference between actual and expected number of rush_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_interception_diff_team` | Float64 | Team-level difference between actual and expected number of pass_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_interception_diff_team` | Float64 | Team-level difference between actual and expected number of rec_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_fantasy_points_diff_team` | Float64 | Team-level difference between actual and expected number of pass_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_fantasy_points_diff_team` | Float64 | Team-level difference between actual and expected number of rec_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_fantasy_points_diff_team` | Float64 | Team-level difference between actual and expected number of rush_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_yards_gained_team` | Float64 | Team-level total total_yards_gained for a game, summed across all plays/players for that team. |
| `total_yards_gained_exp_team` | Float64 | Team-level total expected total_yards_gained_exp for a game, summed across all plays & players for that team. |
| `total_yards_gained_diff_team` | Float64 | Team-level difference between actual and expected number of total_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_touchdown_team` | Float64 | Team-level total total_touchdown for a game, summed across all plays/players for that team. |
| `total_touchdown_exp_team` | Float64 | Team-level total expected total_touchdown_exp for a game, summed across all plays & players for that team. |
| `total_touchdown_diff_team` | Float64 | Team-level difference between actual and expected number of total_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_first_down_team` | Float64 | Team-level total total_first_down for a game, summed across all plays/players for that team. |
| `total_first_down_exp_team` | Float64 | Team-level total expected total_first_down_exp for a game, summed across all plays & players for that team. |
| `total_first_down_diff_team` | Float64 | Team-level difference between actual and expected number of total_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_fantasy_points_team` | Float64 | Team-level total total_fantasy_points for a game, summed across all plays/players for that team. |
| `total_fantasy_points_exp_team` | Float64 | Team-level total expected total_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `total_fantasy_points_diff_team` | Float64 | Team-level difference between actual and expected number of total_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is less than 2006, or if `stat_type` / `model_version` are not allowed values. |

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
| `mfl_id` | String | MyFantasyLeague.com ID - this is the primary key for this table and is unique and complete. Usually an integer of 5 digits. |
| `sportradar_id` | String | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `fantasypros_id` | String | FantasyPros.com ID - usually an integer of 5 digits. |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pff_id` | String | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `sleeper_id` | String | Sleeper ID - usually an integer with ~4 digits. |
| `nfl_id` | String | NFL ID of player (this is used in Big Data Bowl Data) |
| `espn_id` | String | ESPN ID - usual format is an integer with ~5 digits |
| `yahoo_id` | String | Yahoo ID - usual format is an integer with ~5 digits |
| `fleaflicker_id` | String | Fleaflicker ID - usual format is an integer with ~4 digits. Fleaflicker API also has sportradar and that's generally preferred. |
| `cbs_id` | String | CBS ID - usual format is an integer with ~ 7 digits. |
| `pfr_id` | String | Pro-Football-Reference ID for player |
| `cfbref_id` | String | College Football Reference ID - usual format is firstname-lastname-integer |
| `rotowire_id` | String | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `rotoworld_id` | String | Rotoworld ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `ktc_id` | String | KeepTradeCut ID - usual format is an integer with ~four digits. |
| `stats_id` | String | Stats ID - usual format is five digit integer |
| `stats_global_id` | String | Stats Global ID - usual format is a six digit integer |
| `fantasy_data_id` | String | FantasyData ID - usual format five digit integer |
| `swish_id` | String | Player ID for Swish Analytics |
| `name` | String | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `merge_name` | String | Name but formatted for name joins via ffscrapr::dp_cleannames() - coerced to lowercase, stripped of punctuation and suffixes, and common substitutions performed. |
| `position` | String | Primary position as reported by NFL.com |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `birthdate` | String | Birthdate |
| `age` | Float64 | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `draft_year` | Int64 | Year that player was drafted |
| `draft_round` | Int64 | Round that player was drafted in |
| `draft_pick` | Int64 | Draft pick within round, i.e. 32nd pick of second round. |
| `draft_ovr` | Int64 | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `twitter_username` | String | Official twitter handle, if known |
| `height` | Int64 | Official height, in inches |
| `weight` | Int64 | Official weight, in pounds |
| `college` | String | Official college (usually the last one attended) |
| `db_season` | Int64 | Year of database build. Previous years may also be available via dynastyprocess. |

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

`load_ff_rankings(type: 'str' = 'draft', kind: 'str | None' = None, return_as_pandas=False) -> 'pl.DataFrame'`

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
| `fp_page` | String | The relative url that the data was scraped from (add the prefix https://www.fantasypros.com/ to visit the page) |
| `page_type` | String | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `ecr_type` | String | A two letter identifier combining the ranking type (b = bestball; d = dynasty; r = redraft) and position type (o = overall; p = positional; sf = superflex; rk = rookie) |
| `player` | String | Player name |
| `id` | String | ID of the player in the 'name' column. |
| `pos` | String | Position as tracked by FP |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `ecr` | Float64 | Average (mean) expert ranking for this player |
| `sd` | Float64 | Standard deviation of expert rankings for this player |
| `best` | Int64 | The highest ranking given for this player by any one expert |
| `worst` | Int64 | The lowest ranking given for this player by any one expert |
| `sportsdata_id` | String | ID - also known as sportradar_id (they are equivalent!) |
| `player_filename` | String | base URL for this player on fantasypros.com |
| `yahoo_id` | String | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | String | CBS ID - usual format is an integer with ~ 7 digits. |
| `player_owned_avg` | Float64 | The average percentage this player is rostered across ESPN and Yahoo |
| `player_owned_espn` | String | The percentage that this player is rostered in ESPN leagues |
| `player_owned_yahoo` | String | The percentage that this player is rostered in Yahoo leagues |
| `player_image_url` | String | An image of the player |
| `player_square_image_url` | String | An square image of the player |
| `rank_delta` | Int64 | Change in ranks over a recent period |
| `bye` | Int64 | NFL bye week |
| `mergename` | String | Player name after being cleaned by dp_cleannames - generally strips punctuation and suffixes as well as performing common name substitutions. |
| `scrape_date` | String | Date this dataframe was last updated |
| `tm` | String | Team ID as used on MyFantasyLeague.com |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the resolved value is not one of the allowed values. |

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
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `ftn_game_id` | Int64 | FTN game ID |
| `nflverse_game_id` | String | nflverse identifier for games. Format is season, week, away_team, home_team |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `ftn_play_id` | Int64 | FTN play ID |
| `nflverse_play_id` | Int64 | Play ID used by nflverse, corresponds to GSIS play ID |
| `starting_hash` | String | hash the ball was place(L = left, M = middle, R = right) |
| `qb_location` | String | pre-snap position of quarterback(U = under center, S = shotgun, P = pistol) |
| `n_offense_backfield` | Int64 | number of players in the backfield at the snap |
| `n_defense_box` | Int64 | Number of defenders positioned in the box at the snap, as charted by FTN Data. |
| `is_no_huddle` | Boolean | no huddle |
| `is_motion` | Boolean | motion occurred on the play before or at the time of the snap |
| `is_play_action` | Boolean | play-action pass |
| `is_screen_pass` | Boolean | screen pass |
| `is_rpo` | Boolean | play is considered run-pass option |
| `is_trick_play` | Boolean | trick play |
| `is_qb_out_of_pocket` | Boolean | quarterback moved out of pocket |
| `is_interception_worthy` | Boolean | interception worthy pass |
| `is_throw_away` | Boolean | quarterback thrown away |
| `read_thrown` | String | read the ball was thrown |
| `is_catchable_ball` | Boolean | catchable ball(defined by throws that are generally on target that are not defended away) |
| `is_contested_ball` | Boolean | contested ball(defined by whether or not the receiver is facing physical contact at the time of the catch) |
| `is_created_reception` | Boolean | created reception(defined by a reception that only occurs due to an exceptional play by the receiver) |
| `is_drop` | Boolean | receiver drop |
| `is_qb_sneak` | Boolean | quarterback sneak |
| `n_blitzers` | Int64 | number of blitzers |
| `n_pass_rushers` | Int64 | number of pass rushers |
| `is_qb_fault_sack` | Boolean | sack that is the fault of the quarterback |
| `date_pulled` | String | Date the data was retrieved from the FTN Data API by nflverse jobs |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is less than 2022. |

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
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | String | The most recent game type of that season that a player appeared on the roster. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `week` | Int64 | Season week. |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `position` | String | Primary position as reported by NFL.com |
| `full_name` | String | Full name as per NFL.com |
| `first_name` | String | First name of player |
| `last_name` | String | Last name of player |
| `report_primary_injury` | String | Primary injury listed on official injury report |
| `report_secondary_injury` | String | Secondary injury listed on official injury report |
| `report_status` | String | Player's status for game on official injury report |
| `practice_primary_injury` | String | Primary injury listed on practice injury report |
| `practice_secondary_injury` | String | Secondary injury listed on practice injury report |
| `practice_status` | String | Player's participation in practice |
| `date_modified` | String | Date and time that injury information was updated |

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
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — `seasons` is applied as a post-filter on the `season` column. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `stat_type` | `str` | `'passing'` | One of `"passing"`, `"rushing"`, `"receiving"`. Defaults to `"passing"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested `stat_type` and `seasons`. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | Int64 | Season week. |
| `player_display_name` | String | Full name of the player |
| `player_position` | String | Position of the player accordinng to NGS |
| `team_abbr` | String | Official team abbreveation |
| `avg_time_to_throw` | Float64 | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | Float64 | Average air yards on completed passes |
| `avg_intended_air_yards` | Float64 | Average air yards on all attempted passes |
| `avg_air_yards_differential` | Float64 | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | Float64 | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | Float64 | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | Float64 | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | Int64 | The number of pass attempts as defined by the NFL. |
| `pass_yards` | Int64 | Number of yards gained on pass plays |
| `pass_touchdowns` | Int64 | Number of touchdowns scored on pass plays |
| `interceptions` | Int64 | The number of interceptions thrown. |
| `passer_rating` | Float64 | Overall NFL passer rating |
| `completions` | Int64 | The number of completed passes. |
| `completion_percentage` | Float64 | Percentage of completed passes |
| `expected_completion_percentage` | Float64 | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | Float64 | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | Float64 | A receiver's average depth of target |
| `max_air_distance` | Float64 | A receiver's maximum depth of target |
| `player_gsis_id` | String | Unique identifier of the player |
| `player_first_name` | String | Player's first name |
| `player_last_name` | String | Player's last name |
| `player_jersey_number` | Int64 | Player's jersey number |
| `player_short_name` | String | Short version of player's name |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `stat_type` is not one of the allowed values. |
| `SeasonNotFoundError` | If any season is less than 2016. |

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
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | Float64 | Year that player was drafted |
| `draft_team` | String | Team that drafted player |
| `draft_round` | Float64 | Round that player was drafted in |
| `draft_ovr` | Float64 | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | String | Pro-Football-Reference ID for player |
| `cfb_id` | String | Sports Reference (CFB) ID for player |
| `player_name` | String | Full name of player |
| `pos` | String | Position as tracked by FP |
| `school` | String | College of player |
| `ht` | String | Height of player (feet and inches) |
| `wt` | Float64 | Weight of player (lbs) |
| `forty` | Float64 | Player's 40 yard dash time at combine (seconds) |
| `bench` | Float64 | Reps benched by player at combine |
| `vertical` | Float64 | Player's vertical jump at combine (inches) |
| `broad_jump` | Float64 | Player's broad jump at combine (inches) |
| `cone` | Float64 | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | Float64 | Player's shuttle run time at combine (seconds) |

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
| `player` | String | Player name |
| `position` | String | Primary position as reported by NFL.com |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | Boolean | Active contract |
| `year_signed` | Int64 | Year the contract was signed |
| `years` | Int64 | Contract length |
| `value` | Float64 | Total contract value |
| `apy` | Float64 | Average money per contract year |
| `guaranteed` | Float64 | Total guaranteed money |
| `apy_cap_pct` | Float64 | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | Float64 | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | Float64 | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | Float64 | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | String | Player's OverTheCap url |
| `otc_id` | Int64 | Over the Cap ID for player |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | String |  |
| `height` | String | Official height, in inches |
| `weight` | String | Official weight, in pounds |
| `college` | String | Official college (usually the last one attended) |
| `draft_year` | Int64 | Year that player was drafted |
| `draft_round` | Int64 | Round that player was drafted in |
| `draft_overall` | Int64 |  |
| `draft_team` | String | Team that drafted player |
| `cols` | Float64 | Number of contract columns returned in the contracts dataset (metadata artifact from the loader). |
| `season_history` | Float64 | List of structs, one per league year covered by the contract (year as a string, team, base_salary, prorated_bonus, option_bonus, roster_bonus, guaranteed_salary, cap_number, cap_percent, cash_paid, workout_bonus, per_game_roster_bonus, other_bonus), money in millions of dollars and a final 'Total' row per nflreadr. |
| `contract_history` | Int64 | List of structs, one per contract in the player's OverTheCap contract history (team, contract_type, status, year_signed, yrs, total, apy, guarantees, amount_earned, percent_earned, effective_apy), with money fields in millions of dollars. |

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
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | Int64 | Draft round |
| `pick` | Int64 | Draft overall pick |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | String | ID from Pro Football Reference |
| `cfb_player_id` | String | ID from College Football Reference |
| `pfr_player_name` | String | Player's name as recorded by PFR |
| `hof` | Boolean | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | String | Primary position as reported by NFL.com |
| `category` | String | Broader category of player positions |
| `side` | String | O for offense, D for defense, S for special teams |
| `college` | String | Official college (usually the last one attended) |
| `age` | Int64 | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | Int64 | Final season played in NFL |
| `allpro` | Int64 | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | Int64 | Number of Pro Bowls |
| `seasons_started` | Int64 | Number of seasons recorded as primary starter for position |
| `w_av` | Int64 | Weighted Approximate Value |
| `car_av` | Boolean | Career Approximate Value |
| `dr_av` | Int64 | Draft Approximate Value |
| `games` | Int64 | Games played in career |
| `pass_completions` | Int64 | Number of successful completions for a given game |
| `pass_attempts` | Int64 | Career pass attempts |
| `pass_yards` | Int64 | Number of yards gained on pass plays |
| `pass_tds` | Int64 | Career pass touchdowns thrown |
| `pass_ints` | Int64 | Career pass interceptions thrown |
| `rush_atts` | Int64 | Career rushing attempts |
| `rush_yards` | Int64 | The number of rushing yards gained |
| `rush_tds` | Int64 | Career rushing touchdowns |
| `receptions` | Int64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | Int64 | Career receiving yards |
| `rec_tds` | Int64 | Career receiving touchdowns |
| `def_solo_tackles` | Int64 | Career solo tackles |
| `def_ints` | Int64 | Career interceptions |
| `def_sacks` | Float64 | Number of sacks form this player |

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
