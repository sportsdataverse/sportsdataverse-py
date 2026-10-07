---
title: "NFL — additional Python functions — Dataset loaders: nfl_ff–nfl_pfr"
sidebar_label: "Dataset loaders: nfl_ff–nfl_pfr"
sidebar_position: 4
description: "NFL — additional Python functions — Dataset loaders: nfl_ff–nfl_pfr — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Dataset loaders: nfl_ff–nfl_pfr

### load_nfl_ff_opportunity {#load_nfl_ff_opportunity}

`load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_nfl_ff_playerids {#load_nfl_ff_playerids}

`load_nfl_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_nfl_ff_rankings {#load_nfl_ff_rankings}

`load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_nfl_fp_curve {#load_nfl_fp_curve}

`load_nfl_fp_curve() -> 'pl.DataFrame'`

Load the bundled NFL EP-by-yardline curve (no network).

**Returns**

`yardline_own: Int64 (1..99), ep: Float64`.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99); one row per yard line of the bundled NFL EP-by-starting-yardline curve. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_fp_curve
curve = load_nfl_fp_curve()
curve.filter(curve["yardline_own"] == 30)
```

### load_nfl_nextgen_stats {#load_nfl_nextgen_stats}

`load_nfl_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

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

### load_nfl_ngs_passing {#load_nfl_ngs_passing}

`load_nfl_ngs_passing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_nextgen_stats(stat_type='passing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


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
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

### load_nfl_ngs_receiving {#load_nfl_ngs_receiving}

`load_nfl_ngs_receiving(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_nextgen_stats(stat_type='receiving')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_cushion` | double | The distance (in yards) measured between a WR/TE and the defender they're lined up against at the time of snap on all targets. |
| `avg_separation` | double | The distance (in yards) measured between a WR/TE and the nearest defender at the time of catch or incompletion. |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `percent_share_of_intended_air_yards` | double | The sum of the receivers total intended air yards (all attempts) over the sum of his team's total intended air yards. Represented as a percentage, this statistic represents how much of a team's deep yards does the player account for. |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `catch_percentage` | double | Percentage of caught passes relative to targets |
| `yards` | integer | The number of receiving yards |
| `rec_touchdowns` | integer | The number of touchdown receptions |
| `avg_yac` | double | Average yards gained after catch by a receiver. |
| `avg_expected_yac` | double | Average expected yards after catch, based on numerous factors using tracking data such as how open the receiver is, how fast they're traveling, how many defenders/blockers are in space, etc |
| `avg_yac_above_expectation` | double | A receiver's YAC compared to their Expected YAC. |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
```

### load_nfl_ngs_rushing {#load_nfl_ngs_rushing}

`load_nfl_ngs_rushing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_nextgen_stats(stat_type='rushing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `efficiency` | double | Rushing efficiency is calculated by taking the total distance a player traveled on rushing plays as a ball carrier according to Next Gen Stats (measured in yards) per rushing yards gained. The lower the number, the more of a North/South runner. |
| `percent_attempts_gte_eight_defenders` | double | On every play, Next Gen Stats calculates how many defenders are stacked in the box at snap. Using that logic, DIB% calculates how often does a rusher see 8 or more defenders in the box against them. |
| `avg_time_to_los` | double | Next Gen Stats measures the amount of time a ball carrier spends (measured to the 10th of a second) before crossing the Line of Scrimmage. TLOS is the average time behind the LOS on all rushing plays where the player is the rusher. |
| `rush_attempts` | integer | The number of rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `avg_rush_yards` | double | AVerage rush yards gained |
| `rush_touchdowns` | integer | The number of scored rushing touchdowns |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |
| `expected_rush_yards` | double | Expected rushing yards based on Nextgenstats' Big Data Bowl model |
| `rush_yards_over_expected` | double | A rusher's rush yards gained compared to the expected rush yards |
| `rush_yards_over_expected_per_att` | double | Average rush yards above expectation |
| `rush_pct_over_expected` | double | Rushing percentage above expectation |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

### load_nfl_officials {#load_nfl_officials}

`load_nfl_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character | Unique nflverse game identifier linking the officiating record to a specific NFL game. |
| `official_name` | character |  |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character |  |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

# Pandas round-trip

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### load_nfl_pfr_advstats {#load_nfl_pfr_advstats}

`load_nfl_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy's
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double | Total number of catchable passes thrown by the passer that were dropped by receivers per Pro Football Reference. |
| `passing_drop_pct` | double | Percentage of a passer's catchable targets that were dropped by the intended receiver. |
| `receiving_drop` | double | Number of catchable targets the receiver dropped during the game or season period per Pro Football Reference. |
| `receiving_drop_pct` | double | Percentage of the receiver's catchable targets that were dropped during the game or season period. |
| `passing_bad_throws` | double | Total number of passes thrown by the passer that were classified as inaccurate or poor-quality throws per Pro Football Reference. |
| `passing_bad_throw_pct` | double | Percentage of a passer's attempts classified as bad throws (inaccurate, off-target, or uncatchable). |
| `times_sacked` | double | Total number of times the passer was sacked during the game or season period per Pro Football Reference. |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double | Percentage of the passer's dropbacks during which they faced pressure from the opposing defense. |
| `def_times_blitzed` | double | Number of times the defensive player sent additional rushers on a blitz during the game or season period. |
| `def_times_hurried` | double | Number of times the defensive player hurried or pressured the quarterback without recording a sack. |
| `def_times_hitqb` | double | Number of times the defensive player made contact with the quarterback (hit on the QB) during pass rushes. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

# Season-level rushing summaries (one row per player per season)

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

# Defensive stats with a follow-up filter

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

# Pandas round-trip

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### load_nfl_pfr_def {#load_nfl_pfr_def}

`load_nfl_pfr_def(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double |  |
| `gs` | double | Number of games the player started during the season or period covered by this row. |
| `int` | double |  |
| `tgt` | double | Total number of times the player was the nearest defender on a pass attempt (targets in coverage) per Pro Football Reference. |
| `cmp` | double | Number of passes completed by the opposing quarterback when targeting the player in coverage. |
| `cmp_percent` | double | Completion percentage allowed by the player in coverage (completions divided by targets). |
| `yds` | double | Total passing yards allowed by the player in coverage. |
| `yds_cmp` | double | Average yards allowed per completion when the player was in coverage. |
| `yds_tgt` | double | Average yards allowed per target thrown at the player in coverage. |
| `td` | double | Number of touchdowns allowed by the player in coverage. |
| `rat` | double | Passer rating allowed by the player in coverage — the NFL passer rating of quarterbacks when targeting this defender. |
| `dadot` | double | Depth of target air yards on defended passes — average distance downfield at the point of the throw when the player was in coverage. |
| `air` | double | Total air yards (depth of target) on passes thrown at the player in coverage, as tracked by Pro Football Reference. |
| `yac` | double | Yards after catch allowed by the player — yards gained by receivers after the catch when the player was the nearest defender. |
| `bltz` | double | Number of snaps on which the player blitzed the quarterback, as recorded by Pro Football Reference. |
| `hrry` | double | Number of times the player hurried the opposing quarterback without recording a full sack, per Pro Football Reference. |
| `qbkd` | double | Number of times the player knocked down the quarterback, making contact after or during a pass attempt. |
| `sk` | double | Number of sacks recorded by the player, bringing the quarterback down behind the line of scrimmage. |
| `prss` | double | Number of times the player pressured the quarterback (combining sacks, hits, and hurries) per Pro Football Reference. |
| `comb` | double | Total combined tackles (solo plus assisted) recorded by the player per Pro Football Reference. |
| `m_tkl` | double | Number of missed tackles attributed to the player by Pro Football Reference. |
| `m_tkl_percent` | double | Percentage of tackle attempts the player missed out of total tackle opportunities. |
| `loaded` | character | Indicator or metadata field from the Pro Football Reference data load, typically flagging the data source state or row completeness. |
| `bats` | double | Number of passes batted down at the line of scrimmage by the player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="season"
)
```

### load_nfl_pfr_pass {#load_nfl_pfr_pass}

`load_nfl_pfr_pass(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `pass_attempts` | double | Career pass attempts |
| `throwaways` | double | Throwaways |
| `spikes` | double | Spikes |
| `drops` | double | Throws dropped |
| `drop_pct` | double | Percent of throws dropped |
| `bad_throws` | double | Bad throws |
| `bad_throw_pct` | double | Percent of throws that were bad |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pocket_time` | double | Average time in pocket |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `pressure_pct` | double | Percent of the time pressured |
| `batted_balls` | double | Batted balls |
| `on_tgt_throws` | double | On target throws |
| `on_tgt_pct` | double | Percent of throws on target |
| `rpo_plays` | double | Number of RPO plays |
| `rpo_yards` | double | Yards on RPOs |
| `rpo_pass_att` | double | Number of pass attempts on RPOs |
| `rpo_pass_yards` | double | Passing yards on RPOs |
| `rpo_rush_att` | double | Rush attempts on RPOs |
| `rpo_rush_yards` | double | Rushing yards on RPOs |
| `pa_pass_att` | double | Play action pass attempts |
| `pa_pass_yards` | double | Play action passing yards |
| `intended_air_yards` | double | Total air yards on all pass attempts including incompletions, measuring aggregate downfield targeting intent from Pro Football Reference. |
| `intended_air_yards_per_pass_attempt` | double | Average intended air yards per pass attempt, capturing the passer's average depth of target regardless of completion outcome. |
| `completed_air_yards` | double | Total air yards on completed passes only, measuring how far the ball traveled downfield through the air to the point of completion. |
| `completed_air_yards_per_completion` | double | Average air yards per completed pass, representing the passer's typical depth of target on successful throws. |
| `completed_air_yards_per_pass_attempt` | double | Average completed air yards per pass attempt (including incompletions), a rate measure of downfield passing efficiency. |
| `pass_yards_after_catch` | double | Total yards gained by receivers after the catch, isolating the yards generated after initial ball reception from Pro Football Reference. |
| `pass_yards_after_catch_per_completion` | double | Average yards after catch per completion, measuring how much yardage receivers generate on the ground after catching the ball. |
| `scrambles` | double | Total number of quarterback scrambles (designed dropback converted to a run) recorded by Pro Football Reference. |
| `scramble_yards_per_attempt` | double | Average yards gained per scramble attempt by the quarterback, from Pro Football Reference advanced passing stats. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="season"
)
```

### load_nfl_pfr_rec {#load_nfl_pfr_rec}

`load_nfl_pfr_rec(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double |  |
| `gs` | double | Number of games the player started at a receiver position during the period covered. |
| `tgt` | double | Total number of times the player was the intended receiver on a pass attempt. |
| `rec` | double | Total receptions made by the player during the period covered. |
| `yds` | double | Total receiving yards gained by the player on all receptions. |
| `td` | double | Total receiving touchdowns scored by the player. |
| `x1d` | double | Number of receptions by the player that resulted in a first down. |
| `ybc` | double | Total yards the ball traveled in the air (before the catch) on receptions by the player. |
| `ybc_r` | double | Average air yards before the catch per reception. |
| `yac` | double | Total yards gained by the player after the catch. |
| `yac_r` | double | Average yards after the catch per reception. |
| `adot` | double | Average depth of target — mean air yards at point of throw on pass attempts directed at the receiver, per Pro Football Reference. |
| `brk_tkl` | double | Number of broken tackles credited to the player after a reception, per Pro Football Reference. |
| `rec_br` | double | Receptions per broken tackle — number of receptions for each broken tackle the player forced after the catch, per Pro Football Reference. |
| `drop` | double | Number of catchable passes the player dropped (failed to secure after the ball reached the receiver's hands). |
| `drop_percent` | double | Percentage of catchable targets that the player dropped. |
| `int` | double |  |
| `rat` | double | Passer rating generated on passes thrown to the player — the NFL passer rating when the receiver is targeted. |
| `loaded` | character | Indicator or metadata field from the Pro Football Reference data load, flagging the row's data source state or completeness. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="season"
)
```

### load_nfl_pfr_rush {#load_nfl_pfr_rush}

`load_nfl_pfr_rush(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double |  |
| `gs` | double | Number of games started by the player during the period covered. |
| `att` | double | Total rushing attempts by the player during the period covered. |
| `yds` | double | Total rushing yards gained during the period covered. |
| `td` | double | Total rushing touchdowns scored during the period covered. |
| `x1d` | double | Number of first downs gained via rushing during the period covered. |
| `ybc` | double | Yards before contact accumulated on rushing plays, measuring yards gained in open field before being touched. |
| `ybc_att` | double | Yards before contact per rushing attempt. |
| `yac` | double | Yards after contact accumulated on rushing plays. |
| `yac_att` | double | Yards after contact per rushing attempt. |
| `brk_tkl` | double | Number of broken tackles recorded on rushing plays. |
| `att_br` | double | Rushing attempts per broken tackle, measuring how often the player required contact to break free. |
| `loaded` | character | Source or load-batch identifier indicating which data file or release this row was pulled from. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)
```

### load_nfl_pfr_weekly_def {#load_nfl_pfr_weekly_def}

`load_nfl_pfr_weekly_def(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `def_ints` | double | Interceptions for the week (this loader is per-player-per-week, not career). |
| `def_targets` | double | Number of passing attempts thrown at or into the coverage area of this defender during the week. |
| `def_completions_allowed` | double | Number of completions allowed by the defender on passes thrown into their coverage during the week. |
| `def_completion_pct` | double | Completion percentage allowed by the defender on targets thrown in their coverage during the week. |
| `def_yards_allowed` | double | Total receiving yards allowed by this defender in coverage during the week per Pro Football Reference. |
| `def_yards_allowed_per_cmp` | double | Receiving yards allowed per completion by this defender in coverage during the week. |
| `def_yards_allowed_per_tgt` | double | Receiving yards allowed per target thrown at this defender in coverage during the week. |
| `def_receiving_td_allowed` | double | Number of receiving touchdowns allowed by the defender while in coverage during the week. |
| `def_passer_rating_allowed` | double | NFL passer rating of quarterbacks when targeting this defender in coverage during the week. |
| `def_adot` | double | Average depth of target (in yards) against this defender on passing plays during the week. |
| `def_air_yards_completed` | double | Total air yards on completed passes allowed by the defender during the week. |
| `def_yards_after_catch` | double | Total yards gained by receivers after the catch on completions allowed by this defender during the week. |
| `def_times_blitzed` | double | Number of times this defender was sent as a blitzer on a passing play during the week. |
| `def_times_hurried` | double | Number of times this defender hurried the quarterback on a pass rush without recording a sack during the week. |
| `def_times_hitqb` | double | Number of times this defender made contact with the quarterback as part of a pass rush during the week. |
| `def_sacks` | double | Number of sacks form this player |
| `def_pressures` | double | Total number of quarterback pressures (hurries + hits + sacks) generated by the defender during the week. |
| `def_tackles_combined` | double | Total combined tackles (solo + assisted) recorded by the defender during the week per Pro Football Reference. |
| `def_missed_tackles` | double | Number of missed tackles recorded against this defender during the week per Pro Football Reference. |
| `def_missed_tackle_pct` | double | Percentage of the defender's tackle opportunities that resulted in a missed tackle during the week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="week"
)
```
