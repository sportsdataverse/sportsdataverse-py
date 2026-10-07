---
title: "NFL — additional Python functions — nflverse data releases: nfl_ff–nfl_schedule"
sidebar_label: "nflverse data releases: nfl_ff–nfl_schedule"
sidebar_position: 6
description: "NFL — additional Python functions — nflverse data releases: nfl_ff–nfl_schedule — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: nfl_ff–nfl_schedule

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
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
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

### load_nfl_player_stats {#load_nfl_player_stats}

`load_nfl_player_stats(seasons: 'List[int] | None' = None, kicking=False, return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL player stats data

Week-level player stats. For the default `source="nflverse"` this reads the
live `stats_player` release (`stats_player_week_{season}.parquet`, one
asset per season, 1999-2026) -- **not** the combined `player_stats.parquet`,
which nflverse froze in 2025-05 and which therefore ends at season 2024.

The weekly release is a 150-column superset of the old combined file. To keep
every downstream consumer working, the output is reconciled to ONE stable
schema -- the legacy column set, in the legacy order, at the legacy dtypes:

* **Renamed back:** `team` -> `recent_team`, `passing_interceptions` ->
  `interceptions`, `sacks_suffered` -> `sacks`.
* **Sign-flipped:** `sack_yards_lost` (negative upstream) is negated into
  `sack_yards` (positive yards lost), matching the legacy frame.
* **Kept null:** `dakota` is no longer published upstream; the column
  remains, all-null, so the column set does not move.
* **Dropped:** the ~100 added columns (`def_*`, `pt_*`, punt/kickoff
  returns, yardage buckets, `game_id`, `passing_cpoe`, ...) are not
  emitted. `kicking=True` returns the legacy kicking contract, which the
  weekly release still carries in full (44/44 columns).
* **Rows:** a row is kept when at least one contracted stat is non-zero, so
  the weekly release's defensive / offensive-line rows -- which have no
  column to land in under this contract -- do not arrive as all-null noise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` | `None` | Seasons to load. 1999 is the earliest available season. `None` (the default) loads every season from 1999 through the current one, matching the old whole-file behavior. |
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which player-stats release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse published `stats_player` weekly release, reconciled to the legacy schema described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_player_stats` release built by `sportsdataverse.nfl.build_nfl_player_stats` from SDV-native play-by-play (1999-present, week-level, REG+POST) with its own columns, season-filtered but otherwise untouched. Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing player stats.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character | Abbreviation or name of the opposing team faced by the player in a given game or week. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | double | The number of interceptions thrown. |
| `sacks` | double | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | double | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | double | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | double | The number of rushes with a lost fumble. |
| `rushing_first_downs` | double | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | double | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | double | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | double | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | double | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

# SDV-native player stats (week-level, built from SDV play-by-play)

stats_sdv = load_nfl_player_stats(source="sdv")
stats_sdv.select(["season", "week", "player_id", "attempts"]).head()

# Kicking-only stats (nflverse source only)

kicking = load_nfl_player_stats(seasons=[2025], kicking=True)

# A single season (2025 and 2026 live only in the weekly release)

stats_2025 = load_nfl_player_stats(seasons=[2025])
```

### load_nfl_players {#load_nfl_players}

`load_nfl_players(return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load the nflverse NFL player-identity master.

Reads nflverse's published `players.parquet` — a one-row-per-player
identity master that is the union of **seven** upstream systems (GSIS, ESPN,
NGS roster, Pro-Football-Reference, OverTheCap, PFF, and the Sleeper / Yahoo
cross-walk). It is the canonical source for cross-system identifier
columns (`gsis_id`, `espn_id`, `pfr_id`, `pff_id`, `otc_id`,
`smart_id`, `esb_id`, `nfl_id`) plus name, position, physical, draft,
and status fields.

This is the **full identity master**. For an SDV-native, public-source-only
alternative that does not depend on the nflverse release, see
`sportsdataverse.nfl.build_nfl_players` (ESPN-athletes tier only) and
`sportsdataverse.nfl.nfl_players_crosswalk` (a thin ID-only slice of
this same parquet).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |
| `source` | `str` | `'nflverse'` | Which player-master release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse seven-system `players.parquet` identity master described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_players` release built by `sportsdataverse.nfl.build_nfl_players` from the **public NFL Shield / ESPN-athletes** surface, with `gsis_id` and the other cross-system IDs enriched by a best-effort join against the nflverse player master. The SDV tier is a partial build: its columns are a subset of nflverse's and cross-system IDs are sparser (notably pre-2016), though `espn_id` is populated. The default stays `"nflverse"`. Any other value raises `ValueError`. |

**Returns**

One-row-per-player identity master. `return_as_pandas` narrows the return to a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | NFL Game Statistics & Information System player identifier, the canonical nflverse player key. |
| `display_name` | character | Player's full display name as published by nflverse. |
| `common_first_name` | character | Player's commonly used first name (the name they go by, which may differ from their legal first name). |
| `first_name` | character | Player's legal first name. |
| `last_name` | character | Player's last name. |
| `short_name` | character | Abbreviated name (typically first initial plus last name). |
| `football_name` | character | Player's preferred on-field name as used in broadcast and box-score contexts. |
| `suffix` | character | Generational or honorific name suffix (e.g., Jr., Sr., III), when present. |
| `esb_id` | character | Elias Sports Bureau player identifier. |
| `nfl_id` | character | NFL.com / Shield player identifier. |
| `pfr_id` | character | Pro-Football-Reference player identifier. |
| `pff_id` | character | Pro Football Focus player identifier. |
| `otc_id` | character | OverTheCap player identifier (salary-cap data source). |
| `espn_id` | character | ESPN athlete identifier. |
| `smart_id` | character | NFL SMART (Standard Media and Reference Table) globally unique player identifier. |
| `birth_date` | character | Player's date of birth (ISO YYYY-MM-DD). |
| `position_group` | character | Broad positional grouping the player belongs to (e.g., QB, RB, WR, DL). |
| `position` | character | Player's specific listed position abbreviation. |
| `ngs_position_group` | character | Positional grouping as classified by NFL Next Gen Stats. |
| `ngs_position` | character | Specific position as classified by NFL Next Gen Stats. |
| `height` | integer | Player's height in inches. |
| `weight` | integer | Player's listed weight in pounds. |
| `headshot` | character | URL to the player's official headshot image. |
| `college_name` | character | Name of the college the player attended. |
| `college_conference` | character | Athletic conference of the player's college. |
| `jersey_number` | character | Player's uniform / jersey number. |
| `rookie_season` | integer | Season (year) the player entered the league as a rookie. |
| `last_season` | integer | Most recent season (year) the player appeared on an NFL roster. |
| `latest_team` | character | Abbreviation of the most recent team the player was rostered on. |
| `status` | character | Player's current roster status (e.g., active, retired, free agent). |
| `ngs_status` | character | Player status as reported by NFL Next Gen Stats. |
| `ngs_status_short_description` | character | Short human-readable description of the NFL Next Gen Stats status. |
| `years_of_experience` | integer | Number of accrued NFL seasons of experience. |
| `pff_position` | character | Player's position as classified by Pro Football Focus. |
| `pff_status` | character | Player's status as classified by Pro Football Focus. |
| `draft_year` | integer | Year the player was selected in the NFL Draft (null if undrafted). |
| `draft_round` | integer | Round in which the player was drafted (null if undrafted). |
| `draft_pick` | integer | Overall pick number at which the player was drafted (null if undrafted). |
| `draft_team` | character | Abbreviation of the team that drafted the player (null if undrafted). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
print(players.shape)

# Pandas round-trip

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()

# SDV-native player master (public Shield/ESPN-athletes build; subset of nflverse columns, sparser cross-IDs)

players_sdv = load_nfl_players(source="sdv")
players_sdv.select(["display_name", "position", "espn_id"]).head()

# Pipeline next step (one line)

import polars as pl
load_nfl_players().select(["gsis_id", "display_name", "position"]).head()
```

### load_nfl_schedule {#load_nfl_schedule}

`load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `gameday` | character | The date on which the game occurred. |
| `weekday` | character | The day of the week on which the game occcured. |
| `gametime` | character | The kickoff time of the game. This is represented in 24-hour time and the Eastern time zone, regardless of what time zone the game was being played in. |
| `away_team` | character | String abbreviation for the away team. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `overtime` | integer | Binary indicator of whether or not game went to overtime. |
| `old_game_id` | character | Legacy NFL game ID. |
| `gsis` | integer | The id of the game issued by the NFL Game Statistics & Information System. |
| `nfl_detail_id` | character | The id of the game issued by NFL Detail. |
| `pfr` | character | The id of the game issued by [Pro-Football-Reference](https://www.pro-football-reference.com/) |
| `pff` | integer | The id of the game issued by [Pro Football Focus](https://www.pff.com/) |
| `espn` | character | The id of the game issued by [ESPN](https://www.espn.com/) |
| `ftn` | integer | FTN Data game identifier corresponding to this scheduled game. |
| `away_rest` | integer | Days of rest that the away team is coming off of. |
| `home_rest` | integer | Days of rest that the home team is coming off of. |
| `away_moneyline` | integer | Odds for away team to win the game. |
| `home_moneyline` | integer | Odds for home team to win the game. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `away_spread_odds` | integer | Odds for away team to cover the spread. |
| `home_spread_odds` | integer | Odds for home team to cover the spread. |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `under_odds` | integer | Odds that total score of game would be under the total_line. |
| `over_odds` | integer | Odds that total score of game would be over the total_ine. |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `away_qb_id` | character | GSIS Player ID for away team starting quarterback. |
| `home_qb_id` | character | GSIS Player ID for home team starting quarterback. |
| `away_qb_name` | character | Name of away team starting QB. |
| `home_qb_name` | character | Name of home team starting QB. |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `referee` | character | Name of the game's referee (head official) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium` | character | Name of the stadium |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

# Multi-season range

schedule = load_nfl_schedule(seasons=range(2020, 2025))

# Filter to a single week

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

# Pandas round-trip

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```
