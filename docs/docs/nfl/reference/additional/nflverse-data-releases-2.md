---
title: "NFL — additional Python functions — nflverse data releases: nfl_espn–nfl_players"
sidebar_label: "nflverse data releases: nfl_espn–nfl_players"
sidebar_position: 6
description: "NFL — additional Python functions — nflverse data releases: nfl_espn–nfl_players — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: nfl_espn–nfl_players

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
| `seasons` | `list` |  | Seasons to return. 2006 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `summary_type` | `str` | `'season'` | Aggregation level. `"season"` (default) returns one row per quarterback-season; `"week"` returns one row per quarterback-game. Any other value raises `ValueError`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which QBR release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse `espn_data` release. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_espn_qbr` release (built by `nfl-data` from ESPN's QBR web endpoint -- the same source nflverse's espnscrapeR uses). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing ESPN Total QBR for the requested seasons, summarized per `summary_type`. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | NFL season (year) the Total QBR record covers. |
| `season_type` | String | Season segment for the record -- regular season or postseason. |
| `game_week` | String | Week scope of the QBR aggregation; for season-level rows this is the season-summary tag. |
| `team_abb` | String | Team abbreviation for the quarterback's team during the period. |
| `player_id` | String | ESPN athlete identifier for the quarterback. |
| `name_short` | String | Abbreviated display name of the quarterback (e.g. 'P. Mahomes'). |
| `rank` | Float64 | Quarterback's rank by Total QBR among qualified passers for the period. |
| `qbr_total` | Float64 | ESPN Total QBR on a 0-100 scale -- the headline opponent-adjusted quarterback rating. |
| `pts_added` | Float64 | Points the quarterback added versus a league-average passer (ESPN QBR points-added component). |
| `qb_plays` | Float64 | Count of qualifying quarterback action plays used to compute QBR. |
| `epa_total` | Float64 | Total expected points added across the quarterback's plays (ESPN QBR EPA component). |
| `pass` | Float64 | QBR points contribution from pass plays. |
| `run` | Float64 | QBR points contribution from designed runs and scrambles. |
| `exp_sack` | Float64 | QBR points contribution adjustment from expected sacks. |
| `penalty` | Float64 | QBR points contribution from penalties attributed to the quarterback. |
| `qbr_raw` | Float64 | Raw (non-opponent-adjusted) QBR for the period. |
| `sack` | Float64 | QBR points contribution from sacks taken. |
| `name_first` | String | Quarterback's first name. |
| `name_last` | String | Quarterback's last name. |
| `name_display` | String | Quarterback's full display name. |
| `headshot_href` | String | URL of the quarterback's ESPN headshot image. |
| `team` | String | Full team name for the quarterback's team during the period. |
| `qualified` | Boolean | Whether the quarterback met ESPN's minimum action-play threshold to qualify for ranking. |

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

### load_nfl_ff_opportunity {#load_nfl_ff_opportunity}

`load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_nfl_ff_rankings {#load_nfl_ff_rankings}

`load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str | None' = None, return_as_pandas=False) -> 'pl.DataFrame'`

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
| `game_id` | String | Ten digit identifier for NFL game. |
| `game_key` | String | Unique nflverse game identifier linking the officiating record to a specific NFL game. |
| `official_name` | String |  |
| `position` | String | Primary position as reported by NFL.com |
| `jersey_number` | Int64 | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | String |  |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | Int64 | Season week. |

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
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Ten digit identifier for NFL game. |
| `pfr_game_id` | String | PFR game ID |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `game_type` | String | The most recent game type of that season that a player appeared on the roster. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | String | Opposing team of player |
| `pfr_player_name` | String | Player's name as recorded by PFR |
| `pfr_player_id` | String | ID from Pro Football Reference |
| `passing_drops` | Float64 | Total number of catchable passes thrown by the passer that were dropped by receivers per Pro Football Reference. |
| `passing_drop_pct` | Float64 | Percentage of a passer's catchable targets that were dropped by the intended receiver. |
| `receiving_drop` | Float64 | Number of catchable targets the receiver dropped during the game or season period per Pro Football Reference. |
| `receiving_drop_pct` | Float64 | Percentage of the receiver's catchable targets that were dropped during the game or season period. |
| `passing_bad_throws` | Float64 | Total number of passes thrown by the passer that were classified as inaccurate or poor-quality throws per Pro Football Reference. |
| `passing_bad_throw_pct` | Float64 | Percentage of a passer's attempts classified as bad throws (inaccurate, off-target, or uncatchable). |
| `times_sacked` | Float64 | Total number of times the passer was sacked during the game or season period per Pro Football Reference. |
| `times_blitzed` | Float64 | Number of times blitzed |
| `times_hurried` | Float64 | Number of times hurried |
| `times_hit` | Float64 | Number of times hit |
| `times_pressured` | Float64 | Number of times pressured |
| `times_pressured_pct` | Float64 | Percentage of the passer's dropbacks during which they faced pressure from the opposing defense. |
| `def_times_blitzed` | Float64 | Number of times the defensive player sent additional rushers on a blitz during the game or season period. |
| `def_times_hurried` | Float64 | Number of times the defensive player hurried or pressured the quarterback without recording a sack. |
| `def_times_hitqb` | Float64 | Number of times the defensive player made contact with the quarterback (hit on the QB) during pass rushes. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `stat_type` or `summary_level` are not allowed values, or if any season is less than 2018. |

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
| `seasons` | `list` | `None` | Seasons to load. 1999 is the earliest available season. `None` (the default) loads the current season, `True` every published season from 1999, and `[]` an empty frame (no request); nflreadpy's contract (`None` used to load every season from 1999). |
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which player-stats release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse published `stats_player` weekly release, reconciled to the legacy schema described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_player_stats` release built by `sportsdataverse.nfl.build_nfl_player_stats` from SDV-native play-by-play (1999-present, week-level, REG+POST) with its own columns, season-filtered but otherwise untouched. Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing player stats. `seasons=[]` gives the contracted columns with no rows (nflverse source) or an empty frame with no columns (SDV source).

| col_name | type | description |
|---|---|---|
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | String | Full name of player |
| `player_display_name` | String | Full name of the player |
| `position` | String | Primary position as reported by NFL.com |
| `position_group` | String | Postion group of player as listed by NFL |
| `headshot_url` | String | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | String | Most recent team player appears in `pbp` with. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | String | Abbreviation or name of the opposing team faced by the player in a given game or week. |
| `completions` | Int64 | The number of completed passes. |
| `attempts` | Int64 | The number of pass attempts as defined by the NFL. |
| `passing_yards` | Float64 | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | Int64 | The number of passing touchdowns. |
| `interceptions` | Float64 | The number of interceptions thrown. |
| `sacks` | Float64 | The Number of times sacked. |
| `sack_yards` | Float64 | Yards lost on sack plays. |
| `sack_fumbles` | Int64 | The number of sacks with a fumble. |
| `sack_fumbles_lost` | Int64 | The number of sacks with a lost fumble. |
| `passing_air_yards` | Float64 | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | Float64 | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | Float64 | First downs on pass attempts. |
| `passing_epa` | Float64 | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | Int64 | Two-point conversion passes. |
| `pacr` | Float64 | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | Float64 | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | Int64 | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | Float64 | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | Int64 | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | Float64 | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | Float64 | The number of rushes with a lost fumble. |
| `rushing_first_downs` | Float64 | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | Float64 | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | Int64 | Two-point conversion rushes |
| `receptions` | Int64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | Int64 | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | Float64 | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | Int64 | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | Float64 | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | Float64 | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | Float64 | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | Float64 | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | Float64 | Total number of first downs gained on receptions |
| `receiving_epa` | Float64 | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | Int64 | Two-point conversion receptions |
| `racr` | Float64 | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | Float64 | "Player's share of team receiving targets in this game" |
| `air_yards_share` | Float64 | Player's share of the team's air yards in this game |
| `wopr` | Float64 | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | Float64 | Total number of kick/punt return touchdowns |
| `fantasy_points` | Float64 | Standard fantasy points. |
| `fantasy_points_ppr` | Float64 | PPR fantasy points. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If any requested season is less than 1999. |
| `TypeError` | If `seasons` is `False` -- the first positional argument used to be `kicking`, so `load_nfl_player_stats(False)` is rejected loudly. `True` is nflreadpy's "every season". |
| `ValueError` | If `source` is not one of `"nflverse"`, `None`, `"sportsdataverse"`, or `"sdv"`; or if `kicking=True` is combined with the SDV source (the SDV play-by-play surface lacks a `kicker_player_id`, so kicking stats are nflverse-only). |

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
| `gsis_id` | String | NFL Game Statistics & Information System player identifier, the canonical nflverse player key. |
| `display_name` | String | Player's full display name as published by nflverse. |
| `common_first_name` | String | Player's commonly used first name (the name they go by, which may differ from their legal first name). |
| `first_name` | String | Player's legal first name. |
| `last_name` | String | Player's last name. |
| `short_name` | String | Abbreviated name (typically first initial plus last name). |
| `football_name` | String | Player's preferred on-field name as used in broadcast and box-score contexts. |
| `suffix` | String | Generational or honorific name suffix (e.g., Jr., Sr., III), when present. |
| `esb_id` | String | Elias Sports Bureau player identifier. |
| `nfl_id` | String | NFL.com / Shield player identifier. |
| `pfr_id` | String | Pro-Football-Reference player identifier. |
| `pff_id` | String | Pro Football Focus player identifier. |
| `otc_id` | String | OverTheCap player identifier (salary-cap data source). |
| `espn_id` | String | ESPN athlete identifier. |
| `smart_id` | String | NFL SMART (Standard Media and Reference Table) globally unique player identifier. |
| `birth_date` | String | Player's date of birth (ISO YYYY-MM-DD). |
| `position_group` | String | Broad positional grouping the player belongs to (e.g., QB, RB, WR, DL). |
| `position` | String | Player's specific listed position abbreviation. |
| `ngs_position_group` | String | Positional grouping as classified by NFL Next Gen Stats. |
| `ngs_position` | String | Specific position as classified by NFL Next Gen Stats. |
| `height` | Int64 | Player's height in inches. |
| `weight` | Int64 | Player's listed weight in pounds. |
| `headshot` | String | URL to the player's official headshot image. |
| `college_name` | String | Name of the college the player attended. |
| `college_conference` | String | Athletic conference of the player's college. |
| `jersey_number` | String | Player's uniform / jersey number. |
| `rookie_season` | Int64 | Season (year) the player entered the league as a rookie. |
| `last_season` | Int64 | Most recent season (year) the player appeared on an NFL roster. |
| `latest_team` | String | Abbreviation of the most recent team the player was rostered on. |
| `status` | String | Player's current roster status (e.g., active, retired, free agent). |
| `ngs_status` | String | Player status as reported by NFL Next Gen Stats. |
| `ngs_status_short_description` | String | Short human-readable description of the NFL Next Gen Stats status. |
| `years_of_experience` | Int64 | Number of accrued NFL seasons of experience. |
| `pff_position` | String | Player's position as classified by Pro Football Focus. |
| `pff_status` | String | Player's status as classified by Pro Football Focus. |
| `draft_year` | Int64 | Year the player was selected in the NFL Draft (null if undrafted). |
| `draft_round` | Int64 | Round in which the player was drafted (null if undrafted). |
| `draft_pick` | Int64 | Overall pick number at which the player was drafted (null if undrafted). |
| `draft_team` | String | Abbreviation of the team that drafted the player (null if undrafted). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `source` is not one of `"nflverse"`, `None`, `"sportsdataverse"`, or `"sdv"`. |
| `Exception` | Propagates any network / parquet-read error from the underlying `pl.read_parquet` against the release URL. |

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
