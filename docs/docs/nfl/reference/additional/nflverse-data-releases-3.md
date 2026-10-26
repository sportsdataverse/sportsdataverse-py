---
title: "NFL — additional Python functions — nflverse data releases: nfl_schedule–participation"
sidebar_label: "nflverse data releases: nfl_schedule–participation"
sidebar_position: 7
description: "NFL — additional Python functions — nflverse data releases: nfl_schedule–participation — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: nfl_schedule–participation

### load_nfl_schedule {#load_nfl_schedule}

`load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Ten digit identifier for NFL game. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | String | The most recent game type of that season that a player appeared on the roster. |
| `week` | Int64 | Season week. |
| `gameday` | String | The date on which the game occurred. |
| `weekday` | String | The day of the week on which the game occcured. |
| `gametime` | String | The kickoff time of the game. This is represented in 24-hour time and the Eastern time zone, regardless of what time zone the game was being played in. |
| `away_team` | String | String abbreviation for the away team. |
| `away_score` | Int64 | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_team` | String | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `home_score` | Int64 | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | String | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | Int64 | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | Int64 | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `overtime` | Int64 | Binary indicator of whether or not game went to overtime. |
| `old_game_id` | String | Legacy NFL game ID. |
| `gsis` | Int64 | The id of the game issued by the NFL Game Statistics & Information System. |
| `nfl_detail_id` | String | The id of the game issued by NFL Detail. |
| `pfr` | String | The id of the game issued by [Pro-Football-Reference](https://www.pro-football-reference.com/) |
| `pff` | Int64 | The id of the game issued by [Pro Football Focus](https://www.pff.com/) |
| `espn` | String | The id of the game issued by [ESPN](https://www.espn.com/) |
| `ftn` | Int64 | FTN Data game identifier corresponding to this scheduled game. |
| `away_rest` | Int64 | Days of rest that the away team is coming off of. |
| `home_rest` | Int64 | Days of rest that the home team is coming off of. |
| `away_moneyline` | Int64 | Odds for away team to win the game. |
| `home_moneyline` | Int64 | Odds for home team to win the game. |
| `spread_line` | Float64 | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `away_spread_odds` | Int64 | Odds for away team to cover the spread. |
| `home_spread_odds` | Int64 | Odds for home team to cover the spread. |
| `total_line` | Float64 | The closing total line for the game. (Source: Pro-Football-Reference) |
| `under_odds` | Int64 | Odds that total score of game would be under the total_line. |
| `over_odds` | Int64 | Odds that total score of game would be over the total_ine. |
| `div_game` | Int64 | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | String | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | String | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | Int64 | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | Int64 | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `away_qb_id` | String | GSIS Player ID for away team starting quarterback. |
| `home_qb_id` | String | GSIS Player ID for home team starting quarterback. |
| `away_qb_name` | String | Name of away team starting QB. |
| `home_qb_name` | String | Name of home team starting QB. |
| `away_coach` | String | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `home_coach` | String | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `referee` | String | Name of the game's referee (head official) |
| `stadium_id` | String | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium` | String | Name of the stadium |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is less than 1999. |

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

### load_nfl_team_stats {#load_nfl_team_stats}

`load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". Ignored when `source` is the SDV-native release (a single week-level parquet covering all seasons; filter post-load). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which team-stats release to read. `"nflverse"` (the default) reads the per-season nflverse `stats_team` releases. `"sportsdataverse"` / `"sdv"` reads the SDV-native `nfl_team_stats` release (a single combined week-level parquet, built by `sportsdataverse.nfl.build_nfl_team_stats` from the SDV play-by-play and filtered to the requested seasons post-load). |

**Returns**

Polars dataframe containing team stats available for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `game_id` | String | Ten digit identifier for NFL game. |
| `opponent_team` | String | Team abbreviation or identifier of the opposing team faced during the game or period. |
| `completions` | Int64 | The number of completed passes. |
| `attempts` | Int64 | The number of pass attempts as defined by the NFL. |
| `passing_yards` | Int64 | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | Int64 | The number of passing touchdowns. |
| `passing_interceptions` | Int64 | Total number of interceptions thrown by the team's passers during the game or season period. |
| `sacks_suffered` | Int64 | Total number of times the team's quarterback was sacked by the opposing defense during the period. |
| `sack_yards_lost` | Int64 | Total offensive yards lost by the team on plays where the quarterback was sacked during the period. |
| `sack_fumbles` | Int64 | The number of sacks with a fumble. |
| `sack_fumbles_lost` | Int64 | The number of sacks with a lost fumble. |
| `passing_air_yards` | Int64 | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | Int64 | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | Int64 | First downs on pass attempts. |
| `passing_epa` | Float64 | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | Float64 | Completion percentage over expectation (CPOE) for the team's passing attack during the period, relative to a model-based baseline. Percentage points (100 * the completion-rate gap), not a 0-1 rate. |
| `passing_2pt_conversions` | Int64 | Two-point conversion passes. |
| `passing_10` | Int64 | Number of the team's completed passes that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_16` | Int64 | Number of the team's completed passes that gained 16 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_20` | Int64 | Number of the team's completed passes that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_40` | Int64 | Number of the team's completed passes that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `carries` | Int64 | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | Int64 | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | Int64 | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | Int64 | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | Int64 | The number of rushes with a lost fumble. |
| `rushing_first_downs` | Int64 | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | Float64 | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | Int64 | Two-point conversion rushes |
| `rushing_10` | Int64 | Number of the team's runs that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_12` | Int64 | Number of the team's runs that gained 12 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_20` | Int64 | Number of the team's runs that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_40` | Int64 | Number of the team's runs that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receptions` | Int64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | Int64 | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | Int64 | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | Int64 | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | Int64 | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | Int64 | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | Int64 | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | Int64 | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | Int64 | Total number of first downs gained on receptions |
| `receiving_epa` | Float64 | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | Int64 | Two-point conversion receptions |
| `receiving_10` | Int64 | Number of the team's receptions that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_16` | Int64 | Number of the team's receptions that gained 16 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_20` | Int64 | Number of the team's receptions that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_40` | Int64 | Number of the team's receptions that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `special_teams_tds` | Int64 | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | Int64 | Total number of solo tackles for this player |
| `def_tackles_with_assist` | Int64 | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | Int64 | Number of assisted tackles for this player |
| `def_tackles_for_loss` | Int64 | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | Int64 | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | Int64 | Number of times a fumble was forced from this player |
| `def_sacks` | Float64 | Number of sacks form this player |
| `def_sack_yards` | Float64 | Yards lost from sacks forced by this player |
| `def_qb_hits` | Int64 | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | Int64 | Number of interceptions forced by this player |
| `def_interception_yards` | Int64 | yards gained/lost by interception returns from this player |
| `def_pass_defended` | Int64 | Number of passes defended/broken up by this player |
| `def_tds` | Int64 | Number of defensive touchdowns scored by this player |
| `def_fumbles` | Int64 | Number of fumbles by this player |
| `def_safeties` | Int64 | Number of safeties scored by the defense (opponent tackled in their own end zone) during the period. |
| `def_punt_blocks` | Int64 | Number of opponent punts blocked by the team's defense. |
| `def_pat_blocks` | Int64 | Number of opponent extra point attempts blocked by the team's defense. |
| `def_fg_blocks` | Int64 | Number of opponent field goal attempts blocked by the team's defense. |
| `def_2pt_atts` | Int64 | Number of defensive two-point conversion returns attempted by the team (nflfastR stat id 403). |
| `def_2pt_made` | Int64 | Number of successful defensive two-point conversion returns by the team (nflfastR stat id 404). |
| `misc_yards` | Int64 | Miscellaneous yards not attributed to passing, rushing, or standard return categories during the period. |
| `fumble_recovery_own` | Int64 | Number of fumbles recovered by the team that were originally fumbled by their own players. |
| `fumble_recovery_yards_own` | Int64 | Total yards gained (or lost) on recoveries of the team's own fumbles during the period. |
| `fumble_recovery_opp` | Int64 | Number of fumbles recovered by the team that were originally lost by the opposing team. |
| `fumble_recovery_yards_opp` | Int64 | Total yards gained on returns of fumbles recovered from the opposing team during the period. |
| `fumble_recovery_tds` | Int64 | Number of touchdowns scored by the team on fumble recoveries during the game or season period. |
| `penalties` | Int64 |  |
| `penalty_yards` | Int64 | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | Int64 | Number of timeouts remaining or used by the team during the game or period. |
| `fumbles_forced_by_opp` | Int64 | Fumbles by the team's players that were forced by the opponent, counted across all units (offense, defense and special teams). |
| `fumbles_not_forced` | Int64 | Fumbles by the team's players that were not forced by the opponent, counted across all units. |
| `fumbles_out_of_bounds` | Int64 | Fumbles by the team's players where the ball went out of bounds, forced or not; each is also counted in fumbles_forced_by_opp or fumbles_not_forced. |
| `fumbles_total` | Int64 | Total fumbles by the team's players across all units; equals fumbles_forced_by_opp + fumbles_not_forced. |
| `fumbles_lost_total` | Int64 | Total fumbles lost by the team's players, counted across all units. |
| `punt_returns` | Int64 |  |
| `punt_return_yards` | Int64 |  |
| `kickoff_returns` | Int64 | Total number of kickoff returns recorded by the team during the game or season period. |
| `kickoff_return_yards` | Int64 | Total yards gained by the team on kickoff returns during the game or season period. |
| `fg_made` | Int64 |  |
| `fg_att` | Int64 | Total number of field goal attempts by the team's kicker during the game or season period. |
| `fg_missed` | Int64 | Total number of field goal attempts missed (not blocked) by the team's kicker during the period. |
| `fg_blocked` | Int64 | Total number of field goal attempts that were blocked by the opposing defense during the period. |
| `fg_long` | Int64 | Distance in yards of the longest successful field goal made by the team's kicker during the period. |
| `fg_pct` | Float64 |  |
| `fg_made_0_19` | Int64 | Number of successful field goals made from 0–19 yards during the game or season period. |
| `fg_made_20_29` | Int64 | Number of successful field goals made from 20–29 yards during the game or season period. |
| `fg_made_30_39` | Int64 | Number of successful field goals made from 30–39 yards during the game or season period. |
| `fg_made_40_49` | Int64 | Number of successful field goals made from 40–49 yards during the game or season period. |
| `fg_made_50_59` | Int64 | Number of successful field goals made from 50–59 yards during the game or season period. |
| `fg_made_60_` | Int64 | Number of successful field goals made from 60 yards or longer during the game or season period. |
| `fg_missed_0_19` | Int64 | Number of field goal attempts from 0–19 yards that were missed (not blocked) during the period. |
| `fg_missed_20_29` | Int64 | Number of field goal attempts from 20–29 yards that were missed (not blocked) during the period. |
| `fg_missed_30_39` | Int64 | Number of field goal attempts from 30–39 yards that were missed (not blocked) during the period. |
| `fg_missed_40_49` | Int64 | Number of field goal attempts from 40–49 yards that were missed (not blocked) during the period. |
| `fg_missed_50_59` | Int64 | Number of field goal attempts from 50–59 yards that were missed (not blocked) during the period. |
| `fg_missed_60_` | Int64 | Number of field goal attempts from 60 yards or longer that were missed (not blocked) during the period. |
| `fg_made_list` | String | List of distances (in yards) of each successful field goal made during the game or season period. |
| `fg_missed_list` | String | List of distances (in yards) of each missed field goal attempt during the game or season period. |
| `fg_blocked_list` | String | List of distances (in yards) of each blocked field goal attempt during the game or season period. |
| `fg_made_distance` | Int64 | Cumulative distance in yards of all successful field goals made during the game or season period. |
| `fg_missed_distance` | Int64 | Cumulative distance in yards of all missed field goal attempts during the game or season period. |
| `fg_blocked_distance` | Int64 | Distance (in yards) of field goal attempts that were blocked by the opposing defense during the period. |
| `pat_made` | Int64 | Total number of successful point-after-touchdown kicks made by the team's kicker during the period. |
| `pat_att` | Int64 | Total number of point-after-touchdown (PAT / extra point) attempts by the team's kicker during the period. |
| `pat_missed` | Int64 | Number of point-after-touchdown attempts that were missed (not blocked) during the period. |
| `pat_blocked` | Int64 | Number of point-after-touchdown attempts that were blocked by the opposing defense during the period. |
| `pat_pct` | Float64 | Percentage of point-after-touchdown attempts that were successfully converted during the period. |
| `gwfg_made` | Int64 | Number of successful game-winning field goals made to secure a victory in the final moments. |
| `gwfg_att` | Int64 | Number of game-winning field goal attempts made in the final moments to win the game. |
| `gwfg_missed` | Int64 | Number of game-winning field goal attempts that were missed (no good) in the final moments. |
| `gwfg_blocked` | Int64 | Number of game-winning field goal attempts that were blocked by the opposing defense. |
| `gwfg_distance` | Int64 | Distance in yards of the game-winning field goal attempt (or attempts) during the period. |
| `pt_att` | Int64 | Number of punts kicked by the team; blocked punts are counted separately in pt_blocked. |
| `pt_blocked` | Int64 | Number of the team's punts that were blocked. |
| `pt_long` | Int64 | Length in yards of the team's longest punt; null when the team had no kicked punt (never 0 in the 2024 sample). |
| `pt_yards` | Int64 | Total gross yards of the team's punts. |
| `pt_inside_20` | Int64 | Number of the team's punts credited as ending inside the opponent's 20-yard line (nflfastR defines the spot as where the return ended). |
| `pt_out_of_bounds` | Int64 | Number of the team's punts that went out of bounds without a return. |
| `pt_downed` | Int64 | Number of the team's punts that were downed without a return. |
| `pt_touchback` | Int64 | Number of the team's punts that resulted in a touchback. |
| `pt_fair_caught` | Int64 | Number of the team's punts that were fair caught by the opponent. |
| `pt_returned` | Int64 | Number of the team's punts that were returned by the opponent. |
| `pt_return_yards` | Int64 | Punt return yards gained by the opponent on the team's punts; can be negative (minimum -4 in the 2024 sample). |
| `pt_return_tds` | Int64 | Number of the team's punts that the opponent returned for a touchdown. |
| `pt_net_yards` | Int64 | Net punting yards: pt_yards minus pt_return_yards minus 20 yards per touchback. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is less than 1999, if `summary_level` is not one of the allowed values, or if `source` is not one of `"nflverse"`, `None`, `"sportsdataverse"`, or `"sdv"`. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

# Regular-season-only team stats

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

# SDV-native team stats (built from SDV play-by-play)

sdv = load_nfl_team_stats(seasons=[2024], source="sdv")
```

### load_nfl_teams {#load_nfl_teams}

`load_nfl_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | String | Official team abbreveation |
| `team_name` | String |  |
| `team_id` | Int64 |  |
| `team_nick` | String | Team nickname (e.g., 'Chiefs', 'Eagles', 'Patriots') without the city or state prefix. |
| `team_conf` | String | Conference affiliation of the team (e.g., 'AFC' or 'NFC'). |
| `team_division` | String | Division affiliation of the team (e.g., 'AFC North', 'NFC West'). |
| `team_color` | String |  |
| `team_color2` | String | Secondary brand color for the team in hexadecimal format (e.g., '#FFB612'). |
| `team_color3` | String | Tertiary brand color for the team in hexadecimal format, used in alternate uniforms or accents. |
| `team_color4` | String | Quaternary brand color for the team in hexadecimal format, part of the team's full brand palette. |
| `team_logo_wikipedia` | String | URL to the team's primary logo image as hosted on Wikimedia Commons / Wikipedia. |
| `team_logo_espn` | String | URL to the team's primary logo image as hosted by ESPN. |
| `team_wordmark` | String | URL to the team's wordmark image (team name rendered in official typography without the primary logo mark). |
| `team_conference_logo` | String | URL to the logo image for the team's conference (AFC or NFC). |
| `team_league_logo` | String | URL to the NFL league logo image. |
| `team_logo_squared` | String | URL to a square-cropped version of the team's logo suitable for thumbnails and grid layouts. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### load_nfl_trades {#load_nfl_trades}

`load_nfl_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | Int64 | ID of Trade |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `trade_date` | String | Exact date that trade occurred |
| `gave` | String | Team that gave pick/player in row |
| `received` | String | Team that received pick/player in row |
| `pick_season` | Int64 | Draft in which traded pick was in |
| `pick_round` | Int64 | Round in which traded pick was in |
| `pick_number` | Int64 | Pick number of traded pick |
| `conditional` | Int64 | Binary indicator of whether or not traded pick was conditional |
| `pfr_id` | String | Pro-Football-Reference ID for player |
| `pfr_name` | String | Full name of traded player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

# Filter to a single season

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

### load_officials {#load_officials}

`load_officials(return_as_pandas=False) -> 'pl.DataFrame'`

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
| `game_key` | String | Unique numeric key assigned by the NFL to identify the specific game in official records. |
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

### load_participation {#load_participation}

`load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `nflverse_game_id` | String | nflverse identifier for games. Format is season, week, away_team, home_team |
| `old_game_id` | String | Legacy NFL game ID. |
| `play_id` | Float64 | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `possession_team` | String | String abbreviation for the team with possession. |
| `offense_formation` | String | Formation the offense lines up in to snap the ball. |
| `offense_personnel` | String | The positions of the offensive personnel lined up on the field for a play. |
| `defenders_in_box` | Int64 | Number of defensive players lined up in the box at the snap. |
| `defense_personnel` | String | The positions of the defensive personnel lined up on the field for a play. |
| `number_of_pass_rushers` | Int64 | Number of defensive player who rushed the passer. |
| `players_on_play` | String | A list of every player on the field for the play, by gsis_id |
| `offense_players` | String | A list of every offensive player on the field for the play, by gsis_id |
| `defense_players` | String | A list of every defensive player on the field for the play, by gsis_id |
| `n_offense` | Int64 | Number of offensive players on the field for the play |
| `n_defense` | Int64 | Number of defensive players on the field for the play |
| `ngs_air_yards` | Float64 | Legacy column. For 2023 and prior years, reflects the distance (in yards) that the ball traveled in the air on a given passing play as tracked by NGS. Is NA for 2024 on--we advise instead using the air_yards column from nflreadr::load_pbp() moving forward. |
| `time_to_throw` | Float64 | Duration (in seconds) between the time of the ball being snapped and the time of release of a pass attempt |
| `was_pressure` | Boolean | A boolean indicating whether or not the QB was pressured on a play |
| `route` | String | A string indicating the route the primary receiver on a play took. Has the following possible values: "CORNER", "DEEP OUT", "GO", "HITCH/CURL", "IN/DIG", "POST", "QUICK OUT", "SCREEN", "SHALLOW CROSS/DRAG", "SLANT", "SWING", "TEXAS/ANGLE", "WHEEL". |
| `defense_man_zone_type` | String | A string indicating whether the defense was in man or zone coverage on a play |
| `defense_coverage_type` | String | A string indicating what type of cover the defense was in on a play. Has one of the following values: "COVER_0", "COVER_1", "COVER_2", "2_MAN", "COVER_3", "COVER_4", "COVER_6", "COVER_9", "COMBO", "BLOWN". |
| `offense_names` | String | A string listing all of the names of offensive players in the order of their gsis_ids in offense_players. |
| `defense_names` | String | A string listing all of the names of defensive players in the order of their gsis_ids in defense_players. |
| `offense_positions` | String | A string listing all of the positions of offensive players in the order of their gsis_ids in offense_players. |
| `defense_positions` | String | A string listing all of the positions of defensive players in the order of their gsis_ids in defense_players. |
| `offense_numbers` | String | A string listing all of the numbers of offensive players in the order of their gsis_ids in offense_players. |
| `defense_numbers` | String | A string listing all of the numbers of defensive players in the order of their gsis_ids in defense_players. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

# Multi-season range

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```
