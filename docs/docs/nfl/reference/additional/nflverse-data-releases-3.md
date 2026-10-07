---
title: "NFL — additional Python functions — nflverse data releases: nfl_team–snap_counts"
sidebar_label: "nflverse data releases: nfl_team–snap_counts"
sidebar_position: 7
description: "NFL — additional Python functions — nflverse data releases: nfl_team–snap_counts — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: nfl_team–snap_counts

### load_nfl_team_stats {#load_nfl_team_stats}

`load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". Ignored when `source` is the SDV-native release (a single week-level parquet covering all seasons; filter post-load). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which team-stats release to read. `"nflverse"` (the default) reads the per-season nflverse `stats_team` releases. `"sportsdataverse"` / `"sdv"` reads the SDV-native `nfl_team_stats` release (a single combined week-level parquet, built by `sportsdataverse.nfl.build_nfl_team_stats` from the SDV play-by-play and filtered to the requested seasons post-load). |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `opponent_team` | character | Team abbreviation or identifier of the opposing team faced during the game or period. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer | Total number of interceptions thrown by the team's passers during the game or season period. |
| `sacks_suffered` | integer | Total number of times the team's quarterback was sacked by the opposing defense during the period. |
| `sack_yards_lost` | integer | Total offensive yards lost by the team on plays where the quarterback was sacked during the period. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | integer | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | integer | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double | Completion percentage over expectation (CPOE) for the team's passing attack during the period, relative to a model-based baseline. Percentage points (100 * the completion-rate gap), not a 0-1 rate. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `passing_10` | integer | Number of the team's completed passes that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_16` | integer | Number of the team's completed passes that gained 16 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_20` | integer | Number of the team's completed passes that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_40` | integer | Number of the team's completed passes that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `rushing_10` | integer | Number of the team's runs that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_12` | integer | Number of the team's runs that gained 12 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_20` | integer | Number of the team's runs that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_40` | integer | Number of the team's runs that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | integer | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | integer | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `receiving_10` | integer | Number of the team's receptions that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_16` | integer | Number of the team's receptions that gained 16 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_20` | integer | Number of the team's receptions that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_40` | integer | Number of the team's receptions that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | integer | Total number of solo tackles for this player |
| `def_tackles_with_assist` | integer | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | integer | Number of assisted tackles for this player |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | integer | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | integer | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | integer | Number of interceptions forced by this player |
| `def_interception_yards` | integer | yards gained/lost by interception returns from this player |
| `def_pass_defended` | integer | Number of passes defended/broken up by this player |
| `def_tds` | integer | Number of defensive touchdowns scored by this player |
| `def_fumbles` | integer | Number of fumbles by this player |
| `def_safeties` | integer | Number of safeties scored by the defense (opponent tackled in their own end zone) during the period. |
| `def_punt_blocks` | integer | Number of opponent punts blocked by the team's defense. |
| `def_pat_blocks` | integer | Number of opponent extra point attempts blocked by the team's defense. |
| `def_fg_blocks` | integer | Number of opponent field goal attempts blocked by the team's defense. |
| `def_2pt_atts` | integer | Number of defensive two-point conversion returns attempted by the team (nflfastR stat id 403). |
| `def_2pt_made` | integer | Number of successful defensive two-point conversion returns by the team (nflfastR stat id 404). |
| `misc_yards` | integer | Miscellaneous yards not attributed to passing, rushing, or standard return categories during the period. |
| `fumble_recovery_own` | integer | Number of fumbles recovered by the team that were originally fumbled by their own players. |
| `fumble_recovery_yards_own` | integer | Total yards gained (or lost) on recoveries of the team's own fumbles during the period. |
| `fumble_recovery_opp` | integer | Number of fumbles recovered by the team that were originally lost by the opposing team. |
| `fumble_recovery_yards_opp` | integer | Total yards gained on returns of fumbles recovered from the opposing team during the period. |
| `fumble_recovery_tds` | integer | Number of touchdowns scored by the team on fumble recoveries during the game or season period. |
| `penalties` | integer | Total number of penalties. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer | Number of timeouts remaining or used by the team during the game or period. |
| `fumbles_forced_by_opp` | integer | Fumbles by the team's players that were forced by the opponent, counted across all units (offense, defense and special teams). |
| `fumbles_not_forced` | integer | Fumbles by the team's players that were not forced by the opponent, counted across all units. |
| `fumbles_out_of_bounds` | integer | Fumbles by the team's players where the ball went out of bounds, forced or not; each is also counted in fumbles_forced_by_opp or fumbles_not_forced. |
| `fumbles_total` | integer | Total fumbles by the team's players across all units; equals fumbles_forced_by_opp + fumbles_not_forced. |
| `fumbles_lost_total` | integer | Total fumbles lost by the team's players, counted across all units. |
| `punt_returns` | integer | Number of punt returns. |
| `punt_return_yards` | integer | Team punt return yards. |
| `kickoff_returns` | integer | Total number of kickoff returns recorded by the team during the game or season period. |
| `kickoff_return_yards` | integer | Total yards gained by the team on kickoff returns during the game or season period. |
| `fg_made` | integer | TRUE when the field goal attempt was successful. |
| `fg_att` | integer | Total number of field goal attempts by the team's kicker during the game or season period. |
| `fg_missed` | integer | Total number of field goal attempts missed (not blocked) by the team's kicker during the period. |
| `fg_blocked` | integer | Total number of field goal attempts that were blocked by the opposing defense during the period. |
| `fg_long` | integer | Distance in yards of the longest successful field goal made by the team's kicker during the period. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg_made_0_19` | integer | Number of successful field goals made from 0–19 yards during the game or season period. |
| `fg_made_20_29` | integer | Number of successful field goals made from 20–29 yards during the game or season period. |
| `fg_made_30_39` | integer | Number of successful field goals made from 30–39 yards during the game or season period. |
| `fg_made_40_49` | integer | Number of successful field goals made from 40–49 yards during the game or season period. |
| `fg_made_50_59` | integer | Number of successful field goals made from 50–59 yards during the game or season period. |
| `fg_made_60_` | integer | Number of successful field goals made from 60 yards or longer during the game or season period. |
| `fg_missed_0_19` | integer | Number of field goal attempts from 0–19 yards that were missed (not blocked) during the period. |
| `fg_missed_20_29` | integer | Number of field goal attempts from 20–29 yards that were missed (not blocked) during the period. |
| `fg_missed_30_39` | integer | Number of field goal attempts from 30–39 yards that were missed (not blocked) during the period. |
| `fg_missed_40_49` | integer | Number of field goal attempts from 40–49 yards that were missed (not blocked) during the period. |
| `fg_missed_50_59` | integer | Number of field goal attempts from 50–59 yards that were missed (not blocked) during the period. |
| `fg_missed_60_` | integer | Number of field goal attempts from 60 yards or longer that were missed (not blocked) during the period. |
| `fg_made_list` | character | List of distances (in yards) of each successful field goal made during the game or season period. |
| `fg_missed_list` | character | List of distances (in yards) of each missed field goal attempt during the game or season period. |
| `fg_blocked_list` | character | List of distances (in yards) of each blocked field goal attempt during the game or season period. |
| `fg_made_distance` | integer | Cumulative distance in yards of all successful field goals made during the game or season period. |
| `fg_missed_distance` | integer | Cumulative distance in yards of all missed field goal attempts during the game or season period. |
| `fg_blocked_distance` | integer | Distance (in yards) of field goal attempts that were blocked by the opposing defense during the period. |
| `pat_made` | integer | Total number of successful point-after-touchdown kicks made by the team's kicker during the period. |
| `pat_att` | integer | Total number of point-after-touchdown (PAT / extra point) attempts by the team's kicker during the period. |
| `pat_missed` | integer | Number of point-after-touchdown attempts that were missed (not blocked) during the period. |
| `pat_blocked` | integer | Number of point-after-touchdown attempts that were blocked by the opposing defense during the period. |
| `pat_pct` | double | Percentage of point-after-touchdown attempts that were successfully converted during the period. |
| `gwfg_made` | integer | Number of successful game-winning field goals made to secure a victory in the final moments. |
| `gwfg_att` | integer | Number of game-winning field goal attempts made in the final moments to win the game. |
| `gwfg_missed` | integer | Number of game-winning field goal attempts that were missed (no good) in the final moments. |
| `gwfg_blocked` | integer | Number of game-winning field goal attempts that were blocked by the opposing defense. |
| `gwfg_distance` | integer | Distance in yards of the game-winning field goal attempt (or attempts) during the period. |
| `pt_att` | integer | Number of punts kicked by the team; blocked punts are counted separately in pt_blocked. |
| `pt_blocked` | integer | Number of the team's punts that were blocked. |
| `pt_long` | integer | Length in yards of the team's longest punt; null when the team had no kicked punt (never 0 in the 2024 sample). |
| `pt_yards` | integer | Total gross yards of the team's punts. |
| `pt_inside_20` | integer | Number of the team's punts credited as ending inside the opponent's 20-yard line (nflfastR defines the spot as where the return ended). |
| `pt_out_of_bounds` | integer | Number of the team's punts that went out of bounds without a return. |
| `pt_downed` | integer | Number of the team's punts that were downed without a return. |
| `pt_touchback` | integer | Number of the team's punts that resulted in a touchback. |
| `pt_fair_caught` | integer | Number of the team's punts that were fair caught by the opponent. |
| `pt_returned` | integer | Number of the team's punts that were returned by the opponent. |
| `pt_return_yards` | integer | Punt return yards gained by the opponent on the team's punts; can be negative (minimum -4 in the 2024 sample). |
| `pt_return_tds` | integer | Number of the team's punts that the opponent returned for a touchdown. |
| `pt_net_yards` | integer | Net punting yards: pt_yards minus pt_return_yards minus 20 yards per touchback. |

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
| `team_abbr` | character | Official team abbreveation |
| `team_name` | character | Team nickname. |
| `team_id` | integer | ESPN team id. |
| `team_nick` | character | Team nickname (e.g., 'Chiefs', 'Eagles', 'Patriots') without the city or state prefix. |
| `team_conf` | character | Conference affiliation of the team (e.g., 'AFC' or 'NFC'). |
| `team_division` | character | Division affiliation of the team (e.g., 'AFC North', 'NFC West'). |
| `team_color` | character | Primary team color. |
| `team_color2` | character | Secondary brand color for the team in hexadecimal format (e.g., '#FFB612'). |
| `team_color3` | character | Tertiary brand color for the team in hexadecimal format, used in alternate uniforms or accents. |
| `team_color4` | character | Quaternary brand color for the team in hexadecimal format, part of the team's full brand palette. |
| `team_logo_wikipedia` | character | URL to the team's primary logo image as hosted on Wikimedia Commons / Wikipedia. |
| `team_logo_espn` | character | URL to the team's primary logo image as hosted by ESPN. |
| `team_wordmark` | character | URL to the team's wordmark image (team name rendered in official typography without the primary logo mark). |
| `team_conference_logo` | character | URL to the logo image for the team's conference (AFC or NFC). |
| `team_league_logo` | character | URL to the NFL league logo image. |
| `team_logo_squared` | character | URL to a square-cropped version of the team's logo suitable for thumbnails and grid layouts. |

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
| `trade_id` | integer | ID of Trade |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `trade_date` | character | Exact date that trade occurred |
| `gave` | character | Team that gave pick/player in row |
| `received` | character | Team that received pick/player in row |
| `pick_season` | integer | Draft in which traded pick was in |
| `pick_round` | integer | Round in which traded pick was in |
| `pick_number` | integer | Pick number of traded pick |
| `conditional` | integer | Binary indicator of whether or not traded pick was conditional |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pfr_name` | character | Full name of traded player |

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
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character | Unique numeric key assigned by the NFL to identify the specific game in official records. |
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

### load_participation {#load_participation}

`load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `old_game_id` | character | Legacy NFL game ID. |
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `possession_team` | character | String abbreviation for the team with possession. |
| `offense_formation` | character | Formation the offense lines up in to snap the ball. |
| `offense_personnel` | character | The positions of the offensive personnel lined up on the field for a play. |
| `defenders_in_box` | integer | Number of defensive players lined up in the box at the snap. |
| `defense_personnel` | character | The positions of the defensive personnel lined up on the field for a play. |
| `number_of_pass_rushers` | integer | Number of defensive player who rushed the passer. |
| `players_on_play` | character | A list of every player on the field for the play, by gsis_id |
| `offense_players` | character | A list of every offensive player on the field for the play, by gsis_id |
| `defense_players` | character | A list of every defensive player on the field for the play, by gsis_id |
| `n_offense` | integer | Number of offensive players on the field for the play |
| `n_defense` | integer | Number of defensive players on the field for the play |
| `ngs_air_yards` | double | Legacy column. For 2023 and prior years, reflects the distance (in yards) that the ball traveled in the air on a given passing play as tracked by NGS. Is NA for 2024 on--we advise instead using the air_yards column from nflreadr::load_pbp() moving forward. |
| `time_to_throw` | double | Duration (in seconds) between the time of the ball being snapped and the time of release of a pass attempt |
| `was_pressure` | logical | A boolean indicating whether or not the QB was pressured on a play |
| `route` | character | A string indicating the route the primary receiver on a play took. Has the following possible values: "CORNER", "DEEP OUT", "GO", "HITCH/CURL", "IN/DIG", "POST", "QUICK OUT", "SCREEN", "SHALLOW CROSS/DRAG", "SLANT", "SWING", "TEXAS/ANGLE", "WHEEL". |
| `defense_man_zone_type` | character | A string indicating whether the defense was in man or zone coverage on a play |
| `defense_coverage_type` | character | A string indicating what type of cover the defense was in on a play. Has one of the following values: "COVER_0", "COVER_1", "COVER_2", "2_MAN", "COVER_3", "COVER_4", "COVER_6", "COVER_9", "COMBO", "BLOWN". |
| `offense_names` | character | A string listing all of the names of offensive players in the order of their gsis_ids in offense_players. |
| `defense_names` | character | A string listing all of the names of defensive players in the order of their gsis_ids in defense_players. |
| `offense_positions` | character | A string listing all of the positions of offensive players in the order of their gsis_ids in offense_players. |
| `defense_positions` | character | A string listing all of the positions of defensive players in the order of their gsis_ids in defense_players. |
| `offense_numbers` | character | A string listing all of the numbers of offensive players in the order of their gsis_ids in offense_players. |
| `defense_numbers` | character | A string listing all of the numbers of defensive players in the order of their gsis_ids in defense_players. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

# Multi-season range

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### load_pfr_advstats {#load_pfr_advstats}

`load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

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
| `passing_drops` | double | Raw count of catchable passes dropped by the intended receiver, from Pro Football Reference charting. |
| `passing_drop_pct` | double | Percentage of pass attempts dropped by the receiver, isolating receiver-side incompletions from passer error. |
| `receiving_drop` | double | Number of catchable targets dropped by the receiver in the given game or season, from Pro Football Reference advanced receiving data. |
| `receiving_drop_pct` | double | Percentage of targets that resulted in a drop by the receiver, from Pro Football Reference advanced receiving data. |
| `passing_bad_throws` | double | Raw count of bad throws by the passer, as charted and defined by Pro Football Reference advanced passing data. |
| `passing_bad_throw_pct` | double | Percentage of pass attempts classified as bad throws by Pro Football Reference (passes the passer should not have attempted or severely underthreww/overthrew). |
| `times_sacked` | double | Total number of times the defensive player recorded a sack of the quarterback, from Pro Football Reference. |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double | Percentage of pass-blocking snaps on which the lineman or back allowed the quarterback to be pressured, from Pro Football Reference. |
| `def_times_blitzed` | double | Number of plays on which the defensive player sent five or more pass rushers, from Pro Football Reference advanced defensive stats. |
| `def_times_hurried` | double | Number of times the defensive player hurried (pressured but did not sack or hit) the quarterback, from Pro Football Reference. |
| `def_times_hitqb` | double | Number of times the defensive player hit the quarterback on a pass play without recording a sack, from Pro Football Reference. |

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

### load_player_stats {#load_player_stats}

`load_player_stats(seasons: 'List[int] | None' = None, kicking=False, return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

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
| `opponent_team` | character | Abbreviation of the opposing team the player faced in the game or week represented by this row. |
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

### load_players {#load_players}

`load_players(return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

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

### load_rosters_weekly {#load_rosters_weekly}

`load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL weekly roster data for the requested seasons.

Reads nflverse's published weekly-roster parquet (one row per player per
team per week), so the roster snapshot reflects mid-season transactions
(signings, releases, IR moves) rather than a single season-end view. Like
`load_nfl_rosters` it is sourced from nflverse's full multi-tier
roster product and carries densely populated cross-system identifier columns
plus a `week` / `game_type` pair identifying each snapshot.

Unlike `load_nfl_rosters` and `load_nfl_players`, this loader has
**no SDV-native (`source="sdv"`) tier**: the SDV roster build
(`build_nfl_rosters`) is season-only, and weekly snapshots require the
credential-gated NFL Data Exchange that the public build cannot reach.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to load (e.g. `[2024]` or `range(2022, 2025)`). A single `int` is accepted and wrapped. 2002 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe (default). |

**Returns**

Polars dataframe of weekly rosters for the requested seasons (`pandas.DataFrame` when `return_as_pandas=True`).

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (year) the weekly roster snapshot applies to. |
| `team` | character | Team abbreviation in the nflverse standard (relocations folded, e.g. 'OAK' -> 'LV', 'SD' -> 'LAC', 'STL' -> 'LA'). |
| `position` | character | Position the player is listed at on the roster (e.g. 'QB', 'WR', 'CB'). |
| `depth_chart_position` | character | Fine-grained depth-chart position label, which may differ from the broader position group. |
| `jersey_number` | integer | Uniform (jersey) number the player wears. |
| `status` | character | Roster status code for the player (e.g. 'ACT' active, 'INA' inactive, 'RES' reserve/injured). |
| `full_name` | character | Player's full display name. |
| `first_name` | character | Player's first (given) name. |
| `last_name` | character | Player's last (family) name. |
| `birth_date` | character | Player's date of birth (YYYY-MM-DD). |
| `height` | double | Player's height in inches. |
| `weight` | integer | Player's listed weight in pounds. |
| `college` | character | College or university the player attended. |
| `gsis_id` | character | NFL GSIS player identifier — the canonical nflverse player key used to join across datasets. |
| `espn_id` | character | ESPN player identifier for cross-system joins. |
| `sportradar_id` | character | Sportradar player identifier for cross-system joins. |
| `yahoo_id` | character | Yahoo Sports player identifier for cross-system joins. |
| `rotowire_id` | character | RotoWire player identifier for cross-system joins. |
| `pff_id` | character | Pro Football Focus (PFF) player identifier for cross-system joins. |
| `pfr_id` | character | Pro Football Reference (PFR) player identifier for cross-system joins. |
| `fantasy_data_id` | character | FantasyData player identifier for cross-system joins. |
| `sleeper_id` | character | Sleeper player identifier for cross-system joins. |
| `years_exp` | integer | Number of accrued NFL seasons of experience for the player. |
| `headshot_url` | character | URL of the player's headshot image. |
| `ngs_position` | character | Player's position as classified by NFL Next Gen Stats. |
| `week` | integer | Week of the season the weekly roster snapshot applies to. |
| `game_type` | character | Type of game the weekly roster snapshot applies to (e.g. 'REG', 'POST'). |
| `status_description_abbr` | character | Abbreviated roster status description code from the source feed. |
| `football_name` | character | Player's preferred football (commonly used) first name. |
| `esb_id` | character | Elias Sports Bureau (ESB) player identifier used for official NFL record-keeping. |
| `gsis_it_id` | character | NFL GSIS internal tracking identifier for the player. |
| `smart_id` | character | NFL SMART player identifier (GUID) used across modern NFL data feeds. |
| `entry_year` | integer | Calendar year the player first entered the NFL. |
| `rookie_year` | integer | Calendar year of the player's rookie season. |
| `draft_club` | character | Team abbreviation of the club that drafted the player. |
| `draft_number` | integer | Overall pick number at which the player was selected in the NFL draft. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

# Multi-season range with a follow-up week filter

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### load_schedules {#load_schedules}

`load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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
| `ftn` | integer | FTN Data game identifier used to join schedule records with FTN charting and tracking data. |
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

### load_snap_counts {#load_snap_counts}

`load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL snap counts data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2012 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing snap counts available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `player` | character | Player name |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `offense_snaps` | double | Number of snaps on offense |
| `offense_pct` | double | Percent of offensive snaps taken |
| `defense_snaps` | double | Number of snaps on defense |
| `defense_pct` | double | Percent of defensive snaps taken |
| `st_snaps` | double | Number of snaps on special teams |
| `st_pct` | double | Percent of special teams snaps taken |

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

# Multi-season range with offense-only filter

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```
