---
title: "NFL — additional Python functions — nflverse data releases: nfl_team–pbp"
sidebar_label: "nflverse data releases: nfl_team–pbp"
sidebar_position: 7
description: "NFL — additional Python functions — nflverse data releases: nfl_team–pbp — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: nfl_team–pbp

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
| `penalties` | integer |  |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer | Number of timeouts remaining or used by the team during the game or period. |
| `fumbles_forced_by_opp` | integer | Fumbles by the team's players that were forced by the opponent, counted across all units (offense, defense and special teams). |
| `fumbles_not_forced` | integer | Fumbles by the team's players that were not forced by the opponent, counted across all units. |
| `fumbles_out_of_bounds` | integer | Fumbles by the team's players where the ball went out of bounds, forced or not; each is also counted in fumbles_forced_by_opp or fumbles_not_forced. |
| `fumbles_total` | integer | Total fumbles by the team's players across all units; equals fumbles_forced_by_opp + fumbles_not_forced. |
| `fumbles_lost_total` | integer | Total fumbles lost by the team's players, counted across all units. |
| `punt_returns` | integer |  |
| `punt_return_yards` | integer |  |
| `kickoff_returns` | integer | Total number of kickoff returns recorded by the team during the game or season period. |
| `kickoff_return_yards` | integer | Total yards gained by the team on kickoff returns during the game or season period. |
| `fg_made` | integer |  |
| `fg_att` | integer | Total number of field goal attempts by the team's kicker during the game or season period. |
| `fg_missed` | integer | Total number of field goal attempts missed (not blocked) by the team's kicker during the period. |
| `fg_blocked` | integer | Total number of field goal attempts that were blocked by the opposing defense during the period. |
| `fg_long` | integer | Distance in yards of the longest successful field goal made by the team's kicker during the period. |
| `fg_pct` | double |  |
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
| `team_name` | character |  |
| `team_id` | integer |  |
| `team_nick` | character | Team nickname (e.g., 'Chiefs', 'Eagles', 'Patriots') without the city or state prefix. |
| `team_conf` | character | Conference affiliation of the team (e.g., 'AFC' or 'NFC'). |
| `team_division` | character | Division affiliation of the team (e.g., 'AFC North', 'NFC West'). |
| `team_color` | character |  |
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

### load_pbp {#load_pbp}

`load_pbp(seasons: 'List[int]', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL play by play data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which enriched play-by-play release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse/nflfastR `play_by_play_{season}.parquet` releases — full history from 1999, unchanged behavior. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_model_pbp` release: a Python-built, nflfastR-faithful enriched frame (ep/epa, wp/wpa/vegas_wp, cp/cpoe, xyac_*/air_epa) that covers 1999+ (28 assets, verified 2026-09-17) and drops administrative / timeout rows for a clean modeling subset. Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing the play-by-plays available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `old_game_id` | character | Legacy NFL game ID. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | character | String abbreviation for the away team. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `posteam` | character | String abbreviation for the team with possession. |
| `posteam_type` | character | String indicating whether the posteam team is home or away. |
| `defteam` | character | String abbreviation for the team on defense. |
| `side_of_field` | character | String abbreviation for which team's side of the field the team with possession is currently on. |
| `yardline_100` | double | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `game_date` | character | Date of the game. |
| `quarter_seconds_remaining` | double | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | double | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | double | Numeric seconds remaining in the game. |
| `game_half` | character | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `quarter_end` | double | Binary indicator for whether or not the row of the data is marking the end of a quarter. |
| `drive` | double | Numeric drive number in the game. |
| `sp` | double | Binary indicator for whether or not a score occurred on the play. |
| `qtr` | double | Quarter of the game (5 is overtime). |
| `down` | double | The down for the given play. |
| `goal_to_go` | double | Binary indicator for whether or not the posteam is in a goal down situation. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `yrdln` | character | String indicating the current field position for a given play. |
| `ydstogo` | double | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `ydsnet` | double | Numeric value for total yards gained on the given drive. |
| `desc` | character | Detailed string description for the given play. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | double | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `shotgun` | double | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | double | Binary indicator for whether or not the play was in no_huddle formation. |
| `qb_dropback` | double | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `qb_kneel` | double | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | double | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | double | Binary indicator for whether or not the QB scrambled. |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `air_yards` | double | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | double | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `run_location` | character | String indicator for location of run: left, middle, or right. |
| `run_gap` | character | String indicator for line gap of run: end, guard, or tackle |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `kick_distance` | double | Numeric distance in yards for kickoffs, field goals, and punts. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `home_timeouts_remaining` | double | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | double | Numeric timeouts remaining in the half for the away team. |
| `timeout` | double | Binary indicator for whether or not a timeout was called by either team. |
| `timeout_team` | character | String abbreviation for which team called the timeout. |
| `td_team` | character | String abbreviation for which team scored the touchdown. |
| `td_player_name` | character | String name of the player who scored a touchdown. |
| `td_player_id` | character | Unique identifier of the player who scored a touchdown. |
| `posteam_timeouts_remaining` | double | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | double | Number of timeouts remaining for the team on defense. |
| `total_home_score` | double | Score for the home team at the start of the play. |
| `total_away_score` | double | Score for the away team at the start of the play. |
| `posteam_score` | double | Score the posteam at the start of the play. |
| `defteam_score` | double | Score the defteam at the start of the play. |
| `score_differential` | double | Score differential between the posteam and defteam at the start of the play. |
| `posteam_score_post` | double | Score for the posteam at the end of the play. |
| `defteam_score_post` | double | Score for the defteam at the end of the play. |
| `score_differential_post` | double | Score differential between the posteam and defteam at the end of the play. |
| `no_score_prob` | double | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `opp_fg_prob` | double | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | double | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | double | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | double | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | double | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `td_prob` | double | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `extra_point_prob` | double | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | double | Predicted probability of the posteam scoring the two point conversion. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | double | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | double | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | double | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | double | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | double | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | double | Cumulative total passing EPA for the away team in the game so far. |
| `air_epa` | double | EPA from the air yards alone. For completions this represents the actual value provided through the air. For incompletions this represents the hypothetical value that could've been added through the air if the pass was completed. |
| `yac_epa` | double | EPA from the yards after catch alone. For completions this represents the actual value provided after the catch. For incompletions this represents the difference between the hypothetical air_epa and the play's raw observed EPA (how much the incomplete pass cost the posteam). |
| `comp_air_epa` | double | EPA from the air yards alone only for completions. |
| `comp_yac_epa` | double | EPA from the yards after catch alone only for completions. |
| `total_home_comp_air_epa` | double | Cumulative total completions air EPA for the home team in the game so far. |
| `total_away_comp_air_epa` | double | Cumulative total completions air EPA for the away team in the game so far. |
| `total_home_comp_yac_epa` | double | Cumulative total completions yac EPA for the home team in the game so far. |
| `total_away_comp_yac_epa` | double | Cumulative total completions yac EPA for the away team in the game so far. |
| `total_home_raw_air_epa` | double | Cumulative total raw air EPA for the home team in the game so far. |
| `total_away_raw_air_epa` | double | Cumulative total raw air EPA for the away team in the game so far. |
| `total_home_raw_yac_epa` | double | Cumulative total raw yac EPA for the home team in the game so far. |
| `total_away_raw_yac_epa` | double | Cumulative total raw yac EPA for the away team in the game so far. |
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `vegas_wpa` | double | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `vegas_home_wpa` | double | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | double | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | double | Estimated win probability for the away team at the end of the play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `vegas_home_wp` | double | Estimated win probability for the home team incorporating pre-game Vegas line. |
| `total_home_rush_wpa` | double | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | double | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | double | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | double | Cumulative total passing WPA for the away team in the game so far. |
| `air_wpa` | double | WPA through the air (same logic as air_epa). |
| `yac_wpa` | double | WPA from yards after the catch (same logic as yac_epa). |
| `comp_air_wpa` | double | The air_wpa for completions only. |
| `comp_yac_wpa` | double | The yac_wpa for completions only. |
| `total_home_comp_air_wpa` | double | Cumulative total completions air WPA for the home team in the game so far. |
| `total_away_comp_air_wpa` | double | Cumulative total completions air WPA for the away team in the game so far. |
| `total_home_comp_yac_wpa` | double | Cumulative total completions yac WPA for the home team in the game so far. |
| `total_away_comp_yac_wpa` | double | Cumulative total completions yac WPA for the away team in the game so far. |
| `total_home_raw_air_wpa` | double | Cumulative total raw air WPA for the home team in the game so far. |
| `total_away_raw_air_wpa` | double | Cumulative total raw air WPA for the away team in the game so far. |
| `total_home_raw_yac_wpa` | double | Cumulative total raw yac WPA for the home team in the game so far. |
| `total_away_raw_yac_wpa` | double | Cumulative total raw yac WPA for the away team in the game so far. |
| `punt_blocked` | double | Binary indicator for if the punt was blocked. |
| `first_down_rush` | double | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | double | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | double | Binary indicator for if a penalty converted the first down. |
| `third_down_converted` | double | Binary indicator for if the first down was converted on third down. |
| `third_down_failed` | double | Binary indicator for if the posteam failed to convert first down on third down. |
| `fourth_down_converted` | double | Binary indicator for if the first down was converted on fourth down. |
| `fourth_down_failed` | double | Binary indicator for if the posteam failed to convert first down on fourth down. |
| `incomplete_pass` | double | Binary indicator for if the pass was incomplete. |
| `touchback` | double | Binary indicator for if a touchback occurred on the play. |
| `interception` | double | Binary indicator for if the pass was intercepted. |
| `punt_inside_twenty` | double | Binary indicator for if the punt ended inside the twenty yard line. |
| `punt_in_endzone` | double | Binary indicator for if the punt was in the endzone. |
| `punt_out_of_bounds` | double | Binary indicator for if the punt went out of bounds. |
| `punt_downed` | double | Binary indicator for if the punt was downed. |
| `punt_fair_catch` | double | Binary indicator for if the punt was caught with a fair catch. |
| `kickoff_inside_twenty` | double | Binary indicator for if the kickoff ended inside the twenty yard line. |
| `kickoff_in_endzone` | double | Binary indicator for if the kickoff was in the endzone. |
| `kickoff_out_of_bounds` | double | Binary indicator for if the kickoff went out of bounds. |
| `kickoff_downed` | double | Binary indicator for if the kickoff was downed. |
| `kickoff_fair_catch` | double | Binary indicator for if the kickoff was caught with a fair catch. |
| `fumble_forced` | double | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | double | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | double | Binary indicator for if the fumble went out of bounds. |
| `solo_tackle` | double | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `safety` | double | Binary indicator for whether or not a safety occurred. |
| `penalty` | double | Binary indicator for whether or not a penalty occurred. |
| `tackled_for_loss` | double | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_lost` | double | Binary indicator for if the fumble was lost. |
| `own_kickoff_recovery` | double | Binary indicator for if the kicking team recovered the kickoff. |
| `own_kickoff_recovery_td` | double | Binary indicator for if the kicking team recovered the kickoff and scored a TD. |
| `qb_hit` | double | Binary indicator if the QB was hit on the play. |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `sack` | double | Binary indicator for if the play ended in a sack. |
| `touchdown` | double | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | double | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `extra_point_attempt` | double | Binary indicator for extra point attempt. |
| `two_point_attempt` | double | Binary indicator for two point conversion attempt. |
| `field_goal_attempt` | double | Binary indicator for field goal attempt. |
| `kickoff_attempt` | double | Binary indicator for kickoff. |
| `punt_attempt` | double | Binary indicator for punts. |
| `fumble` | double | Binary indicator for if a fumble occurred. |
| `complete_pass` | double | Binary indicator for if the pass was completed. |
| `assist_tackle` | double | Binary indicator for if an assist tackle occurred. |
| `lateral_reception` | double | Binary indicator for if a lateral occurred on the reception. |
| `lateral_rush` | double | Binary indicator for if a lateral occurred on a run. |
| `lateral_return` | double | Binary indicator for if a lateral occurred on a return. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_recovery` | double | Binary indicator for if a lateral occurred on a fumble recovery. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiver_player_id` | character | Unique identifier for the player that received the last(!) lateral on a pass play. |
| `lateral_receiver_player_name` | character | String name for the player that received the last(!) lateral on a pass play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral receiving yards. |
| `lateral_receiving_yards` | double | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `lateral_rusher_player_id` | character | Unique identifier for the player that received the last(!) lateral on a run play. |
| `lateral_rusher_player_name` | character | String name for the player that received the last(!) lateral on a run play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral rushing yards. |
| `lateral_rushing_yards` | double | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_sack_player_id` | character | Unique identifier for the player that received the lateral on a sack. |
| `lateral_sack_player_name` | character | String name for the player that received the lateral on a sack. |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `lateral_interception_player_id` | character | Unique identifier for the player that received the lateral on an interception. |
| `lateral_interception_player_name` | character | String name for the player that received the lateral on an interception. |
| `punt_returner_player_id` | character | Unique identifier for the punt returner. |
| `punt_returner_player_name` | character | String name for the punt returner. |
| `lateral_punt_returner_player_id` | character | Unique identifier for the player that received the lateral on a punt return. |
| `lateral_punt_returner_player_name` | character | String name for the player that received the lateral on a punt return. |
| `kickoff_returner_player_name` | character | String name for the kickoff returner. |
| `kickoff_returner_player_id` | character | Unique identifier for the kickoff returner. |
| `lateral_kickoff_returner_player_id` | character | Unique identifier for the player that received the lateral on a kickoff return. |
| `lateral_kickoff_returner_player_name` | character | String name for the player that received the lateral on a kickoff return. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `punter_player_name` | character | String name for the punter. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `own_kickoff_recovery_player_id` | character | Unique identifier for the player that recovered their own kickoff. |
| `own_kickoff_recovery_player_name` | character | String name for the player that recovered their own kickoff. |
| `blocked_player_id` | character | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | character | String name for the player that blocked the punt or FG. |
| `tackle_for_loss_1_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `qb_hit_1_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `forced_fumble_player_1_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | character | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | character | String name of one of the players with a forced fumble. |
| `solo_tackle_1_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_2_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_1_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | character | String name of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | character | String name of one of the players with a solo tackle. |
| `assist_tackle_1_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_1_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_1_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_2_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_2_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_3_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_3_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_3_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_4_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_4_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_4_team` | character | Team of one of the players with a tackle assist. |
| `tackle_with_assist` | double | Binary indicator for if there has been a tackle with assist. |
| `tackle_with_assist_1_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | character | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | character | Team of one of the players with a tackle with assist. |
| `pass_defense_1_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | character | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | character | String name of one of the players with a pass defense. |
| `fumbled_1_team` | character | Team of one of the first player with a fumble. |
| `fumbled_1_player_id` | character | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | character | String name of one of the first player who fumbled on the play. |
| `fumbled_2_player_id` | character | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | character | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | character | Team of one of the second player with a fumble. |
| `fumble_recovery_1_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_1_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | character | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | character | String name of one of the players with a fumble recovery. |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `half_sack_1_player_id` | character | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | character | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | character | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | character | String name of the second player who recorded half a sack. |
| `return_team` | character | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `return_yards` | double | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `penalty_team` | character | String abbreviation of the team with the penalty. |
| `penalty_player_id` | character | Unique identifier for the player with the penalty. |
| `penalty_player_name` | character | String name for the player with the penalty. |
| `penalty_yards` | double | Yards gained (or lost) by the posteam from the penalty. |
| `replay_or_challenge` | double | Binary indicator for whether or not a replay or challenge. |
| `replay_or_challenge_result` | character | String indicating the result of the replay or challenge. |
| `penalty_type` | character | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `defensive_two_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | double | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `defensive_extra_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on an extra point attempt, this results following a blocked attempt that the defense recovers the ball. |
| `defensive_extra_point_conv` | double | Binary indicator whether or not the defense successfully scored on an extra point attempt. |
| `safety_player_name` | character | String name for the player who scored a safety. |
| `safety_player_id` | character | Unique identifier for the player who scored a safety. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `cp` | double | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `series` | double | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_success` | double | 1: scored touchdown, gained enough yards for first down. |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `order_sequence` | double | Column provided by NFL to fix out-of-order plays. Available 2011 and beyond with source "nfl". |
| `start_time` | character | Kickoff time in eastern time zone. |
| `time_of_day` | character | Time of day of play in UTC "HH:MM:SS" format. Available 2011 and beyond with source "nfl". |
| `stadium` | character | Name of the stadium |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `nfl_api_id` | character | UUID of the game in the new NFL API. |
| `play_clock` | character | Time on the playclock when the ball was snapped. |
| `play_deleted` | double | Binary indicator for deleted plays. |
| `play_type_nfl` | character | Play type as listed in the NFL source. Slightly different to the regular play_type variable. |
| `special_teams_play` | double | Binary indicator for whether play is special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `st_play_type` | character | Type of special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `end_clock_time` | character | Game time at the end of a given play. |
| `end_yard_line` | character | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `fixed_drive` | double | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `drive_real_start_time` | character | Local day time when the drive started (currently not used by the NFL and therefore mostly 'NA'). |
| `drive_play_count` | double | Numeric value of how many regular plays happened in a given drive. |
| `drive_time_of_possession` | character | Time of possession in a given drive. |
| `drive_first_downs` | double | Number of first downs in a given drive. |
| `drive_inside20` | double | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | double | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | double | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | double | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | double | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | character | String indicating how the offense got the ball. |
| `drive_end_transition` | character | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | character | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | character | Game time at the end of a given drive. |
| `drive_start_yard_line` | character | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | character | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | double | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | double | Play_id of the last play in the given drive. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `game_stadium` | character | Name of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `aborted_play` | double | Binary indicator if the play description indicates "Aborted". |
| `success` | double | Binary indicator whether epa > 0 in the given play. |
| `passer` | character | Name of the dropback player (scrambles included) including plays with penalties. |
| `passer_jersey_number` | integer | Jersey number of the passer. |
| `rusher` | character | Name of the rusher (no scrambles) including plays with penalties. |
| `rusher_jersey_number` | integer | Jersey number of the rusher. |
| `receiver` | character | Name of the receiver including plays with penalties. |
| `receiver_jersey_number` | integer | Jersey number of the receiver. |
| `pass` | double | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | double | Binary indicator if the play was a rushing play. |
| `first_down` | double | Binary indicator if the play ended in a first down. |
| `special` | double | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | double | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | character | ID of the player in the 'passer' column. |
| `rusher_id` | character | ID of the player in the 'rusher' column. |
| `receiver_id` | character | ID of the player in the 'receiver' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `id` | character | ID of the player in the 'name' column. |
| `fantasy_player_name` | character | Name of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy_player_id` | character | ID of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy` | character | Name of the rusher on rush plays or receiver on pass plays. |
| `fantasy_id` | character | ID of the rusher on rush plays or receiver on pass plays. |
| `out_of_bounds` | double | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `home_opening_kickoff` | double | 1 if the home team received the opening kickoff, 0 otherwise. |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `xyac_epa` | double | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | double | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | integer | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

# Multi-season range

pbp = load_nfl_pbp(seasons=range(2020, 2025))

# SDV-native enriched play-by-play (Python-built, nflfastR-faithful EP/WP/CP/xYAC; published seasons only; admin/timeout rows dropped)

pbp_sdv = load_nfl_pbp(seasons=[2024], source="sdv")
pbp_sdv.select(["ep", "epa", "wp", "wpa", "cp", "cpoe", "xyac_epa"]).head()

# With cache off (development workflow)

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

# Pandas round-trip

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```
