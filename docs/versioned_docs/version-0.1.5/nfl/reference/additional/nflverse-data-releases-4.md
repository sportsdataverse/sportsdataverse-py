---
title: "NFL — additional Python functions — nflverse data releases: team_stats–trades"
sidebar_label: "nflverse data releases: team_stats–trades"
sidebar_position: 8
description: "NFL — additional Python functions — nflverse data releases: team_stats–trades — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: team_stats–trades

### load_team_stats {#load_team_stats}

`load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

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
| `opponent_team` | character | Abbreviation of the opposing team the team faced in the game or week represented by this row. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer | Total number of interceptions thrown by the team's quarterbacks during the period covered. |
| `sacks_suffered` | integer | Total number of times the team's quarterback was sacked during the period covered. |
| `sack_yards_lost` | integer | Total yards lost by the team's offense as a result of being sacked. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | integer | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | integer | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double | Completion percentage over expectation for the team's passing game — how much better or worse actual completion rate was versus the model-predicted rate. Percentage points (100 * the completion-rate gap), not a 0-1 rate. |
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
| `def_safeties` | integer | Number of safeties recorded by the team's defense (tackling an opponent in their own end zone). |
| `def_punt_blocks` | integer | Number of opponent punts blocked by the team's defense. |
| `def_pat_blocks` | integer | Number of opponent extra point attempts blocked by the team's defense. |
| `def_fg_blocks` | integer | Number of opponent field goal attempts blocked by the team's defense. |
| `def_2pt_atts` | integer | Number of defensive two-point conversion returns attempted by the team (nflfastR stat id 403). |
| `def_2pt_made` | integer | Number of successful defensive two-point conversion returns by the team (nflfastR stat id 404). |
| `misc_yards` | integer | Yards gained by the team through miscellaneous means not captured in standard rushing, passing, or return categories. |
| `fumble_recovery_own` | integer | Number of the team's own fumbles that were recovered by the team itself. |
| `fumble_recovery_yards_own` | integer | Total yards gained after recovering their own fumbles. |
| `fumble_recovery_opp` | integer | Number of fumbles recovered by the team from the opposing offense (defensive fumble recoveries). |
| `fumble_recovery_yards_opp` | integer | Total yards gained by the team on returns of opponent fumble recoveries. |
| `fumble_recovery_tds` | integer | Number of touchdowns scored by the team on fumble recoveries (own or opponent). |
| `penalties` | integer |  |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer | Number of timeouts remaining or used by the team during the game or period covered. |
| `fumbles_forced_by_opp` | integer | Fumbles by the team's players that were forced by the opponent, counted across all units (offense, defense and special teams). |
| `fumbles_not_forced` | integer | Fumbles by the team's players that were not forced by the opponent, counted across all units. |
| `fumbles_out_of_bounds` | integer | Fumbles by the team's players where the ball went out of bounds, forced or not; each is also counted in fumbles_forced_by_opp or fumbles_not_forced. |
| `fumbles_total` | integer | Total fumbles by the team's players across all units; equals fumbles_forced_by_opp + fumbles_not_forced. |
| `fumbles_lost_total` | integer | Total fumbles lost by the team's players, counted across all units. |
| `punt_returns` | integer |  |
| `punt_return_yards` | integer |  |
| `kickoff_returns` | integer | Total number of kickoff return attempts by the team. |
| `kickoff_return_yards` | integer | Total yards gained by the team on kickoff returns during the period covered. |
| `fg_made` | integer |  |
| `fg_att` | integer | Total field goal attempts by the team's kicker during the period covered. |
| `fg_missed` | integer | Total number of field goal attempts that were missed (not blocked, not made) by the team's kicker. |
| `fg_blocked` | integer | Total number of field goal attempts that were blocked by the opposing defense. |
| `fg_long` | integer | Distance in yards of the team's longest successful field goal during the period covered. |
| `fg_pct` | double |  |
| `fg_made_0_19` | integer | Number of field goals made by the team from 0–19 yards. |
| `fg_made_20_29` | integer | Number of field goals made by the team from 20–29 yards. |
| `fg_made_30_39` | integer | Number of field goals made by the team from 30–39 yards. |
| `fg_made_40_49` | integer | Number of field goals made by the team from 40–49 yards. |
| `fg_made_50_59` | integer | Number of field goals made by the team from 50–59 yards. |
| `fg_made_60_` | integer | Number of field goals made by the team from 60 yards or longer. |
| `fg_missed_0_19` | integer | Number of field goal attempts missed from 0–19 yards. |
| `fg_missed_20_29` | integer | Number of field goal attempts missed from 20–29 yards. |
| `fg_missed_30_39` | integer | Number of field goal attempts missed from 30–39 yards. |
| `fg_missed_40_49` | integer | Number of field goal attempts missed from 40–49 yards. |
| `fg_missed_50_59` | integer | Number of field goal attempts missed from 50–59 yards. |
| `fg_missed_60_` | integer | Number of field goal attempts missed from 60 yards or longer. |
| `fg_made_list` | character | Comma-separated list of distances (in yards) for each successful field goal made by the team. |
| `fg_missed_list` | character | Comma-separated list of distances (in yards) for each missed field goal attempt by the team. |
| `fg_blocked_list` | character | Comma-separated list of distances (in yards) for field goal attempts blocked by or against the team. |
| `fg_made_distance` | integer | Total cumulative distance in yards of all successful field goals made by the team. |
| `fg_missed_distance` | integer | Total cumulative distance in yards of all missed field goal attempts by the team. |
| `fg_blocked_distance` | integer | Distance in yards of the most recent or representative blocked field goal attempt. |
| `pat_made` | integer | Total number of extra points successfully kicked by the team. |
| `pat_att` | integer | Total number of extra point (PAT) kick attempts by the team. |
| `pat_missed` | integer | Number of extra point kick attempts that were missed (neither made nor blocked). |
| `pat_blocked` | integer | Number of extra point attempts that were blocked by the opposing defense. |
| `pat_pct` | double | Extra point conversion percentage (pat_made divided by pat_att) for the team's kicker. |
| `gwfg_made` | integer | Number of game-winning field goals successfully converted by the team's kicker. |
| `gwfg_att` | integer | Number of game-winning field goal attempts (potential go-ahead kicks in the final moments). |
| `gwfg_missed` | integer | Number of game-winning field goal attempts that were missed by the team's kicker. |
| `gwfg_blocked` | integer | Number of game-winning field goal attempts that were blocked by the opposing defense. |
| `gwfg_distance` | integer | Distance in yards of the game-winning field goal attempt(s) during the period covered. |
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

### load_teams {#load_teams}

`load_teams(return_as_pandas=False) -> 'pl.DataFrame'`

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
| `team_nick` | character | Team nickname or mascot name (e.g., 'Chiefs', 'Patriots'). |
| `team_conf` | character | Conference the team belongs to (e.g., 'AFC', 'NFC'). |
| `team_division` | character | Division within the conference the team belongs to (e.g., 'AFC East'). |
| `team_color` | character |  |
| `team_color2` | character | Secondary brand color for the team, expressed as a hex color code. |
| `team_color3` | character | Tertiary brand color for the team, expressed as a hex color code. |
| `team_color4` | character | Quaternary brand color for the team, expressed as a hex color code. |
| `team_logo_wikipedia` | character | URL of the team's logo image as hosted on Wikipedia. |
| `team_logo_espn` | character | URL of the team's primary logo as hosted on ESPN. |
| `team_wordmark` | character | URL of the team's wordmark (text-based logo) image. |
| `team_conference_logo` | character | URL of the conference logo image associated with the team. |
| `team_league_logo` | character | URL of the NFL league logo image. |
| `team_logo_squared` | character | URL of a square-format version of the team's logo. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### load_trades {#load_trades}

`load_trades(return_as_pandas=False) -> 'pl.DataFrame'`

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
