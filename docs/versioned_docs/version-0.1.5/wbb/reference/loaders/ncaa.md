---
title: "WBB dataset loaders — NCAA (stats.ncaa.org)"
sidebar_label: "NCAA (stats.ncaa.org)"
sidebar_position: 1
description: "WBB dataset loaders — NCAA (stats.ncaa.org) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WBB dataset loaders — NCAA (stats.ncaa.org)

## load_ncaa_wbb_rapm

Release: [ncaa_wbb_rapm](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_rapm) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_rapm/ncaa_wbb_rapm_{season}.parquet`
### Returns {#load_ncaa_wbb_rapm-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `player_id` | String | Unique player identifier. |
| `person_id` | String | Unique player identifier (V3 endpoints). |
| `player` | String | Player name. |
| `team` | String | Team-side label or team identifier. |
| `orapm` | Float64 | Offensive regularized adjusted plus-minus: points contributed per 100 possessions on offense, adjusted for the other 9 players on the floor. |
| `drapm` | Float64 | Defensive regularized adjusted plus-minus: points prevented per 100 possessions on defense (higher is better defense), adjusted for the other 9 players on the floor. |
| `rapm_net` | Float64 | Net RAPM (orapm plus drapm): overall point contribution per 100 possessions. Verified against live data: rapm_net == orapm + drapm exactly. |
| `off_poss` | Int64 | Offensive possessions the player was on court for; the regression weight behind orapm. |
| `def_poss` | Int64 | Defensive possessions the player was on court for; the regression weight behind drapm. |
| `estimand` | String | Fit-scope tag for the RAPM model that produced this row (observed value: 'league', a Division I-wide fit) -- one row per player-season, not per estimand. |

```python
load_ncaa_wbb_rapm(seasons=2024)
```

## load_ncaa_wbb_pbp

Release: [ncaa_wbb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_pbp/ncaa_wbb_pbp_{season}.parquet`
### Returns {#load_ncaa_wbb_pbp-returns}

| col_name | type | description |
|---|---|---|
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
| `player_1` | String | Name of the primary player credited on the event (shooter, fouler, rebounder, etc.), as scraped from stats.ncaa.org. |
| `player_2` | String | Name of the secondary player on the event (e.g., the assister or the player subbed for), when present. |
| `event_type` | String | Event / play type code (V2 PBP). |
| `event_result` | String | Outcome of the event, e.g. made or missed for shot attempts. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `event_length` | Int64 | Seconds elapsed between this event and the previous event in the game. |
| `poss_num` | Int64 | Sequential possession number within the game that the event belongs to. |
| `poss_team` | String | Name of the team in possession when the event occurred. |
| `poss_length` | Int64 | Duration of the enclosing possession in seconds. |
| `is_transition` | Boolean | Flag marking events that occurred in transition, within the opening seconds of the possession. |
| `home_1` | String | Name of the home team's on-floor player in lineup slot 1 for the event, from the substitution walk-forward. |
| `home_2` | String | Name of the home team's on-floor player in lineup slot 2 for the event, from the substitution walk-forward. |
| `home_3` | String | Name of the home team's on-floor player in lineup slot 3 for the event, from the substitution walk-forward. |
| `home_4` | String | Name of the home team's on-floor player in lineup slot 4 for the event, from the substitution walk-forward. |
| `home_5` | String | Name of the home team's on-floor player in lineup slot 5 for the event, from the substitution walk-forward. |
| `away_1` | String | Name of the away team's on-floor player in lineup slot 1 for the event, from the substitution walk-forward. |
| `away_2` | String | Name of the away team's on-floor player in lineup slot 2 for the event, from the substitution walk-forward. |
| `away_3` | String | Name of the away team's on-floor player in lineup slot 3 for the event, from the substitution walk-forward. |
| `away_4` | String | Name of the away team's on-floor player in lineup slot 4 for the event, from the substitution walk-forward. |
| `away_5` | String | Name of the away team's on-floor player in lineup slot 5 for the event, from the substitution walk-forward. |
| `status` | String | Status label. |
| `is_garbage_time` | Boolean | Flag marking events in garbage time under the score-margin and clock rule of the pbp builder. |
| `sub_deviate` | Int64 | Per-game count of substitution-tracking deviations found while walking lineups forward; nonzero flags imperfect substitution data. |
| `contest_id` | String |  |
| `home_ncaa_team_id` | String | stats.ncaa.org team identifier for the home team. |
| `home_espn_team_id` | String | ESPN home team id (NA for bart-only rows). |
| `away_ncaa_team_id` | String | stats.ncaa.org team identifier for the away team. |
| `away_espn_team_id` | String | ESPN away team id (NA for bart-only rows). |
| `event_team_ncaa_team_id` | String | stats.ncaa.org team identifier of the team credited with the event. |
| `event_team_espn_team_id` | String | ESPN team identifier of the team credited with the event, via the NCAA-to-ESPN crosswalk. |
| `poss_team_ncaa_team_id` | String | stats.ncaa.org team identifier of the team in possession. |
| `poss_team_espn_team_id` | String | ESPN team identifier of the team in possession, via the NCAA-to-ESPN crosswalk. |
| `player_1_id` | String | stats.ncaa.org player identifier for player_1, resolved through the roster name matcher. |
| `player_1_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name for player_1. |
| `player_2_id` | String | stats.ncaa.org player identifier for player_2, resolved through the roster name matcher. |
| `player_2_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name for player_2. |
| `home_1_player_id` | String | stats.ncaa.org player identifier for the home slot-1 on-floor player. |
| `home_1_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-1 on-floor player. |
| `home_2_player_id` | String | stats.ncaa.org player identifier for the home slot-2 on-floor player. |
| `home_2_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-2 on-floor player. |
| `home_3_player_id` | String | stats.ncaa.org player identifier for the home slot-3 on-floor player. |
| `home_3_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-3 on-floor player. |
| `home_4_player_id` | String | stats.ncaa.org player identifier for the home slot-4 on-floor player. |
| `home_4_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-4 on-floor player. |
| `home_5_player_id` | String | stats.ncaa.org player identifier for the home slot-5 on-floor player. |
| `home_5_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-5 on-floor player. |
| `away_1_player_id` | String | stats.ncaa.org player identifier for the away slot-1 on-floor player. |
| `away_1_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-1 on-floor player. |
| `away_2_player_id` | String | stats.ncaa.org player identifier for the away slot-2 on-floor player. |
| `away_2_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-2 on-floor player. |
| `away_3_player_id` | String | stats.ncaa.org player identifier for the away slot-3 on-floor player. |
| `away_3_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-3 on-floor player. |
| `away_4_player_id` | String | stats.ncaa.org player identifier for the away slot-4 on-floor player. |
| `away_4_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-4 on-floor player. |
| `away_5_player_id` | String | stats.ncaa.org player identifier for the away slot-5 on-floor player. |
| `away_5_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-5 on-floor player. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `is_fastbreak` | Boolean | Flag marking the shot as a fast-break attempt, from the stats.ncaa.org play text. |
| `is_from_turnover` | Boolean | Flag marking an attempt generated off an opponent turnover. |
| `is_paint` | Boolean | Flag marking the shot as attempted in the paint. |
| `is_second_chance` | Boolean | Flag marking a second-chance attempt following an offensive rebound. |
| `assist_player` | String | Name of the player credited with the assist on a made shot, when present. |
| `ft_number` | Int64 | Which free throw of the trip this attempt is (1 of 2, 2 of 2, etc.). |
| `ft_attempts` | Int64 | Total free throws in the trip this attempt belongs to. |
| `foul_class` | String | Parsed category of the foul event (e.g. personal, offensive). |
| `is_shooting_foul` | Boolean | Flag marking the foul as a shooting foul. |
| `is_looseball_foul` | Boolean | Flag marking the foul as a loose-ball foul. |
| `is_one_and_one` | Boolean | Flag marking a bonus one-and-one free-throw trip. |
| `is_flagrant` | Boolean | Flag marking the foul as flagrant. |
| `foul_tech_class` | String | Parsed technical-foul class for technical foul events, when present. |
| `ft_awarded` | Int64 | Number of free throws awarded by the foul. |
| `turnover_type` | String | Parsed turnover subtype (e.g. lost ball, bad pass, travel). |
| `is_team_turnover` | Boolean | Flag marking a turnover charged to the team rather than an individual player. |
| `timeout_type` | String | Type of timeout called (e.g. full, 30-second, media). |
| `challenge_outcome` | String | Outcome of a coach's challenge or video-review event, when present. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_pbp(seasons=2024)
```

## load_ncaa_wbb_schedule

Release: [ncaa_wbb_schedule](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_schedule) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_schedule/ncaa_wbb_schedule_{season}.parquet`
### Returns {#load_ncaa_wbb_schedule-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String |  |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home` | String | Home. |
| `away` | String | Away record. |
| `home_score` | Int64 | Home team score at the time of the play. |
| `away_score` | Int64 | Away team score at the time of the play. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_schedule(seasons=2024)
```

## load_ncaa_wbb_player_box

Release: [ncaa_wbb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_player_box) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_player_box/ncaa_wbb_player_box_{season}.parquet`
### Returns {#load_ncaa_wbb_player_box-returns}

| col_name | type | description |
|---|---|---|
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home` | String | Home. |
| `away` | String | Away record. |
| `team` | String | Team-side label or team identifier. |
| `player` | String | Player name. |
| `mins` | Float64 | Minutes played, derived from the lineup walk-forward through the play-by-play. |
| `o_poss` | Float64 | Offensive possessions the player was on the floor for. |
| `pts` | Float64 | Points scored. |
| `orb` | Float64 | Offensive rebounds. |
| `drb` | Float64 | Defensive rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `efg_pct` | Float64 | Effective field-goal percentage, weighting made threes at 1.5. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `tpm` | Float64 | Three-point field goals made. |
| `tpa` | Float64 | Three-point field goals attempted. |
| `tp_pct` | Float64 | Three-point field-goal percentage. |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `rimm` | Float64 | Rim shots (dunks, layups, hooks, tip-ins) made. |
| `rima` | Float64 | Rim shots (dunks, layups, hooks, tip-ins) attempted. |
| `rim_pct` | Float64 | Field-goal percentage on rim attempts. |
| `midm` | Float64 | Mid-range (non-rim two-point) shots made. |
| `mida` | Float64 | Mid-range (non-rim two-point) shots attempted. |
| `mid_pct` | Float64 | Field-goal percentage on mid-range attempts. |
| `pbackm` | Float64 | Putbacks made — rim shots immediately following an offensive rebound. |
| `pbacka` | Float64 | Putbacks attempted — rim shots immediately following an offensive rebound. |
| `pback_pct` | Float64 | Field-goal percentage on putback attempts. |
| `blk_rim` | Float64 | Blocks recorded against opponent rim attempts. |
| `blk_mid` | Float64 | Blocks recorded against opponent mid-range attempts. |
| `blk_three` | Float64 | Blocks recorded against opponent three-point attempts. |
| `pct_fga_trans` | Float64 | Share of the player's field-goal attempts taken in transition. |
| `pct_tpa_trans` | Float64 | Share of the player's three-point attempts taken in transition. |
| `pct_rima_trans` | Float64 | Share of the player's rim attempts taken in transition. |
| `pct_fgm_trans` | Float64 | Share of the player's field-goal makes that came in transition. |
| `pct_tpm_trans` | Float64 | Share of the player's three-point makes that came in transition. |
| `pct_rimm_trans` | Float64 | Share of the player's rim makes that came in transition. |
| `pct_fgm_ast` | Float64 | Share of the player's made field goals that were assisted. |
| `pct_tpm_ast` | Float64 | Share of the player's made threes that were assisted. |
| `pct_rimm_ast` | Float64 | Share of the player's made rim shots that were assisted. |
| `pts_trans` | Float64 | Points scored in transition possessions. |
| `orb_trans` | Float64 | Offensive rebounds in transition possessions. |
| `drb_trans` | Float64 | Defensive rebounds in transition possessions. |
| `ast_trans` | Float64 | Assists in transition possessions. |
| `stl_trans` | Float64 | Steals in transition possessions. |
| `blk_trans` | Float64 | Blocks in transition possessions. |
| `tov_trans` | Float64 | Turnovers in transition possessions. |
| `ts_pct_trans` | Float64 | True-shooting percentage in transition possessions. |
| `efg_pct_trans` | Float64 | Effective field-goal percentage in transition possessions. |
| `fgm_trans` | Float64 | Field goals made in transition possessions. |
| `fga_trans` | Float64 | Field goals attempted in transition possessions. |
| `fg_pct_trans` | Float64 | Field-goal percentage in transition possessions. |
| `tpm_trans` | Float64 | Three-pointers made in transition possessions. |
| `tpa_trans` | Float64 | Three-pointers attempted in transition possessions. |
| `tp_pct_trans` | Float64 | Three-point percentage in transition possessions. |
| `ftm_trans` | Float64 | Free throws made in transition possessions. |
| `fta_trans` | Float64 | Free throws attempted in transition possessions. |
| `ft_pct_trans` | Float64 | Free-throw percentage in transition possessions. |
| `rimm_trans` | Float64 | Rim shots made in transition possessions. |
| `rima_trans` | Float64 | Rim shots attempted in transition possessions. |
| `rim_pct_trans` | Float64 | Rim field-goal percentage in transition possessions. |
| `midm_trans` | Float64 | Mid-range shots made in transition possessions. |
| `mida_trans` | Float64 | Mid-range shots attempted in transition possessions. |
| `mid_pct_trans` | Float64 | Mid-range field-goal percentage in transition possessions. |
| `pts_half` | Float64 | Points scored in halfcourt possessions. |
| `orb_half` | Float64 | Offensive rebounds in halfcourt possessions. |
| `drb_half` | Float64 | Defensive rebounds in halfcourt possessions. |
| `ast_half` | Float64 | Assists in halfcourt possessions. |
| `stl_half` | Float64 | Steals in halfcourt possessions. |
| `blk_half` | Float64 | Blocks in halfcourt possessions. |
| `tov_half` | Float64 | Turnovers in halfcourt possessions. |
| `ts_pct_half` | Float64 | True-shooting percentage in halfcourt possessions. |
| `efg_pct_half` | Float64 | Effective field-goal percentage in halfcourt possessions. |
| `fgm_half` | Float64 | Field goals made in halfcourt possessions. |
| `fga_half` | Float64 | Field goals attempted in halfcourt possessions. |
| `fg_pct_half` | Float64 | Field-goal percentage in halfcourt possessions. |
| `tpm_half` | Float64 | Three-pointers made in halfcourt possessions. |
| `tpa_half` | Float64 | Three-pointers attempted in halfcourt possessions. |
| `tp_pct_half` | Float64 | Three-point percentage in halfcourt possessions. |
| `ftm_half` | Float64 | Free throws made in halfcourt possessions. |
| `fta_half` | Float64 | Free throws attempted in halfcourt possessions. |
| `ft_pct_half` | Float64 | Free-throw percentage in halfcourt possessions. |
| `rimm_half` | Float64 | Rim shots made in halfcourt possessions. |
| `rima_half` | Float64 | Rim shots attempted in halfcourt possessions. |
| `rim_pct_half` | Float64 | Rim field-goal percentage in halfcourt possessions. |
| `midm_half` | Float64 | Mid-range shots made in halfcourt possessions. |
| `mida_half` | Float64 | Mid-range shots attempted in halfcourt possessions. |
| `mid_pct_half` | Float64 | Mid-range field-goal percentage in halfcourt possessions. |
| `pts_ast` | Float64 | Points from the player's assisted field-goal makes. |
| `fgm_ast` | Float64 | Assisted field-goal makes. |
| `tpm_ast` | Float64 | Assisted three-point makes. |
| `rimm_ast` | Float64 | Assisted rim makes. |
| `midm_ast` | Float64 | Assisted mid-range makes. |
| `pts_unast` | Float64 | Points from the player's unassisted field-goal makes. |
| `efg_pct_unast` | Float64 | Effective field-goal percentage in the unassisted split (makes for which no assist was credited). |
| `fgm_unast` | Float64 | Field goals made in the unassisted split (makes for which no assist was credited). |
| `fga_unast` | Float64 | Field goals attempted in the unassisted split (makes for which no assist was credited). |
| `fg_pct_unast` | Float64 | Field-goal percentage in the unassisted split (makes for which no assist was credited). |
| `tpm_unast` | Float64 | Three-pointers made in the unassisted split (makes for which no assist was credited). |
| `tpa_unast` | Float64 | Three-pointers attempted in the unassisted split (makes for which no assist was credited). |
| `tp_pct_unast` | Float64 | Three-point percentage in the unassisted split (makes for which no assist was credited). |
| `rimm_unast` | Float64 | Rim shots made in the unassisted split (makes for which no assist was credited). |
| `rima_unast` | Float64 | Rim shots attempted in the unassisted split (makes for which no assist was credited). |
| `rim_pct_unast` | Float64 | Rim field-goal percentage in the unassisted split (makes for which no assist was credited). |
| `midm_unast` | Float64 | Mid-range shots made in the unassisted split (makes for which no assist was credited). |
| `mida_unast` | Float64 | Mid-range shots attempted in the unassisted split (makes for which no assist was credited). |
| `mid_pct_unast` | Float64 | Mid-range field-goal percentage in the unassisted split (makes for which no assist was credited). |
| `contest_id` | String |  |
| `home_ncaa_team_id` | String | stats.ncaa.org team identifier for the home team. |
| `home_espn_team_id` | String | ESPN home team id (NA for bart-only rows). |
| `away_ncaa_team_id` | String | stats.ncaa.org team identifier for the away team. |
| `away_espn_team_id` | String | ESPN away team id (NA for bart-only rows). |
| `team_ncaa_team_id` | String | stats.ncaa.org team identifier of the player's team. |
| `team_espn_team_id` | String | ESPN team identifier of the player's team, via the NCAA-to-ESPN crosswalk. |
| `player_id` | String | Unique player identifier. |
| `clean_name` | String | Normalized (diacritics- and punctuation-cleaned) player name used to join across the NCAA datasets. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_player_box(seasons=2024)
```

## load_ncaa_wbb_team_box

Release: [ncaa_wbb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_team_box) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_team_box/ncaa_wbb_team_box_{season}.parquet`
### Returns {#load_ncaa_wbb_team_box-returns}

| col_name | type | description |
|---|---|---|
| `home` | String | Home. |
| `away` | String | Away record. |
| `team` | String | Team-side label or team identifier. |
| `mins` | Float64 | Minutes covered by the team's tracked lineups in the game. |
| `o_mins` | Float64 | Minutes spent on tracked offensive possessions. |
| `d_mins` | Float64 | Minutes spent on tracked defensive possessions. |
| `o_poss` | Float64 | Offensive possessions. |
| `d_poss` | Float64 | Defensive possessions. |
| `ortg` | Float64 | Offensive rating — points scored per 100 possessions. |
| `drtg` | Float64 | Defensive rating — points allowed per 100 possessions. |
| `netrtg` | Float64 | Net rating — offensive rating minus defensive rating. |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 | Points allowed. |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 | Opponent field-goal attempts. |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 | Opponent field goals made. |
| `tpa` | Float64 | Three-point attempts. |
| `d_tpa` | Float64 | Opponent three-point attempts. |
| `tpm` | Float64 | Three-pointers made. |
| `d_tpm` | Float64 | Opponent three-pointers made. |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 | Opponent free-throw attempts. |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 | Opponent free throws made. |
| `rima` | Float64 | Rim shots (dunks, layups, hooks, tip-ins) attempted. |
| `d_rima` | Float64 | Opponent rim shots attempted. |
| `rimm` | Float64 | Rim shots made. |
| `d_rimm` | Float64 | Opponent rim shots made. |
| `orb` | Float64 | Offensive rebounds. |
| `d_orb` | Float64 | Opponent offensive rebounds. |
| `drb` | Float64 | Defensive rebounds. |
| `d_drb` | Float64 | Opponent defensive rebounds. |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 | Opponent blocks (own shots blocked). |
| `to` | Float64 | To. |
| `d_to` | Float64 | Opponent turnovers forced. |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 | Opponent assists allowed. |
| `e_poss` | Float64 | Estimated possessions — the average of the team's and the opponent's raw possession counts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 | Opponent field-goal percentage. |
| `tpp` | Float64 | Three-point percentage. |
| `d_tpp` | Float64 | Opponent three-point percentage. |
| `ftp` | Float64 | Free-throw percentage. |
| `d_ftp` | Float64 | Opponent free-throw percentage. |
| `efg_pct` | Float64 | Effective field-goal percentage, weighting made threes at 1.5. |
| `d_efg_pct` | Float64 | Opponent effective field-goal percentage. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 | Opponent true-shooting percentage. |
| `rim_pct` | Float64 | Field-goal percentage on rim attempts. |
| `d_rim_pct` | Float64 | Opponent field-goal percentage on rim attempts. |
| `mid_pct` | Float64 | Field-goal percentage on mid-range attempts. |
| `d_mid_pct` | Float64 | Opponent field-goal percentage on mid-range attempts. |
| `tp_rate` | Float64 | Three-point attempts as a share of field-goal attempts. |
| `d_tp_rate` | Float64 | Opponent three-point attempts as a share of their field-goal attempts. |
| `rim_rate` | Float64 | Rim attempts as a share of field-goal attempts. |
| `d_rim_rate` | Float64 | Opponent rim attempts as a share of their field-goal attempts. |
| `mid_rate` | Float64 | Mid-range attempts as a share of field-goal attempts. |
| `d_mid_rate` | Float64 | Opponent mid-range attempts as a share of their field-goal attempts. |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 | Opponent free-throw attempts relative to their field-goal attempts. |
| `ast_rate` | Float64 | Share of the team's made field goals that were assisted. |
| `d_ast_rate` | Float64 | Share of opponent made field goals that were assisted. |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 | Opponent turnovers as a share of their possessions (forced-turnover rate). |
| `blk_rate` | Float64 | Share of opponent two-point attempts the team blocked. |
| `o_blk_rate` | Float64 | Share of the team's own two-point attempts blocked by the opponent. |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 | Average seconds per offensive possession. |
| `d_time_per_poss` | Float64 | Average seconds per defensive possession. |
| `contest_id` | String |  |
| `home_ncaa_team_id` | String | stats.ncaa.org team identifier for the home team. |
| `home_espn_team_id` | String | ESPN home team id (NA for bart-only rows). |
| `away_ncaa_team_id` | String | stats.ncaa.org team identifier for the away team. |
| `away_espn_team_id` | String | ESPN away team id (NA for bart-only rows). |
| `team_ncaa_team_id` | String | stats.ncaa.org team identifier of the team the row belongs to. |
| `team_espn_team_id` | String | ESPN team identifier of the team, via the NCAA-to-ESPN crosswalk. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_team_box(seasons=2024)
```

## load_ncaa_wbb_rosters

Release: [ncaa_wbb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_rosters/ncaa_wbb_rosters_{season}.parquet`
### Returns {#load_ncaa_wbb_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team` | String | Team-side label or team identifier. |
| `player` | String | Player name. |
| `games` | Int64 | Games played. |

```python
load_ncaa_wbb_rosters(seasons=2024)
```

## load_ncaa_wbb_team_rosters

Release: [ncaa_wbb_team_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_team_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_team_rosters/ncaa_wbb_team_rosters_{season}.parquet`
### Returns {#load_ncaa_wbb_team_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team_id` | String | Unique team identifier. |
| `team` | String | Team-side label or team identifier. |
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `clean_name` | String | Normalized (diacritics- and punctuation-cleaned) player name used to join across the NCAA datasets. |
| `name` | String | Display name. |
| `jersey` | String | Jersey number worn by the player. |
| `class` | String | College class / draft eligibility note. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `ht_inches` | Int64 | Player height converted to total inches from the stats.ncaa.org roster listing. |
| `hometown` | String | Player hometown. |
| `high_school` | String |  |
| `gp` | String | Games played. |
| `gs` | String | Games started. |

```python
load_ncaa_wbb_team_rosters(seasons=2024)
```

## load_ncaa_wbb_team_ids

Release: [ncaa_wbb_team_ids](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_team_ids) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_team_ids/ncaa_wbb_team_ids_{season}.parquet`
### Returns {#load_ncaa_wbb_team_ids-returns}

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `conference` | String | Filter players or teams by conference. |
| `id` | String | Unique play identification number |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_team_ids(seasons=2024)
```

## load_ncaa_wbb_possessions

Release: [ncaa_wbb_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_possessions) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_possessions/ncaa_wbb_possessions_{season}.parquet`
### Returns {#load_ncaa_wbb_possessions-returns}

| col_name | type | description |
|---|---|---|
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home` | String | Home. |
| `away` | String | Away record. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `poss_num` | Int64 | Sequential possession number within the game. |
| `poss_team` | String | Name of the team in possession. |
| `home_1` | String | Name of the home team's on-floor player in lineup slot 1 for the possession, from the substitution walk-forward. |
| `home_2` | String | Name of the home team's on-floor player in lineup slot 2 for the possession, from the substitution walk-forward. |
| `home_3` | String | Name of the home team's on-floor player in lineup slot 3 for the possession, from the substitution walk-forward. |
| `home_4` | String | Name of the home team's on-floor player in lineup slot 4 for the possession, from the substitution walk-forward. |
| `home_5` | String | Name of the home team's on-floor player in lineup slot 5 for the possession, from the substitution walk-forward. |
| `away_1` | String | Name of the away team's on-floor player in lineup slot 1 for the possession, from the substitution walk-forward. |
| `away_2` | String | Name of the away team's on-floor player in lineup slot 2 for the possession, from the substitution walk-forward. |
| `away_3` | String | Name of the away team's on-floor player in lineup slot 3 for the possession, from the substitution walk-forward. |
| `away_4` | String | Name of the away team's on-floor player in lineup slot 4 for the possession, from the substitution walk-forward. |
| `away_5` | String | Name of the away team's on-floor player in lineup slot 5 for the possession, from the substitution walk-forward. |
| `home_score` | Int64 | Home team score at the time of the play. |
| `away_score` | Int64 | Away team score at the time of the play. |
| `pts` | Int64 | Points scored. |
| `is_assisted` | Int64 | 1 when the possession's made field goal was assisted, else 0. |
| `is_transition` | Int64 | 1 for transition possessions, else 0. |
| `is_garbage_time` | Int64 | 1 for possessions in garbage time under the score-margin and clock rule, else 0. |
| `start_event_type` | String | Event type that opened the possession (e.g., a defensive rebound or a made-basket inbound). |
| `first_shot_time` | Int64 | Clock time in seconds at the possession's first shot attempt, from the possession segmentation engine. |
| `first_shot_type` | String | Shot class of the possession's first attempt (rim, mid-range, or three). |
| `last_event_time` | Int64 | Clock time in seconds at the possession's final event. |
| `last_event_type` | String | Event type that ended the possession (e.g., a made shot, turnover, or defensive rebound). |
| `contest_id` | String |  |
| `home_ncaa_team_id` | String | stats.ncaa.org team identifier for the home team. |
| `home_espn_team_id` | String | ESPN home team id (NA for bart-only rows). |
| `away_ncaa_team_id` | String | stats.ncaa.org team identifier for the away team. |
| `away_espn_team_id` | String | ESPN away team id (NA for bart-only rows). |
| `poss_team_ncaa_team_id` | String | stats.ncaa.org team identifier of the team in possession. |
| `poss_team_espn_team_id` | String | ESPN team identifier of the team in possession, via the NCAA-to-ESPN crosswalk. |
| `home_1_player_id` | String | stats.ncaa.org player identifier for the home slot-1 on-floor player. |
| `home_1_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-1 on-floor player. |
| `home_2_player_id` | String | stats.ncaa.org player identifier for the home slot-2 on-floor player. |
| `home_2_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-2 on-floor player. |
| `home_3_player_id` | String | stats.ncaa.org player identifier for the home slot-3 on-floor player. |
| `home_3_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-3 on-floor player. |
| `home_4_player_id` | String | stats.ncaa.org player identifier for the home slot-4 on-floor player. |
| `home_4_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-4 on-floor player. |
| `home_5_player_id` | String | stats.ncaa.org player identifier for the home slot-5 on-floor player. |
| `home_5_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the home slot-5 on-floor player. |
| `away_1_player_id` | String | stats.ncaa.org player identifier for the away slot-1 on-floor player. |
| `away_1_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-1 on-floor player. |
| `away_2_player_id` | String | stats.ncaa.org player identifier for the away slot-2 on-floor player. |
| `away_2_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-2 on-floor player. |
| `away_3_player_id` | String | stats.ncaa.org player identifier for the away slot-3 on-floor player. |
| `away_3_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-3 on-floor player. |
| `away_4_player_id` | String | stats.ncaa.org player identifier for the away slot-4 on-floor player. |
| `away_4_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-4 on-floor player. |
| `away_5_player_id` | String | stats.ncaa.org player identifier for the away slot-5 on-floor player. |
| `away_5_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the away slot-5 on-floor player. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_possessions(seasons=2024)
```

## load_ncaa_wbb_lineups

Release: [ncaa_wbb_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_lineups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_lineups/ncaa_wbb_lineups_{season}.parquet`
### Returns {#load_ncaa_wbb_lineups-returns}

| col_name | type | description |
|---|---|---|
| `lineup_key` | String | Sorted player-code key identifying the five-player unit on the floor (hoop-explorer convention). |
| `date` | String | Date in YYYY-MM-DD format. |
| `location_type` | String | Whether the lineup's team was the Home or Away side in the game. |
| `team` | String | Team-side label or team identifier. |
| `team_year` | Int64 | Season year of the team-season the lineup row belongs to. |
| `opponent` | String | Opponent. |
| `lineup_id` | String | Sorted-name identifier of the five-player lineup, joined from the on-floor player names. |
| `start_min` | Float64 | Game minute at which the stint began. |
| `end_min` | Float64 | Game minute at which the stint ended. |
| `duration_mins` | Float64 | Length of the stint in minutes. |
| `player_1` | String | Name of the first player (in sorted order) of the five-player lineup. |
| `player_2` | String | Name of the second player (in sorted order) of the five-player lineup. |
| `player_3` | String | Name of the third player (in sorted order) of the five-player lineup. |
| `player_4` | String | Name of the fourth player (in sorted order) of the five-player lineup. |
| `player_5` | String | Name of the fifth player (in sorted order) of the five-player lineup. |
| `players_in` | String | Delimited names of the players substituted in at the start of the stint. |
| `players_out` | String | Delimited names of the players substituted out at the end of the stint. |
| `start_scored` | Int64 | Team points scored at the moment the stint began. |
| `start_allowed` | Int64 | Points allowed at the moment the stint began. |
| `end_scored` | Int64 | Team points scored at the moment the stint ended. |
| `end_allowed` | Int64 | Points allowed at the moment the stint ended. |
| `start_diff` | Int64 | Score margin (scored minus allowed) when the stint began. |
| `end_diff` | Int64 | Score margin (scored minus allowed) when the stint ended. |
| `player_count_error` | Null | Flag marking stints where the reconciled on-floor count was not exactly five players (all-null when clean). |
| `poss` | Int64 | Poss. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `fga` | Int64 | Field goal attempts. |
| `fgm` | Int64 | Field goals made. |
| `rima` | Int64 | Rim shots (dunks, layups, hooks, tip-ins) attempted by the lineup during the stint. |
| `rimm` | Int64 | Rim shots made by the lineup during the stint. |
| `rim_ast` | Int64 | Assisted rim makes by the lineup during the stint. |
| `mida` | Int64 | Mid-range shots attempted by the lineup during the stint. |
| `midm` | Int64 | Mid-range shots made by the lineup during the stint. |
| `mid_ast` | Int64 | Assisted mid-range makes by the lineup during the stint. |
| `fg2a` | Int64 | Two-point field goals attempted by the lineup during the stint. |
| `fg2m` | Int64 | Two-point field goals made by the lineup during the stint. |
| `tpa` | Int64 | Three-pointers attempted by the lineup during the stint. |
| `tpm` | Int64 | Three-pointers made by the lineup during the stint. |
| `tp_ast` | Int64 | Assisted three-point makes by the lineup during the stint. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `orb` | Int64 | Offensive rebounds by the lineup during the stint. |
| `drb` | Int64 | Defensive rebounds by the lineup during the stint. |
| `to` | Int64 | To. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `ast` | Int64 | Assists. |
| `foul` | Int64 | Fouls committed by the lineup during the stint. |
| `opp_poss` | Int64 | Opponent possessions while the lineup was on the floor during the stint. |
| `opp_pts` | Int64 | Opponent points. |
| `opp_plus_minus` | Int64 | Opponent scoring margin while the lineup was on the floor during the stint. |
| `opp_fga` | Int64 | Opponent field-goal attempts while the lineup was on the floor during the stint. |
| `opp_fgm` | Int64 | Opponent field goals made while the lineup was on the floor during the stint. |
| `opp_rima` | Int64 | Opponent rim shots attempted while the lineup was on the floor during the stint. |
| `opp_rimm` | Int64 | Opponent rim shots made while the lineup was on the floor during the stint. |
| `opp_rim_ast` | Int64 | Opponent assisted rim makes while the lineup was on the floor during the stint. |
| `opp_mida` | Int64 | Opponent mid-range shots attempted while the lineup was on the floor during the stint. |
| `opp_midm` | Int64 | Opponent mid-range shots made while the lineup was on the floor during the stint. |
| `opp_mid_ast` | Int64 | Opponent assisted mid-range makes while the lineup was on the floor during the stint. |
| `opp_fg2a` | Int64 | Opponent two-point attempts while the lineup was on the floor during the stint. |
| `opp_fg2m` | Int64 | Opponent two-point makes while the lineup was on the floor during the stint. |
| `opp_tpa` | Int64 | Opponent three-point attempts while the lineup was on the floor during the stint. |
| `opp_tpm` | Int64 | Opponent three-point makes while the lineup was on the floor during the stint. |
| `opp_tp_ast` | Int64 | Opponent assisted three-point makes while the lineup was on the floor during the stint. |
| `opp_fta` | Int64 | Opponent free-throw attempts while the lineup was on the floor during the stint. |
| `opp_ftm` | Int64 | Opponent free throws made while the lineup was on the floor during the stint. |
| `opp_orb` | Int64 | Opponent offensive rebounds while the lineup was on the floor during the stint. |
| `opp_drb` | Int64 | Opponent defensive rebounds while the lineup was on the floor during the stint. |
| `opp_to` | Int64 | Opponent turnovers while the lineup was on the floor during the stint. |
| `opp_stl` | Int64 | Opponent steals while the lineup was on the floor during the stint. |
| `opp_blk` | Int64 | Opponent blocks while the lineup was on the floor during the stint. |
| `opp_ast` | Int64 | Opponent assists while the lineup was on the floor during the stint. |
| `opp_foul` | Int64 | Opponent fouls committed while the lineup was on the floor during the stint. |
| `stint_num` | Int64 | Sequential on-floor stint number for the lineup within the game. |
| `contest_id` | String |  |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_wbb_lineups(seasons=2024)
```

## load_ncaa_wbb_matchup_stints

Release: [ncaa_wbb_matchup_stints](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_matchup_stints) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_matchup_stints/ncaa_wbb_matchup_stints_{season}.parquet`
### Returns {#load_ncaa_wbb_matchup_stints-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String |  |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home` | String | Home. |
| `away` | String | Away record. |
| `game_stint_num` | Int64 | Sequential stint number within the game, incremented at every substitution by either team. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `start_seconds` | Int64 | Elapsed game seconds at which the stint began. |
| `end_seconds` | Int64 | Elapsed game seconds at which the stint ended. |
| `duration_seconds` | Int64 | Duration of the lineup stint in seconds. |
| `matchup_key` | String | Combined home-plus-away lineup key identifying the ten-player matchup on the floor. |
| `home_lineup_key` | String | Sorted player-code key for the home five on the floor. |
| `away_lineup_key` | String | Sorted player-code key for the away five on the floor. |
| `home_lineup` | String | Delimited names of the home five on the floor during the stint. |
| `away_lineup` | String | Delimited names of the away five on the floor during the stint. |
| `end_home_score` | Int64 | Home team score when the stint ended. |
| `end_away_score` | Int64 | Away team score when the stint ended. |
| `n_events` | Int64 | Number of play-by-play events falling within the stint. |
| `n_possessions` | Int64 | Number of possessions falling within the stint. |
| `start_home_score` | Int64 | Home team score when the stint began. |
| `start_away_score` | Int64 | Away team score when the stint began. |
| `home_pts` | Int64 | Points scored by the home team during the stint. |
| `away_pts` | Int64 | Points scored by the away team during the stint. |
| `home_1` | String | Name of the home team's on-floor player in lineup slot 1 for the stint. |
| `home_2` | String | Name of the home team's on-floor player in lineup slot 2 for the stint. |
| `home_3` | String | Name of the home team's on-floor player in lineup slot 3 for the stint. |
| `home_4` | String | Name of the home team's on-floor player in lineup slot 4 for the stint. |
| `home_5` | String | Name of the home team's on-floor player in lineup slot 5 for the stint. |
| `away_1` | String | Name of the away team's on-floor player in lineup slot 1 for the stint. |
| `away_2` | String | Name of the away team's on-floor player in lineup slot 2 for the stint. |
| `away_3` | String | Name of the away team's on-floor player in lineup slot 3 for the stint. |
| `away_4` | String | Name of the away team's on-floor player in lineup slot 4 for the stint. |
| `away_5` | String | Name of the away team's on-floor player in lineup slot 5 for the stint. |

```python
load_ncaa_wbb_matchup_stints(seasons=2024)
```

## load_ncaa_wbb_shots

Release: [ncaa_wbb_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_shots/ncaa_wbb_shots_{season}.parquet`
### Returns {#load_ncaa_wbb_shots-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team_id` | String | Unique team identifier. |
| `shooter_id` | String | Unique identifier for shooter. |
| `shot_x` | Float64 | Court x-coordinate of the attempt in feet, decoded from the stats.ncaa.org shot-chart map. |
| `shot_y` | Float64 | Court y-coordinate of the attempt in feet, decoded from the stats.ncaa.org shot-chart map. |
| `dist_ft` | Float64 | Shot distance from the basket in feet. |
| `shot_zone` | String | Labeled zone of the attempt (rim, mid-range, or three-point). |
| `shot_type` | String | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `made` | Boolean | Whether the shot was made. |
| `point_value` | Int64 | Point value of the attempt (2 or 3). |
| `period` | Null | Period of the game (1-4 quarters; 5+ for OT). |
| `sec_left` | Null | Seconds remaining in the period when the shot was taken (all-null in current captures). |
| `source` | String |  |
| `contest_id` | String |  |
| `ncaa_team_id` | String | stats.ncaa.org team identifier of the shooting team. |
| `espn_team_id` | String | ESPN team id (canonical key). |
| `shooter_player_id` | String | stats.ncaa.org player identifier of the shooter. |
| `shooter_clean_name` | String | Normalized (diacritics- and punctuation-cleaned) name of the shooter. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |

```python
load_ncaa_wbb_shots(seasons=2024)
```

## load_ncaa_wbb_rapm_within_team

Release: [ncaa_wbb_rapm_within_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_wbb_rapm_within_team) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_wbb_rapm_within_team/ncaa_wbb_rapm_within_team_{season}.parquet`
### Returns {#load_ncaa_wbb_rapm_within_team-returns}

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `player_code` | String | Short unique-within-team player code generated from the player's name (hoop-explorer convention). |
| `rapm_off` | Float64 | Ridge-regressed offensive RAPM per 100 possessions, estimated relative to the player's own teammates. |
| `rapm_def` | Float64 | Ridge-regressed defensive RAPM per 100 possessions relative to teammates; positive means good defense. |
| `team_off_poss` | Float64 | Team offensive possessions underlying the within-team fit. |
| `num_players` | Int64 | Number of players in the team's RAPM design matrix. |
| `rapm_net` | Float64 | Net RAPM — the sum of the offensive and defensive components, per 100 possessions. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `player_id` | String | Unique player identifier. |
| `team_id` | String | Unique team identifier. |
| `person_id` | String | Unique player identifier (V3 endpoints). |

```python
load_ncaa_wbb_rapm_within_team(seasons=2024)
```
