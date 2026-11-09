---
title: "WNBA dataset loaders — Player"
sidebar_label: "Player"
sidebar_position: 1
description: "WNBA dataset loaders — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA dataset loaders — Player

## load_wnba_player_boxscore

Release: [espn_wnba_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_player_boxscores/player_box_{season}.parquet`
### Returns {#load_wnba_player_boxscore-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `team_id` | Int32 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_location` | String | Team city or location string. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `minutes` | Float64 | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int32 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int32 | Field goal attempts (2-pt + 3-pt). |
| `three_point_field_goals_made` | Int32 | Three-point field goals made. |
| `three_point_field_goals_attempted` | Int32 | Three-point field goal attempts. |
| `free_throws_made` | Int32 | Free throws made. |
| `free_throws_attempted` | Int32 | Free throw attempts. |
| `offensive_rebounds` | Int32 | Offensive rebounds. |
| `defensive_rebounds` | Int32 | Defensive rebounds. |
| `rebounds` | Int32 | Total rebounds. |
| `assists` | Int32 | Total assists. |
| `steals` | Int32 | Total steals. |
| `blocks` | Int32 | Total blocks. |
| `turnovers` | Int32 | Total turnovers. |
| `fouls` | Int32 | Personal fouls. |
| `plus_minus` | String | Plus/minus point differential while on court. |
| `points` | Int32 | Points scored. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `reason` | String | Reason. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_headshot_href` | String | Athlete headshot image URL. |
| `athlete_position_name` | String | Athlete position ('Guard', 'Forward', 'Center'). |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `team_display_name` | String | Full team display name. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_logo` | String | Team logo image URL. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `team_winner` | Boolean | TRUE if the team won this game. |
| `team_score` | Int32 | Team's score / final score. |
| `opponent_team_id` | Int32 | Unique identifier for the opponent team. |
| `opponent_team_name` | String | Opponent team display name. |
| `opponent_team_location` | String | Opponent team city / location. |
| `opponent_team_display_name` | String | Opponent team full display name. |
| `opponent_team_abbreviation` | String | Opponent team abbreviation. |
| `opponent_team_logo` | String | Opponent team logo URL. |
| `opponent_team_color` | String | Opponent team primary color (hex). |
| `opponent_team_alternate_color` | String | Opponent team alternate color (hex). |
| `opponent_team_score` | Int32 | Opponent team's score. |

```python
load_wnba_player_boxscore(seasons=2024)
```

## load_wnba_player_season_stats

Release: [espn_wnba_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_player_season_stats/player_season_stats_{season}.parquet`

:::caution[Coverage]
Published seasons: every season from the floor except 2022 and 2023, which the release has no asset for; a request for one is skipped with a warning.
:::

### Returns {#load_wnba_player_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_first_name` | String |  |
| `athlete_last_name` | String |  |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `athlete_jersey` | String | Athlete jersey number. |
| `team_id` | Int32 | Unique team identifier. |
| `team_display_name` | String | Full team display name. |
| `category` | String | Category label. |
| `stat_label` | String |  |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String | ESPN's prose definition of the statistic on this row, for example The average number of points scored per game for avgPoints; combined made-attempted stats carry both halves joined by a hyphen. |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wnba_player_season_stats(seasons=2024)
```

## load_wnba_player_crosswalk

Release: [wnba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_crosswalk/wnba_player_crosswalk_{season}.parquet`

:::caution[Coverage]
wnba_player_id is Int64, as WNBA Stats ships player_id / person_id, so the bridge joins without a cast.
:::

### Returns {#load_wnba_player_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `espn_team_id` | Int32 | ESPN team id (canonical key). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_name` | String | Player name. |
| `espn_athlete_id` | Int32 | ESPN athlete id. |
| `espn_full_name` | String | ESPN full name. |
| `espn_jersey` | String | ESPN jersey number. |
| `espn_position` | String | ESPN position abbreviation. |
| `wnba_player_id` | Int64 | Player identifier on stats.wnba.com matched to the ESPN player. |
| `wnba_player_name` | String | Player display name on the stats.wnba.com side of the crosswalk. |
| `wnba_jersey_num` | String | Jersey number listed on the stats.wnba.com side of the crosswalk. |
| `wnba_position` | String | Position listed on the stats.wnba.com side of the crosswalk. |
| `fox_athlete_id` | String | Fox athlete id (NA if unmatched). |
| `fox_player` | String | Fox player name (NA if unmatched). |
| `fox_jersey` | String | Fox jersey number (NA if unmatched). |
| `fox_position_group` | String | Fox position group label (NA if unmatched). |
| `yahoo_player_id` | String | Yahoo player id (NA placeholder). |
| `yahoo_player_name` | String | Yahoo player name (NA placeholder). |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | Float64 | Jaro-Winkler score or 1 for exact (NA if none). |
| `match_keys` | String | NA (reserved for future use). |

```python
load_wnba_player_crosswalk(seasons=2026)
```

## load_wnba_player_core

Release: [espn_wnba_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_core) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_player_core/player_core_{season}.parquet`
### Returns {#load_wnba_player_core-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `guid` | String | Stable cross-league team GUID. |
| `uid` | String | ESPN UID string. |
| `slug` | String | URL-safe identifier. |
| `type` | String | Record type / category. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `age` | Int32 | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `birth_city` | String | Birth city. |
| `birth_state` | String | Birth state / region. |
| `birth_country` | String |  |
| `jersey` | String | Jersey number worn by the player. |
| `position_id` | Int32 | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `position_display_name` | String | Position display name. |
| `college_id` | Int32 | Unique identifier for college. |
| `current_team_id` | Int32 |  |
| `headshot_href` | String | Headshot image URL. |
| `experience_years` | Int32 | Experience years. |
| `status_id` | Int32 | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |
| `draft_year` | Int32 | Draft year (4-digit). |
| `draft_round` | Int32 | Round of the draft selection. |
| `draft_selection` | Int32 | Draft selection. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |

```python
load_wnba_player_core(seasons=2025)
```

## load_wnba_player_impact

Release: [wnba_player_impact](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_player_impact) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_player_impact/wnba_player_impact_{season}.parquet`

:::caution[Coverage]
The BOBAN (Bayesian Online Box-Augmented Net) columns boban_filtered_skill, boban_projected_rating and boban_projected_sd were renamed from darko_*; the loader returns both names for one release, so move to boban_* now (darko_* is deprecated and will be removed).
:::

### Returns {#load_wnba_player_impact-returns}

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | String | Nested list of member-team membership spans. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `o_rapm` | Float64 | Offensive RAPM: ridge-regularized on/off impact on team offense, in points per 100 possessions, at the fixed WNBA penalty described under rapm. |
| `d_rapm` | Float64 | Defensive RAPM: ridge-regularized on/off impact on team defense, in points per 100 possessions, at the fixed WNBA penalty described under rapm. |
| `rapm` | Float64 | Total RAPM: offensive plus defensive regularized on/off impact, in points per 100 possessions. The ridge penalty is one fixed WNBA value (1,931, sportsdataverse.nba.nba_rapm.RAPM_PENALTY) in every season and season type, so the scale compares across seasons (regular-season sd 1.47 to 2.13 per 100 over players with 200 possessions, 1997-2026; fewer possessions shrink harder, so part of a spread difference is sample size). Seasons published before November 2026 were fit at a per-season RidgeCV pick: 1,931 in 23 of 30 regular seasons, but 5,179 or 13,895 in 1999, 2001, 2002, 2004, 2011, 2014 and 2016 (sd 0.32 to 0.93 there) and 268 to 100,000 in the playoffs. Do not compare those seasons with others or with values fit at the fixed penalty. |
| `off_poss` | Int64 | Offensive possessions the player was on the floor for in the RAPM sample. |
| `def_poss` | Int64 | Defensive possessions the player was on the floor for in the RAPM sample. |
| `o_adj_rapm` | Float64 | Offensive prior-informed RAPM per 100 possessions: the ridge fit shrunk toward the player's box-score (SPM) prior. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025 +0.45). A player with no prior (a rookie, or no previous-season SPM) is shrunk toward 0 instead. Releases fit before #1137, whatever their build date, fit no intercept, so the league's points per possession leaked into every coefficient: o_adj_rapm averaged about +10.97 and d_adj_rapm -9.60 (2025), a thinly-used player kept less of that offset, and only their sum, adj_rapm, read on a common scale. |
| `d_adj_rapm` | Float64 | Defensive prior-informed RAPM per 100 possessions, positive for a defender who lowers opponent scoring. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025 +0.11). Releases fit before #1137, whatever their build date, averaged about -9.60 here (see o_adj_rapm). |
| `adj_rapm` | Float64 | Total prior-informed RAPM per 100 possessions, exactly the sum of o_adj_rapm and d_adj_rapm. Releases fit with sportsdataverse-py #1137 or later fit the ridge with an unpenalized intercept; releases fit before it (whatever their build date) fit none, which leaned this total toward heavy-minute players. |
| `ospm` | Float64 | Offensive statistical plus-minus: box-score features regressed onto the offensive RAPM target, per 100 possessions. |
| `dspm` | Float64 | Defensive statistical plus-minus: box-score features regressed onto the defensive RAPM target, per 100 possessions. |
| `spm` | Float64 | Statistical plus-minus: offensive plus defensive box-score estimate of the RAPM target, per 100 possessions. |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |
| `obpm` | Float64 | Offensive box plus-minus per 100 possessions for this season and season type only; bpm minus dbpm. |
| `dbpm` | Float64 | Defensive box plus-minus per 100 possessions for this season and season type only; bpm minus obpm. |
| `bpm` | Float64 | Box plus-minus (BPM 2.0) per 100 possessions for this season and season type only, not a career value: the box-score estimate of total impact, team-adjusted to the team's margin. |
| `war` | Float64 | Wins above replacement implied by the player's impact and playing time, calibrated from team points-per-win. |
| `boban_filtered_skill` | Float64 | BOBAN Kalman-filtered estimate of the player's current skill level. |
| `boban_projected_rating` | Float64 | BOBAN projected forward rating, including the empirical aging-curve drift. |
| `boban_projected_sd` | Float64 | Posterior standard deviation of the BOBAN projected rating. |
| `darko_filtered_skill` | Float64 | Deprecated alias of boban_filtered_skill, kept for one release; use that column. |
| `darko_projected_rating` | Float64 | Deprecated alias of boban_projected_rating, kept for one release; use that column. |
| `darko_projected_sd` | Float64 | Deprecated alias of boban_projected_sd, kept for one release; use that column. |
| `o_rapm_n` | Int64 | Possessions o_rapm is measured over: off_poss, his offensive possessions. o_rapm_rank and o_rapm_pct are set only when it reaches 610, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_rapm_n` | Int64 | Possessions d_rapm is measured over: def_poss, his defensive possessions. d_rapm_rank and d_rapm_pct are set only when it reaches 1,200, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `rapm_n` | Int64 | Possessions rapm is measured over: off_poss + def_poss, his possessions at both ends. rapm_rank and rapm_pct are set only when it reaches 1,600, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `o_adj_rapm_n` | Int64 | Possessions o_adj_rapm is measured over: off_poss, his offensive possessions. o_adj_rapm_rank and o_adj_rapm_pct are set only when it reaches 610, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_adj_rapm_n` | Int64 | Possessions d_adj_rapm is measured over: def_poss, his defensive possessions. d_adj_rapm_rank and d_adj_rapm_pct are set only when it reaches 1,200, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `adj_rapm_n` | Int64 | Possessions adj_rapm is measured over: off_poss + def_poss, his possessions at both ends. adj_rapm_rank and adj_rapm_pct are set only when it reaches 1,600, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `ospm_n` | Int64 | Possessions ospm is measured over: off_poss, his offensive possessions. ospm_rank and ospm_pct are set only when it reaches 610, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dspm_n` | Int64 | Possessions dspm is measured over: def_poss, his defensive possessions. dspm_rank and dspm_pct are set only when it reaches 1,200, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `spm_n` | Int64 | Possessions spm is measured over: off_poss + def_poss, his possessions at both ends. spm_rank and spm_pct are set only when it reaches 1,600, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `obpm_n` | Int64 | Possessions obpm is measured over: off_poss, his offensive possessions. obpm_rank and obpm_pct are set only when it reaches 610, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dbpm_n` | Int64 | Possessions dbpm is measured over: def_poss, his defensive possessions. dbpm_rank and dbpm_pct are set only when it reaches 1,200, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `bpm_n` | Int64 | Possessions bpm is measured over: off_poss + def_poss, his possessions at both ends. bpm_rank and bpm_pct are set only when it reaches 1,600, the WNBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `war_n` | Int64 | Possessions behind war: off_poss + def_poss. war is a count, not a rate, so its rank has no sample gate. |
| `boban_filtered_skill_n` | Int64 | Possessions behind boban_filtered_skill: off_poss + def_poss, this season's possessions at both ends. The boban filtered skill is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_filtered_skill_rank and boban_filtered_skill_pct are set only when this reaches 1,600. Conservative, since the filter also carries earlier seasons. |
| `boban_projected_rating_n` | Int64 | Possessions behind boban_projected_rating: off_poss + def_poss, this season's possessions at both ends. The boban next-season projection is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_projected_rating_rank and boban_projected_rating_pct are set only when this reaches 1,600. Conservative, since the filter also carries earlier seasons. |
| `boban_projected_sd_n` | Int64 | Possessions behind boban_projected_sd: off_poss + def_poss, this season's possessions at both ends. The boban projection's standard deviation is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_projected_sd_rank and boban_projected_sd_pct are set only when this reaches 1,600. Conservative, since the filter also carries earlier seasons. |
| `o_rapm_rank` | Float64 | Rank of o_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_rapm_n is under 610, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_rapm_rank` | Float64 | Rank of d_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_rapm_n is under 1,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `rapm_rank` | Float64 | Rank of rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when rapm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `o_adj_rapm_rank` | Float64 | Rank of o_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_adj_rapm_n is under 610, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_adj_rapm_rank` | Float64 | Rank of d_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_adj_rapm_n is under 1,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `adj_rapm_rank` | Float64 | Rank of adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when adj_rapm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `ospm_rank` | Float64 | Rank of ospm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when ospm_n is under 610, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dspm_rank` | Float64 | Rank of dspm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dspm_n is under 1,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `spm_rank` | Float64 | Rank of spm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when spm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `obpm_rank` | Float64 | Rank of obpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when obpm_n is under 610, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dbpm_rank` | Float64 | Rank of dbpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dbpm_n is under 1,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `bpm_rank` | Float64 | Rank of bpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when bpm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `war_rank` | Float64 | Rank of war among the season and season type's ranked players, 1 = best (higher is better), ties averaged; ungated (every player with a finite war qualifies), null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. |
| `boban_filtered_skill_rank` | Float64 | Rank of boban_filtered_skill among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when boban_filtered_skill_n is under 1,600, when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `boban_projected_rating_rank` | Float64 | Rank of boban_projected_rating among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when boban_projected_rating_n is under 1,600, when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `boban_projected_sd_rank` | Float64 | Rank of boban_projected_sd among the season and season type's ranked players, ASCENDING: 1 = the smallest sd, the most certain projection; ties averaged; null when boban_projected_sd_n is under 1,600, when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `o_rapm_pct` | Float64 | Percentile of o_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `d_rapm_pct` | Float64 | Percentile of d_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `rapm_pct` | Float64 | Percentile of rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `o_adj_rapm_pct` | Float64 | Percentile of o_adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `d_adj_rapm_pct` | Float64 | Percentile of d_adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `adj_rapm_pct` | Float64 | Percentile of adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `ospm_pct` | Float64 | Percentile of ospm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `dspm_pct` | Float64 | Percentile of dspm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `spm_pct` | Float64 | Percentile of spm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `obpm_pct` | Float64 | Percentile of obpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `dbpm_pct` | Float64 | Percentile of dbpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `bpm_pct` | Float64 | Percentile of bpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `war_pct` | Float64 | Percentile of war_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_filtered_skill_pct` | Float64 | Percentile of boban_filtered_skill_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_projected_rating_pct` | Float64 | Percentile of boban_projected_rating_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_projected_sd_pct` | Float64 | Percentile of boban_projected_sd_rank, 100 * (n + 1 - rank) / (n + 1): high means a more certain projection (a smaller sd), not a larger one; null where the rank is. |

```python
load_wnba_player_impact(seasons=2024)
```

## load_wnba_player_aliases

Release: [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_player_aliases.parquet`

:::caution[Coverage]
One season-less file: every player id another source (fox, wnba_stats, yahoo) uses for an ESPN athlete (athlete_id, a string; athlete_id_source = espn), with the ESPN team the match was made on (team_id) and the seasons it held (valid_from / valid_to, inclusive). match_method and match_confidence carry the matching leg's own method and score. Filter on source before joining on source_player_id. Seasons are the calendar year.
:::

### Returns {#load_wnba_player_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wnba"). |
| `athlete_id` | String | ESPN athlete id the provider id points to, as a string (e.g. "4433134"). Join on it to reach ESPN player data; the ESPN player loaders (load_wnba_player_core and its siblings) carry it as Int32, so cast first with pl.col("athlete_id").cast(pl.Int32). |
| `athlete_id_source` | String | Id space of athlete_id; always "espn" in this table. |
| `source` | String | Provider whose player id this is (in this table: fox, wnba_stats, yahoo). |
| `source_player_id` | String | The source's own player id (e.g. Yahoo wnba.p.5010, Fox 76284). It means something only within its source, so filter on source first. |
| `team_id` | String | ESPN team id (a string) the athlete was on in these seasons: the team block the match was made in, an ESPN team-season of load_wnba_team_group_seasons. |
| `source_team_id` | String | The source's own team id when the matching leg gives one (the season's stats.ncaa.org team id; the Fox / Yahoo / CBS team for the NFL); null otherwise. |
| `valid_from` | Int32 | First season the id is on that team, inclusive (calendar year). |
| `valid_to` | Int32 | Last season the id is on that team, inclusive (calendar year). A gap or a change of match method splits the window into two rows. |
| `match_method` | String | The matching leg's own method (exact_name, fuzzy_jw, the NCAA tiers name_jersey / name / last_jersey / jaro_winkler, CFB rosters name); crosswalk for a leg that names none; propagated when a pair matched in another season was carried to this provider id; override for a hand-verified override. |
| `match_confidence` | Float64 | The leg's own match confidence where it gives one (the lowest in the window); the NCAA tiers score 1.0 / 0.95 / 0.9 / 0.85; null otherwise. |
| `evidence` | String | The release asset or sportsdataverse/sdv-reference-data file the row came from. |

```python
load_wnba_player_aliases()
```
