---
title: "NBA dataset loaders — Player"
sidebar_label: "Player"
sidebar_position: 1
description: "NBA dataset loaders — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA dataset loaders — Player

## load_nba_player_boxscore

Release: [espn_nba_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_player_boxscores/player_box_{season}.parquet`
### Returns {#load_nba_player_boxscore-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season year. |
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
load_nba_player_boxscore(seasons=2024)
```

## load_nba_player_season_stats

Release: [espn_nba_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_player_season_stats/player_season_stats_{season}.parquet`
### Returns {#load_nba_player_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season year. |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `athlete_jersey` | String | Athlete jersey number. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_display_name` | String | Full team display name. |
| `category` | String | Category label. |
| `stat_label` | String |  |
| `stat_name` | String | Stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String | ESPN's prose definition of the statistic on this row, for example the ratio of field goals made to field goals attempted; for the paired Made-Attempted stats it is the two definitions joined with a hyphen. |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_nba_player_season_stats(seasons=2025)
```

## load_nba_player_crosswalk

Release: [nba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_crosswalk/nba_player_crosswalk_{season}.parquet`
### Returns {#load_nba_player_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season year. |
| `espn_team_id` | Int32 | ESPN team id (canonical key). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_name` | String | Player name. |
| `espn_athlete_id` | Int32 | ESPN athlete id. |
| `espn_full_name` | String | ESPN full name. |
| `espn_jersey` | String | ESPN jersey number. |
| `espn_position` | String | ESPN position abbreviation. |
| `nba_player_id` | String | NBA Stats player id side of the ESPN-to-NBA player crosswalk. |
| `nba_player_name` | String | Player name as listed by the NBA Stats API. |
| `nba_jersey_num` | String | Player's jersey number as listed by the NBA Stats API. |
| `nba_position` | String | Player's position as listed by the NBA Stats API. |
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
load_nba_player_crosswalk(seasons=2026)
```

## load_nba_player_core

Release: [espn_nba_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_core) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_player_core/player_core_{season}.parquet`
### Returns {#load_nba_player_core-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season year. |
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
load_nba_player_core(seasons=2025)
```

## load_nba_player_impact

Release: [nba_player_impact](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_player_impact) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_player_impact/nba_player_impact_{season}.parquet`

:::caution[Coverage]
The BOBAN (Bayesian Online Box-Augmented Net) columns boban_filtered_skill, boban_projected_rating and boban_projected_sd were renamed from darko_*; the loader returns both names for one release, so move to boban_* now (darko_* is deprecated and will be removed).
:::

### Returns {#load_nba_player_impact-returns}

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | String | Nested list of member-team membership spans. |
| `season` | Int64 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `o_rapm` | Float64 | Offensive regularized adjusted plus-minus per 100 possessions from the single-season ridge fit over possession-level lineup indicators, at the fixed NBA penalty described under rapm; positive means the player raised his team's scoring rate while on offense. |
| `d_rapm` | Float64 | Defensive regularized adjusted plus-minus per 100 possessions, negated from the raw points-allowed coefficient so a positive value marks a defender who suppresses opponent scoring; fit at the fixed NBA penalty described under rapm. |
| `rapm` | Float64 | Total regularized adjusted plus-minus per 100 possessions, exactly the sum of o_rapm and d_rapm. The ridge penalty is one fixed NBA value (2,683, sportsdataverse.nba.nba_rapm.RAPM_PENALTY) in every season and season type, so the scale compares across seasons (regular-season sd 1.53 to 1.99 per 100 over players with 400 possessions, 1997-2026; a shorter season, such as the 1999 lockout (1.53), shrinks harder, so part of a spread difference is sample size). Seasons published before November 2026 were fit at a per-season RidgeCV pick (1,931 or 5,179 in the regular season, up to 37,276 in the playoffs), and their spread jumps with it (sd 1.15 to 2.34): do not compare those values across seasons or with values fit at the fixed penalty. |
| `off_poss` | Int64 | Number of possessions the player was on the floor on offense, the count of design-matrix rows carrying his offensive indicator and therefore the offensive-side sample size behind o_rapm. |
| `def_poss` | Int64 | Number of possessions the player was on the floor on defense, the sample size behind d_rapm; it tracks off_poss almost exactly because substitutions rarely split an offense-defense pair. |
| `o_adj_rapm` | Float64 | Offensive prior-informed RAPM per 100 possessions: the ridge fit shrunk toward the player's box-score (SPM) prior. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025-26 +0.32). A player with no prior (a rookie, or no previous-season SPM) is shrunk toward 0 instead. Releases fit before #1137, whatever their build date, fit no intercept, so the league's points per possession leaked into every coefficient: o_adj_rapm averaged about +12.03 and d_adj_rapm -10.84 (2025-26), a thinly-used player kept less of that offset, and only their sum, adj_rapm, read on a common scale. |
| `d_adj_rapm` | Float64 | Defensive prior-informed RAPM per 100 possessions, positive for a defender who lowers opponent scoring. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025-26 +0.06). Releases fit before #1137, whatever their build date, averaged about -10.84 here (see o_adj_rapm). |
| `adj_rapm` | Float64 | Total prior-informed RAPM per 100 possessions, exactly the sum of o_adj_rapm and d_adj_rapm. Releases fit with sportsdataverse-py #1137 or later fit the ridge with an unpenalized intercept; releases fit before it (whatever their build date) fit none, which leaned this total toward heavy-minute players. |
| `ospm` | Float64 | Offensive statistical plus-minus per 100 possessions: the player's per-100 box-score feature vector scored through ridge coefficients trained on that season's o_rapm target. |
| `dspm` | Float64 | Defensive statistical plus-minus per 100 possessions from the same box-score feature vector scored through coefficients trained on the d_rapm target. |
| `spm` | Float64 | Total statistical plus-minus per 100 possessions, exactly the sum of ospm and dspm. |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |
| `obpm` | Float64 | Offensive box plus-minus per 100 possessions for this season and season type only, team-adjusted on its own so a team's minute-weighted obpm is its offensive rating minus the league's, divided by 5 (league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 put the whole team constant here (league mean about -2.9). |
| `dbpm` | Float64 | Defensive box plus-minus per 100 possessions for this season and season type only: bpm minus obpm, so it lands on the team's defensive rating relative to the league (league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 left it unadjusted (league mean about +2.9, nearly every player positive). |
| `bpm` | Float64 | Box plus-minus (BPM 2.0) per 100 possessions for this season and season type only, not a career value: the box-score estimate of total impact. The team adjustment makes a team's minute-weighted bpm its net rating per 100 divided by 5 (the five %Min slots sum to the net rating). Releases fit before sportsdataverse-py #1148 used the full net rating, five times too large. |
| `war` | Float64 | Wins above replacement, computed as (rapm minus a replacement level of -2.0 per 100) times total possessions divided by 100, divided by a points-per-win constant calibrated each season by regressing team wins on full-season point margin. |
| `boban_filtered_skill` | Float64 | BOBAN Kalman-filtered skill estimate at the end of the player's observed multi-season RAPM panel, i.e. his current-form rating after aging drift and possession-weighted observation noise. |
| `boban_projected_rating` | Float64 | One-season-ahead BOBAN forecast, the filtered skill plus the empirical aging-curve drift at the player's last observed age; both season_type rows of a player-season carry the same value because the projection is not playoff-specific. |
| `boban_projected_sd` | Float64 | Standard deviation of the one-season-ahead BOBAN forecast, the square root of the filtered state variance plus the Kalman process variance; it sits at roughly 10.19 for a player with only one season in the panel, whose diffuse prior variance was never updated. |
| `darko_filtered_skill` | Float64 | Deprecated alias of boban_filtered_skill, kept for one release; use that column. |
| `darko_projected_rating` | Float64 | Deprecated alias of boban_projected_rating, kept for one release; use that column. |
| `darko_projected_sd` | Float64 | Deprecated alias of boban_projected_sd, kept for one release; use that column. |
| `o_rapm_n` | Int64 | Possessions o_rapm is measured over: off_poss, his offensive possessions. o_rapm_rank and o_rapm_pct are set only when it reaches 1,700, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_rapm_n` | Int64 | Possessions d_rapm is measured over: def_poss, his defensive possessions. d_rapm_rank and d_rapm_pct are set only when it reaches 1,600, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `rapm_n` | Int64 | Possessions rapm is measured over: off_poss + def_poss, his possessions at both ends. rapm_rank and rapm_pct are set only when it reaches 3,200, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `o_adj_rapm_n` | Int64 | Possessions o_adj_rapm is measured over: off_poss, his offensive possessions. o_adj_rapm_rank and o_adj_rapm_pct are set only when it reaches 1,700, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_adj_rapm_n` | Int64 | Possessions d_adj_rapm is measured over: def_poss, his defensive possessions. d_adj_rapm_rank and d_adj_rapm_pct are set only when it reaches 1,600, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `adj_rapm_n` | Int64 | Possessions adj_rapm is measured over: off_poss + def_poss, his possessions at both ends. adj_rapm_rank and adj_rapm_pct are set only when it reaches 3,200, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `ospm_n` | Int64 | Possessions ospm is measured over: off_poss, his offensive possessions. ospm_rank and ospm_pct are set only when it reaches 1,700, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dspm_n` | Int64 | Possessions dspm is measured over: def_poss, his defensive possessions. dspm_rank and dspm_pct are set only when it reaches 1,600, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `spm_n` | Int64 | Possessions spm is measured over: off_poss + def_poss, his possessions at both ends. spm_rank and spm_pct are set only when it reaches 3,200, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `obpm_n` | Int64 | Possessions obpm is measured over: off_poss, his offensive possessions. obpm_rank and obpm_pct are set only when it reaches 1,700, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dbpm_n` | Int64 | Possessions dbpm is measured over: def_poss, his defensive possessions. dbpm_rank and dbpm_pct are set only when it reaches 1,600, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `bpm_n` | Int64 | Possessions bpm is measured over: off_poss + def_poss, his possessions at both ends. bpm_rank and bpm_pct are set only when it reaches 3,200, the NBA sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `war_n` | Int64 | Possessions behind war: off_poss + def_poss. war is a count, not a rate, so its rank has no sample gate. |
| `o_rapm_rank` | Float64 | Rank of o_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_rapm_n is under 1,700, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_rapm_rank` | Float64 | Rank of d_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_rapm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `rapm_rank` | Float64 | Rank of rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when rapm_n is under 3,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `o_adj_rapm_rank` | Float64 | Rank of o_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_adj_rapm_n is under 1,700, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_adj_rapm_rank` | Float64 | Rank of d_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_adj_rapm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `adj_rapm_rank` | Float64 | Rank of adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when adj_rapm_n is under 3,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `ospm_rank` | Float64 | Rank of ospm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when ospm_n is under 1,700, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dspm_rank` | Float64 | Rank of dspm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dspm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `spm_rank` | Float64 | Rank of spm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when spm_n is under 3,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `obpm_rank` | Float64 | Rank of obpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when obpm_n is under 1,700, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dbpm_rank` | Float64 | Rank of dbpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dbpm_n is under 1,600, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `bpm_rank` | Float64 | Rank of bpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when bpm_n is under 3,200, and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `war_rank` | Float64 | Rank of war among the season and season type's ranked players, 1 = best (higher is better), ties averaged; ungated (every player with a finite war qualifies), null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. |
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

```python
load_nba_player_impact(seasons=2024)
```

## load_nba_player_aliases

Release: [nba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_groups/nba_player_aliases.parquet`

:::caution[Coverage]
One season-less file: every player id another source (fox, nba_stats, yahoo) uses for an ESPN athlete (athlete_id, a string; athlete_id_source = espn), with the ESPN team the match was made on (team_id) and the seasons it held (valid_from / valid_to, inclusive). match_method and match_confidence carry the matching leg's own method and score. Filter on source before joining on source_player_id. Seasons are the ENDING year (2025 = the 2024-25 season).
:::

### Returns {#load_nba_player_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("nba"). |
| `athlete_id` | String | ESPN athlete id the provider id points to, as a string (e.g. "4433134"). Join on it to reach ESPN player data; the ESPN player loaders (load_nba_player_core and its siblings) carry it as Int32, so cast first with pl.col("athlete_id").cast(pl.Int32). |
| `athlete_id_source` | String | Id space of athlete_id; always "espn" in this table. |
| `source` | String | Provider whose player id this is (in this table: fox, nba_stats, yahoo). |
| `source_player_id` | String | The source's own player id (e.g. Yahoo nba.p.5637, Fox 76284). It means something only within its source, so filter on source first. |
| `team_id` | String | ESPN team id (a string) the athlete was on in these seasons: the team block the match was made in, an ESPN team-season of load_nba_team_group_seasons. |
| `source_team_id` | String | The source's own team id when the matching leg gives one (the season's stats.ncaa.org team id; the Fox / Yahoo / CBS team for the NFL); null otherwise. |
| `valid_from` | Int32 | First season the id is on that team, inclusive (ENDING year: 2025 = the 2024-25 season). |
| `valid_to` | Int32 | Last season the id is on that team, inclusive (ENDING year: 2025 = the 2024-25 season). A gap or a change of match method splits the window into two rows. |
| `match_method` | String | The matching leg's own method (exact_name, fuzzy_jw, the NCAA tiers name_jersey / name / last_jersey / jaro_winkler, CFB rosters name); crosswalk for a leg that names none; propagated when a pair matched in another season was carried to this provider id; override for a hand-verified override. |
| `match_confidence` | Float64 | The leg's own match confidence where it gives one (the lowest in the window); the NCAA tiers score 1.0 / 0.95 / 0.9 / 0.85; null otherwise. |
| `evidence` | String | The release asset or sportsdataverse/sdv-reference-data file the row came from. |

```python
load_nba_player_aliases()
```
