# MLB dataset loaders — NCAA (stats.ncaa.org)

> MLB dataset loaders — NCAA (stats.ncaa.org) — function reference in sdv-py, the SportsDataverse Python package.

## load_ncaa_baseball_pbp

Release: [ncaa_baseball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_pbp/ncaa_baseball_pbp_{season}.parquet`
### Returns {#load_ncaa_baseball_pbp-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `inning` | Int64 | Inning number. |
| `inning_top_bot` | String | Half-inning ("top" or "bot"). |
| `batting` | String | Whether the situation applies to batting stats. |
| `fielding` | String | Whether the situation applies to fielding stats. |
| `play_number` | Int64 |  |
| `score_away` | Int64 |  |
| `score_home` | Int64 |  |
| `batter` | String | MLBAM player id of the batter. |
| `play_type` | String | Play category the NCAA baseball parser classified from the play text: single, double, triple, home_run, strikeout, walk, hit_by_pitch, groundout, flyout, lineout, out, double_play, fielders_choice, reached_on_error, stolen_base, wild_pitch, passed_ball, runner_advance, substitution, other, or unknown when the clause could not be classified. |
| `hit_trajectory` | String | Batted-ball trajectory: one of ground, line, fly, pop, foul. |
| `fielded_position` | String | Free-text description of where/how the ball was fielded or the runner advanced, as written by the scorer. |
| `is_hit` | Boolean | Whether the plate appearance resulted in a hit. |
| `is_out` | Boolean | Whether the play recorded at least one out. |
| `strikeout_type` | String | How the strikeout ended: 'swinging' or 'looking'. |
| `is_sacrifice` | Boolean | Whether the play was scored as a sacrifice. |
| `sac_type` | String | Kind of sacrifice when one was scored: 'fly' or 'bunt'. |
| `is_double_play` | Boolean | Whether the play resulted in a double play. |
| `rbi` | Int64 | Runs batted in. |
| `count_balls` | Int64 | Ball count when the plate appearance resolved. |
| `count_strikes` | Int64 | Strike count when the plate appearance resolved. |
| `pitch_sequence` | String | Per-pitch result string for the plate appearance (e.g. 'BBKKS'), one character per pitch. |
| `error_position` | String | Fielding position credited with the error, as the feed labels it (e.g. 'ss', 'rf', 'c'). |
| `unearned` | Boolean | Whether the run(s) on the play were scored as unearned. |
| `runs_scored` | Int64 | Runs that scored on the play. |
| `scoring_runners` | List(String) | List of runner names who scored on the play. |
| `runners_advanced` | List(String) | List of 'runner->base' strings describing each runner's advance on the play. |
| `outs_on_play` | Int64 | Number of outs recorded on the play (0-3). |
| `is_scoring_play` | Boolean | Flag indicating that the play put points on the board (1 = scoring play, 0 = not). |
| `description` | String | Long-form description text. |
| `source` | String | Source. |
| `espn_game_id` | String |  |
| `game_key` | String | NCAA contest identifier for the game the row belongs to; the join key to the other ncaa_baseball_* tables. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_pbp(seasons=2023)
```

## load_ncaa_baseball_schedule

Release: [ncaa_baseball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_schedules/ncaa_baseball_schedule_{season}.parquet`
### Returns {#load_ncaa_baseball_schedule-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | String | Unique ESPN team identifier. |
| `team_name` | String | Team name. |
| `date` | String | Date in YYYY-MM-DD format. |
| `game_number` | Int64 | Game number within a doubleheader. |
| `opponent_id` | String | Unique identifier for opponent. |
| `opponent` | String |  |
| `result` | String | Win/loss/tie result for `team_id`. |
| `outcome` | String | Result for the team the row is keyed to: 'W', 'L' or 'T'. |
| `team_score` | Int64 | Team's score / final score. |
| `opponent_score` | Int64 | Opponent score. |
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `division` | Int64 | NCAA division (1, 2, 3). |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_schedule(seasons=2023)
```

## load_ncaa_baseball_teams

Release: [ncaa_baseball_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_teams) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_teams/ncaa_baseball_teams_{season}.parquet`
### Returns {#load_ncaa_baseball_teams-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | String | Unique ESPN team identifier. |
| `team_name` | String | Team name. |
| `division` | Int64 | NCAA division (1, 2, 3). |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_teams(seasons=2025)
```

## load_ncaa_baseball_rosters

Release: [ncaa_baseball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_rosters/ncaa_baseball_rosters_{season}.parquet`
### Returns {#load_ncaa_baseball_rosters-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | String | Unique ESPN team identifier. |
| `team_name` | String | Team name. |
| `player_id` | String | stats.ncaa.org player identifier. |
| `player_name` | String | Player name. |
| `jersey` | String | Jersey number worn by the player. |
| `statcrew_jersey` | String | StatCrew jersey number; present in the schema but entirely unpopulated in the published asset. |
| `player_class` | String | Class year as the feed reports it (Fr., So., Jr., Sr.); '---' when unreported. |
| `position` | String | Position the NCAA baseball feed reports for the player. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `hometown` | String |  |
| `high_school` | String |  |
| `games_played` | Int64 | Games played. |
| `games_started` | Int64 | Games started. |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_rosters(seasons=2025)
```

## load_ncaa_baseball_linescore

Release: [ncaa_baseball_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_linescore) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_linescore/ncaa_baseball_linescore_{season}.parquet`
### Returns {#load_ncaa_baseball_linescore-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `team` | String | Team. |
| `home_away` | String | Venue label for the team ('home' or 'away'). |
| `inning` | String | Inning number. |
| `runs` | Int64 | Runs scored. |
| `runs_total` | Int64 | Total runs scored by the team across the whole game. |
| `hits` | Int64 | Hits. |
| `errors` | Int64 | Fielding errors. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `venue` | String |  |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `source` | String | Source. |
| `espn_game_id` | String |  |
| `game_key` | String | NCAA contest identifier for the game the row belongs to; the join key to the other ncaa_baseball_* tables. |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_linescore(seasons=2025)
```

## load_ncaa_baseball_team_stats

Release: [ncaa_baseball_team_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_team_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_team_stats/ncaa_baseball_team_stats_{season}.parquet`
### Returns {#load_ncaa_baseball_team_stats-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `category` | String | Category label. |
| `stat` | String |  |
| `period` | String | Inning number. |
| `away_team` | String | Away team name. |
| `away_value` | String | Away team's value for the stat named by the row, as a string (the table is long/tidy, one stat per row). |
| `home_team` | String | Home team name. |
| `home_value` | String | Home team's value for the stat named by the row, as a string (the table is long/tidy, one stat per row). |
| `source` | String | Source. |
| `espn_game_id` | String |  |
| `game_key` | String | NCAA contest identifier for the game the row belongs to; the join key to the other ncaa_baseball_* tables. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_team_stats(seasons=2025)
```

## load_ncaa_baseball_player_stats

Release: [ncaa_baseball_player_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_player_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_player_stats/ncaa_baseball_player_stats_{season}.parquet`
### Returns {#load_ncaa_baseball_player_stats-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `team_id` | String | Unique ESPN team identifier. |
| `number` | String | Jersey number. |
| `name` | String | Display name. |
| `position` | String | Position the NCAA baseball feed reports for the player. |
| `r` | String | Runs scored. |
| `ab` | String | At-bats. |
| `h` | String | Hits. |
| `2b` | String | Doubles hit by the batter. |
| `3b` | String | Triples hit by the batter. |
| `tb` | String | Total bases accumulated by the batter. |
| `hr` | String | Home runs hit by the batter over the covered sample. |
| `rbi` | String | Runs batted in. |
| `bb` | String | Bases on balls (walks). |
| `hbp` | String | Times the batter was hit by a pitch. |
| `sf` | String | Sacrifice flies hit by the batter. |
| `sh` | String | Sacrifice hits (bunts) laid down by the batter. |
| `k` | String | Strikeouts. |
| `opp_dp` | String | Double plays turned by the opposing defense against this side. |
| `cs` | String | Times the baserunner was caught stealing. |
| `picked` | String | Times the baserunner was picked off. |
| `sb` | String | Stolen bases by the baserunner. |
| `ibb` | String | Intentional bases on balls drawn by the batter. |
| `kl` | String | Strikeouts looking -- called third strikes, as opposed to swinging strikeouts. |
| `category` | String | Category label. |
| `source` | String | Source. |
| `espn_game_id` | String |  |
| `game_key` | String | NCAA contest identifier for the game the row belongs to; the join key to the other ncaa_baseball_* tables. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `ip` | String | Innings pitched. |
| `er` | String | Earned runs. |
| `so` | String | Strikeouts: the batter's strikeouts on the batting line, the pitcher's strikeouts recorded on the pitching line. |
| `bf` | String | Batters faced by the pitcher. |
| `2b_a` | String | Doubles allowed by the pitcher. |
| `3b_a` | String | Triples allowed by the pitcher. |
| `bk` | String | Balks charged to the pitcher. |
| `hr_a` | String | Home runs allowed by the pitcher. |
| `wp` | String | Wild pitches charged to the pitcher. |
| `hb` | String | Batters hit by a pitch from this pitcher. |
| `inh_run` | String | Inherited runners on base when this relief pitcher entered. |
| `inh_run_score` | String | Inherited runners who subsequently scored. |
| `sha` | String | Sacrifice hits allowed by the pitcher. |
| `sfa` | String | Sacrifice flies allowed by the pitcher. |
| `tuer` | String | Team unearned runs scored while this pitcher was in the game. |
| `pickoffs` | String | Pickoffs. |
| `po` | String | Putouts recorded by the fielder. |
| `a` | String | Fielding assists credited to the player. |
| `tc` | String | Total chances for the fielder (putouts + assists + errors). |
| `e` | String | Errors charged to the fielder. |
| `ci` | String | Times the batter reached base on catcher's interference. |
| `pb` | String | Passed balls charged to the catcher. |
| `sba` | String | Stolen bases allowed while this catcher was behind the plate. |
| `csb` | String | Runners caught stealing while this catcher was behind the plate. |
| `idp` | String | Times the batter grounded into a double play. |
| `tp` | String | Triple plays the player took part in. |
| `sbapct` | String | Stolen-base success rate allowed by the catcher, as a PROPORTION on 0-1 (e.g. 0.625), not a percentage. |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_player_stats(seasons=2025)
```

## load_ncaa_baseball_situational_stats

Release: [ncaa_baseball_situational_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_situational_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_situational_stats/ncaa_baseball_situational_stats_{season}.parquet`
### Returns {#load_ncaa_baseball_situational_stats-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `team_seq` | Int64 | Side indicator for the row: 0 and 1 distinguish the two teams in the contest. |
| `player` | String |  |
| `position` | String | Position the NCAA baseball feed reports for the player. |
| `with_runrs` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- plate appearances with runners on base. |
| `hits_scorepos` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- at-bats with runners in scoring position. |
| `vs_lhp` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- batting against left-handed pitching. |
| `vs_rhp` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- batting against right-handed pitching. |
| `leadoff_pct` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- leadoff batters reaching base; despite the name it is a pair string, not a percentage. |
| `rbi3rd` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- chances to drive in a runner from third. |
| `h_pinchit` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- pinch-hitting appearances. |
| `adv_ops` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- advancing-opportunity situations. |
| `with_2_outs` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- plate appearances with two outs. |
| `with_runrs2` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- secondary runners-on split emitted by the feed. |
| `with_scorepos2` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- secondary runners-in-scoring-position split emitted by the feed. |
| `bases_empty` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- plate appearances with the bases empty. |
| `bases_loaded` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- plate appearances with the bases loaded. |
| `category` | String | Category label. |
| `source` | String | Source. |
| `espn_game_id` | String |  |
| `game_key` | String | NCAA contest identifier for the game the row belongs to; the join key to the other ncaa_baseball_* tables. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `location` | String | Team city/region (e.g. "Los Angeles"). |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `runners` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- plate appearances with runners on base. |
| `vs_lhb` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- pitching against left-handed batters. |
| `with_2outs` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- two-out split under the feed's second, unpunctuated key (distinct column from with_2_outs). |
| `emtpy` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- bases-empty split under the upstream feed's misspelled key ('emtpy', sic). |
| `with_scorepos` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- plate appearances with runners in scoring position. |
| `with_runners2` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- secondary runners-on split emitted by the feed alongside with_runrs2. |
| `vs_rhb` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- pitching against right-handed batters. |
| `field_pct` | String | Situational split as a 'successes-opportunities' pair string (e.g. '5-13'), not a numeric rate -- fielding chances handled cleanly; despite the name it is a pair string, not a fielding percentage. |
| `season` | Int64 | Season year. |

```python
load_ncaa_baseball_situational_stats(seasons=2025)
```

## load_ncaa_baseball_games

Release: [ncaa_baseball_games](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_games) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_games/ncaa_baseball_games_{season}.parquet`
### Returns {#load_ncaa_baseball_games-returns}

| col_name | type | description |
|---|---|---|
| `game_key` | String | NCAA contest identifier for the game the row belongs to; the join key to the other ncaa_baseball_* tables. |
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `game_pbp_id` | Int64 | stats.ncaa.org play-by-play (contest) identifier. |
| `season` | Int64 | Season year. |
| `source` | String | Source. |
| `espn_game_id` | String |  |
| `away_team` | String | Away team name. |
| `away_final` | Int64 | Final runs scored by the away team. |
| `home_team` | String | Home team name. |
| `home_final` | Int64 | Final runs scored by the home team. |

```python
load_ncaa_baseball_games(seasons=2024)
```

## load_ncaa_baseball_groups

Release: [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_groups/ncaa_baseball_groups.parquet`

:::caution[Coverage]
One season-less file: one row per group lineage (the league, subdivisions, conferences, divisions) with the first and last season it had members. group_id is SDV's own id (e.g. ncaa_baseball:acc) and names a lineage: a rename that keeps continuity keeps the id, a new body gets a new one, and notes records each call. Seasons are the calendar year.
:::

### Returns {#load_ncaa_baseball_groups-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("ncaa_baseball"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `first_season` | Int32 | First season in which the group had at least one member (calendar year). |
| `last_season` | Int32 | Last season in which the group had at least one member (calendar year). |
| `notes` | String | Builder notes on the group: the lineage decisions behind its group_id and any source caveats. |

```python
load_ncaa_baseball_groups()
```

## load_ncaa_baseball_group_seasons

Release: [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_groups/ncaa_baseball_group_seasons.parquet`

:::caution[Coverage]
One season-less file: one row per group per season it existed, with its name, short name, abbreviation and parent group AS OF that season (never today's label applied to the past) and its member count. season is the calendar year.
:::

### Returns {#load_ncaa_baseball_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("ncaa_baseball"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `season` | Int32 | Season the row describes (calendar year). |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `name` | String | Full name of the group as of that season -- the label in use then, not today's name. |
| `short_name` | String | Short display name of the group as of that season. |
| `abbreviation` | String | Abbreviation of the group as of that season. |
| `parent_group_id` | String | group_id one level up as of that season (division -> conference -> subdivision -> league); null at the top level or where no higher group applied that season. |
| `n_teams` | Int32 | Number of member teams in the group that season. |

```python
load_ncaa_baseball_group_seasons()
```

## load_ncaa_baseball_group_aliases

Release: [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_groups/ncaa_baseball_group_aliases.parquet`

:::caution[Coverage]
One season-less file: every name, abbreviation, slug and source id that a source (ncaa, sdv) uses for a group, each with the seasons it is valid for (valid_from / valid_to, inclusive; null = unbounded). Match a source's conference or division label here to reach group_id.
:::

### Returns {#load_ncaa_baseball_group_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("ncaa_baseball"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `source` | String | Source that uses this label or id (in this table: ncaa, sdv); "sdv" marks SDV's own labels. |
| `source_id` | String | The source's own id for the group (ESPN group id, NCAA conf_id, CFBD id, MLB division id) when it has one; null otherwise. |
| `name_kind` | String | Kind of label in value: "name", "short_name", "abbreviation", "slug" or "code". |
| `value` | String | The label exactly as the source writes it; match a source's conference or division label against it to reach group_id. |
| `valid_from` | Int32 | First season the alias is valid for, inclusive (calendar year); null = unbounded. |
| `valid_to` | Int32 | Last season the alias is valid for, inclusive (calendar year); null = unbounded (still in use). |

```python
load_ncaa_baseball_group_aliases()
```

## load_ncaa_baseball_team_group_seasons

Release: [ncaa_baseball_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_baseball_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_baseball_groups/ncaa_baseball_team_group_seasons_{season}.parquet`

:::caution[Coverage]
One row per team per season: the SDV subdivision, conference and division group ids the team belonged to that season (null where a level does not apply), the team name as of that season, where the membership came from, and whether a second source agreed (null when only one source covers the season). team_id is a string: the stats.ncaa.org org id (not an ESPN id); team_id_source names the id space. season is the calendar year; seasons 2010-2026.
:::

### Returns {#load_ncaa_baseball_team_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("ncaa_baseball"); the prefix of every group_id in it. |
| `season` | Int32 | Season of the membership (calendar year). |
| `team_id` | String | Team id as a string: the ESPN team id where ESPN covers the team, otherwise the league's own id; team_id_source says which. |
| `team_id_source` | String | Id space of team_id (in this table: ncaa_org). |
| `team_name` | String | Team name as of that season, not today's. |
| `subdivision_id` | String | SDV group_id of the team's subdivision that season (e.g. FBS / FCS, Division I); null where the league has no subdivision level. |
| `conference_id` | String | SDV group_id of the team's conference that season; null where the team had no conference (an independent, or a season played without conferences). |
| `division_id` | String | SDV group_id of the team's division that season; null where the level does not apply. |
| `source` | String | Source the membership was taken from -- the most reliable per-season source for that era. |
| `sources_agree` | Boolean | Whether a second source agreed on the membership; null when only one source covers the season. |
| `notes` | String | Builder notes on the team-season, such as a source disagreement or which of several listed memberships was kept. |

```python
load_ncaa_baseball_team_group_seasons(seasons=2024)
```
