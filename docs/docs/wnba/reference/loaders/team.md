---
title: "WNBA dataset loaders — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "WNBA dataset loaders — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA dataset loaders — Team

## load_wnba_team_boxscore

Release: [espn_wnba_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_team_boxscores/team_box_{season}.parquet`
### Returns {#load_wnba_team_boxscore-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `team_id` | Int32 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `team_home_away` | String | Team home away. |
| `team_score` | Int32 | Team's score / final score. |
| `team_winner` | Boolean | TRUE if the team won this game. |
| `assists` | Int32 | Total assists. |
| `blocks` | Int32 | Total blocks. |
| `defensive_rebounds` | Int32 | Defensive rebounds. |
| `fast_break_points` | String | Fast-break points scored. |
| `field_goal_pct` | Float64 | Field goal percentage (0-1). |
| `field_goals_made` | Int32 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int32 | Field goal attempts (2-pt + 3-pt). |
| `flagrant_fouls` | Int32 | Total flagrant fouls. |
| `fouls` | Int32 | Personal fouls. |
| `free_throw_pct` | Float64 | Free throw percentage (0-1). |
| `free_throws_made` | Int32 | Free throws made. |
| `free_throws_attempted` | Int32 | Free throw attempts. |
| `largest_lead` | String | Largest lead during the game. |
| `lead_changes` | String | Lead changes. |
| `lead_percentage` | String | Share of game time the team spent in the lead, as reported by ESPN. |
| `offensive_rebounds` | Int32 | Offensive rebounds. |
| `points_in_paint` | String | Points scored in the paint. |
| `steals` | Int32 | Total steals. |
| `team_turnovers` | Int32 | Team turnovers (turnovers credited to the team rather than a player). |
| `technical_fouls` | Int32 | Total technical fouls. |
| `three_point_field_goal_pct` | Float64 | Three-point field goal percentage (0-1). |
| `three_point_field_goals_made` | Int32 | Three-point field goals made. |
| `three_point_field_goals_attempted` | Int32 | Three-point field goal attempts. |
| `total_rebounds` | Int32 | Total rebounds. |
| `total_technical_fouls` | Int32 | Total technical fouls (player + team). |
| `total_turnovers` | Int32 | Total turnovers (player + team). |
| `turnover_points` | String | Turnover points. |
| `turnovers` | Int32 | Total turnovers. |
| `opponent_team_id` | Int32 | Unique identifier for the opponent team. |
| `opponent_team_uid` | String | Opponent team uid. |
| `opponent_team_slug` | String | Opponent team slug. |
| `opponent_team_location` | String | Opponent team city / location. |
| `opponent_team_name` | String | Opponent team display name. |
| `opponent_team_abbreviation` | String | Opponent team abbreviation. |
| `opponent_team_display_name` | String | Opponent team full display name. |
| `opponent_team_short_display_name` | String | Opponent team short display name. |
| `opponent_team_color` | String | Opponent team primary color (hex). |
| `opponent_team_alternate_color` | String | Opponent team alternate color (hex). |
| `opponent_team_logo` | String | Opponent team logo URL. |
| `opponent_team_score` | Int32 | Opponent team's score. |

```python
load_wnba_team_boxscore(seasons=2024)
```

## load_wnba_team_season_stats

Release: [espn_wnba_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_team_season_stats/team_season_stats_{season}.parquet`

:::caution[Coverage]
Published seasons: every season from the floor except 2022 and 2023, which the release has no asset for; a request for one is skipped with a warning.
:::

### Returns {#load_wnba_team_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `category` | String | Category label. |
| `stat_label` | String |  |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String | Human-readable description of the statistic the row reports. |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wnba_team_season_stats(seasons=2024)
```

## load_wnba_team_crosswalk

Release: [wnba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_crosswalk/wnba_team_crosswalk_{season}.parquet`

:::caution[Coverage]
wnba_team_id is Int64, as WNBA Stats ships team_id, so the bridge joins without a cast.
:::

### Returns {#load_wnba_team_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `espn_team_id` | Int32 | ESPN team id (canonical key). |
| `espn_abbreviation` | String | ESPN abbreviation. |
| `espn_display_name` | String | ESPN display name (school + mascot). |
| `espn_short_name` | String | ESPN short name. |
| `espn_location` | String | ESPN school/location only. |
| `espn_mascot` | String | ESPN team mascot/nickname. |
| `wnba_team_id` | Int64 | WNBA Stats team id. |
| `wnba_team_tricode` | String | WNBA Stats tricode. |
| `wnba_team_name` | String | WNBA Stats team name. |
| `wnba_team_city` | String | WNBA Stats team city. |
| `wnba_team_slug` | String | WNBA Stats team slug. |
| `fox_team_id` | String | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | String | Fox team name (NA if unmatched). |
| `yahoo_team_id` | String | Dotted Yahoo Sports team id (e.g. 'wnba.t.4'), filled when built with yahoo=True from Yahoo's team listing joined on full name, short name, then nickname; null otherwise and where no Yahoo team matched. |
| `yahoo_team_abbreviation` | String | Yahoo Sports team abbreviation for yahoo_team_id; null when yahoo_team_id is. |
| `yahoo_team_name` | String | Yahoo Sports full team name for yahoo_team_id; null when yahoo_team_id is. |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | Float64 | Jaro-Winkler score or 1 for exact (NA if none). |

```python
load_wnba_team_crosswalk(seasons=2026)
```

## load_wnba_team_group_seasons

Release: [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_team_group_seasons_{season}.parquet`

:::caution[Coverage]
One row per team per season: the SDV subdivision, conference and division group ids the team belonged to that season (null where a level does not apply), the team name as of that season, where the membership came from, and whether a second source agreed (null when only one source covers the season). team_id is a string: the ESPN team id; team_id_source names the id space. season is the calendar year; seasons 1997-2026.
:::

### Returns {#load_wnba_team_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wnba"); the prefix of every group_id in it. |
| `season` | Int32 | Season of the membership (calendar year). |
| `team_id` | String | Team id as a string: the ESPN team id where ESPN covers the team, otherwise the league's own id; team_id_source says which. |
| `team_id_source` | String | Id space of team_id (in this table: espn). |
| `team_name` | String | Team name as of that season, not today's. |
| `subdivision_id` | String | SDV group_id of the team's subdivision that season (e.g. FBS / FCS, Division I); null where the league has no subdivision level. |
| `conference_id` | String | SDV group_id of the team's conference that season; null where the team had no conference (an independent, or a season played without conferences). |
| `division_id` | String | SDV group_id of the team's division that season; null where the level does not apply. |
| `source` | String | Source the membership was taken from -- the most reliable per-season source for that era. |
| `sources_agree` | Boolean | Whether a second source agreed on the membership; null when only one source covers the season. |
| `notes` | String | Builder notes on the team-season, such as a source disagreement or which of several listed memberships was kept. |

```python
load_wnba_team_group_seasons(seasons=2024)
```
