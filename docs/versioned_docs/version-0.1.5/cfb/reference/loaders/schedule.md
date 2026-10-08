---
title: "CFB dataset loaders — Schedule"
sidebar_label: "Schedule"
sidebar_position: 8
description: "CFB dataset loaders — Schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Schedule

## load_cfb_schedule

Release: [cfb_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_schedules/cfb_schedules_{season}.parquet`
### Returns {#load_cfb_schedule-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, and the primary key: exactly one row per game. Every CFB surface in this package keys on the same id -- pbp, box scores, rosters, the crosswalks -- so this column joins them all. Pinned to Int64 on every build path so a join never fails on a dtype mismatch. |
| `season` | Int64 | Season year the game belongs to, matching the season argument you asked for. Bowl and playoff games played in January carry the PRIOR calendar year here, so group on this rather than on the year inside start_date. |
| `week` | Int64 | Week number within the season as ESPN numbers it, restarting at 1 for the postseason. Pair it with season_type before using it as a sort or join key -- week 1 is ambiguous on its own. |
| `season_type` | String | ESPN's season-type label, snake_cased: "regular", "postseason", "offseason" (all-star games), and 2020's COVID-only "spring_regular" / "spring_postseason". Always agrees with season_type_id; use whichever reads better in your code. |
| `season_type_id` | Int64 | ESPN's season-type integer, the canonical form ESPN publishes at /seasons/{year}/types: 1 preseason, 2 regular, 3 postseason, 4 offseason, 5 spring regular and 6 spring postseason (2020 only). A strict 1:1 partner to season_type, and the value to filter on when joining ESPN endpoints that take a numeric season type. |
| `start_date` | String | Scheduled kickoff instant from ESPN, ISO-8601 in UTC -- not local time, so a Saturday-night kickoff on the West Coast lands on the Sunday date. Parse it before comparing; sorting the raw string works only within one season. |
| `start_time_tbd` | Boolean | True while ESPN has announced the date but not the kickoff time, which is normal for games more than a few weeks out. When true, treat the time portion of start_date as a placeholder rather than a real kickoff. |
| `completed` | Boolean | True once the game has been played to a final. False covers both a future game and one that was cancelled or postponed -- read status to tell those apart. Filter on this before aggregating points or winners. |
| `neutral_site` | Boolean | True when neither team was hosting -- bowls, playoff games, and neutral kickoff-weekend matchups. The home/away labels still populate, so use this flag rather than assuming the home team had home-field advantage. |
| `conference_game` | Boolean | True when the two teams share a conference, derived from their conference membership for the season. Prefer it over comparing home_conference to away_conference, which mislabels independents and conference realignment. |
| `conference_competition` | Boolean | ESPN's own flag on the competition record for whether the game counts as a conference matchup. Kept alongside conference_game because the two measurably disagree on a handful of games each season -- membership says one thing, the game record another. Null for games ESPN's native feed does not carry. |
| `attendance` | Int64 | Announced attendance for the game as reported to ESPN. Null for games with no announced figure and for closed-door 2020 games; a null is unknown, not zero, so exclude it rather than filling it. |
| `venue_id` | Int64 | ESPN's identifier for the stadium hosting the game. Stable across seasons and across renames, so it is the join key for venue metadata in cfb_team_info. Null for games with no recorded venue. |
| `venue` | String | Stadium the game was played in, as ESPN spells it that season. Sponsor renames change this string between seasons for the same building -- join on venue_id instead of on this text. |
| `status` | String | ESPN's game-status enum ("STATUS_FINAL", "STATUS_SCHEDULED", "STATUS_POSTPONED", "STATUS_CANCELED", ...). This is what separates a scheduled-then-cancelled game from an unplayed future one -- the 121 COVID postponements and cancellations in 2020 are found here, not through completed. Null for games ESPN's native feed does not carry. |
| `home_id` | Int64 | ESPN team id of the home team. The join key to every other team-level CFB table in this package; join on it rather than on home_team, which is a display string. |
| `home_team` | String | School name of the home team as ESPN spells it. Display text only -- spellings drift across seasons and endpoints, so join on home_id. |
| `home_abbreviation` | String | Short ESPN abbreviation for the home team ("OSU", "MICH"), the form that fits a scoreboard or a chart axis. Abbreviations are not unique across all of college football, so never join on this. Null for games ESPN's native feed does not carry. |
| `home_division` | String | Home team's NCAA classification as ESPN records it: "fbs", "fcs", "ii", "iii", or null when ESPN publishes no classification for that team. Null means unclassified, not confirmed non-FBS -- the derived flags treat it conservatively as not-FBS. Filter with fbs_game / fbs_participant rather than comparing this column, because a null comparison yields null in polars and silently drops rows. |
| `home_conference` | String | Conference the home team played in that season, as ESPN records it. It follows realignment, so the same school carries different values across seasons -- exactly what you want for a season-by-season breakdown. |
| `home_points` | Int64 | Final points scored by the home team. Null until the game is played, so filter on completed before summing or differencing scores. |
| `home_winner` | Boolean | True when the home team won. Taken from ESPN's winner flag where it is set and otherwise derived from the final score, so it is populated for the full history rather than only recent seasons. Null when the game is not completed or carries no score -- a tie is not representable here. |
| `away_id` | Int64 | ESPN team id of the away team. Same role as home_id: the join key to every other team-level CFB table. |
| `away_team` | String | School name of the away team as ESPN spells it. Display text only -- join on away_id. |
| `away_abbreviation` | String | Short ESPN abbreviation for the away team; see home_abbreviation for the display-only caveat. |
| `away_division` | String | Away team's NCAA classification, same vocabulary and same unclassified-null caveat as home_division. This is the column that is null most often, because FBS schools schedule opponents ESPN leaves unclassified. |
| `away_conference` | String | Conference the away team played in that season, as ESPN records it; follows realignment season by season. |
| `away_points` | Int64 | Final points scored by the away team. Null until the game is played. |
| `away_winner` | Boolean | True when the away team won; same ESPN-flag-then-derived-from-score handling and the same nulls as home_winner. |
| `fbs_game` | Boolean | True when BOTH teams are classified FBS -- the filter for an FBS-only schedule. An unclassified (null) division is treated conservatively as not-FBS, so the flag reads False rather than null and is directly usable as a mask without any fill_null. A game whose opponent ESPN never classified therefore reads False even if that opponent was in fact FBS. |
| `fbs_participant` | Boolean | True when AT LEAST ONE team is classified FBS -- the filter that keeps an FBS team's games against FCS and lower opponents. An unclassified (null) division contributes False, never null, so the flag is always a usable mask. |
| `highlights` | String | Link to ESPN's highlight package for the game, where one was published. Null for most games and for the whole early history. |
| `notes` | String | Free-text note ESPN attaches to the game -- typically the bowl or event name, occasionally a weather or relocation note. Unstructured; do not parse it for bowl identity, use playoff_bowl_name. |
| `playoff_competition` | String | Playoff competition the game belongs to (e.g. "cfp"). Null for every non-playoff game, which makes is_not_null() the playoff filter. |
| `playoff_format` | String | Playoff format in effect for the game (e.g. "four_team", "twelve_team_2025"). Lets you compare bracket eras without hard-coding season cutoffs. |
| `playoff_round` | String | Playoff round slug (e.g. "first_round", "quarterfinal", "semifinal", "championship") -- the machine-readable partner to playoff_round_name. |
| `playoff_round_name` | String | Playoff round as ESPN displays it (e.g. "Semifinal"). Snake_cased column name here; cfbfastR's flatten emitted it as playoff_roundName. |
| `playoff_bracket_slot` | String | Slot the game occupies in the bracket (e.g. "SF1", "FR4"), which is what lets you reconstruct the bracket tree rather than just list the games. |
| `playoff_home_seed` | Int64 | Seed the home team entered the playoff with. Null outside the playoff and for the seasons before seeding was published. |
| `playoff_away_seed` | Int64 | Seed the away team entered the playoff with; same nulls as playoff_home_seed. |
| `playoff_bowl_name` | String | Bowl hosting the playoff game (e.g. "Rose Bowl") -- the reliable way to attribute a playoff game to a bowl site, rather than parsing notes. |
| `home_rank` | Int64 | ESPN's displayed Top-25 rank (1-25) of the home team -- the AP poll until that season's CFP ranking (BCS standings through 2013) is released -- at kickoff for a completed game and as of the last raw capture for an unplayed one. Null when the team is unranked, for every season before 2004, and in games with no FBS team. |
| `away_rank` | Int64 | ESPN's displayed Top-25 rank (1-25) of the away team; same poll, timing and nulls as home_rank. |

```python
load_cfb_schedule(seasons=2024)
```

## load_cfb_schedule_crosswalk

Release: [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_schedule_crosswalk_{season}.parquet`
### Returns {#load_cfb_schedule_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `matchup_key` | String | Order-independent key for the game: the two normalized team names sorted alphabetically and joined with a pipe. |
| `espn_game_id` | Int64 | ESPN game id for the crosswalk row. |
| `fox_game_id` | String | Fox Sports game id for the same game. |
| `yahoo_game_id` | String | Yahoo Sports game id for the same game. |
| `yahoo_global_game_id` | String | Yahoo's cross-season global game key in ncaaf.g.<number> form, distinct from the date-encoded yahoo_game_id. |
| `home_team` | String | Home team name. |
| `away_team` | String | Away team name. |
| `espn_date` | String | Kickoff date as YYYY-MM-DD taken from ESPN's schedule, null on games that matched no ESPN row. |
| `fox_date` | String | Kickoff date as YYYY-MM-DD taken from the Fox Sports schedule, null on games that matched no Fox row. |
| `yahoo_date` | String | Kickoff date as YYYY-MM-DD, parsed from Yahoo's RFC-2822 start_time string. |
| `matched_sources` | String | Plus-joined provenance tag naming which of espn, fox, and yahoo actually supplied a row for this game. |

```python
load_cfb_schedule_crosswalk(seasons=2024)
```
