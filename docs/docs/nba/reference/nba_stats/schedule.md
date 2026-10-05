---
title: "NBA — NBA Stats API (stats.nba.com) — Schedule"
sidebar_label: "Schedule"
sidebar_position: 18
description: "NBA — NBA Stats API (stats.nba.com) — Schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Schedule

## nba_stats_scheduleleaguev2

GET /stats/scheduleleaguev2

**Endpoint URL:** `GET https://stats.nba.com/stats/scheduleleaguev2`

**Valid URL:** [https://stats.nba.com/stats/scheduleleaguev2?LeagueID=00](https://stats.nba.com/stats/scheduleleaguev2?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#nba_stats_scheduleleaguev2-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `arena_city` | character | City hosting the game's arena. |
| `arena_name` | character | Name of the arena hosting the game. |
| `arena_state` | character | State or province of the game's arena (blank for international sites). |
| `away_team_city` | character | City of the away team. |
| `away_team_id` | integer | stats.nba.com / stats.wnba.com team id of the away team. |
| `away_team_losses` | integer | Away team's losses entering the game. |
| `away_team_name` | character | Away team nickname (e.g. Fever). |
| `away_team_score` | integer | Away team's final score, 0 before the game is played. |
| `away_team_seed` | integer | Away team's playoff seed, 0 outside the postseason. |
| `away_team_slug` | character | URL slug of the away team. |
| `away_team_time` | character | Scheduled tip-off in the away team's local time zone. |
| `away_team_tricode` | character | Three-letter abbreviation of the away team. |
| `away_team_wins` | integer | Away team's wins entering the game. |
| `branch_link` | character | Deep link for the game, blank when not published. |
| `day` | character | Three-letter day of week of the game date. |
| `game_code` | character | Provider game code, `YYYYMMDD/AWYHOM`. |
| `game_date` | character | Game date as served by the schedule feed (`MM/DD/YYYY HH:MM:SS`). |
| `game_date_est` | character | Game date at midnight Eastern, ISO-8601. |
| `game_date_time_est` | character | Scheduled tip-off in Eastern time, ISO-8601. |
| `game_date_time_utc` | character | Scheduled tip-off in UTC, ISO-8601. The timestamp to reduce to a calendar date. |
| `game_date_utc` | character | Game date at midnight UTC, ISO-8601. |
| `game_id` | character | Unique stats.nba.com / stats.wnba.com game id; its 3rd character encodes the season type. |
| `game_label` | character | Human-readable round or event label (e.g. Preseason, Conf. Finals). |
| `game_sequence` | integer | Ordinal of the game within its date. |
| `game_status` | integer | Game status code: 1 scheduled, 2 in progress, 3 final. |
| `game_status_text` | character | Human-readable game status (e.g. Final, 7:00 pm ET). |
| `game_sub_label` | character | Secondary event label (e.g. NBA Abu Dhabi Game). |
| `game_subtype` | character | Game subtype tag (e.g. Global Games), blank for standard games. |
| `game_time_est` | character | Scheduled tip-off time of day, Eastern. |
| `game_time_utc` | character | Scheduled tip-off time of day, UTC. |
| `home_team_city` | character | City of the home team. |
| `home_team_id` | integer | stats.nba.com / stats.wnba.com team id of the home team. |
| `home_team_losses` | integer | Home team's losses entering the game. |
| `home_team_name` | character | Home team nickname (e.g. Liberty). |
| `home_team_score` | integer | Home team's final score, 0 before the game is played. |
| `home_team_seed` | integer | Home team's playoff seed, 0 outside the postseason. |
| `home_team_slug` | character | URL slug of the home team. |
| `home_team_time` | character | Scheduled tip-off in the home team's local time zone. |
| `home_team_tricode` | character | Three-letter abbreviation of the home team. |
| `home_team_wins` | integer | Home team's wins entering the game. |
| `if_necessary` | character | Whether the game is a conditional series game that may not be played. |
| `is_neutral` | logical | Whether the game is played at a neutral site. |
| `league_id` | character | League id of the schedule: '00' NBA, '10' WNBA, '20' G-League. |
| `month_num` | integer | Calendar month number of the game date. |
| `postponed_status` | character | Postponement status; 'N' when the game is on as scheduled. |
| `season` | character | Season the schedule covers, as published by the feed ('2025-26' for the NBA, '2026' for the WNBA). |
| `season_type_description` | character | Season type label derived from season_type_id: Pre-Season, Regular Season, All-Star, Playoffs, Play-In Game. |
| `season_type_id` | character | Season type digit, the 3rd character of game_id. |
| `series_game_number` | character | Game number within a playoff series, blank outside a series. |
| `series_text` | character | Series context line (e.g. series tied 1-1), blank when not applicable. |
| `week_name` | character | Name of the schedule week, blank outside the regular season. |
| `week_number` | integer | Schedule week number, 0 outside the regular season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_scheduleleaguev2-example}

```python
nba_stats_scheduleleaguev2(league_id='00')
```

_Last validated n/a._

## nba_stats_scheduleleaguev2int

GET /stats/scheduleleaguev2int

**Endpoint URL:** `GET https://stats.nba.com/stats/scheduleleaguev2int`

**Valid URL:** [https://stats.nba.com/stats/scheduleleaguev2int?LeagueID=00](https://stats.nba.com/stats/scheduleleaguev2int?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#nba_stats_scheduleleaguev2int-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `arena_city` | character | City hosting the game's arena. |
| `arena_name` | character | Name of the arena hosting the game. |
| `arena_state` | character | State or province of the game's arena (blank for international sites). |
| `away_team_city` | character | City of the away team. |
| `away_team_id` | integer | stats.nba.com / stats.wnba.com team id of the away team. |
| `away_team_losses` | integer | Away team's losses entering the game. |
| `away_team_name` | character | Away team nickname (e.g. Fever). |
| `away_team_score` | integer | Away team's final score, 0 before the game is played. |
| `away_team_seed` | integer | Away team's playoff seed, 0 outside the postseason. |
| `away_team_slug` | character | URL slug of the away team. |
| `away_team_time` | character | Scheduled tip-off in the away team's local time zone. |
| `away_team_tricode` | character | Three-letter abbreviation of the away team. |
| `away_team_wins` | integer | Away team's wins entering the game. |
| `branch_link` | character | Deep link for the game, blank when not published. |
| `day` | character | Three-letter day of week of the game date. |
| `game_code` | character | Provider game code, `YYYYMMDD/AWYHOM`. |
| `game_date` | character | Game date as served by the schedule feed (`MM/DD/YYYY HH:MM:SS`). |
| `game_date_est` | character | Game date at midnight Eastern, ISO-8601. |
| `game_date_time_est` | character | Scheduled tip-off in Eastern time, ISO-8601. |
| `game_date_time_utc` | character | Scheduled tip-off in UTC, ISO-8601. The timestamp to reduce to a calendar date. |
| `game_date_utc` | character | Game date at midnight UTC, ISO-8601. |
| `game_id` | character | Unique stats.nba.com / stats.wnba.com game id; its 3rd character encodes the season type. |
| `game_label` | character | Human-readable round or event label (e.g. Preseason, Conf. Finals). |
| `game_sequence` | integer | Ordinal of the game within its date. |
| `game_status` | integer | Game status code: 1 scheduled, 2 in progress, 3 final. |
| `game_status_text` | character | Human-readable game status (e.g. Final, 7:00 pm ET). |
| `game_sub_label` | character | Secondary event label (e.g. NBA Abu Dhabi Game). |
| `game_subtype` | character | Game subtype tag (e.g. Global Games), blank for standard games. |
| `game_time_est` | character | Scheduled tip-off time of day, Eastern. |
| `game_time_utc` | character | Scheduled tip-off time of day, UTC. |
| `home_team_city` | character | City of the home team. |
| `home_team_id` | integer | stats.nba.com / stats.wnba.com team id of the home team. |
| `home_team_losses` | integer | Home team's losses entering the game. |
| `home_team_name` | character | Home team nickname (e.g. Liberty). |
| `home_team_score` | integer | Home team's final score, 0 before the game is played. |
| `home_team_seed` | integer | Home team's playoff seed, 0 outside the postseason. |
| `home_team_slug` | character | URL slug of the home team. |
| `home_team_time` | character | Scheduled tip-off in the home team's local time zone. |
| `home_team_tricode` | character | Three-letter abbreviation of the home team. |
| `home_team_wins` | integer | Home team's wins entering the game. |
| `if_necessary` | logical | Whether the game is a conditional series game that may not be played. |
| `is_neutral` | logical | Whether the game is played at a neutral site. |
| `league_id` | character | League id of the schedule: '00' NBA, '10' WNBA, '20' G-League. |
| `month_num` | integer | Calendar month number of the game date. |
| `postponed_status` | character | Postponement status; 'N' when the game is on as scheduled. |
| `season` | character | Season the schedule covers, as published by the feed ('2025-26' for the NBA, '2026' for the WNBA). |
| `season_type_description` | character | Season type label derived from season_type_id: Pre-Season, Regular Season, All-Star, Playoffs, Play-In Game. |
| `season_type_id` | character | Season type digit, the 3rd character of game_id. |
| `series_game_number` | character | Game number within a playoff series, blank outside a series. |
| `series_text` | character | Series context line (e.g. series tied 1-1), blank when not applicable. |
| `week_name` | character | Name of the schedule week, blank outside the regular season. |
| `week_number` | integer | Schedule week number, 0 outside the regular season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_scheduleleaguev2int-example}

```python
nba_stats_scheduleleaguev2int(league_id='00')
```

_Last validated n/a._
