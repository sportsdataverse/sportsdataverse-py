---
title: "NBA — NBA Stats API (stats.nba.com) — Scoreboard"
sidebar_label: "Scoreboard"
sidebar_position: 19
description: "NBA — NBA Stats API (stats.nba.com) — Scoreboard — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Scoreboard

## nba_stats_scoreboardv2

GET /stats/scoreboardv2

**Endpoint URL:** `GET https://stats.nba.com/stats/scoreboardv2`

**Valid URL:** [https://stats.nba.com/stats/scoreboardv2?LeagueID=00](https://stats.nba.com/stats/scoreboardv2?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DayOffset` | `day_offset` |  |  | `Y` |  |
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_scoreboardv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameHeader`, `LineScore`, `SeriesStandings`, `LastMeeting`, `EastConfStandingsByDay`, `WestConfStandingsByDay`, `Available`, `TeamLeaders`, `TicketLinks`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**GameHeader**

| col_name | type | description |
|---|---|---|
| `game_date_est` | character | Game date est. |
| `game_sequence` | integer | Game sequence. |
| `game_id` | character | Unique game identifier. |
| `game_status_id` | integer | Numeric game status identifier. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `gamecode` | character | Gamecode. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `season` | character | Season year. |
| `live_period` | integer | Live period. |
| `live_pc_time` | character | Time / clock value. |
| `natl_tv_broadcaster_abbreviation` | character | Natl tv broadcaster abbreviation. |
| `home_tv_broadcaster_abbreviation` | character | Home team's tv broadcaster abbreviation. |
| `away_tv_broadcaster_abbreviation` | character | Away team's tv broadcaster abbreviation. |
| `live_period_time_bcast` | character | Live period time bcast. |
| `arena_name` | character | Arena name. |
| `wh_status` | integer | Wh status. |
| `wnba_commissioner_flag` | integer |  |

**LineScore**

| col_name | type | description |
|---|---|---|
| `game_date_est` | character | Game date est. |
| `game_sequence` | integer | Game sequence. |
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city_name` | character | Team city name. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_wins_losses` | character | Team wins losses. |
| `pts_qtr1` | integer | Pts qtr1. |
| `pts_qtr2` | integer | Pts qtr2. |
| `pts_qtr3` | integer | Pts qtr3. |
| `pts_qtr4` | integer | Pts qtr4. |
| `pts_ot1` | integer | Pts ot1. |
| `pts_ot2` | integer | Points scored by the team in overtime period 2. |
| `pts_ot3` | integer | Points scored by the team in overtime period 3. |
| `pts_ot4` | integer | Points scored by the team in overtime period 4. |
| `pts_ot5` | integer | Points scored by the team in overtime period 5. |
| `pts_ot6` | integer | Points scored by the team in overtime period 6. |
| `pts_ot7` | integer | Points scored by the team in overtime period 7. |
| `pts_ot8` | integer | Points scored by the team in overtime period 8. |
| `pts_ot9` | integer | Points scored by the team in overtime period 9. |
| `pts_ot10` | integer | Points scored by the team in overtime period 10. |
| `pts` | integer | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ast` | integer | Assists. |
| `reb` | integer | Rebounds per game. |
| `tov` | integer | Turnovers. |

**SeriesStandings**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `game_date_est` | character | Game date est. |
| `home_team_wins` | integer | Home team's team wins. |
| `home_team_losses` | integer | Home team's team losses. |
| `series_leader` | character |  |

**LastMeeting**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `last_game_id` | character |  |
| `last_game_date_est` | character |  |
| `last_game_home_team_id` | integer |  |
| `last_game_home_team_city` | character |  |
| `last_game_home_team_name` | character |  |
| `last_game_home_team_abbreviation` | character |  |
| `last_game_home_team_points` | integer |  |
| `last_game_visitor_team_id` | integer |  |
| `last_game_visitor_team_city` | character |  |
| `last_game_visitor_team_name` | character |  |
| `last_game_visitor_team_city1` | character |  |
| `last_game_visitor_team_points` | integer |  |

**EastConfStandingsByDay**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_id` | character | Unique season identifier. |
| `standingsdate` | character |  |
| `conference` | character | Conference name. |
| `team` | character | Team-side label or team identifier. |
| `g` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `home_record` | character | Home win-loss record. |
| `road_record` | character | Win-loss record for road. |

**WestConfStandingsByDay**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_id` | character | Unique season identifier. |
| `standingsdate` | character |  |
| `conference` | character | Conference name. |
| `team` | character | Team-side label or team identifier. |
| `g` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `home_record` | character | Home win-loss record. |
| `road_record` | character | Win-loss record for road. |

**Available**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `pt_available` | integer | Pt available. |

**TeamLeaders**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_nickname` | character | Team nickname. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pts_player_id` | integer |  |
| `pts_player_name` | character |  |
| `pts` | integer | Points scored. |
| `reb_player_id` | integer |  |
| `reb_player_name` | character |  |
| `reb` | integer | Rebounds per game. |
| `ast_player_id` | integer |  |
| `ast_player_name` | character |  |
| `ast` | integer | Assists. |

**TicketLinks**

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `leag_tix` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_scoreboardv2-example}

```python
nba_stats_scoreboardv2(league_id='00')
```

_Last validated n/a._

## nba_stats_scoreboardv3

GET /stats/scoreboardv3

**Endpoint URL:** `GET https://stats.nba.com/stats/scoreboardv3`

**Valid URL:** [https://stats.nba.com/stats/scoreboardv3?LeagueID=00](https://stats.nba.com/stats/scoreboardv3?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_scoreboardv3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `awayteam_inbonus` | character | Whether the away team is currently in the bonus (penalty) foul situation. |
| `awayteam_losses` | integer | Away team's loss total entering the game. |
| `awayteam_score` | integer | Current or final points scored by the away team. |
| `awayteam_seed` | integer | Playoff seed of the away team, populated for postseason games. |
| `awayteam_teamcity` | character | City name of the away team. |
| `awayteam_teamid` | integer | Team identifier of the away team from the league's stats API. |
| `awayteam_teamname` | character | Nickname of the away team. |
| `awayteam_teamslug` | character | URL-friendly slug for the away team's name. |
| `awayteam_teamtricode` | character | Three-letter abbreviation of the away team. |
| `awayteam_timeoutsremaining` | integer | Timeouts the away team has remaining. |
| `awayteam_wins` | integer | Away team's win total entering the game. |
| `gameclock` | character | Current game clock display for a live game. |
| `gamecode` | character | Gamecode. |
| `gamedate` | character | Game date as parsed from the source feed. |
| `gameet` | character | Scheduled game start time in US Eastern time. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `gamelabel` | character | Display label for the game (e.g. a playoff series or event name). |
| `gameleaders_awayleaders_assists` | integer | Assist total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_jerseynum` | character | Jersey number of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_name` | character | Name of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_personid` | integer | Stats API player id of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_playerslug` | character | URL name slug of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_points` | integer | Point total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_position` | character | Position of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_rebounds` | integer | Rebound total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_teamtricode` | character | Team tricode of the away team's in-game statistical leader. |
| `gameleaders_homeleaders_assists` | integer | Assist total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_jerseynum` | character | Jersey number of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_name` | character | Name of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_personid` | integer | Stats API player id of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_playerslug` | character | URL name slug of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_points` | integer | Point total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_position` | character | Position of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_rebounds` | integer | Rebound total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_teamtricode` | character | Team tricode of the home team's in-game statistical leader. |
| `gamestatus` | integer | Numeric game status code (1 = scheduled, 2 = in progress, 3 = final). |
| `gamestatustext` | character | Human-readable game status (e.g. "Final", "7:00 pm ET"). |
| `gamesublabel` | character | Secondary display label for the game (e.g. game number within a series). |
| `gamesubtype` | character | Subtype code for the game as reported by the stats API (e.g. in-season tournament flags). |
| `gametimeutc` | character | Scheduled game start time in UTC. |
| `hometeam_inbonus` | character | Whether the home team is currently in the bonus (penalty) foul situation. |
| `hometeam_losses` | integer | Home team's loss total entering the game. |
| `hometeam_score` | integer | Current or final points scored by the home team. |
| `hometeam_seed` | integer | Playoff seed of the home team, populated for postseason games. |
| `hometeam_teamcity` | character | City name of the home team. |
| `hometeam_teamid` | integer | Team identifier of the home team from the league's stats API. |
| `hometeam_teamname` | character | Nickname of the home team. |
| `hometeam_teamslug` | character | URL-friendly slug for the home team's name. |
| `hometeam_teamtricode` | character | Three-letter abbreviation of the home team. |
| `hometeam_timeoutsremaining` | integer | Timeouts the home team has remaining. |
| `hometeam_wins` | integer | Home team's win total entering the game. |
| `ifnecessary` | logical | Whether the game is an if-necessary playoff series game. |
| `isneutral` | logical | Whether the game is played at a neutral site. |
| `leagueid` | character | League identifier from the stats API ("00" = NBA, "10" = WNBA). |
| `leaguename` | character | Display name of the league. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `porounddesc` | character | Playoff round description (e.g. Conference Finals). |
| `regulationperiods` | integer | Number of regulation periods for the game (4). |
| `seriesconference` | character | Conference of the playoff series the game belongs to. |
| `seriesgamenumber` | character | Game number within the playoff series. |
| `seriestext` | character | Display text summarizing the series state (e.g. "BOS leads 2-1"). |
| `teamleaders_awayleaders_assists` | numeric | Assist total of the away team's season statistical leader. |
| `teamleaders_awayleaders_jerseynum` | character | Jersey number of the away team's season statistical leader. |
| `teamleaders_awayleaders_name` | character | Name of the away team's season statistical leader. |
| `teamleaders_awayleaders_personid` | integer | Stats API player id of the away team's season statistical leader. |
| `teamleaders_awayleaders_playerslug` | character | URL name slug of the away team's season statistical leader. |
| `teamleaders_awayleaders_points` | numeric | Point total of the away team's season statistical leader. |
| `teamleaders_awayleaders_position` | character | Position of the away team's season statistical leader. |
| `teamleaders_awayleaders_rebounds` | numeric | Rebound total of the away team's season statistical leader. |
| `teamleaders_awayleaders_teamtricode` | character | Team tricode of the away team's season statistical leader. |
| `teamleaders_homeleaders_assists` | numeric | Assist total of the home team's season statistical leader. |
| `teamleaders_homeleaders_jerseynum` | character | Jersey number of the home team's season statistical leader. |
| `teamleaders_homeleaders_name` | character | Name of the home team's season statistical leader. |
| `teamleaders_homeleaders_personid` | integer | Stats API player id of the home team's season statistical leader. |
| `teamleaders_homeleaders_playerslug` | character | URL name slug of the home team's season statistical leader. |
| `teamleaders_homeleaders_points` | numeric | Point total of the home team's season statistical leader. |
| `teamleaders_homeleaders_position` | character | Position of the home team's season statistical leader. |
| `teamleaders_homeleaders_rebounds` | numeric | Rebound total of the home team's season statistical leader. |
| `teamleaders_homeleaders_teamtricode` | character | Team tricode of the home team's season statistical leader. |
| `teamleaders_seasonleadersflag` | integer | Flag indicating the team-leaders block carries season-long leaders rather than in-game leaders. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_scoreboardv3-example}

```python
nba_stats_scoreboardv3(league_id='00')
```

_Last validated n/a._
