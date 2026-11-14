# WNBA — WNBA Stats API (stats.wnba.com) — Scoreboard

> WNBA — WNBA Stats API (stats.wnba.com) — Scoreboard — function reference in sdv-py, the SportsDataverse Python package.

## wnba_stats_scoreboardv2

Scoreboard v2.

**Endpoint URL:** `GET https://stats.wnba.com/stats/scoreboardv2`

**Valid URL:** [https://stats.wnba.com/stats/scoreboardv2?DayOffset=0&GameDate=2022-07-20&LeagueID=10](https://stats.wnba.com/stats/scoreboardv2?DayOffset=0&GameDate=2022-07-20&LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DayOffset` | `day_offset` |  |  | `Y` |  |
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_scoreboardv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameHeader`, `LineScore`, `SeriesStandings`, `LastMeeting`, `EastConfStandingsByDay`, `WestConfStandingsByDay`, `Available`, `TeamLeaders`, `TicketLinks`, `WinProbability`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GameHeader**

| col_name | type | description |
|---|---|---|
| `game_date_est` | String | Game date est. |
| `game_sequence` | Int64 | Game sequence. |
| `game_id` | String | Unique game identifier. |
| `game_status_id` | Int64 | Numeric game status identifier. |
| `game_status_text` | String | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `gamecode` | String | Gamecode. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `season` | String | Season identifier (4-digit year or 'YYYY-YY' string). |
| `live_period` | Int64 | Live period. |
| `live_pc_time` | String | Time / clock value. |
| `natl_tv_broadcaster_abbreviation` | String | Natl tv broadcaster abbreviation. |
| `home_tv_broadcaster_abbreviation` | String | Home team's tv broadcaster abbreviation. |
| `away_tv_broadcaster_abbreviation` | String | Away team's tv broadcaster abbreviation. |
| `live_period_time_bcast` | String | Live period time bcast. |
| `arena_name` | String | Arena name. |
| `wh_status` | Int64 | Wh status. |
| `wnba_commissioner_flag` | Int64 |  |

**LineScore**

| col_name | type | description |
|---|---|---|
| `game_date_est` | String | Game date est. |
| `game_sequence` | Int64 | Game sequence. |
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city_name` | String | Team city name. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_wins_losses` | String | Team wins losses. |
| `pts_qtr1` | Int64 | Pts qtr1. |
| `pts_qtr2` | Int64 | Pts qtr2. |
| `pts_qtr3` | Int64 | Pts qtr3. |
| `pts_qtr4` | Int64 | Pts qtr4. |
| `pts_ot1` | Int64 | Pts ot1. |
| `pts_ot2` | Int64 | Points scored by the team in overtime period 2. |
| `pts_ot3` | Int64 | Points scored by the team in overtime period 3. |
| `pts_ot4` | Int64 | Points scored by the team in overtime period 4. |
| `pts_ot5` | Int64 | Points scored by the team in overtime period 5. |
| `pts_ot6` | Int64 | Points scored by the team in overtime period 6. |
| `pts_ot7` | Int64 | Points scored by the team in overtime period 7. |
| `pts_ot8` | Int64 | Points scored by the team in overtime period 8. |
| `pts_ot9` | Int64 | Points scored by the team in overtime period 9. |
| `pts_ot10` | Int64 | Points scored by the team in overtime period 10. |
| `pts` | Int64 | Points scored. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ast` | Int64 | Assists. |
| `reb` | Int64 | Total rebounds. |
| `tov` | Int64 | Turnovers. |

**SeriesStandings**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `game_date_est` | String | Game date est. |
| `home_team_wins` | Int64 | Home team's team wins. |
| `home_team_losses` | Int64 | Home team's team losses. |
| `series_leader` | String |  |

**LastMeeting**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `last_game_id` | String |  |
| `last_game_date_est` | String |  |
| `last_game_home_team_id` | Int64 |  |
| `last_game_home_team_city` | String |  |
| `last_game_home_team_name` | String |  |
| `last_game_home_team_abbreviation` | String |  |
| `last_game_home_team_points` | Int64 |  |
| `last_game_visitor_team_id` | Int64 |  |
| `last_game_visitor_team_city` | String |  |
| `last_game_visitor_team_name` | String |  |
| `last_game_visitor_team_city1` | String |  |
| `last_game_visitor_team_points` | Int64 |  |

**EastConfStandingsByDay**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_id` | String | Unique season identifier. |
| `standingsdate` | String |  |
| `conference` | String | Filter players or teams by conference. |
| `team` | String | Team-side label or team identifier. |
| `g` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `home_record` | String | Home win-loss record. |
| `road_record` | String | Win-loss record for road. |

**WestConfStandingsByDay**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_id` | String | Unique season identifier. |
| `standingsdate` | String |  |
| `conference` | String | Filter players or teams by conference. |
| `team` | String | Team-side label or team identifier. |
| `g` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `home_record` | String | Home win-loss record. |
| `road_record` | String | Win-loss record for road. |

**Available**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `pt_available` | Int64 | Pt available. |

**TeamLeaders**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_nickname` | String | Team nickname. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `pts_player_id` | Int64 |  |
| `pts_player_name` | String |  |
| `pts` | Int64 | Points scored. |
| `reb_player_id` | Int64 |  |
| `reb_player_name` | String |  |
| `reb` | Int64 | Total rebounds. |
| `ast_player_id` | Int64 |  |
| `ast_player_name` | String |  |
| `ast` | Int64 | Assists. |

**TicketLinks**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `leag_tix` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_scoreboardv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_scoreboardv2-example}

```python
wnba_stats_scoreboardv2(league_id='10')
```

## wnba_stats_scoreboardv3

Scoreboard v3.

**Endpoint URL:** `GET https://stats.wnba.com/stats/scoreboardv3`

**Valid URL:** [https://stats.wnba.com/stats/scoreboardv3?GameDate=2022-06-26&LeagueID=10](https://stats.wnba.com/stats/scoreboardv3?GameDate=2022-06-26&LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_scoreboardv3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `awayteam_inbonus` | String | Whether the away team is currently in the bonus (penalty) foul situation. |
| `awayteam_losses` | Int64 | Away team's loss total entering the game. |
| `awayteam_score` | Int64 | Current or final points scored by the away team. |
| `awayteam_seed` | Int64 | Playoff seed of the away team, populated for postseason games. |
| `awayteam_teamcity` | String | City name of the away team. |
| `awayteam_teamid` | Int64 | Team identifier of the away team from the league's stats API. |
| `awayteam_teamname` | String | Nickname of the away team. |
| `awayteam_teamslug` | String | URL-friendly slug for the away team's name. |
| `awayteam_teamtricode` | String | Three-letter abbreviation of the away team. |
| `awayteam_timeoutsremaining` | Int64 | Timeouts the away team has remaining. |
| `awayteam_wins` | Int64 | Away team's win total entering the game. |
| `gameclock` | String | Current game clock display for a live game. |
| `gamecode` | String | Gamecode. |
| `gamedate` | String | Game date the scoreboard was requested for (YYYY-MM-DD). |
| `gameet` | String | Scheduled game start time in US Eastern time. |
| `gameid` | String | Unique 10-character game identifier from the league's stats API. |
| `gamelabel` | String | Display label for the game (e.g. a playoff series or event name). |
| `gameleaders_awayleaders_assists` | Int64 | Assist total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_jerseynum` | String | Jersey number of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_name` | String | Name of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_personid` | Int64 | Stats API player id of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_playerslug` | String | URL name slug of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_points` | Int64 | Point total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_position` | String | Position of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_rebounds` | Int64 | Rebound total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_teamtricode` | String | Team tricode of the away team's in-game statistical leader. |
| `gameleaders_homeleaders_assists` | Int64 | Assist total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_jerseynum` | String | Jersey number of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_name` | String | Name of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_personid` | Int64 | Stats API player id of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_playerslug` | String | URL name slug of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_points` | Int64 | Point total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_position` | String | Position of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_rebounds` | Int64 | Rebound total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_teamtricode` | String | Team tricode of the home team's in-game statistical leader. |
| `gamestatus` | Int64 | Numeric game status code (1 = scheduled, 2 = in progress, 3 = final). |
| `gamestatustext` | String | Human-readable game status (e.g. "Final", "7:00 pm ET"). |
| `gamesublabel` | String | Secondary display label for the game (e.g. game number within a series). |
| `gamesubtype` | String | Subtype code for the game as reported by the stats API (e.g. in-season tournament flags). |
| `gametimeutc` | String | Scheduled game start time in UTC. |
| `hometeam_inbonus` | String | Whether the home team is currently in the bonus (penalty) foul situation. |
| `hometeam_losses` | Int64 | Home team's loss total entering the game. |
| `hometeam_score` | Int64 | Current or final points scored by the home team. |
| `hometeam_seed` | Int64 | Playoff seed of the home team, populated for postseason games. |
| `hometeam_teamcity` | String | City name of the home team. |
| `hometeam_teamid` | Int64 | Team identifier of the home team from the league's stats API. |
| `hometeam_teamname` | String | Nickname of the home team. |
| `hometeam_teamslug` | String | URL-friendly slug for the home team's name. |
| `hometeam_teamtricode` | String | Three-letter abbreviation of the home team. |
| `hometeam_timeoutsremaining` | Int64 | Timeouts the home team has remaining. |
| `hometeam_wins` | Int64 | Home team's win total entering the game. |
| `ifnecessary` | Boolean | Whether the game is an if-necessary playoff series game. |
| `isneutral` | Boolean | Whether the game is played at a neutral site. |
| `leagueid` | String | League identifier from the stats API ("00" = NBA, "10" = WNBA). |
| `leaguename` | String | Display name of the league. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `porounddesc` | String | Playoff round description (e.g. Conference Finals). |
| `regulationperiods` | Int64 | Number of regulation periods for the game (4). |
| `seriesconference` | String | Conference of the playoff series the game belongs to. |
| `seriesgamenumber` | String | Game number within the playoff series. |
| `seriestext` | String | Display text summarizing the series state (e.g. "BOS leads 2-1"). |
| `teamleaders_awayleaders_assists` | Float64 | Assist total of the away team's season statistical leader. |
| `teamleaders_awayleaders_jerseynum` | String | Jersey number of the away team's season statistical leader. |
| `teamleaders_awayleaders_name` | String | Name of the away team's season statistical leader. |
| `teamleaders_awayleaders_personid` | Int64 | Stats API player id of the away team's season statistical leader. |
| `teamleaders_awayleaders_playerslug` | String | URL name slug of the away team's season statistical leader. |
| `teamleaders_awayleaders_points` | Float64 | Point total of the away team's season statistical leader. |
| `teamleaders_awayleaders_position` | String | Position of the away team's season statistical leader. |
| `teamleaders_awayleaders_rebounds` | Float64 | Rebound total of the away team's season statistical leader. |
| `teamleaders_awayleaders_teamtricode` | String | Team tricode of the away team's season statistical leader. |
| `teamleaders_homeleaders_assists` | Float64 | Assist total of the home team's season statistical leader. |
| `teamleaders_homeleaders_jerseynum` | String | Jersey number of the home team's season statistical leader. |
| `teamleaders_homeleaders_name` | String | Name of the home team's season statistical leader. |
| `teamleaders_homeleaders_personid` | Int64 | Stats API player id of the home team's season statistical leader. |
| `teamleaders_homeleaders_playerslug` | String | URL name slug of the home team's season statistical leader. |
| `teamleaders_homeleaders_points` | Float64 | Point total of the home team's season statistical leader. |
| `teamleaders_homeleaders_position` | String | Position of the home team's season statistical leader. |
| `teamleaders_homeleaders_rebounds` | Float64 | Rebound total of the home team's season statistical leader. |
| `teamleaders_homeleaders_teamtricode` | String | Team tricode of the home team's season statistical leader. |
| `teamleaders_seasonleadersflag` | Int64 | Flag indicating the team-leaders block carries season-long leaders rather than in-game leaders. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_scoreboardv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_scoreboardv3-example}

```python
wnba_stats_scoreboardv3(league_id='10')
```
