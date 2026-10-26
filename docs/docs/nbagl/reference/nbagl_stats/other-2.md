---
title: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Other: playbyplayv3–scoreboardv3"
sidebar_label: "Other: playbyplayv3–scoreboardv3"
sidebar_position: 24
description: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Other: playbyplayv3–scoreboardv3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Other: playbyplayv3–scoreboardv3

## nbagl_stats_playbyplayv3

Play by play v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv3?EndPeriod=0&GameID=2022500137&StartPeriod=0](https://stats.nba.com/stats/playbyplayv3?EndPeriod=0&GameID=2022500137&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#nbagl_stats_playbyplayv3-returns}

**`return_parsed=True`** (default) — the output of `parse_nbagl_stats_result_sets`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: the committed capture tests/fixtures/nbagl_stats/endpoints/playbyplayv3.json is not a resultSets envelope (top-level keys: game, meta), which parse_nba_stats_result_sets does not read, so the parsed output is empty; use return_parsed=False for the raw payload.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playbyplayv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playbyplayv3-example}

```python
nbagl_stats_playbyplayv3()
```

## nbagl_stats_scoreboardv3

Scoreboard v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/scoreboardv3`

**Valid URL:** [https://stats.nba.com/stats/scoreboardv3?GameDate=2025-01-15&LeagueID=20](https://stats.nba.com/stats/scoreboardv3?GameDate=2025-01-15&LeagueID=20)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_scoreboardv3-returns}

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
| `gamecode` | String |  |
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
| `period` | Int64 |  |
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

### Raises {#nbagl_stats_scoreboardv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_scoreboardv3-example}

```python
nbagl_stats_scoreboardv3(league_id='20')
```
