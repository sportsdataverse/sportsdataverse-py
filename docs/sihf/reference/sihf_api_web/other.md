# SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Other

> SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Other — function reference in sdv-py, the SportsDataverse Python package.

## sihf_results

Games of a league season: date, teams, score, per-period score, decision, status and game_id.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=results&searchQuery=1%2F%2F1&language=en&filterBy=Season%2CDate&filterQuery=2026%2F01.07.2025-30.06.2026](https://data.sihf.ch/statistic/api/cms/table?alias=results&searchQuery=1%2F%2F1&language=en&filterBy=Season%2CDate&filterQuery=2026%2F01.07.2025-30.06.2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase (player / goalie tables), every game (results), the regular-season table (standings). |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `day` | character | Weekday abbreviation, German (e.g., "Di"). |
| `date` | character | Game date (local). |
| `time` | character | Scheduled local start time, "hh:mm". |
| `home_team_id` | integer | SIHF team id of the home team. |
| `home_team_name` | character | Home team display name. |
| `home_team_acronym` | character | Home team abbreviation. |
| `away_team_id` | integer | SIHF team id of the away team. |
| `away_team_name` | character | Away team display name. |
| `away_team_acronym` | character | Away team abbreviation. |
| `score_home` | integer | Final home score. |
| `score_away` | integer | Final away score. |
| `score_per_period_home` | integer | Home goals per period (overtime included as its own period). |
| `score_per_period_away` | integer | Away goals per period (overtime included as its own period). |
| `decision` | character | How the game was decided beyond regulation: "OT" or "SO"; null in regulation. |
| `status_id` | integer | SIHF game-status id (12 = finished). |
| `status_name` | character | Game-status name; German whatever the language (e.g., "Ende"). |
| `status_percent` | integer | Game progress, 0-100 (100 once the game is over). |
| `status_canceled` | logical | Whether the game was cancelled. |
| `start_date_time` | character | Scheduled start, ISO-8601 with UTC offset, as a string. |
| `end_date_time` | character | End of the game, ISO-8601 with UTC offset, as a string. |
| `update_date_time` | character | When SIHF last updated the record, ISO-8601 with UTC offset, as a string. |
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `broadcasts` | character | Broadcasters, comma-joined (e.g., "MYSPORTS,TV24"). |
| `row_type` | character | "data" for a regular row; "average" or "total" for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_results-example}

```python
sihf_results(season=2026, league_id=1)
```

_Last validated n/a._

## sihf_standings

League table of a season (every game, or home / away games only): rank, games, points, regulation / overtime / shootout wins and losses, goals, streak.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=standing&searchQuery=1%2F%2F1&language=en&filterBy=Season%2CContentType&filterQuery=2026%2FAll](https://data.sihf.ch/statistic/api/cms/table?alias=standing&searchQuery=1%2F%2F1&language=en&filterBy=Season%2CContentType&filterQuery=2026%2FAll)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the regular-season table. |
| `ContentType` | `venue` |  |  | `Y` | Which games the table counts: All (the full table), Home or Away. Always sent: without it SIHF answers the away-games table (26 of the 52 National League games). |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row. |
| `team_id` | integer | SIHF team id (Int64 join key). |
| `team_name` | character | Team display name as SIHF publishes it. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"). |
| `games_played` | integer | Games the team played in the selection (every game, or home / away games only, per venue). |
| `points` | integer | Table points earned in the standings. |
| `points_quotient` | double | Points per game. |
| `games_won_in_time` | integer | Wins in regulation. |
| `games_won_extra_time` | integer | Wins in overtime. |
| `games_won_penalty` | integer | Wins in a shootout. |
| `games_lost_penalty` | integer | Losses in a shootout. |
| `games_lost_extra_time` | integer | Losses in overtime. |
| `games_lost_in_time` | integer | Losses in regulation. |
| `goals_for` | integer | Goals scored by the team. |
| `goals_against` | integer | Goals allowed by the team. |
| `goals_difference` | integer | Goal difference. |
| `streak` | character | Recent results, newest first, as SIHF prints them (e.g., "W+L-"; "+" regulation, "-" after regulation). |
| `row_type` | character | "data" for a regular row; "average" or "total" for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_standings-example}

```python
sihf_standings(season=2026, league_id=1)
```

_Last validated n/a._

## sihf_phases

Phases of a league season (regular season, play-in and playoff rounds, playout): the phase_id values the table wrappers take, with each phase's name and game count.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?searchQuery=1%2F%2F1&alias=results&language=en&filterBy=Season%2CPhase%2CDate&filterQuery=2026%2F%2F01.07.2025-30.06.2026](https://data.sihf.ch/statistic/api/cms/table?searchQuery=1%2F%2F1&alias=results&language=en&filterBy=Season%2CPhase%2CDate&filterQuery=2026%2F%2F01.07.2025-30.06.2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `alias` | `table` |  |  | `Y` | SIHF table whose phase list to read; each table lists its own. results (sihf_results) lists every round with its game count; player / goalkeeper (sihf_player_stats / sihf_goalie_stats) add the Playoff Overall and Play-In Overall totals; standing (sihf_standings) lists only the phases played as a table (the regular season, in some seasons a playout round). |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_phases-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (Int64), the value the phase_id argument of sihf_results, sihf_standings, sihf_player_stats and sihf_goalie_stats takes. Ids differ by league, season and table. |
| `phase_name` | character | Phase name as SIHF publishes it, without the trailing count (e.g., "Regular Season", "Playoff 1/4 Final", "Playoff Overall"). |
| `n_games` | integer | Games played in the phase, from the results table's phase list; null for the other tables and for a phase still in progress. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_phases-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_phases-example}

```python
sihf_phases(season=2026, league_id=1)
```

_Last validated n/a._

## sihf_pbp

Event timeline of one game: goals, penalties, goaltender changes, revoked goals and shootout attempts.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/gameoverview`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en](https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchQuery` | `game_id` |  | `Y` |  | Required. SIHF game id (game_id in sihf_results), e.g. 20261105000325. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `event_index` | integer | 1-based event order within the game. |
| `period_number` | integer | 1-based period (4 = first overtime); the shootout follows the last period. |
| `period` | character | Period name as SIHF prints it, German (e.g., "1. Drittel", "Shootout"). |
| `event_type` | character | Event kind: "goal", "penalty", "goalie", "revoked_goal" or "shootout". |
| `time` | character | Elapsed game time of the event, "mm:ss" (null on shootout rows). |
| `game_seconds` | integer | Elapsed game time of the event in seconds. |
| `team_id` | integer | SIHF team id of the team the event belongs to. |
| `team_side` | character | Side of the game the row belongs to: "home" or "away". |
| `player_id` | integer | Licence number of the scorer, penalized player, goaltender (goalie rows) or shooter. |
| `player_name` | character | Name of the scorer, penalized player, goaltender (goalie rows) or shooter. |
| `player_jersey` | integer | Jersey number of the player in player_id. |
| `assist1_player_id` | integer | Licence number of the first assist. |
| `assist1_player_name` | character | Name of the first assist. |
| `assist2_player_id` | integer | Licence number of the second assist. |
| `assist2_player_name` | character | Name of the second assist. |
| `goalie_id` | integer | Licence number of the goaltender faced (shootout rows). |
| `goalie_name` | character | Name of the goaltender faced (shootout rows). |
| `strength` | character | First goal tag: "EQ" even strength, "PP1" power play, "SH1" shorthanded, ... |
| `goal_tags` | character | Every goal tag, comma-joined as SIHF prints them (e.g., "PP1,GWG"). |
| `score_home` | integer | Home score after the goal. |
| `score_away` | integer | Away score after the goal. |
| `plus_player_ids` | integer | Licence numbers credited +1 on the goal (listed only where plus-minus applies). |
| `minus_player_ids` | integer | Licence numbers charged -1 on the goal (listed only where plus-minus applies). |
| `penalty_id` | integer | SIHF penalty-type id. |
| `penalty_name` | character | Penalty name, German whatever the language (e.g., "Haken"). |
| `penalty_minutes` | integer | Penalty length in minutes. |
| `penalty_start` | character | Game time the penalty starts, "mm:ss". |
| `penalty_end` | character | Game time the penalty ended, "mm:ss" (earlier than its full length when a goal ended it). |
| `goalie_change` | character | Goaltender change direction: "in" or "out". |
| `revoke_reason` | character | Why the goal was disallowed, as SIHF prints it (e.g., "CC-Offiside"). |
| `shootout_number` | integer | 1-based shootout attempt number. |
| `shootout_scored` | logical | Whether the shootout attempt scored. |
| `text` | character | Event text exactly as SIHF prints it (markdown bold included). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_pbp-example}

```python
sihf_pbp(game_id=20261105000325)
```

_Last validated n/a._
