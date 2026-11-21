# CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Other

> CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Other — function reference in sdv-py, the SportsDataverse Python package.

## cba_seasons

Every season of the CBA data site, newest first (the upcoming season is the default), 1995-96 on.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/com-code-tables/getSeason`

**Valid URL:** [https://data-server.cbaleague.com/api/com-code-tables/getSeason](https://data-server.cbaleague.com/api/com-code-tables/getSeason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#cba_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `is_default` | Int64 | Default-selection flag: 1 = the site's default season (exactly one row: the season in progress or next), 2 = not the default. |
| `key` | String | Season label 'YYYY-YYYY' (START year - END year), e.g. '2025-2026'; the sdv season key is the second year. The list runs from the upcoming season (2026-2027, 32 rows) down to 1995-1996 with no gap. |
| `value` | String | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. String of digits ('2025'); the value every season parameter takes. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_seasons-example}

```python
cba_seasons()
```

## cba_match_types

The match types (regular season, playoffs, preseason, all-star, relegation play-off) with their ids.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/com-code-tables/getMatchType`

**Valid URL:** [https://data-server.cbaleague.com/api/com-code-tables/getMatchType](https://data-server.cbaleague.com/api/com-code-tables/getMatchType)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#cba_match_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `is_default` | Int64 | Default-selection flag: 1 = the site's default (the preseason, '3', at capture time: it follows the calendar), 2 = not the default. |
| `key` | String | Match type name (Chinese): 常规赛 regular season, 季后赛 playoffs, 季前赛 preseason, 全明星 all-star, 保级赛 relegation play-off. |
| `value` | String | Match type id as a string ('1' regular season, '2' playoffs, '3' preseason, '4' all-star, '9' relegation); the matchTypeId of every other route. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_match_types-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_match_types-example}

```python
cba_match_types()
```

## cba_schedule_month

Game count per day of one calendar month.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/schedules/month`

**Valid URL:** [https://data-server.cbaleague.com/api/schedules/month?year=2025&month=12](https://data-server.cbaleague.com/api/schedules/month?year=2025&month=12)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required. CALENDAR year (not the season). |
| `month` | `month` |  | `Y` |  | Required. Calendar month, 1-12. |

### Returns {#cba_schedule_month-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_num` | Int64 | Number of games scheduled on that day (0 = none). |
| `schedule_time` | String | The day as 'YYYY-MM-DD 00:00:00' (China local time, no zone); one row per calendar day of the month. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_schedule_month-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_schedule_month-example}

```python
cba_schedule_month(month=12, year=2025)
```

## cba_schedule_day

The games of one day: ids, teams, status, score, round and the data flags.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/schedules/day`

**Valid URL:** [https://data-server.cbaleague.com/api/schedules/day?auto=0&day=2025-12-12](https://data-server.cbaleague.com/api/schedules/day?auto=0&day=2025-12-12)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `day` | `day` |  | `Y` |  | Required. Date 'YYYY-MM-DD'. The runtime always sends auto=0 (the site's auto mode jumps to the next game day) and refuses rows on another date. |

### Returns {#cba_schedule_day-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `home_team_id` | Int64 | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `home_team_logo` | String | Absolute URL of the home team logo (300x300 png). |
| `home_team_lose` | Int64 | Home team's cumulative losses after this game. An integer (the API sends it as digit text). |
| `home_team_name` | String | Home team name (Chinese) as of the game's season; changes with the sponsor. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `home_team_score` | Int64 | Home team final points (a game not yet played has no score; a scheduled 2026-27 game outside the samples showed null). |
| `home_team_win` | Int64 | Home team's cumulative wins after this game in the match type. An integer (the API sends it as digit text). |
| `is_events` | Int64 | Play-by-play flag: 1 = play-by-play exists (2017-18 and later in the samples), 2 = none (the 2009-10 sample answers empty pages). |
| `is_report` | Int64 | Game-report flag: 1 = a game report page exists (every sample). |
| `match_order` | Int64 | Sequence number of the game within its season and match type (1-based). |
| `match_type_id` | Int64 | Match type id (1 regular season, 2 playoffs, 3 preseason, 4 all-star, 9 relegation; see getMatchType). |
| `minutes` | Int64 | Regulation length of the game in minutes: 40 (4 x 10) in the 2025-26 sample, 48 (4 x 12) in the 2009-10 and 2017-18 samples. |
| `quarter` | Int64 | Number of periods played (4 = no overtime; 5 and up would be overtime periods). |
| `round` | Int64 | Round number within the match type (1-based). |
| `round_desc` | String | Unobserved: null in every sample, so no meaning is claimed beyond its name (round description). |
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `schedule_num` | Int64 | Unobserved: null in every sample on this route (the game count is filled on schedules/month). Typed Int64 like the family's other routes. |
| `schedule_time` | String | Tip-off as 'YYYY-MM-DD HH:MM:SS' (China local time, no zone). |
| `schedule_type_name` | String | Round label (Chinese), e.g. 常规赛第1轮 = regular season, round 1. |
| `season` | Int64 | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `seconds` | Float64 | Unobserved: null in every sample (the clock seconds of a live game, by its name). Typed Float64 like the family's other routes. |
| `status` | Int64 | Game status: 4 = finished (every sampled game). 1 = not started and 2 = in progress are inferred from the site's polling logic; 3 is unobserved. See the code table. |
| `visiting_team_id` | Int64 | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `visiting_team_logo` | String | Absolute URL of the visiting team logo (300x300 png). |
| `visiting_team_lose` | Int64 | Visiting team's cumulative losses after this game. An integer (the API sends it as digit text). |
| `visiting_team_name` | String | Visiting team name (Chinese) as of the game's season; changes with the sponsor. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `visiting_team_score` | Int64 | Visiting team final points. |
| `visiting_team_win` | Int64 | Visiting team's cumulative wins after this game. An integer (the API sends it as digit text). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_schedule_day-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_schedule_day-example}

```python
cba_schedule_day(day='2025-12-12')
```

## cba_period_scores

Points each side scored in each period of a finished game (up to seven periods).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/schedule-quarters/getQuarterInfo`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/schedule-quarters/getQuarterInfo' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` | body |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | String | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `period` | Int64 | Period number: 1-4 regulation, 5 and up overtime (the API holds up to seven periods). |
| `points` | Int64 | Points the side scored IN this period (not cumulative); overtime periods included. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_period_scores-example}

```python
cba_period_scores(schedule_id=100105799)
```

## cba_best_player

The best player of each side in the game report.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/bestPlayer/{schedule_id}`

**Valid URL:** [https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/bestPlayer/100105799](https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/bestPlayer/100105799)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_id` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_best_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | String | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `assists` | Int64 | Assists by the player in this game. |
| `blocked` | Int64 | Blocks by the player in this game. |
| `fouls` | Int64 | Personal fouls by the player in this game. |
| `free_throw_str` | String | Free throws as the string 'made/attempted'. |
| `minutes` | Int64 | Minutes played (whole minutes). |
| `number` | String | Jersey number as a string. |
| `player_img` | String | Absolute URL of the player's photo; null when there is none. |
| `player_name` | String | Full player name (Chinese, may include the first name: 贾卡尔-桑普森). Null on some rows (the short name is in playerNameStr). |
| `player_name_str` | String | Short display name (Chinese surname form: 桑普森). |
| `points` | Int64 | Points scored by the player in this game. |
| `real_point_rate` | Float64 | 真实命中率 true-shooting percentage. A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |
| `rebounds` | Int64 | Total rebounds by the player in this game. |
| `steals` | Int64 | Steals by the player in this game. |
| `three_points_str` | String | Three-point field goals as the string 'made/attempted'. |
| `turn_share` | Float64 | 回合占有率 possession share (see advance-count turnShare). A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |
| `turnovers` | Int64 | Turnovers by the player in this game. |
| `two_points_str` | String | Two-point field goals as the string 'made/attempted' ('3/5'). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_best_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_best_player-example}

```python
cba_best_player(schedule_id=100105799)
```

## cba_best_stats

Single-category leaders of each side in the game report.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/bestDataQuota/{schedule_id}`

**Valid URL:** [https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/bestDataQuota/100105799](https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/bestDataQuota/100105799)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_id` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_best_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | String | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `ctype` | String | Statistic the row leads: points, assists, steals, rebounds, blocked (five rows per side in every sample). See the code table. |
| `cvalue` | String | The leader's value of ctype, as a STRING ('16'). |
| `player_id` | Int64 | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. A STRING here ('100005271'), an integer on every box route. |
| `player_name` | String | Player name (Chinese). Null on rows whose name the report does not repeat (the same player's earlier row carries it). |
| `player_no` | String | Jersey number as a string. Null where playerName is null. |
| `seconds` | Int64 | The leader's time on court in SECONDS. |
| `team_id` | Int64 | Unobserved: null in every sample (the side is the enclosing list). |
| `type` | Int64 | Side flag: 0 on home rows, 2 on visiting rows (differs from advance-count, where the visiting side is 1). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_best_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_best_stats-example}

```python
cba_best_stats(schedule_id=100105799)
```

## cba_rosters

Registered rosters of both sides with player bio fields (the season registration, not the game box).

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/schedules/getPlayersByScheduleId/{schedule_id}`

**Valid URL:** [https://data-server.cbaleague.com/api/schedules/getPlayersByScheduleId/100105799](https://data-server.cbaleague.com/api/schedules/getPlayersByScheduleId/100105799)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_id` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_rosters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | String | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `age` | Int64 | Age in years as registered (0 on every sampled row: not maintained). |
| `birth_date` | String | Date of birth 'YYYY-MM-DD'. A public player bio field of the roster (kept; officials' and coaches' dates would be redacted by the capture). |
| `body_data` | String | JSON string of physical-testing results as the league stores it (wingspan, standing reach, vertical reach, hand size); null where not tested. |
| `cn_alias` | String | Player short name (Chinese). |
| `cn_alias_show` | String | Display name (Chinese); '---' for a placeholder row. |
| `cn_name` | String | Player full name (Chinese; may include a first-name form for foreigners). |
| `created_date` | String | Audit field: row creation time 'YYYY-MM-DD HH:MM:SS'. |
| `first_name` | String | Given name in Latin letters as registered, in capitals (e.g. 'CAI S.L.S.'). |
| `height` | Int64 | Height in cm (integer). |
| `id` | Int64 | Internal row id of the (player, team, season) registration, a 19-digit integer beyond 2^53: read it as a string/int64, never as a JS number. Not the playerId. |
| `is_captain` | Boolean | Team captain flag (false on every sampled row). |
| `is_delete` | Boolean | Soft-delete flag (false on every sampled row). |
| `is_injury` | Boolean | Injury flag (false on every sampled row). |
| `is_three` | Int64 | Flag, 2 on every sampled row; meaning unverified. |
| `last_modified_date` | String | Audit field: last modification time; null on rows never modified. |
| `last_name` | String | Family name in Latin letters; '' when none, and null on some older rows. |
| `match_type_id` | Int64 | Match type id (1 = regular season) of the registration. |
| `nationality` | String | Nationality (Chinese country name; see the nationality code table for every value seen and its English name). Chinese country names seen: 中国 = China; 美国 = United States; 加拿大 = Canada; 法国 = France; 中国台湾 = Chinese Taipei; 伊朗 = Iran; 尼日利亚 = Nigeria; 约旦 = Jordan. |
| `number` | String | Jersey number as a string. |
| `player_cba_id` | Int64 | A registration id from the league's own system (e.g. 502672); 0 when unset (27 of 101 sampled rows). Not the playerId (unverified beyond that). |
| `player_id` | Int64 | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `player_pic` | String | Absolute URL of the player's photo; null when there is none. |
| `position` | String | Position code as registered: SG, SF, PG, PF, C and their '/' pairs, or the coarse G (guard) / F (forward) on older rows (see positionDescription). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `position_description` | String | Position label (Chinese). Eras differ: coarse 后卫 (guard) / 前锋 (forward) / 中锋 (center) in older seasons, five-way 控球后卫 (point guard) / 得分后卫 (shooting guard) / 小前锋 (small forward) / 大前锋 (power forward) / 中锋 (center) in newer ones; dual positions are joined with '/' (e.g. 小前锋/大前锋). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `position_string` | String | Unobserved: null in every sample. |
| `season` | Int64 | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `season_id` | Int64 | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. Same value as season. |
| `season_name` | String | Unobserved: null in every sample. |
| `season_year` | String | Unobserved: null in every sample. |
| `team_cba_id` | Int64 | teamId again as an integer (29128); meaning unverified beyond equalling team_id in the sample. |
| `team_cn_alias` | String | Team short name (Chinese) in that season. |
| `team_cn_name` | String | Team full legal name (Chinese), e.g. 浙江广厦雄狮俱乐部浙商证券篮球队 'Zhejiang Guangsha Lions Club Zheshang Securities basketball team'. |
| `team_en_alias` | String | Team code or English city name as registered: a three-letter code on recent rows ('ZGL', 'SBW'), an English city name on older ones ('Guangdong', 'Sichuan'), '' when none. |
| `team_en_name` | String | Team English name as the league registers it (the sample reads 'GUANG SHA'). |
| `team_id` | Int64 | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `weight` | Int64 | Weight in kg (integer). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_rosters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_rosters-example}

```python
cba_rosters(schedule_id=100105799)
```

## cba_report_status

Whether the game report page exists for the game.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReportStatues/{schedule_id}`

**Valid URL:** [https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReportStatues/100105799](https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReportStatues/100105799)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_id` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_report_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `is_report` | Boolean | True when the game report page exists (equals schedules/day isReport == 1 in every sample). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_report_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_report_status-example}

```python
cba_report_status(schedule_id=100105799)
```

## cba_pbp

Play-by-play of a finished game, every period merged, oldest first (Chinese text; no player or team ids).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/schedule-eventses/filterPage`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/schedule-eventses/filterPage?startNum=0' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` | body |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |
| `quarterNum` | `quarter_num` | body |  |  | `Y` | Optional period number. Omitted = every period (the API serves one period per request; the runtime fetches them all and checks the set). |

### Returns {#cba_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | Int64 | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `bt_textual_format` | String | Second copy of the description; equal to textualFormat on every sampled row (unverified difference). |
| `color` | String | Hex colour of the acting team (homeColor or visitingColor). |
| `distance` | String | Unobserved: null in every sample (a shot distance slot). The API has NO shot x/y coordinates. |
| `home_color` | String | Home team colour '#RRGGBB'. |
| `home_score` | Int64 | Home team points after the play. NOT monotone row by row: a substitution row can carry a stale score (2017 game, Q1 0:40); the game's final score is the maximum. |
| `minutes` | Int64 | Game clock, minutes REMAINING in the period (counts down: 10 or 12 at the start, depending on the era). |
| `player_cn_names` | String | Names (Chinese surnames/short names) of the players in textualFormat, in order of mention (shooter or fouler first, then assister, rebounder or fouled player). 0-3 names. There are NO player ids in the play-by-play. |
| `player_fouls` | String | Unobserved: null in every sample (a player's fouls slot). |
| `player_score` | String | Unobserved: null in every sample (a player's points slot). |
| `quarter_num` | Int64 | Period of the row (1-4; 5+ would be overtime). Equals the request's quarterNum on every row. |
| `seconds` | Int64 | Game clock, seconds remaining within the minute. |
| `team_cn_alias1` | String | Unobserved: null in every sample (a team-name slot). |
| `team_cn_alias2` | String | Unobserved: null in every sample (a team-name slot). |
| `team_cn_alias3` | String | Unobserved: null in every sample (a team-name slot). |
| `textual_format` | String | Play description sentence (Chinese) naming the players; the phrase glossary translates every form seen. Team names inside it can be a different era's name than the game header (a 2025-26 text reads 浙江方兴渡队). Sentence forms ({P} a player, {T} a team): {T}队换人，{P}换下了{P} = {T} substitution, {P} replaces {P}; {P}三分跳投不中，{P}获得防守篮板 = {P} three-point jump shot, misses, {P} gets the defensive rebound; {P}带球上篮得分 = {P} layup (driving), scores; {P}两罚，第二罚命中 = {P} takes two free throws, free throw 2 made; {P}在对方投篮时犯规 (造成了犯规) = {P} shooting foul on the opponent's shot (drawn by an unnamed player); {P}接到{P}的助攻，跳投，三分入网 = {P} receives an assist from {P}, jump shot, three-pointer made; the full glossary is in cba_glossary.yaml. |
| `type` | Int64 | Team the play belongs to: 1 = home, 2 = visiting (checked on all 667 rows of two games: color equals homeColor when 1 and visitingColor when 2). See the code table. |
| `visiting_color` | String | Visiting team colour '#RRGGBB'. |
| `visitor_score` | Int64 | Visiting team points after the play (same caveat as homeScore). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_pbp-example}

```python
cba_pbp(schedule_id=100105799)
```
