---
title: "TPBL — TPBL API (api.tpbl.basketball, Taiwan) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "TPBL — TPBL API (api.tpbl.basketball, Taiwan) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# TPBL — TPBL API (api.tpbl.basketball, Taiwan) — Other

## tpbl_seasons

Every TPBL season with its TPBL and GoHoops ids, label, status and date window.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/seasons`

**Valid URL:** [https://api.tpbl.basketball/api/seasons](https://api.tpbl.basketball/api/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#tpbl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year, read from the label '2024-2025 賽季' (賽季 = season): 2025 for 2024-25. |
| `season_id` | integer | TPBL season id (key space: TPBL seasons on api.tpbl.basketball; 1 = 2024-25, 2 = 2025-26, 3 = 2026-27): the {season_id} of the season game list and, equal to it, the {event_id} of /events/{event_id}/*. Not the GoHoops season id. |
| `gohoops_season_id` | integer | GoHoops season id (key space: GoHoops seasons, the id system the API echoes beside its own; 101 = 2024-25, 213 = 2025-26, 275 = 2026-27). Not the TPBL season id. |
| `name` | character | Season label in Traditional Chinese: '2024-2025 賽季' = 'season 2024-2025'. |
| `status` | character | Season status: COMPLETED or IN_PROGRESS. |
| `started_at` | character | Season start, 'YYYY-MM-DD 00:00:00' text. Preseason games can be played before it (the 2026-27 start is 2026-10-17, its first games were played 2026-10-09). |
| `ended_at` | character | Season end, 'YYYY-MM-DD 00:00:00' text (the planned end for a season in progress). |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_seasons-example}

```python
tpbl_seasons()
```

## tpbl_divisions

The divisions (preseason, regular season, play-in, playoffs, finals) of one season.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/events/{season}/divisions`

**Valid URL:** [https://api.tpbl.basketball/api/events/2026/divisions](https://api.tpbl.basketball/api/events/2026/divisions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season; 2025 is the first). The API keys a season by its own small integer (1, 2, 3 = 2024-25, 2025-26, 2026-27; the event id of /events/{id}/* is the same number): ``tpbl_runtime`` reads /seasons once and sends that id, so the example URL shows the year. A year the list lacks raises NoDataError. |

### Returns {#tpbl_divisions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = the 2025-26 season), the year asked for. Null when the body was parsed without the request stamp. |
| `season_id` | integer | TPBL season id (key space: TPBL seasons on api.tpbl.basketball; 1 = 2024-25, 2 = 2025-26, 3 = 2026-27): the {season_id} of the season game list and, equal to it, the {event_id} of /events/{event_id}/*. Not the GoHoops season id. Null when the body was parsed without the request stamp. |
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `gohoops_division_id` | integer | GoHoops division id (key space: GoHoops divisions; e.g. 64 = the 2024-25 preseason, 65 = the 2024-25 regular season). Not the TPBL division id. |
| `gohoops_event_id` | integer | GoHoops event id (key space: GoHoops events, one per TPBL season; 319 = 2024-25, 490 = 2025-26, 613 = 2026-27). Not the TPBL season id and not the GoHoops season id. |
| `name` | character | Division name (Traditional Chinese: 熱身賽 = preseason (warm-up games), 例行賽 = regular season, 季後挑戰賽 = postseason challenge (play-in round), 季後賽 = playoffs, 冠軍賽 = finals). |
| `status` | character | Division status: COMPLETED or IN_PROGRESS. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_divisions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_divisions-example}

```python
tpbl_divisions(season=2026)
```

## tpbl_teams

The teams of one season (bracket placeholders flagged).

**Endpoint URL:** `GET https://api.tpbl.basketball/api/events/{season}/teams`

**Valid URL:** [https://api.tpbl.basketball/api/events/2026/teams](https://api.tpbl.basketball/api/events/2026/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season; 2025 is the first). The API keys a season by its own small integer (1, 2, 3 = 2024-25, 2025-26, 2026-27; the event id of /events/{id}/* is the same number): ``tpbl_runtime`` reads /seasons once and sends that id, so the example URL shows the year. A year the list lacks raises NoDataError. |

### Returns {#tpbl_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = the 2025-26 season), the year asked for. Null when the body was parsed without the request stamp. |
| `season_id` | integer | TPBL season id (key space: TPBL seasons on api.tpbl.basketball; 1 = 2024-25, 2 = 2025-26, 3 = 2026-27): the {season_id} of the season game list and, equal to it, the {event_id} of /events/{event_id}/*. Not the GoHoops season id. Null when the body was parsed without the request stamp. |
| `team_id` | integer | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `gohoops_team_id` | integer | GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). |
| `name` | character | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `is_real_team` | logical | True for a real club; false for a bracket placeholder (67 '高順位隊伍' = higher-seed team, 68 '低順位隊伍' = lower-seed team in 2025-26). |
| `status` | character | Team status. 'ACTIVE' is the only value seen in every sampled route; the full set of statuses is unknown. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_teams-example}

```python
tpbl_teams(season=2026)
```

## tpbl_games

Every game of one season, all divisions, played and scheduled, with teams, venue, status and score.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/seasons/{season}/games`

**Valid URL:** [https://api.tpbl.basketball/api/seasons/2026/games](https://api.tpbl.basketball/api/seasons/2026/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season; 2025 is the first). The API keys a season by its own small integer (1, 2, 3 = 2024-25, 2025-26, 2026-27; the event id of /events/{id}/* is the same number): ``tpbl_runtime`` reads /seasons once and sends that id, so the example URL shows the year. A year the list lacks raises NoDataError. |
| `team_id` | `team_id` |  |  | `Y` | TPBL team id (2-8): keep only that team's games (the API's own filter). Cannot be combined with ``verify``. |
| `verify` | `verify` |  |  | `Y` | Default False. Cross-check the list for completeness: every division's completed games must equal the matches of its own standings (one extra request per division; a division whose standings are empty is checked against the team-filtered lists instead). A mismatch raises AssetFetchError. |

### Returns {#tpbl_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = the 2025-26 season), the year asked for. Null when the body was parsed without the request stamp. |
| `season_id` | integer | TPBL season id (key space: TPBL seasons on api.tpbl.basketball; 1 = 2024-25, 2 = 2025-26, 3 = 2026-27): the {season_id} of the season game list and, equal to it, the {event_id} of /events/{event_id}/*. Not the GoHoops season id. Null when the body was parsed without the request stamp. |
| `game_id` | integer | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `gohoops_game_id` | integer | GoHoops game id (key space: GoHoops games; 1242 for TPBL game 6). Not the TPBL game id. |
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `gohoops_division_id` | integer | GoHoops division id (key space: GoHoops divisions; e.g. 64 = the 2024-25 preseason, 65 = the 2024-25 regular season). Not the TPBL division id. |
| `gohoops_season_id` | integer | GoHoops season id (key space: GoHoops seasons, the id system the API echoes beside its own; 101 = 2024-25, 213 = 2025-26, 275 = 2026-27). Not the TPBL season id. Read from the home team. |
| `gohoops_event_id` | integer | GoHoops event id (key space: GoHoops events, one per TPBL season; 319 = 2024-25, 490 = 2025-26, 613 = 2026-27). Not the TPBL season id and not the GoHoops season id. Read from the home team. |
| `code` | character | Game code inside the division: the game number as text in the regular season ('1' ... '42'), a series label in the playoffs ('AG1' = series A game 1, 'G7' = finals game 7), '' for preseason games. |
| `status` | character | Game status. Observed: COMPLETED (finished) and NOT_STARTED (scheduled). A game in progress has is_live true. |
| `is_live` | logical | True while the game is being played; false for finished and scheduled games. |
| `periods` | integer | Number of quarters played: 4 for a regulation game, 5 for one overtime, 6 for two; 0 before tip-off. |
| `game_date` | character | Game date (Taiwan local date). |
| `game_day_of_week` | character | Traditional Chinese weekday in parentheses: (一) Mon, (二) Tue, (三) Wed, (四) Thu, (五) Fri, (六) Sat, (日) Sun. |
| `game_time` | character | Scheduled tip-off 'HH:MM:SS'. Taiwan local date and time (UTC+8 by every tip-off seen; the API sends no zone). |
| `tipoff` | character | Scheduled tip-off as a naive timestamp (no zone). Taiwan local date and time (UTC+8 by every tip-off seen; the API sends no zone). |
| `venue` | character | Venue name in Traditional Chinese (新莊體育館 = Xinzhuang Gymnasium, 和平籃球館 = Heping Basketball Gymnasium, 臺中洲際迷你蛋 = Taichung Intercontinental Mini Dome, 新竹縣體育館 = Hsinchu County Gymnasium, 高雄巨蛋 = Kaohsiung Arena, 桃園巨蛋 = Taoyuan Arena). |
| `attendance` | integer | Attendance as an integer, parsed from the API's visitors string; null before the game, when the API sends none or on a postponed game. |
| `home_team_id` | integer | Home TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `home_gohoops_team_id` | integer | Home GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). |
| `home_gohoops_squad_id` | integer | Home GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `home_team` | character | Home team name. Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `home_alt_name` | character | Home team slug. Lowercase English slug of the team: aquas, dreamers, lioneers, leopards, dea, kings, mars. |
| `home_score` | integer | Home team final score (points scored); null before tip-off. |
| `away_team_id` | integer | Away TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `away_gohoops_team_id` | integer | Away GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). |
| `away_gohoops_squad_id` | integer | Away GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `away_team` | character | Away team name. Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `away_alt_name` | character | Away team slug. Lowercase English slug of the team: aquas, dreamers, lioneers, leopards, dea, kings, mars. |
| `away_score` | integer | Away team final score (points scored); null before tip-off. |
| `recap_url` | character | YouTube recap URL; null when the game has none (the list can omit it, the game detail carries it). |
| `live_stream_url` | character | YouTube live-stream URL of the game; null when the API omits it. |
| `judgement_report_url` | character | URL of the referees' judgement report (a Google Drive file); null when none. |
| `ticket_url` | character | URL of the ticket page; null on most rows. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_games-example}

```python
tpbl_games(season=2026)
```

## tpbl_standings

League table of one division: rank, record, games behind and streak.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/divisions/{division_id}/games/standings`

**Valid URL:** [https://api.tpbl.basketball/api/divisions/2/games/standings](https://api.tpbl.basketball/api/divisions/2/games/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `division_id` | `division_id` |  | `Y` |  | Required. TPBL division id (one competition phase of one season: 1, 2, 5, 6, 7 = 2024-25 preseason, regular season, playoffs, postseason challenge, finals; 8, 9, 11, 12, 35 = 2025-26; 655, 656 = 2026-27); ``division_id`` in tpbl_divisions. Not the GoHoops division id. |

### Returns {#tpbl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `rank` | integer | Rank in the division, 1-based. |
| `team_id` | integer | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `gohoops_team_id` | integer | GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). |
| `gohoops_squad_id` | integer | GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `team` | character | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `alt_name` | character | Lowercase English slug of the team: aquas, dreamers, lioneers, leopards, dea, kings, mars. |
| `won` | integer | Games won in the division so far. |
| `lost` | integer | Games lost in the division so far. |
| `drawn` | integer | Games drawn (always 0 in basketball). |
| `games_behind` | numeric | Games behind the leader (0 for the leader); a whole number in every captured division, read as a float so half games cannot break the dtype. |
| `streak` | character | Current streak: 'W' + count for wins, 'L' + count for losses (W2 = two straight wins). |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_standings-example}

```python
tpbl_standings(division_id=2)
```

## tpbl_standings_against

Head-to-head record of every team against every other team in one division.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/divisions/{division_id}/games/standings`

**Valid URL:** [https://api.tpbl.basketball/api/divisions/2/games/standings](https://api.tpbl.basketball/api/divisions/2/games/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `division_id` | `division_id` |  | `Y` |  | Required. TPBL division id (one competition phase of one season: 1, 2, 5, 6, 7 = 2024-25 preseason, regular season, playoffs, postseason challenge, finals; 8, 9, 11, 12, 35 = 2025-26; 655, 656 = 2026-27); ``division_id`` in tpbl_divisions. Not the GoHoops division id. |

### Returns {#tpbl_standings_against-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `team_id` | integer | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. The ranked team. |
| `opponent_team_id` | integer | TPBL team id of the opponent (franchise, 2-8). Joins team_id. |
| `opponent_gohoops_team_id` | integer | GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). The opponent. |
| `opponent` | character | Opponent name. Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `won` | integer | Games the ranked team won against this opponent in the division. |
| `lost` | integer | Games the ranked team lost against this opponent in the division. |
| `drawn` | integer | Games drawn against this opponent (always 0). |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_standings_against-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_standings_against-example}

```python
tpbl_standings_against(division_id=2)
```

## tpbl_game

Header of one finished game: teams with scores, venue, status, quarters, tip-off, attendance and links.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/games/{game_id}`

**Valid URL:** [https://api.tpbl.basketball/api/games/6](https://api.tpbl.basketball/api/games/6)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served. |

### Returns {#tpbl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = the 2025-26 season), the year asked for. Null when the body was parsed without the request stamp. |
| `season_id` | integer | TPBL season id (key space: TPBL seasons on api.tpbl.basketball; 1 = 2024-25, 2 = 2025-26, 3 = 2026-27): the {season_id} of the season game list and, equal to it, the {event_id} of /events/{event_id}/*. Not the GoHoops season id. Null when the body was parsed without the request stamp. |
| `game_id` | integer | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `gohoops_game_id` | integer | GoHoops game id (key space: GoHoops games; 1242 for TPBL game 6). Not the TPBL game id. |
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `gohoops_division_id` | integer | GoHoops division id (key space: GoHoops divisions; e.g. 64 = the 2024-25 preseason, 65 = the 2024-25 regular season). Not the TPBL division id. |
| `gohoops_season_id` | integer | GoHoops season id (key space: GoHoops seasons, the id system the API echoes beside its own; 101 = 2024-25, 213 = 2025-26, 275 = 2026-27). Not the TPBL season id. Read from the home team. |
| `gohoops_event_id` | integer | GoHoops event id (key space: GoHoops events, one per TPBL season; 319 = 2024-25, 490 = 2025-26, 613 = 2026-27). Not the TPBL season id and not the GoHoops season id. Read from the home team. |
| `code` | character | Game code inside the division: the game number as text in the regular season ('1' ... '42'), a series label in the playoffs ('AG1' = series A game 1, 'G7' = finals game 7), '' for preseason games. |
| `status` | character | Game status. Observed: COMPLETED (finished) and NOT_STARTED (scheduled). A game in progress has is_live true. |
| `is_live` | logical | True while the game is being played; false for finished and scheduled games. |
| `periods` | integer | Number of quarters played: 4 for a regulation game, 5 for one overtime, 6 for two; 0 before tip-off. |
| `game_date` | character | Game date (Taiwan local date). |
| `game_day_of_week` | character | Traditional Chinese weekday in parentheses: (一) Mon, (二) Tue, (三) Wed, (四) Thu, (五) Fri, (六) Sat, (日) Sun. |
| `game_time` | character | Scheduled tip-off 'HH:MM:SS'. Taiwan local date and time (UTC+8 by every tip-off seen; the API sends no zone). |
| `tipoff` | character | Scheduled tip-off as a naive timestamp (no zone). Taiwan local date and time (UTC+8 by every tip-off seen; the API sends no zone). |
| `venue` | character | Venue name in Traditional Chinese (新莊體育館 = Xinzhuang Gymnasium, 和平籃球館 = Heping Basketball Gymnasium, 臺中洲際迷你蛋 = Taichung Intercontinental Mini Dome, 新竹縣體育館 = Hsinchu County Gymnasium, 高雄巨蛋 = Kaohsiung Arena, 桃園巨蛋 = Taoyuan Arena). |
| `attendance` | integer | Attendance as an integer, parsed from the API's visitors string; null before the game, when the API sends none or on a postponed game. |
| `home_team_id` | integer | Home TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `home_gohoops_team_id` | integer | Home GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). |
| `home_gohoops_squad_id` | integer | Home GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `home_team` | character | Home team name. Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `home_alt_name` | character | Home team slug. Lowercase English slug of the team: aquas, dreamers, lioneers, leopards, dea, kings, mars. |
| `home_score` | integer | Home team final score (points scored); null before tip-off. |
| `away_team_id` | integer | Away TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `away_gohoops_team_id` | integer | Away GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938). |
| `away_gohoops_squad_id` | integer | Away GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `away_team` | character | Away team name. Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `away_alt_name` | character | Away team slug. Lowercase English slug of the team: aquas, dreamers, lioneers, leopards, dea, kings, mars. |
| `away_score` | integer | Away team final score (points scored); null before tip-off. |
| `recap_url` | character | YouTube recap URL; null when the game has none (the list can omit it, the game detail carries it). |
| `live_stream_url` | character | YouTube live-stream URL of the game; null when the API omits it. |
| `judgement_report_url` | character | URL of the referees' judgement report (a Google Drive file); null when none. |
| `ticket_url` | character | URL of the ticket page; null on most rows. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_game-example}

```python
tpbl_game(game_id=6)
```

## tpbl_referees

The officiating crew of one finished game, chief referee first (names only).

**Endpoint URL:** `GET https://api.tpbl.basketball/api/games/{game_id}`

**Valid URL:** [https://api.tpbl.basketball/api/games/6](https://api.tpbl.basketball/api/games/6)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served. |

### Returns {#tpbl_referees-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `sequence` | integer | Position in the crew as the API lists it: 1 = the chief referee. |
| `referee_id` | integer | TPBL referee id (key space: TPBL referees on api.tpbl.basketball). Not the GoHoops referee id. |
| `gohoops_referee_id` | integer | GoHoops referee id (key space: GoHoops referees). Not the TPBL referee id. |
| `name` | character | Referee name in Traditional Chinese. Names only: no contact or birth data is published. |
| `role` | character | Crew role enum: BASKETBALL_CHIEF_REFEREE = crew chief, BASKETBALL_FIRST_ASSISTANT_REFEREE = first assistant (umpire 1), BASKETBALL_SECOND_ASSISTANT_REFEREE = second assistant (umpire 2). |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_referees-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_referees-example}

```python
tpbl_referees(game_id=6)
```

## tpbl_roster

The registered roster of one team in one division (birth dates omitted).

**Endpoint URL:** `GET https://api.tpbl.basketball/api/divisions/{division_id}/teams/{team_id}/players`

**Valid URL:** [https://api.tpbl.basketball/api/divisions/2/teams/4/players](https://api.tpbl.basketball/api/divisions/2/teams/4/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `division_id` | `division_id` |  | `Y` |  | Required. TPBL division id (one competition phase of one season: 1, 2, 5, 6, 7 = 2024-25 preseason, regular season, playoffs, postseason challenge, finals; 8, 9, 11, 12, 35 = 2025-26; 655, 656 = 2026-27); ``division_id`` in tpbl_divisions. Not the GoHoops division id. |
| `team_id` | `team_id` |  | `Y` |  | Required. TPBL team id (the franchise, 2-8, stable across seasons); ``team_id`` in tpbl_teams. Not the GoHoops id. |

### Returns {#tpbl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `team_id` | integer | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `team` | character | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `player_id` | integer | TPBL player id (key space: TPBL players; one per person across seasons and teams): the {player_id} of the player routes. Not the GoHoops person id and not the roster id. |
| `gohoops_person_id` | integer | GoHoops person id (key space: GoHoops persons; one per human). Not the TPBL player id and not the roster id. |
| `gohoops_roster_id` | integer | GoHoops roster id (key space: GoHoops rosters = one player registered with one squad in one season; it changes when the player changes season or team). The key of the box score players: join tpbl_box_players to tpbl_roster on it. Not the TPBL player id and not the person id. |
| `gohoops_squad_id` | integer | GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `gohoops_event_id` | integer | GoHoops event id (key space: GoHoops events, one per TPBL season; 319 = 2024-25, 490 = 2025-26, 613 = 2026-27). Not the TPBL season id and not the GoHoops season id. |
| `gohoops_season_id` | integer | GoHoops season id (key space: GoHoops seasons, the id system the API echoes beside its own; 101 = 2024-25, 213 = 2025-26, 275 = 2026-27). Not the TPBL season id. |
| `player` | character | Player name (Traditional Chinese, or a transliteration for foreign players). |
| `alt_name` | character | Romanized name (e.g. 'Chian Kuang-Chien'); '' when none. |
| `number` | character | Jersey number as text (so '0' and '00' stay distinct). |
| `position` | character | Playing position as a PascalCase enum: PointGuard, ShootingGuard, SmallForward, PowerForward, Center, and the combinations PointGuardAndShootingGuard, ShootingGuardAndSmallForward, SmallForwardAndPowerForward and PowerForwardAndCenter; '' when unset. |
| `height_cm` | integer | Height in centimetres. |
| `weight_kg` | numeric | Weight in kilograms (a number; fractions possible). |
| `nationality` | character | Nationality text: 'Local' for a Taiwan-born player, '' when unset. |
| `national_identity` | character | Player category: 'Local' (Taiwanese), '' when unset (foreign and naturalized categories use other strings). |
| `national_identity_detail` | character | Free-text detail on the category; '' in every sample. |
| `registered` | logical | True when the player is registered with the squad. |
| `is_listing` | logical | True when the player is shown on the team's public roster page. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_roster-example}

```python
tpbl_roster(division_id=2, team_id=4)
```

## tpbl_team_shots

Every field-goal attempt of one team in one division with its court position (optionally per quarter).

**Endpoint URL:** `GET https://api.tpbl.basketball/api/divisions/{division_id}/teams/{team_id}/shooting-spots`

**Valid URL:** [https://api.tpbl.basketball/api/divisions/2/teams/4/shooting-spots](https://api.tpbl.basketball/api/divisions/2/teams/4/shooting-spots)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `division_id` | `division_id` |  | `Y` |  | Required. TPBL division id (one competition phase of one season: 1, 2, 5, 6, 7 = 2024-25 preseason, regular season, playoffs, postseason challenge, finals; 8, 9, 11, 12, 35 = 2025-26; 655, 656 = 2026-27); ``division_id`` in tpbl_divisions. Not the GoHoops division id. |
| `team_id` | `team_id` |  | `Y` |  | Required. TPBL team id (the franchise, 2-8, stable across seasons); ``team_id`` in tpbl_teams. Not the GoHoops id. |
| `rounds` | `rounds` |  |  | `Y` | Quarter filter: a quarter number or a list of them (1-4, 5 and 6 for overtimes); the runtime sends it as the API's repeatable ``rounds[]``. Omit for all quarters. |

### Returns {#tpbl_team_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `division_id` | integer | TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, 8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops division id. |
| `team_id` | integer | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `rounds` | character | The quarter filter asked for, comma-separated ('1' or '1,2'); null when all quarters were asked. |
| `is_made` | logical | True when the shot went in. |
| `point_x` | integer | Shot position in MILLIMETRES, 0-28000 along the court length (a 28 m court). The hoops sit at x = 1575 (offense_basket 0) and x = 26425 (offense_basket 1), y = 7500. Verified on 2,971 shots of one team: the made rate within 1.2 m of the hoop is 62-71 %. Which baseline is x = 0 is unverified. |
| `point_y` | integer | Shot position in MILLIMETRES, 0-15000 across the court (a 15 m court). The hoops sit at x = 1575 (offense_basket 0) and x = 26425 (offense_basket 1), y = 7500. The direction of y is unverified. |
| `offense_basket` | integer | Basket the team attacked: 0 = the hoop at x = 1575, 1 = the hoop at x = 26425. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_team_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_team_shots-example}

```python
tpbl_team_shots(division_id=2, team_id=4)
```
