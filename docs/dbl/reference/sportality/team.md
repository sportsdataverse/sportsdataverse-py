# Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Team

> Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Team — function reference in sdv-py, the SportsDataverse Python package.

## sportality_team

One team.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/teams/{team_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/teams/rfo82vr830](https://www.basketligaen.dk/api/sports-v2/teams/rfo82vr830)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid: `homeTeamInfo.uuid` / `awayTeamInfo.uuid` in the schedule or `teamList[].uuid`. Key space: platform team uuids (not the numeric Genius team id). |

### Returns {#sportality_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `names_short` | String | Short name of the team. |
| `names_long` | String | Long name of the team. |
| `names_full` | String | Full name of the team. |
| `names_code_site` | String | Team code as the club's own site shows it. |
| `names_short_site` | String | Short team name as the club's own site shows it. |
| `names_full_site` | String | Full team name as the club's own site shows it. |
| `names_long_site` | String | Long team name as the club's own site shows it. |
| `info` | String | Detail record (shape depends on the route; see the child columns). |
| `uuid` | String | Platform uuid of this record (10-character lowercase key space of its record type; joins the route that serves it). |
| `nationality` | String | ISO 3166-1 alpha-2 country code. |
| `arena_name` | String | Name of the arena. |
| `public_url` | String | The club's own website. |
| `icon` | String | Team logo URL (SVG on sportality.cdn.s8y.se). |
| `owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `site_display_name` | String | Team name as the league site displays it. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_team-example}

```python
sportality_team(team_uuid='rfo82vr830')
```

## sportality_team_played_games

A team's recent games (current season view).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/played-games/{team_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/played-games/rfo82vr830](https://www.basketligaen.dk/api/sports-v2/played-games/rfo82vr830)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid: `homeTeamInfo.uuid` / `awayTeamInfo.uuid` in the schedule or `teamList[].uuid`. Key space: platform team uuids (not the numeric Genius team id). |

### Returns {#sportality_team_played_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Game date, 'D mon YY' in English ('10 oct 26'). |
| `time` | String | Local start time 'HH:MM' (Europe/Copenhagen). |
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `start_date_time` | String | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | String | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `post-game` = played (schedule, day views). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | String | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | String | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `WIN` = the team won. |
| `home_team_uuid` | String | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | String | Three-letter code of the home team. |
| `home_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | String | Short name of the team. |
| `home_team_names_long` | String | Long name of the team. |
| `home_team_names_full` | String | Full name of the team. |
| `home_team_names_code_site` | String | Team code as the club's own site shows it. |
| `home_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `home_team_score` | Int64 | Home points; null before the game is played. |
| `home_team_icon` | String | Home team logo URL. |
| `away_team_status` | String | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `WIN` = the team won. |
| `away_team_uuid` | String | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | String | Three-letter code of the away team. |
| `away_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | String | Short name of the team. |
| `away_team_names_long` | String | Long name of the team. |
| `away_team_names_full` | String | Full name of the team. |
| `away_team_names_code_site` | String | Team code as the club's own site shows it. |
| `away_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `away_team_score` | Int64 | Away points; null before the game is played. |
| `away_team_icon` | String | Away team logo URL. |
| `venue_uuid` | String | Venue uuid (platform key space). |
| `venue_name` | String | Venue name; absent when unknown. |
| `series_uuid` | String | Series uuid (platform key space). |
| `series_code` | String | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | String | Series display name (Basketligaen). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_team_played_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_team_played_games-example}

```python
sportality_team_played_games(team_uuid='rfo82vr830')
```

## sportality_team_stats

Team totals per period (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/team-stats/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/team-stats/c4hh5nig6t](https://www.basketligaen.dk/api/gameday/team-stats/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_side` | String | home or away: the side the row belongs to. |
| `game_source_id` | Int64 | Genius match id (Int64), the same number as game_id and sports-v2 game_info_ext_id. |
| `game_id` | Int64 | Genius match id (Int64), the join to the genius family; on play-by-play lines, an integer elsewhere. |
| `team_id` | Int64 | Genius team id (Int64): in gameday bodies, an integer in standings and stats tables. |
| `team_name` | String | Name of the team. |
| `team_code` | String | Platform team code (HOL, RAN). |
| `team_number` | Int64 | Team number; home is 1. |
| `instance_id` | String | Site instance that owns the feed (platform key space; dbl1_dbl). |
| `source` | String | Feed that produced the row (genius-basketball-stream-reader). |
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `period` | Int64 | Period: 0 = the whole game, 1-4 the quarters, 5 and up overtime. |
| `period_type` | String | REGULAR or OVERTIME; absent on the whole-game row of some eras. Values: `REGULAR` = regulation period. |
| `tp` | Int64 | Points scored by the team in the period (period 0 = the whole game). |
| `fgm` | Int64 | Field goals made (total). |
| `a` | Int64 | Assists (total). |
| `reb` | Int64 | Total rebounds. |
| `stl` | Int64 | Number of steals. |
| `blk` | Int64 | Number of blocked shots made. |
| `pf` | Int64 | Personal fouls. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_team_stats-example}

```python
sportality_team_stats(game_uuid='c4hh5nig6t')
```

## sportality_team_upcoming_games

A team's next games (current season view).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/upcoming-games/{team_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/upcoming-games/rfo82vr830](https://www.basketligaen.dk/api/sports-v2/upcoming-games/rfo82vr830)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid: `homeTeamInfo.uuid` / `awayTeamInfo.uuid` in the schedule or `teamList[].uuid`. Key space: platform team uuids (not the numeric Genius team id). |

### Returns {#sportality_team_upcoming_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Game date, 'D mon YY' in English. |
| `time` | String | Local start time 'HH:MM' (Europe/Copenhagen). |
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `start_date_time` | String | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | String | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `pre-game` = not yet played (schedule, day views). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | String | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | String | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `N/A` = no result yet / not applicable. |
| `home_team_uuid` | String | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | String | Three-letter code of the home team. |
| `home_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | String | Short name of the team. |
| `home_team_names_long` | String | Long name of the team. |
| `home_team_names_full` | String | Full name of the team. |
| `home_team_names_code_site` | String | Team code as the club's own site shows it. |
| `home_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `home_team_score` | Int64 | Home points; null before the game is played. |
| `home_team_icon` | String | Home team logo URL. |
| `away_team_status` | String | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `N/A` = no result yet / not applicable. |
| `away_team_uuid` | String | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | String | Three-letter code of the away team. |
| `away_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | String | Short name of the team. |
| `away_team_names_long` | String | Long name of the team. |
| `away_team_names_full` | String | Full name of the team. |
| `away_team_names_code_site` | String | Team code as the club's own site shows it. |
| `away_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `away_team_score` | Int64 | Away points; null before the game is played. |
| `away_team_icon` | String | Away team logo URL. |
| `venue_uuid` | String | Venue uuid (platform key space). |
| `venue_name` | String | Venue name; absent when unknown. |
| `series_uuid` | String | Series uuid (platform key space). |
| `series_code` | String | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | String | Series display name (Basketligaen). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_team_upcoming_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_team_upcoming_games-example}

```python
sportality_team_upcoming_games(team_uuid='rfo82vr830')
```
