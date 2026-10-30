# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Other

> TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Other — function reference in sdv-py, the SportsDataverse Python package.

## tbf_seasons

The 20 most recent TBF seasons, domestic and international (a server cap), newest first.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Season/get-seasons`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Season/get-seasons](https://miniappapi.tbf.org.tr/webapi-service/api/Season/get-seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#tbf_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | String | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | Int64 | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `start_date` | String | Season start, Turkey local time (Europe/Istanbul, UTC+3), ISO 8601 without zone: domestic seasons run 07-01 to 06-30, international ones 10-01 to 09-30. |
| `end_date` | String | Season end, Turkey local time (Europe/Istanbul, UTC+3), ISO 8601 without zone ('2027-06-30T23:59:00'). |
| `international` | Boolean | True for the international UA- seasons (odd season ids); false for domestic ones. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_seasons-example}

```python
tbf_seasons()
```

## tbf_leagues

The featured leagues (league-season activities) of one TBF season: BSL, TBL, KBSL and the others.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/League/get-leagues-for-filter`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/League/get-leagues-for-filter?season=2027](https://miniappapi.tbf.org.tr/webapi-service/api/League/get-leagues-for-filter?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |

### Returns {#tbf_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `activity_id` | Int64 | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `name` | String | Activity name ' YYYY-YYYY' ('BSL 2026-2027'). |
| `display_name` | String | League short name ('BSL', 'KBSL', 'TBL', 'TKBL', 'TB2L', 'BGL', 'BGLK', 'KBBL'). |
| `competition` | String | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `gender` | String | Gender in Turkish: 'Erkek' = men, 'Kadın' = women. |
| `season_id` | Int64 | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | String | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | Int64 | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `sort` | Int64 | Display order in the app (integer, 1 = first: BSL). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leagues-example}

```python
tbf_leagues(season=2027)
```

## tbf_league_seasons

Every season of one TBF league (bsl back to 1966-67) with its activity and season ids.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/League/get-leagues-and-seasons-by-prefix`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/League/get-leagues-and-seasons-by-prefix?competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/League/get-leagues-and-seasons-by-prefix?competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_league_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `activity_id` | Int64 | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | String | League-season name as TBF prints it ('BSL 2026-2027'; the top flight reads 'TBL 2006-2007' until 2007-08). |
| `url` | String | League-season slug, '<prefix>--' (bsl-2026-2027). |
| `season_id` | Int64 | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | String | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | Int64 | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | String | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_league_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_league_seasons-example}

```python
tbf_league_seasons(competition='bsl')
```

## tbf_weeks

The weeks (rounds) of one league-season, playoff rounds included.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/League/get-league-weeks`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/League/get-league-weeks?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/League/get-league-weeks?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `activity_id` | Int64 | Activity id of the request's league-season (key space: TBF league-seasons). |
| `season` | Int64 | Season END year of the request (2027 = 2026-2027). |
| `competition` | String | TBF league of the request: bsl, tbl or kbsl. |
| `week` | String | Week number as a string; the value the week argument of tbf_standings and tbf_schedule takes. |
| `phase` | Int64 | Phase code (devre = half / phase): 1 = first half of the regular season, 2 = second half, 3 = playoff quarter-final, 4 = semi-final, 5 = final. |
| `phase_name` | String | Phase name in Turkish on playoff weeks (Playoff Final = final); null on regular-season weeks. |
| `label` | String | Week label: 'Hafta N' (week N) or the playoff round name. |
| `is_current` | Boolean | True on the current (or last played) week; exactly one row. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_weeks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_weeks-example}

```python
tbf_weeks(competition='bsl', season=2027)
```

## tbf_standings

League table of one league-season after a week (default the latest), one row per team and group.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/League/get-standings-table`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/League/get-standings-table?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/League/get-standings-table?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |
| `week` | `week` |  |  | `Y` | Week (hafta) number as returned by tbf_weeks, e.g. 1; playoff rounds continue the numbering (31-33 in 2025-26). Default: the latest week for standings, every week for the schedule. |

### Returns {#tbf_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | String | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `group` | String | Group name (grup); empty for a single-group league. |
| `activity_id` | Int64 | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `team_process_id` | Int64 | Team-season id (teamProcessId = takimIslemId, a team REGISTRATION; key space: TBF team registrations, a NEW id per club per league-season: Tofas = 265995 in BSL 2026-27). The team_process_id argument of the team wrappers; not the stable club_id. |
| `team_name` | String | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `week` | Int64 | The week the table is computed after (hafta); equals the week argument, else the latest. |
| `rank` | Int64 | League rank (sira) after that week. |
| `played` | Int64 | Games played (o = oynanan). |
| `won` | Int64 | Games won (g = galibiyet). |
| `lost` | Int64 | Games lost (m = maglubiyet). |
| `points_for` | Int64 | Points scored (a = atilan). |
| `points_against` | Int64 | Points conceded (y = yenilen). |
| `points` | Int64 | League points (puan): 2 for a win, 1 for a loss. |
| `head_to_head_points` | Int64 | Head-to-head league points among teams tied on points (icP = ic puan). |
| `head_to_head_diff` | Int64 | Head-to-head point difference among teams tied on points (icA = ic averaj). |
| `total_diff` | Int64 | Total point difference over all games (toA = toplam averaj). |
| `excluded` | Int64 | Exclusion marker (ihrac = expulsion); 0 for teams still in the league. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_standings-example}

```python
tbf_standings(competition='bsl', season=2027)
```

## tbf_schedule

Schedule and results of one league-season: the games of one week, or of every week.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/get-all-matches-for-filter`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/get-all-matches-for-filter?season=2027&competition=bsl&week=1](https://miniappapi.tbf.org.tr/webapi-service/api/Match/get-all-matches-for-filter?season=2027&competition=bsl&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |
| `week` | `week` |  |  | `Y` | Week (hafta) number as returned by tbf_weeks, e.g. 1; playoff rounds continue the numbering (31-33 in 2025-26). Default: the latest week for standings, every week for the schedule. |

### Returns {#tbf_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `match_code` | String | TBF short game code ('1EA4917'); unique per game, meaning of the letters unverified. |
| `genius_id` | Int64 | Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one. |
| `activity_id` | Int64 | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | String | Activity name ('BSL 2026-2027'). |
| `competition` | String | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `season_id` | Int64 | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | String | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | Int64 | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `tipoff` | String | Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3); the API sends no offset. |
| `match_date` | String | Tip-off date 'YYYY-MM-DD' (Turkey local). |
| `week` | String | Week label: 'N. Hafta' (week N) or the playoff round ('Playoff Final'). |
| `phase` | Int64 | Phase code as an integer: 1 = first half of the regular season; see tbf_weeks. |
| `status_id` | Int64 | Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date and time. Cast to an integer from the 1 / 1.0 the API sends. |
| `home_team_process_id` | Int64 | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `home_team` | String | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `home_score` | Int64 | Home points (null before the game; the API sends '' ). |
| `away_team_process_id` | Int64 | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `away_team` | String | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `away_score` | Int64 | Away points (null before the game; the API sends '' ). |
| `venue` | String | Venue (salon) name. |
| `province` | String | Province (il) of the venue, uppercase Turkish ('ISTANBUL'). |
| `gender` | String | Gender in Turkish: Erkek = men, Kadin = women. |
| `broadcast_channel` | String | Broadcaster ('beINSports', 'HT SPOR', 'Youtube / TBF TV'), or ''. |
| `has_overtime` | Boolean | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_schedule-example}

```python
tbf_schedule(competition='bsl', season=2027, week=1)
```

## tbf_daily_matches

Every TBF game played on one date, grouped by competition, optionally for one league-season.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/get-daily-matches`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/get-daily-matches?match_date=2026-10-11](https://miniappapi.tbf.org.tr/webapi-service/api/Match/get-daily-matches?match_date=2026-10-11)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_date` | `match_date` |  | `Y` |  | Required. Game date 'YYYY-MM-DD' (Turkey local date), e.g. '2026-10-11'. |
| `season` | `season` |  |  | `Y` | Optional, with competition: Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. Default: games of every competition. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. Optional, with season. Default: every competition. |

### Returns {#tbf_daily_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | String | Competition group the app shows the game under (grup adi). |
| `match_id` | Int64 | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `match_code` | String | TBF short game code ('1EA4917'); unique per game, meaning of the letters unverified. |
| `genius_id` | Int64 | Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one. |
| `activity_id` | Int64 | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | String | Activity name ('BSL 2026-2027'). |
| `competition` | String | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `season_id` | Int64 | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | String | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | Int64 | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `tipoff` | String | Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3); the API sends no offset. |
| `match_date` | String | Tip-off date 'YYYY-MM-DD'; equals the MatchDate parameter (checked). |
| `week` | String | Week (hafta) label or number: 'N. Hafta' on schedule rows, the number on standings rows, a playoff round name later in the season. |
| `phase` | Int64 | Phase code as a string ('1' = first half of the regular season); see get-league-weeks devre_ID. |
| `status_id` | Int64 | Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date and time. Cast to an integer from the 1 / 1.0 the API sends. |
| `home_team_process_id` | Int64 | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `home_team` | String | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `home_score` | Int64 | Home points (null before the game). |
| `away_team_process_id` | Int64 | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `away_team` | String | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `away_score` | Int64 | Away points (null before the game). |
| `venue` | String | Venue (salon) name. |
| `province` | String | Province (il) of the venue when sent, uppercase Turkish; null otherwise. |
| `gender` | String | Gender in Turkish when sent; empty on get-daily-matches, so null. |
| `broadcast_channel` | String | Broadcaster ('beINSports', 'HT SPOR', 'Youtube / TBF TV'), or ''. |
| `has_overtime` | Boolean | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |
| `status` | String | Game status text in Turkish: Oynandi = played, Oynanmadi = not played. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_daily_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_daily_matches-example}

```python
tbf_daily_matches(match_date='2026-10-11')
```

## tbf_pbp

Play-by-play of a finished game (2025-26 onward), in game order, with the running score and an English gloss.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/game-flow`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/game-flow?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/game-flow?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `action_number` | Int64 | Play sequence number (actionNumber); gaps are deleted or hidden actions. Rows are in the API's game order, and a period-end line can carry a LOWER number than the last play of the period (884 after 885 in the sample), so sort by period and clock, not by this number. |
| `period` | Int64 | Period number (quarterNumber): 1-4; overtime numbering is unverified. |
| `period_type` | Int64 | Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the samples). |
| `clock` | String | Game clock REMAINING in the period as 'mm:ss' (counts down from 10:00). |
| `seconds_remaining` | Int64 | Game clock remaining in the period, in seconds. |
| `home_score` | Int64 | Running home score after the play (the home half of the 'home-away' score text). |
| `away_score` | Int64 | Running away score after the play (the away half of the score text). |
| `side` | String | Side of the acting team (isHome); null on game events (period start / end, game end). |
| `player_name` | String | Acting player's name; null on game events. The feed carries no player id. |
| `jersey_number` | String | Shirt number as a string, so '0' and '00' stay distinct. |
| `action` | String | Play type in Turkish exactly as TBF sends it (77 values seen). |
| `action_en` | String | English gloss of the Turkish action, from the refs action vocabulary; null for an action not in it. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_pbp-example}

```python
tbf_pbp(match_id=346279)
```

## tbf_shots

Every field-goal attempt of a finished game (2025-26 onward) with its court position.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/shot-chart`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/shot-chart?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/shot-chart?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `period` | Int64 | Period number of the attempt: 1-4; overtime numbering is unverified. |
| `period_type` | Int64 | Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the samples). |
| `side` | String | Side of the shooting team (isHome). |
| `shot_player_id` | Int64 | Shooter id in the SHOT-CHART key space, which is NOT the TBF person id of the box score (Quentin Peterson = 2814324 here, 501076 as a TBF person): join shots to box rows on player_name + jersey_number + side. |
| `player_name` | String | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `jersey_number` | String | Shirt number as a string, so '0' and '00' stay distinct. |
| `x` | Int64 | Court x position: integer percent (0-100) of the LENGTH of a horizontal full court from the LEFT baseline (baskets near 5 and 95); teams change ends at half-time. Metres (x / 100 x 28) are an inference. |
| `y` | Int64 | Court y position: integer percent (0-100) of the court WIDTH from the BOTTOM sideline (baskets near 50), verified from the app's drawing code (top = 100 - y). Metres (y / 100 x 15) are an inference. |
| `made` | Boolean | True when the attempt was made (isSucceed). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_shots-example}

```python
tbf_shots(match_id=346279)
```

## tbf_period_scores

Points each side scored in each period of a finished game, overtime included.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-ozet`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-ozet?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-ozet?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `side` | String | Side of the score: home (homeTeamScore) or away (awayTeamScore). |
| `period` | Int64 | Period number (quarter): 1-4 regulation; overtime numbering is unverified. |
| `period_type` | Int64 | Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the samples). |
| `points` | Int64 | Points the side scored IN the period (not cumulative). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_period_scores-example}

```python
tbf_period_scores(match_id=346279)
```

## tbf_highlights

Team comparison of a finished game: rebounds, paint points, fast-break points, lead changes and more.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-ozet`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-ozet?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-ozet?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_highlights-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `title` | String | Statistic name in Turkish (Ribaund = rebounds, Asist = assists, Top Kaybi = turnovers). |
| `category` | String | Statistic code (leaderCategory), e.g. PointsFromPaint, LeadChanges, TimeLeading. |
| `home_value` | String | Home value exactly as sent (a string: '90', or '28:38' for TimeLeading). |
| `away_value` | String | Away value exactly as sent (a string: '89', or '8:19' for TimeLeading). |
| `home_number` | Float64 | Home value as a number; null for a time value such as TimeLeading. |
| `away_number` | Float64 | Away value as a number; null for a time value such as TimeLeading. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_highlights-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_highlights-example}

```python
tbf_highlights(match_id=346279)
```
