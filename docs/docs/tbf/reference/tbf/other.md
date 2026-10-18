---
title: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Other

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
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `start_date` | character | Season start, Turkey local time (Europe/Istanbul, UTC+3), ISO 8601 without zone: domestic seasons run 07-01 to 06-30, international ones 10-01 to 09-30. |
| `end_date` | character | Season end, Turkey local time (Europe/Istanbul, UTC+3), ISO 8601 without zone ('2027-06-30T23:59:00'). |
| `international` | logical | True for the international UA- seasons (odd season ids); false for domestic ones. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_seasons-example}

```python
tbf_seasons()
```

_Last validated n/a._

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
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `name` | character | Activity name '<LEAGUE> YYYY-YYYY' ('BSL 2026-2027'). |
| `display_name` | character | League short name ('BSL', 'KBSL', 'TBL', 'TKBL', 'TB2L', 'BGL', 'BGLK', 'KBBL'). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `gender` | character | Gender in Turkish: 'Erkek' = men, 'Kadın' = women. |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `sort` | integer | Display order in the app (integer, 1 = first: BSL). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_leagues-example}

```python
tbf_leagues(season=2027)
```

_Last validated n/a._

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
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | character | League-season name as TBF prints it ('BSL 2026-2027'; the top flight reads 'TBL 2006-2007' until 2007-08). |
| `url` | character | League-season slug, '<prefix>-<YYYY>-<YYYY>' (bsl-2026-2027). |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_league_seasons-example}

```python
tbf_league_seasons(competition='bsl')
```

_Last validated n/a._

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
| `activity_id` | integer | Activity id of the request's league-season (key space: TBF league-seasons). |
| `season` | integer | Season END year of the request (2027 = 2026-2027). |
| `competition` | character | TBF league of the request: bsl, tbl or kbsl. |
| `week` | character | Week number as a string; the value the week argument of tbf_standings and tbf_schedule takes. |
| `phase` | integer | Phase code (devre = half / phase): 1 = first half of the regular season, 2 = second half, 3 = playoff quarter-final, 4 = semi-final, 5 = final. |
| `phase_name` | character | Phase name in Turkish on playoff weeks (Playoff Final = final); null on regular-season weeks. |
| `label` | character | Week label: 'Hafta N' (week N) or the playoff round name. |
| `is_current` | logical | True on the current (or last played) week; exactly one row. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_weeks-example}

```python
tbf_weeks(competition='bsl', season=2027)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `group` | character | Group name (grup); empty for a single-group league. |
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `team_process_id` | integer | Team-season id (teamProcessId = takimIslemId, a team REGISTRATION; key space: TBF team registrations, a NEW id per club per league-season: Tofas = 265995 in BSL 2026-27). The team_process_id argument of the team wrappers; not the stable club_id. |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `week` | integer | The week the table is computed after (hafta); equals the week argument, else the latest. |
| `rank` | integer | League rank (sira) after that week. |
| `played` | integer | Games played (o = oynanan). |
| `won` | integer | Games won (g = galibiyet). |
| `lost` | integer | Games lost (m = maglubiyet). |
| `points_for` | integer | Points scored (a = atilan). |
| `points_against` | integer | Points conceded (y = yenilen). |
| `points` | integer | League points (puan): 2 for a win, 1 for a loss. |
| `head_to_head_points` | integer | Head-to-head league points among teams tied on points (icP = ic puan). |
| `head_to_head_diff` | integer | Head-to-head point difference among teams tied on points (icA = ic averaj). |
| `total_diff` | integer | Total point difference over all games (toA = toplam averaj). |
| `excluded` | integer | Exclusion marker (ihrac = expulsion); 0 for teams still in the league. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_standings-example}

```python
tbf_standings(competition='bsl', season=2027)
```

_Last validated n/a._

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
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `match_code` | character | TBF short game code ('1EA4917'); unique per game, meaning of the letters unverified. |
| `genius_id` | integer | Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one. |
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | character | Activity name ('BSL 2026-2027'). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `tipoff` | character | Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3); the API sends no offset. |
| `match_date` | character | Tip-off date 'YYYY-MM-DD' (Turkey local). |
| `week` | character | Week label: 'N. Hafta' (week N) or the playoff round ('Playoff Final'). |
| `phase` | integer | Phase code as an integer: 1 = first half of the regular season; see tbf_weeks. |
| `status_id` | integer | Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date and time. Cast to an integer from the 1 / 1.0 the API sends. |
| `home_team_process_id` | integer | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `home_team` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `home_score` | integer | Home points (null before the game; the API sends '' ). |
| `away_team_process_id` | integer | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `away_team` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `away_score` | integer | Away points (null before the game; the API sends '' ). |
| `venue` | character | Venue (salon) name. |
| `province` | character | Province (il) of the venue, uppercase Turkish ('ISTANBUL'). |
| `gender` | character | Gender in Turkish: Erkek = men, Kadin = women. |
| `broadcast_channel` | character | Broadcaster ('beINSports', 'HT SPOR', 'Youtube / TBF TV'), or ''. |
| `has_overtime` | logical | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_schedule-example}

```python
tbf_schedule(competition='bsl', season=2027, week=1)
```

_Last validated n/a._

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
| `group_name` | character | Competition group the app shows the game under (grup adi). |
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `match_code` | character | TBF short game code ('1EA4917'); unique per game, meaning of the letters unverified. |
| `genius_id` | integer | Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one. |
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | character | Activity name ('BSL 2026-2027'). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label as TBF writes it, 'YYYY-YYYY' (start and end year). |
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `tipoff` | character | Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3); the API sends no offset. |
| `match_date` | character | Tip-off date 'YYYY-MM-DD'; equals the MatchDate parameter (checked). |
| `week` | character | Week (hafta) label or number: 'N. Hafta' on schedule rows, the number on standings rows, a playoff round name later in the season. |
| `phase` | integer | Phase code as a string ('1' = first half of the regular season); see get-league-weeks devre_ID. |
| `status_id` | integer | Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date and time. Cast to an integer from the 1 / 1.0 the API sends. |
| `home_team_process_id` | integer | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `home_team` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `home_score` | integer | Home points (null before the game). |
| `away_team_process_id` | integer | Team-season id (teamProcessId = takimIslemId, 'team registration'; key space: TBF team registrations, a NEW id per club per league-season: Tofaş = 265995 in BSL 2026-27). The TeamProcessId parameter of the /Team/ routes. Not the club id (takimID / teamId of the team lists). |
| `away_team` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `away_score` | integer | Away points (null before the game). |
| `venue` | character | Venue (salon) name. |
| `province` | character | Province (il) of the venue when sent, uppercase Turkish; null otherwise. |
| `gender` | character | Gender in Turkish when sent; empty on get-daily-matches, so null. |
| `broadcast_channel` | character | Broadcaster ('beINSports', 'HT SPOR', 'Youtube / TBF TV'), or ''. |
| `has_overtime` | logical | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |
| `status` | character | Game status text in Turkish: Oynandi = played, Oynanmadi = not played. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_daily_matches-example}

```python
tbf_daily_matches(match_date='2026-10-11')
```

_Last validated n/a._

## tbf_game_header

Game header of a finished game: teams, coaches, final score, officials, venue and the Genius LiveStats id.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `match_code` | character | TBF short game code ('1EA4917'). |
| `genius_id` | integer | Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one. |
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | character | Activity name ('BSL 2026-2027'). |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label 'YYYY-YYYY' (seasonName). |
| `season` | integer | Season END year, read from the season label (seasonName). |
| `tipoff` | character | Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3). |
| `status_id` | integer | Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date and time. Cast to an integer from the 1 / 1.0 the API sends. |
| `status` | character | Game status text in Turkish: Oynandi = played. Only finished games are served. |
| `venue` | character | Venue name (location) as TBF prints it. |
| `attendance` | integer | Spectator count (matchSpectatorCount); 0 when not recorded. |
| `has_overtime` | logical | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |
| `broadcast_channel` | character | Broadcaster (matchBroadcastChannel), e.g. beINSports, or null. |
| `home_team_process_id` | integer | Home team-season id (key space: TBF team registrations; the header calls it homeTeamProcessId). |
| `home_team` | character | Home team name as registered for that season, uppercase with Turkish letters. |
| `home_coach` | character | Home head coach name, uppercase. |
| `home_score` | integer | Home final score. |
| `away_team_process_id` | integer | Away team-season id (key space: TBF team registrations). |
| `away_team` | character | Away team name as registered for that season, uppercase with Turkish letters. |
| `away_coach` | character | Away head coach name, uppercase. |
| `away_score` | integer | Away final score. |
| `referee` | character | Main referee's name (names only: no contact or birth data is returned). |
| `first_assistant_referee` | character | First assistant referee's name. |
| `second_assistant_referee` | character | Second assistant referee's name. |
| `commissioner` | character | Commissioner's (temsilci = representative) name. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_game_header-example}

```python
tbf_game_header(match_id=346279)
```

_Last validated n/a._

## tbf_game_players

Game roster from the header: every listed player of both sides (names and shirt numbers, no usable ids).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_game_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `side` | character | Side of the game the row belongs to: home or away (from the route's isHome flag or home / away block). |
| `team` | character | Team name for the roster row; empty in the samples, so null. |
| `player_name` | character | Player name as the header roster lists it. |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `is_starter` | logical | True for a starter in the header roster (isStarter). |
| `is_active` | logical | True when the player is flagged active in the header (isActive). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_game_players-example}

```python
tbf_game_players(match_id=346279)
```

_Last validated n/a._

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
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `action_number` | integer | Play sequence number (actionNumber); gaps are deleted or hidden actions. Rows are in the API's game order, and a period-end line can carry a LOWER number than the last play of the period (884 after 885 in the sample), so sort by period and clock, not by this number. |
| `period` | integer | Period number (quarterNumber): 1-4; overtime numbering is unverified. |
| `period_type` | integer | Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the samples). |
| `clock` | character | Game clock REMAINING in the period as 'mm:ss' (counts down from 10:00). |
| `seconds_remaining` | integer | Game clock remaining in the period, in seconds. |
| `home_score` | integer | Running home score after the play (the home half of the 'home-away' score text). |
| `away_score` | integer | Running away score after the play (the away half of the score text). |
| `side` | character | Side of the acting team (isHome); null on game events (period start / end, game end). |
| `player_name` | character | Acting player's name; null on game events. The feed carries no player id. |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `action` | character | Play type in Turkish exactly as TBF sends it (77 values seen). |
| `action_en` | character | English gloss of the Turkish action, from the refs action vocabulary; null for an action not in it. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_pbp-example}

```python
tbf_pbp(match_id=346279)
```

_Last validated n/a._

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
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `period` | integer | Period number of the attempt: 1-4; overtime numbering is unverified. |
| `period_type` | integer | Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the samples). |
| `side` | character | Side of the shooting team (isHome). |
| `shot_player_id` | integer | Shooter id in the SHOT-CHART key space, which is NOT the TBF person id of the box score (Quentin Peterson = 2814324 here, 501076 as a TBF person): join shots to box rows on player_name + jersey_number + side. |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `x` | integer | Court x position: integer percent (0-100) of the LENGTH of a horizontal full court from the LEFT baseline (baskets near 5 and 95); teams change ends at half-time. Metres (x / 100 x 28) are an inference. |
| `y` | integer | Court y position: integer percent (0-100) of the court WIDTH from the BOTTOM sideline (baskets near 50), verified from the app's drawing code (top = 100 - y). Metres (y / 100 x 15) are an inference. |
| `made` | logical | True when the attempt was made (isSucceed). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_shots-example}

```python
tbf_shots(match_id=346279)
```

_Last validated n/a._

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
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `side` | character | Side of the score: home (homeTeamScore) or away (awayTeamScore). |
| `period` | integer | Period number (quarter): 1-4 regulation; overtime numbering is unverified. |
| `period_type` | integer | Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the samples). |
| `points` | integer | Points the side scored IN the period (not cumulative). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_period_scores-example}

```python
tbf_period_scores(match_id=346279)
```

_Last validated n/a._

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
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `title` | character | Statistic name in Turkish (Ribaund = rebounds, Asist = assists, Top Kaybi = turnovers). |
| `category` | character | Statistic code (leaderCategory), e.g. PointsFromPaint, LeadChanges, TimeLeading. |
| `home_value` | character | Home value exactly as sent (a string: '90', or '28:38' for TimeLeading). |
| `away_value` | character | Away value exactly as sent (a string: '89', or '8:19' for TimeLeading). |
| `home_number` | numeric | Home value as a number; null for a time value such as TimeLeading. |
| `away_number` | numeric | Away value as a number; null for a time value such as TimeLeading. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_highlights-example}

```python
tbf_highlights(match_id=346279)
```

_Last validated n/a._

## tbf_game_leaders

Game leaders of a finished game per statistic category (top five each).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-leader`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-leader?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-leader?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_game_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `category` | character | Leader category code (leaderCategory), e.g. VerimlilikPuani = efficiency rating. |
| `category_label` | character | Leader category label in Turkish (leaderCategoryDescription), e.g. Verimlilik Puani. |
| `rank` | integer | Position in the category's list, 1 = best (top five per category). |
| `side` | character | Side of the leader's team (isHome). |
| `player_id` | integer | TBF person id of the leader (key space: TBF persons; equals the box score player_id). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `value` | numeric | The leader's value in the category (points, rebounds, ...). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Example {#tbf_game_leaders-example}

```python
tbf_game_leaders(match_id=346279)
```

_Last validated n/a._
