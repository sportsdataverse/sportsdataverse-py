---
title: "KBL — KBL API (api.kbl.or.kr, Korea) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "KBL — KBL API (api.kbl.or.kr, Korea) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KBL — KBL API (api.kbl.or.kr, Korea) — Other

## kbl_seasons

Every KBL regular season (1997 to the next one) with its competition key and date range, newest first.

**Endpoint URL:** `GET https://api.kbl.or.kr/season/list`

**Valid URL:** [https://api.kbl.or.kr/season/list](https://api.kbl.or.kr/season/list)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kbl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: sequential over every season and D-League edition (2 = 1997-98, 47 = 2025-26); the crosswalk to the sdv END year is in the README. |
| `glkey` | character | Competition key `S{seasonCode}G{gameCode}` (e.g. S47G01 = 2025-26 regular season): the id of the /leagues and /league/rank routes. |
| `season_name` | character | Season label: 'YYYY-YYYY' for a KBL season (e.g. '2025-2026'), '1997' for the inaugural season. |
| `season_name_full` | character | Division label: '1군시즌' = first-team (top-division) season. |
| `season_grade` | integer | Division: 1 = the KBL itself, 2 = the KBL D-League (reserve league). |
| `season_category` | character | Category code of the competition: R regular season, PO playoffs, CP final, D1/D2 D-League stages, AS All-Star, EG/CU cups, EA EASL, OM Open Match Day (table below). |
| `game_code` | character | Competition type within the season: 01 regular season (the only value the list returns). |
| `start_date` | date | First date of the season, YYYYMMDD. |
| `end_date` | date | Last date of the season, YYYYMMDD. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_seasons-example}

```python
kbl_seasons()
```

## kbl_schedule

Every game (all competitions, KBL and D-League) of a season or of a date window, with scores and venue.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/list`

**Valid URL:** [https://api.kbl.or.kr/match/list?season=2026](https://api.kbl.or.kr/match/list?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26): the window from the first to the last game of that END year's KBL and D-League editions, fetched as at most two requests (one per calendar year). Pass this or from_date and to_date. |
| `fromDate` | `from_date` |  |  | `Y` | First date of a window, YYYYMMDD (KST). Needs to_date; inside one calendar year the API answers the whole window in one request (up to 396 games a year, no paging). |
| `toDate` | `to_date` |  |  | `Y` | Last date of the window, YYYYMMDD (KST), not before from_date. |

### Returns {#kbl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key `S{seasonCode}G{gameCode}N{gameNo}` (e.g. S47G01N49): the id of every /match/{gmkey} route. |
| `glkey` | character | Competition key `S{seasonCode}G{gameCode}` (e.g. S47G01 = 2025-26 regular season): the id of the /leagues and /league/rank routes. |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: sequential over every season and D-League edition (2 = 1997-98, 47 = 2025-26); the crosswalk to the sdv END year is in the README. |
| `season_name` | character | Season label as sent: 'YYYY-YYYY' for a KBL season, '1997' for the inaugural one, a D-League edition label, or an empty string on EASL rows. |
| `season_grade` | integer | Division: 1 = the KBL itself, 2 = the KBL D-League (reserve league). |
| `game_code` | character | Competition type within the season (2-digit string; code table in the README and below): 01 regular season, 03 playoffs, 04 final, ... |
| `game_no` | integer | Game number within the competition (glkey); the N part of the gmkey. |
| `season_category` | character | Category code of the competition: R regular season, PO playoffs, CP final, D1/D2 D-League stages, AS All-Star, EG/CU cups, EA EASL, OM Open Match Day (table below). |
| `season_category_name` | character | Category name in Korean: '정규시즌' regular season, '플레이오프' playoffs, '챔피언결정전' championship final, 'D리그 1차' / 'D리그 2차' D-League stage 1 / 2, '올스타게임' All-Star Game, '컵대회' cup, 'KBL컵대회' KBL Cup, 'EASL', 'OPEN MATCH DAY'. |
| `game_date` | date | Game date (KST). |
| `week_day` | character | Day of the week in Korean: 월 Mon, 화 Tue, 수 Wed, 목 Thu, 금 Fri, 토 Sat, 일 Sun. |
| `game_start` | character | Tip-off time, HHMM (KST). |
| `game_end` | character | End time, HHMM (KST); '0000' or blanks when not recorded. |
| `game_time` | integer | Duration of the game in minutes (a numeric string on the wire; null on some rows). |
| `home_team_code` | character | Home team code. KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `away_team_code` | character | Away team code. KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `home_team_name` | character | Home team short name (Korean, e.g. '고양 소노' = Goyang Sono). English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `away_team_name` | character | Away team short name (Korean, e.g. '창원 LG' = Changwon LG). English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `home_team_name_full` | character | Home team full name (Korean). |
| `away_team_name_full` | character | Away team full name (Korean). |
| `home_score` | integer | Home team points (scoreH). |
| `away_score` | integer | Away team points (scoreA). |
| `is_started` | integer | 1 when the game has started. Unreliable for old games (0 on finished 1997-2006 games in /match/list). |
| `is_ended` | integer | 1 when the game is over. Unreliable: 0 on every finished game through 2005-06 and on some later ones (e.g. S45G01N95's header); the capture treats a past-dated game with scores as final. |
| `playing_quarter` | character | Period in progress: 'X' when none (before or after the game), else e.g. 'Q4' or 'EQ1'. Can stay stale after the game (the overtime sample says 'EQ1'). |
| `stadium_name` | character | Venue short name (Korean). |
| `stadium_name_full` | character | Venue full name (Korean). |
| `stadium_name_en` | character | Venue name in English; an empty string, or the Korean name, for some old venues. |
| `tv` | character | Broadcaster name(s), e.g. 'tvN SPORTS', 'SPOTV2 / SPOTV ON'. |
| `tv_code` | character | Broadcaster code(s), comma-separated (e.g. 'T24,T25'); the site uses it instead of `tv` from seasonCode 45. |
| `article_id` | integer | Id of the game's news article on kbl.or.kr; null when none. |
| `challenge_id` | integer | Id of the game in KBL Challenge (the site's prediction game); null when none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_schedule-example}

```python
kbl_schedule(season=2026)
```

## kbl_game

Game header of a final game: teams, final score, overtimes, venue, attendance, officials and coaches.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49](https://api.kbl.or.kr/match/S47G01N49)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key `S{seasonCode}G{gameCode}N{gameNo}` (e.g. S47G01N49): the id of every /match/{gmkey} route. |
| `glkey` | character | Competition key `S{seasonCode}G{gameCode}` (e.g. S47G01 = 2025-26 regular season): the id of the /leagues and /league/rank routes. |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: sequential over every season and D-League edition (2 = 1997-98, 47 = 2025-26); the crosswalk to the sdv END year is in the README. |
| `season_name` | character | Season label: 'YYYY-YYYY' for a KBL season (e.g. '2025-2026'), '1997' for the 1997 inaugural season, or the D-League edition name (e.g. '2010KBLWINTER', '2024 KBL D리그' = 2024 KBL D-League). |
| `season_grade` | integer | Division: 1 = the KBL itself, 2 = the KBL D-League (reserve league). |
| `game_code` | character | Competition type within the season (2-digit string; code table in the README and below): 01 regular season, 03 playoffs, 04 final, ... |
| `game_no` | integer | Game number within the competition (glkey); the N part of the gmkey. |
| `season_category_name` | character | Category name in Korean: '정규시즌' regular season, '플레이오프' playoffs, '챔피언결정전' championship final, 'D리그 1차' / 'D리그 2차' D-League stage 1 / 2, '올스타게임' All-Star Game, '컵대회' cup, 'KBL컵대회' KBL Cup, 'EASL', 'OPEN MATCH DAY'. |
| `game_date` | date | Game date (KST). |
| `week_day` | character | Day of the week in Korean: 월 Mon, 화 Tue, 수 Wed, 목 Thu, 금 Fri, 토 Sat, 일 Sun. |
| `game_start` | character | Tip-off time, HHMM (KST). |
| `game_end` | character | End time, HHMM (KST); '0000' or blanks when not recorded. |
| `game_time` | integer | Duration of the game in minutes. |
| `home_team_code` | character | Home team code. KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `away_team_code` | character | Away team code. KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `home_team_name` | character | Home team short name (Korean, e.g. '고양 소노' = Goyang Sono). English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `away_team_name` | character | Away team short name (Korean, e.g. '창원 LG' = Changwon LG). English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `home_team_name_full` | character | Home team full name (Korean). |
| `away_team_name_full` | character | Away team full name (Korean). |
| `home_score` | integer | Home team final points, summed from the period scores (the header carries no totals). |
| `away_score` | integer | Away team final points, summed from the period scores (the header carries no totals). |
| `overtimes` | integer | Number of overtime periods played (0 for a game decided in regulation). |
| `is_started` | integer | 1 when the game has started. Unreliable for old games (0 on finished 1997-2006 games in /match/list). |
| `is_ended` | integer | 1 when the game is over. Unreliable: 0 on every finished game through 2005-06 and on some later ones (e.g. S45G01N95's header); the capture treats a past-dated game with scores as final. |
| `playing_quarter` | character | Period in progress: 'X' when none (before or after the game), else e.g. 'Q4' or 'EQ1'. Can stay stale after the game (the overtime sample says 'EQ1'). |
| `stadium_name` | character | Venue short name (Korean). |
| `stadium_name_full` | character | Venue full name (Korean). |
| `stadium_name_en` | character | Venue name in English; an empty string, or the Korean name, for some old venues. |
| `crowds` | integer | Attendance of the game (spectators). |
| `tv` | character | Broadcaster name(s), e.g. 'tvN SPORTS', 'SPOTV2 / SPOTV ON'. |
| `tv_code` | character | Broadcaster code(s), comma-separated (e.g. 'T24,T25'); the site uses it instead of `tv` from seasonCode 45. |
| `detail_url` | character | Path of the game page on www.kbl.or.kr. |
| `highlight_url` | character | URL of the game's page on Naver Sports. |
| `referee_main` | character | Crew chief's name (officials appear by name only). |
| `referees_sub` | character | JSON list of the umpires' names (officials appear by name only). |
| `home_manager` | character | Home head coach's name (officials and coaches appear by name only). |
| `away_manager` | character | Away head coach's name (officials and coaches appear by name only). |
| `home_coaches` | character | JSON list of the home assistant coaches' names (staff appear by name only). |
| `away_coaches` | character | JSON list of the away assistant coaches' names (staff appear by name only). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_game-example}

```python
kbl_game(gmkey='S47G01N49')
```

## kbl_period_scores

Points by period (quarters and overtimes) and side of a final game, from the game header.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49](https://api.kbl.or.kr/match/S47G01N49)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `side` | character | Which side of the game the row belongs to: home or away. |
| `team_code` | character | Team code: the KBL club in that season (two-digit string with a leading zero, e.g. '50' = Changwon LG). Joins kbl_teams team_code; not stable across ownership changes (the Anyang club is '40' in 1997-98, '70' today). |
| `period` | integer | Period number: 1-4 for the quarters, 5, 6, ... for the overtimes (the wire says X1 in the play-by-play, EQ1 in the match chart). |
| `period_label` | character | Period label: Q1-Q4, then OT1, OT2, ... (normalised from the wire's X1 / EQ1 / eq1). |
| `score` | integer | Points the side scored in the period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_period_scores-example}

```python
kbl_period_scores(gmkey='S47G01N49')
```

## kbl_pbp

Play-by-play (the site's text cast) of a final game, one row per event, from 1997-98 on.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}/text-cast`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49/text-cast](https://api.kbl.or.kr/match/S47G01N49/text-cast)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `action_number` | integer | 1-based position of the event in the feed, which is in game order (the wire's n is 0 on every row of some games). |
| `event_number` | integer | Sequence number of the event in the game (1-based); 0 on every row of some games (e.g. S45G01N95), so order by array position. |
| `period` | integer | Period number: 1-4 for the quarters, 5, 6, ... for the overtimes (the wire says X1 in the play-by-play, EQ1 in the match chart). |
| `period_label` | character | Period label: Q1-Q4, then OT1, OT2, ... (normalised from the wire's X1 / EQ1 / eq1). |
| `minutes_left` | integer | Minutes remaining in the period (10-minute quarters, 5-minute overtimes). |
| `seconds_left` | integer | Seconds part of the time remaining (0-59). |
| `clock_seconds_left` | integer | Seconds left in the period (minutes_left * 60 + seconds_left); quarters are 10 minutes, overtimes 5. |
| `team_code` | character | Team code of the acting team (the KBL team code, as `tcodeH`/`tcodeA`); '' (or ' ' in 1997-98) on game events such as period start/end. |
| `player_name` | character | Player name in Korean; absent on team and game events. |
| `player_name_en` | character | Player name in English / romanized; absent on team and game events. |
| `action_code` | character | Action code (string, e.g. '201'); table in the returns md. |
| `action` | character | English meaning of action_code (kbl_codes.ACTIONS), translated from the site's Korean label; null for an unknown code. |
| `action_ko` | character | The site's own Korean label of action_code (e.g. 2점슛성공 = two-point field goal made). |
| `points` | integer | Points the event scored: 2 for a made two-point shot or dunk (201, 207), 3 for a made three (205), 1 for a made free throw (203); null otherwise. |
| `substitution_code` | character | Substitution sub-type code on substitution-out (102) rows (e.g. '101_0'); table in the returns md. |
| `substitution` | character | English meaning of substitution_code on substitution-out rows (e.g. 106_1 = fouled out, five fouls); null otherwise. |
| `foul_code` | character | Foul type code (the site reads an f field; no sampled row carries it): PNF personal, TCF technical, FRF unsportsmanlike, FTF fighting, PCF punching, EBF elbow. |
| `foul` | character | English meaning of foul_code (e.g. TCF = technical foul). No sampled row carries a foul code, so this is unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_pbp-example}

```python
kbl_pbp(gmkey='S47G01N49')
```

## kbl_shots

Shot chart of a final game (2006-07 on): every charted shot with x/y pixels on the full-court image.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}/match-chart`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49/match-chart](https://api.kbl.or.kr/match/S47G01N49/match-chart)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `pcode` | character | KBL player code (six-digit string) of the shooter. |
| `player_name` | character | Shooter's name in Korean. |
| `player_name_en` | character | Shooter's name in English / romanized. |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `period` | integer | Period number: 1-4 for the quarters, 5, 6, ... for the overtimes (the wire says X1 in the play-by-play, EQ1 in the match chart). |
| `period_label` | character | Period label: Q1-Q4, then OT1, OT2, ... (normalised from the wire's X1 / EQ1 / eq1). |
| `x` | integer | Shot location x, pixels on the site's 726 x 412 full-court image (origin top-left, x along the court, right); see basket. |
| `y` | integer | Shot location y, pixels on the 726 x 412 full-court image (origin top-left, y down). |
| `made` | logical | True when the shot went in (wire o = 'O'), False when it missed ('X'). |
| `basket` | character | Target basket: '1' the right-hand one (x > 363), '2' the left-hand one; teams switch baskets at half-time. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_shots-example}

```python
kbl_shots(gmkey='S47G01N49')
```

## kbl_score_track

Lead tracker and score chart of a final game: home margin and both scores at eight checkpoints per period.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}/match-chart`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49/match-chart](https://api.kbl.or.kr/match/S47G01N49/match-chart)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_score_track-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `period` | integer | Period number: 1-4 for the quarters, 5, 6, ... for the overtimes (the wire says X1 in the play-by-play, EQ1 in the match chart). |
| `period_label` | character | Period label: Q1-Q4, then OT1, OT2, ... (normalised from the wire's X1 / EQ1 / eq1). |
| `checkpoint` | integer | 1-based checkpoint inside the period: eight per period, evenly spaced is assumed (every 75 s of a quarter; unverified), the last at the period's end. |
| `home_margin` | integer | Home margin (home minus away points) at the checkpoint. |
| `home_score` | integer | Home team points: cumulative at the checkpoint on the score track, the final total (all periods summed) on a game row, the full-time score in the schedule. |
| `away_score` | integer | Away team points: cumulative at the checkpoint on the score track, the final total (all periods summed) on a game row, the full-time score in the schedule. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_score_track-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_score_track-example}

```python
kbl_score_track(gmkey='S47G01N49')
```

## kbl_game_leaders

Each team's leader per category (points, rebounds, assists, steals, blocks) in a final game.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}/match-chart`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49/match-chart](https://api.kbl.or.kr/match/S47G01N49/match-chart)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_game_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `part` | character | Category code: score points, rb rebounds, ast assists, stl steals, bs blocks. |
| `side` | character | Side of the leader: home or away. |
| `pcode` | character | KBL player code (six-digit string) of the team's leader in the category. |
| `player_name` | character | Leader's name in Korean (foreign players in Hangul transliteration). |
| `player_name_en` | character | Leader's name in English / romanized. |
| `team_name` | character | The leader's team short name (Korean). |
| `value` | numeric | The leader's total in the category in this game. |
| `play_min` | integer | Minutes the leader played this game; add play_sec seconds. |
| `play_sec` | integer | Seconds part of the leader's playing time (0-59). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_game_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_game_leaders-example}

```python
kbl_game_leaders(gmkey='S47G01N49')
```

## kbl_standings

Standings of one competition with each team's season totals.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/rank/{season}`

**Valid URL:** [https://api.kbl.or.kr/league/rank/2025?division=kbl&gameCode=01](https://api.kbl.or.kr/league/rank/2025?division=kbl&gameCode=01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |
| `division` | `division` |  |  | `Y` | 'kbl' (the KBL itself, the default) or 'd_league' (the KBL D-League reserve league, seasonGrade 2, which has its own season codes; it starts with the 2009 summer edition). |
| `gameCode` | `game_code` |  |  | `Y` | Competition inside the season, two digits: '01' regular season (default; the first stage of a D-League edition), '03' playoffs, '04' championship final, '07' EASL, '08' D-League stage 2, '10' All-Star, '11'/'13' cups, '14' Open Match Day. Only '01' was probed live. |

### Returns {#kbl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `game_code` | character | Competition type within the season (2-digit string; code table in the README and below): 01 regular season, 03 playoffs, 04 final, ... |
| `rank` | integer | Standing (1 = first). |
| `team_code` | character | Team code: the KBL club in that season (two-digit string with a leading zero, e.g. '50' = Changwon LG). Joins kbl_teams team_code; not stable across ownership changes (the Anyang club is '40' in 1997-98, '70' today). |
| `team_name` | character | Team short name (Korean, e.g. '원주 DB' = Wonju DB). English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `team_name_full` | character | Team full name (Korean). |
| `game_count` | integer | Games played (this season or competition). |
| `wins` | integer | Season wins of the team (the wire's TWin). |
| `losses` | integer | Season losses of the team (the wire's TLoss). |
| `draws` | integer | Draws (TDraw, always 0). |
| `win_rate` | numeric | Win fraction, 0-1. |
| `win_diff` | numeric | Games behind the leader. |
| `home_wins` | integer | Wins in the team's home games. |
| `home_losses` | integer | Losses in the team's home games. |
| `away_wins` | integer | Wins in the team's away games. |
| `away_losses` | integer | Losses in the team's away games. |
| `conti_win` | integer | Current winning streak (0 when the last game was a loss). |
| `conti_loss` | integer | Current losing streak (0 when the last game was a win). |
| `conti_date` | date | Date (YYYYMMDD) the current streak started (inferred; unverified). |
| `max_win` | integer | Longest winning streak of the season. |
| `max_loss` | integer | Longest losing streak of the season. |
| `last_game_date` | date | Date of the latest game counted in the row, YYYYMMDD. |
| `last_five` | character | The last five results as a comma-separated string, most recent first: 1 win, 0 loss (the name is cut short in the API). |
| `last_game_won` | logical | True when the team's latest game was a win (the wire's win; a boolean, not a count). |
| `last_game_lost` | logical | True when the team's latest game was a loss (inferred from conti_loss). |
| `last_game_drawn` | logical | True when the team's latest game was a draw (always false). |
| `game_flag` | logical | Boolean flag in the standings row; meaning unverified (true in the sample). |
| `updated_at` | character | When the standings row was written (ISO 8601 UTC timestamp). |
| `score` | integer | Points. Team season total for the competition (glkey). |
| `fg` | integer | Two-point field goals made, dunks included: 201 + 207 rows (score = 2*fg + 3*threep + ft holds on every sampled box). Team season total for the competition (glkey). |
| `fg_a` | integer | Two-point field-goal attempts, made + missed, dunks included: 201 + 202 + 207 + 208 rows (checked per team on S47G01N49). Team season total for the competition (glkey). |
| `threep` | integer | Three-point field goals made (action 205). Team season total for the competition (glkey). |
| `threep_a` | integer | Three-point attempts, made + missed (205 + 206). Team season total for the competition (glkey). |
| `ft` | integer | Free throws made (action 203). Team season total for the competition (glkey). |
| `ft_a` | integer | Free-throw attempts, made + missed (203 + 204). Team season total for the competition (glkey). |
| `dk` | integer | Dunks made (action 207); included in `fg`. Team season total for the competition (glkey). |
| `dk_a` | integer | Dunk attempts (made + missed, actions 207 + 208). Team season total for the competition (glkey). |
| `ast` | integer | Assists. Team season total for the competition (glkey). |
| `bs` | integer | Blocked shots. Team season total for the competition (glkey). |
| `defr` | integer | Defensive rebounds. Team season total for the competition (glkey). |
| `offr` | integer | Offensive rebounds. Team season total for the competition (glkey). |
| `stl` | integer | Steals. Team season total for the competition (glkey). |
| `to` | integer | Turnovers. Team season total for the competition (glkey). |
| `team_r` | integer | Team rebounds (action 218), unverified against `teamR`. Team season total for the competition (glkey). |
| `pp` | integer | Paint field goals made: 0 in every sampled row (see PScore). |
| `pp_a` | integer | Paint field-goal attempts: 0 in every sampled row (see PScore). |
| `pscore` | integer | Points in the paint (pp/ppA are 0 on this route in every sampled row). Team season total for the competition (glkey). |
| `wft` | integer | Fouls that awarded free throws (action 215). Team season total for the competition (glkey). |
| `woft` | integer | Fouls that awarded no free throws (action 216). Team season total for the competition (glkey). |
| `foul_total` | integer | Personal fouls over the season (foulTot). |
| `tfb` | integer | Team fast breaks (action 217). Team season total for the competition (glkey). |
| `tto` | integer | Team turnovers (action 223). Team season total for the competition (glkey). |
| `tf` | integer | Probably technical fouls (unverified; 0 in most rows). Team season total for the competition (glkey). |
| `fb` | integer | Fast breaks credited to the player (action 217 rows that name a player, as in 1997-98); 0 in recent seasons, where fast breaks are team rows (`tfb`). Team season total for the competition (glkey). |
| `ef` | integer | Meaning unverified (0 in every game sample; non-zero in some season totals). Team season total for the competition (glkey). |
| `bf` | integer | Meaning unverified (probably bench fouls; 0 in every game sample). Team season total for the competition (glkey). |
| `gd` | integer | Good-defense plays (action 221). Team season total for the competition (glkey). |
| `idf` | integer | Illegal-defense violations (action 219; an early-KBL rule, so non-zero only in old seasons). Team season total for the competition (glkey). |
| `inout` | integer | Times the player entered the game, the start included (checked on the 2025-26 sample game); a team row sums its players. Team season total for the competition (glkey). |
| `inout1` | integer | Times the player was substituted out before the end (checked on the 2025-26 sample game); a team row sums its players. Team season total for the competition (glkey). |
| `foulout` | integer | Players fouled out (unverified). Team season total for the competition (glkey). |
| `play_min` | integer | Minutes of playing time; add `playSec` seconds. Team season total for the competition (glkey). |
| `play_sec` | integer | Seconds part of the playing time (0-59), added to `playMin`. Team season total for the competition (glkey). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_standings-example}

```python
kbl_standings(season=2025)
```

## kbl_rounds

The season's round calendar (the KBL plays six rounds) with each round's first and last date.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/rank/{season}/team-tracks`

**Valid URL:** [https://api.kbl.or.kr/league/rank/2025/team-tracks?division=kbl&gameCode=01](https://api.kbl.or.kr/league/rank/2025/team-tracks?division=kbl&gameCode=01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |
| `division` | `division` |  |  | `Y` | 'kbl' (the KBL itself, the default) or 'd_league' (the KBL D-League reserve league, seasonGrade 2, which has its own season codes; it starts with the 2009 summer edition). |
| `gameCode` | `game_code` |  |  | `Y` | Competition inside the season, two digits: '01' regular season (default; the first stage of a D-League edition), '03' playoffs, '04' championship final, '07' EASL, '08' D-League stage 2, '10' All-Star, '11'/'13' cups, '14' Open Match Day. Only '01' was probed live. |

### Returns {#kbl_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `glkey` | character | Competition key S{seasonCode}G{gameCode} (e.g. S47G01 = 2025-26 regular season): the id of the season routes. |
| `round` | integer | Round number of the season (the KBL plays six rounds). |
| `from_date` | date | First date of the round, YYYYMMDD. |
| `to_date` | date | Last date of the round, YYYYMMDD. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_rounds-example}

```python
kbl_rounds(season=2025)
```

## kbl_round_mvps

The round MVPs of a season with their totals over the round.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/round-mvp/{season}`

**Valid URL:** [https://api.kbl.or.kr/league/round-mvp/2025](https://api.kbl.or.kr/league/round-mvp/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |

### Returns {#kbl_round_mvps-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: sequential over every season and D-League edition (2 = 1997-98, 47 = 2025-26); the crosswalk to the sdv END year is in the README. |
| `round` | integer | Round number of the season (the KBL plays six rounds). |
| `pcode` | character | KBL player code (six-digit string) of the round MVP (the wire's playerNo). |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `player_name` | character | Round MVP's name in Korean (kname). |
| `player_name_en` | character | Player name in English / romanized (sometimes Hangul for former players, e.g. 'CJ 레슬리'). |
| `game_count` | integer | Games played in the round. |
| `total_seconds` | integer | Playing time over the round in seconds (totalHour * 3600 + totalMinute * 60 + totalSecond; only the seconds part is non-zero in the samples). |
| `total_score` | integer | Points over the round. |
| `total_rebound` | integer | Rebounds over the round. |
| `total_assist` | integer | Assists over the round. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_round_mvps-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_round_mvps-example}

```python
kbl_round_mvps(season=2025)
```

## kbl_current_standings

Current standings of the active season (no parameter): record, games behind, streaks, splits.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/rank/team`

**Valid URL:** [https://api.kbl.or.kr/league/rank/team](https://api.kbl.or.kr/league/rank/team)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kbl_current_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Standing (1 = first). |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_name` | character | Team short name (Korean, e.g. '창원 LG' = Changwon LG), as it was at the time. English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `team_name_full` | character | Team full name (Korean, e.g. '창원 LG 세이커스' = Changwon LG Sakers). |
| `wins` | integer | Wins (a count on this route; on /league/rank/{glkey} win is a boolean about the latest game). |
| `losses` | integer | Losses (a count on this route). |
| `draws` | integer | Draws (always 0 in basketball). |
| `win_diff` | numeric | Games behind the leader. |
| `home_wins` | integer | Wins in the team's home games. |
| `home_losses` | integer | Losses in the team's home games. |
| `away_wins` | integer | Wins in the team's away games. |
| `away_losses` | integer | Losses in the team's away games. |
| `conti_win` | integer | Current winning streak (0 when the last game was a loss). |
| `conti_loss` | integer | Current losing streak (0 when the last game was a win). |
| `max_win` | integer | Longest winning streak of the season. |
| `max_loss` | integer | Longest losing streak of the season. |
| `last_five` | character | The last five results as a comma-separated string, most recent first: 1 win, 0 loss. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_current_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_current_standings-example}

```python
kbl_current_standings()
```

## kbl_season_leaders

Current season leaders per category (no parameter): points, rebounds, assists, steals, blocks, shooting.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/season-leader`

**Valid URL:** [https://api.kbl.or.kr/league/season-leader](https://api.kbl.or.kr/league/season-leader)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kbl_season_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `part` | character | Leader category code: score, rb, ast, bs, stl, fgt, threep, ftR, fgtR, threepR or dk. |
| `part_label` | character | English meaning of part (kbl_codes.LEADER_PARTS), e.g. score = points per game. |
| `rank` | integer | Rank in the category. |
| `value` | numeric | The player's value in the category: a per-game average, or a percentage for the *R categories. |
| `game_count` | integer | Games played this season. |
| `pcode` | character | KBL player code (6-digit string). Joins to `pcode` on every route and to `playerNo` in /league/round-mvp. |
| `player_name` | character | Player name in Korean (foreign players in Hangul transliteration). |
| `player_name_en` | character | Player name in English / romanized (sometimes Hangul for former players, e.g. 'CJ 레슬리'). |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_name` | character | Team short name (Korean, e.g. '창원 LG' = Changwon LG), as it was at the time. English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `back_num` | character | Jersey number (string). |
| `pos` | character | Position code (string): C center, FD forward, GD guard, FC forward-center, GF guard-forward; table in the returns md. |
| `player_flag` | character | Player type (string): '0' domestic, '1' foreign import, '2' Asian-quota player. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_season_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_season_leaders-example}

```python
kbl_season_leaders()
```
