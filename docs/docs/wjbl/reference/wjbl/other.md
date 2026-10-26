---
title: "W League (Japan) — W League API (api.wjbl.01core.app, Japan women) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "W League (Japan) — W League API (api.wjbl.01core.app, Japan women) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# W League (Japan) — W League API (api.wjbl.01core.app, Japan women) — Other

## wjbl_leagues

Every competition edition (league_id): the W League seasons back to 1999-2000, all-star games, cups and camps.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/leagues`

**Valid URL:** [https://api.wjbl.01core.app/api/leagues?target=play_schedule](https://api.wjbl.01core.app/api/leagues?target=play_schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#wjbl_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | String | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | Int64 | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_leagues-example}

```python
wjbl_leagues()
```

## wjbl_schedule

Games of a season in ONE month (default: the season's latest month) with teams, venue, status and score.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/play-schedules`

**Valid URL:** [https://api.wjbl.01core.app/api/play-schedules?season=2026&game_month=2026-04](https://api.wjbl.01core.app/api/play-schedules?season=2026&game_month=2026-04)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, 2000 = 1999-2000); the wrapper maps it to the league edition id (league_id) with the live league list (wjbl_leagues, one extra cached request). |
| `game_month` | `game_month` |  |  | `Y` | Month to list, 'YYYY-MM' (see wjbl_game_months). Omitted = the season's latest month only, NOT the whole season: the API pages by month. |
| `team_id` | `team_id` |  |  | `Y` | Franchise id (team_id in wjbl_teams): stable across seasons and sponsor renames, e.g. 13 (Toyota Antelopes). Narrows the month to that team's games. |

### Returns {#wjbl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | The league_id asked for (integer): Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | String | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | Int64 | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | Int64 | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_title` | String | Phase title in Japanese: 'レギュラーシーズン' = regular season, 'プレーオフ　ファイナル' / 'プレーオフ／ファイナル' = playoff final, 'プレーオフ／セミファイナル' = playoff semifinal (the separator varies by era). |
| `season_full_name` | String | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `play_schedule_id` | Int64 | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `game_year` | Int64 | Calendar year of the game, 'YYYY' string (not the season); parsed to an integer. |
| `game_date` | String | Game day 'MM.DD' ('04.04'); with game_year it is the date, Japan time. |
| `game_day` | String | Game day 'MM.DD' ('04.04'); with game_year it is the date, Japan time. |
| `game_date_week_day_name` | String | Weekday in Japanese, one character: 月 Mon, 火 Tue, 水 Wed, 木 Thu, 金 Fri, 土 Sat, 日 Sun. |
| `game_start_time` | String | Scheduled tip-off 'HH:MM', Japan time (JST, UTC+9). |
| `started_at` | String | Actual tip-off 'HH:MM', JST, as recorded ('15:01'); junk on 2013-14 games ('05:06' for a 19:00 game, and it CHANGES between requests: 05:06 / 05:16 / 05:25 for game 4243), so ignore it there. |
| `opening_time` | String | Doors-open time 'HH:MM', JST; empty when not published. |
| `venue` | String | Arena name, Japanese. |
| `venue_address` | String | Arena postal address, Japanese. |
| `game_icon` | Int64 | Icon code shown next to the game: 0 = none, 1 = home-game icon (home.svg), 2 = neutral-site icon (neutral.svg). |
| `ticket_url` | String | Ticket sale URL; empty string in every sample. |
| `vod_link` | String | Video-on-demand URL (Basketball LIVE / W-TV); empty when none. |
| `live_link` | String | Live-stream URL; empty when none. |
| `game_status` | String | Game state: 'fixed' = final (confirmed result), 'scheduled' = not played yet (seen on 26-27 games); the site's JS also handles 'live', 'canceled' and 'delayed'. The capture banks only 'fixed' games. |
| `box_flg` | Int64 | Box-score flag (integer). 1 on every sampled game, scheduled ones included, so it does NOT prove a box score exists. |
| `pbp_flg` | Int64 | Play-by-play flag: 1 = /pbp/* routes have data (2018-19 onward), 0 = not (they answer []). |
| `cancel_flg` | Int64 | Cancelled flag: 1 = cancelled, 0 = not. |
| `delay_flg` | Int64 | Postponed flag: 1 = postponed, 0 = not. |
| `ot_flg` | Int64 | Overtime flag: 1 = the game went to overtime, 0 = not. |
| `home_team_id` | Int64 | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `home_season_team_name` | String | Home team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `home_season_team_short_name` | String | Home team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `home_team_logo_image` | String | Home team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `home_team_free_text` | String | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `home_team_score` | Int64 | Home final points (integer). Every sampled game is finished, so null is not in the type; unplayed games (2026-27 'scheduled') carry null. |
| `home_team_entry_flg` | Int64 | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `away_team_id` | Int64 | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_season_team_name` | String | Away team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `away_season_team_short_name` | String | Away team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `away_team_logo_image` | String | Away team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `away_team_free_text` | String | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `away_team_score` | Int64 | Away final points (integer). Null on unplayed games, as for home_team_score (not in the finished samples). |
| `away_team_entry_flg` | Int64 | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_schedule-example}

```python
wjbl_schedule(game_month='2026-04', season=2026)
```

## wjbl_game

Game header of a final game: both teams, the result flag and the score by period, one row.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/play-schedule/detail`

**Valid URL:** [https://api.wjbl.01core.app/api/play-schedule/detail?play_schedule_id=7987](https://api.wjbl.01core.app/api/play-schedule/detail?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | Int64 | Game id: ONE global sequence across every league edition and season (7987 = 2025-26 playoff final game 1); the play_schedule_id of the request. |
| `game_status` | String | Game state; always fixed (final) here, the wrapper refuses a game that is not final. |
| `home_team_id` | Int64 | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `home_season_team_name` | String | Home team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `home_season_team_short_name` | String | Home team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `home_team_logo_image` | String | Home team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `home_team_entry_flg` | Int64 | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `home_season_team_area` | String | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's standings JS: 0 = East, 1 = West conference when season_league_flg = 2.) |
| `home_free_text` | String | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `home_winner_flg` | Int64 | Result for this side: 1 = won, -1 = lost (integer; a 0 tie code is unobserved). |
| `home_score` | Int64 | Home points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `home_score_p1` | Int64 | Points scored IN the 1st quarter (not cumulative). |
| `home_score_p2` | Int64 | Points scored in the 2nd quarter. |
| `home_score_p3` | Int64 | Points scored in the 3rd quarter. |
| `home_score_p4` | Int64 | Points scored in the 4th quarter. |
| `home_score_ot1` | Int64 | Points scored in the 1st overtime (5 min); null when there was none. |
| `home_score_ot2` | Int64 | Unobserved: null in every sample. When filled: Points in the 2nd overtime; null when none. |
| `home_score_ot3` | Int64 | Unobserved: null in every sample. When filled: Points in the 3rd overtime; null when none. |
| `home_score_ot4` | Int64 | Unobserved: null in every sample. When filled: Points in the 4th overtime; null when none. There is no score_ot5+: overtimes 5-10 appear only as play-by-play period 9-14 and in the /pbp/team-stats and /pbp/player-stats periods maps. |
| `away_team_id` | Int64 | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_season_team_name` | String | Away team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `away_season_team_short_name` | String | Away team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `away_team_logo_image` | String | Away team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `away_team_entry_flg` | Int64 | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `away_season_team_area` | String | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's standings JS: 0 = East, 1 = West conference when season_league_flg = 2.) |
| `away_free_text` | String | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `away_winner_flg` | Int64 | Result for this side: 1 = won, -1 = lost (integer; a 0 tie code is unobserved). |
| `away_score` | Int64 | Away points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `away_score_p1` | Int64 | Points scored IN the 1st quarter (not cumulative). |
| `away_score_p2` | Int64 | Points scored in the 2nd quarter. |
| `away_score_p3` | Int64 | Points scored in the 3rd quarter. |
| `away_score_p4` | Int64 | Points scored in the 4th quarter. |
| `away_score_ot1` | Int64 | Points scored in the 1st overtime (5 min); null when there was none. |
| `away_score_ot2` | Int64 | Unobserved: null in every sample. When filled: Points in the 2nd overtime; null when none. |
| `away_score_ot3` | Int64 | Unobserved: null in every sample. When filled: Points in the 3rd overtime; null when none. |
| `away_score_ot4` | Int64 | Unobserved: null in every sample. When filled: Points in the 4th overtime; null when none. There is no score_ot5+: overtimes 5-10 appear only as play-by-play period 9-14 and in the /pbp/team-stats and /pbp/player-stats periods maps. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_game-example}

```python
wjbl_game(play_schedule_id=7987)
```

## wjbl_period_scores

Points of each side in each period played (quarters and overtimes) from the game header.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/play-schedule/detail`

**Valid URL:** [https://api.wjbl.01core.app/api/play-schedule/detail?play_schedule_id=7987](https://api.wjbl.01core.app/api/play-schedule/detail?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | Int64 | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `team_side` | String | Which side of the game the row belongs to: home or away (the API's home / away blocks). |
| `team_id` | Int64 | Franchise id team_id of the side (stable across seasons and sponsor renames; 13 = Toyota Antelopes). |
| `period` | Int64 | Period number: 1-4 are the quarters and 5-8 the first to fourth overtime (score_ot1 to score_ot4). |
| `period_label` | String | Readable period: Q1-Q4 quarters, OT1-OT10 overtimes (period categories 5-14), H1 / H2 halves (15 / 16), OT all overtimes together (17), GAME the whole game (18). |
| `score` | Int64 | Points the side scored in that period (null periods, such as overtimes not played, have no row). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_period_scores-example}

```python
wjbl_period_scores(play_schedule_id=7987)
```

## wjbl_box_score

Box score of a final game (2013-14 on): one row per player of both teams.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/boxscore`

**Valid URL:** [https://api.wjbl.01core.app/api/boxscore?play_schedule_id=7987](https://api.wjbl.01core.app/api/boxscore?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | Int64 | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `team_side` | String | Which side of the game the row belongs to: home or away (the API's home / away blocks). |
| `team_id` | Int64 | Franchise id team_id of the side (stable across seasons and sponsor renames; 13 = Toyota Antelopes). |
| `player_id` | Int64 | Player id player_id (key space: W League persons, STABLE across seasons and teams). The player_id of /player and the pi= of the site's /player/detail?pi=; joins the play-by-play player_id1 / player_id2. |
| `player_name` | String | Player name in Japanese, 'SURNAME GIVEN' with a space (foreign players in katakana). |
| `player_number` | String | Shirt number, a string. |
| `gs` | Int64 | Game started: 1 = starter, 0 = bench (integer). |
| `pts` | Int64 | Points. Integer. |
| `p2m` | Int64 | Two-point field goals made. Integer. |
| `p2a` | Int64 | Two-point field goals attempted. Integer. |
| `p3m` | Int64 | Three-point field goals made. Integer. |
| `p3a` | Int64 | Three-point field goals attempted. Integer. |
| `ftm` | Int64 | Free throws made. Integer. |
| `fta` | Int64 | Free throws attempted. Integer. |
| `f` | Int64 | Personal fouls committed. Integer. |
| `off` | Int64 | Offensive rebounds. Integer. |
| `def` | Int64 | Defensive rebounds. Integer. |
| `tot` | Int64 | Total rebounds (off + def). Integer. |
| `ast` | Int64 | Assists. Integer. |
| `stl` | Int64 | Steals. Integer. |
| `blk` | Int64 | Blocked shots (made by this player / team). Integer. |
| `to` | Int64 | Turnovers. Integer. |
| `min` | String | Minutes played 'mm:ss' ('22:19'), a string. |
| `min_seconds` | Int64 | Minutes played in whole seconds, from the 'mm:ss' clock string min (null when it does not parse). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_box_score-example}

```python
wjbl_box_score(play_schedule_id=7987)
```

## wjbl_pbp

Play-by-play of a final game (2018-19 on): one row per play, Japanese action vocabulary, no shot x/y.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/pbp/play-by-play`

**Valid URL:** [https://api.wjbl.01core.app/api/pbp/play-by-play?play_schedule_id=7987](https://api.wjbl.01core.app/api/pbp/play-by-play?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | Int64 | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `no` | Int64 | Play sequence number, ascending, 1 = first; numbers can skip (deleted plays: 443 numbers for 439 rows on 7987). |
| `period` | Int64 | Period: 1-4 = quarters, 5-14 = overtime 1-10 (the site's tab map). Integer. |
| `rest_time` | String | Game clock REMAINING in the period, 'm:ss' without a leading zero ('9:38'; '10:00' at the start, 5:00 in overtime). |
| `seconds_remaining` | Int64 | Seconds remaining in the period, from the 'm:ss' clock string rest_time. |
| `team_id` | Int64 | Acting team: franchise id team_id; null on period and game-end lines. |
| `team_name` | String | Acting team name, Japanese (may contain a full-width space and differ from the box score's spelling); empty on period/game-end lines. |
| `team_side` | String | Side of the acting team, home or away (from home_away: 1 home, 2 away); null on period and game-end lines. |
| `player_number1` | String | Shirt number of the first actor (the player going OUT on a substitution), a string; '' on team and period lines. |
| `player_number2` | String | Shirt number of the second actor (the player coming IN on a substitution), a string; '' otherwise. |
| `player_name1` | String | Name of the first actor, Japanese; '' on team and period lines. |
| `player_name2` | String | Name of the second actor (substitution in), Japanese; '' otherwise. |
| `player_id1` | Int64 | First actor (the player going OUT on a substitution): Player id (key space: W League persons, STABLE across seasons and teams). Joins /boxscore home/away.players[].player_id and the player_id of /player (the pi= of the site's /player/detail?pi=). Null on team and period lines. |
| `player_id2` | Int64 | Second actor, filled on substitutions only (the player coming IN; verified 89/89 on game 7987): Player id (key space: W League persons, STABLE across seasons and teams). Joins /boxscore home/away.players[].player_id and the player_id of /player (the pi= of the site's /player/detail?pi=). Null otherwise. |
| `play_text` | String | Display text in Japanese: '#11 岡本 2Pシュート○(2点)'. Bracketed running counts: shots '(N点)' = the player's points so far, assists/steals/blocks/turnovers '(N本)' = count so far, rebounds '(o-d-t)' = the player's offensive-defensive-total so far, fouls '(a-b:c)' = a the player's personal fouls, b the team's fouls in the period, c free throws awarded (verified on 7987), substitution '#15 A → #7 B' = A out, B in. Empty on period and game-end lines. |
| `score` | String | Score 'HOME-AWAY' after the play ('4-3'); filled on scoring plays and period/game-end lines, '' otherwise. |
| `home_score` | Int64 | Home points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `away_score` | Int64 | Away points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `end_period_flg` | Int64 | End-of-period flag (integer); 0 on every observed row, the period end is the 'クォーターエンド' action instead. |
| `end_game_flg` | Int64 | End-of-game flag: 1 on the final '試合終了' line only (the site skips that row), else 0. |
| `action1` | String | Action in Japanese (vocabulary of 8 full games, 2018-19 to 2025-26): 'プレイヤーイン' = player in (the starting five at the start of the game), 'プレイヤーインアウト' = substitution (player_id1 OUT, player_id2 IN), '2Pシュート アウトサイドペイント○' / '×' = two-point shot outside the paint made / missed ('インサイドペイント' inside-the-paint is not in the sample games), '3Pシュート○' / '×' = three-point shot made / missed, 'フリースロー○' / '×' = free throw made / missed, 'アシスト' = assist, 'オフェンスリバウンド' = offensive rebound, 'ディフェンスリバウンド' = defensive rebound, 'チームオフェンスリバウンド' / 'チームディフェンスリバウンド' = team offensive / defensive rebound, 'スティール' = steal, 'ブロックショット' = blocked shot, 'ターンオーバー' = turnover, 'チームターンオーバー' = team turnover, 'パーソナルファウル' = personal foul, 'オフェンスファウル' = offensive foul, 'アンスポーツマンファウル' = unsportsmanlike foul, 'タイムアウト' = timeout, 'クォーターエンド' = end of period (quarter or overtime), '試合終了' = end of game. ○ = made, × = missed. |
| `action2` | String | Sub-action in Japanese: 'ジャンプショット' = jump shot (on field-goal attempts), 'フリースローオンファウルN' = free throws awarded on this foul, N = 0-3; '' otherwise. |
| `action3` | String | Second qualifier in Japanese: 'シュートファウル' = shooting foul; '' otherwise. |
| `home_away` | Int64 | Side of the acting team: 1 = home, 2 = away; null on period/game-end lines. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_pbp-example}

```python
wjbl_pbp(play_schedule_id=7987)
```

## wjbl_standing_seasons

Every season phase (season_id) with its edition name and standings flags.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/seasons/standings`

**Valid URL:** [https://api.wjbl.01core.app/api/seasons/standings](https://api.wjbl.01core.app/api/seasons/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#wjbl_standing_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_full_name` | String | Edition name with the sponsor, Japanese, e.g. '26-27 Wリーグ 大樹生命 Wリーグ ユナイテッド' (truncated by the API). |
| `season` | Int64 | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. Read from the start of season_full_name. |
| `season_league_name` | String | Phase / division label in Japanese: 'プレミア' = Premier, 'フューチャー' = Future, 'グループステージ' = group stage, '2次ラウンド 下位リーグ' = second round, lower league; empty for a single-table phase. |
| `team_standings_flg` | Int64 | 1 = the phase has a standings grid (/team-standings), 0 = not (cups, all-star games, phases not started). |
| `season_tournament_text_flg` | Int64 | 1 = the phase shows a tournament text instead of a table, 0 = not; 0 in every sample. |
| `season_tournament_text` | String | Tournament text (HTML) for knockout phases; empty in every sample. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_standing_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_standing_seasons-example}

```python
wjbl_standing_seasons()
```

## wjbl_head_to_head

Head-to-head results grid (hoshitori-hyo) of one or more season phases: one row per game played.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/team-standings`

**Valid URL:** [https://api.wjbl.01core.app/api/team-standings?season_ids=169](https://api.wjbl.01core.app/api/team-standings?season_ids=169)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_ids` | `season_ids` |  | `Y` |  | Season phase id or ids (season_id in wjbl_standing_seasons; a phase inside a league edition, not a year): an integer, a list or a comma string such as '169,170' (2025-26 regular season Premier and Future divisions). |

### Returns {#wjbl_head_to_head-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Season phase id of the grid, e.g. 169 (2025-26 regular season, Premier division). |
| `team_id` | Int64 | Franchise id team_id of the ROW team (the team whose points are win). |
| `opponent_team_id` | Int64 | Franchise id team_id of the opponent in that game. |
| `game_number` | Int64 | 1-based order of the game within the pair of teams in the grid (the wire list has no number). |
| `win` | Int64 | The ROW team's points in that game (a string on the wire, whatever the result; 56 on a loss). |
| `losg` | Int64 | The OPPONENT's points in that game (a string on the wire; the wire key is spelled losg). |
| `result` | String | '○ 63-56' = row team won, '● 56-63' = lost (row team's points first); '○ 20-0' is a forfeit win (FIBA forfeit score). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_head_to_head-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_head_to_head-example}

```python
wjbl_head_to_head(season_ids='169')
```

## wjbl_roster

Players of a team in a season phase, with the head coach.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/team/roster`

**Valid URL:** [https://api.wjbl.01core.app/api/team/roster?team_id=13&season_id=169](https://api.wjbl.01core.app/api/team/roster?team_id=13&season_id=169)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Franchise id (team_id in wjbl_teams): stable across seasons and sponsor renames, e.g. 13 (Toyota Antelopes). |
| `season_id` | `season_id` |  |  | `Y` | Season phase id (season_id in wjbl_standing_seasons), e.g. 169 (2025-26 regular season, Premier division). |

### Returns {#wjbl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | String | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | Int64 | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | Int64 | The season_id asked for: Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_full_name` | String | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `season_team_id` | Int64 | Team-season registration id (key space: one per team per season phase; 1343 = Toyota in season 169). Not team_id. |
| `team_id` | Int64 | The team_id asked for: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `coach_id` | Int64 | Head-coach id (key space: W League coaches; the coach_id of /coach and of the site's /coach/?coach_id=). |
| `coach_name` | String | Head coach name, Japanese. |
| `team_comment` | String | Free text the league shows with the roster (captains and other notes, HTML with <br>), as sent. |
| `player_id` | Int64 | Player id player_id (key space: W League persons, STABLE across seasons and teams). The player_id of /player and the pi= of the site's /player/detail?pi=; joins the play-by-play player_id1 / player_id2. |
| `player_name` | String | Player name, Japanese. |
| `player_number` | String | Shirt number, a string. |
| `captain_flg` | Int64 | Captain: 1 = yes, 0 = no. |
| `sub_captain_flg` | Int64 | Vice-captain: 1 = yes, 0 = no. |
| `transfer_flg` | Int64 | Transferred in this season (移籍 icon): 1 = yes, 0 = no. |
| `position` | String | Playing position (G / F / C style code); empty in every sampled row. |
| `rookie` | Int64 | Rookie (ルーキー icon): 1 = yes, 0 = no. |
| `early_entry` | Int64 | Early-entry player (アーリーエントリー, a student who joined before graduating): 1 = yes, 0 = no. |
| `picture` | String | Player photo path relative to https://www.wjbl.org. |
| `foreign_national_flg` | Int64 | Foreign national (外国籍 icon): 1 = yes, 0 = no. |
| `naturalized_flg` | Int64 | Naturalised player (帰化 icon): 1 = yes, 0 = no. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_roster-example}

```python
wjbl_roster(season_id=169, team_id=13)
```

## wjbl_staff

Staff of a team in a season phase (general manager, assistant coaches, support staff).

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/team/roster`

**Valid URL:** [https://api.wjbl.01core.app/api/team/roster?team_id=13&season_id=169](https://api.wjbl.01core.app/api/team/roster?team_id=13&season_id=169)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Franchise id (team_id in wjbl_teams): stable across seasons and sponsor renames, e.g. 13 (Toyota Antelopes). |
| `season_id` | `season_id` |  |  | `Y` | Season phase id (season_id in wjbl_standing_seasons), e.g. 169 (2025-26 regular season, Premier division). |

### Returns {#wjbl_staff-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | String | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | Int64 | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | Int64 | The season_id asked for: Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_full_name` | String | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `season_team_id` | Int64 | Team-season registration id (key space: one per team per season phase; 1343 = Toyota in season 169). Not team_id. |
| `team_id` | Int64 | The team_id asked for: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `staff_name` | String | Staff member name, Japanese. |
| `position` | String | Staff role, Japanese: アソシエイトヘッドコーチ associate head coach, アシスタントコーチ assistant coach, ゼネラルマネージャー general manager, 総括 team director, サポートスタッフ support staff. |
| `url` | String | Link attached to the staff entry; empty in most rows. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_staff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_staff-example}

```python
wjbl_staff(season_id=169, team_id=13)
```
