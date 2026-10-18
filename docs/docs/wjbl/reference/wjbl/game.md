---
title: "WJBL — W League API (api.wjbl.01core.app, Japan women) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "WJBL — W League API (api.wjbl.01core.app, Japan women) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WJBL — W League API (api.wjbl.01core.app, Japan women) — Game

## wjbl_game_months

Months (YYYY-MM) that have games in a season, newest first; each is one page of wjbl_schedule.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/play-schedule-months`

**Valid URL:** [https://api.wjbl.01core.app/api/play-schedule-months?season=2026](https://api.wjbl.01core.app/api/play-schedule-months?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, 2000 = 1999-2000); the wrapper maps it to the league edition id (league_id) with the live league list (wjbl_leagues, one extra cached request). |

### Returns {#wjbl_game_months-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | integer | Competition edition id league_id, cast to an integer (the API echoes a string here and an integer elsewhere). |
| `season` | integer | Season END year of the request (2026 = 2025-26); the body itself does not carry it. |
| `game_month` | character | Month 'YYYY-MM': the game_month parameter of /play-schedules. |
| `game_month_jpn` | character | The month in Japanese, 'YYYY年M月' ('2026年4月' = April 2026). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_game_months-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | a season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_game_months-example}

```python
wjbl_game_months(season=2026)
```

_Last validated n/a._

## wjbl_game_info

Game information of a final game: venue, times, referees, links and the Japanese game report, one row.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/play-schedule`

**Valid URL:** [https://api.wjbl.01core.app/api/play-schedule?play_schedule_id=7987](https://api.wjbl.01core.app/api/play-schedule?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `league_id` | integer | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | character | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | integer | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_title` | character | Phase title in Japanese: 'レギュラーシーズン' = regular season, 'プレーオフ　ファイナル' / 'プレーオフ／ファイナル' = playoff final, 'プレーオフ／セミファイナル' = playoff semifinal (the separator varies by era). |
| `season_full_name` | character | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `game_date` | character | Game date in Japanese, 'YYYY年M月D日(曜)' ('2026年4月4日(土)' = Saturday 4 April 2026). |
| `game_day` | character | Game date as an ISO date (YYYY-MM-DD, Japan time), built from the game year and the game date. |
| `game_start_time` | character | Scheduled tip-off 'HH:MM', Japan time (JST, UTC+9). |
| `started_at` | character | Actual tip-off 'HH:MM', JST, as recorded ('15:01'); junk on 2013-14 games ('05:06' for a 19:00 game, and it CHANGES between requests: 05:06 / 05:16 / 05:25 for game 4243), so ignore it there. |
| `opening_time` | character | Doors-open time 'HH:MM', JST; empty when not published. |
| `venue` | character | Arena name, Japanese. |
| `game_icon` | integer | Icon code shown next to the game: 0 = none, 1 = home-game icon (home.svg), 2 = neutral-site icon (neutral.svg). |
| `venue_address` | character | Arena postal address, Japanese. |
| `venue_others` | character | Access directions to the arena (nearest station, walking time), Japanese free text. |
| `allow_shoes_on` | integer | Outdoor shoes allowed in the arena (土足): 1 = not stated, 2 = yes (◯), 3 or 4 = no (×). Integer. |
| `allow_shoes_on_others` | character | Extra note on outdoor shoes; empty string in every sample. |
| `ticket_info` | character | Ticket information, Japanese free text with CRLF line breaks and HTML links. |
| `ticket_url` | character | Ticket sale URL; empty string in every sample. |
| `event_info` | character | Event-day information (gate times, food, shoes, tip-off), Japanese free text with CRLF line breaks. |
| `live_link` | character | Live-stream URL; empty when none. |
| `vod_link` | character | Video-on-demand URL (Basketball LIVE / W-TV); empty when none. |
| `bs_url` | character | Satellite-TV (BS) broadcast URL; empty string in every sample. |
| `cs_url` | character | Cable/satellite (CS) broadcast URL; empty string in every sample. |
| `ground_url` | character | Terrestrial-TV broadcast URL; empty string in every sample. |
| `other_url` | character | Other broadcast URL; empty string in every sample. |
| `cancel_flg` | integer | Cancelled flag: 1 = cancelled, 0 = not. |
| `delay_flg` | integer | Postponed flag: 1 = postponed, 0 = not. |
| `game_status` | character | Game state: 'fixed' = final (confirmed result), 'scheduled' = not played yet (seen on 26-27 games); the site's JS also handles 'live', 'canceled' and 'delayed'. The capture banks only 'fixed' games. |
| `box_flg` | integer | Box-score flag (integer). 1 on every sampled game, scheduled ones included, so it does NOT prove a box score exists. |
| `pbp_flg` | integer | Play-by-play flag: 1 = /pbp/* routes have data (2018-19 onward), 0 = not (they answer []). |
| `referee_name` | character | Crew chief (主審 / クルーチーフ), 'SURNAME GIVEN' in Japanese; empty on 2013-14 (names are in comment); placeholder '審判01' (= 'referee 01') on game 5533. |
| `sub_referee_name1` | character | First umpire (副審 / ファーストアンパイア), Japanese name; empty when not recorded. |
| `sub_referee_name2` | character | Second umpire (セカンドアンパイア), Japanese name; empty when not recorded. |
| `comment` | character | Officials block, Japanese free text: 'クルーチーフ：' crew chief, 'ファーストアンパイア：' first umpire, 'セカンドアンパイア：' second umpire, 2013-14 '主審' referee / '副審' umpires / 'コミッショナー' commissioner; CRLF or &lt;br> separated. |
| `report` | character | Game report (quarter-by-quarter narrative) in Japanese HTML; filled on 2013-14, empty on the newer samples. |
| `home_team_id` | integer | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `home_season_team_name` | character | Home team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `home_season_team_short_name` | character | Home team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `home_team_logo_image` | character | Home team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `home_free_text` | character | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `away_team_id` | integer | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_season_team_name` | character | Away team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `away_season_team_short_name` | character | Away team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `away_team_logo_image` | character | Away team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `away_free_text` | character | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_game_info-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | a season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_game_info-example}

```python
wjbl_game_info(play_schedule_id=7987)
```

_Last validated n/a._
