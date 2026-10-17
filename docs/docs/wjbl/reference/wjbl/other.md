---
title: "WJBL — W League API (api.wjbl.01core.app, Japan women) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "WJBL — W League API (api.wjbl.01core.app, Japan women) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WJBL — W League API (api.wjbl.01core.app, Japan women) — Other

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
| `league_id` | integer | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | character | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_leagues-example}

```python
wjbl_leagues()
```

_Last validated n/a._

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
| `league_id` | integer | The league_id asked for (integer): Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | character | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | integer | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_title` | character | Phase title in Japanese: 'レギュラーシーズン' = regular season, 'プレーオフ　ファイナル' / 'プレーオフ／ファイナル' = playoff final, 'プレーオフ／セミファイナル' = playoff semifinal (the separator varies by era). |
| `season_full_name` | character | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `game_year` | integer | Calendar year of the game, 'YYYY' string (not the season); parsed to an integer. |
| `game_date` | character | Game day 'MM.DD' ('04.04'); with game_year it is the date, Japan time. |
| `game_day` | character | Game day 'MM.DD' ('04.04'); with game_year it is the date, Japan time. |
| `game_date_week_day_name` | character | Weekday in Japanese, one character: 月 Mon, 火 Tue, 水 Wed, 木 Thu, 金 Fri, 土 Sat, 日 Sun. |
| `game_start_time` | character | Scheduled tip-off 'HH:MM', Japan time (JST, UTC+9). |
| `started_at` | character | Actual tip-off 'HH:MM', JST, as recorded ('15:01'); junk on 2013-14 games ('05:06' for a 19:00 game, and it CHANGES between requests: 05:06 / 05:16 / 05:25 for game 4243), so ignore it there. |
| `opening_time` | character | Doors-open time 'HH:MM', JST; empty when not published. |
| `venue` | character | Arena name, Japanese. |
| `venue_address` | character | Arena postal address, Japanese. |
| `game_icon` | integer | Icon code shown next to the game: 0 = none, 1 = home-game icon (home.svg), 2 = neutral-site icon (neutral.svg). |
| `ticket_url` | character | Ticket sale URL; empty string in every sample. |
| `vod_link` | character | Video-on-demand URL (Basketball LIVE / W-TV); empty when none. |
| `live_link` | character | Live-stream URL; empty when none. |
| `game_status` | character | Game state: 'fixed' = final (confirmed result), 'scheduled' = not played yet (seen on 26-27 games); the site's JS also handles 'live', 'canceled' and 'delayed'. The capture banks only 'fixed' games. |
| `box_flg` | integer | Box-score flag (integer). 1 on every sampled game, scheduled ones included, so it does NOT prove a box score exists. |
| `pbp_flg` | integer | Play-by-play flag: 1 = /pbp/* routes have data (2018-19 onward), 0 = not (they answer []). |
| `cancel_flg` | integer | Cancelled flag: 1 = cancelled, 0 = not. |
| `delay_flg` | integer | Postponed flag: 1 = postponed, 0 = not. |
| `ot_flg` | integer | Overtime flag: 1 = the game went to overtime, 0 = not. |
| `home_team_id` | integer | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `home_season_team_name` | character | Home team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `home_season_team_short_name` | character | Home team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `home_team_logo_image` | character | Home team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `home_team_free_text` | character | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `home_team_score` | integer | Home final points (integer). Every sampled game is finished, so null is not in the type; unplayed games (2026-27 'scheduled') carry null. |
| `home_team_entry_flg` | integer | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `away_team_id` | integer | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_season_team_name` | character | Away team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `away_season_team_short_name` | character | Away team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `away_team_logo_image` | character | Away team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `away_team_free_text` | character | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `away_team_score` | integer | Away final points (integer). Null on unplayed games, as for home_team_score (not in the finished samples). |
| `away_team_entry_flg` | integer | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_schedule-example}

```python
wjbl_schedule(game_month='2026-04', season=2026)
```

_Last validated n/a._

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
| `play_schedule_id` | integer | Game id: ONE global sequence across every league edition and season (7987 = 2025-26 playoff final game 1); the play_schedule_id of the request. |
| `game_status` | character | Game state; always fixed (final) here, the wrapper refuses a game that is not final. |
| `home_team_id` | integer | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `home_season_team_name` | character | Home team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `home_season_team_short_name` | character | Home team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `home_team_logo_image` | character | Home team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `home_team_entry_flg` | integer | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `home_season_team_area` | character | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's standings JS: 0 = East, 1 = West conference when season_league_flg = 2.) |
| `home_free_text` | character | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `home_winner_flg` | integer | Result for this side: 1 = won, -1 = lost (integer; a 0 tie code is unobserved). |
| `home_score` | integer | Home points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `home_score_p1` | integer | Points scored IN the 1st quarter (not cumulative). |
| `home_score_p2` | integer | Points scored in the 2nd quarter. |
| `home_score_p3` | integer | Points scored in the 3rd quarter. |
| `home_score_p4` | integer | Points scored in the 4th quarter. |
| `home_score_ot1` | integer | Points scored in the 1st overtime (5 min); null when there was none. |
| `home_score_ot2` | integer | Unobserved: null in every sample. When filled: Points in the 2nd overtime; null when none. |
| `home_score_ot3` | integer | Unobserved: null in every sample. When filled: Points in the 3rd overtime; null when none. |
| `home_score_ot4` | integer | Unobserved: null in every sample. When filled: Points in the 4th overtime; null when none. There is no score_ot5+: overtimes 5-10 appear only as play-by-play period 9-14 and in the /pbp/team-stats and /pbp/player-stats periods maps. |
| `away_team_id` | integer | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_season_team_name` | character | Away team name: Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `away_season_team_short_name` | character | Away team short name: Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `away_team_logo_image` | character | Away team logo: Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `away_team_entry_flg` | integer | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `away_season_team_area` | character | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's standings JS: 0 = East, 1 = West conference when season_league_flg = 2.) |
| `away_free_text` | character | Free-text team label used when the slot has no team (e.g. 'winner of game X'); empty string in every sample. |
| `away_winner_flg` | integer | Result for this side: 1 = won, -1 = lost (integer; a 0 tie code is unobserved). |
| `away_score` | integer | Away points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `away_score_p1` | integer | Points scored IN the 1st quarter (not cumulative). |
| `away_score_p2` | integer | Points scored in the 2nd quarter. |
| `away_score_p3` | integer | Points scored in the 3rd quarter. |
| `away_score_p4` | integer | Points scored in the 4th quarter. |
| `away_score_ot1` | integer | Points scored in the 1st overtime (5 min); null when there was none. |
| `away_score_ot2` | integer | Unobserved: null in every sample. When filled: Points in the 2nd overtime; null when none. |
| `away_score_ot3` | integer | Unobserved: null in every sample. When filled: Points in the 3rd overtime; null when none. |
| `away_score_ot4` | integer | Unobserved: null in every sample. When filled: Points in the 4th overtime; null when none. There is no score_ot5+: overtimes 5-10 appear only as play-by-play period 9-14 and in the /pbp/team-stats and /pbp/player-stats periods maps. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_game-example}

```python
wjbl_game(play_schedule_id=7987)
```

_Last validated n/a._

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
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `team_side` | character | Which side of the game the row belongs to: home or away (the API's home / away blocks). |
| `team_id` | integer | Franchise id team_id of the side (stable across seasons and sponsor renames; 13 = Toyota Antelopes). |
| `period` | integer | Period number: 1-4 are the quarters and 5-8 the first to fourth overtime (score_ot1 to score_ot4). |
| `period_label` | character | Readable period: Q1-Q4 quarters, OT1-OT10 overtimes (period categories 5-14), H1 / H2 halves (15 / 16), OT all overtimes together (17), GAME the whole game (18). |
| `score` | integer | Points the side scored in that period (null periods, such as overtimes not played, have no row). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_period_scores-example}

```python
wjbl_period_scores(play_schedule_id=7987)
```

_Last validated n/a._

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
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `team_side` | character | Which side of the game the row belongs to: home or away (the API's home / away blocks). |
| `team_id` | integer | Franchise id team_id of the side (stable across seasons and sponsor renames; 13 = Toyota Antelopes). |
| `player_id` | integer | Player id player_id (key space: W League persons, STABLE across seasons and teams). The player_id of /player and the pi= of the site's /player/detail?pi=; joins the play-by-play player_id1 / player_id2. |
| `player_name` | character | Player name in Japanese, 'SURNAME GIVEN' with a space (foreign players in katakana). |
| `player_number` | character | Shirt number, a string. |
| `gs` | integer | Game started: 1 = starter, 0 = bench (integer). |
| `pts` | integer | Points. Integer. |
| `p2m` | integer | Two-point field goals made. Integer. |
| `p2a` | integer | Two-point field goals attempted. Integer. |
| `p3m` | integer | Three-point field goals made. Integer. |
| `p3a` | integer | Three-point field goals attempted. Integer. |
| `ftm` | integer | Free throws made. Integer. |
| `fta` | integer | Free throws attempted. Integer. |
| `f` | integer | Personal fouls committed. Integer. |
| `off` | integer | Offensive rebounds. Integer. |
| `def` | integer | Defensive rebounds. Integer. |
| `tot` | integer | Total rebounds (off + def). Integer. |
| `ast` | integer | Assists. Integer. |
| `stl` | integer | Steals. Integer. |
| `blk` | integer | Blocked shots (made by this player / team). Integer. |
| `to` | integer | Turnovers. Integer. |
| `min` | character | Minutes played 'mm:ss' ('22:19'), a string. |
| `min_seconds` | integer | Minutes played in whole seconds, from the 'mm:ss' clock string min (null when it does not parse). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_box_score-example}

```python
wjbl_box_score(play_schedule_id=7987)
```

_Last validated n/a._

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
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `no` | integer | Play sequence number, ascending, 1 = first; numbers can skip (deleted plays: 443 numbers for 439 rows on 7987). |
| `period` | integer | Period: 1-4 = quarters, 5-14 = overtime 1-10 (the site's tab map). Integer. |
| `rest_time` | character | Game clock REMAINING in the period, 'm:ss' without a leading zero ('9:38'; '10:00' at the start, 5:00 in overtime). |
| `seconds_remaining` | integer | Seconds remaining in the period, from the 'm:ss' clock string rest_time. |
| `team_id` | integer | Acting team: franchise id team_id; null on period and game-end lines. |
| `team_name` | character | Acting team name, Japanese (may contain a full-width space and differ from the box score's spelling); empty on period/game-end lines. |
| `team_side` | character | Side of the acting team, home or away (from home_away: 1 home, 2 away); null on period and game-end lines. |
| `player_number1` | character | Shirt number of the first actor (the player going OUT on a substitution), a string; '' on team and period lines. |
| `player_number2` | character | Shirt number of the second actor (the player coming IN on a substitution), a string; '' otherwise. |
| `player_name1` | character | Name of the first actor, Japanese; '' on team and period lines. |
| `player_name2` | character | Name of the second actor (substitution in), Japanese; '' otherwise. |
| `player_id1` | integer | First actor (the player going OUT on a substitution): Player id (key space: W League persons, STABLE across seasons and teams). Joins /boxscore home/away.players[].player_id and the player_id of /player (the pi= of the site's /player/detail?pi=). Null on team and period lines. |
| `player_id2` | integer | Second actor, filled on substitutions only (the player coming IN; verified 89/89 on game 7987): Player id (key space: W League persons, STABLE across seasons and teams). Joins /boxscore home/away.players[].player_id and the player_id of /player (the pi= of the site's /player/detail?pi=). Null otherwise. |
| `play_text` | character | Display text in Japanese: '#11 岡本 2Pシュート○(2点)'. Bracketed running counts: shots '(N点)' = the player's points so far, assists/steals/blocks/turnovers '(N本)' = count so far, rebounds '(o-d-t)' = the player's offensive-defensive-total so far, fouls '(a-b:c)' = a the player's personal fouls, b the team's fouls in the period, c free throws awarded (verified on 7987), substitution '#15 A → #7 B' = A out, B in. Empty on period and game-end lines. |
| `score` | character | Score 'HOME-AWAY' after the play ('4-3'); filled on scoring plays and period/game-end lines, '' otherwise. |
| `home_score` | integer | Home points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `away_score` | integer | Away points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score. |
| `end_period_flg` | integer | End-of-period flag (integer); 0 on every observed row, the period end is the 'クォーターエンド' action instead. |
| `end_game_flg` | integer | End-of-game flag: 1 on the final '試合終了' line only (the site skips that row), else 0. |
| `action1` | character | Action in Japanese (vocabulary of 8 full games, 2018-19 to 2025-26): 'プレイヤーイン' = player in (the starting five at the start of the game), 'プレイヤーインアウト' = substitution (player_id1 OUT, player_id2 IN), '2Pシュート アウトサイドペイント○' / '×' = two-point shot outside the paint made / missed ('インサイドペイント' inside-the-paint is not in the sample games), '3Pシュート○' / '×' = three-point shot made / missed, 'フリースロー○' / '×' = free throw made / missed, 'アシスト' = assist, 'オフェンスリバウンド' = offensive rebound, 'ディフェンスリバウンド' = defensive rebound, 'チームオフェンスリバウンド' / 'チームディフェンスリバウンド' = team offensive / defensive rebound, 'スティール' = steal, 'ブロックショット' = blocked shot, 'ターンオーバー' = turnover, 'チームターンオーバー' = team turnover, 'パーソナルファウル' = personal foul, 'オフェンスファウル' = offensive foul, 'アンスポーツマンファウル' = unsportsmanlike foul, 'タイムアウト' = timeout, 'クォーターエンド' = end of period (quarter or overtime), '試合終了' = end of game. ○ = made, × = missed. |
| `action2` | character | Sub-action in Japanese: 'ジャンプショット' = jump shot (on field-goal attempts), 'フリースローオンファウルN' = free throws awarded on this foul, N = 0-3; '' otherwise. |
| `action3` | character | Second qualifier in Japanese: 'シュートファウル' = shooting foul; '' otherwise. |
| `home_away` | integer | Side of the acting team: 1 = home, 2 = away; null on period/game-end lines. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_pbp-example}

```python
wjbl_pbp(play_schedule_id=7987)
```

_Last validated n/a._

## wjbl_player_period_leaders

Leading scorer, passer and rebounder of each team per period, half and game (2018-19 on).

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/pbp/player-stats`

**Valid URL:** [https://api.wjbl.01core.app/api/pbp/player-stats?play_schedule_id=7987](https://api.wjbl.01core.app/api/pbp/player-stats?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_player_period_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `home_team_id` | integer | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_team_id` | integer | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `period_category` | integer | The period category again, as an integer (camelCase here, period_category on /pbp/team-stats). |
| `period_label` | character | Readable period: Q1-Q4 quarters, OT1-OT10 overtimes (period categories 5-14), H1 / H2 halves (15 / 16), OT all overtimes together (17), GAME the whole game (18). |
| `home_point_player` | character | Home points leader(s) in the period as '#NUMBER NAME' (Japanese); ties are joined with ' , '. |
| `home_point_value` | character | Their points with the counter 点 ('5点'); ties joined with ' , ' in the same order. |
| `home_assist_player` | character | Home assist leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `home_assist_value` | character | Their assists with the counter 本 ('2本'), ' , '-joined for ties. |
| `home_rebound_player` | character | Home rebound leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `home_rebound_value` | character | Their rebounds 'offensive-defensive-total' ('0-3-3'), ' , '-joined for ties. |
| `away_point_player` | character | Away points leader(s) in the period as '#NUMBER NAME' (Japanese); ties are joined with ' , '. |
| `away_point_value` | character | Their points with the counter 点 ('5点'); ties joined with ' , ' in the same order. |
| `away_assist_player` | character | Away assist leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `away_assist_value` | character | Their assists with the counter 本 ('2本'), ' , '-joined for ties. |
| `away_rebound_player` | character | Away rebound leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `away_rebound_value` | character | Their rebounds 'offensive-defensive-total' ('0-3-3'), ' , '-joined for ties. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_player_period_leaders-example}

```python
wjbl_player_period_leaders(play_schedule_id=7987)
```

_Last validated n/a._

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
| `season_id` | integer | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_full_name` | character | Edition name with the sponsor, Japanese, e.g. '26-27 Wリーグ 大樹生命 Wリーグ ユナイテッド' (truncated by the API). |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. Read from the start of season_full_name. |
| `season_league_name` | character | Phase / division label in Japanese: 'プレミア' = Premier, 'フューチャー' = Future, 'グループステージ' = group stage, '2次ラウンド 下位リーグ' = second round, lower league; empty for a single-table phase. |
| `team_standings_flg` | integer | 1 = the phase has a standings grid (/team-standings), 0 = not (cups, all-star games, phases not started). |
| `season_tournament_text_flg` | integer | 1 = the phase shows a tournament text instead of a table, 0 = not; 0 in every sample. |
| `season_tournament_text` | character | Tournament text (HTML) for knockout phases; empty in every sample. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_standing_seasons-example}

```python
wjbl_standing_seasons()
```

_Last validated n/a._

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
| `season_id` | integer | Season phase id of the grid, e.g. 169 (2025-26 regular season, Premier division). |
| `team_id` | integer | Franchise id team_id of the ROW team (the team whose points are win). |
| `opponent_team_id` | integer | Franchise id team_id of the opponent in that game. |
| `game_number` | integer | 1-based order of the game within the pair of teams in the grid (the wire list has no number). |
| `win` | integer | The ROW team's points in that game (a string on the wire, whatever the result; 56 on a loss). |
| `losg` | integer | The OPPONENT's points in that game (a string on the wire; the wire key is spelled losg). |
| `result` | character | '○ 63-56' = row team won, '● 56-63' = lost (row team's points first); '○ 20-0' is a forfeit win (FIBA forfeit score). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_head_to_head-example}

```python
wjbl_head_to_head(season_ids='169')
```

_Last validated n/a._

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
| `league_id` | integer | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | character | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | integer | The season_id asked for: Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_full_name` | character | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `season_team_id` | integer | Team-season registration id (key space: one per team per season phase; 1343 = Toyota in season 169). Not team_id. |
| `team_id` | integer | The team_id asked for: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `coach_id` | integer | Head-coach id (key space: W League coaches; the coach_id of /coach and of the site's /coach/?coach_id=). |
| `coach_name` | character | Head coach name, Japanese. |
| `team_comment` | character | Free text the league shows with the roster (captains and other notes, HTML with <br>), as sent. |
| `player_id` | integer | Player id player_id (key space: W League persons, STABLE across seasons and teams). The player_id of /player and the pi= of the site's /player/detail?pi=; joins the play-by-play player_id1 / player_id2. |
| `player_name` | character | Player name, Japanese. |
| `player_number` | character | Shirt number, a string. |
| `captain_flg` | integer | Captain: 1 = yes, 0 = no. |
| `sub_captain_flg` | integer | Vice-captain: 1 = yes, 0 = no. |
| `transfer_flg` | integer | Transferred in this season (移籍 icon): 1 = yes, 0 = no. |
| `position` | character | Playing position (G / F / C style code); empty in every sampled row. |
| `rookie` | integer | Rookie (ルーキー icon): 1 = yes, 0 = no. |
| `early_entry` | integer | Early-entry player (アーリーエントリー, a student who joined before graduating): 1 = yes, 0 = no. |
| `picture` | character | Player photo path relative to https://www.wjbl.org. |
| `foreign_national_flg` | integer | Foreign national (外国籍 icon): 1 = yes, 0 = no. |
| `naturalized_flg` | integer | Naturalised player (帰化 icon): 1 = yes, 0 = no. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_roster-example}

```python
wjbl_roster(season_id=169, team_id=13)
```

_Last validated n/a._

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
| `league_id` | integer | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | character | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | integer | The season_id asked for: Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_full_name` | character | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `season_team_id` | integer | Team-season registration id (key space: one per team per season phase; 1343 = Toyota in season 169). Not team_id. |
| `team_id` | integer | The team_id asked for: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `staff_name` | character | Staff member name, Japanese. |
| `position` | character | Staff role, Japanese: アソシエイトヘッドコーチ associate head coach, アシスタントコーチ assistant coach, ゼネラルマネージャー general manager, 総括 team director, サポートスタッフ support staff. |
| `url` | character | Link attached to the staff entry; empty in most rows. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_staff-example}

```python
wjbl_staff(season_id=169, team_id=13)
```

_Last validated n/a._

## wjbl_player

Player profile: current team, names, bio and career notes, one row.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/player`

**Valid URL:** [https://api.wjbl.01core.app/api/player?player_id=9799](https://api.wjbl.01core.app/api/player?player_id=9799)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id (player_id in wjbl_box_score or wjbl_roster): the person, stable across seasons, e.g. 9799. |

### Returns {#wjbl_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id player_id (key space: W League persons, STABLE across seasons and teams). The player_id of /player and the pi= of the site's /player/detail?pi=; joins the play-by-play player_id1 / player_id2. |
| `player_name` | character | Name in Japanese, 'SURNAME GIVEN'. |
| `player_name_en` | character | Name in romaji, uppercase, 'SURNAME GIVEN' ('OKAMOTO MIYUU'). |
| `player_furigana` | character | Name reading in hiragana ('おかもと みゆう'). |
| `team_id` | integer | Current (or last) team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `season_team_name` | character | Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `season_team_short_name` | character | Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `season_team_player_number` | character | Shirt number this season, a string. |
| `team_logo_image` | character | Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `team_url` | character | Team's own website URL. |
| `player_birthday` | character | Date of birth 'YYYY/MM/DD' (public player bio). |
| `player_nickname` | character | Nickname, Japanese. |
| `player_position` | character | Playing position code ('PG', 'SG', 'SF', 'PF', 'C'). |
| `player_home_town` | character | Home prefecture, Japanese ('北海道' = Hokkaido). |
| `player_height` | character | Height with unit, a string ('179cm'). |
| `player_weight` | character | Weight with unit, a string ('77kg'). |
| `player_height_cm` | integer | Height in whole centimetres, parsed from player_height ('179cm'). |
| `player_weight_kg` | integer | Weight in whole kilograms, parsed from player_weight ('77kg'). |
| `player_affiliation_history` | character | Career path, Japanese, '→'-separated (mini-basketball club → junior high → high school → university → team). |
| `player_independent_flg` | integer | Retired flag (the site greys the team and hides the number): 1 = retired / no longer registered, 0 = active. |
| `player_national_team` | character | National-team history, Japanese free text ('ユニバ' = Universiade). |
| `player_awards` | character | Awards and honours, Japanese free text. |
| `player_coach_history` | character | Coaching history (for players who became coaches); empty string in the sample. |
| `player_coach_record` | character | Coaching record; empty string in the sample. |
| `player_picture` | character | Photo path relative to https://www.wjbl.org. |
| `player_wjbl_flg` | integer | 1 = a W League player (0 presumably for others; unverified). |
| `player_rookie` | integer | Rookie (ルーキー icon): 1 = yes, 0 = no. |
| `player_early_entry` | integer | Early-entry player (アーリーエントリー, a student who joined before graduating): 1 = yes, 0 = no. |
| `player_foreign_national_flg` | integer | Foreign national (外国籍 icon): 1 = yes, 0 = no. |
| `player_naturalized_flg` | integer | Naturalised player (帰化 icon): 1 = yes, 0 = no. |
| `player_appeal` | character | Self-introduction text; empty string in the sample. |
| `player_championships` | character | Championships text; empty string in the sample. |
| `player_url` | character | Personal website URL; empty string in the sample. |
| `player_facebook_url` | character | Facebook URL; empty string in the sample. |
| `player_twitter_url` | character | X (Twitter) URL; empty string in the sample. |
| `player_instagram_url` | character | Instagram URL; empty string in the sample. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_player-example}

```python
wjbl_player(player_id=9799)
```

_Last validated n/a._
