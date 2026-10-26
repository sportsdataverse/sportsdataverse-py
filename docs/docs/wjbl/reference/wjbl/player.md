---
title: "W League (Japan) — W League API (api.wjbl.01core.app, Japan women) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "W League (Japan) — W League API (api.wjbl.01core.app, Japan women) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# W League (Japan) — W League API (api.wjbl.01core.app, Japan women) — Player

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
| `play_schedule_id` | Int64 | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `home_team_id` | Int64 | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_team_id` | Int64 | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `period_category` | Int64 | The period category again, as an integer (camelCase here, period_category on /pbp/team-stats). |
| `period_label` | String | Readable period: Q1-Q4 quarters, OT1-OT10 overtimes (period categories 5-14), H1 / H2 halves (15 / 16), OT all overtimes together (17), GAME the whole game (18). |
| `home_point_player` | String | Home points leader(s) in the period as '#NUMBER NAME' (Japanese); ties are joined with ' , '. |
| `home_point_value` | String | Their points with the counter 点 ('5点'); ties joined with ' , ' in the same order. |
| `home_assist_player` | String | Home assist leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `home_assist_value` | String | Their assists with the counter 本 ('2本'), ' , '-joined for ties. |
| `home_rebound_player` | String | Home rebound leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `home_rebound_value` | String | Their rebounds 'offensive-defensive-total' ('0-3-3'), ' , '-joined for ties. |
| `away_point_player` | String | Away points leader(s) in the period as '#NUMBER NAME' (Japanese); ties are joined with ' , '. |
| `away_point_value` | String | Their points with the counter 点 ('5点'); ties joined with ' , ' in the same order. |
| `away_assist_player` | String | Away assist leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `away_assist_value` | String | Their assists with the counter 本 ('2本'), ' , '-joined for ties. |
| `away_rebound_player` | String | Away rebound leader(s), '#NUMBER NAME', ties joined with ' , '. |
| `away_rebound_value` | String | Their rebounds 'offensive-defensive-total' ('0-3-3'), ' , '-joined for ties. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_player_period_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_player_period_leaders-example}

```python
wjbl_player_period_leaders(play_schedule_id=7987)
```

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
| `player_id` | Int64 | Player id player_id (key space: W League persons, STABLE across seasons and teams). The player_id of /player and the pi= of the site's /player/detail?pi=; joins the play-by-play player_id1 / player_id2. |
| `player_name` | String | Name in Japanese, 'SURNAME GIVEN'. |
| `player_name_en` | String | Name in romaji, uppercase, 'SURNAME GIVEN' ('OKAMOTO MIYUU'). |
| `player_furigana` | String | Name reading in hiragana ('おかもと みゆう'). |
| `team_id` | Int64 | Current (or last) team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `season_team_name` | String | Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `season_team_short_name` | String | Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `season_team_player_number` | String | Shirt number this season, a string. |
| `team_logo_image` | String | Team logo path, relative to https://www.wjbl.org ('/static/images/base_team/...png' or '/assets/img/team/...png'). |
| `team_url` | String | Team's own website URL. |
| `player_birthday` | String | Date of birth 'YYYY/MM/DD' (public player bio). |
| `player_nickname` | String | Nickname, Japanese. |
| `player_position` | String | Playing position code ('PG', 'SG', 'SF', 'PF', 'C'). |
| `player_home_town` | String | Home prefecture, Japanese ('北海道' = Hokkaido). |
| `player_height` | String | Height with unit, a string ('179cm'). |
| `player_weight` | String | Weight with unit, a string ('77kg'). |
| `player_height_cm` | Int64 | Height in whole centimetres, parsed from player_height ('179cm'). |
| `player_weight_kg` | Int64 | Weight in whole kilograms, parsed from player_weight ('77kg'). |
| `player_affiliation_history` | String | Career path, Japanese, '→'-separated (mini-basketball club → junior high → high school → university → team). |
| `player_independent_flg` | Int64 | Retired flag (the site greys the team and hides the number): 1 = retired / no longer registered, 0 = active. |
| `player_national_team` | String | National-team history, Japanese free text ('ユニバ' = Universiade). |
| `player_awards` | String | Awards and honours, Japanese free text. |
| `player_coach_history` | String | Coaching history (for players who became coaches); empty string in the sample. |
| `player_coach_record` | String | Coaching record; empty string in the sample. |
| `player_picture` | String | Photo path relative to https://www.wjbl.org. |
| `player_wjbl_flg` | Int64 | 1 = a W League player (0 presumably for others; unverified). |
| `player_rookie` | Int64 | Rookie (ルーキー icon): 1 = yes, 0 = no. |
| `player_early_entry` | Int64 | Early-entry player (アーリーエントリー, a student who joined before graduating): 1 = yes, 0 = no. |
| `player_foreign_national_flg` | Int64 | Foreign national (外国籍 icon): 1 = yes, 0 = no. |
| `player_naturalized_flg` | Int64 | Naturalised player (帰化 icon): 1 = yes, 0 = no. |
| `player_appeal` | String | Self-introduction text; empty string in the sample. |
| `player_championships` | String | Championships text; empty string in the sample. |
| `player_url` | String | Personal website URL; empty string in the sample. |
| `player_facebook_url` | String | Facebook URL; empty string in the sample. |
| `player_twitter_url` | String | X (Twitter) URL; empty string in the sample. |
| `player_instagram_url` | String | Instagram URL; empty string in the sample. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Raises {#wjbl_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, an id it does not know, a game without play-by-play before 2018-19, a season without months or games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a body for another game, league edition, month, team, season phase or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season that is not the END year of a W League edition, an id that is not an integer, a game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422. |

### Example {#wjbl_player-example}

```python
wjbl_player(player_id=9799)
```
