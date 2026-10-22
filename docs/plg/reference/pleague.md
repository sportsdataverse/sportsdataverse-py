# PLG — P. LEAGUE+ site (pleagueofficial.com, Taiwan)

> PLG — P. LEAGUE+ site (pleagueofficial.com, Taiwan) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.plg` — 11 endpoints.

> **Note:** Unofficial use of a public source: these wrappers read pleagueofficial.com (P. LEAGUE+, Taiwan's men's league), keyless, behind Cloudflare (the runtime needs curl_cffi: pip install curl_cffi). Release flag: the data release needs owner sign-off. The site's /user-policy (members' terms, section 11) reserves all content on the site to the service and its rights holders and forbids use, copying, adaptation or distribution without authorisation (it says nothing about automated access); robots.txt holds only Cloudflare content-signal comment lines with no Allow, Disallow or Crawl-delay rule. Use for reference only; payloads are not redistributed as release assets. The runtime waits at least 1 s before every request, retries included.

> **Note:** Coverage: schedules, standings and player tables from 2020-21 (season=2021); box scores for every finished game from 2020-21; play-by-play (with shot x / y) from 2022-23 (gid 191 on). 2020-21 and 2021-22 pages embed an empty game uuid, so they have no play-by-play and no Preciser data. A game is keyed by gid: ONE global integer sequence over every stage and season. Only finished games are served (status 已完賽): an unplayed game, or a box or play-by-play that does not match its game page (the routes echo no id, so the runtime checks scores, quarters and points), raises AssetFetchError.

> **Note:** Seasons are END years (2025 = the slug '2024-25'). Translation: 客隊 / 主隊 = away / home, 已完賽 = finished, 熱身賽 / 例行賽 / 季後賽 / 總冠軍賽 = pre-season / regular season / playoffs / finals, 主裁判 = referee, 第一檢查員 / 第二檢查員 = first / second umpire, 洋將 = import, 本土 = local, 勝 / 敗 = win / loss. Names stay as printed. Ids: team_id (1-8) is stable across renames; box player_id is the player-team membership; the box random_id is unreliable before 2022-23 (join on player_id, and box to Preciser shots on person_id). The date of birth is dropped from the player frame; officials appear by name only.

## pleague_schedule

Schedule and results of one season and stage (pre-season, regular season, playoffs or finals): one row per game, away team first.

**Endpoint URL:** `GET https://pleagueofficial.com/schedule/{season}`

**Valid URL:** [https://pleagueofficial.com/schedule/2025?stage=regular-season](https://pleagueofficial.com/schedule/2025?stage=regular-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2025 = the site's slug '2024-25'). ``pleague_runtime`` sends the slug ``f'{season - 1}-{season % 100:02d}'``. The first season is 2020-21 (``season=2021``); earlier slugs redirect. |
| `stage` | `stage` |  |  | `Y` | Stage: pre-season (熱身賽, exhibition games), regular-season (例行賽), playoffs (季後賽) or finals (總冠軍賽). A stage a season does not have (2025-26 pre-season) answers NoDataError. |

### Returns {#pleague_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2025 = the site's slug '2024-25'): the first year of the slug plus one. |
| `season_slug` | character | The site's season slug as printed: the START year, then the END year's last two digits ('2024-25'). |
| `stage` | character | Stage slug: pre-season (熱身賽, exhibition games before the season), regular-season (例行賽), playoffs (季後賽) or finals (總冠軍賽, the championship series). |
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. Cross-checked with the page's trackGame() call. |
| `date` | character | Game date, Asia/Taipei (UTC+8) local time. |
| `weekday_zh` | character | Weekday as one Chinese character: 日 Sun, 一 Mon, 二 Tue, 三 Wed, 四 Thu, 五 Fri, 六 Sat. |
| `time` | character | Scheduled tip-off 'HH:MM', Asia/Taipei (UTC+8) local time. |
| `starred` | logical | True when the date cell carries the site's star icon (a highlighted game). |
| `game_label` | character | Game label as printed: 'G01' = game 1 of the stage, 'PLAYOFFS Game1' / 'PLAYOFFS A G1' = playoff game (series A / B), 'FINALS G1' / 'Final Game1' = finals game, '第一站G1' = game 1 of the first 'stop' (站, a round-robin hub weekend; 2020-21). |
| `venue_zh` | character | Venue (arena) name in Traditional Chinese. |
| `attendance` | integer | Spectators: the first number of the page's 'N / M' seat cell; 0 on future games. |
| `capacity` | integer | Seats: the second number of the 'N / M' seat cell (the venue capacity as the site records it); 0 on future games. |
| `phase` | character | Row state from the page's CSS class: before_today = the date is past (the score is the result), is_future = not yet played (both scores 0), is_today. A past game printing 0-0 may be a series game that was never played: check the game page's status. |
| `away_team_id` | integer | P. LEAGUE+ club id on the site (stable across seasons AND renames): 1 = Taipei Fubon Braves, 2 = Taoyuan Pauian Pilots, 3 = Hsinchu Jko / Toplus Lioneers, 4 = Formosa Taishin Dreamers, 5 = Kaohsiung 17LIVE Steelers, 6 = New Taipei Kings, 7 = TSG Ghosthawks, 8 = Yankey Ark. NOT a Preciser team uuid (that joins through the standings team_uuid). |
| `away_name_zh` | character | Away team's name on that date in Traditional Chinese (short or long form; 勇士 = Braves, 領航猿 = Pilots, 攻城獅 = Lioneers, 夢想家 = Dreamers, 鋼鐵人 = Steelers, 國王 = Kings, 獵鷹 = Ghosthawks, 洋基工程 = Yankey Ark). Printed names change by season (short and long forms, renames): key on the id, not the name. |
| `away_name_en` | character | Away team's English name on that date; renames change it. Null on 2020-21 game pages (no English name is printed). |
| `away_logo` | character | Away team logo url as printed by the page: protocol-relative ('//d36fypkbmmogz6.cloudfront.net/upload/p_team/logo_{team}_{epoch}.png'); some game pages print a malformed 'https:////d36...'. The trailing number is the upload time. |
| `away_score` | integer | Away team's final points (0 on a future game). |
| `home_team_id` | integer | P. LEAGUE+ club id on the site (stable across seasons AND renames): 1 = Taipei Fubon Braves, 2 = Taoyuan Pauian Pilots, 3 = Hsinchu Jko / Toplus Lioneers, 4 = Formosa Taishin Dreamers, 5 = Kaohsiung 17LIVE Steelers, 6 = New Taipei Kings, 7 = TSG Ghosthawks, 8 = Yankey Ark. NOT a Preciser team uuid (that joins through the standings team_uuid). |
| `home_name_zh` | character | Home team's name on that date in Traditional Chinese (short or long form). Printed names change by season (short and long forms, renames): key on the id, not the name. |
| `home_name_en` | character | Home team's English name on that date; renames change it. Null on 2020-21 game pages (no English name is printed). |
| `home_logo` | character | Home team logo url as printed by the page: protocol-relative ('//d36fypkbmmogz6.cloudfront.net/upload/p_team/logo_{team}_{epoch}.png'); some game pages print a malformed 'https:////d36...'. The trailing number is the upload time. |
| `home_score` | integer | Home team's final points (0 on a future game). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_schedule-example}

```python
pleague_schedule(season=2025)
```

_Last validated n/a._

## pleague_game

Header of one finished game: date, venue, both clubs with their score and the Preciser game uuid.

**Endpoint URL:** `GET https://pleagueofficial.com/game/{gid}`

**Valid URL:** [https://pleagueofficial.com/game/197](https://pleagueofficial.com/game/197)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gid` | `gid` |  | `Y` |  | Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games are served. |

### Returns {#pleague_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. |
| `uuid` | character | Preciser game uuid (key space: the analytics vendor's games, a version-1 uuid), embedded in the game page as getPlayByPlay('uuid'); keys the play-by-play and every pleague_preciser wrapper. Null before 2022-23 (the page embeds an empty string: no play-by-play or vendor data). |
| `status_zh` | character | Status badge: 已完賽 = finished. The only value the wrappers serve (anything else raises AssetFetchError). |
| `finished` | logical | True when the status badge is 已完賽 (finished); always true for a served game. |
| `date` | character | Game date from the header, Asia/Taipei (UTC+8) local time. |
| `time` | character | Tip-off 'HH:MM' from the header, Asia/Taipei (UTC+8) local time. |
| `venue_zh` | character | Venue (arena) name in Traditional Chinese. |
| `game_label_zh` | character | Stage and game number as printed in the header: '例行賽G1' = regular season game 1 ('例行賽G01' on 2020-21 pages); 季後賽 = playoffs, 總冠軍賽 = finals. |
| `away_name_zh` | character | Away team's name on that date in Traditional Chinese (short or long form; 勇士 = Braves, 領航猿 = Pilots, 攻城獅 = Lioneers, 夢想家 = Dreamers, 鋼鐵人 = Steelers, 國王 = Kings, 獵鷹 = Ghosthawks, 洋基工程 = Yankey Ark). Printed names change by season (short and long forms, renames): key on the id, not the name. |
| `away_name_en` | character | Away team's English name on that date; renames change it. Null on 2020-21 game pages (no English name is printed). |
| `away_logo` | character | Away team logo url as printed by the page: protocol-relative ('//d36fypkbmmogz6.cloudfront.net/upload/p_team/logo_{team}_{epoch}.png'); some game pages print a malformed 'https:////d36...'. The trailing number is the upload time. |
| `away_score` | integer | Away team's final points (0 on a future game). |
| `home_name_zh` | character | Home team's name on that date in Traditional Chinese (short or long form). Printed names change by season (short and long forms, renames): key on the id, not the name. |
| `home_name_en` | character | Home team's English name on that date; renames change it. Null on 2020-21 game pages (no English name is printed). |
| `home_logo` | character | Home team logo url as printed by the page: protocol-relative ('//d36fypkbmmogz6.cloudfront.net/upload/p_team/logo_{team}_{epoch}.png'); some game pages print a malformed 'https:////d36...'. The trailing number is the upload time. |
| `home_score` | integer | Home team's final points (0 on a future game). |
| `overtimes` | integer | Number of overtime periods played (overtime cells with points on the page). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_game-example}

```python
pleague_game(gid=197)
```

_Last validated n/a._

## pleague_periods

Points each team scored in each quarter and overtime of one finished game.

**Endpoint URL:** `GET https://pleagueofficial.com/game/{gid}`

**Valid URL:** [https://pleagueofficial.com/game/197](https://pleagueofficial.com/game/197)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gid` | `gid` |  | `Y` |  | Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games are served. |

### Returns {#pleague_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. |
| `period` | character | Period label: Q1-Q4, then OT1, OT2 for overtime. |
| `period_no` | integer | 1-based position of the period: 1-4 for Q1-Q4, then 5 for OT1, 6 for OT2. |
| `home_points` | integer | Home points scored IN the period (not cumulative). |
| `away_points` | integer | Away points scored IN the period (not cumulative). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_periods-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_periods-example}

```python
pleague_periods(gid=197)
```

_Last validated n/a._

## pleague_officials

The referee and the two umpires printed under the header of one finished game (names only).

**Endpoint URL:** `GET https://pleagueofficial.com/game/{gid}`

**Valid URL:** [https://pleagueofficial.com/game/197](https://pleagueofficial.com/game/197)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gid` | `gid` |  | `Y` |  | Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games are served. |

### Returns {#pleague_officials-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. |
| `official_no` | integer | 1-based position of the official in the printed line (referee first). |
| `role` | character | Role translated: referee (主裁判, the crew chief), first_umpire (第一檢查員, literally 'first inspector'), second_umpire (第二檢查員). Null for a role this table does not know. |
| `role_zh` | character | Official's role as printed: 主裁判 = referee, 第一檢查員 = first umpire, 第二檢查員 = second umpire. |
| `name_zh` | character | Official's name in Traditional Chinese. Names only: the page carries no ids or contact data. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_officials-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_officials-example}

```python
pleague_officials(gid=197)
```

_Last validated n/a._

## pleague_box_players

Player box score of one finished game: minutes, points, shooting splits, rebounds, assists, plus/minus, and the ratings from 2022-23; did-not-play rows included.

**Endpoint URL:** `GET https://pleagueofficial.com/api/boxscore.preciser.php`

**Valid URL:** [https://pleagueofficial.com/api/boxscore.preciser.php?gid=197](https://pleagueofficial.com/api/boxscore.preciser.php?gid=197)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gid` | `gid` |  | `Y` |  | Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games are served. |

### Returns {#pleague_box_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. |
| `side` | character | Which team the row belongs to: home or away. The box lists home first, the play-by-play and the schedule list away first. |
| `player_id` | integer | P. LEAGUE+ player-team membership id on the site (key space: the member id the player page passes to trackPlayer(), e.g. 40 = Lai Ting-En), cast to an integer. NOT the 8-character page id. Join box rows on it, never on the printed name. |
| `person_id` | character | Preciser person uuid (key space: the analytics vendor's persons): the join between a box row and a pleague_preciser_shots row. Present on played rows from 2022-23; null on 2020-21 and 2021-22 rows and did-not-play rows (the refs notes report some 2025-26 finals boxes without it; gid 716 has it). |
| `random_id` | character | The 8-character page id of /player/{id} (same space as player_page_id). Reliable from 2022-23 only: on 2020-21 and 2021-22 rows it is wrong for most players (gid 13 gives Lai Ting-En the page of Kao Kuo-Hao), so never join across eras on it. |
| `name_zh` | character | Player name as printed (Traditional Chinese, or a transliteration for foreign players). |
| `jersey` | character | Shirt number as printed, a string ('0', '00' and '10' differ). |
| `starter` | logical | True for a starter (the site's '〇' marker). Null on did-not-play rows. |
| `played` | logical | True when the player has court time; false on a 'DNP' (did not play) row, whose statistics are null. |
| `minutes` | character | Minutes played 'mm:ss'. Null on did-not-play rows. |
| `seconds` | integer | Playing time in whole seconds: the site's seconds column in 2020-21 and 2021-22, derived from minutes from 2022-23. Null on did-not-play rows. |
| `points` | integer | Points scored. Null on did-not-play rows. |
| `reb_off` | integer | Offensive rebounds. Null on did-not-play rows. |
| `reb_def` | integer | Defensive rebounds. Null on did-not-play rows. |
| `reb` | integer | Total rebounds (offensive plus defensive). Null on did-not-play rows. |
| `ast` | integer | Assists. Null on did-not-play rows. |
| `stl` | integer | Steals. Null on did-not-play rows. |
| `blk` | integer | Blocks made. Null on did-not-play rows. |
| `tov` | integer | Turnovers committed. Null on did-not-play rows. |
| `pf` | integer | Personal fouls committed. Null on did-not-play rows. |
| `plus_minus` | integer | Plus/minus: the team's points minus the opponent's while the player was on court (signed). Null on did-not-play rows. |
| `eff` | integer | Efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers (verified on every 2020-21 and 2021-22 row). Null on did-not-play rows. |
| `fg2_made` | integer | 2-point field goals made, from the site's 'made-attempted' string. Null on did-not-play rows. |
| `fg2_att` | integer | 2-point field goals attempted, from the site's 'made-attempted' string. Null on did-not-play rows. |
| `fg2_pct` | numeric | 2-point field-goal percentage, 0-100 (the site sends a number or a string). Null on did-not-play rows. |
| `fg3_made` | integer | 3-point field goals made, from the site's 'made-attempted' string. Null on did-not-play rows. |
| `fg3_att` | integer | 3-point field goals attempted, from the site's 'made-attempted' string. Null on did-not-play rows. |
| `fg3_pct` | numeric | 3-point field-goal percentage, 0-100. Null on did-not-play rows. |
| `ft_made` | integer | Free throws made, from the site's 'made-attempted' string. Null on did-not-play rows. |
| `ft_att` | integer | Free throws attempted, from the site's 'made-attempted' string. Null on did-not-play rows. |
| `ft_pct` | numeric | Free-throw percentage, 0-100. Null on did-not-play rows. |
| `efg_pct` | numeric | Effective field-goal percentage 0-100, (FGM + 0.5 x 3PM) / FGA x 100 (verified). 2020-21 and 2021-22 rows only; null from 2022-23. |
| `ts_pct` | numeric | True-shooting percentage 0-100, PTS / (2 x (FGA + 0.44 x FTA)) x 100 (verified). 2020-21 and 2021-22 rows only; null from 2022-23. |
| `usage_pct` | numeric | Usage percentage as the site prints it, 0-100; the standard usage formula reproduces it on only 685 of 3,932 rows, so the formula is unverified. 2020-21 and 2021-22 rows only. |
| `position` | character | Position code on some 2020-21 and 2021-22 rows ('G' guard, 'F' forward, 'C' centre); null on the rest and from 2022-23. |
| `ortg` | numeric | Offensive rating: the team's points scored per 100 offensive possessions while the player was on court (2022-23 on; null before). Null on did-not-play rows. |
| `drtg` | numeric | Defensive rating: the opponent's points per 100 of its possessions while the player was on court, lower is better (2022-23 on). Null on did-not-play rows. |
| `netrtg` | numeric | Net rating, ortg minus drtg (exact on every checked row; 2022-23 on). Null on did-not-play rows. |
| `pace` | numeric | Pace: 48 x the team's offensive possessions / minutes on court, the team's possessions per 48 minutes with this player on court (verified on 1,124 of 1,149 rows; 2022-23 on). Null on did-not-play rows. |
| `ast_ratio` | numeric | Assist ratio 0-100: assists per 100 of the player's own plays. The common formula AST / (FGA + 0.5 x FTA + AST + TOV) reproduces it on 919 of 1,174 rows, so the exact formula is unverified (2022-23 on). |
| `to_ratio` | numeric | Turnover ratio 0-100: turnovers per 100 of the player's own plays; same unverified formula family as ast_ratio (917 of 1,174 rows; 2022-23 on). |
| `off_possessions` | integer | The team's offensive possessions while the player was on court (the base of ortg; 2022-23 on). Null on did-not-play rows. |
| `opp_off_possessions` | integer | The opponent's offensive possessions while the player was on court (the base of drtg; 2022-23 on). Null on did-not-play rows. |
| `opp_pts_second_chance` | integer | Opponent second-chance points while the player was on court (2022-23 on). Null on did-not-play rows. |
| `opp_pts_fast_break` | integer | Opponent fast-break points while the player was on court (2022-23 on). Null on did-not-play rows. |
| `opp_pts_off_turnover` | integer | Opponent points off turnovers while the player was on court (2022-23 on). Null on did-not-play rows. |
| `opp_pts_paint` | integer | Opponent points in the paint while the player was on court (2022-23 on). Null on did-not-play rows. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_box_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_box_players-example}

```python
pleague_box_players(gid=197)
```

_Last validated n/a._

## pleague_box_teams

Team totals of one finished game: the box score's total row, home then away.

**Endpoint URL:** `GET https://pleagueofficial.com/api/boxscore.preciser.php`

**Valid URL:** [https://pleagueofficial.com/api/boxscore.preciser.php?gid=197](https://pleagueofficial.com/api/boxscore.preciser.php?gid=197)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gid` | `gid` |  | `Y` |  | Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games are served. |

### Returns {#pleague_box_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. |
| `side` | character | Which team the row belongs to: home or away. The box lists home first, the play-by-play and the schedule list away first. |
| `minutes` | character | Team minutes 'mm:ss' ('240:00' = 5 players x 48 min; more with overtime). |
| `seconds` | integer | Team seconds on court (14400 = 240 minutes): the site's value in 2020-21 and 2021-22, derived from minutes from 2022-23. |
| `points` | integer | Team points; equals the final score (the runtime checks it). |
| `fg2_made` | integer | Team 2-point field goals made. |
| `fg2_att` | integer | Team 2-point field goals attempted. |
| `fg2_pct` | numeric | Team 2-point percentage, 0-100. |
| `fg3_made` | integer | Team 3-point field goals made. |
| `fg3_att` | integer | Team 3-point field goals attempted. |
| `fg3_pct` | numeric | Team 3-point percentage, 0-100. |
| `ft_made` | integer | Team free throws made. |
| `ft_att` | integer | Team free throws attempted. |
| `ft_pct` | numeric | Team free-throw percentage, 0-100. |
| `reb` | integer | Team total rebounds, including team rebounds that no player row carries (so it can exceed the sum of the player rows). |
| `reb_off` | integer | Team offensive rebounds. |
| `reb_def` | integer | Team defensive rebounds. |
| `ast` | integer | Team assists in the game (the total row). |
| `stl` | integer | Team steals in the game. |
| `blk` | integer | Team blocks made in the game. |
| `tov` | integer | Team turnovers. |
| `pf` | integer | Team personal fouls. |
| `plus_minus` | integer | Sum of the players' plus/minus (2020-21 and 2021-22 only; null from 2022-23). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_box_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_box_teams-example}

```python
pleague_box_teams(gid=197)
```

_Last validated n/a._

## pleague_pbp

Play-by-play of one finished game (2022-23 on) in game order, with shot x / y and the clip references.

**Endpoint URL:** `GET https://pleagueofficial.com/api/playbyplay.php`

**Valid URL:** [https://pleagueofficial.com/api/playbyplay.php?gid=197](https://pleagueofficial.com/api/playbyplay.php?gid=197)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gid` | `gid` |  | `Y` |  | Required. P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (197 = the 2022-23 opener, 662 = the 2025-26 opener), e.g. 197; ``gid`` in pleague_schedule. Only finished games are served. |

### Returns {#pleague_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gid` | integer | P. LEAGUE+ game id on the site: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 13 = the 2020-21 opener, 197 = the 2022-23 opener, 662 = the 2025-26 opener), with gaps; a row id, not a count. The {gid} of /game/{gid}. |
| `seq` | integer | Position of the play in game order, 1-based (the fragment lists plays oldest first within the fragment). |
| `period` | character | Period label: Q1-Q4, then OT1, OT2 for overtime. |
| `period_no` | integer | 1-based position of the period: 1-4 for Q1-Q4, then 5 for OT1, 6 for OT2. |
| `clock` | character | Game clock REMAINING in the period 'mm:ss' (counts down; Q1 opens at 12:00, OT at 05:00). |
| `seconds_left` | integer | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `side` | character | Side of the acting team: away or home (the fragment places the play in the away or home column). |
| `jersey` | character | Shirt number of the acting player as printed; null on team rows and where the name is printed without a number. |
| `player_zh` | character | Acting player's name as printed; null on team rows (a team rebound, timeout or shot-clock violation). The page prints no player ids: join to the box on side, jersey and name_zh. |
| `action` | character | Action code from the vocabulary observed on 258,183 plays of 404 finished 2022-23..2025-26 games (82 phrases plus the blank row; an unseen phrase is refused as layout drift). The code = meaning [Traditional Chinese phrase, events]: ast = assist [助攻, 17,020]; blk = block [阻攻, 3,181]; fg2_alley_oop_made = 2-point alley-oop, made [alleyOop得分, 205]; fg2_alley_oop_missed = 2-point alley-oop, missed [alleyOop不進, 81]; fg2_driving_layup_made = 2-point driving layup, made [突破上籃得分, 4,281]; fg2_driving_layup_missed = 2-point driving layup, missed [突破上籃不進, 3,961]; fg2_dunk_made = 2-point dunk, made [灌籃得分, 1,316]; fg2_dunk_missed = 2-point dunk, missed [灌籃不進, 162]; fg2_fadeaway_jumper_made = 2-point fadeaway jumper, made [後仰跳投得分, 583]; fg2_fadeaway_jumper_missed = 2-point fadeaway jumper, missed [後仰跳投不進, 1,121]; fg2_floater_made = 2-point floater, made [拋投得分, 1,920]; fg2_floater_missed = 2-point floater, missed [拋投不進, 3,052]; fg2_hook_shot_made = 2-point hook shot, made [勾射得分, 610]; fg2_hook_shot_missed = 2-point hook shot, missed [勾射不進, 773]; fg2_jump_shot_made = 2-point jump shot, made [跳投得分, 1,657]; fg2_jump_shot_missed = 2-point jump shot, missed [跳投不進, 3,606]; fg2_layup_made = 2-point layup, made [上籃得分, 4,428]; fg2_layup_missed = 2-point layup, missed [上籃不進, 3,403]; fg2_pull_up_jumper_made = 2-point pull-up jumper, made [急停跳投得分, 1,004]; fg2_pull_up_jumper_missed = 2-point pull-up jumper, missed [急停跳投不進, 1,564]; fg2_putback_made = 2-point putback / tip-in, made [補籃得分, 2,138]; fg2_putback_missed = 2-point putback / tip-in, missed [補籃不進, 1,539]; fg2_step_back_shot_made = 2-point step-back shot, made [後撤步投籃得分, 243]; fg2_step_back_shot_missed = 2-point step-back shot, missed [後撤步投籃不進, 410]; fg2_turnaround_jumper_made = 2-point turnaround jumper, made [轉身跳投得分, 672]; fg2_turnaround_jumper_missed = 2-point turnaround jumper, missed [轉身跳投不進, 1,158]; fg3_fadeaway_jumper_made = 3-point fadeaway jumper, made [三分外線後仰跳投得分, 48]; fg3_fadeaway_jumper_missed = 3-point fadeaway jumper, missed [三分外線後仰跳投不進, 92]; fg3_made = 3-point shot, style not recorded, made [三分球得分, 7,817]; fg3_missed = 3-point shot, style not recorded, missed [三分球不進, 17,046]; fg3_pull_up_jumper_made = 3-point pull-up jumper, made [三分外線急停跳投得分, 480]; fg3_pull_up_jumper_missed = 3-point pull-up jumper, missed [三分外線急停跳投不進, 922]; fg3_step_back_shot_made = 3-point step-back shot, made [三分外線後撤步投籃得分, 245]; fg3_step_back_shot_missed = 3-point step-back shot, missed [三分外線後撤步投籃不進, 559]; fg3_turnaround_jumper_made = 3-point turnaround jumper, made [三分外線轉身跳投得分, 10]; fg3_turnaround_jumper_missed = 3-point turnaround jumper, missed [三分外線轉身跳投不進, 23]; fg_made = field goal made, shot type and distance not recorded [得分, 158]; fg_missed = field goal missed, shot type and distance not recorded [不進, 168]; foul = foul, kind not recorded (team row) [犯規, 18]; foul_bench_technical = technical foul on the bench [犯規benchTechnical, 35]; foul_coach_technical = technical foul on the coach [犯規coachTechnical, 75]; foul_disqualifying = disqualifying foul [犯規disqualifying, 2]; foul_drawn = foul drawn (the opponent's foul on this player) [造成對手犯規, 17,035]; foul_offensive = offensive foul committed [進攻犯規, 1,024]; foul_personal = personal foul committed [個人犯規, 15,745]; foul_technical = technical foul [技術犯規, 210]; foul_unsportsmanlike = unsportsmanlike foul [違反運動道德犯規, 261]; ft_made = free throw 1 of 1 awarded, made [得到一次罰球 第一罰 罰進, 1,326]; ft_made = free throw 1 of 3 awarded, made [得到三次罰球 第一罰 罰進, 128]; ft_made = free throw 1 of 2 awarded, made [得到兩次罰球 第一罰 罰進, 5,187]; ft_made = free throw 3, made [第三罰 罰進, 149]; ft_made = free throw 2, made [第二罰 罰進, 5,798]; ft_missed = free throw 1 of 1 awarded, missed [得到一次罰球 第一罰 不進, 669]; ft_missed = free throw 1 of 3 awarded, missed [得到三次罰球 第一罰 不進, 67]; ft_missed = free throw 1 of 2 awarded, missed [得到兩次罰球 第一罰 不進, 2,736]; ft_missed = free throw 3, missed [第三罰 不進, 46]; ft_missed = free throw 2, missed [第二罰 不進, 2,319]; jump_ball_lost = jump ball lost [跳球 輸, 404]; jump_ball_won = jump ball won [跳球 贏, 708]; reb_def = defensive rebound (a team rebound has no player) [搶到防守籃板, 29,537]; reb_off = offensive rebound (a team rebound has no player) [搶到進攻籃板, 12,379]; review_coach_challenge = video review: coach's challenge (2025-26 onward; the printed text ends with a bare comma) [videoReview, coachChallenge,, 82]; review_instant_replay = video review: instant-replay system, started by the officials (2025-26 onward) [videoReview, instantReplaySystem,, 67]; review_instant_replay_1 = video review: instant-replay system, variant printed with a trailing '1' (meaning unexplained; the row also prints the running score and the points tag '+videoReview') [videoReview, instantReplaySystem, 1, 6]; stl = steal [抄截, 7,361]; sub_in = substitution: enters the game [替換上場, 25,505]; sub_out = substitution: leaves the game [下場休息, 25,532]; timeout = timeout (a team row) [暫停, 2,854]; tov_3sec = turnover: 3-second violation [3Sec 失誤, 249]; tov_5sec = turnover: 5-second violation [5Sec 失誤, 40]; tov_8sec = turnover: 8-second violation [8Sec 失誤, 26]; tov_backcourt = turnover: backcourt violation [backCourt 失誤, 96]; tov_bad_pass = turnover: bad pass [傳球失誤, 6,325]; tov_ball_handling = turnover: ball-handling [控球失誤, 3,547]; tov_carrying = turnover: carrying the ball [carrying 失誤, 47]; tov_double_dribble = turnover: double dribble [doubleDribble 失誤, 53]; tov_offensive = turnover: offensive (kind not recorded) [進攻失誤, 1,007]; tov_offensive_goaltending = turnover: offensive goaltending [offensiveGoalTending 失誤, 4]; tov_other = turnover: other [其他失誤, 51]; tov_out_of_bounds = turnover: ball out of bounds [球出界, 441]; tov_shot_clock = turnover: shot-clock violation [shotClock 失誤, 489]; tov_travel = turnover: travelling [帶球走步, 718]; unlabelled = row has a player but no action text; in the corpus a shot with x/y followed by a rebound, i.e. a missed shot (inferred) [(blank), 169]. |
| `action_zh` | character | The action phrase as printed, Traditional Chinese (灌籃得分 = dunk, made; '#35 辛巴 灌籃得分' = '#35 {player} dunk, made'); blank on an unlabelled row. The 82 observed phrases = action code (events): 搶到防守籃板 = reb_def (29,537); 下場休息 = sub_out (25,532); 替換上場 = sub_in (25,505); 三分球不進 = fg3_missed (17,046); 造成對手犯規 = foul_drawn (17,035); 助攻 = ast (17,020); 個人犯規 = foul_personal (15,745); 搶到進攻籃板 = reb_off (12,379); 三分球得分 = fg3_made (7,817); 抄截 = stl (7,361); 傳球失誤 = tov_bad_pass (6,325); 第二罰 罰進 = ft_made (5,798); 得到兩次罰球 第一罰 罰進 = ft_made (5,187); 上籃得分 = fg2_layup_made (4,428); 突破上籃得分 = fg2_driving_layup_made (4,281); 突破上籃不進 = fg2_driving_layup_missed (3,961); 跳投不進 = fg2_jump_shot_missed (3,606); 控球失誤 = tov_ball_handling (3,547); 上籃不進 = fg2_layup_missed (3,403); 阻攻 = blk (3,181); 拋投不進 = fg2_floater_missed (3,052); 暫停 = timeout (2,854); 得到兩次罰球 第一罰 不進 = ft_missed (2,736); 第二罰 不進 = ft_missed (2,319); 補籃得分 = fg2_putback_made (2,138); 拋投得分 = fg2_floater_made (1,920); 跳投得分 = fg2_jump_shot_made (1,657); 急停跳投不進 = fg2_pull_up_jumper_missed (1,564); 補籃不進 = fg2_putback_missed (1,539); 得到一次罰球 第一罰 罰進 = ft_made (1,326); 灌籃得分 = fg2_dunk_made (1,316); 轉身跳投不進 = fg2_turnaround_jumper_missed (1,158); 後仰跳投不進 = fg2_fadeaway_jumper_missed (1,121); 進攻犯規 = foul_offensive (1,024); 進攻失誤 = tov_offensive (1,007); 急停跳投得分 = fg2_pull_up_jumper_made (1,004); 三分外線急停跳投不進 = fg3_pull_up_jumper_missed (922); 勾射不進 = fg2_hook_shot_missed (773); 帶球走步 = tov_travel (718); 跳球 贏 = jump_ball_won (708); 轉身跳投得分 = fg2_turnaround_jumper_made (672); 得到一次罰球 第一罰 不進 = ft_missed (669); 勾射得分 = fg2_hook_shot_made (610); 後仰跳投得分 = fg2_fadeaway_jumper_made (583); 三分外線後撤步投籃不進 = fg3_step_back_shot_missed (559); shotClock 失誤 = tov_shot_clock (489); 三分外線急停跳投得分 = fg3_pull_up_jumper_made (480); 球出界 = tov_out_of_bounds (441); 後撤步投籃不進 = fg2_step_back_shot_missed (410); 跳球 輸 = jump_ball_lost (404); 違反運動道德犯規 = foul_unsportsmanlike (261); 3Sec 失誤 = tov_3sec (249); 三分外線後撤步投籃得分 = fg3_step_back_shot_made (245); 後撤步投籃得分 = fg2_step_back_shot_made (243); 技術犯規 = foul_technical (210); alleyOop得分 = fg2_alley_oop_made (205); (blank) = unlabelled (169); 不進 = fg_missed (168); 灌籃不進 = fg2_dunk_missed (162); 得分 = fg_made (158); 第三罰 罰進 = ft_made (149); 得到三次罰球 第一罰 罰進 = ft_made (128); backCourt 失誤 = tov_backcourt (96); 三分外線後仰跳投不進 = fg3_fadeaway_jumper_missed (92); videoReview, coachChallenge, = review_coach_challenge (82); alleyOop不進 = fg2_alley_oop_missed (81); 犯規coachTechnical = foul_coach_technical (75); 得到三次罰球 第一罰 不進 = ft_missed (67); videoReview, instantReplaySystem, = review_instant_replay (67); doubleDribble 失誤 = tov_double_dribble (53); 其他失誤 = tov_other (51); 三分外線後仰跳投得分 = fg3_fadeaway_jumper_made (48); carrying 失誤 = tov_carrying (47); 第三罰 不進 = ft_missed (46); 5Sec 失誤 = tov_5sec (40); 犯規benchTechnical = foul_bench_technical (35); 8Sec 失誤 = tov_8sec (26); 三分外線轉身跳投不進 = fg3_turnaround_jumper_missed (23); 犯規 = foul (18); 三分外線轉身跳投得分 = fg3_turnaround_jumper_made (10); videoReview, instantReplaySystem, 1 = review_instant_replay_1 (6); offensiveGoalTending 失誤 = tov_offensive_goaltending (4); 犯規disqualifying = foul_disqualifying (2). |
| `action_en` | character | English meaning of the action code (see action); null for a code outside the observed table. |
| `text_zh` | character | The whole printed text of the play, e.g. '#35 辛巴 灌籃得分' = '#35 {player} dunk, made'. |
| `ft_number` | integer | Which free throw of the trip this is (1, 2 or 3), from 第一罰 / 第二罰 / 第三罰; null on every other play. |
| `ft_awarded` | integer | Free throws awarded in the trip (1, 2 or 3) from 得到一次罰球 / 得到兩次罰球 / 得到三次罰球; set only on the FIRST free throw's row, null otherwise. |
| `points` | integer | Points scored by the play from the '+2' / '+3' / '+1' tag; null when the tag is a bare '+'. |
| `score_away` | integer | Away score after the play, from the 'A - H' cell (AWAY first, unlike the box); null on plays that print no score (substitutions, rebounds, jump balls). |
| `score_home` | integer | Home score after the play, from the 'A - H' cell; null when the play prints no score. |
| `x` | numeric | Shot / play x: PERCENT (0-100) of the court LENGTH in the PHYSICAL court frame (not normalised per team): baskets near x = 5 and x = 95, teams swap ends at half-time. The Preciser shot list uses a home-attacks-95 frame: x_vendor = 100 - x in the halves where the home team attacks x < 50. Null on plays without a location. Court 28.75 m long. |
| `y` | numeric | Shot / play y: PERCENT (0-100) of the court WIDTH in the physical frame (baskets at y = 50); y_vendor = 100 - y in the halves where the home team attacks x < 50. Which sideline is y = 0 is unverified. Court 15.25 m wide. |
| `nice_play` | logical | True when the site stars the play (a nice_play row class and a starred clip). |
| `video_uuid` | character | Clip uuid (key space: the clip vendor tryspot); null on plays without a clip (about 3 in 4). The clip urls are never kept: they carry a vendor access key. |
| `synergy_play_id` | character | Synergy Sports play uuid of the clip (the site links Synergy); a string on every one of the 50,862 clips in the corpus. |
| `video_stat_category` | character | The clip's own stat label (ast, blk, ft_made, ft_miss, pfoul, reb_d, reb_o, stl, three_made, three_miss, timeout, turnover, two_made, two_miss): it names the PLAY the clip shows, not always this row's action (a rebound row carries the missed shot's clip). |
| `tryspot_player_id` | integer | Player id in the clip vendor's key space (an integer such as 59476); NOT the box player_id. |
| `video_created_at` | character | When the clip was created, UTC ISO 8601 with milliseconds. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_pbp-example}

```python
pleague_pbp(gid=197)
```

_Last validated n/a._

## pleague_standings

Regular-season standings of one season: record, scoring averages, home / away and recent form.

**Endpoint URL:** `GET https://pleagueofficial.com/standings/{season}`

**Valid URL:** [https://pleagueofficial.com/standings/2025](https://pleagueofficial.com/standings/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2025 = the site's slug '2024-25'). ``pleague_runtime`` sends the slug ``f'{season - 1}-{season % 100:02d}'``. The first season is 2020-21 (``season=2021``); earlier slugs redirect. |

### Returns {#pleague_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2025 = the site's slug '2024-25'): the first year of the slug plus one. |
| `season_slug` | character | The site's season slug as printed: the START year, then the END year's last two digits ('2024-25'). |
| `rank` | integer | Rank on the page (1 = best); ties are not shared. |
| `team_id` | integer | P. LEAGUE+ club id on the site (stable across seasons AND renames): 1 = Taipei Fubon Braves, 2 = Taoyuan Pauian Pilots, 3 = Hsinchu Jko / Toplus Lioneers, 4 = Formosa Taishin Dreamers, 5 = Kaohsiung 17LIVE Steelers, 6 = New Taipei Kings, 7 = TSG Ghosthawks, 8 = Yankey Ark. NOT a Preciser team uuid (that joins through the standings team_uuid). |
| `team_zh` | character | Team short name in Traditional Chinese. |
| `team_uuid` | character | Preciser team uuid cut from the logo url (photonew.preciser.io/plg/teams/{uuid}.png): the join between the site team_id and the vendor's home_id / away_id / team_id. Clubs: 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark). |
| `team_logo` | character | Team logo url (photonew.preciser.io/plg/teams/{team uuid}.png). |
| `games_played` | integer | Games played (已賽 GP). |
| `wins` | integer | Games won so far this season (勝 W). |
| `losses` | integer | Games lost so far this season (敗 L). |
| `win_pct` | numeric | Win percentage 0-100 as a whole percent ('68%' -> 68.0; 勝率 PCT). |
| `games_behind` | numeric | Games behind the leader (勝差). |
| `points_for_avg` | numeric | Points scored per game (平均得分). |
| `points_against_avg` | numeric | Points allowed per game (平均失分). |
| `point_diff_avg` | numeric | Average point margin (平均分差). |
| `home_record` | character | Home record 'W-L' (主場). |
| `away_record` | character | Away record 'W-L' (客場). |
| `streak` | character | Current streak: W{n} wins or L{n} losses (連勝連敗). |
| `last_n` | integer | Games in the 'last games' column (近10場比賽 = the last 10), read from the header. |
| `last_n_record` | character | Record 'W-L' over the last last_n games (近N場比賽). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_standings-example}

```python
pleague_standings(season=2025)
```

_Last validated n/a._

## pleague_stat_player

Player season averages and totals of one season and stage: shooting, rebounds, assists and fouls.

**Endpoint URL:** `GET https://pleagueofficial.com/stat-player/{season}/{stage_sn}`

**Valid URL:** [https://pleagueofficial.com/stat-player/2025/2](https://pleagueofficial.com/stat-player/2025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2025 = the site's slug '2024-25'). ``pleague_runtime`` sends the slug ``f'{season - 1}-{season % 100:02d}'``. The first season is 2020-21 (``season=2021``); earlier slugs redirect. |
| `stage_sn` | `stage_sn` |  | `Y` |  | Required. Stage select value of the player table: 2 = regular season, 3 = playoffs, 4 = finals. |

### Returns {#pleague_stat_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2025 = the site's slug '2024-25'): the first year of the slug plus one. |
| `season_slug` | character | The site's season slug as printed: the START year, then the END year's last two digits ('2024-25'). |
| `stage_sn` | integer | Stage select value: 2 = regular season, 3 = playoffs, 4 = finals (echo of the request, checked against the page's selected option). |
| `stage` | character | Stage slug: pre-season (熱身賽, exhibition games before the season), regular-season (例行賽), playoffs (季後賽) or finals (總冠軍賽, the championship series). |
| `player_page_id` | character | The 8-character page id of /player/{id} (e.g. '1JbPozLB'). The box random_id on 2022-23+ rows; NOT the box player_id. |
| `name_zh` | character | Player name as printed (Traditional Chinese, or a transliteration for foreign players). |
| `jersey` | character | Shirt number as a string (the player's number in the latest game). |
| `team_zh` | character | Team name in Traditional Chinese as the page prints it (a traded player shows the current team). |
| `games` | integer | Games played (出賽). |
| `minutes_avg` | character | Minutes per game 'mm:ss' (上場時間). |
| `minutes_total` | character | Total minutes 'mmm:ss' (上場時間). |
| `fg2_made_avg` | numeric | Per-game average of 2-point makes (兩分命中). |
| `fg2_made_total` | integer | Season total of 2-point makes (兩分命中). |
| `fg2_att_avg` | numeric | Per-game average of 2-point attempts (兩分出手). |
| `fg2_att_total` | integer | Season total of 2-point attempts (兩分出手). |
| `fg3_made_avg` | numeric | Per-game average of 3-point makes (三分命中). |
| `fg3_made_total` | integer | Season total of 3-point makes (三分命中). |
| `fg3_att_avg` | numeric | Per-game average of 3-point attempts (三分出手). |
| `fg3_att_total` | integer | Season total of 3-point attempts (三分出手). |
| `ft_made_avg` | numeric | Per-game average of free-throw makes (罰球命中). |
| `ft_made_total` | integer | Season total of free-throw makes (罰球命中). |
| `ft_att_avg` | numeric | Per-game average of free-throw attempts (罰球出手). |
| `ft_att_total` | integer | Season total of free-throw attempts (罰球出手). |
| `points_avg` | numeric | Per-game average of points (得分). |
| `points_total` | integer | Season total of points (得分). |
| `reb_off_avg` | numeric | Per-game average of offensive rebounds (攻板). |
| `reb_off_total` | integer | Season total of offensive rebounds (攻板). |
| `reb_def_avg` | numeric | Per-game average of defensive rebounds (防板). |
| `reb_def_total` | integer | Season total of defensive rebounds (防板). |
| `reb_avg` | numeric | Per-game average of total rebounds (籃板). |
| `reb_total` | integer | Season total of total rebounds (籃板). |
| `ast_avg` | numeric | Per-game average of assists (助攻). |
| `ast_total` | integer | Season total of assists (助攻). |
| `stl_avg` | numeric | Per-game average of steals (抄截). |
| `stl_total` | integer | Season total of steals (抄截). |
| `blk_avg` | numeric | Per-game average of blocks (阻攻). |
| `blk_total` | integer | Season total of blocks (阻攻). |
| `tov_avg` | numeric | Per-game average of turnovers (失誤). |
| `tov_total` | integer | Season total of turnovers (失誤). |
| `pf_avg` | numeric | Per-game average of personal fouls (犯規). |
| `pf_total` | integer | Season total of personal fouls (犯規). |
| `fg2_pct` | numeric | 2-point percentage 0-100 (兩分命中率; the cell holds the same value as average and total). |
| `fg3_pct` | numeric | 3-point percentage 0-100 (三分命中率; same value as average and total). |
| `ft_pct` | numeric | Free-throw percentage 0-100 (罰球命中率; same value as average and total). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_stat_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_stat_player-example}

```python
pleague_stat_player(season=2025, stage_sn=2)
```

_Last validated n/a._

## pleague_player

One player's profile: names, club, jersey, position, height, weight and birthplace.

**Endpoint URL:** `GET https://pleagueofficial.com/player/{player_page_id}`

**Valid URL:** [https://pleagueofficial.com/player/1JbPozLB](https://pleagueofficial.com/player/1JbPozLB)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_page_id` | `player_page_id` |  | `Y` |  | Required. The 8-character page id of /player/{id} (e.g. '1JbPozLB' = Lai Ting-En); ``player_page_id`` in pleague_stat_player, ``random_id`` in pleague_box_players from 2022-23 (unreliable before). |

### Returns {#pleague_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_page_id` | character | The 8-character page id of /player/{id} (e.g. '1JbPozLB'). The box random_id on 2022-23+ rows; NOT the box player_id. |
| `player_id` | integer | P. LEAGUE+ player-team membership id (the box player_id), read from the page's trackPlayer() call, e.g. 40 = Lai Ting-En. NOT the 8-character page id. |
| `name_zh` | character | Player name as printed (Traditional Chinese, or a transliteration for foreign players). |
| `name_en` | character | Player name in Latin letters. |
| `team_zh` | character | Current team's short name in Traditional Chinese. |
| `team_en` | character | Current team's English name. |
| `jersey` | character | Shirt number as printed, a string ('0', '00' and '10' differ). |
| `position` | character | Position letter: G guard, F forward, C centre. |
| `nickname` | character | Nickname (綽號); null when blank. |
| `tenure` | character | Years of service (年資) as the page prints it, with a prime character ('0‘'). |
| `height_cm` | numeric | Height in centimetres (身高). |
| `weight_kg` | numeric | Weight in kilograms (體重). |
| `birthplace_zh` | character | Birthplace in Traditional Chinese (出生地; a country for foreign players). The date of birth is dropped on purpose. |
| `status_zh` | character | Player status (身份): 洋將 = import (foreign player), 本土 = local (Taiwanese) player. |
| `education` | character | Education lines (學歷) as printed, joined with '; '. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_player-example}

```python
pleague_player(player_page_id='1JbPozLB')
```

_Last validated n/a._

## pleague_player_seasons

One player's season totals and per-game averages, league and East Asia Super League, with a career row.

**Endpoint URL:** `GET https://pleagueofficial.com/player/{player_page_id}`

**Valid URL:** [https://pleagueofficial.com/player/1JbPozLB](https://pleagueofficial.com/player/1JbPozLB)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_page_id` | `player_page_id` |  | `Y` |  | Required. The 8-character page id of /player/{id} (e.g. '1JbPozLB' = Lai Ting-En); ``player_page_id`` in pleague_stat_player, ``random_id`` in pleague_box_players from 2022-23 (unreliable before). |

### Returns {#pleague_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_page_id` | character | The 8-character page id of /player/{id} (e.g. '1JbPozLB'). The box random_id on 2022-23+ rows; NOT the box player_id. |
| `competition` | character | Table block: plg (the league's tables) or easl (the East Asia Super League block, present only for players with games there). |
| `table` | character | totals (the 累計 table: season totals) or averages (the 平均 table: per game). |
| `season` | integer | Season END year of the slug; null on the career row. |
| `season_slug` | character | Season slug as printed ('2025-26'), or 'career' for the final totals / averages row. |
| `games` | integer | Games played (出賽). |
| `minutes` | character | Minutes 'mm:ss' (a season total in the totals table, a per-game average in the averages table). |
| `points` | numeric | Points (total in the totals table, per game in the averages table; 得分). |
| `reb` | numeric | Rebounds (total / per game; 籃板). |
| `ast` | numeric | Assists (total / per game; 助攻). |
| `stl` | numeric | Steals (total / per game; 抄截). |
| `blk` | numeric | Blocks (total / per game; 阻攻). |
| `tov` | numeric | Turnovers (total / per game; 失誤). |
| `pf` | numeric | Personal fouls (total / per game; 犯規). |
| `fg2_made` | numeric | 2-point makes: the first number of the page's '56 - 103' cell (total / per game). |
| `fg2_att` | numeric | 2-point attempts: the second number of the cell. |
| `fg2_pct` | numeric | 2-point percentage 0-100 ('54.4%' -> 54.4). |
| `fg3_made` | numeric | 3-point makes (total / per game). |
| `fg3_att` | numeric | 3-point attempts (total / per game). |
| `fg3_pct` | numeric | 3-point percentage 0-100. |
| `ft_made` | numeric | Free-throw makes (total / per game). |
| `ft_att` | numeric | Free-throw attempts (total / per game). |
| `ft_pct` | numeric | Free-throw percentage 0-100. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape), the box body as the site sends it, or the parsed play-by-play record, plus `_request` (kind and the season, stage, game or player asked for).

### Raises {#pleague_player_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_player_seasons-example}

```python
pleague_player_seasons(player_page_id='1JbPozLB')
```

_Last validated n/a._
