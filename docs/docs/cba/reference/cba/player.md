---
title: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Player: box–defensive"
sidebar_label: "Player: box–defensive"
sidebar_position: 2
description: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Player: box–defensive — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Player: box–defensive

## cba_player_box

Player box score of a finished game, both sides, plus a team rebounds/turnovers row per side (null player_id).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-match-datas/data-count`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-match-datas/data-count' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` | body |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | integer | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | character | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `assists` | integer | Assists in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `blocked` | integer | Blocks in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `cn_alias` | character | Player name (Chinese). The team row reads 团队篮板/失误 = 'team rebounds / turnovers' (rebounds and turnovers not credited to a player; that row does not exist in the 2009-10 sample). |
| `field_goals` | integer | Field goals made in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `field_goals_attempted` | integer | Field goals attempted in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `field_goals_percentage` | numeric | Field-goal percentage as a FRACTION 0-1 (0.308), null when no attempt (0/0) as well as on the team row. |
| `fouls` | integer | Personal fouls in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `free_throws` | integer | Free throws made in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `free_throws_attempted` | integer | Free throws attempted in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `free_throws_percentage` | numeric | Free-throw percentage as a FRACTION 0-1, null on the team row. |
| `game_start` | integer | Starter flag: 1 = started the game. Null on the team row. |
| `number` | character | Jersey number as a string. Null on the team row. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. Null on the team row. |
| `plus_minus` | integer | Plus-minus while on court. Null on the team row and on every row of the 2009-10 sample (not tracked then). |
| `points` | integer | Points in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `rebounds` | integer | Total rebounds in this game (integer). The team row carries the team's own total here. |
| `rebounds_defensive` | integer | Defensive rebounds in this game (integer). The team row carries the team's own total here. |
| `rebounds_offensive` | integer | Offensive rebounds in this game (integer). The team row carries the team's own total here. |
| `steals` | integer | Steals in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `three_point_attempted` | integer | Three-pointers attempted in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `three_point_goals` | integer | Three-pointers made in this game (integer). Null on the team row (the 团队篮板/失误 rebounds/turnovers row). |
| `three_point_percentage` | numeric | Three-point percentage as a FRACTION 0-1, null on the team row. |
| `turnovers` | integer | Turnovers in this game (integer). The team row carries the team's own total here. |
| `up_time` | integer | Time on court in SECONDS (1692 = 28:12). Null on the team row. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_box-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_box-example}

```python
cba_player_box(schedule_id=100105799)
```

_Last validated n/a._

## cba_player_advanced

Advanced player box of a finished game (usage, true shooting, rebound/steal/block rates, zone splits), one row per player.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-match-datas/advance-count`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-match-datas/advance-count' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` | body |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_player_advanced-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | integer | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | character | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `assists_percentage` | numeric | 助攻率 assist rate: the share of teammates' made field goals the player assisted while on the court (site definition: 球员在场时队友受其助攻命中数占所有命中数的比例). |
| `assists_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 助攻率 assist rate: the share of teammates' made field goals the player assisted while on the court (site definition: 球员在场时队友受其助攻命中数占所有命中数的比例). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `assists_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 助攻率 assist rate: the share of teammates' made field goals the player assisted while on the court (site definition: 球员在场时队友受其助攻命中数占所有命中数的比例). |
| `blocked_percentage` | numeric | 盖帽率 block rate: blocks as a share of the opponent's two-point attempts while the player is on the court (site definition). |
| `blocked_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 盖帽率 block rate: blocks as a share of the opponent's two-point attempts while the player is on the court (site definition). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `blocked_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 盖帽率 block rate: blocks as a share of the opponent's two-point attempts while the player is on the court (site definition). |
| `cn_alias` | character | Player name (Chinese). |
| `field_goals_at_rim_percentage` | character | At-the-rim (篮下, inside the restricted area 合理冲撞区) field goals as the string 'made/attempted'. Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `field_goals_at_rim_percentage_rank` | character | Unobserved: null in every sample. When filled: colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: At-the-rim (篮下, inside the restricted area 合理冲撞区) field goals as the string 'made/attempted'. |
| `field_goals_at_rim_percentage_sort` | character | Unobserved: null in every sample. When filled: unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: At-the-rim (篮下, inside the restricted area 合理冲撞区) field goals as the string 'made/attempted'. |
| `field_goals_mid_percentage` | character | Mid-range (中距离, two-point shots outside the restricted area) field goals as the string 'made/attempted'. Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `field_goals_mid_percentage_rank` | character | Unobserved: null in every sample. When filled: colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: Mid-range (中距离, two-point shots outside the restricted area) field goals as the string 'made/attempted'. |
| `field_goals_mid_percentage_sort` | character | Unobserved: null in every sample. When filled: unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: Mid-range (中距离, two-point shots outside the restricted area) field goals as the string 'made/attempted'. |
| `game_start` | integer | Starter flag: 1 = started the game. |
| `is_color` | integer | Highlight flag, 1 on every sampled row (unverified meaning; the site uses it to colour the row). |
| `number` | character | Jersey number as a string. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `player_times` | integer | 0 on every sampled row; meaning unverified (a games-played slot the game route does not fill). |
| `position` | character | Position label (Chinese). Eras differ: coarse 后卫 (guard) / 前锋 (forward) / 中锋 (center) in older seasons, five-way 控球后卫 (point guard) / 得分后卫 (shooting guard) / 小前锋 (small forward) / 大前锋 (power forward) / 中锋 (center) in newer ones; dual positions are joined with '/' (e.g. 小前锋/大前锋). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `rebounds_defensive_percentage` | numeric | 防守篮板率 defensive-rebound rate: the player's defensive rebounds as a share of the defensive rebounds available while he is on the court (site definition). |
| `rebounds_defensive_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 防守篮板率 defensive-rebound rate: the player's defensive rebounds as a share of the defensive rebounds available while he is on the court (site definition). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `rebounds_defensive_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 防守篮板率 defensive-rebound rate: the player's defensive rebounds as a share of the defensive rebounds available while he is on the court (site definition). |
| `rebounds_offensive_percentage` | numeric | 进攻篮板率 offensive-rebound rate: the player's offensive rebounds as a share of the offensive rebounds available while he is on the court (site definition). |
| `rebounds_offensive_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 进攻篮板率 offensive-rebound rate: the player's offensive rebounds as a share of the offensive rebounds available while he is on the court (site definition). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `rebounds_offensive_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 进攻篮板率 offensive-rebound rate: the player's offensive rebounds as a share of the offensive rebounds available while he is on the court (site definition). |
| `steals_percentage` | numeric | 抢断率 steal rate: steals as a share of the opponent's possessions while the player is on the court (site definition). |
| `steals_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 抢断率 steal rate: steals as a share of the opponent's possessions while the player is on the court (site definition). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `steals_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 抢断率 steal rate: steals as a share of the opponent's possessions while the player is on the court (site definition). |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `true_shooting_percentage` | numeric | 真实命中率 true-shooting percentage: scoring efficiency counting field goals and free throws, all attempts converted to two-point attempts (site definition: 球员运动战出手和罚球出手都折算为两分出手时的命中率). |
| `true_shooting_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 真实命中率 true-shooting percentage: scoring efficiency counting field goals and free throws, all attempts converted to two-point attempts (site definition: 球员运动战出手和罚球出手都折算为两分出手时的命中率). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `true_shooting_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 真实命中率 true-shooting percentage: scoring efficiency counting field goals and free throws, all attempts converted to two-point attempts (site definition: 球员运动战出手和罚球出手都折算为两分出手时的命中率). |
| `turn_share` | numeric | 回合占有率 'possession share': the share of the team's possessions the player ends while on the court (site definition: 球员在场时终结回合占球队总回合的比例). |
| `turn_share_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 回合占有率 'possession share': the share of the team's possessions the player ends while on the court (site definition: 球员在场时终结回合占球队总回合的比例). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `turn_share_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 回合占有率 'possession share': the share of the team's possessions the player ends while on the court (site definition: 球员在场时终结回合占球队总回合的比例). |
| `turnovers_percentage` | numeric | 失误率 turnover rate: the share of the player's possession-ending plays that are turnovers (site definition: 球员以失误终结的回合占他所有终结回合的比例). |
| `turnovers_percentage_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 失误率 turnover rate: the share of the player's possession-ending plays that are turnovers (site definition: 球员以失误终结的回合占他所有终结回合的比例). Null on every row of some eras: the 2017-18 and 2009-10 samples have no Rank values, and the at-rim / mid-range strings are null in 2009-10 and '0/0' strings in 2025-26. |
| `turnovers_percentage_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 失误率 turnover rate: the share of the player's possession-ending plays that are turnovers (site definition: 球员以失误终结的回合占他所有终结回合的比例). |
| `type` | integer | Side flag: 0 on every home row, 1 on every visiting row (differs from best_quota, where the visiting side is 2). |
| `up_time` | integer | Time on court in SECONDS (1692 = 28:12). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_advanced-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_advanced-example}

```python
cba_player_advanced(schedule_id=100105799)
```

_Last validated n/a._

## cba_player_extras

Fast-break points, dunk points and paint points per player of a finished game, one row per player.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-match-datas/schedule-other-data`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-match-datas/schedule-other-data' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` | body |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_player_extras-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | integer | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `side` | character | Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block). |
| `cn_alias` | character | Player name (Chinese). |
| `dunks_points` | integer | 扣篮得分 points scored on dunks. |
| `fast_break_points` | integer | 快攻得分 fast-break points scored by the player. |
| `field_goals_at_rim_made` | integer | 油漆区得分 points scored in the paint (the site's own column title). The field name says 'made' but the value is a POINTS count: it exceeds field goals made for 15 of 65 sampled players (e.g. 10 against 9 field goals made, 28 points). |
| `game_start` | integer | Starter flag: 1 = started the game. |
| `number` | character | Jersey number as a string. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `up_time` | integer | Time on court in SECONDS. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_extras-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_extras-example}

```python
cba_player_extras(schedule_id=100105799)
```

_Last validated n/a._

## cba_player_base

Season player box (one page of the season player table).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-base-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-base-list?pageNumber=1&pageSize=100' -H 'Content-Type: application/json' -d '{"season": 2025, "matchTypeId": 1, "countRanger": 1, "playerRanger": 1, "teamId": 29128}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The API takes the START year (2025), so cba_runtime sends season - 1. |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |
| `countRanger` | `count_ranger` | body |  |  | `Y` | How counting statistics scale: 1 per game (default), 2 season total, 3 per 36 minutes, 4 per 100 possessions. |
| `playerRanger` | `player_ranger` | body |  |  | `Y` | Which players: 1 all (default), 2 domestic (Chinese) only, 4 foreign only. |
| `teamId` | `team_id` | body |  |  | `Y` | Optional CBA team id (clubs, stable across seasons and renames) to list one team's players. |
| `pageNumber` | `page_number` | query |  |  | `Y` | Page number of the paged list, from 1. |
| `pageSize` | `page_size` | query |  |  | `Y` | Rows per page (the site uses 100). |

### Returns {#cba_player_base-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `age` | integer | Age in years in that season. |
| `assists` | numeric | Assists per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `birth_date` | character | Date of birth 'YYYY-MM-DD' (a public player bio field; midnight-suffixed 'YYYY-MM-DD 00:00:00' on player-base-list). |
| `blocked` | numeric | Blocks per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `cn_alias` | character | Player name (Chinese). Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `field_goals` | numeric | Field goals made per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `field_goals_at_rim_attempted` | numeric | At-the-rim (inside the restricted area) field goals attempted in the season (total), a float. |
| `field_goals_at_rim_made` | numeric | At-the-rim field goals made in the season (total), a float. |
| `field_goals_attempted` | numeric | Field goals attempted per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `field_goals_mid_range_attempted` | numeric | Mid-range (two-point shots outside the restricted area) field goals attempted in the season (total), a float. |
| `field_goals_mid_range_made` | numeric | Mid-range field goals made in the season (total), a float. |
| `field_goals_percentage` | numeric | Field-goal percentage as a percent string ('64.9%'). |
| `field_goals_percentage_sort` | numeric | Field-goal percentage as a FRACTION 0-1 for sorting. |
| `fouls` | numeric | Personal fouls per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `fouls_defensive` | numeric | Defensive personal fouls in the season (total), a float. |
| `free_throws` | numeric | Free throws made per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `free_throws_attempted` | numeric | Free throws attempted per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `free_throws_percentage` | numeric | Free-throw percentage as a percent string. |
| `free_throws_percentage_sort` | numeric | Free-throw percentage as a FRACTION 0-1 for sorting. |
| `game_start_num` | integer | Games started (首发). |
| `minutes` | numeric | Minutes: per game as a decimal STRING ('25.9') on player-base-list; a whole-season total integer on the offensive/defensive/shot lists (827). |
| `nationality` | character | Nationality (Chinese country name; see the nationality code table for every value seen and its English name). Chinese country names seen: 中国 = China; 美国 = United States; 加拿大 = Canada; 法国 = France; 中国台湾 = Chinese Taipei; 伊朗 = Iran; 尼日利亚 = Nigeria; 约旦 = Jordan. |
| `number` | character | Jersey number as a string. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `player_times` | numeric | Games played (场数), a float (39.0). |
| `player_times2` | character | Unobserved: null in every sample. |
| `points` | numeric | Points per game, as a decimal STRING ('16.8') when countRanger is 1 (the request value); other countRanger values change the unit (2 total, 3 per 36 minutes, 4 per 100 possessions). |
| `position` | character | Position label (Chinese). Eras differ: coarse 后卫 (guard) / 前锋 (forward) / 中锋 (center) in older seasons, five-way 控球后卫 (point guard) / 得分后卫 (shooting guard) / 小前锋 (small forward) / 大前锋 (power forward) / 中锋 (center) in newer ones; dual positions are joined with '/' (e.g. 小前锋/大前锋). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `position_num` | numeric | Position as a number 1-5 (1 point guard, 2 shooting guard, 3 small forward, 4 power forward, 5 center); a fraction such as 4.5 averages a dual position; typed as float. |
| `rank_sort` | character | Unobserved: null in every sample. |
| `rebounds` | numeric | Total rebounds per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `rebounds_defensive` | numeric | Defensive rebounds per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `rebounds_offensive` | numeric | Offensive rebounds per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `schedule_id` | integer | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `seconds` | numeric | Seconds: per-game average as a decimal STRING on player-base-list; a whole-season total integer elsewhere. |
| `steals` | numeric | Steals per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `team_cn_alias` | character | Team short name (Chinese) in that season. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `three_point_attempted` | numeric | Three-pointers attempted per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `three_point_goals` | numeric | Three-pointers made per game, as a decimal STRING ('1.2') when countRanger is 1. |
| `three_point_percentage` | numeric | Three-point percentage as a percent string. |
| `three_point_percentage_sort` | numeric | Three-point percentage as a FRACTION 0-1 for sorting. |
| `turnovers` | numeric | Turnovers per game, as a decimal STRING ('1.2') when countRanger is 1. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_base-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_base-example}

```python
cba_player_base(season=2026, team_id=29128)
```

_Last validated n/a._

## cba_player_offensive

Season player offensive rates (one page).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-offensive-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-offensive-list?pageNumber=1&pageSize=100' -H 'Content-Type: application/json' -d '{"season": 2025, "matchTypeId": 1, "countRanger": 1, "playerRanger": 1, "teamId": 29128, "sort": 3, "rank": 1}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The API takes the START year (2025), so cba_runtime sends season - 1. |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |
| `countRanger` | `count_ranger` | body |  |  | `Y` | How counting statistics scale: 1 per game (default), 2 season total, 3 per 36 minutes, 4 per 100 possessions. |
| `playerRanger` | `player_ranger` | body |  |  | `Y` | Which players: 1 all (default), 2 domestic (Chinese) only, 4 foreign only. |
| `teamId` | `team_id` | body |  |  | `Y` | Optional CBA team id (clubs, stable across seasons and renames) to list one team's players. |
| `sort` | `sort` | body |  |  | `Y` | Sort column code the site sends (3 is the default order); the API answers an empty body without it. |
| `rank` | `rank` | body |  |  | `Y` | Sort direction code the site sends (1 default); the API answers an empty body without it. |
| `pageNumber` | `page_number` | query |  |  | `Y` | Page number of the paged list, from 1. |
| `pageSize` | `page_size` | query |  |  | `Y` | Rows per page (the site uses 100). |

### Returns {#cba_player_offensive-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `age` | integer | Age in years in that season. |
| `assists` | integer | Assists in the season (total, integer). |
| `assists_rate` | numeric | 助攻率 assist rate (site definition, as assistsPercentage). |
| `assists_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 助攻率 assist rate (site definition, as assistsPercentage). |
| `assists_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 助攻率 assist rate (site definition, as assistsPercentage). |
| `assists_round_rate` | numeric | 助攻率/回合占有率 assist rate divided by possession share (site definition: 球员助攻率和回合占有率的比值). |
| `assists_round_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 助攻率/回合占有率 assist rate divided by possession share (site definition: 球员助攻率和回合占有率的比值). |
| `assists_round_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 助攻率/回合占有率 assist rate divided by possession share (site definition: 球员助攻率和回合占有率的比值). |
| `birth_date` | character | Date of birth 'YYYY-MM-DD' (a public player bio field; midnight-suffixed 'YYYY-MM-DD 00:00:00' on player-base-list). |
| `field_goals_attempted` | integer | Field goals attempted in the season (total, integer). |
| `first_position` | character | Primary position label (Chinese; see position for the label set). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `free_throws` | integer | Free throws made in the season (total, integer). |
| `is_color` | integer | Highlight flag, 1 or 2 (unverified meaning). |
| `minutes` | integer | Minutes: per game as a decimal STRING ('25.9') on player-base-list; a whole-season total integer on the offensive/defensive/shot lists (827). |
| `nationality` | character | Nationality (Chinese country name; see the nationality code table for every value seen and its English name). Chinese country names seen: 中国 = China; 美国 = United States; 加拿大 = Canada; 法国 = France; 中国台湾 = Chinese Taipei; 伊朗 = Iran; 尼日利亚 = Nigeria; 约旦 = Jordan. |
| `number` | character | Jersey number as a string. |
| `play_times` | integer | Games played (场数), integer. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `player_name` | character | Player name (Chinese). |
| `position` | character | Position label (Chinese). Eras differ: coarse 后卫 (guard) / 前锋 (forward) / 中锋 (center) in older seasons, five-way 控球后卫 (point guard) / 得分后卫 (shooting guard) / 小前锋 (small forward) / 大前锋 (power forward) / 中锋 (center) in newer ones; dual positions are joined with '/' (e.g. 小前锋/大前锋). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `position_num` | numeric | Position as a number 1-5 (1 point guard, 2 shooting guard, 3 small forward, 4 power forward, 5 center); a fraction such as 4.5 averages a dual position; typed as float. |
| `rank` | character | Unobserved: null in every sample (a row-rank slot; the site numbers rows itself). |
| `real_rate` | numeric | 真实命中率 true-shooting percentage (site definition, as trueShootingPercentage). |
| `real_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 真实命中率 true-shooting percentage (site definition, as trueShootingPercentage). |
| `real_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 真实命中率 true-shooting percentage (site definition, as trueShootingPercentage). |
| `round_rate` | numeric | 回合占有率 'possession share': the share of the team's possessions the player ends while on the court (site definition). |
| `round_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 回合占有率 'possession share': the share of the team's possessions the player ends while on the court (site definition). |
| `round_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 回合占有率 'possession share': the share of the team's possessions the player ends while on the court (site definition). |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `seconds` | integer | Seconds: per-game average as a decimal STRING on player-base-list; a whole-season total integer elsewhere. |
| `team_cn_alias` | character | Team short name (Chinese) in that season. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `turnovers` | integer | Turnovers in the season (total, integer). |
| `turnovers_rate` | numeric | 失误率 turnover rate: for a player, the share of his possession-ending plays that are turnovers; for a team, the share of the team's possessions ending in a turnover (site definitions). |
| `turnovers_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 失误率 turnover rate: for a player, the share of his possession-ending plays that are turnovers; for a team, the share of the team's possessions ending in a turnover (site definitions). |
| `turnovers_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 失误率 turnover rate: for a player, the share of his possession-ending plays that are turnovers; for a team, the share of the team's possessions ending in a turnover (site definitions). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_offensive-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_offensive-example}

```python
cba_player_offensive(season=2026, team_id=29128)
```

_Last validated n/a._

## cba_player_defensive

Season player defensive rates (one page).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-defensive-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-defensive-list?pageNumber=1&pageSize=100' -H 'Content-Type: application/json' -d '{"season": 2025, "matchTypeId": 1, "countRanger": 1, "playerRanger": 1, "teamId": 29128, "sort": 3, "rank": 1}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The API takes the START year (2025), so cba_runtime sends season - 1. |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |
| `countRanger` | `count_ranger` | body |  |  | `Y` | How counting statistics scale: 1 per game (default), 2 season total, 3 per 36 minutes, 4 per 100 possessions. |
| `playerRanger` | `player_ranger` | body |  |  | `Y` | Which players: 1 all (default), 2 domestic (Chinese) only, 4 foreign only. |
| `teamId` | `team_id` | body |  |  | `Y` | Optional CBA team id (clubs, stable across seasons and renames) to list one team's players. |
| `sort` | `sort` | body |  |  | `Y` | Sort column code the site sends (3 is the default order); the API answers an empty body without it. |
| `rank` | `rank` | body |  |  | `Y` | Sort direction code the site sends (1 default); the API answers an empty body without it. |
| `pageNumber` | `page_number` | query |  |  | `Y` | Page number of the paged list, from 1. |
| `pageSize` | `page_size` | query |  |  | `Y` | Rows per page (the site uses 100). |

### Returns {#cba_player_defensive-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `age` | integer | Age in years in that season. |
| `blocked_rate` | numeric | 盖帽率 block rate (site definition, as blockedPercentage). |
| `blocked_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 盖帽率 block rate (site definition, as blockedPercentage). |
| `blocked_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 盖帽率 block rate (site definition, as blockedPercentage). |
| `first_position` | character | Primary position label (Chinese; see position for the label set). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `fouls_rate` | numeric | 犯规率 foul rate: defensive fouls per 100 possessions (site definition: 每百回合防守犯规数). |
| `fouls_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 犯规率 foul rate: defensive fouls per 100 possessions (site definition: 每百回合防守犯规数). |
| `fouls_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 犯规率 foul rate: defensive fouls per 100 possessions (site definition: 每百回合防守犯规数). |
| `is_color` | integer | Highlight flag, 1 or 2 (unverified meaning; the site uses it to colour the row). |
| `minutes` | integer | Minutes: per game as a decimal STRING ('25.9') on player-base-list; a whole-season total integer on the offensive/defensive/shot lists (827). |
| `nationality` | character | Nationality (Chinese country name; see the nationality code table for every value seen and its English name). Chinese country names seen: 中国 = China; 美国 = United States; 加拿大 = Canada; 法国 = France; 中国台湾 = Chinese Taipei; 伊朗 = Iran; 尼日利亚 = Nigeria; 约旦 = Jordan. |
| `number` | character | Jersey number as a string. |
| `play_times` | integer | Games played (场数), integer. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `player_name` | character | Player name (Chinese). |
| `position` | character | Position label (Chinese). Eras differ: coarse 后卫 (guard) / 前锋 (forward) / 中锋 (center) in older seasons, five-way 控球后卫 (point guard) / 得分后卫 (shooting guard) / 小前锋 (small forward) / 大前锋 (power forward) / 中锋 (center) in newer ones; dual positions are joined with '/' (e.g. 小前锋/大前锋). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `position_num` | numeric | Position as a number 1-5 (1 point guard, 2 shooting guard, 3 small forward, 4 power forward, 5 center); a fraction such as 4.5 averages a dual position; typed as float. |
| `rank` | character | Unobserved: null in every sample (a row-rank slot; the site numbers rows itself). |
| `rebounds_defensive_rate` | numeric | 防守篮板率 defensive-rebound rate (site definition, as reboundsDefensivePercentage). |
| `rebounds_defensive_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 防守篮板率 defensive-rebound rate (site definition, as reboundsDefensivePercentage). |
| `rebounds_defensive_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 防守篮板率 defensive-rebound rate (site definition, as reboundsDefensivePercentage). |
| `rebounds_offensive_rate` | numeric | 进攻篮板率 offensive-rebound rate (site definition, as reboundsOffensivePercentage). |
| `rebounds_offensive_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 进攻篮板率 offensive-rebound rate (site definition, as reboundsOffensivePercentage). |
| `rebounds_offensive_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 进攻篮板率 offensive-rebound rate (site definition, as reboundsOffensivePercentage). |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `seconds` | integer | Seconds: per-game average as a decimal STRING on player-base-list; a whole-season total integer elsewhere. |
| `steals_rate` | numeric | 抢断率 steal rate (site definition, as stealsPercentage). |
| `steals_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 抢断率 steal rate (site definition, as stealsPercentage). |
| `steals_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 抢断率 steal rate (site definition, as stealsPercentage). |
| `team_cn_alias` | character | Team short name (Chinese) in that season. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_defensive-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_defensive-example}

```python
cba_player_defensive(season=2026, team_id=29128)
```

_Last validated n/a._
