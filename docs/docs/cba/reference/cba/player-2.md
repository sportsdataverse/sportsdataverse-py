---
title: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Player: shots"
sidebar_label: "Player: shots"
sidebar_position: 3
description: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Player: shots — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Player: shots

## cba_player_shots

Season player shot-zone profile (one page).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/player-shoot-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/player-shoot-list?pageNumber=1&pageSize=100' -H 'Content-Type: application/json' -d '{"season": 2025, "matchTypeId": 1, "countRanger": 1, "playerRanger": 1, "teamId": 29128, "sort": 3, "rank": 1}'`

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

### Returns {#cba_player_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `age` | integer | Age in years in that season. |
| `at_rim_made_rate` | numeric | 篮下频率 at-rim frequency: restricted-area attempts per 100 possessions (site definition: 每百回合合理冲撞区内的出手数). |
| `at_rim_made_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 篮下频率 at-rim frequency: restricted-area attempts per 100 possessions (site definition: 每百回合合理冲撞区内的出手数). |
| `at_rim_made_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 篮下频率 at-rim frequency: restricted-area attempts per 100 possessions (site definition: 每百回合合理冲撞区内的出手数). |
| `first_position` | character | Primary position label (Chinese; see position for the label set). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `free_rate` | numeric | 罚球频率 free-throw frequency: free-throw attempts per 100 possessions (site definition). |
| `free_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 罚球频率 free-throw frequency: free-throw attempts per 100 possessions (site definition). |
| `free_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 罚球频率 free-throw frequency: free-throw attempts per 100 possessions (site definition). |
| `hit_at_rim_made_rate` | numeric | 篮下命中率 at-rim shooting percentage: restricted-area field-goal percentage (site definition). |
| `hit_at_rim_made_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 篮下命中率 at-rim shooting percentage: restricted-area field-goal percentage (site definition). |
| `hit_at_rim_made_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 篮下命中率 at-rim shooting percentage: restricted-area field-goal percentage (site definition). |
| `hit_free_rate` | numeric | 罚球命中率 free-throw percentage. |
| `hit_free_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 罚球命中率 free-throw percentage. |
| `hit_free_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 罚球命中率 free-throw percentage. |
| `hit_middle_rate` | numeric | 中距离命中率 mid-range shooting percentage: percentage on two-point attempts outside the restricted area (site definition). |
| `hit_middle_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 中距离命中率 mid-range shooting percentage: percentage on two-point attempts outside the restricted area (site definition). |
| `hit_middle_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 中距离命中率 mid-range shooting percentage: percentage on two-point attempts outside the restricted area (site definition). |
| `hit_rate` | numeric | 有效命中率 effective field-goal percentage: all field-goal attempts converted to two-point attempts (site definition: 球员所有运动战出手都折算为两分出手时的命中率). |
| `hit_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 有效命中率 effective field-goal percentage: all field-goal attempts converted to two-point attempts (site definition: 球员所有运动战出手都折算为两分出手时的命中率). |
| `hit_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 有效命中率 effective field-goal percentage: all field-goal attempts converted to two-point attempts (site definition: 球员所有运动战出手都折算为两分出手时的命中率). |
| `hit_three_rate` | numeric | 三分命中率 three-point shooting percentage. |
| `hit_three_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 三分命中率 three-point shooting percentage. |
| `hit_three_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 三分命中率 three-point shooting percentage. |
| `is_color` | integer | Highlight flag, 1 or 2 (unverified meaning; the site uses it to colour the row). |
| `middle_rate` | numeric | 中距离频率 mid-range frequency: two-point attempts outside the restricted area per 100 possessions (site definition). |
| `middle_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 中距离频率 mid-range frequency: two-point attempts outside the restricted area per 100 possessions (site definition). |
| `middle_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 中距离频率 mid-range frequency: two-point attempts outside the restricted area per 100 possessions (site definition). |
| `minutes` | integer | Minutes: per game as a decimal STRING ('25.9') on player-base-list; a whole-season total integer on the offensive/defensive/shot lists (827). |
| `nationality` | character | Nationality (Chinese country name; see the nationality code table for every value seen and its English name). Chinese country names seen: 中国 = China; 美国 = United States; 加拿大 = Canada; 法国 = France; 中国台湾 = Chinese Taipei; 伊朗 = Iran; 尼日利亚 = Nigeria; 约旦 = Jordan. |
| `number` | character | Jersey number as a string. |
| `play_times` | integer | Games played (场数), integer. |
| `player_id` | integer | playerId (key space: CBA persons, stable across seasons and clubs; 3-9 digits, older players have short ids (393433) and newer ones 100xxxxxx). Joins the player box routes, the season player lists and getPlayersByScheduleId. Not a team id. |
| `player_name` | character | Player name (Chinese). |
| `position` | character | Position label (Chinese). Eras differ: coarse 后卫 (guard) / 前锋 (forward) / 中锋 (center) in older seasons, five-way 控球后卫 (point guard) / 得分后卫 (shooting guard) / 小前锋 (small forward) / 大前锋 (power forward) / 中锋 (center) in newer ones; dual positions are joined with '/' (e.g. 小前锋/大前锋). Label glosses: C = 中锋; C/PF = 中锋/大前锋; F = 前锋; G = 后卫; PF = 大前锋; PF/C = 大前锋/中锋; PF/SF = 大前锋/小前锋; PG = 控球后卫; PG/SG = 控球后卫/得分后卫; SF = 小前锋; SF/PF = 小前锋/大前锋; SF/SG = 小前锋/得分后卫; SG = 得分后卫; SG/PG = 得分后卫/控球后卫; SG/SF = 得分后卫/小前锋; 中锋 = 中锋; 中锋/大前锋 = 中锋/大前锋; 前锋 = 前锋; 后卫 = 后卫; 大前锋 = 大前锋; 大前锋/中锋 = 大前锋/中锋; 大前锋/小前锋 = 大前锋/小前锋; 小前锋 = 小前锋; 小前锋/大前锋 = 小前锋/大前锋; 小前锋/得分后卫 = 小前锋/得分后卫; 得分后卫 = 得分后卫; 控球后卫 = 控球后卫; 控球后卫/得分后卫 = 控球后卫/得分后卫. |
| `position_num` | numeric | Position as a number 1-5 (1 point guard, 2 shooting guard, 3 small forward, 4 power forward, 5 center); a fraction such as 4.5 averages a dual position; typed as float. |
| `rank` | character | Unobserved: null in every sample (a row-rank slot; the site numbers rows itself). |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `seconds` | integer | Seconds: per-game average as a decimal STRING on player-base-list; a whole-season total integer elsewhere. |
| `team_cn_alias` | character | Team short name (Chinese) in that season. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `three_rate` | numeric | 三分频率 three-point frequency: three-point attempts per 100 possessions (site definition). |
| `three_rate_rank` | numeric | Colour rank for the metric, a fraction 0-1 (1 = best, inverted for 'bad' statistics such as turnovers) that the site uses to colour the cell (inferred from the sample values; the base population is not documented). Metric: 三分频率 three-point frequency: three-point attempts per 100 possessions (site definition). |
| `three_rate_sort` | numeric | Unformatted numeric value behind the formatted string of the same metric (a fraction 0-1 for percentages), used by the site to sort the table. Metric: 三分频率 three-point frequency: three-point attempts per 100 possessions (site definition). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_player_shots-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host answered 404, or the game has no play-by-play (2009-10). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_player_shots-example}

```python
cba_player_shots(season=2026, team_id=29128)
```

_Last validated n/a._
