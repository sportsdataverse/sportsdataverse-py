# CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Team

> CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Team — function reference in sdv-py, the SportsDataverse Python package.

## cba_team_options

The teams of one season as a select list: team id and the season's display name.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/com-code-tables/getTeam`

**Valid URL:** [https://data-server.cbaleague.com/api/com-code-tables/getTeam?type=2&season=2026](https://data-server.cbaleague.com/api/com-code-tables/getTeam?type=2&season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The API takes the START year (2025), so cba_runtime sends season - 1. |

### Returns {#cba_team_options-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `is_default` | integer | Default-selection flag: 2 = not the default on every row (the site prepends its own 'all teams' row, which this route does not send). |
| `key` | character | Team display name (Chinese) in the requested season; names change with the sponsor, the teamId does not. |
| `value` | character | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. Here a string of digits. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_team_options-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_team_options-example}

```python
cba_team_options(season=2026)
```

_Last validated n/a._

## cba_teams

The current team list: logo, team id, club name and display name.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/teams/teamList`

**Valid URL:** [https://data-server.cbaleague.com/api/teams/teamList](https://data-server.cbaleague.com/api/teams/teamList)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#cba_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club` | character | Empty string on every row; meaning unverified (a club-code slot the site leaves unused). |
| `team_cn_alias` | character | Current team name (Chinese short name, e.g. 北京控股 'Beijing Holdings'). Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_logo` | character | Protocol-relative URL of the 300x300 team logo ('//image.cbaleague.com/cbaleague/teamlogo/300x300/<teamId>_300.png'). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_teams-example}

```python
cba_teams()
```

_Last validated n/a._

## cba_team_counts

Team counts of one season, keyed by match type.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/team-count/season`

**Valid URL:** [https://data-server.cbaleague.com/api/team-count/season?season=2026](https://data-server.cbaleague.com/api/team-count/season?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The API takes the START year (2025), so cba_runtime sends season - 1. |

### Returns {#cba_team_counts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | character | Match type id as a string ('1' regular season, '2' playoffs); see getMatchType. |
| `value` | integer | Number of teams in that match type in the requested season (inferred: 20 for key '1' in 2025, equal to the 2025 getTeam list length; the playoff count is smaller). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_team_counts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_team_counts-example}

```python
cba_team_counts(season=2026)
```

_Last validated n/a._

## cba_team_basic

A team's per-game averages or totals, one row per season of the team.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/teams/basic-data`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/teams/basic-data' -H 'Content-Type: application/json' -d '{"targetId": 29128, "matchTypeId": 1, "statisticalDimension": 1, "selfOrOther": 1}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `targetId` | `team_id` | body |  | `Y` |  | Required. CBA team id (clubs, stable across seasons and renames). |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |
| `statisticalDimension` | `statistical_dimension` | body |  |  | `Y` | How counting columns are formatted: 1 per game (default), 2 season total, 3 and 4 per 100 possessions. |
| `selfOrOther` | `self_or_other` | body |  |  | `Y` | Whose statistics: 1 the team's own (default), 2 its opponents'. |

### Returns {#cba_team_basic-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | numeric | Assists, formatted by statisticalDimension. A number (the API sends it as display text). |
| `blocked` | numeric | Blocks, formatted by statisticalDimension. A number (the API sends it as display text). |
| `cba_rank` | character | Unobserved: null in every sample (a league-rank slot). |
| `field_goals` | numeric | Field goals made, formatted by statisticalDimension. A number (the API sends it as display text). |
| `field_goals_attempted` | numeric | Field goals attempted, formatted by statisticalDimension. A number (the API sends it as display text). |
| `field_goals_percentage_rate` | numeric | Field-goal percentage. A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |
| `fouls` | numeric | Personal fouls, formatted by statisticalDimension. A number (the API sends it as display text). |
| `free_throws` | numeric | Free throws made, formatted by statisticalDimension. A number (the API sends it as display text). |
| `free_throws_attempted` | numeric | Free throws attempted, formatted by statisticalDimension. A number (the API sends it as display text). |
| `free_throws_percentage_rate` | numeric | Free-throw percentage. A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |
| `losses_number` | integer | Losses (负场). (as the API names it) |
| `points` | numeric | Points, formatted by statisticalDimension (1 per-game average, 2 season total, 3 and 4 per 100 possessions: both give the same values, 120.1 = boutPoints). A number (the API sends it as display text). |
| `real_player_times` | integer | Games counted for the averages (实际场数 by its name); equals scheduleNumber in the sample except in short seasons (unverified). |
| `rebounds` | numeric | Total rebounds, formatted by statisticalDimension. A number (the API sends it as display text). |
| `rebounds_defensive` | numeric | Defensive rebounds, formatted by statisticalDimension. A number (the API sends it as display text). |
| `rebounds_offensive` | numeric | Offensive rebounds, formatted by statisticalDimension. A number (the API sends it as display text). |
| `round` | numeric | Possessions in the season (回合数) as a float (4074.09); identical for statisticalDimension 1-4. |
| `schedule_number` | integer | Games played in that season and match type (场数). |
| `schedule_season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. One row per season of the team. |
| `season` | character | Season label 'YYYY-YYYY' as a string ('2024-2025' = start year - end year; the sdv END year is the second one); scheduleSeason is the numeric START year. |
| `steals` | numeric | Steals, formatted by statisticalDimension. A number (the API sends it as display text). |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_name` | character | Team name (Chinese) in that season; changes with the sponsor (the sampled team 29128 has 9 different names across 20 seasons). Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `three_point_attempted` | numeric | Three-pointers attempted, formatted by statisticalDimension. A number (the API sends it as display text). |
| `three_point_goals` | numeric | Three-pointers made, formatted by statisticalDimension. A number (the API sends it as display text). |
| `three_point_percentage_rate` | numeric | Three-point percentage. A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |
| `time` | numeric | Team minutes per game (statisticalDimension 1) or in total (2). A number (the API sends it as display text). |
| `turnovers` | numeric | Turnovers, formatted by statisticalDimension. A number (the API sends it as display text). |
| `win_number` | integer | Wins (胜场). (as the API names it) |
| `win_rate` | numeric | Win rate (胜率). A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_team_basic-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_team_basic-example}

```python
cba_team_basic(team_id=29128)
```

_Last validated n/a._

## cba_team_four

A team's four-factors table, one row per season of the team.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/teams/four-elements`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/teams/four-elements' -H 'Content-Type: application/json' -d '{"targetId": 29128, "matchTypeId": 1}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `targetId` | `team_id` | body |  | `Y` |  | Required. CBA team id (clubs, stable across seasons and renames). |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |

### Returns {#cba_team_four-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `effective_hit_rate` | numeric | 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `effective_hit_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `effective_hit_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `effective_hit_sort` | numeric | Unformatted fraction 0-1 behind effectiveHitRate. |
| `field_goals` | integer | Field goals made in the season (sum). |
| `field_goals_attempted` | integer | Field goals attempted in the season (sum). |
| `free_throw_rate` | numeric | 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `free_throw_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `free_throw_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `free_throw_sort` | numeric | Unformatted fraction 0-1 behind freeThrowRate. |
| `free_throws` | integer | Free throws made in the season (sum). |
| `loss_average` | numeric | League-average losses; 0.0 in every sample. |
| `loss_num` | integer | Losses in that season. |
| `offensive_rebound_rate` | numeric | 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `offensive_rebound_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `offensive_rebound_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `offensive_rebound_sort` | numeric | Unformatted fraction 0-1 behind offensiveReboundRate. |
| `other_effective_hit_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `other_effective_hit_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `other_effective_hit_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `other_effective_hit_sort` | numeric | Unformatted fraction 0-1 behind otherEffectiveHitRate. |
| `other_field_goals` | integer | Field goals made by the team's opponents (sum). |
| `other_field_goals_attempted` | integer | Field goals attempted by the opponents (sum). |
| `other_free_throw_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `other_free_throw_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `other_free_throw_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `other_free_throw_sort` | numeric | Unformatted fraction 0-1 behind otherFreeThrowRate. |
| `other_free_throws` | integer | Free throws made by the opponents (sum). |
| `other_offensive_rebound_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_offensive_rebound_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_offensive_rebound_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_offensive_rebound_sort` | numeric | Unformatted fraction 0-1 behind otherOffensiveReboundRate. |
| `other_points_hundred_round` | numeric | 百回合失分 points allowed per 100 possessions (defensive rating): the opponents' points per 100 possessions. |
| `other_points_hundred_round_average` | numeric | League average of the metric. Metric: 百回合失分 points allowed per 100 possessions (defensive rating): the opponents' points per 100 possessions. |
| `other_points_hundred_round_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 百回合失分 points allowed per 100 possessions (defensive rating): the opponents' points per 100 possessions. |
| `other_rebounds_defensive` | integer | Defensive rebounds by the opponents (sum). |
| `other_rebounds_offensive` | integer | Offensive rebounds by the opponents (sum). |
| `other_three_point_goals` | integer | Three-pointers made by the opponents (sum). |
| `other_turnovers` | integer | Turnovers by the opponents (sum). |
| `other_turnovers_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `other_turnovers_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `other_turnovers_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `other_turnovers_sort` | numeric | Unformatted fraction 0-1 behind otherTurnoversRate. |
| `other_win_points_hundred_round` | numeric | Net points per 100 possessions as seen from the opponents' side (the site's 'other' block). |
| `points_hundred_round` | numeric | 百回合得分 points scored per 100 possessions (offensive rating). |
| `points_hundred_round_average` | numeric | League average of the metric. Metric: 百回合得分 points scored per 100 possessions (offensive rating). |
| `points_hundred_round_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 百回合得分 points scored per 100 possessions (offensive rating). |
| `rhythm` | numeric | 节奏 pace: possessions per 48 minutes (site definition: 每48分钟回合数). A number (the API sends it as display text). |
| `rhythm_num` | numeric | Unobserved: null in every sample (a numeric form of the pace; the schedule-four-elements route fills it). Typed Float64 like the family's other routes. |
| `rhythm_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 节奏 pace: possessions per 48 minutes (site definition: 每48分钟回合数). |
| `round` | numeric | Possessions in the season (回合数), a float such as 2606.36. |
| `round_jingque` | numeric | Possessions in the season computed 'precisely' (精确回合数 by its name); equals round in the sample (unverified difference). |
| `schedule_season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. One row per season of the team. |
| `season` | integer | Season of the row ('2025' = the START year of 2025-26; sdv END year = value + 1); equals scheduleSeason. An integer (the API sends it as digit text). |
| `self_rebounds_defensive` | integer | Defensive rebounds by the team in the season (sum). |
| `self_rebounds_offensive` | integer | Offensive rebounds by the team in the season (sum). |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_name` | character | Team name (Chinese) in that season; changes with the sponsor. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `three_point_goals` | integer | Three-pointers made in the season (sum). |
| `total_minute` | integer | Team minutes played in the season (sum, 1440 = 30 games x 48). |
| `total_other_points` | integer | Points the team's opponents scored in the season (sum). |
| `total_points` | integer | Points the team scored in the season (sum). |
| `turnovers` | integer | Turnovers in the season (sum). |
| `turnovers_rate` | numeric | 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `turnovers_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `turnovers_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `turnovers_sort` | numeric | Unformatted fraction 0-1 behind turnoversRate. |
| `win_num` | integer | Wins in that season (regular season for matchTypeId 1). |
| `win_points_hundred_round` | numeric | 百回合净胜分 net points per 100 possessions. |
| `win_points_hundred_round_average` | numeric | League average of the metric. Metric: 百回合净胜分 net points per 100 possessions. |
| `win_points_hundred_round_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 百回合净胜分 net points per 100 possessions. |
| `wine_average` | numeric | League-average wins (field name sic); 0.0 in every sample. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_team_four-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_team_four-example}

```python
cba_team_four(team_id=29128)
```

_Last validated n/a._

## cba_team_overview

A team's overview table (record, pace, per-100 ratings), one row per season of the team.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/team-match-datas/team-entirety-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/team-match-datas/team-entirety-list?pageNumber=1&pageSize=100' -H 'Content-Type: application/json' -d '{"teamId": 29128, "matchTypeId": 1}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `teamId` | `team_id` | body |  | `Y` |  | Required. CBA team id (clubs, stable across seasons and renames). |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |
| `pageNumber` | `page_number` | query |  |  | `Y` | Page number of the paged list, from 1. |
| `pageSize` | `page_size` | query |  |  | `Y` | Rows per page (the site uses 100). |

### Returns {#cba_team_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `bout_no_points` | numeric | 百回合失分 points allowed per 100 possessions (defensive rating), a float. |
| `bout_no_points_rank` | numeric | Rank among the teams of that season for boutNoPoints (integer-valued float; direction undocumented). |
| `bout_no_points_sort` | numeric | Unobserved: null in every sample (numeric value behind boutNoPoints). |
| `bout_point_sort` | numeric | Unobserved: null in every sample (numeric value behind boutPoints). |
| `bout_points` | numeric | 百回合得分 points scored per 100 possessions (offensive rating), a float (120.1). |
| `bout_points_rank` | numeric | Rank among the teams of that season for boutPoints (integer-valued float; direction undocumented). |
| `bout_subtract_points` | numeric | 百回合净胜分 net points per 100 possessions (boutPoints - boutNoPoints), a float. |
| `bout_subtract_points_rank` | numeric | Rank among the teams of that season for boutSubtractPoints (integer-valued float). |
| `bout_subtract_points_sort` | numeric | Unobserved: null in every sample (numeric value behind boutSubtractPoints). |
| `is_avg` | integer | Row kind: 2 on every sampled row = a team's own row (1 presumed league average, unobserved). |
| `loss_num` | numeric | Losses in that season, a float. |
| `player_times` | numeric | Games played, a float (42.0). |
| `rhythm` | numeric | 节奏 pace: possessions per 48 minutes ('75.7'; site definition: 每48分钟回合数). A number (the API sends it as display text). |
| `rhythm_rank` | numeric | Unobserved: null in every sample (a rank slot for pace). Typed Float64 like the family's other routes. |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. One row per season of the team. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_name` | character | Team name (Chinese) in that season; changes with the sponsor. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `win_num` | numeric | Wins in that season (match type of the request), a number typed as float (33.0). |
| `win_rate` | numeric | Win rate. A FRACTION 0-1 (the API sends a percent string such as '39.4%'). |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_team_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_team_overview-example}

```python
cba_team_overview(team_id=29128)
```

_Last validated n/a._

## cba_team_shots

A team's shot-zone profile, one row per season of the team (zone splits from 2017-18).

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/team-match-datas/team-shoot-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/team-match-datas/team-shoot-list?pageNumber=1&pageSize=100' -H 'Content-Type: application/json' -d '{"teamId": 29128, "matchTypeId": 1}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `teamId` | `team_id` | body |  | `Y` |  | Required. CBA team id (clubs, stable across seasons and renames). |
| `matchTypeId` | `match_type_id` | body |  |  | `Y` | Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off (cba_match_types). |
| `pageNumber` | `page_number` | query |  |  | `Y` | Page number of the paged list, from 1. |
| `pageSize` | `page_size` | query |  |  | `Y` | Rows per page (the site uses 100). |

### Returns {#cba_team_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `bout_no_points_sort` | numeric | Unobserved: null in every sample (numeric value behind 百回合失分 points allowed per 100 possessions). Typed Float64 like the family's other routes. |
| `bout_point_sort` | numeric | Unobserved: null in every sample (numeric value behind 百回合得分 points per 100 possessions). Typed Float64 like the family's other routes. |
| `bout_subtract_points_sort` | numeric | Unobserved: null in every sample (numeric value behind 百回合净胜分 net points per 100 possessions). Typed Float64 like the family's other routes. |
| `guard_field_goals_at_rim` | numeric | 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_field_goals_at_rim_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_field_goals_at_rim_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_hit_at_rim_made_rate` | numeric | 篮下命中率 at-rim shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_hit_at_rim_made_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下命中率 at-rim shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_hit_at_rim_made_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下命中率 at-rim shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_hit_middle_rate` | numeric | 中距离命中率 mid-range shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_hit_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离命中率 mid-range shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_hit_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离命中率 mid-range shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_middle_rate` | numeric | 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `guard_real_point` | numeric | 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_real_point_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_real_point_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_goals_rate` | numeric | 三分命中率 three-point shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_three_goals_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分命中率 three-point shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_goals_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分命中率 three-point shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_rate` | numeric | 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `guard_three_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `is_avg` | integer | Row kind: 2 on every sampled row = a team's own row; 1 is presumed to mark a league-average row but is unobserved. |
| `num` | numeric | Games the profile covers (the site's 场数 'games'), e.g. 42.0; a number typed as float. |
| `offensive_field_goals_at_rim` | numeric | 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the team's own shots (攻, offence).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_field_goals_at_rim_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_field_goals_at_rim_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_hit_at_rim_made_rate` | numeric | 篮下命中率 at-rim shooting percentage, for the team's own shots (攻, offence).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_hit_at_rim_made_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下命中率 at-rim shooting percentage, for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_hit_at_rim_made_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下命中率 at-rim shooting percentage, for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_hit_middle_rate` | numeric | 中距离命中率 mid-range shooting percentage, for the team's own shots (攻, offence).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_hit_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离命中率 mid-range shooting percentage, for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_hit_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离命中率 mid-range shooting percentage, for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_middle_rate` | numeric | 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the team's own shots (攻, offence).. Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the team's own shots (攻, offence). Null for the seasons 2006-2016 (sampled team 29128): the at-rim and mid-range zone splits start with 2017-18. |
| `offensive_real_point` | numeric | 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the team's own shots (攻, offence). A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_real_point_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the team's own shots (攻, offence). |
| `offensive_real_point_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the team's own shots (攻, offence). |
| `offensive_three_goals_rate` | numeric | 三分命中率 three-point shooting percentage, for the team's own shots (攻, offence). A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_three_goals_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分命中率 three-point shooting percentage, for the team's own shots (攻, offence). |
| `offensive_three_goals_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分命中率 three-point shooting percentage, for the team's own shots (攻, offence). |
| `offensive_three_rate` | numeric | 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the team's own shots (攻, offence). A FRACTION 0-1 (the API sends a percent string such as '39.4%'; the _sort column holds the same fraction). |
| `offensive_three_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the team's own shots (攻, offence). |
| `offensive_three_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the team's own shots (攻, offence). |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_name` | character | Team name (Chinese) in that season; changes with the sponsor. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Raises {#cba_team_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered 404, or the game has no play-by-play (2009-10). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body for another game or season, a partial play-by-play set, a dead connection). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cba_team_shots-example}

```python
cba_team_shots(team_id=29128)
```

_Last validated n/a._
