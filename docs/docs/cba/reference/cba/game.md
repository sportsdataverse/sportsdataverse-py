---
title: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — Game

## cba_game_info

Game report header: both teams, score, tip-off time and colours.

**Endpoint URL:** `GET https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/baseInfo/{schedule_id}`

**Valid URL:** [https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/baseInfo/100105799](https://data-server.cbaleague.com/api/team-match-datas/getMatchDataReport/baseInfo/100105799)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_id` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_id` | integer | CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed without it. |
| `count` | integer | Unverified: an integer shown in the report header (83 in the 2025-26 sample, 99 in 2017-18). Not the play-by-play row count; plausibly possessions per team, which the bundle does not confirm. |
| `guest_color` | character | Visiting team colour as a '#RRGGBB' hex string. |
| `guest_score` | integer | Visiting team final points. |
| `guest_team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. Visiting team. |
| `guest_team_logo` | character | Absolute URL of the visiting team logo. |
| `guest_team_name` | character | Visiting team name (Chinese) at the time of the game. Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `host_color` | character | Home team colour as a '#RRGGBB' hex string (the game report's theme colour). |
| `host_score` | integer | Home team final points. |
| `host_team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. Home team. |
| `host_team_logo` | character | Absolute URL of the home team logo. |
| `host_team_name` | character | Home team name (Chinese) at the time of the game (e.g. 广东东莞银行 in 2017-18). Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml. |
| `schedule_time` | character | Tip-off as 'YYYY-MM-DD HH:MM:SS' (China local time); equals schedules/day scheduleTime. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Example {#cba_game_info-example}

```python
cba_game_info(schedule_id=100105799)
```

_Last validated n/a._

## cba_game_team_shots

Shot-zone profile of the two teams of a game. NOT game data: the teams' current season table.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/team-match-datas/schedule-shoot-list`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/team-match-datas/schedule-shoot-list' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_game_team_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `bout_no_points_sort` | character | Unobserved: null in every sample (numeric value behind 百回合失分 points allowed per 100 possessions). |
| `bout_point_sort` | character | Unobserved: null in every sample (numeric value behind 百回合得分 points per 100 possessions). |
| `bout_subtract_points_sort` | character | Unobserved: null in every sample (numeric value behind 百回合净胜分 net points per 100 possessions). |
| `guard_field_goals_at_rim` | character | 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_field_goals_at_rim_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_field_goals_at_rim_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_hit_at_rim_made_rate` | character | 篮下命中率 at-rim shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_hit_at_rim_made_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下命中率 at-rim shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_hit_at_rim_made_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下命中率 at-rim shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_hit_middle_rate` | character | 中距离命中率 mid-range shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_hit_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离命中率 mid-range shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_hit_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离命中率 mid-range shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_middle_rate` | character | 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_real_point` | character | 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_real_point_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_real_point_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_goals_rate` | character | 三分命中率 three-point shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_three_goals_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分命中率 three-point shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_goals_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分命中率 three-point shooting percentage, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_rate` | character | 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). A string percent ('36.3%'). |
| `guard_three_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `guard_three_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the shots of the OPPONENTS against this team (守, defence; the 'guard' prefix is inferred from the site's 攻/防 offence/defence toggle). |
| `is_avg` | integer | Row kind: 2 on every sampled row = a team's own row; 1 is presumed to mark a league-average row but is unobserved. |
| `num` | numeric | Games the profile covers (the site's 场数 'games'), e.g. 42.0; a number typed as float. |
| `offensive_field_goals_at_rim` | character | 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_field_goals_at_rim_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the team's own shots (攻, offence). |
| `offensive_field_goals_at_rim_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下占比 at-rim share: the share of field-goal attempts taken at the rim (inside the restricted area 合理冲撞区), for the team's own shots (攻, offence). |
| `offensive_hit_at_rim_made_rate` | character | 篮下命中率 at-rim shooting percentage, for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_hit_at_rim_made_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 篮下命中率 at-rim shooting percentage, for the team's own shots (攻, offence). |
| `offensive_hit_at_rim_made_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 篮下命中率 at-rim shooting percentage, for the team's own shots (攻, offence). |
| `offensive_hit_middle_rate` | character | 中距离命中率 mid-range shooting percentage, for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_hit_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离命中率 mid-range shooting percentage, for the team's own shots (攻, offence). |
| `offensive_hit_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离命中率 mid-range shooting percentage, for the team's own shots (攻, offence). |
| `offensive_middle_rate` | character | 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_middle_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the team's own shots (攻, offence). |
| `offensive_middle_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 中距离占比 mid-range share: the share of field-goal attempts that are two-pointers outside the restricted area, for the team's own shots (攻, offence). |
| `offensive_real_point` | character | 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_real_point_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the team's own shots (攻, offence). |
| `offensive_real_point_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 有效命中率 effective field-goal percentage (all field goals counted as two-point attempts), for the team's own shots (攻, offence). |
| `offensive_three_goals_rate` | character | 三分命中率 three-point shooting percentage, for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_three_goals_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分命中率 three-point shooting percentage, for the team's own shots (攻, offence). |
| `offensive_three_goals_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分命中率 three-point shooting percentage, for the team's own shots (攻, offence). |
| `offensive_three_rate` | character | 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the team's own shots (攻, offence). A string percent ('36.3%'). |
| `offensive_three_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the team's own shots (攻, offence). |
| `offensive_three_rate_sort` | numeric | Unformatted fraction 0-1 behind the formatted string of the metric. Metric: 三分占比 three-point share: the share of field-goal attempts that are three-pointers, for the team's own shots (攻, offence). |
| `season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_name` | character | Team name (Chinese) in that season; changes with the sponsor. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Example {#cba_game_team_shots-example}

```python
cba_game_team_shots(schedule_id=100105799)
```

_Last validated n/a._

## cba_game_team_four

Four-factors table of the two teams of a game. NOT game data: the teams' previous season table.

**Endpoint URL:** `POST https://data-server.cbaleague.com/api/teams/schedule-four-elements`

**Example request:** `curl -X POST 'https://data-server.cbaleague.com/api/teams/schedule-four-elements' -H 'Content-Type: application/json' -d '{"scheduleId": 100105799}'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `scheduleId` | `schedule_id` |  | `Y` |  | Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served. |

### Returns {#cba_game_team_four-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `effective_hit_rate` | numeric | 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `effective_hit_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `effective_hit_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `free_throw_rate` | numeric | 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `free_throw_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `free_throw_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `loss_average` | numeric | League-average losses; 0.0 in every sample. |
| `loss_num` | integer | Losses of the team in the table's season. |
| `offensive_rebound_rate` | numeric | 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `offensive_rebound_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `offensive_rebound_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_effective_hit_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `other_effective_hit_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `other_effective_hit_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 有效命中率 effective field-goal percentage, in PERCENT as a number (53.6 = 53.6%) (site definition: 球队所有运动战出手都折算为两分出手时的命中率). |
| `other_free_throw_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `other_free_throw_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `other_free_throw_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 罚球率 free-throw rate: free throws made relative to field-goal attempts, in percent as a number (site definition: 球队罚球命中数和运动战出手数的比值). |
| `other_offensive_rebound_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_offensive_rebound_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_offensive_rebound_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 进攻篮板率 offensive-rebound rate: the team's offensive rebounds as a share of all offensive-rebound chances, in percent as a number (site definition). |
| `other_points_hundred_round` | numeric | 百回合失分 points allowed per 100 possessions (defensive rating): the opponents' points per 100 possessions. |
| `other_points_hundred_round_average` | numeric | League average of the metric. Metric: 百回合失分 points allowed per 100 possessions (defensive rating): the opponents' points per 100 possessions. |
| `other_points_hundred_round_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 百回合失分 points allowed per 100 possessions (defensive rating): the opponents' points per 100 possessions. |
| `other_turnovers_rate` | numeric | The same measure for the team's OPPONENTS (the site's 'other' block): 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `other_turnovers_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `other_turnovers_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: The same measure for the team's OPPONENTS (the site's 'other' block): 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `points_hundred_round` | numeric | 百回合得分 points scored per 100 possessions (offensive rating). |
| `points_hundred_round_average` | numeric | League average of the metric. Metric: 百回合得分 points scored per 100 possessions (offensive rating). |
| `points_hundred_round_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 百回合得分 points scored per 100 possessions (offensive rating). |
| `rhythm` | numeric | 节奏 pace: possessions per 48 minutes (site definition: 每48分钟回合数). |
| `rhythm_num` | character | Pace as a STRING with one decimal ('96.3'); same value as rhythm. |
| `rhythm_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 节奏 pace: possessions per 48 minutes (site definition: 每48分钟回合数). |
| `schedule_season` | integer | Season START year (2025 = the 2025-26 season). The sdv season key is the END year = season + 1; map at the boundary. The season of this table: one before the game's season (2025 game -> 2024). |
| `season` | character | Season of the table as a STRING ('2024' = the START year of 2024-25; sdv END year = value + 1); equals scheduleSeason. NOT the season of the game: the table is the PREVIOUS season's. |
| `team_id` | integer | teamId (key space: CBA clubs; 5 digits 29115-29140 for the long-standing clubs, 9 digits for newer ones (100074683 = Ningbo); stable across seasons and club renames, but the display name changes with the sponsor: 29128 = 浙江广厦 'Zhejiang Guangsha' under many sponsor names, 浙江浙商证券 in 2025-26). Joins getTeam / teamList and the team and player routes. Not a player or schedule id. |
| `team_name` | character | Team name (Chinese) in the table's season (can differ from the game's: 四川锦城 in the 2016 table of a 2017 Sichuan game). |
| `turnovers_rate` | numeric | 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `turnovers_rate_average` | character | Unobserved: null in every sample. When filled: league average of the metric. Metric: 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `turnovers_rate_rank` | numeric | Rank among the teams of that season (integer-valued number; direction per metric undocumented). Metric: 失误率 turnover rate: the share of the team's possessions ending in a turnover, in percent as a number (site definition). |
| `win_num` | integer | Wins of the team in the table's season (regular season). |
| `win_points_hundred_round` | numeric | 百回合净胜分 net points per 100 possessions. |
| `win_points_hundred_round_average` | numeric | League average of the metric. Metric: 百回合净胜分 net points per 100 possessions. |
| `wine_average` | numeric | League-average wins (field name sic); 0.0 in every sample. |

**`return_parsed=False`** — the raw DECRYPTED JSON (`list` or `dict`); a game route that answers an object also carries `_request` (the game id), and play-by-play is merged over every period into `{records, total}`.

### Example {#cba_game_team_four-example}

```python
cba_game_team_four(schedule_id=100105799)
```

_Last validated n/a._
