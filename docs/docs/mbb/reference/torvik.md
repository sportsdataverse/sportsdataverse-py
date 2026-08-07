---
title: MBB — Bart Torvik T-Rank (barttorvik.com)
sidebar_label: Bart Torvik T-Rank (barttorvik.com)
sidebar_position: 10
---
# MBB — Bart Torvik T-Rank (barttorvik.com)

`sportsdataverse.mbb` — 2 endpoints.

## `torvik_ratings`

GET /{year}_team_results.csv — men's T-Rank team ratings (adjoe/adjde/barthag, one row per team; the team/conf pair feeds the MBB crosswalk).

**Endpoint URL:** `GET https://barttorvik.com/{year}_team_results.csv`

**Valid URL:** [https://barttorvik.com/2025_team_results.csv](https://barttorvik.com/2025_team_results.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | T-Rank position (overall barthag rank). |
| `team` | character | Torvik team name (the crosswalk join key). |
| `conf` | character | Torvik conference abbreviation (e.g. B12, ACC, BE). |
| `record` | character | Overall win-loss record to date. |
| `adjoe` | numeric | Adjusted offensive efficiency (points per 100 possessions vs an average defense). |
| `oe_rank` | integer | National rank of adjusted offensive efficiency. |
| `adjde` | numeric | Adjusted defensive efficiency (points allowed per 100 possessions vs an average offense). |
| `de_rank` | integer | National rank of adjusted defensive efficiency. |
| `barthag` | numeric | Torvik power rating: win probability vs an average team on a neutral floor. |
| `rank_2` | integer | Barthag rank repeated as shipped in the source CSV (duplicate of rank). |
| `proj_w` | numeric | Projected full-season wins. |
| `proj_l` | numeric | Projected full-season losses. |
| `pro_con_w` | numeric | Projected conference wins. |
| `pro_con_l` | numeric | Projected conference losses. |
| `con_rec` | character | Conference win-loss record to date. |
| `sos` | numeric | Strength of schedule faced to date. |
| `ncsos` | numeric | Non-conference strength of schedule. |
| `consos` | numeric | Conference strength of schedule. |
| `proj_sos` | numeric | Projected full-season strength of schedule. |
| `proj_noncon_sos` | numeric | Projected non-conference strength of schedule. |
| `proj_con_sos` | numeric | Projected conference strength of schedule. |
| `elite_sos` | numeric | Elite strength of schedule (share of schedule vs elite opponents). |
| `elite_noncon_sos` | numeric | Elite non-conference strength of schedule. |
| `opp_oe` | numeric | Average opponent offensive efficiency. |
| `opp_de` | numeric | Average opponent defensive efficiency. |
| `opp_proj_oe` | numeric | Projected average opponent offensive efficiency. |
| `opp_proj_de` | numeric | Projected average opponent defensive efficiency. |
| `con_adj_oe` | numeric | Adjusted offensive efficiency in conference games only. |
| `con_adj_de` | numeric | Adjusted defensive efficiency in conference games only. |
| `qual_o` | numeric | Offensive efficiency vs quality (top-tier) opponents. |
| `qual_d` | numeric | Defensive efficiency vs quality (top-tier) opponents. |
| `qual_barthag` | numeric | Barthag computed from the quality-opponent efficiency splits. |
| `qual_games` | numeric | Number of quality games underlying the qual_* splits. |
| `fun` | numeric | Torvik's FUN style/entertainment index. |
| `con_pf` | numeric | Points scored in conference play. |
| `con_pa` | numeric | Points allowed in conference play. |
| `con_poss` | numeric | Possessions played in conference play. |
| `con_oe` | numeric | Raw offensive efficiency in conference play. |
| `con_de` | numeric | Raw defensive efficiency in conference play. |
| `con_sos_remain` | numeric | Strength of the remaining conference schedule. |
| `conf_win_percent` | numeric | Conference win percentage. |
| `wab` | numeric | Wins above bubble (resume quality vs a bubble-level team). |
| `wab_rk` | integer | National rank of wins above bubble. |
| `fun_rk` | integer | National rank of the FUN index. |
| `adjt` | numeric | Adjusted tempo (possessions per 40 minutes). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Example

```python
torvik_ratings(year=2025)
```

_Last validated n/a._

## `torvik_team_factors`

GET /{year}_fffinal.csv — men's four-factors splits (eFG%/FTR/OR%/TO% offense + defense, with per-stat ranks).

**Endpoint URL:** `GET https://barttorvik.com/{year}_fffinal.csv`

**Valid URL:** [https://barttorvik.com/2025_fffinal.csv](https://barttorvik.com/2025_fffinal.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_name` | character | Torvik team name. |
| `e_fg_percent` | numeric | Effective field-goal percentage on offense. |
| `rk` | integer | National rank of offensive eFG%. |
| `e_fg_percent_def` | numeric | Effective field-goal percentage allowed on defense. |
| `rk_2` | integer | National rank of defensive eFG% allowed. |
| `ftr` | numeric | Free-throw rate on offense (FTA per FGA). |
| `rk_3` | integer | National rank of offensive free-throw rate. |
| `ftr_def` | numeric | Free-throw rate allowed on defense. |
| `rk_4` | integer | National rank of defensive free-throw rate allowed. |
| `or_percent` | numeric | Offensive rebound percentage. |
| `rk_5` | integer | National rank of offensive rebound percentage. |
| `dr_percent` | numeric | Defensive rebound percentage. |
| `rk_6` | integer | National rank of defensive rebound percentage. |
| `to_percent` | numeric | Turnover percentage on offense. |
| `rk_7` | integer | National rank of offensive turnover percentage. |
| `to_percent_def` | numeric | Turnover percentage forced on defense. |
| `rk_8` | integer | National rank of defensive turnover percentage forced. |
| `x3p_percent` | numeric | Three-point percentage on offense. |
| `rk_9` | integer | National rank of offensive three-point percentage. |
| `x3p_d_percent` | numeric | Three-point percentage allowed on defense. |
| `rk_10` | integer | National rank of three-point percentage allowed. |
| `x2p_percent` | numeric | Two-point percentage on offense. |
| `rk_11` | integer | National rank of offensive two-point percentage. |
| `x2p_percent_d` | numeric | Two-point percentage allowed on defense. |
| `rk_12` | integer | National rank of two-point percentage allowed. |
| `ft_percent` | numeric | Free-throw percentage. |
| `rk_13` | integer | National rank of free-throw percentage. |
| `ft_percent_d` | numeric | Opponent free-throw percentage. |
| `rk_14` | integer | National rank of opponent free-throw percentage. |
| `x3p_rate` | numeric | Three-point attempt rate on offense (3PA per FGA). |
| `rk_15` | integer | National rank of offensive three-point attempt rate. |
| `x3p_rate_d` | numeric | Three-point attempt rate allowed on defense. |
| `rk_16` | integer | National rank of three-point attempt rate allowed. |
| `arate` | numeric | Assist rate on offense (assists per made field goal). |
| `rk_17` | integer | National rank of offensive assist rate. |
| `arate_d` | numeric | Assist rate allowed on defense. |
| `rk_18` | integer | National rank of assist rate allowed. |
| `unnamed` | numeric | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_2` | integer | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_3` | numeric | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_4` | integer | Headerless trailing column shipped in the source CSV (undocumented upstream). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Example

```python
torvik_team_factors(year=2025)
```

_Last validated n/a._
