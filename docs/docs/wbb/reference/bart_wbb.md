---
title: WBB — Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)
sidebar_label: Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)
sidebar_position: 10
---
# WBB — Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)

`sportsdataverse.wbb` — 1 endpoint.

## `bart_wbb_ratings`

GET /ncaaw/{year}_team_results.csv — women's T-Rank team ratings (adjoe/adjde/barthag, one row per team; the team/conf pair feeds the WBB crosswalk).

**Endpoint URL:** `GET https://barttorvik.com/ncaaw/{year}_team_results.csv`

**Valid URL:** [https://barttorvik.com/ncaaw/2025_team_results.csv](https://barttorvik.com/ncaaw/2025_team_results.csv)

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
bart_wbb_ratings(year=2025)
```

_Last validated n/a._
