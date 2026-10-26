---
title: Women's college basketball — Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)
sidebar_label: Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)
description: "Women's college basketball — Bart Torvik Women's T-Rank (barttorvik.com/ncaaw) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# Women's college basketball — Bart Torvik Women's T-Rank (barttorvik.com/ncaaw)

`sportsdataverse.wbb` — 1 endpoint.

## bart_wbb_ratings

GET /ncaaw/{year}_team_results.csv — women's T-Rank team ratings (adjoe/adjde/barthag, one row per team; the team/conf pair feeds the WBB crosswalk).

**Endpoint URL:** `GET https://barttorvik.com/ncaaw/{year}_team_results.csv`

**Valid URL:** [https://barttorvik.com/ncaaw/2025_team_results.csv](https://barttorvik.com/ncaaw/2025_team_results.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#bart_wbb_ratings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | T-Rank position (overall barthag rank). |
| `team` | String | Torvik team name (the crosswalk join key). |
| `conf` | String | Torvik conference abbreviation (e.g. B12, ACC, BE). |
| `record` | String | Overall win-loss record to date. |
| `adjoe` | Float64 | Adjusted offensive efficiency (points per 100 possessions vs an average defense). |
| `oe_rank` | Int64 | National rank of adjusted offensive efficiency. |
| `adjde` | Float64 | Adjusted defensive efficiency (points allowed per 100 possessions vs an average offense). |
| `de_rank` | Int64 | National rank of adjusted defensive efficiency. |
| `barthag` | Float64 | Torvik power rating: win probability vs an average team on a neutral floor. |
| `rank_2` | Int64 | Barthag rank repeated as shipped in the source CSV (duplicate of rank). |
| `proj_w` | Float64 | Projected full-season wins. |
| `proj_l` | Float64 | Projected full-season losses. |
| `pro_con_w` | Float64 | Projected conference wins. |
| `pro_con_l` | Float64 | Projected conference losses. |
| `con_rec` | String | Conference win-loss record to date. |
| `sos` | Float64 | Strength of schedule faced to date. |
| `ncsos` | Float64 | Non-conference strength of schedule. |
| `consos` | Float64 | Conference strength of schedule. |
| `proj_sos` | Float64 | Projected full-season strength of schedule. |
| `proj_noncon_sos` | Float64 | Projected non-conference strength of schedule. |
| `proj_con_sos` | Float64 | Projected conference strength of schedule. |
| `elite_sos` | Float64 | Elite strength of schedule (share of schedule vs elite opponents). |
| `elite_noncon_sos` | Float64 | Elite non-conference strength of schedule. |
| `opp_oe` | Float64 | Average opponent offensive efficiency. |
| `opp_de` | Float64 | Average opponent defensive efficiency. |
| `opp_proj_oe` | Float64 | Projected average opponent offensive efficiency. |
| `opp_proj_de` | Float64 | Projected average opponent defensive efficiency. |
| `con_adj_oe` | Float64 | Adjusted offensive efficiency in conference games only. |
| `con_adj_de` | Float64 | Adjusted defensive efficiency in conference games only. |
| `qual_o` | Float64 | Offensive efficiency vs quality (top-tier) opponents. |
| `qual_d` | Float64 | Defensive efficiency vs quality (top-tier) opponents. |
| `qual_barthag` | Float64 | Barthag computed from the quality-opponent efficiency splits. |
| `qual_games` | Float64 | Number of quality games underlying the qual_* splits. |
| `fun` | Float64 | Torvik's FUN style/entertainment index. |
| `con_pf` | Float64 | Points scored in conference play. |
| `con_pa` | Float64 | Points allowed in conference play. |
| `con_poss` | Float64 | Possessions played in conference play. |
| `con_oe` | Float64 | Raw offensive efficiency in conference play. |
| `con_de` | Float64 | Raw defensive efficiency in conference play. |
| `con_sos_remain` | Float64 | Strength of the remaining conference schedule. |
| `conf_win_percent` | Float64 | Conference win percentage. |
| `wab` | Float64 | Wins above bubble (resume quality vs a bubble-level team). |
| `wab_rk` | Int64 | National rank of wins above bubble. |
| `fun_rk` | Int64 | National rank of the FUN index. |
| `adjt` | Float64 | Adjusted tempo (possessions per 40 minutes). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#bart_wbb_ratings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. Barttorvik.com returned 404 (no data file for that season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#bart_wbb_ratings-example}

```python
bart_wbb_ratings(year=2025)
```
