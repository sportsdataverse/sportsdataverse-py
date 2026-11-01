# NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Team

> NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Team — function reference in sdv-py, the SportsDataverse Python package.

## nfl_pro_team_offense_overview_season

GET /api/secured/stats/team-offense/overview/season — one row per team for the season — team offensive overview incl. EPA per play, pass and rush splits.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-offense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-offense/overview/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-offense/overview/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_offense_overview_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | Int64 | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | Int64 | Offensive plays, season total (pass + run). |
| `pass` | Int64 | Pass plays, season total. |
| `run` | Int64 | Run plays, season total. |
| `pass_pct` | Float64 | Share of plays that were passes (pass / total). |
| `ppg` | Float64 | Points per game. |
| `yds` | Int64 | Yards from scrimmage, season total (pass_yds + rush_yds). |
| `ypg` | Float64 | Yards per game (yds / gp). |
| `ypp` | Float64 | Yards per play (yds / total). |
| `td` | Int64 | Touchdowns, season total. |
| `epa` | Float64 | Total expected points added over all plays (epa_pass + epa_rush). |
| `epa_pp` | Float64 | Expected points added per play (epa / total). |
| `pass_yds` | Int64 | Net passing yards, season total. |
| `pass_ypg` | Float64 | Passing yards per game (pass_yds / gp). |
| `pass_ypp` | Float64 | Passing yards per pass play (pass_yds / pass). |
| `sacked_yds` | Int64 | Yards lost to sacks, season total. |
| `sacked_ypg` | Float64 | Sack yards lost per game (sacked_yds / gp). |
| `epa_pass` | Float64 | Total expected points added on pass plays. |
| `epa_pass_pp` | Float64 | Expected points added per pass play (epa_pass / pass). |
| `rush_yds` | Int64 | Rushing yards, season total. |
| `rush_ypg` | Float64 | Rushing yards per game (rush_yds / gp). |
| `rush_ypp` | Float64 | Rushing yards per run play (rush_yds / run). |
| `epa_rush` | Float64 | Total expected points added on run plays. |
| `epa_rush_pp` | Float64 | Expected points added per run play (epa_rush / run). |
| `ryoe` | Float64 | Rushing yards over expected, season total, per Next Gen Stats. |
| `ttt` | Float64 | Average time to throw in seconds on pass plays, per Next Gen Stats. |
| `qbp` | Int64 | Quarterback pressures on pass plays, season total. |
| `qbp_pct` | Float64 | Pressure rate: share of pass plays with a quarterback pressure. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_team_offense_overview_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_team_offense_overview_season-example}

```python
nfl_pro_team_offense_overview_season(season=2024, season_type='REG')
```

## nfl_pro_team_offense_overview_week

GET /api/secured/stats/team-offense/overview/week — one row per team per week — team offensive overview incl. EPA per play, pass and rush splits.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-offense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-offense/overview/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-offense/overview/week?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_offense_overview_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | Int64 | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | Int64 | Offensive plays in the game (pass + run). |
| `pass` | Int64 | Pass plays in the game. |
| `run` | Int64 | Run plays in the game. |
| `pass_pct` | Float64 | Share of plays that were passes (pass / total). |
| `ppg` | Int64 | Points per game. |
| `yds` | Int64 | Yards from scrimmage in the game (pass_yds + rush_yds). |
| `ypg` | Int64 | Yards per game (yds / gp). |
| `ypp` | Float64 | Yards per play (yds / total). |
| `td` | Int64 | Touchdowns in the game. |
| `epa` | Float64 | Total expected points added over all plays (epa_pass + epa_rush). |
| `epa_pp` | Float64 | Expected points added per play (epa / total). |
| `pass_yds` | Int64 | Net passing yards in the game. |
| `pass_ypg` | Int64 | Passing yards per game (pass_yds / gp). |
| `pass_ypp` | Float64 | Passing yards per pass play (pass_yds / pass). |
| `sacked_yds` | Int64 | Yards lost to sacks in the game. |
| `sacked_ypg` | Int64 | Sack yards lost per game (sacked_yds / gp). |
| `epa_pass` | Float64 | Total expected points added on pass plays. |
| `epa_pass_pp` | Float64 | Expected points added per pass play (epa_pass / pass). |
| `rush_yds` | Int64 | Rushing yards in the game. |
| `rush_ypg` | Int64 | Rushing yards per game (rush_yds / gp). |
| `rush_ypp` | Float64 | Rushing yards per run play (rush_yds / run). |
| `epa_rush` | Float64 | Total expected points added on run plays. |
| `epa_rush_pp` | Float64 | Expected points added per run play (epa_rush / run). |
| `ryoe` | Float64 | Rushing yards over expected in the game, per Next Gen Stats. |
| `ttt` | Float64 | Average time to throw in seconds on pass plays, per Next Gen Stats. |
| `qbp` | Int64 | Quarterback pressures on pass plays in the game. |
| `qbp_pct` | Float64 | Pressure rate: share of pass plays with a quarterback pressure. |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_team_offense_overview_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_team_offense_overview_week-example}

```python
nfl_pro_team_offense_overview_week(season=2024, season_type='REG')
```

## nfl_pro_team_defense_overview_season

GET /api/secured/stats/team-defense/overview/season — one row per team for the season — team defensive overview incl. EPA allowed per play and takeaways.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-defense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-defense/overview/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-defense/overview/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_defense_overview_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | Int64 | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | Int64 | Defensive plays faced, season total (pass + run). |
| `pass` | Int64 | Pass plays, season total, from the defense's side (allowed / faced). |
| `run` | Int64 | Run plays, season total, from the defense's side (allowed / faced). |
| `pass_pct` | Float64 | Share of plays that were passes (pass / total), from the defense's side (allowed / faced). |
| `ppg` | Float64 | Points per game, from the defense's side (allowed / faced). |
| `yds` | Int64 | Yards from scrimmage, season total (pass_yds + rush_yds), from the defense's side (allowed / faced). |
| `ypg` | Float64 | Yards per game (yds / gp), from the defense's side (allowed / faced). |
| `ypp` | Float64 | Yards per play (yds / total), from the defense's side (allowed / faced). |
| `td` | Int64 | Touchdowns, season total, from the defense's side (allowed / faced). |
| `pass_td` | Int64 | Passing touchdowns allowed, season total. |
| `rush_td` | Int64 | Rushing touchdowns allowed, season total. |
| `epa` | Float64 | Total expected points added over all plays (epa_pass + epa_rush), from the defense's side (allowed / faced). |
| `epa_pp` | Float64 | Expected points added per play (epa / total), from the defense's side (allowed / faced). |
| `pass_yds` | Int64 | Net passing yards, season total, from the defense's side (allowed / faced). |
| `pass_ypg` | Float64 | Passing yards per game (pass_yds / gp), from the defense's side (allowed / faced). |
| `pass_ypp` | Float64 | Passing yards per pass play (pass_yds / pass), from the defense's side (allowed / faced). |
| `sacked_yds` | Int64 | Yards lost to sacks, season total, from the defense's side (allowed / faced). |
| `sacked_ypg` | Float64 | Sack yards lost per game (sacked_yds / gp), from the defense's side (allowed / faced). |
| `epa_pass` | Float64 | Total expected points added on pass plays, from the defense's side (allowed / faced). |
| `epa_pass_pp` | Float64 | Expected points added per pass play (epa_pass / pass), from the defense's side (allowed / faced). |
| `rush_yds` | Int64 | Rushing yards, season total, from the defense's side (allowed / faced). |
| `rush_ypg` | Float64 | Rushing yards per game (rush_yds / gp), from the defense's side (allowed / faced). |
| `rush_ypp` | Float64 | Rushing yards per run play (rush_yds / run), from the defense's side (allowed / faced). |
| `epa_rush` | Float64 | Total expected points added on run plays, from the defense's side (allowed / faced). |
| `epa_rush_pp` | Float64 | Expected points added per run play (epa_rush / run), from the defense's side (allowed / faced). |
| `ryoe` | Float64 | Rushing yards over expected, season total, per Next Gen Stats, from the defense's side (allowed / faced). |
| `ttt` | Float64 | Average time to throw in seconds on pass plays, per Next Gen Stats, from the defense's side (allowed / faced). |
| `qbp` | Int64 | Quarterback pressures on pass plays, season total, from the defense's side (allowed / faced). |
| `qbp_pct` | Float64 | Pressure rate: share of pass plays with a quarterback pressure, from the defense's side (allowed / faced). |
| `interception` | Int64 | Interceptions made, season total. |
| `forced_fumble` | Int64 | Fumbles forced, season total. |
| `fumble_recovered` | Int64 | Opponent fumbles recovered, season total. |
| `defensive_touchdown` | Int64 | Defensive touchdowns scored, season total. |
| `total_takeaways` | Int64 | Takeaways, season total (interception + fumble_recovered). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_team_defense_overview_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_team_defense_overview_season-example}

```python
nfl_pro_team_defense_overview_season(season=2024, season_type='REG')
```

## nfl_pro_team_defense_overview_week

GET /api/secured/stats/team-defense/overview/week — one row per team per week — team defensive overview incl. EPA allowed per play and takeaways.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-defense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-defense/overview/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-defense/overview/week?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_defense_overview_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | Int64 | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | Int64 | Defensive plays faced in the game (pass + run). |
| `pass` | Int64 | Pass plays in the game, from the defense's side (allowed / faced). |
| `run` | Int64 | Run plays in the game, from the defense's side (allowed / faced). |
| `pass_pct` | Float64 | Share of plays that were passes (pass / total), from the defense's side (allowed / faced). |
| `ppg` | Int64 | Points per game, from the defense's side (allowed / faced). |
| `yds` | Int64 | Yards from scrimmage in the game (pass_yds + rush_yds), from the defense's side (allowed / faced). |
| `ypg` | Int64 | Yards per game (yds / gp), from the defense's side (allowed / faced). |
| `ypp` | Float64 | Yards per play (yds / total), from the defense's side (allowed / faced). |
| `td` | Int64 | Touchdowns in the game, from the defense's side (allowed / faced). |
| `pass_td` | Int64 | Passing touchdowns allowed in the game. |
| `rush_td` | Int64 | Rushing touchdowns allowed in the game. |
| `epa` | Float64 | Total expected points added over all plays (epa_pass + epa_rush), from the defense's side (allowed / faced). |
| `epa_pp` | Float64 | Expected points added per play (epa / total), from the defense's side (allowed / faced). |
| `pass_yds` | Int64 | Net passing yards in the game, from the defense's side (allowed / faced). |
| `pass_ypg` | Int64 | Passing yards per game (pass_yds / gp), from the defense's side (allowed / faced). |
| `pass_ypp` | Float64 | Passing yards per pass play (pass_yds / pass), from the defense's side (allowed / faced). |
| `sacked_yds` | Int64 | Yards lost to sacks in the game, from the defense's side (allowed / faced). |
| `sacked_ypg` | Int64 | Sack yards lost per game (sacked_yds / gp), from the defense's side (allowed / faced). |
| `epa_pass` | Float64 | Total expected points added on pass plays, from the defense's side (allowed / faced). |
| `epa_pass_pp` | Float64 | Expected points added per pass play (epa_pass / pass), from the defense's side (allowed / faced). |
| `rush_yds` | Int64 | Rushing yards in the game, from the defense's side (allowed / faced). |
| `rush_ypg` | Int64 | Rushing yards per game (rush_yds / gp), from the defense's side (allowed / faced). |
| `rush_ypp` | Float64 | Rushing yards per run play (rush_yds / run), from the defense's side (allowed / faced). |
| `epa_rush` | Float64 | Total expected points added on run plays, from the defense's side (allowed / faced). |
| `epa_rush_pp` | Float64 | Expected points added per run play (epa_rush / run), from the defense's side (allowed / faced). |
| `ryoe` | Float64 | Rushing yards over expected in the game, per Next Gen Stats, from the defense's side (allowed / faced). |
| `ttt` | Float64 | Average time to throw in seconds on pass plays, per Next Gen Stats, from the defense's side (allowed / faced). |
| `qbp` | Int64 | Quarterback pressures on pass plays in the game, from the defense's side (allowed / faced). |
| `qbp_pct` | Float64 | Pressure rate: share of pass plays with a quarterback pressure, from the defense's side (allowed / faced). |
| `interception` | Int64 | Interceptions made in the game. |
| `forced_fumble` | Int64 | Fumbles forced in the game. |
| `fumble_recovered` | Int64 | Opponent fumbles recovered in the game. |
| `defensive_touchdown` | Int64 | Defensive touchdowns scored in the game. |
| `total_takeaways` | Int64 | Takeaways in the game (interception + fumble_recovered). |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_team_defense_overview_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_team_defense_overview_week-example}

```python
nfl_pro_team_defense_overview_week(season=2024, season_type='REG')
```
