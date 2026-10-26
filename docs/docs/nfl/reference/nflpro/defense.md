---
title: "NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Defense"
sidebar_label: "Defense"
sidebar_position: 1
description: "NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Defense — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Defense

## nfl_pro_defense_overview_season

GET /api/secured/stats/defense/overview/season — one row per defender for the season — defensive overview incl. snap counts, pressures and havoc stops.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/overview/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/overview/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_defense_overview_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `snap` | Int64 | Defensive snaps played, season total. |
| `snap_pct` | Float64 | Share of the team's defensive snaps the player played (snap / team_snap). |
| `rd` | Int64 |  |
| `pr` | Int64 | Pass-rush snaps, season total; the denominator of qbp_r. |
| `tck` | Int64 | Tackles, season total. |
| `t_stop` | Int64 |  |
| `h_stop` | Int64 |  |
| `qbp` | Int64 | Quarterback pressures, season total. |
| `qbp_r` | Float64 | Pressure rate: pressures per pass-rush snap (qbp / pr). |
| `sack` | Float64 | Sacks, season total (a count, not a per-play flag). |
| `tgt_nd` | Int64 | Targets on which the player was the nearest defender, season total, per Next Gen Stats. |
| `rec_nd` | Int64 | Receptions allowed as the nearest defender, season total. |
| `rec_yds_nd` | Int64 | Receiving yards allowed as the nearest defender, season total. |
| `rec_td_nd` | Int64 | Receiving touchdowns allowed as the nearest defender, season total. |
| `int` | Int64 | Interceptions made, season total (a count, not a per-play flag). |
| `pass_rating_nd` | Float64 | Passer rating allowed on targets where the player was the nearest defender. |
| `qd` | Boolean | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | Int64 | Defensive snaps the player played, season total; equals snap on the overview table. |
| `team_snap` | Int64 | Defensive snaps the player's team played, season total; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_defense_overview_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_defense_overview_season-example}

```python
nfl_pro_defense_overview_season(season=2024, season_type='REG')
```

## nfl_pro_defense_overview_week

GET /api/secured/stats/defense/overview/week — one row per defender per week — defensive overview incl. snap counts, pressures and havoc stops.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/overview/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/overview/week?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns {#nfl_pro_defense_overview_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `snap` | Int64 | Defensive snaps played in the game. |
| `snap_pct` | Float64 | Share of the team's defensive snaps the player played (snap / team_snap). |
| `rd` | Int64 |  |
| `pr` | Int64 | Pass-rush snaps in the game; the denominator of qbp_r. |
| `tck` | Int64 | Tackles in the game. |
| `t_stop` | Int64 |  |
| `h_stop` | Int64 |  |
| `qbp` | Int64 | Quarterback pressures in the game. |
| `qbp_r` | Float64 | Pressure rate: pressures per pass-rush snap (qbp / pr). |
| `sack` | Int64 | Sacks in the game (a count, not a per-play flag). |
| `tgt_nd` | Int64 | Targets on which the player was the nearest defender in the game, per Next Gen Stats. |
| `rec_nd` | Int64 | Receptions allowed as the nearest defender in the game. |
| `rec_yds_nd` | Int64 | Receiving yards allowed as the nearest defender in the game. |
| `rec_td_nd` | Int64 | Receiving touchdowns allowed as the nearest defender in the game. |
| `int` | Int64 | Interceptions made in the game (a count, not a per-play flag). |
| `pass_rating_nd` | Float64 | Passer rating allowed on targets where the player was the nearest defender. |
| `qd` | Boolean | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | Int64 | Defensive snaps the player played in the game; equals snap on the overview table. |
| `team_snap` | Int64 | Defensive snaps the player's team played in the game; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_defense_overview_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_defense_overview_week-example}

```python
nfl_pro_defense_overview_week(season=2024, season_type='REG')
```

## nfl_pro_defense_nearest_season

GET /api/secured/stats/defense/nearest/season — one row per defender for the season — nearest-defender coverage incl. targets, catch rate and CROE allowed.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/nearest/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/nearest/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/nearest/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_defense_nearest_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `cov` | Int64 |  |
| `cov_nd` | Int64 |  |
| `tgt_nd` | Int64 | Targets on which the player was the nearest defender, season total, per Next Gen Stats. |
| `rec_nd` | Int64 | Receptions allowed as the nearest defender, season total. |
| `rec_yds_nd` | Int64 | Receiving yards allowed as the nearest defender, season total. |
| `rec_td_nd` | Int64 | Receiving touchdowns allowed as the nearest defender, season total. |
| `int` | Int64 | Interceptions made, season total (a count, not a per-play flag). |
| `pass_rating_nd` | Float64 | Passer rating allowed on targets where the player was the nearest defender. |
| `catch_nd` | Float64 | Catch rate allowed as the nearest defender (rec_nd / tgt_nd), as a fraction. |
| `croe_nd` | Float64 | Catch rate over expected allowed as the nearest defender (actual minus expected catch rate), per Next Gen Stats. |
| `tgt_epa_nd` | Float64 | Total expected points added allowed on targets where the player was the nearest defender. |
| `tgt_r_nd` | Float64 | Target rate as the nearest defender: share of coverage snaps on which the receiver he covered was targeted. |
| `sep` | Float64 | Average separation in yards allowed at pass arrival as the nearest defender, per Next Gen Stats. |
| `twf_pct` | Float64 |  |
| `bh_pct` | Float64 |  |
| `yacpr_nd` | Float64 |  |
| `qd` | Boolean | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | Int64 | Defensive snaps the player played, season total; equals snap on the overview table. |
| `team_snap` | Int64 | Defensive snaps the player's team played, season total; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_defense_nearest_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_defense_nearest_season-example}

```python
nfl_pro_defense_nearest_season(season=2024, season_type='REG')
```

## nfl_pro_defense_nearest_week

GET /api/secured/stats/defense/nearest/week — one row per defender per week — nearest-defender coverage incl. targets, catch rate and CROE allowed.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/nearest/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/nearest/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/nearest/week?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns {#nfl_pro_defense_nearest_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `cov` | Int64 |  |
| `cov_nd` | Int64 |  |
| `tgt_nd` | Int64 | Targets on which the player was the nearest defender in the game, per Next Gen Stats. |
| `rec_nd` | Int64 | Receptions allowed as the nearest defender in the game. |
| `rec_yds_nd` | Int64 | Receiving yards allowed as the nearest defender in the game. |
| `rec_td_nd` | Int64 | Receiving touchdowns allowed as the nearest defender in the game. |
| `int` | Int64 | Interceptions made in the game (a count, not a per-play flag). |
| `pass_rating_nd` | Float64 | Passer rating allowed on targets where the player was the nearest defender. |
| `catch_nd` | Float64 | Catch rate allowed as the nearest defender (rec_nd / tgt_nd), as a fraction. |
| `croe_nd` | Float64 | Catch rate over expected allowed as the nearest defender (actual minus expected catch rate), per Next Gen Stats. |
| `tgt_epa_nd` | Float64 | Total expected points added allowed on targets where the player was the nearest defender. |
| `tgt_r_nd` | Float64 | Target rate as the nearest defender: share of coverage snaps on which the receiver he covered was targeted. |
| `sep` | Float64 | Average separation in yards allowed at pass arrival as the nearest defender, per Next Gen Stats. |
| `twf_pct` | Int64 |  |
| `bh_pct` | Float64 |  |
| `yacpr_nd` | Float64 |  |
| `qd` | Boolean | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | Int64 | Defensive snaps the player played in the game; equals snap on the overview table. |
| `team_snap` | Int64 | Defensive snaps the player's team played in the game; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_defense_nearest_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_defense_nearest_week-example}

```python
nfl_pro_defense_nearest_week(season=2024, season_type='REG')
```
