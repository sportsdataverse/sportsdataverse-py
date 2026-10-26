---
title: "Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Team

## liiga_teams_history

Every team's regular-season record, one row per team and season since 1976.

**Endpoint URL:** `GET https://liiga.fi/api/v2/teams/info`

**Valid URL:** [https://liiga.fi/api/v2/teams/info?team=all](https://liiga.fi/api/v2/teams/info?team=all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#liiga_teams_history-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Row id of this team-season row (NOT a team id; `teamId` is the team). |
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `league_id` | Int64 | League id: 1 = Liiga (SM-liiga) on every /teams/info row 1976-2027. |
| `serie_id` | Int64 | Tournament id, the same every season: 1188 regular season, 1189 playoffs, 1194 preseason, 1890 CHL. |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `away_games` | Int64 | Away games (team, that regular season). |
| `away_losses` | Int64 | Away losses (team, that regular season). |
| `away_ties` | Int64 | Away ties (team, that regular season). |
| `away_wins` | Int64 | Away wins (team, that regular season). |
| `away_wins_in_extra_time` | Int64 | Away wins in extra time (team, that regular season). |
| `games` | Int64 | Games (team, that regular season). |
| `goals_against` | Int64 | Goals against (team, that regular season). |
| `goals_scored` | Int64 | Goals scored (team, that regular season). |
| `home_games` | Int64 | Home games (team, that regular season). |
| `home_losses` | Int64 | Home losses (team, that regular season). |
| `home_ties` | Int64 | Home ties (team, that regular season). |
| `home_wins` | Int64 | Home wins (team, that regular season). |
| `home_wins_in_extra_time` | Int64 | Home wins in extra time (team, that regular season). |
| `losses` | Int64 | Losses (team, that regular season). |
| `playoff_rank` | Int64 | Playoff rank (team, that regular season). |
| `points` | Int64 | Points (team, that regular season). |
| `regular_season_rank` | Int64 | Regular season rank (team, that regular season). |
| `ties` | Int64 | Ties (team, that regular season). |
| `wins` | Int64 | Wins (team, that regular season). |
| `wins_in_extra_time` | Int64 | Wins in extra time (team, that regular season). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_teams_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_teams_history-example}

```python
liiga_teams_history()
```

## liiga_teams

Team directory: every current and past team with logos, venue capacity and club info.

**Endpoint URL:** `GET https://liiga.fi/api/v2/teams/info`

**Valid URL:** [https://liiga.fi/api/v2/teams/info](https://liiga.fi/api/v2/teams/info)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#liiga_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `name` | String | Club name as Liiga lists it, e.g. HIFK or K-Espoo. |
| `contact_info` | String | Club contact details as HTML, in Finnish (company, address, phone). |
| `externals` | String | JSON list of the club's external links (url, label), e.g. its ticket shops. |
| `country_code` | String | Club's country, 3-letter code (e.g. FIN). |
| `country_name` | String | Club's country name, e.g. Finland. |
| `current_venue_capacity` | Int64 | Capacity of the team's current home arena (spectators). |
| `general_info` | String | Club history and facts as an HTML table, in Finnish (founding year, titles, ...). |
| `url` | String | Club website URL. |
| `locality` | String | Club's home locality; null in every capture (unverified). |
| `logo` | String | Team logo image URL (old.liiga.fi media). |
| `short_name` | String | Team abbreviation. |
| `teamtournamentstats` | String | Team totals per season and tournament, snake_case, percentages and averages sent as STRINGS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_teams-example}

```python
liiga_teams()
```

## liiga_team_stats

Team stat table of a season range, by data_type.

**Endpoint URL:** `GET https://liiga.fi/api/v2/teams/stats`

**Valid URL:** [https://liiga.fi/api/v2/teams/stats?dataType=shots&seasonFrom=2026&seasonTo=2026&tournament=runkosarja](https://liiga.fi/api/v2/teams/stats?dataType=shots&seasonFrom=2026&seasonTo=2026&tournament=runkosarja)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dataType` | `data_type` |  | `Y` |  | Stat table: shots, passes, faceoffs, penalties, powerplay, penalty_kill, even_strength. |
| `seasonFrom` | `season_from` |  |  | `Y` | First season (END year) of the range. |
| `seasonTo` | `season_to` |  |  | `Y` | Last season (END year) of the range. |
| `tournament` | `tournament` |  |  | `Y` | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). Required by the API; defaults to runkosarja. |

### Returns {#liiga_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sort_desc` | Boolean | Default sort is descending. |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_name` | String | Team's display name, e.g. HPK or Tappara. |
| `team_short_name` | String | Team abbreviation. |
| `team_logos_dark_bg` | String | Logo image URL for dark backgrounds. |
| `team_logos_light_bg` | String | Logo image URL for light backgrounds. |
| `team_logos_dark_bg_original` | String | Original-file logo URL for dark backgrounds; null in every capture. |
| `team_logos_light_bg_original` | String | Original-file logo URL for light backgrounds; null in every capture. |
| `games` | Int64 | Games (team, over the season range). |
| `ranking` | Int64 | Table position. |
| `previous_game_day_ranking` | Int64 | Table position after the previous game day. |
| `shots` | Int64 | Shots (team, over the season range). |
| `shots_saved` | Int64 | Shots saved (team, over the season range). |
| `shots_even_strength` | Int64 | Shots at even strength (team, over the season range). |
| `shots_powerplay` | Int64 | Shots on the power play (team, over the season range). |
| `shots_penalty_kill` | Int64 | Shots on the penalty kill (team, over the season range). |
| `shots_against` | Int64 | Shots against (team, over the season range). |
| `goals` | Int64 | Goals (team, over the season range). |
| `goals_against` | Int64 | Goals against (team, over the season range). |
| `shots_against_even_strength` | Int64 | Shots against at even strength (team, over the season range). |
| `shots_against_powerplay` | Int64 | Shots against on the power play (team, over the season range). |
| `shots_against_penalty_kill` | Int64 | Shots against on the penalty kill (team, over the season range). |
| `non_blocked_shots` | Int64 | Non blocked shots (team, over the season range). |
| `non_blocked_shots_even_strength` | Int64 | Non blocked shots at even strength (team, over the season range). |
| `non_blocked_shots_powerplay` | Int64 | Non blocked shots on the power play (team, over the season range). |
| `non_blocked_shots_penalty_kill` | Int64 | Non blocked shots on the penalty kill (team, over the season range). |
| `non_blocked_shots_against` | Int64 | Non blocked shots against (team, over the season range). |
| `non_blocked_shots_against_even_strength` | Int64 | Non blocked shots against at even strength (team, over the season range). |
| `non_blocked_shots_against_powerplay` | Int64 | Non blocked shots against on the power play (team, over the season range). |
| `non_blocked_shots_against_penalty_kill` | Int64 | Non blocked shots against on the penalty kill (team, over the season range). |
| `powerplay_goals` | Int64 | Powerplay goals (team, over the season range). |
| `powerplay_goals_against` | Int64 | Powerplay goals against (team, over the season range). |
| `penalty_kill_goals` | Int64 | Penalty kill goals (team, over the season range). |
| `penalty_kill_goals_against` | Int64 | Penalty kill goals against (team, over the season range). |
| `even_strength_goals` | Int64 | Even strength goals (team, over the season range). |
| `even_strength_goals_against` | Int64 | Even strength goals against (team, over the season range). |
| `previous_game_day_goals` | Int64 | Goals as of the previous game day (team, over the season range). |
| `previous_game_day_shots` | Int64 | Shots as of the previous game day (team, over the season range). |
| `previous_game_day_shots_percentage` | Float64 | Shots percentage as of the previous game day (team, over the season range). Percent, 0-100. |
| `shots_percentage_even_strength` | Float64 | Shots percentage at even strength (team, over the season range). Percent, 0-100. |
| `shots_percentage_powerplay` | Float64 | Shots percentage on the power play (team, over the season range). Percent, 0-100. |
| `shots_percentage_penalty_kill` | Float64 | Shots percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `shots_difference_even_strength` | Int64 | Shots difference at even strength (team, over the season range). |
| `shots_difference_powerplay` | Int64 | Shots difference on the power play (team, over the season range). |
| `shots_difference_penalty_kill` | Int64 | Shots difference on the penalty kill (team, over the season range). |
| `shots_for_shots_against_percentage` | Float64 | Shots for shots against percentage (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_difference` | Int64 | Non blocked shots difference (team, over the season range). |
| `save_percentage_even_strength` | Float64 | Save percentage at even strength (team, over the season range). Percent, 0-100. |
| `save_percentage_powerplay` | Float64 | Save percentage on the power play (team, over the season range). Percent, 0-100. |
| `save_percentage_penalty_kill` | Float64 | Save percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `shots_difference` | Int64 | Shots difference (team, over the season range). |
| `pdo_penalty_kill` | Float64 | PDO (on-ice shooting % + save %) on the penalty kill (team, over the season range). Percent, 0-100. |
| `pdo_even_strength` | Float64 | PDO (on-ice shooting % + save %) at even strength (team, over the season range). Percent, 0-100. |
| `pdo_powerplay` | Float64 | PDO (on-ice shooting % + save %) on the power play (team, over the season range). Percent, 0-100. |
| `shots_percentage` | Float64 | Shots percentage (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_for_shots_against_percentage_even_strength` | Float64 | Non blocked shots for shots against percentage at even strength (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_for_shots_against_percentage_powerplay` | Float64 | Non blocked shots for shots against percentage on the power play (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_for_shots_against_percentage_penalty_kill` | Float64 | Non blocked shots for shots against percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `shots_for_shots_against_percentage_even_strength` | Float64 | Shots for shots against percentage at even strength (team, over the season range). Percent, 0-100. |
| `shots_for_shots_against_percentage_powerplay` | Float64 | Shots for shots against percentage on the power play (team, over the season range). Percent, 0-100. |
| `shots_for_shots_against_percentage_penalty_kill` | Float64 | Shots for shots against percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_difference_powerplay` | Int64 | Non blocked shots difference on the power play (team, over the season range). |
| `non_blocked_shots_difference_penalty_kill` | Int64 | Non blocked shots difference on the penalty kill (team, over the season range). |
| `non_blocked_shots_for_shots_against_percentage` | Float64 | Non blocked shots for shots against percentage (team, over the season range). Percent, 0-100. |
| `pdo` | Float64 | PDO (on-ice shooting % + save %) (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_difference_even_strength` | Int64 | Non blocked shots difference at even strength (team, over the season range). |
| `save_percentage` | Float64 | Save percentage (team, over the season range). Percent, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_team_stats-example}

```python
liiga_team_stats(data_type='shots', season_from=2026, season_to=2026)
```
