# LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Team

> LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Team — function reference in sdv-py, the SportsDataverse Python package.

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
| `id` | integer | Row id of this team-season row (NOT a team id; `teamId` is the team). |
| `season` | integer | Season END year (2026 = 2025-26). |
| `league_id` | integer | League id: 1 = Liiga (SM-liiga) on every /teams/info row 1976-2027. |
| `serie_id` | integer | Tournament id, the same every season: 1188 regular season, 1189 playoffs, 1194 preseason, 1890 CHL. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `away_games` | integer | Away games (team, that regular season). |
| `away_losses` | integer | Away losses (team, that regular season). |
| `away_ties` | integer | Away ties (team, that regular season). |
| `away_wins` | integer | Away wins (team, that regular season). |
| `away_wins_in_extra_time` | integer | Away wins in extra time (team, that regular season). |
| `games` | integer | Games (team, that regular season). |
| `goals_against` | integer | Goals against (team, that regular season). |
| `goals_scored` | integer | Goals scored (team, that regular season). |
| `home_games` | integer | Home games (team, that regular season). |
| `home_losses` | integer | Home losses (team, that regular season). |
| `home_ties` | integer | Home ties (team, that regular season). |
| `home_wins` | integer | Home wins (team, that regular season). |
| `home_wins_in_extra_time` | integer | Home wins in extra time (team, that regular season). |
| `losses` | integer | Losses (team, that regular season). |
| `playoff_rank` | integer | Playoff rank (team, that regular season). |
| `points` | integer | Points (team, that regular season). |
| `regular_season_rank` | integer | Regular season rank (team, that regular season). |
| `ties` | integer | Ties (team, that regular season). |
| `wins` | integer | Wins (team, that regular season). |
| `wins_in_extra_time` | integer | Wins in extra time (team, that regular season). |

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

_Last validated n/a._

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
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `name` | character | Club name as Liiga lists it, e.g. HIFK or K-Espoo. |
| `contact_info` | character | Club contact details as HTML, in Finnish (company, address, phone). |
| `externals` | character | JSON list of the club's external links (url, label), e.g. its ticket shops. |
| `country_code` | character | Club's country, 3-letter code (e.g. FIN). |
| `country_name` | character | Club's country name, e.g. Finland. |
| `current_venue_capacity` | integer | Capacity of the team's current home arena (spectators). |
| `general_info` | character | Club history and facts as an HTML table, in Finnish (founding year, titles, ...). |
| `url` | character | Club website URL. |
| `locality` | character | Club's home locality; null in every capture (unverified). |
| `logo` | character | Team logo image URL (old.liiga.fi media). |
| `short_name` | character | Team abbreviation. |
| `teamtournamentstats` | character | Team totals per season and tournament, snake_case, percentages and averages sent as STRINGS. |

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

_Last validated n/a._

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
| `sort_desc` | logical | Default sort is descending. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `team_short_name` | character | Team abbreviation. |
| `team_logos_dark_bg` | character | Logo image URL for dark backgrounds. |
| `team_logos_light_bg` | character | Logo image URL for light backgrounds. |
| `team_logos_dark_bg_original` | character | Original-file logo URL for dark backgrounds; null in every capture. |
| `team_logos_light_bg_original` | character | Original-file logo URL for light backgrounds; null in every capture. |
| `games` | integer | Games (team, over the season range). |
| `ranking` | integer | Table position. |
| `previous_game_day_ranking` | integer | Table position after the previous game day. |
| `shots` | integer | Shots (team, over the season range). |
| `shots_saved` | integer | Shots saved (team, over the season range). |
| `shots_even_strength` | integer | Shots at even strength (team, over the season range). |
| `shots_powerplay` | integer | Shots on the power play (team, over the season range). |
| `shots_penalty_kill` | integer | Shots on the penalty kill (team, over the season range). |
| `shots_against` | integer | Shots against (team, over the season range). |
| `goals` | integer | Goals (team, over the season range). |
| `goals_against` | integer | Goals against (team, over the season range). |
| `shots_against_even_strength` | integer | Shots against at even strength (team, over the season range). |
| `shots_against_powerplay` | integer | Shots against on the power play (team, over the season range). |
| `shots_against_penalty_kill` | integer | Shots against on the penalty kill (team, over the season range). |
| `non_blocked_shots` | integer | Non blocked shots (team, over the season range). |
| `non_blocked_shots_even_strength` | integer | Non blocked shots at even strength (team, over the season range). |
| `non_blocked_shots_powerplay` | integer | Non blocked shots on the power play (team, over the season range). |
| `non_blocked_shots_penalty_kill` | integer | Non blocked shots on the penalty kill (team, over the season range). |
| `non_blocked_shots_against` | integer | Non blocked shots against (team, over the season range). |
| `non_blocked_shots_against_even_strength` | integer | Non blocked shots against at even strength (team, over the season range). |
| `non_blocked_shots_against_powerplay` | integer | Non blocked shots against on the power play (team, over the season range). |
| `non_blocked_shots_against_penalty_kill` | integer | Non blocked shots against on the penalty kill (team, over the season range). |
| `powerplay_goals` | integer | Powerplay goals (team, over the season range). |
| `powerplay_goals_against` | integer | Powerplay goals against (team, over the season range). |
| `penalty_kill_goals` | integer | Penalty kill goals (team, over the season range). |
| `penalty_kill_goals_against` | integer | Penalty kill goals against (team, over the season range). |
| `even_strength_goals` | integer | Even strength goals (team, over the season range). |
| `even_strength_goals_against` | integer | Even strength goals against (team, over the season range). |
| `previous_game_day_goals` | integer | Goals as of the previous game day (team, over the season range). |
| `previous_game_day_shots` | integer | Shots as of the previous game day (team, over the season range). |
| `previous_game_day_shots_percentage` | numeric | Shots percentage as of the previous game day (team, over the season range). Percent, 0-100. |
| `shots_percentage_even_strength` | numeric | Shots percentage at even strength (team, over the season range). Percent, 0-100. |
| `shots_percentage_powerplay` | numeric | Shots percentage on the power play (team, over the season range). Percent, 0-100. |
| `shots_percentage_penalty_kill` | numeric | Shots percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `shots_difference_even_strength` | integer | Shots difference at even strength (team, over the season range). |
| `shots_difference_powerplay` | integer | Shots difference on the power play (team, over the season range). |
| `shots_difference_penalty_kill` | integer | Shots difference on the penalty kill (team, over the season range). |
| `shots_for_shots_against_percentage` | numeric | Shots for shots against percentage (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_difference` | integer | Non blocked shots difference (team, over the season range). |
| `save_percentage_even_strength` | numeric | Save percentage at even strength (team, over the season range). Percent, 0-100. |
| `save_percentage_powerplay` | numeric | Save percentage on the power play (team, over the season range). Percent, 0-100. |
| `save_percentage_penalty_kill` | numeric | Save percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `shots_difference` | integer | Shots difference (team, over the season range). |
| `pdo_penalty_kill` | numeric | PDO (on-ice shooting % + save %) on the penalty kill (team, over the season range). Percent, 0-100. |
| `pdo_even_strength` | numeric | PDO (on-ice shooting % + save %) at even strength (team, over the season range). Percent, 0-100. |
| `pdo_powerplay` | numeric | PDO (on-ice shooting % + save %) on the power play (team, over the season range). Percent, 0-100. |
| `shots_percentage` | numeric | Shots percentage (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_for_shots_against_percentage_even_strength` | numeric | Non blocked shots for shots against percentage at even strength (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_for_shots_against_percentage_powerplay` | numeric | Non blocked shots for shots against percentage on the power play (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_for_shots_against_percentage_penalty_kill` | numeric | Non blocked shots for shots against percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `shots_for_shots_against_percentage_even_strength` | numeric | Shots for shots against percentage at even strength (team, over the season range). Percent, 0-100. |
| `shots_for_shots_against_percentage_powerplay` | numeric | Shots for shots against percentage on the power play (team, over the season range). Percent, 0-100. |
| `shots_for_shots_against_percentage_penalty_kill` | numeric | Shots for shots against percentage on the penalty kill (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_difference_powerplay` | integer | Non blocked shots difference on the power play (team, over the season range). |
| `non_blocked_shots_difference_penalty_kill` | integer | Non blocked shots difference on the penalty kill (team, over the season range). |
| `non_blocked_shots_for_shots_against_percentage` | numeric | Non blocked shots for shots against percentage (team, over the season range). Percent, 0-100. |
| `pdo` | numeric | PDO (on-ice shooting % + save %) (team, over the season range). Percent, 0-100. |
| `non_blocked_shots_difference_even_strength` | integer | Non blocked shots difference at even strength (team, over the season range). |
| `save_percentage` | numeric | Save percentage (team, over the season range). Percent, 0-100. |

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

_Last validated n/a._
