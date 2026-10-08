---
title: "NHL — NHL EDGE API — Goalie"
sidebar_label: "Goalie"
sidebar_position: 2
description: "NHL — NHL EDGE API — Goalie — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL EDGE API — Goalie

## nhl_edge_goalie_detail

Pull EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-detail/8480801/now](https://api-web.nhle.com/v1/edge/goalie-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Serialized list of seasons for which NHL EDGE player-tracking data is available for this goalie. |
| `shot_location_summary` | character | Serialized summary of shot-location zones faced by the goalie, aggregated from NHL EDGE tracking data. |
| `shot_location_details` | character | Serialized detailed breakdown of shot locations faced by the goalie, derived from NHL EDGE tracking data. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character | Hand on which the goalie catches (glove side), typically 'L' for left or 'R' for right. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer | Number of wins credited to the goalie for the season in this NHL EDGE detail record. |
| `player_losses` | integer | Number of regulation losses credited to the goalie for the season in this NHL EDGE detail record. |
| `player_overtime_losses` | integer | Number of overtime or shootout losses (OTL) credited to the goalie during the season. |
| `player_goals_against_avg` | double | Goals-against average (GAA) for the goalie during the season, reflecting the average number of goals allowed per 60 minutes played. |
| `player_save_pctg` | double | Save percentage (SV%) for the goalie during the season, expressed as a decimal ratio of saves to shots faced. |
| `player_games_played` | integer | Total number of regular-season games the goalie appeared in for the season covered by this NHL EDGE detail record. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `stats_goals_against_avg_value` | double | Goalie's goals-against average value as reported in the NHL EDGE detail stat block (mirrors player_goals_against_avg at the EDGE layer). |
| `stats_goals_against_avg_percentile` | double | Percentile rank among all NHL goalies for goals-against average as reported in the NHL EDGE detail. |
| `stats_goals_against_avg_league_avg` | double | League-average GAA value used as the EDGE comparative baseline for this goalie's goals-against-average metric. |
| `stats_games_above900_value` | double | Goalie's own count of games in which save percentage exceeded .900, as tracked by the NHL EDGE system. |
| `stats_games_above900_percentile` | double | Percentile rank among all NHL goalies for the 'games above .900 save percentage' EDGE metric during the season. |
| `stats_games_above900_league_avg` | double | League-average value for the 'games above .900 save percentage' EDGE metric, used as a comparative baseline for the goalie. |
| `stats_goal_differential_per60_value` | double | Goalie's net goal differential (team goals scored minus goals allowed while in net) per 60 minutes of play, as tracked by the NHL EDGE system. |
| `stats_goal_differential_per60_percentile` | double | Percentile rank among all NHL goalies for the goals-differential-per-60 EDGE metric during the season. |
| `stats_goal_differential_per60_league_avg` | double | League-average net goal differential (team goals scored minus goals allowed while in net) per 60 minutes, the EDGE baseline comparator for the goalie. |
| `stats_goal_support_avg_value` | double | Average number of goals scored by the goalie's team per game while this goalie was in net, as tracked by NHL EDGE. |
| `stats_goal_support_avg_percentile` | double | Percentile rank among all NHL goalies for average goal support received while the goalie was in net. |
| `stats_goal_support_avg_league_avg` | double | League-average goal-support value (average goals scored for the goalie while in net), used as the EDGE baseline comparator. |
| `stats_point_pctg_value` | double | Team points percentage in games started by this goalie during the season, as tracked by the NHL EDGE system. |
| `stats_point_pctg_percentile` | double | Percentile rank among all NHL goalies for team points percentage in games the goalie started, per NHL EDGE. |
| `stats_point_pctg_league_avg` | double | League-average points percentage (team winning percentage when the goalie starts) used as the EDGE comparative baseline. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_detail-example}

```python
nhl_edge_goalie_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_goalie_5v5_detail

Pull EDGE 5-on-5 detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801/now](https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_5v5_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `save_pctg5v5_last10` | character | Serialized last-10-game 5-on-5 save percentage trend data for the goalie. |
| `save_pctg5v5_details_save_pctg_value` | double | Goalie's overall 5-on-5 save percentage (saves divided by shots faced at even strength). |
| `save_pctg5v5_details_save_pctg_league_avg` | double | League-average 5-on-5 save percentage across all NHL goalies for the current period. |
| `save_pctg5v5_details_save_pctg_percentile` | double | Percentile rank of the goalie's overall 5-on-5 save percentage relative to all qualifying NHL goalies. |
| `save_pctg5v5_details_save_pctg_close_value` | double | Goalie's 5-on-5 save percentage recorded specifically in close-score situations. |
| `save_pctg5v5_details_save_pctg_close_league_avg` | double | League-average 5-on-5 save percentage in close-score situations (one-goal games in the third period or overtime) for the current period. |
| `save_pctg5v5_details_save_pctg_close_percentile` | double | Percentile rank of the goalie's 5-on-5 save percentage in close-score situations relative to all qualifying NHL goalies. |
| `save_pctg5v5_details_shots_value` | integer | Total number of 5-on-5 shots the goalie faced during the current period. |
| `save_pctg5v5_details_shots_league_avg` | integer | League-average number of 5-on-5 shots faced per game by NHL goalies for the current period. |
| `save_pctg5v5_details_shots_percentile` | double | Percentile rank of the goalie's 5-on-5 shots-faced count relative to all qualifying NHL goalies. |
| `save_pctg5v5_details_shots_per60_value` | double | Goalie's rate of 5-on-5 shots faced per 60 minutes of even-strength ice time. |
| `save_pctg5v5_details_shots_per60_league_avg` | double | League-average rate of 5-on-5 shots faced per 60 minutes of even-strength ice time. |
| `save_pctg5v5_details_shots_per60_percentile` | double | Percentile rank of the goalie's 5-on-5 shots-faced-per-60-minutes rate relative to all qualifying NHL goalies. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_5v5_detail-example}

```python
nhl_edge_goalie_5v5_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_goalie_5v5_top_10

Pull the EDGE top-10 goalies by 5-on-5 metrics.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/save-pctg/20242025/2](https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/save-pctg/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_5v5_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `save_pctg` | double | Save percentage. |
| `save_pctg_close` | double |  |
| `shots` | integer | Shots on goal. |
| `shots_per60` | double |  |
| `player_first_name_default` | character | Player first name (default language). |
| `player_headshot` | character | URL to the player headshot image. |
| `player_last_name_default` | character | Player last name (default language). |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_first_name_cs` | character | Player first name (Czech locale). |
| `player_first_name_sk` | character | Player first name (Slovak locale). |
| `player_last_name_cs` | character | Player last name (Czech locale). |
| `player_last_name_fi` | character | Player last name (Finnish). |
| `player_last_name_sk` | character | Player last name (Slovak locale). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_5v5_top_10-example}

```python
nhl_edge_goalie_5v5_top_10(sort_by='save-pctg', season=20242025)
```

_Last validated n/a._

## nhl_edge_goalie_comparison

Pull EDGE comparison data for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-comparison/8480801/now](https://api-web.nhle.com/v1/edge/goalie-comparison/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_comparison-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Serialized list of season identifiers for which EDGE player-tracking data is available for this goalie. |
| `shot_location_summary` | character | High-level serialized summary of the goalie's save percentages grouped by shot location zone. |
| `shot_location_details` | character | Serialized shot-zone breakdown detailing save rates from each ice region (slot, off-wing, etc.). |
| `save_pctg5v5_last10` | character | Serialized breakdown of the goalie's 5-on-5 save percentage across their last 10 games. |
| `save_pctg_last10` | character | Serialized summary of the goalie's save percentage across their most recent 10 games. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character | Handedness indicator showing which side the goalie catches (L = left-catch, R = right-catch). |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer | Number of decisions recorded as wins for the goalie during the comparison period. |
| `player_losses` | integer | Number of decisions recorded as losses for the goalie during the comparison period. |
| `player_overtime_losses` | integer | Number of losses the goalie suffered after regulation time, counting as an overtime loss in the standings. |
| `player_goals_against_avg` | double | Goalie's goals-against average — average goals allowed per 60 minutes of ice time. |
| `player_save_pctg` | double | Overall save percentage for the goalie — proportion of shots faced that were stopped. |
| `player_games_played` | integer | Total number of regular-season or playoff games the goalie appeared in during the comparison period. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `save_pctg5v5_details_save_pctg` | double | Goalie's save percentage in 5-on-5 even-strength situations only. |
| `save_pctg5v5_details_save_pctg_close` | double | Save percentage in 5-on-5 situations where the game score was within one goal (close-game situations). |
| `save_pctg5v5_details_shots` | integer | Total number of shots the goalie faced in 5-on-5 situations during the comparison period. |
| `save_pctg5v5_details_shots_per60` | double | Rate of shots faced per 60 minutes of 5-on-5 ice time, reflecting workload intensity. |
| `save_pctg_details_games_above900` | integer | Number of games in which the goalie posted a save percentage above .900 during the comparison period. |
| `save_pctg_details_pctg_games_above900` | double | Proportion of the goalie's games (as a percentage) in which they achieved a save percentage above .900. |
| `save_pctg_details_point_pctg` | double | Team points percentage in games started by this goalie, reflecting their contribution to standings. |
| `save_pctg_details_goals_against_avg` | double | Goals-against average from the detailed save-percentage breakdown dataset for this goalie. |
| `save_pctg_details_save_pctg` | double | Overall save percentage from the detailed breakdown dataset, capturing all situations. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_comparison-example}

```python
nhl_edge_goalie_comparison(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_goalie_save_percentage_detail

Pull EDGE save-percentage detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801/now](https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_save_percentage_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `save_pctg_last10` | character | Serialized summary of the goalie's save percentage across their most recent 10 games. |
| `save_pctg_details_games_above900_value` | integer | Actual count of games in which this goalie achieved a save percentage above .900. |
| `save_pctg_details_games_above900_percentile` | double | Percentile rank of this goalie's games-above-.900 count relative to all qualifying goalies. |
| `save_pctg_details_games_above900_league_avg` | double | League-average number of games in which goalies posted a save percentage above .900, used as a comparison baseline. |
| `save_pctg_details_pctg_games_above900_value` | double | Proportion (as a decimal fraction) of the goalie's games in which they exceeded a .900 save percentage. |
| `save_pctg_details_pctg_games_above900_percentile` | double | Percentile rank of this goalie's proportion of games above .900 relative to all qualifying goalies. |
| `save_pctg_details_pctg_games_above900_league_avg` | double | League-average percentage of games with a save percentage above .900, used as the baseline comparison. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_save_percentage_detail-example}

```python
nhl_edge_goalie_save_percentage_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_goalie_edge_save_pctg_top_10

Pull the EDGE top-10 goalies by save-percentage.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points/now](https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_edge_save_pctg_top_10-returns}

**`return_parsed=True`** (default) — the output of `parse_edge_top10`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: every value tried 404s for season 20242025 (sort_by points/total/savePctg/save-pctg, position F/forwards/all, category shots/high/all/high-danger, fastRhockey's examples included), so the valid path values are unconfirmed.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_edge_save_pctg_top_10-example}

```python
nhl_edge_goalie_edge_save_pctg_top_10(sort_by='points')
```

_Last validated n/a._

## nhl_edge_goalie_shot_location_detail

Pull EDGE shot-location detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801/now](https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_shot_location_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `area` | character | Net/ice zone the shots were taken from. |
| `shots_against` | integer | Shots faced. |
| `saves` | integer | Saves made. |
| `goals_against` | integer | Goals against. |
| `save_pctg` | double | Save percentage. |
| `shots_against_percentile` | double | League percentile rank for shots against. |
| `saves_percentile` | double | League percentile rank for saves. |
| `goals_against_percentile` | double | League percentile rank for goals against. |
| `save_pctg_percentile` | double | League percentile rank for save percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_shot_location_detail-example}

```python
nhl_edge_goalie_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_goalie_shot_location_top_10

Pull the EDGE top-10 goalies for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points/now](https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_shot_location_top_10-returns}

**`return_parsed=True`** (default) — the output of `parse_edge_top10`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: every value tried 404s for season 20242025 (sort_by points/total/savePctg/save-pctg, position F/forwards/all, category shots/high/all/high-danger, fastRhockey's examples included), so the valid path values are unconfirmed.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_shot_location_top_10-example}

```python
nhl_edge_goalie_shot_location_top_10(category='shots', sort_by='points')
```

_Last validated n/a._

## nhl_edge_goalie_landing

Pull the EDGE goalie landing page (summary across all goalies).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-landing/now](https://api-web.nhle.com/v1/edge/goalie-landing/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_goalie_landing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Serialized list of season identifiers for which EDGE player-tracking data is available on this landing page. |
| `minimum_minutes_played` | integer | Minimum minutes-played threshold a goalie must meet to qualify for the EDGE leaderboards. |
| `leaders_high_danger_save_pctg_player_id` | integer | NHL player identifier for the goalie leading the high-danger save-percentage leaderboard. |
| `leaders_high_danger_save_pctg_player_first_name_default` | character | First name of the goalie leading the high-danger save-percentage leaderboard (default locale). |
| `leaders_high_danger_save_pctg_player_last_name_default` | character | Last name of the goalie leading the high-danger save-percentage leaderboard (default locale). |
| `leaders_high_danger_save_pctg_player_last_name_cs` | character | Last name of the high-danger save-percentage leader rendered in Czech locale. |
| `leaders_high_danger_save_pctg_player_last_name_sk` | character | Last name of the high-danger save-percentage leader rendered in Slovak locale. |
| `leaders_high_danger_save_pctg_player_sweater_number` | integer | Jersey number worn by the goalie leading the high-danger save-percentage leaderboard. |
| `leaders_high_danger_save_pctg_player_position` | character | Position code for the goalie leading the high-danger save-percentage leaderboard (always G). |
| `leaders_high_danger_save_pctg_player_slug` | character | URL-friendly slug for the high-danger save-percentage leader, used in NHL.com profile links. |
| `leaders_high_danger_save_pctg_player_headshot` | character | URL pointing to the headshot image of the goalie leading the high-danger save-percentage leaderboard. |
| `leaders_high_danger_save_pctg_player_team_common_name_default` | character | Common team name for the club of the leader in the high-danger save-percentage category. |
| `leaders_high_danger_save_pctg_player_team_place_name_with_preposition_default` | character | City/place name with grammatical preposition for the high-danger save-percentage leader's team, default locale. |
| `leaders_high_danger_save_pctg_player_team_place_name_with_preposition_fr` | character | City/place name with grammatical preposition for the high-danger save-percentage leader's team in the French locale. |
| `leaders_high_danger_save_pctg_player_team_abbrev` | character | Three-letter team abbreviation for the club of the goalie leading the high-danger save-percentage leaderboard. |
| `leaders_high_danger_save_pctg_player_team_team_logo_light` | character | URL for the light-background team logo for the high-danger save-percentage leaderboard leader. |
| `leaders_high_danger_save_pctg_player_team_team_logo_dark` | character | URL for the dark-background team logo for the high-danger save-percentage leaderboard leader. |
| `leaders_high_danger_save_pctg_save_pctg` | double | Save percentage value for the leader of the high-danger save-percentage leaderboard. |
| `leaders_high_danger_save_pctg_shot_location_details` | character | Serialized shot-zone breakdown for the high-danger save-percentage leaderboard leader. |
| `leaders_high_danger_saves_player_id` | integer | NHL player identifier for the goalie leading the high-danger saves leaderboard. |
| `leaders_high_danger_saves_player_first_name_default` | character | First name of the goalie leading the high-danger saves leaderboard (default locale). |
| `leaders_high_danger_saves_player_last_name_default` | character | Last name of the goalie leading the high-danger saves leaderboard (default locale). |
| `leaders_high_danger_saves_player_sweater_number` | integer | Jersey number worn by the goalie leading the high-danger saves leaderboard. |
| `leaders_high_danger_saves_player_position` | character | Position code for the goalie leading the high-danger saves leaderboard (always G). |
| `leaders_high_danger_saves_player_slug` | character | URL-friendly slug for the high-danger saves leaderboard leader, used in NHL.com profile links. |
| `leaders_high_danger_saves_player_headshot` | character | URL pointing to the headshot image of the goalie leading the high-danger saves leaderboard. |
| `leaders_high_danger_saves_player_team_common_name_default` | character | Common team name for the club of the leader in the high-danger saves category. |
| `leaders_high_danger_saves_player_team_place_name_with_preposition_default` | character | City/place name with grammatical preposition for the high-danger saves leader's team, default locale. |
| `leaders_high_danger_saves_player_team_place_name_with_preposition_fr` | character | City/place name with grammatical preposition for the high-danger saves leader's team in the French locale. |
| `leaders_high_danger_saves_player_team_abbrev` | character | Three-letter team abbreviation for the club of the goalie leading the high-danger saves leaderboard. |
| `leaders_high_danger_saves_player_team_team_logo_light` | character | URL for the light-background team logo for the high-danger saves leaderboard leader. |
| `leaders_high_danger_saves_player_team_team_logo_dark` | character | URL for the dark-background team logo for the high-danger saves leaderboard leader. |
| `leaders_high_danger_saves_saves` | integer | Total high-danger saves made by the goalie leading the high-danger saves leaderboard. |
| `leaders_high_danger_saves_shot_location_details` | character | Serialized shot-zone breakdown for the high-danger saves leaderboard leader. |
| `leaders_high_danger_goals_against_player_id` | integer | NHL player identifier for the goalie leading the high-danger goals-against leaderboard. |
| `leaders_high_danger_goals_against_player_first_name_default` | character | First name of the goalie leading the high-danger goals-against leaderboard (default locale). |
| `leaders_high_danger_goals_against_player_last_name_default` | character | Last name of the goalie leading the high-danger goals-against leaderboard (default locale). |
| `leaders_high_danger_goals_against_player_sweater_number` | integer | Jersey number worn by the goalie leading the high-danger goals-against leaderboard. |
| `leaders_high_danger_goals_against_player_position` | character | Position code for the goalie leading the high-danger goals-against leaderboard (always G). |
| `leaders_high_danger_goals_against_player_slug` | character | URL-friendly slug for the goalie leading the high-danger goals-against leaderboard, used in NHL.com profile links. |
| `leaders_high_danger_goals_against_player_headshot` | character | URL pointing to the headshot image of the goalie leading the high-danger goals-against leaderboard. |
| `leaders_high_danger_goals_against_player_team_common_name_default` | character | Common team name for the club of the leader in the high-danger goals-against category. |
| `leaders_high_danger_goals_against_player_team_place_name_with_preposition_default` | character | City/place name with grammatical preposition for the high-danger goals-against leader's team, default locale. |
| `leaders_high_danger_goals_against_player_team_place_name_with_preposition_fr` | character | City/place name with grammatical preposition for the high-danger goals-against leader's team in the French locale. |
| `leaders_high_danger_goals_against_player_team_abbrev` | character | Three-letter team abbreviation for the club of the goalie leading the high-danger goals-against leaderboard. |
| `leaders_high_danger_goals_against_player_team_team_logo_light` | character | URL for the light-background team logo for the high-danger goals-against leaderboard leader. |
| `leaders_high_danger_goals_against_player_team_team_logo_dark` | character | URL for the dark-background team logo for the high-danger goals-against leaderboard leader. |
| `leaders_high_danger_goals_against_goals_against` | integer | Number of high-danger goals allowed by the goalie leading the high-danger goals-against leaderboard. |
| `leaders_save_pctg5v5_player_id` | integer | NHL player identifier for the goalie leading the 5-on-5 save-percentage leaderboard. |
| `leaders_save_pctg5v5_player_first_name_default` | character | First name of the goalie leading the 5-on-5 save-percentage leaderboard (default locale). |
| `leaders_save_pctg5v5_player_last_name_default` | character | Last name of the goalie leading the 5-on-5 save-percentage leaderboard (default locale). |
| `leaders_save_pctg5v5_player_last_name_cs` | character | Last name of the 5-on-5 save-percentage leader rendered in Czech locale. |
| `leaders_save_pctg5v5_player_last_name_sk` | character | Last name of the 5-on-5 save-percentage leader rendered in Slovak locale. |
| `leaders_save_pctg5v5_player_sweater_number` | integer | Jersey number worn by the goalie leading the 5-on-5 save-percentage leaderboard. |
| `leaders_save_pctg5v5_player_position` | character | Position code for the goalie leading the 5-on-5 save-percentage leaderboard (always G). |
| `leaders_save_pctg5v5_player_slug` | character | URL-friendly slug for the 5-on-5 save-percentage leaderboard leader, used in NHL.com profile links. |
| `leaders_save_pctg5v5_player_headshot` | character | URL pointing to the headshot image of the goalie leading the 5-on-5 save-percentage leaderboard. |
| `leaders_save_pctg5v5_player_team_common_name_default` | character | Common team name for the club of the leader in the 5-on-5 save-percentage category. |
| `leaders_save_pctg5v5_player_team_place_name_with_preposition_default` | character | City/place name with grammatical preposition for the 5-on-5 save-percentage leader's team, default locale. |
| `leaders_save_pctg5v5_player_team_place_name_with_preposition_fr` | character | City/place name with grammatical preposition for the 5-on-5 save-percentage leader's team in the French locale. |
| `leaders_save_pctg5v5_player_team_abbrev` | character | Three-letter team abbreviation for the club of the goalie leading the 5-on-5 save-percentage leaderboard. |
| `leaders_save_pctg5v5_player_team_team_logo_light` | character | URL for the light-background team logo for the 5-on-5 save-percentage leaderboard leader. |
| `leaders_save_pctg5v5_player_team_team_logo_dark` | character | URL for the dark-background team logo for the 5-on-5 save-percentage leaderboard leader. |
| `leaders_save_pctg5v5_save_pctg` | double | Save percentage value for the leader of the 5-on-5 save-percentage leaderboard. |
| `leaders_games_above900_player_id` | integer | NHL player identifier for the goalie leading the games-above-.900 leaderboard. |
| `leaders_games_above900_player_first_name_default` | character | First name of the goalie leading the games-above-.900 leaderboard (default locale). |
| `leaders_games_above900_player_last_name_default` | character | Last name of the goalie leading the games-above-.900 leaderboard (default locale). |
| `leaders_games_above900_player_sweater_number` | integer | Jersey number worn by the goalie leading the games-above-.900 leaderboard. |
| `leaders_games_above900_player_position` | character | Position code for the goalie leading the games-above-.900 leaderboard (always G). |
| `leaders_games_above900_player_slug` | character | URL-friendly slug for the goalie leading the games-above-.900 leaderboard, used in NHL.com profile links. |
| `leaders_games_above900_player_headshot` | character | URL pointing to the headshot image of the goalie leading the games-above-.900 leaderboard. |
| `leaders_games_above900_player_team_common_name_default` | character | Common team name (e.g., Maple Leafs) for the club of the leader in the games-above-.900 category. |
| `leaders_games_above900_player_team_place_name_with_preposition_default` | character | City/place name with grammatical preposition (e.g., in Toronto) for the leader's team, in the default locale. |
| `leaders_games_above900_player_team_place_name_with_preposition_fr` | character | City/place name with grammatical preposition for the leader's team in the French locale. |
| `leaders_games_above900_player_team_abbrev` | character | Three-letter team abbreviation for the club of the goalie leading the games-above-.900 leaderboard. |
| `leaders_games_above900_player_team_team_logo_light` | character | URL for the light-background version of the team logo for the games-above-.900 leaderboard leader. |
| `leaders_games_above900_player_team_team_logo_dark` | character | URL for the dark-background version of the team logo for the games-above-.900 leaderboard leader. |
| `leaders_games_above900_games` | integer | Number of games above a .900 save percentage for the top-ranked goalie in that leaderboard category. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_goalie_landing-example}

```python
nhl_edge_goalie_landing()
```

_Last validated n/a._
