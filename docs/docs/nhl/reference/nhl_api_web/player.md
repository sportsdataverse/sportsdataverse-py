---
title: "NHL — NHL Web API — Player"
sidebar_label: "Player"
sidebar_position: 3
description: "NHL — NHL Web API — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Web API — Player

## nhl_player_landing

Pull the player profile / overview.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/landing](https://api-web.nhle.com/v1/player/8480801/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |

### Returns {#nhl_player_landing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `is_active` | Boolean | Whether the team is active. |
| `current_team_id` | Int64 | Player's current team identifier. |
| `current_team_abbrev` | String | Three-letter abbreviation of the NHL team the player is currently rostered on (e.g., 'TOR', 'BOS'). |
| `badges` | String | Serialized list of achievement or milestone badges displayed on the player's NHL api-web profile page. |
| `team_logo` | String | URL to the team logo image. |
| `sweater_number` | Int64 | Jersey number. |
| `position` | String | Player position. |
| `headshot` | String | URL to the player headshot image. |
| `hero_image` | String | URL to the large hero/banner image of the player displayed at the top of their NHL api-web profile page. |
| `height_in_inches` | Int64 | Height in inches. |
| `height_in_centimeters` | Int64 | Height in centimeters. |
| `weight_in_pounds` | Int64 | Weight in pounds. |
| `weight_in_kilograms` | Int64 | Weight in kilograms. |
| `birth_date` | String | Player birth date. |
| `birth_country` | String | Player birth country. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `player_slug` | String | URL slug for the player. |
| `in_top100_all_time` | Int64 | Flag (1/0) indicating whether the player is recognized among the NHL's Top 100 all-time greatest players. |
| `in_hhof` | Int64 | Flag (1/0) indicating whether the player has been inducted into the Hockey Hall of Fame. |
| `shop_link` | String | URL to the NHL shop page where merchandise for this player (e.g., jerseys) can be purchased. |
| `twitter_link` | String | URL to the player's official Twitter/X account as listed on their NHL api-web profile. |
| `watch_link` | String | URL to the NHL.tv or league streaming page where the player's games can be watched. |
| `last5_games` | String | Serialized array of stat lines for the player's five most recent NHL games, as returned by the player landing endpoint. |
| `season_totals` | String | Serialized array of per-season stat totals for the player across all regular seasons and playoffs in their NHL career. |
| `awards` | String | Serialized list of NHL awards and honors the player has received, as returned by the NHL api-web player landing endpoint. |
| `current_team_roster` | String | Serialized roster-position metadata for the player's current NHL team assignment, as returned by the player landing endpoint. |
| `full_team_name_default` | String | Full English name of the player's current NHL team (e.g., 'Toronto Maple Leafs'), as returned by the player landing endpoint. |
| `full_team_name_fr` | String | Full French-language name of the player's current NHL team, as returned by the player landing endpoint. |
| `team_common_name_default` | String | Team common name (default language). |
| `team_place_name_with_preposition_default` | String | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | String | Team place name with preposition (French). |
| `first_name_default` | String | Player first name (default language). |
| `last_name_default` | String | Player last name (default language). |
| `birth_city_default` | String | Birth city (default localization). |
| `birth_state_province_default` | String | Birth state/province (default localization). |
| `draft_details_year` | Int64 | Calendar year in which the player was selected in the NHL Entry Draft. |
| `draft_details_team_abbrev` | String | Three-letter abbreviation of the NHL team that drafted the player in the Entry Draft. |
| `draft_details_round` | Int64 | Round number in which the player was selected during the NHL Entry Draft. |
| `draft_details_pick_in_round` | Int64 | Pick number within the player's draft round in the NHL Entry Draft. |
| `draft_details_overall_pick` | Int64 | Overall pick number at which the player was selected in the NHL Entry Draft. |
| `featured_stats_season` | Int64 | Eight-digit NHL season identifier (e.g., 20232024) indicating which season the featured stats on the player's profile correspond to. |
| `featured_stats_regular_season_sub_season_assists` | Int64 | Assists recorded by the player in the featured regular-season sub-season (typically the current or most recent season) on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_game_winning_goals` | Int64 | Game-winning goals recorded by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_games_played` | Int64 | Games played by the player in the featured regular-season sub-season (typically the current or most recent season) on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_goals` | Int64 | Goals scored by the player in the featured regular-season sub-season (typically the current or most recent season) on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_ot_goals` | Int64 | Overtime goals scored by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_pim` | Int64 | Penalty minutes accumulated by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_plus_minus` | Int64 | Plus/minus rating for the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_points` | Int64 | Points (goals + assists) recorded by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_power_play_goals` | Int64 | Power-play goals scored by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_power_play_points` | Int64 | Power-play points accumulated by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_shooting_pctg` | Float64 | Shooting percentage for the player in the featured regular-season sub-season on their NHL api-web profile, expressed as a decimal. |
| `featured_stats_regular_season_sub_season_shorthanded_goals` | Int64 | Shorthanded goals scored by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_shorthanded_points` | Int64 | Shorthanded points accumulated by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_shots` | Int64 | Shots on goal taken by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_career_assists` | Int64 | Career regular-season assists highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_game_winning_goals` | Int64 | Career regular-season game-winning goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_games_played` | Int64 | Career regular-season games played highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_goals` | Int64 | Career regular-season goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_ot_goals` | Int64 | Career regular-season overtime goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_pim` | Int64 | Career regular-season penalty minutes highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_plus_minus` | Int64 | Career regular-season plus/minus rating highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_points` | Int64 | Career regular-season points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_power_play_goals` | Int64 | Career regular-season power-play goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_power_play_points` | Int64 | Career regular-season power-play points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_shooting_pctg` | Float64 | Career regular-season shooting percentage highlighted on the player's NHL api-web profile, expressed as a decimal. |
| `featured_stats_regular_season_career_shorthanded_goals` | Int64 | Career regular-season shorthanded goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_shorthanded_points` | Int64 | Career regular-season shorthanded points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_shots` | Int64 | Career regular-season shots on goal highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_sub_season_assists` | Int64 | Assists recorded by the player in the featured playoff sub-season (typically the most recent postseason) on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_game_winning_goals` | Int64 | Game-winning goals recorded by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_games_played` | Int64 | Games played by the player in the featured playoff sub-season (typically the most recent postseason) on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_goals` | Int64 | Goals scored by the player in the featured playoff sub-season (typically the most recent postseason) on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_ot_goals` | Int64 | Overtime goals scored by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_pim` | Int64 | Penalty minutes accumulated by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_plus_minus` | Int64 | Plus/minus rating for the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_points` | Int64 | Points (goals + assists) recorded by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_power_play_goals` | Int64 | Power-play goals scored by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_power_play_points` | Int64 | Power-play points accumulated by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_shooting_pctg` | Float64 | Shooting percentage for the player in the featured playoff sub-season on their NHL api-web profile, expressed as a decimal. |
| `featured_stats_playoffs_sub_season_shorthanded_goals` | Int64 | Shorthanded goals scored by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_shorthanded_points` | Int64 | Shorthanded points accumulated by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_shots` | Int64 | Shots on goal taken by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_career_assists` | Int64 | Career playoff assists highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_game_winning_goals` | Int64 | Career playoff game-winning goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_games_played` | Int64 | Career playoff games played highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_goals` | Int64 | Career playoff goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_ot_goals` | Int64 | Career playoff overtime goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_pim` | Int64 | Career playoff penalty minutes highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_plus_minus` | Int64 | Career playoff plus/minus rating highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_points` | Int64 | Career playoff points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_power_play_goals` | Int64 | Career playoff power-play goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_power_play_points` | Int64 | Career playoff power-play points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_shooting_pctg` | Float64 | Career playoff shooting percentage highlighted on the player's NHL api-web profile, expressed as a decimal. |
| `featured_stats_playoffs_career_shorthanded_goals` | Int64 | Career playoff shorthanded goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_shorthanded_points` | Int64 | Career playoff shorthanded points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_shots` | Int64 | Career playoff shots on goal highlighted on the player's NHL api-web profile for the featured season context. |
| `career_totals_regular_season_assists` | Int64 | Career cumulative assists recorded by the player across all NHL regular-season games. |
| `career_totals_regular_season_avg_toi` | String | Career average time on ice per game in the NHL regular season, expressed as an MM:SS string. |
| `career_totals_regular_season_faceoff_winning_pctg` | Float64 | Career faceoff win percentage for the player across all NHL regular-season games. |
| `career_totals_regular_season_game_winning_goals` | Int64 | Career total of game-winning goals the player has scored in NHL regular-season games. |
| `career_totals_regular_season_games_played` | Int64 | Total number of NHL regular-season games the player has appeared in across their career. |
| `career_totals_regular_season_goals` | Int64 | Career total goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_ot_goals` | Int64 | Career total overtime goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_pim` | Int64 | Career total penalty minutes accumulated by the player in NHL regular-season games. |
| `career_totals_regular_season_plus_minus` | Int64 | Career plus/minus rating accumulated by the player across all NHL regular-season games. |
| `career_totals_regular_season_points` | Int64 | Career total points (goals + assists) accumulated by the player in NHL regular-season games. |
| `career_totals_regular_season_power_play_goals` | Int64 | Career total power-play goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_power_play_points` | Int64 | Career total power-play points (goals + assists on the power play) in NHL regular-season games. |
| `career_totals_regular_season_shooting_pctg` | Float64 | Career shooting percentage for the player in NHL regular-season games, expressed as a decimal. |
| `career_totals_regular_season_shorthanded_goals` | Int64 | Career total shorthanded goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_shorthanded_points` | Int64 | Career total shorthanded points (goals + assists while shorthanded) in NHL regular-season games. |
| `career_totals_regular_season_shots` | Int64 | Career total shots on goal taken by the player in NHL regular-season games. |
| `career_totals_playoffs_assists` | Int64 | Career cumulative assists recorded by the player across all NHL playoff appearances. |
| `career_totals_playoffs_avg_toi` | String | Career average time on ice per game in NHL playoff play, expressed as an MM:SS string. |
| `career_totals_playoffs_faceoff_winning_pctg` | Float64 | Career faceoff win percentage for the player across all NHL playoff games. |
| `career_totals_playoffs_game_winning_goals` | Int64 | Career total of game-winning goals the player has scored in NHL playoff games. |
| `career_totals_playoffs_games_played` | Int64 | Total number of NHL playoff games the player has appeared in across their career. |
| `career_totals_playoffs_goals` | Int64 | Career total goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_ot_goals` | Int64 | Career total overtime goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_pim` | Int64 | Career total penalty minutes accumulated by the player in NHL playoff games. |
| `career_totals_playoffs_plus_minus` | Int64 | Career plus/minus rating accumulated by the player across all NHL playoff games. |
| `career_totals_playoffs_points` | Int64 | Career total points (goals + assists) accumulated by the player in NHL playoff games. |
| `career_totals_playoffs_power_play_goals` | Int64 | Career total power-play goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_power_play_points` | Int64 | Career total power-play points (goals + assists on the power play) in NHL playoff games. |
| `career_totals_playoffs_shooting_pctg` | Float64 | Career shooting percentage for the player in NHL playoff games, expressed as a decimal. |
| `career_totals_playoffs_shorthanded_goals` | Int64 | Career total shorthanded goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_shorthanded_points` | Int64 | Career total shorthanded points (goals + assists while shorthanded) in NHL playoff games. |
| `career_totals_playoffs_shots` | Int64 | Career total shots on goal taken by the player in NHL playoff games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_player_landing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_player_landing-example}

```python
nhl_player_landing(player_id=8480801)
```

## nhl_player_game_log

Pull a player's game-by-game log.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/game-log/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/game-log/now](https://api-web.nhle.com/v1/player/8480801/game-log/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_player_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Unique game identifier. |
| `team_abbrev` | String | Team abbreviation. |
| `home_road_flag` | String | Home or road indicator. |
| `game_date` | String | Game date. |
| `goals` | Int64 | Goals scored. |
| `assists` | Int64 | Assists. |
| `points` | Int64 | Total points (goals + assists). |
| `plus_minus` | Int64 | Plus/minus rating. |
| `power_play_goals` | Int64 | Power-play goals. |
| `power_play_points` | Int64 | Power play points. |
| `game_winning_goals` | Int64 | Game-winning goals. |
| `ot_goals` | Int64 | Overtime goals. |
| `shots` | Int64 | Shots on goal. |
| `shifts` | Int64 | Number of shifts. |
| `shorthanded_goals` | Int64 | Shorthanded goals. |
| `shorthanded_points` | Int64 | Shorthanded points. |
| `opponent_abbrev` | String | Opponent team abbreviation. |
| `pim` | Int64 | Penalty minutes. |
| `toi` | String | Time on ice. |
| `common_name_default` | String | Player's team common name. |
| `opponent_common_name_default` | String | Opponent team common name. |
| `opponent_common_name_fr` | String | French-language common name of the opposing team in the player's individual game log entry from the NHL api-web feed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_player_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_player_game_log-example}

```python
nhl_player_game_log(player_id=8480801)
```

## nhl_player_spotlight

Pull the league's currently featured players.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player-spotlight`

**Valid URL:** [https://api-web.nhle.com/v1/player-spotlight](https://api-web.nhle.com/v1/player-spotlight)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_player_spotlight-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_slug` | String | URL slug for the player. |
| `position` | String | Player position. |
| `sweater_number` | Int64 | Jersey number. |
| `team_id` | Int64 | Unique team identifier. |
| `headshot` | String | URL to the player headshot image. |
| `team_tri_code` | String | Team tri-code abbreviation. |
| `team_logo` | String | URL to the team logo image. |
| `sort_id` | Int64 | Sort order identifier for the spotlight. |
| `name_default` | String | Player name (default localization). |
| `name_cs` | String | Player name (Czech localization). |
| `name_fi` | String | Player name (Finnish localization). |
| `name_sk` | String | Player name (Slovak localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_player_spotlight-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_player_spotlight-example}

```python
nhl_player_spotlight()
```
