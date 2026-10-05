---
title: "NHL — NHL Web API — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "NHL — NHL Web API — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Web API — Other

## nhl_boxscore

Pull the boxscore for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore](https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `home_away` | character | Home or away indicator. |
| `position_group` | character | Position group name (e.g. Centers). |
| `player_id` | integer | Unique player identifier. |
| `sweater_number` | integer | Jersey number. |
| `position` | character | Player position. |
| `goals` | double | Goals scored. |
| `assists` | double | Assists. |
| `points` | double | Total points (goals + assists). |
| `plus_minus` | double | Plus/minus rating. |
| `pim` | integer | Penalty minutes. |
| `hits` | double | Hits. |
| `power_play_goals` | double | Power-play goals. |
| `sog` | double | Shots on goal from the area. |
| `faceoff_winning_pctg` | double | Faceoff win percentage. |
| `toi` | character | Time on ice. |
| `blocked_shots` | double | Blocked shots. |
| `shifts` | double | Number of shifts. |
| `giveaways` | double | Giveaways. |
| `takeaways` | double | Takeaways. |
| `name_default` | character | Player name (default localization). |
| `even_strength_shots_against` | character | Even-strength shots against (saves/total). |
| `power_play_shots_against` | character | Power-play shots against (saves/total). |
| `shorthanded_shots_against` | character | Shorthanded shots against (saves/total). |
| `save_shots_against` | character | Total shots against (saves/total). |
| `even_strength_goals_against` | double | Even-strength goals against. |
| `power_play_goals_against` | double | Power-play goals against. |
| `shorthanded_goals_against` | double | Shorthanded goals against. |
| `goals_against` | double | Goals against. |
| `starter` | logical | Whether the goalie started the game. |
| `shots_against` | double | Shots faced. |
| `saves` | double | Saves made. |
| `save_pctg` | double | Save percentage. |
| `decision` | character | Goalie decision (W/L/O). |
| `name_cs` | character | Player name (Czech localization). |
| `name_fi` | character | Player name (Finnish localization). |
| `name_sk` | character | Player name (Slovak localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_boxscore-example}

```python
nhl_boxscore(game_id=2024020001)
```

_Last validated n/a._

## nhl_landing

Pull the gamecenter landing payload for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/landing](https://api-web.nhle.com/v1/gamecenter/2024020001/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_landing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `limited_scoring` | logical | Boolean flag indicating whether the game is subject to a limited-scoring designation (e.g., a low-scoring or shootout-resolved game) per NHL api-web metadata. |
| `game_date` | character | Game date. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `venue_timezone` | character | Venue time zone. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `shootout_in_use` | logical | Boolean flag indicating whether a shootout is in use as the tiebreaker format for this game per NHL api-web game landing metadata. |
| `reg_periods` | integer | Number of regulation periods scheduled for this game (typically 3 for NHL, may differ for special-format games). |
| `ot_in_use` | logical | Boolean flag indicating whether overtime rules are in effect for this game, as determined by the NHL api-web game landing endpoint. |
| `ties_in_use` | logical | Whether ties were in use that season. |
| `venue_default` | character | Venue name (default language). |
| `venue_location_default` | character | Default-language display string for the city or location associated with the game's venue, as provided by the NHL api-web landing endpoint. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `away_team_id` | integer | Away team identifier. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_place_name_default` | character | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_score` | integer | Away team final score. |
| `away_team_sog` | integer | Away team shots on goal. |
| `away_team_logo` | character | URL to the away team logo. |
| `away_team_dark_logo` | character | URL to the away team dark logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_score` | integer | Home team final score. |
| `home_team_sog` | integer | Home team shots on goal. |
| `home_team_logo` | character | URL to the home team logo. |
| `home_team_dark_logo` | character | URL to the home team dark logo. |
| `summary_scoring` | character | Serialized summary of scoring events for the game, flattened from the nested NHL api-web landing payload scoring sub-object. |
| `summary_three_stars` | character | Serialized representation of the three-star selections for the game, flattened from the nested NHL api-web landing payload. |
| `summary_penalties` | character | Serialized summary of penalty events for the game, flattened from the nested NHL api-web landing payload penalties sub-object. |
| `clock_time_remaining` | character | Remaining time in the current period formatted as MM:SS, as provided by the NHL api-web game landing endpoint. |
| `clock_seconds_remaining` | integer | Integer count of seconds remaining in the current period at the time the NHL api-web landing payload was captured. |
| `clock_running` | logical | Boolean flag indicating whether the game clock is actively counting down at the time the NHL api-web landing payload was captured. |
| `clock_in_intermission` | logical | Boolean flag indicating whether the game clock is currently paused during an intermission period between regulation periods. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_landing-example}

```python
nhl_landing(game_id=2024020001)
```

_Last validated n/a._

## nhl_right_rail

Pull the gamecenter right-rail payload (in-game widgets).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail](https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_right_rail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_right_rail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_right_rail-example}

```python
nhl_right_rail(game_id=2024020001)
```

_Last validated n/a._

## nhl_score

Pull the single-day scoreboard for `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/score/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/score/now](https://api-web.nhle.com/v1/score/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `game_center_link` | character | Link to the NHL game center page. |
| `series_url` | character | URL path to the NHL api-web series landing page providing full details about this playoff series matchup. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `venue_timezone` | character | Venue time zone. |
| `period` | integer | Period number. |
| `goals` | character | Goals scored. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character | Default-language full team name for the away team in this game, as provided by the NHL api-web scoreboard endpoint. |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | integer | Away team final score. |
| `away_team_sog` | integer | Away team shots on goal. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character | Default-language full team name for the home team in this game, as provided by the NHL api-web scoreboard endpoint. |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | integer | Home team final score. |
| `home_team_sog` | integer | Home team shots on goal. |
| `home_team_logo` | character | URL to the home team logo. |
| `series_status_round` | integer | Numeric identifier for the playoff round (e.g., 1 = First Round, 2 = Second Round) in which this game is being played. |
| `series_status_series_abbrev` | character | Short abbreviation code identifying the specific playoff series matchup, as provided by the NHL api-web score endpoint. |
| `series_status_series_title` | character | Human-readable title describing the playoff series matchup (e.g., team abbreviations and round name), as returned by the NHL api-web score endpoint. |
| `series_status_series_letter` | character | Single-letter label (e.g., 'A', 'B') assigned to the playoff series by the NHL api-web score endpoint to distinguish simultaneous matchups within a round. |
| `series_status_needed_to_win` | integer | Number of additional wins required by the series leader to clinch the playoff round at the time of this game. |
| `series_status_top_seed_team_abbrev` | character | Three-letter abbreviation for the higher-seeded team in this playoff series, as reported by the NHL api-web score endpoint. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the current playoff series as of this game. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter abbreviation for the lower-seeded team in this playoff series, as reported by the NHL api-web score endpoint. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the current playoff series as of this game. |
| `series_status_game_number_of_series` | integer | Sequential game number within the playoff series (e.g., 1 through 7 for a best-of-seven round). |
| `clock_time_remaining` | character | Remaining time in the current period in MM:SS format, as provided by the NHL api-web score endpoint. |
| `clock_seconds_remaining` | integer | Integer count of seconds remaining in the current period as reported by the NHL api-web score endpoint. |
| `clock_running` | logical | Boolean flag indicating whether the game clock is actively running at the time the NHL api-web score payload was captured. |
| `clock_in_intermission` | logical | Boolean flag indicating whether the game clock is paused during an intermission at the time the NHL api-web score payload was captured. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_score-example}

```python
nhl_score()
```

_Last validated n/a._

## nhl_schedule_calendar

Pull the calendar of game-days for the season.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule-calendar/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule-calendar/now](https://api-web.nhle.com/v1/schedule-calendar/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_schedule_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_schedule_calendar-example}

```python
nhl_schedule_calendar()
```

_Last validated n/a._

## nhl_playoff_series

Pull a single playoff series payload.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/playoff-series/{season}/{series_letter}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule/playoff-series/20242025/a](https://api-web.nhle.com/v1/schedule/playoff-series/20242025/a)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `series_letter` | `series_letter` |  | `Y` |  | series_letter path parameter. |

### Returns {#nhl_playoff_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round` | integer | Shootout round number. |
| `series_letter` | character | Single-letter label identifying this series within its playoff round (e.g., 'A', 'B'), as used by the NHL api-web. |
| `top_seed_team_id` | integer | Unique NHL api-web identifier for the higher-seeded team in this playoff series. |
| `top_seed_team_abbrev` | character | Three-letter abbreviation of the higher-seeded team in this NHL playoff series (e.g., 'TOR'). |
| `bottom_seed_team_id` | integer | Unique NHL api-web identifier for the lower-seeded team in this playoff series. |
| `bottom_seed_team_abbrev` | character | Three-letter abbreviation of the lower-seeded team in this NHL playoff series (e.g., 'BOS'). |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_number` | integer | Game number within the schedule. |
| `if_necessary` | logical | If necessary. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `venue_timezone` | character | Venue time zone. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_center_link` | character | Link to the NHL game center page. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_default` | character | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | integer | Away team final score. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | integer | Home team final score. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in this NHL playoff series to date. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in this NHL playoff series to date. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |
| `game_outcome_ot_periods` | double | Number of overtime periods played in the game that concluded this series or scheduled game, where applicable. |
| `away_team_place_name_fr` | character | Away team place name (French). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_playoff_series-example}

```python
nhl_playoff_series(season=2025, series_letter='a')
```

_Last validated n/a._

## nhl_standings

Pull the NHL standings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/standings/now](https://api-web.nhle.com/v1/standings/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `clinch_indicator` | character | Playoff clinch indicator (e.g. 'x' clinched playoff, 'e' eliminated). |
| `conference_abbrev` | character | Conference abbreviation. |
| `conference_home_sequence` | integer | Team's rank within its conference based solely on home-game results in the NHL api-web standings. |
| `conference_l10_sequence` | integer | Team's rank within its conference based on performance in the last 10 games played, as reported by the NHL api-web standings endpoint. |
| `conference_name` | character | Conference name. |
| `conference_road_sequence` | integer | Team's rank within its conference based solely on road-game results in the NHL api-web standings. |
| `conference_sequence` | integer | Team's seeding position within the conference. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `division_abbrev` | character | Division abbreviation. |
| `division_home_sequence` | integer | Team's rank within its division based solely on home-game results in the NHL api-web standings. |
| `division_l10_sequence` | integer | Team's rank within its division based on performance in the last 10 games played, as reported by the NHL api-web standings endpoint. |
| `division_name` | character | Division name. |
| `division_road_sequence` | integer | Team's rank within its division based solely on road-game results in the NHL api-web standings. |
| `division_sequence` | integer | Team's seeding position within the division. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Matches played. |
| `goal_differential` | integer | Goal differential. |
| `goal_differential_pctg` | double | Team's goal differential normalized to a per-game (or percentage) basis, as published in the NHL standings feed. |
| `goal_against` | integer | Total number of goals allowed by the team across all games played in the current standings snapshot. |
| `goal_for` | integer | Total number of goals scored by the team across all games played in the current standings snapshot. |
| `goals_for_pctg` | double | Team's share of total goals scored in all games involving this team, calculated as goals-for divided by (goals-for + goals-against). |
| `home_games_played` | integer | Number of home games the team has completed in the current season as of this standings snapshot. |
| `home_goal_differential` | integer | Net goal differential (goals-for minus goals-against) accumulated across all home games played in the current season. |
| `home_goals_against` | integer | Total number of goals allowed by the team in home games during the current season. |
| `home_goals_for` | integer | Total number of goals scored by the team in home games during the current season. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | integer | Home overtime losses. |
| `home_points` | integer | Home team total points scored in the game so far. |
| `home_regulation_plus_ot_wins` | integer | Number of home wins achieved in regulation or overtime (excluding shootout decisions) in the current season. |
| `home_regulation_wins` | integer | Number of home wins achieved in regulation time (within 60 minutes) in the current season. |
| `home_ties` | integer | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `l10_games_played` | integer | Number of games included in the team's last-10-games performance window (typically 10, may be lower early in the season). |
| `l10_goal_differential` | integer | Net goal differential (goals-for minus goals-against) across the team's most recent 10 games. |
| `l10_goals_against` | integer | Total goals allowed by the team across its most recent 10 games. |
| `l10_goals_for` | integer | Total goals scored by the team across its most recent 10 games. |
| `l10_losses` | integer | Losses in the last ten games. |
| `l10_ot_losses` | integer | Overtime losses in the last ten games. |
| `l10_points` | integer | Total standings points earned by the team across its most recent 10 games. |
| `l10_regulation_plus_ot_wins` | integer | Number of wins in regulation or overtime (excluding shootouts) within the team's most recent 10 games. |
| `l10_regulation_wins` | integer | Number of regulation-time wins within the team's most recent 10 games. |
| `l10_ties` | integer | Number of tied results recorded within the team's most recent 10 games (applicable to seasons using tie rules). |
| `l10_wins` | integer | Wins in the last ten games. |
| `league_home_sequence` | integer | Team's rank league-wide based solely on home-game results in the NHL api-web standings. |
| `league_l10_sequence` | integer | Team's rank league-wide based on performance in the last 10 games played, as reported by the NHL api-web standings endpoint. |
| `league_road_sequence` | integer | Team's rank league-wide based solely on road-game results in the NHL api-web standings. |
| `league_sequence` | integer | Team's seeding position within the league. |
| `losses` | integer | Number of matches the team has lost. |
| `ot_losses` | integer | Overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Competition points. |
| `regulation_plus_ot_win_pctg` | double | Fraction of games won in regulation or overtime (excluding shootout decisions), used as a tiebreaker metric in NHL standings. |
| `regulation_plus_ot_wins` | integer | Wins in regulation plus overtime. |
| `regulation_win_pctg` | double | Fraction of games won in regulation time only, used as a secondary tiebreaker in NHL standings. |
| `regulation_wins` | integer | Wins in regulation. |
| `road_games_played` | integer | Number of road games the team has completed in the current season as of this standings snapshot. |
| `road_goal_differential` | integer | Net goal differential (goals-for minus goals-against) accumulated across all road games played in the current season. |
| `road_goals_against` | integer | Total number of goals allowed by the team in road games during the current season. |
| `road_goals_for` | integer | Total number of goals scored by the team in road games during the current season. |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | integer | Road overtime losses. |
| `road_points` | integer | Total standings points earned by the team in road games during the current season. |
| `road_regulation_plus_ot_wins` | integer | Number of road wins achieved in regulation or overtime (excluding shootout decisions) in the current season. |
| `road_regulation_wins` | integer | Number of road wins achieved in regulation time (within 60 minutes) in the current season. |
| `road_ties` | integer | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `season_id` | integer | Season identifier. |
| `shootout_losses` | integer | Shootout losses. |
| `shootout_wins` | integer | Shootout wins. |
| `streak_code` | character | Current streak code (W/L/OT). |
| `streak_count` | integer | Length of the current streak. |
| `team_logo` | character | URL to the team logo image. |
| `ties` | integer | Number of matches the team has drawn. |
| `waivers_sequence` | integer | Team's position in the NHL waiver-claim priority order for the current season, determined by reverse standings order. |
| `wildcard_sequence` | integer | Team's wild card seeding position. |
| `win_pctg` | double | Team's overall win percentage (wins divided by games played), as reported by the NHL api-web standings endpoint. |
| `wins` | integer | Number of matches the team has won. |
| `place_name_default` | character | Default-language city or place name associated with the team's market (e.g., "Toronto"), as returned by the NHL api-web standings endpoint. |
| `team_name_default` | character | Team name (default locale). |
| `team_name_fr` | character | Team name (French locale). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_abbrev_default` | character | Default three-letter abbreviation for the team (e.g., "TOR"), as provided by the NHL api-web standings endpoint. |
| `place_name_fr` | character | French-language city or place name associated with the team's market, as returned by the NHL api-web standings endpoint. |
| `team_common_name_fr` | character | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_standings-example}

```python
nhl_standings()
```

_Last validated n/a._

## nhl_standings_season

Pull the per-season standings cutover dates.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings-season`

**Valid URL:** [https://api-web.nhle.com/v1/standings-season](https://api-web.nhle.com/v1/standings-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_standings_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `conferences_in_use` | logical | Whether conferences were in use that season. |
| `divisions_in_use` | logical | Whether divisions were in use that season. |
| `point_for_o_tloss_in_use` | logical | Whether a point for overtime losses was in use. |
| `regulation_wins_in_use` | logical | Whether regulation wins were tracked. |
| `row_in_use` | logical | Whether the regulation/overtime/shootout format was in use. |
| `standings_end` | character | End date of the standings period. |
| `standings_start` | character | Start date of the standings period. |
| `ties_in_use` | logical | Whether ties were in use that season. |
| `wildcard_in_use` | logical | Whether the wild-card playoff format was in use this season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_standings_season-example}

```python
nhl_standings_season()
```

_Last validated n/a._

## nhl_roster

Pull a team's roster.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/roster/TOR/current](https://api-web.nhle.com/v1/roster/TOR/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns {#nhl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group name (e.g. Centers). |
| `id` | integer | Unique player identifier. |
| `headshot` | character | URL to the player headshot image. |
| `sweater_number` | integer | Jersey number. |
| `position_code` | character | Player position code. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `height_in_inches` | integer | Height in inches. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `height_in_centimeters` | integer | Height in centimeters. |
| `weight_in_kilograms` | integer | Weight in kilograms. |
| `birth_date` | character | Player birth date. |
| `birth_country` | character | Player birth country. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `birth_city_default` | character | Birth city (default localization). |
| `birth_state_province_default` | character | Birth state/province (default localization). |
| `birth_city_cs` | character | Birth city (Czech localization). |
| `birth_city_de` | character | Birth city (German localization). |
| `birth_city_fi` | character | Birth city (Finnish localization). |
| `birth_city_sk` | character | Birth city (Slovak localization). |
| `birth_city_sv` | character | Birth city (Swedish localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_roster-example}

```python
nhl_roster(team='TOR')
```

_Last validated n/a._

## nhl_roster_season

Pull every season a team has had on file.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/roster-season/TOR](https://api-web.nhle.com/v1/roster-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns {#nhl_roster_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_roster`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_roster_season-example}

```python
nhl_roster_season(team='TOR')
```

_Last validated n/a._

## nhl_skater_leaders

Pull skater stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/skater-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/skater-stats-leaders/current](https://api-web.nhle.com/v1/skater-stats-leaders/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_skater_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | Stat leader category. |
| `id` | integer | Unique player identifier. |
| `sweater_number` | integer | Jersey number. |
| `headshot` | character | URL to the player headshot image. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `position` | character | Player position. |
| `value` | integer | Leader stat numeric value. |
| `first_name_default` | character | Player first name (default language). |
| `first_name_cs` | character | Player first name (Czech localization). |
| `first_name_de` | character | Player first name (German). |
| `first_name_es` | character | Player first name (Spanish). |
| `first_name_fi` | character | Player first name (Finnish). |
| `first_name_sk` | character | Player first name (Slovak localization). |
| `first_name_sv` | character | Player first name (Swedish). |
| `last_name_default` | character | Player last name (default language). |
| `team_name_default` | character | Team name (default locale). |
| `last_name_cs` | character | Player last name (Czech localization). |
| `last_name_fi` | character | Player last name (Finnish localization). |
| `last_name_sk` | character | Player last name (Slovak localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_skater_leaders-example}

```python
nhl_skater_leaders()
```

_Last validated n/a._

## nhl_goalie_leaders

Pull goalie stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/goalie-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/goalie-stats-leaders/current](https://api-web.nhle.com/v1/goalie-stats-leaders/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_goalie_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | Stat leader category. |
| `id` | integer | Unique player identifier. |
| `sweater_number` | integer | Jersey number. |
| `headshot` | character | URL to the player headshot image. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `position` | character | Player position. |
| `value` | integer | Leader stat numeric value. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `team_name_default` | character | Team name (default locale). |
| `first_name_cs` | character | Player first name (Czech localization). |
| `first_name_sk` | character | Player first name (Slovak localization). |
| `last_name_cs` | character | Player last name (Czech localization). |
| `last_name_sk` | character | Player last name (Slovak localization). |
| `last_name_fi` | character | Player last name (Finnish localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_goalie_leaders-example}

```python
nhl_goalie_leaders()
```

_Last validated n/a._
