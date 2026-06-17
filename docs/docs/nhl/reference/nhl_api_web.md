---
title: NHL — NHL Web API
sidebar_label: NHL Web API
sidebar_position: 10
---
# NHL — NHL Web API

`sportsdataverse.nhl` — 27 endpoints.

## `nhl_web_pbp`

Pull the play-by-play feed for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play](https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `event_id` | integer | ESPN event id (echoed from arg). |
| `time_in_period` | character | Time elapsed in the period when the shot occurred. |
| `time_remaining` | character | Time remaining. |
| `situation_code` | character | Code identifying the game situation. |
| `home_team_defending_side` | character | Ice end ('left' or 'right') that the home team is defending in the current period, used to orient x/y coordinates in the NHL api-web play-by-play feed. |
| `type_code` | integer | Numeric event-type code identifying the category of play (e.g., goal, shot, hit, penalty, faceoff) in the NHL api-web play-by-play feed. |
| `type_desc_key` | character | String key describing the event type category (e.g., 'goal', 'shot-on-goal', 'hit', 'faceoff') in the NHL api-web play-by-play feed. |
| `sort_order` | integer | Display sort order for the sport. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `details_event_owner_team_id` | double | NHL api-web team identifier for the team credited with or responsible for the play event in the play-by-play feed. |
| `details_losing_player_id` | double | NHL api-web player identifier for the skater who lost the faceoff on a faceoff event in the play-by-play feed. |
| `details_winning_player_id` | double | NHL api-web player identifier for the skater who won the faceoff on a faceoff event in the play-by-play feed. |
| `details_x_coord` | double | Horizontal rink coordinate (feet from centre ice, positive toward right side) of the play event location in the NHL api-web play-by-play feed. |
| `details_y_coord` | double | Vertical rink coordinate (feet from centre ice, positive toward one end) of the play event location in the NHL api-web play-by-play feed. |
| `details_zone_code` | character | Ice zone where the play event occurred, coded as 'O' (offensive), 'D' (defensive), or 'N' (neutral) relative to the event owner team in the play-by-play feed. |
| `details_shot_type` | character | Classification of the shot attempt (e.g., wrist shot, slap shot, backhand, deflection) as provided in the NHL api-web play-by-play details. |
| `details_shooting_player_id` | double | NHL api-web player identifier for the skater who took the shot on a shot-on-goal, missed-shot, or blocked-shot event in the play-by-play feed. |
| `details_goalie_in_net_id` | double | NHL api-web player identifier for the goaltender who was in the net at the time of the shot, goal, or missed-shot event in the play-by-play feed. |
| `details_away_sog` | double | Cumulative shots on goal by the away team at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_home_sog` | double | Cumulative shots on goal by the home team at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_reason` | character | Primary reason or description for the play event (e.g., specific penalty infraction name) as provided in the NHL api-web play-by-play details. |
| `details_blocking_player_id` | double | NHL api-web player identifier for the skater who blocked a shot on the blocked-shot event in the play-by-play feed. |
| `details_hitting_player_id` | double | NHL api-web player identifier for the skater who delivered the body check on a hit event in the play-by-play feed. |
| `details_hittee_player_id` | double | NHL api-web player identifier for the skater who received the body check on a hit event in the play-by-play feed. |
| `details_player_id` | double | NHL api-web player identifier for the primary player involved in the play event (used on giveaway, takeaway, and similar single-player events). |
| `details_type_code` | character | Structured sub-type code providing additional classification within the play event category in the NHL api-web play-by-play feed. |
| `details_desc_key` | character | Short descriptor key providing additional classification of the play event (e.g., penalty type or shot outcome) in the NHL api-web play-by-play feed. |
| `details_duration` | double | Duration of the penalty in minutes as specified in the play event details of the NHL api-web play-by-play feed. |
| `details_committed_by_player_id` | double | NHL api-web player identifier for the player who committed the infraction on a penalty event in the play-by-play feed. |
| `details_drawn_by_player_id` | double | NHL api-web player identifier for the player who drew (was the victim of) the penalty on a penalty event in the play-by-play feed. |
| `ppt_replay_url` | character | URL to the power-play tracking replay video associated with this play event in the NHL api-web play-by-play feed. |
| `details_scoring_player_id` | double | NHL api-web player identifier for the skater who scored the goal on a goal event in the play-by-play feed. |
| `details_scoring_player_total` | double | Running season goal total for the scoring player at the time of the goal event in the NHL api-web play-by-play feed. |
| `details_assist1_player_id` | double | NHL api-web player identifier for the primary (first) assist credited on a goal event in the play-by-play feed. |
| `details_assist1_player_total` | double | Running season assist total for the primary assist player at the time of the goal event in the play-by-play feed. |
| `details_assist2_player_id` | double | NHL api-web player identifier for the secondary (second) assist credited on a goal event in the play-by-play feed. |
| `details_assist2_player_total` | double | Running season assist total for the secondary assist player at the time of the goal event in the play-by-play feed. |
| `details_away_score` | double | Cumulative away-team score at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_home_score` | double | Cumulative home-team score at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_highlight_clip_sharing_url` | character | Public sharing URL for the English-language broadcast highlight clip of this play event from the NHL api-web play-by-play feed. |
| `details_highlight_clip` | double | NHL api-web identifier for the broadcast highlight clip associated with this play event in the play-by-play feed (English feed). |
| `details_discrete_clip` | double | NHL api-web clip identifier for the discrete video clip of this play event in the play-by-play feed (English feed). |
| `details_discrete_clip_fr` | double | NHL api-web clip identifier for the discrete video clip of this play event in the play-by-play feed (French feed). |
| `details_highlight_clip_sharing_url_fr` | character | Public sharing URL for the French-language broadcast highlight clip of this play event from the NHL api-web play-by-play feed. |
| `details_highlight_clip_fr` | double | NHL api-web identifier for the broadcast highlight clip associated with this play event in the play-by-play feed (French feed). |
| `details_secondary_reason` | character | Secondary descriptive reason or sub-classification for the play event as provided in the NHL api-web play-by-play details (e.g., penalty sub-type). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_web_pbp(game_id=2024020001)
```

_Last validated n/a._

## `nhl_boxscore`

Pull the boxscore for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore](https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

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

### Example

```python
nhl_boxscore(game_id=2024020001)
```

_Last validated n/a._

## `nhl_landing`

Pull the gamecenter landing payload for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/landing](https://api-web.nhle.com/v1/gamecenter/2024020001/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

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

### Example

```python
nhl_landing(game_id=2024020001)
```

_Last validated n/a._

## `nhl_right_rail`

Pull the gamecenter right-rail payload (in-game widgets).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail](https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_right_rail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_right_rail(game_id=2024020001)
```

_Last validated n/a._

## `nhl_web_schedule`

Pull the week-of NHL schedule rooted at `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule](https://api-web.nhle.com/v1/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `schedule_date` | character | Calendar date of the game in YYYY-MM-DD format as returned by the NHL api-web schedule endpoint. |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `venue_timezone` | character | Venue time zone. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `series_url` | character | NHL api-web URL path to the dedicated page for the playoff series associated with this scheduled game. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `game_center_link` | character | Link to the NHL game center page. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_default` | character | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_logo` | character | URL to the away team logo. |
| `away_team_dark_logo` | character | URL to the away team dark logo. |
| `away_team_away_split_squad` | logical | Whether the away team is a split squad. |
| `away_team_score` | integer | Away team final score. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_logo` | character | URL to the home team logo. |
| `home_team_dark_logo` | character | URL to the home team dark logo. |
| `home_team_home_split_squad` | logical | Whether the home team is a split squad. |
| `home_team_score` | integer | Home team final score. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |
| `winning_goalie_player_id` | integer | Winning goalie player identifier. |
| `winning_goalie_first_initial_default` | character | Winning goalie first initial (default language). |
| `winning_goalie_last_name_default` | character | Winning goalie last name (default language). |
| `winning_goalie_last_name_cs` | character | Winning goalie last name (Czech). |
| `winning_goalie_last_name_fi` | character | Winning goalie last name (Finnish). |
| `winning_goalie_last_name_sk` | character | Winning goalie last name (Slovak). |
| `winning_goal_scorer_player_id` | integer | Winning goal scorer player identifier. |
| `winning_goal_scorer_first_initial_default` | character | Winning goal scorer first initial (default). |
| `winning_goal_scorer_last_name_default` | character | Winning goal scorer last name (default language). |
| `series_status_round` | integer | Playoff round number (1 through 4) for the series containing this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_series_abbrev` | character | Short abbreviation identifying the specific playoff series slot within the bracket for this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_series_title` | character | Human-readable display title for the playoff series containing this scheduled game (e.g., 'Eastern Conference Second Round'). |
| `series_status_series_letter` | character | Single-letter label assigned to the playoff series in the bracket structure for this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_needed_to_win` | integer | Number of additional wins needed by the series leader to clinch and advance at the time of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_top_seed_team_abbrev` | character | Three-letter team abbreviation for the higher-seeded team in the playoff series associated with this scheduled game. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the playoff series as of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter team abbreviation for the lower-seeded team in the playoff series associated with this scheduled game. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the playoff series as of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_game_number_of_series` | integer | Sequential game number within the playoff series for this scheduled game (e.g., 1 through 7 for a best-of-seven) from the NHL api-web schedule endpoint. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_web_schedule()
```

_Last validated n/a._

## `nhl_score`

Pull the single-day scoreboard for `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/score/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/score](https://api-web.nhle.com/v1/score)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

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

### Example

```python
nhl_score()
```

_Last validated n/a._

## `nhl_schedule_calendar`

Pull the calendar of game-days for the season.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule-calendar/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule-calendar](https://api-web.nhle.com/v1/schedule-calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_schedule_calendar()
```

_Last validated n/a._

## `nhl_playoff_series`

Pull a single playoff series payload.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/playoff-series/{season}/{series_letter}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule/playoff-series/2025/a](https://api-web.nhle.com/v1/schedule/playoff-series/2025/a)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `series_letter` | `series_letter` |  | `Y` |  | series_letter path parameter. |

### Returns

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

### Example

```python
nhl_playoff_series(season=2025, series_letter='a')
```

_Last validated n/a._

## `nhl_standings`

Pull the NHL standings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/standings](https://api-web.nhle.com/v1/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

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

### Example

```python
nhl_standings()
```

_Last validated n/a._

## `nhl_standings_season`

Pull the per-season standings cutover dates.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings-season`

**Valid URL:** [https://api-web.nhle.com/v1/standings-season](https://api-web.nhle.com/v1/standings-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

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

### Example

```python
nhl_standings_season()
```

_Last validated n/a._

## `nhl_club_schedule_season`

Pull a team's full-season schedule.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule-season/TOR](https://api-web.nhle.com/v1/club-schedule-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `club_previous_season` | integer | Indicator for whether the game belongs to the club's prior completed season (1 = previous season, 0 otherwise). |
| `club_current_season` | integer | Indicator for whether the game falls within the current season for the requesting club (1 = current season, 0 otherwise). |
| `club_next_season` | integer | Indicator for whether the game belongs to the club's next upcoming season (1 = next season, 0 otherwise). |
| `club_timezone` | character | IANA timezone identifier for the home club's arena, used to localise game start times in the schedule. |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
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
| `away_team_logo` | character | URL to the away team logo. |
| `away_team_dark_logo` | character | URL to the away team dark logo. |
| `away_team_away_split_squad` | logical | Whether the away team is a split squad. |
| `away_team_score` | integer | Away team final score. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_logo` | character | URL to the home team logo. |
| `home_team_dark_logo` | character | URL to the home team dark logo. |
| `home_team_home_split_squad` | logical | Whether the home team is a split squad. |
| `home_team_airline_link` | character | Link to home team airline info. |
| `home_team_airline_desc` | character | Home team airline description. |
| `home_team_hotel_link` | character | Link to home team hotel info. |
| `home_team_hotel_desc` | character | Home team hotel description. |
| `home_team_score` | integer | Home team final score. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |
| `winning_goalie_player_id` | integer | Winning goalie player identifier. |
| `winning_goalie_first_initial_default` | character | Winning goalie first initial (default language). |
| `winning_goalie_last_name_default` | character | Winning goalie last name (default language). |
| `away_team_airline_link` | character | Link to away team airline info. |
| `away_team_airline_desc` | character | Away team airline description. |
| `winning_goal_scorer_player_id` | double | Winning goal scorer player identifier. |
| `winning_goal_scorer_first_initial_default` | character | Winning goal scorer first initial (default). |
| `winning_goal_scorer_last_name_default` | character | Winning goal scorer last name (default language). |
| `three_min_recap` | character | Link to the three-minute recap. |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `condensed_game` | character | Link to the condensed game video. |
| `venue_es` | character | Venue name (Spanish). |
| `venue_fr` | character | Venue name (French). |
| `special_event_parent_id` | double | NHL api-web identifier for the parent special-event record grouping multiple games under the same marquee event umbrella. |
| `special_event_name_default` | character | English display name for a special promotional or marquee event designation attached to the game (e.g., 'Winter Classic', 'Heritage Classic'). |
| `special_event_name_fr` | character | French display name for a special promotional or marquee event designation attached to the game, used in bilingual NHL communications. |
| `away_team_hotel_link` | character | Link to away team hotel info. |
| `away_team_hotel_desc` | character | Away team hotel description. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `winning_goalie_last_name_cs` | character | Winning goalie last name (Czech). |
| `winning_goalie_last_name_fi` | character | Winning goalie last name (Finnish). |
| `winning_goalie_last_name_sk` | character | Winning goalie last name (Slovak). |
| `away_team_place_name_fr` | character | Away team place name (French). |
| `away_team_common_name_fr` | character | Away team common name (French). |
| `home_team_common_name_fr` | character | Home team common name (French). |
| `series_url` | character | NHL api-web URL path to the dedicated page for the current playoff series associated with this scheduled game. |
| `series_status_round` | double | Playoff round number to which the current series belongs (1 = first round, 4 = Stanley Cup Final). |
| `series_status_series_abbrev` | character | Short abbreviation identifying the specific playoff series slot (e.g., 'A', 'B') within the bracket for this game. |
| `series_status_series_title` | character | Human-readable display title for the playoff series (e.g., 'Eastern Conference First Round'). |
| `series_status_series_letter` | character | Single-letter label assigned to the playoff series in the bracket structure, used to pair teams across rounds. |
| `series_status_needed_to_win` | double | Wins still required by the leading team to clinch and advance in the current playoff series. |
| `series_status_top_seed_wins` | double | Number of wins accumulated by the higher-seeded team in the current playoff series as of this scheduled game. |
| `series_status_bottom_seed_wins` | double | Number of wins accumulated by the lower-seeded team in the current playoff series as of this scheduled game. |
| `series_status_game_number_of_series` | double | Sequential game number within the playoff series (e.g., 1 through 7 for a best-of-seven). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_club_schedule_season(team='TOR')
```

_Last validated n/a._

## `nhl_club_schedule_month`

Pull a team's schedule for one month.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/month/{month}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/month](https://api-web.nhle.com/v1/club-schedule/TOR/month)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `month` | `month` |  |  | `Y` | month path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_club_schedule_month(team='TOR')
```

_Last validated n/a._

## `nhl_club_schedule_week`

Pull a team's schedule for one week.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/week/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/week](https://api-web.nhle.com/v1/club-schedule/TOR/week)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_club_schedule_week(team='TOR')
```

_Last validated n/a._

## `nhl_club_stats`

Pull a team's season stat block.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats/{team}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats/TOR](https://api-web.nhle.com/v1/club-stats/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_stats`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_club_stats(team='TOR')
```

_Last validated n/a._

## `nhl_club_stats_season`

Pull the seasons a team has stats for.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats-season/TOR](https://api-web.nhle.com/v1/club-stats-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_stats`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_club_stats_season(team='TOR')
```

_Last validated n/a._

## `nhl_roster`

Pull a team's roster.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/roster/TOR](https://api-web.nhle.com/v1/roster/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns

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

### Example

```python
nhl_roster(team='TOR')
```

_Last validated n/a._

## `nhl_roster_season`

Pull every season a team has had on file.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/roster-season/TOR](https://api-web.nhle.com/v1/roster-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_roster`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_roster_season(team='TOR')
```

_Last validated n/a._

## `nhl_player_landing`

Pull the player profile / overview.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/landing](https://api-web.nhle.com/v1/player/8480801/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `is_active` | logical | Whether the team is active. |
| `current_team_id` | integer | Player's current team identifier. |
| `current_team_abbrev` | character | Three-letter abbreviation of the NHL team the player is currently rostered on (e.g., 'TOR', 'BOS'). |
| `badges` | character | Serialized list of achievement or milestone badges displayed on the player's NHL api-web profile page. |
| `team_logo` | character | URL to the team logo image. |
| `sweater_number` | integer | Jersey number. |
| `position` | character | Player position. |
| `headshot` | character | URL to the player headshot image. |
| `hero_image` | character | URL to the large hero/banner image of the player displayed at the top of their NHL api-web profile page. |
| `height_in_inches` | integer | Height in inches. |
| `height_in_centimeters` | integer | Height in centimeters. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `weight_in_kilograms` | integer | Weight in kilograms. |
| `birth_date` | character | Player birth date. |
| `birth_country` | character | Player birth country. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `player_slug` | character | URL slug for the player. |
| `in_top100_all_time` | integer | Flag (1/0) indicating whether the player is recognized among the NHL's Top 100 all-time greatest players. |
| `in_hhof` | integer | Flag (1/0) indicating whether the player has been inducted into the Hockey Hall of Fame. |
| `shop_link` | character | URL to the NHL shop page where merchandise for this player (e.g., jerseys) can be purchased. |
| `twitter_link` | character | URL to the player's official Twitter/X account as listed on their NHL api-web profile. |
| `watch_link` | character | URL to the NHL.tv or league streaming page where the player's games can be watched. |
| `last5_games` | character | Serialized array of stat lines for the player's five most recent NHL games, as returned by the player landing endpoint. |
| `season_totals` | character | Serialized array of per-season stat totals for the player across all regular seasons and playoffs in their NHL career. |
| `awards` | character | Serialized list of NHL awards and honors the player has received, as returned by the NHL api-web player landing endpoint. |
| `current_team_roster` | character | Serialized roster-position metadata for the player's current NHL team assignment, as returned by the player landing endpoint. |
| `full_team_name_default` | character | Full English name of the player's current NHL team (e.g., 'Toronto Maple Leafs'), as returned by the player landing endpoint. |
| `full_team_name_fr` | character | Full French-language name of the player's current NHL team, as returned by the player landing endpoint. |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `birth_city_default` | character | Birth city (default localization). |
| `birth_state_province_default` | character | Birth state/province (default localization). |
| `draft_details_year` | integer | Calendar year in which the player was selected in the NHL Entry Draft. |
| `draft_details_team_abbrev` | character | Three-letter abbreviation of the NHL team that drafted the player in the Entry Draft. |
| `draft_details_round` | integer | Round number in which the player was selected during the NHL Entry Draft. |
| `draft_details_pick_in_round` | integer | Pick number within the player's draft round in the NHL Entry Draft. |
| `draft_details_overall_pick` | integer | Overall pick number at which the player was selected in the NHL Entry Draft. |
| `featured_stats_season` | integer | Eight-digit NHL season identifier (e.g., 20232024) indicating which season the featured stats on the player's profile correspond to. |
| `featured_stats_regular_season_sub_season_assists` | integer | Assists recorded by the player in the featured regular-season sub-season (typically the current or most recent season) on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_game_winning_goals` | integer | Game-winning goals recorded by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_games_played` | integer | Games played by the player in the featured regular-season sub-season (typically the current or most recent season) on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_goals` | integer | Goals scored by the player in the featured regular-season sub-season (typically the current or most recent season) on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_ot_goals` | integer | Overtime goals scored by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_pim` | integer | Penalty minutes accumulated by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_plus_minus` | integer | Plus/minus rating for the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_points` | integer | Points (goals + assists) recorded by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_power_play_goals` | integer | Power-play goals scored by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_power_play_points` | integer | Power-play points accumulated by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_shooting_pctg` | double | Shooting percentage for the player in the featured regular-season sub-season on their NHL api-web profile, expressed as a decimal. |
| `featured_stats_regular_season_sub_season_shorthanded_goals` | integer | Shorthanded goals scored by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_shorthanded_points` | integer | Shorthanded points accumulated by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_sub_season_shots` | integer | Shots on goal taken by the player in the featured regular-season sub-season on their NHL api-web profile. |
| `featured_stats_regular_season_career_assists` | integer | Career regular-season assists highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_game_winning_goals` | integer | Career regular-season game-winning goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_games_played` | integer | Career regular-season games played highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_goals` | integer | Career regular-season goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_ot_goals` | integer | Career regular-season overtime goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_pim` | integer | Career regular-season penalty minutes highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_plus_minus` | integer | Career regular-season plus/minus rating highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_points` | integer | Career regular-season points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_power_play_goals` | integer | Career regular-season power-play goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_power_play_points` | integer | Career regular-season power-play points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_shooting_pctg` | double | Career regular-season shooting percentage highlighted on the player's NHL api-web profile, expressed as a decimal. |
| `featured_stats_regular_season_career_shorthanded_goals` | integer | Career regular-season shorthanded goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_shorthanded_points` | integer | Career regular-season shorthanded points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_regular_season_career_shots` | integer | Career regular-season shots on goal highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_sub_season_assists` | integer | Assists recorded by the player in the featured playoff sub-season (typically the most recent postseason) on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_game_winning_goals` | integer | Game-winning goals recorded by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_games_played` | integer | Games played by the player in the featured playoff sub-season (typically the most recent postseason) on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_goals` | integer | Goals scored by the player in the featured playoff sub-season (typically the most recent postseason) on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_ot_goals` | integer | Overtime goals scored by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_pim` | integer | Penalty minutes accumulated by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_plus_minus` | integer | Plus/minus rating for the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_points` | integer | Points (goals + assists) recorded by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_power_play_goals` | integer | Power-play goals scored by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_power_play_points` | integer | Power-play points accumulated by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_shooting_pctg` | double | Shooting percentage for the player in the featured playoff sub-season on their NHL api-web profile, expressed as a decimal. |
| `featured_stats_playoffs_sub_season_shorthanded_goals` | integer | Shorthanded goals scored by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_shorthanded_points` | integer | Shorthanded points accumulated by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_sub_season_shots` | integer | Shots on goal taken by the player in the featured playoff sub-season on their NHL api-web profile. |
| `featured_stats_playoffs_career_assists` | integer | Career playoff assists highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_game_winning_goals` | integer | Career playoff game-winning goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_games_played` | integer | Career playoff games played highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_goals` | integer | Career playoff goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_ot_goals` | integer | Career playoff overtime goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_pim` | integer | Career playoff penalty minutes highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_plus_minus` | integer | Career playoff plus/minus rating highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_points` | integer | Career playoff points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_power_play_goals` | integer | Career playoff power-play goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_power_play_points` | integer | Career playoff power-play points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_shooting_pctg` | double | Career playoff shooting percentage highlighted on the player's NHL api-web profile, expressed as a decimal. |
| `featured_stats_playoffs_career_shorthanded_goals` | integer | Career playoff shorthanded goals highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_shorthanded_points` | integer | Career playoff shorthanded points highlighted on the player's NHL api-web profile for the featured season context. |
| `featured_stats_playoffs_career_shots` | integer | Career playoff shots on goal highlighted on the player's NHL api-web profile for the featured season context. |
| `career_totals_regular_season_assists` | integer | Career cumulative assists recorded by the player across all NHL regular-season games. |
| `career_totals_regular_season_avg_toi` | character | Career average time on ice per game in the NHL regular season, expressed as an MM:SS string. |
| `career_totals_regular_season_faceoff_winning_pctg` | double | Career faceoff win percentage for the player across all NHL regular-season games. |
| `career_totals_regular_season_game_winning_goals` | integer | Career total of game-winning goals the player has scored in NHL regular-season games. |
| `career_totals_regular_season_games_played` | integer | Total number of NHL regular-season games the player has appeared in across their career. |
| `career_totals_regular_season_goals` | integer | Career total goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_ot_goals` | integer | Career total overtime goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_pim` | integer | Career total penalty minutes accumulated by the player in NHL regular-season games. |
| `career_totals_regular_season_plus_minus` | integer | Career plus/minus rating accumulated by the player across all NHL regular-season games. |
| `career_totals_regular_season_points` | integer | Career total points (goals + assists) accumulated by the player in NHL regular-season games. |
| `career_totals_regular_season_power_play_goals` | integer | Career total power-play goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_power_play_points` | integer | Career total power-play points (goals + assists on the power play) in NHL regular-season games. |
| `career_totals_regular_season_shooting_pctg` | double | Career shooting percentage for the player in NHL regular-season games, expressed as a decimal. |
| `career_totals_regular_season_shorthanded_goals` | integer | Career total shorthanded goals scored by the player in NHL regular-season games. |
| `career_totals_regular_season_shorthanded_points` | integer | Career total shorthanded points (goals + assists while shorthanded) in NHL regular-season games. |
| `career_totals_regular_season_shots` | integer | Career total shots on goal taken by the player in NHL regular-season games. |
| `career_totals_playoffs_assists` | integer | Career cumulative assists recorded by the player across all NHL playoff appearances. |
| `career_totals_playoffs_avg_toi` | character | Career average time on ice per game in NHL playoff play, expressed as an MM:SS string. |
| `career_totals_playoffs_faceoff_winning_pctg` | double | Career faceoff win percentage for the player across all NHL playoff games. |
| `career_totals_playoffs_game_winning_goals` | integer | Career total of game-winning goals the player has scored in NHL playoff games. |
| `career_totals_playoffs_games_played` | integer | Total number of NHL playoff games the player has appeared in across their career. |
| `career_totals_playoffs_goals` | integer | Career total goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_ot_goals` | integer | Career total overtime goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_pim` | integer | Career total penalty minutes accumulated by the player in NHL playoff games. |
| `career_totals_playoffs_plus_minus` | integer | Career plus/minus rating accumulated by the player across all NHL playoff games. |
| `career_totals_playoffs_points` | integer | Career total points (goals + assists) accumulated by the player in NHL playoff games. |
| `career_totals_playoffs_power_play_goals` | integer | Career total power-play goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_power_play_points` | integer | Career total power-play points (goals + assists on the power play) in NHL playoff games. |
| `career_totals_playoffs_shooting_pctg` | double | Career shooting percentage for the player in NHL playoff games, expressed as a decimal. |
| `career_totals_playoffs_shorthanded_goals` | integer | Career total shorthanded goals scored by the player in NHL playoff games. |
| `career_totals_playoffs_shorthanded_points` | integer | Career total shorthanded points (goals + assists while shorthanded) in NHL playoff games. |
| `career_totals_playoffs_shots` | integer | Career total shots on goal taken by the player in NHL playoff games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_player_landing(player_id=8480801)
```

_Last validated n/a._

## `nhl_player_game_log`

Pull a player's game-by-game log.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/game-log/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/game-log](https://api-web.nhle.com/v1/player/8480801/game-log)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `home_road_flag` | character | Home or road indicator. |
| `game_date` | character | Game date. |
| `goals` | integer | Goals scored. |
| `assists` | integer | Assists. |
| `points` | integer | Total points (goals + assists). |
| `plus_minus` | integer | Plus/minus rating. |
| `power_play_goals` | integer | Power-play goals. |
| `power_play_points` | integer | Power play points. |
| `game_winning_goals` | integer | Game-winning goals. |
| `ot_goals` | integer | Overtime goals. |
| `shots` | integer | Shots on goal. |
| `shifts` | integer | Number of shifts. |
| `shorthanded_goals` | integer | Shorthanded goals. |
| `shorthanded_points` | integer | Shorthanded points. |
| `opponent_abbrev` | character | Opponent team abbreviation. |
| `pim` | integer | Penalty minutes. |
| `toi` | character | Time on ice. |
| `common_name_default` | character | Player's team common name. |
| `opponent_common_name_default` | character | Opponent team common name. |
| `opponent_common_name_fr` | character | French-language common name of the opposing team in the player's individual game log entry from the NHL api-web feed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_player_game_log(player_id=8480801)
```

_Last validated n/a._

## `nhl_player_spotlight`

Pull the league's currently featured players.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player-spotlight`

**Valid URL:** [https://api-web.nhle.com/v1/player-spotlight](https://api-web.nhle.com/v1/player-spotlight)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_slug` | character | URL slug for the player. |
| `position` | character | Player position. |
| `sweater_number` | integer | Jersey number. |
| `team_id` | integer | Unique team identifier. |
| `headshot` | character | URL to the player headshot image. |
| `team_tri_code` | character | Team tri-code abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `sort_id` | integer | Sort order identifier for the spotlight. |
| `name_default` | character | Player name (default localization). |
| `name_cs` | character | Player name (Czech localization). |
| `name_fi` | character | Player name (Finnish localization). |
| `name_sk` | character | Player name (Slovak localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_player_spotlight()
```

_Last validated n/a._

## `nhl_skater_leaders`

Pull skater stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/skater-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/skater-stats-leaders](https://api-web.nhle.com/v1/skater-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

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

### Example

```python
nhl_skater_leaders()
```

_Last validated n/a._

## `nhl_goalie_leaders`

Pull goalie stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/goalie-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/goalie-stats-leaders](https://api-web.nhle.com/v1/goalie-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

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

### Example

```python
nhl_goalie_leaders()
```

_Last validated n/a._

## `nhl_draft_picks`

Pull NHL draft picks for a year (and optionally one round).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/{year}/{round_}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/2024](https://api-web.nhle.com/v1/draft/picks/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `round_` | `round_` |  |  | `Y` | round_ path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `round` | integer | Shootout round number. |
| `pick_in_round` | integer | Pick number within the round. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `team_id` | integer | Unique team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo_light` | character | URL to the team logo (light variant). |
| `team_logo_dark` | character | URL to the team logo (dark variant). |
| `team_pick_history` | character | History of the team's picks at this slot. |
| `position_code` | character | Player position code. |
| `country_code` | character | Player country code. |
| `height` | integer | Player height in inches. |
| `weight` | integer | Player weight in pounds. |
| `amateur_league` | character | Amateur league the player played in. |
| `amateur_club_name` | character | Amateur club the player played for. |
| `team_name_default` | character | Team name (default locale). |
| `team_name_fr` | character | Team name (French locale). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `display_abbrev_default` | character | Short display abbreviation for the selected player's nationality or amateur league affiliation shown in the NHL draft picks listing. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `team_common_name_fr` | character | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_draft_picks(year=2024)
```

_Last validated n/a._

## `nhl_draft_rankings`

Pull NHL Central Scouting rankings for a draft year.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/{year}/{category}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/2024](https://api-web.nhle.com/v1/draft/rankings/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `category` | `category` |  |  | `Y` | category path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `draft_year` | integer | Draft year the lottery applies to. |
| `category_id` | integer | Prospect category identifier. |
| `category_key` | character | Machine-readable slug identifying the scouting or ranking category (e.g., 'north-american-skater', 'international-skater') that the prospect belongs to in the NHL draft rankings. |
| `last_name` | character | Player last name. |
| `first_name` | character | Player first name. |
| `position_code` | character | Player position code. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `height_in_inches` | integer | Height in inches. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `last_amateur_club` | character | Prospect's most recent amateur club. |
| `last_amateur_league` | character | Prospect's most recent amateur league. |
| `birth_date` | character | Player birth date. |
| `birth_city` | character | Birth city. |
| `birth_state_province` | character | Birth state or province of the player. |
| `birth_country` | character | Player birth country. |
| `midterm_rank` | double | Prospect's midterm draft ranking. |
| `final_rank` | double | Prospect's final draft ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_draft_rankings(year=2024)
```

_Last validated n/a._

## `nhl_draft_picks_now`

Pull the current / most recent draft pick set.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/now](https://api-web.nhle.com/v1/draft/picks/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_draft_picks`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_draft_picks_now()
```

_Last validated n/a._

## `nhl_draft_rankings_now`

Pull the current Central Scouting rankings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/now](https://api-web.nhle.com/v1/draft/rankings/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `draft_year` | integer | Draft year the lottery applies to. |
| `category_id` | integer | Prospect category identifier. |
| `category_key` | character | Short identifier string for the scouting category or ranking list under which the prospect is evaluated (e.g., 'NA-SKATER', 'GOALIE'). |
| `last_name` | character | Player last name. |
| `first_name` | character | Player first name. |
| `position_code` | character | Player position code. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `height_in_inches` | integer | Height in inches. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `last_amateur_club` | character | Prospect's most recent amateur club. |
| `last_amateur_league` | character | Prospect's most recent amateur league. |
| `birth_date` | character | Player birth date. |
| `birth_city` | character | Birth city. |
| `birth_state_province` | character | Birth state or province of the player. |
| `birth_country` | character | Player birth country. |
| `midterm_rank` | double | Prospect's midterm draft ranking. |
| `final_rank` | double | Prospect's final draft ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_draft_rankings_now()
```

_Last validated n/a._

## `nhl_draft_tracker_picks_now`

Pull the live draft-tracker pick list (during the draft itself).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft-tracker/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft-tracker/picks/now](https://api-web.nhle.com/v1/draft-tracker/picks/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_draft_picks`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_draft_tracker_picks_now()
```

_Last validated n/a._
