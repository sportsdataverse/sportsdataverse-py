# NHL — NHL Web API — Club

> NHL — NHL Web API — Club — function reference in sdv-py, the SportsDataverse Python package.

## nhl_club_schedule_season

Pull a team's full-season schedule.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule-season/TOR/now](https://api-web.nhle.com/v1/club-schedule-season/TOR/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns {#nhl_club_schedule_season-returns}

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

### Example {#nhl_club_schedule_season-example}

```python
nhl_club_schedule_season(team='TOR')
```

_Last validated n/a._

## nhl_club_schedule_month

Pull a team's schedule for one month.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/month/{month}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/month/now](https://api-web.nhle.com/v1/club-schedule/TOR/month/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `month` | `month` |  |  | `Y` | month path parameter. |

### Returns {#nhl_club_schedule_month-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_club_schedule_month-example}

```python
nhl_club_schedule_month(team='TOR')
```

_Last validated n/a._

## nhl_club_schedule_week

Pull a team's schedule for one week.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/week/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/week/now](https://api-web.nhle.com/v1/club-schedule/TOR/week/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_club_schedule_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_club_schedule_week-example}

```python
nhl_club_schedule_week(team='TOR')
```

_Last validated n/a._

## nhl_club_stats

Pull a team's season stat block.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats/{team}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats/TOR/now](https://api-web.nhle.com/v1/club-stats/TOR/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_club_stats-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**skaters**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `headshot` | character | URL to the player headshot image. |
| `position_code` | character | Player position code. |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `assists` | integer | Assists. |
| `points` | integer | Total points (goals + assists). |
| `plus_minus` | integer | Plus/minus rating. |
| `penalty_minutes` | integer | Penalty minutes. |
| `power_play_goals` | integer | Power-play goals. |
| `shorthanded_goals` | integer | Shorthanded goals. |
| `game_winning_goals` | integer | Game-winning goals. |
| `overtime_goals` | integer | Overtime goals. |
| `shots` | integer | Shots on goal. |
| `shooting_pctg` | double | Shooting percentage from the area. |
| `avg_time_on_ice_per_game` | double | Average time on ice per game. |
| `avg_shifts_per_game` | double | Average shifts per game. |
| `faceoff_win_pctg` | double | Faceoff win percentage. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |

**goalies**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `headshot` | character | URL to the player headshot image. |
| `games_played` | integer | Games played. |
| `games_started` | integer | Games started (goalies). |
| `wins` | integer | Wins. |
| `losses` | integer | Losses. |
| `overtime_losses` | integer | Total overtime losses. |
| `goals_against_average` | double | Goals against average. |
| `save_percentage` | double | Save percentage (goalies). |
| `shots_against` | integer | Shots faced. |
| `saves` | integer | Saves made. |
| `goals_against` | integer | Goals against. |
| `shutouts` | integer | Shutouts recorded. |
| `goals` | integer | Goals scored. |
| `assists` | integer | Assists. |
| `points` | integer | Total points (goals + assists). |
| `penalty_minutes` | integer | Penalty minutes. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_club_stats-example}

```python
nhl_club_stats(team='TOR')
```

_Last validated n/a._

## nhl_club_stats_season

Pull the seasons a team has stats for.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats-season/TOR](https://api-web.nhle.com/v1/club-stats-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns {#nhl_club_stats_season-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (parser: `parse_nhl_web_club_stats`); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_club_stats_season-example}

```python
nhl_club_stats_season(team='TOR')
```

_Last validated n/a._
