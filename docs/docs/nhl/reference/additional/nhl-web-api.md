---
title: "NHL — additional Python functions — NHL Web API"
sidebar_label: "NHL Web API"
sidebar_position: 3
description: "NHL — additional Python functions — NHL Web API — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — NHL Web API

### nhl_scoreboard {#nhl_scoreboard}

`nhl_scoreboard(date: 'Optional[str]' = None, team: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Dict'`

In-game scoreboard payload (renamed from `nhl_web_scoreboard`).

Picks among three mutually-exclusive NHL api-web forms (kept hand-written
because the URL-builder codegen can't represent the 3-way branch):

* `GET /v1/scoreboard/{team}/now` -- team-scoped now (when `team` set),
* `GET /v1/scoreboard/{date}` -- league-wide on a date,
* `GET /v1/scoreboard/now` -- league-wide now (both args None).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `YYYY-MM-DD`; `None` -> `/now`. Mutually exclusive with `team`. |
| `team` | `Optional[str]` | `None` | 3-letter abbreviation; takes precedence over `date`. |
| `return_parsed` | `bool` | `True` | dispatch the raw payload through `parse_nhl_web_scoreboard`. |
| `return_as_pandas` | `bool` | `False` | with `return_parsed`, return pandas instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `scoreboard_date` | character | Calendar date (YYYY-MM-DD) for which this scoreboard snapshot was retrieved from the NHL api-web feed. |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_center_link` | character | Link to the NHL game center page. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tickets_link` | character | URL to the English-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `tickets_link_fr` | character | URL to the French-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `period` | double | Period number. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character | Full English-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_name_fr` | character | Full French-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | double | Away team final score. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character | Full English-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_name_fr` | character | Full French-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | double | Home team final score. |
| `home_team_logo` | character | URL to the home team logo. |
| `period_descriptor_number` | double | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | double | Maximum number of regulation periods. |
| `series_status_round` | integer | Playoff round number for this game's series (1 = first round, 2 = second round, etc.). |
| `series_status_series_abbrev` | character | Short abbreviation string identifying the specific playoff series matchup (e.g., 'A1' for a particular bracket slot). |
| `series_status_game` | integer | Game number within the current playoff series (e.g., 1 through 7) for the game represented in this scoreboard row. |
| `series_status_top_seed_team_abbrev` | character | Three-letter abbreviation for the higher-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the current playoff series as of this scoreboard snapshot. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter abbreviation for the lower-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the current playoff series as of this scoreboard snapshot. |
| `period_descriptor_ot_periods` | double | Number of overtime periods played when the game extended beyond regulation, as reported in the scoreboard period descriptor. |
| `away_team_record` | character |  |
| `home_team_record` | character |  |
| `away_team_common_name_fr` | character | Away team common name (French). |
| `home_team_common_name_fr` | character | Home team common name (French). |
| `away_team_sog` | double | Away team shots on goal. |
| `home_team_sog` | double | Home team shots on goal. |
| `clock_time_remaining` | character |  |
| `clock_seconds_remaining` | double |  |
| `clock_running` | logical |  |
| `clock_in_intermission` | logical |  |

**Example**

```python
nhl_scoreboard(date="2024-03-01")
```
