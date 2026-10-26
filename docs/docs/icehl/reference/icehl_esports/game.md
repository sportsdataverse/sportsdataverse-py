---
title: "ICE Hockey League — ICE Hockey League data feed (eSports hokejovyzapis S3 bucket, Austria) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "ICE Hockey League — ICE Hockey League data feed (eSports hokejovyzapis S3 bucket, Austria) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ICE Hockey League — ICE Hockey League data feed (eSports hokejovyzapis S3 bucket, Austria) — Game

## icehl_game_info

Game info of one match: state, arena, attendance, officials, coaches, period scores.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/matches/{match_id}/game-header.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/matches/8161/game-header.json](https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/matches/8161/game-header.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in icehl_schedule). Not the league website's game id. |

### Returns {#icehl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `actual_time_name` | String | Display label of the game state, e.g. `end` / `Ende`. |
| `actual_time_alias` | String | Short game-state code: `K` = finished. |
| `status` | String | Match status: `BEFORE_MATCH`, `LIVE`, `AFTER_MATCH`. |
| `stadium` | String | Name of the arena the game was played in. |
| `number_of_viewers` | Int64 | Spectators at the game (attendance). |
| `referees_head_referee1` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_head_referee2` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_line_referee1` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_line_referee2` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_head_referee1number` | String | Official's sweater number; null when not recorded. |
| `referees_head_referee2number` | String | Official's sweater number; null when not recorded. |
| `referees_line_referee1number` | String | Official's sweater number; null when not recorded. |
| `referees_line_referee2number` | String | Official's sweater number; null when not recorded. |
| `team_info_home_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_home_name` | String | Team display name as the feed prints it. |
| `team_info_home_shortcut` | String | Three-letter team code. |
| `team_info_visitor_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_visitor_name` | String | Team display name as the feed prints it. |
| `team_info_visitor_shortcut` | String | Three-letter team code. |
| `trainers_visitor_head_coach` | String | Head coach (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `trainers_home_head_coach` | String | Head coach (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `team_bilance_home_win` | Int64 | Wins of that team in the head-to-head record (unverified). |
| `team_bilance_visitor_win` | Int64 | Wins of that team in the head-to-head record (unverified). |
| `results_extra_time` | Boolean | True when the game went to overtime. |
| `results_shooting` | Boolean | True when the game was decided by a shootout. |
| `results_contumacy` | Boolean | True when the result was awarded by forfeit. |
| `results_score_final_score_home` | Int64 | Home team goals in that span. |
| `results_score_final_score_guest` | Int64 | Visiting team goals in that span. |
| `results_score_first_period_score_home` | Int64 | Home team goals in that span. |
| `results_score_first_period_score_guest` | Int64 | Visiting team goals in that span. |
| `results_score_second_period_score_home` | Int64 | Home team goals in that span. |
| `results_score_second_period_score_guest` | Int64 | Visiting team goals in that span. |
| `results_score_third_period_score_home` | Int64 | Home team goals in that span. |
| `results_score_third_period_score_guest` | Int64 | Visiting team goals in that span. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#icehl_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#icehl_game_info-example}

```python
icehl_game_info(match_id=8161)
```

## icehl_game_center

Game-center bundle of one match as one row: game data, team stats, goal and penalty lists.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/widget/esports/match/{match_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/widget/esports/match/8161.json](https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/widget/esports/match/8161.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in icehl_schedule). Not the league website's game id. |

### Returns {#icehl_game_center-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `status_id` | Int64 | Widget API status code, a code and not a joinable id: `1` = OK. |
| `status_msg` | String | Widget API status text, `Ok`. |
| `data_game_data_id` | Int64 | Match id (match key space, per bucket); joins to `league-matches` `matches[].id` and the `{match_id}` path param. |
| `data_game_data_division_id` | Int64 | League part id under the widget's name `divisionId` (league-part key space, per bucket: one stage of a competition, e.g. ICEHL 56 = 2026/27 regular season); joins to `league_part_id` in league-matches and the `{league_part_id}` path param. |
| `data_game_data_division_longname` | String | League part name, e.g. `ICEHL 2026/27`. |
| `data_game_data_division_shortname` | String | League part name, e.g. `ICEHL 2026/27`. |
| `data_game_data_scheduled_date_formatted_short` | String | Short display text `DD.MM.YY` (plus `HH:MM` for times). |
| `data_game_data_scheduled_date_formatted_long` | String | Long display text, e.g. `18.September. 2026`. |
| `data_game_data_scheduled_date_timestamp` | Int64 | Epoch milliseconds. |
| `data_game_data_scheduled_date_value` | String | ISO text `YYYY-MM-DD` (plus ` HH:MM` for times). |
| `data_game_data_scheduled_date_diff_to_now` | Int64 | Difference to now as computed by the widget server; `0` in the samples. |
| `data_game_data_scheduled_time` | String | Local time `HH:MM` (scheduled face-off, actual start, actual end). |
| `data_game_data_start_time` | String | Local time `HH:MM` (scheduled face-off, actual start, actual end). |
| `data_game_data_end_time` | String | Local time `HH:MM` (scheduled face-off, actual start, actual end). |
| `data_game_data_is_overtime` | Int64 | Overtime flag sent as an integer, `1` = yes. |
| `data_game_data_is_shoot_out` | Int64 | Shootout flag sent as an integer, `1` = yes. |
| `data_game_data_home_team_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `data_game_data_home_team_longname` | String | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_home_team_shortname` | String | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_home_team_flavourname` | String | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_home_team_score` | Int64 | Final goals of the home or away team (by name). |
| `data_game_data_away_team_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `data_game_data_away_team_longname` | String | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_away_team_shortname` | String | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_away_team_flavourname` | String | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_away_team_score` | Int64 | Final goals of the home or away team (by name). |
| `data_game_data_game_status` | Int64 | Game status code; `4` = finished in the samples (other codes unverified). |
| `data_game_data_game_extended_status` | Int64 | Extended status code; `0` in the samples. |
| `data_game_data_game_extended_status_info_text` | String | Extended status text; empty string in the samples. |
| `data_game_data_game_name` | String | Game number sent as a string (e.g. `"66"`). |
| `data_game_data_game_day` | String | Matchday number sent as a string (`"1"`); null in Denmark. |
| `data_game_data_game_round` | Int64 | Round number; `0` in the samples. |
| `data_game_data_location_longname` | String | Arena name; empty string when not set. |
| `data_game_data_location_shortname` | String | Arena name; empty string when not set. |
| `data_game_data_location_coordinates_lat` | Int64 | Latitude / longitude; `0` placeholder in the samples. |
| `data_game_data_location_coordinates_lng` | Int64 | Latitude / longitude; `0` placeholder in the samples. |
| `data_game_data_attendance` | Int64 | Spectators at the game (attendance). |
| `data_game_data_live_time` | Int64 | Game clock, seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). 3600 at the end of regulation. |
| `data_game_data_live_time_formatted` | String | Clock text; the placeholder `END-GAME` after the game, `m:ss` while live. |
| `data_game_data_live_time_period` | Int64 | Current period number; `4` after the game. |
| `data_game_data_live_time_string` | String | Clock text; the placeholder `END-GAME` after the game, `m:ss` while live. |
| `data_game_data_live_time_game_phase` | String | Game phase text, e.g. `END OF GAME`. |
| `data_game_data_period_stats` | String | Per-period score and shots. |
| `data_game_data_game_officials_ref1_lastname` | String | Last name of that official or coach. |
| `data_game_data_game_officials_ref1_firstname` | String | First name of that official or coach. |
| `data_game_data_game_officials_ref2_lastname` | String | Last name of that official or coach. |
| `data_game_data_game_officials_ref2_firstname` | String | First name of that official or coach. |
| `data_game_data_game_officials_linesman1_lastname` | String | Last name of that official or coach. |
| `data_game_data_game_officials_linesman1_firstname` | String | First name of that official or coach. |
| `data_game_data_game_officials_linesman2_lastname` | String | Last name of that official or coach. |
| `data_game_data_game_officials_linesman2_firstname` | String | First name of that official or coach. |
| `data_game_data_team_officials_headcoachhome_lastname` | String | Last name of that official or coach. |
| `data_game_data_team_officials_headcoachhome_firstname` | String | First name of that official or coach. |
| `data_game_data_team_officials_headcoachaway_lastname` | String | Last name of that official or coach. |
| `data_game_data_team_officials_headcoachaway_firstname` | String | First name of that official or coach. |
| `data_game_data_live_time_period_bar` | String | Period progress bar; null in the samples, so unobserved. |
| `data_game_data_you_tube_link` | String | Video link; null in the samples, so unobserved. |
| `data_game_data_number_of_periods` | Int64 | Regulation periods (3) or the maximum overtimes allowed. |
| `data_game_data_length_of_period` | Int64 | Length of a period or overtime, seconds (1200). |
| `data_game_data_number_of_overtimes` | Int64 | Regulation periods (3) or the maximum overtimes allowed. |
| `data_game_data_length_of_overtime` | Int64 | Length of a period or overtime, seconds (1200). |
| `data_game_data_shootout_shots` | Int64 | Shootout attempts in the game. |
| `data_game_data_images_home_team_logo` | String | Team logo; null in the samples, so unobserved. |
| `data_game_data_images_away_team_logo` | String | Team logo; null in the samples, so unobserved. |
| `data_game_data_streams` | String | Video streams; empty array in the samples. |
| `data_game_data_home_team_logo_url` | String | Team logo URL; null in the samples, so unobserved. |
| `data_game_data_away_team_logo_url` | String | Team logo URL; null in the samples, so unobserved. |
| `data_home_goals` | String | Goals scored by that team. |
| `data_home_penalties` | String | Penalties of that team. |
| `data_home_field_players` | String | Skaters of that team with their game stats. |
| `data_home_goal_keepers` | String | Goalies of that team with their game stats. |
| `data_home_team_stats_faceoffs_won` | Int64 | Face-offs the team won in this game. |
| `data_home_team_stats_faceoffs_total` | Int64 | Face-offs the team took in this game. |
| `data_home_team_stats_shots_on_goal` | Int64 | Team shots that reached the goalie in this game. |
| `data_home_team_stats_shots_blocked` | Int64 | Team blocked-shot count in this game (direction unverified). |
| `data_home_team_stats_penalty_minutes` | Int64 | Penalty minutes assessed to the team in this game. |
| `data_home_team_stats_powerplay_goals` | Int64 | Goals the team scored on the power play in this game. |
| `data_home_team_stats_powerplay_opportunities` | Int64 | Power plays the team had in this game. |
| `data_home_team_stats_penaltykill_successes` | Int64 | Penalties killed without conceding. |
| `data_home_team_stats_penaltykill_situations` | Int64 | Times the team had to kill a penalty in this game. |
| `data_home_team_stats_time_values_leading` | Int64 | Seconds spent leading, tied or trailing (by name). |
| `data_home_team_stats_time_values_tied` | Int64 | Seconds spent leading, tied or trailing (by name). |
| `data_home_team_stats_time_values_trailing` | Int64 | Seconds spent leading, tied or trailing (by name). |
| `data_home_team_stats_time_values_time_game_strength_even` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_plus_one` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_plus_two` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_minus_one` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_minus_two` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_on_powerplay` | Int64 | Seconds on the power play. |
| `data_home_team_stats_time_values_time_while_shorthanded` | Int64 | Seconds the team played shorthanded in this game. |
| `data_away_goals` | String | Goals scored by that team. |
| `data_away_penalties` | String | Penalties of that team. |
| `data_away_field_players` | String | Skaters of that team with their game stats. |
| `data_away_goal_keepers` | String | Goalies of that team with their game stats. |
| `data_away_team_stats_faceoffs_won` | Int64 | Face-offs the team won in this game. |
| `data_away_team_stats_faceoffs_total` | Int64 | Face-offs the team took in this game. |
| `data_away_team_stats_shots_on_goal` | Int64 | Team shots that reached the goalie in this game. |
| `data_away_team_stats_shots_blocked` | Int64 | Team blocked-shot count in this game (direction unverified). |
| `data_away_team_stats_penalty_minutes` | Int64 | Penalty minutes assessed to the team in this game. |
| `data_away_team_stats_powerplay_goals` | Int64 | Goals the team scored on the power play in this game. |
| `data_away_team_stats_powerplay_opportunities` | Int64 | Power plays the team had in this game. |
| `data_away_team_stats_penaltykill_successes` | Int64 | Penalties killed without conceding. |
| `data_away_team_stats_penaltykill_situations` | Int64 | Times the team had to kill a penalty in this game. |
| `data_away_team_stats_time_values_leading` | Int64 | Seconds spent leading, tied or trailing (by name). |
| `data_away_team_stats_time_values_tied` | Int64 | Seconds spent leading, tied or trailing (by name). |
| `data_away_team_stats_time_values_trailing` | Int64 | Seconds spent leading, tied or trailing (by name). |
| `data_away_team_stats_time_values_time_game_strength_even` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_plus_one` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_plus_two` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_minus_one` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_minus_two` | Int64 | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_on_powerplay` | Int64 | Seconds on the power play. |
| `data_away_team_stats_time_values_time_while_shorthanded` | Int64 | Seconds the team played shorthanded in this game. |
| `data_home_goal_keeper_changes` | String | Goalie on/off events of that team. |
| `data_away_goal_keeper_changes` | String | Goalie on/off events of that team. |
| `data_game_actions` | String | Empty array in both samples; item shape unobserved. |
| `data_faceoffs_head_to_head` | String | Empty array in both samples; item shape unobserved. |
| `data_faceoffs_individual` | String | Empty array in both samples; item shape unobserved. |
| `data_game_situations` | String | Empty array in both samples; item shape unobserved. |
| `data_shootouts` | String | Empty array in both samples; item shape unobserved. |
| `calc_time` | Int64 | Server computation time; `0` in the samples. |
| `last_update_formatted_short` | String | Short display text `DD.MM.YY` (plus `HH:MM` for times). |
| `last_update_formatted_long` | String | Long display text, e.g. `18.September. 2026`. |
| `last_update_timestamp` | Int64 | Epoch milliseconds. |
| `last_update_value` | String | ISO text `YYYY-MM-DD` (plus ` HH:MM` for times). |
| `served_by_server` | String | Host that built the bundle, e.g. `icehl.hokejovyzapis.cz`. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#icehl_game_center-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#icehl_game_center-example}

```python
icehl_game_center(match_id=8161)
```
