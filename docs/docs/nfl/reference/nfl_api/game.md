---
title: "NFL — NFL.com API — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "NFL — NFL.com API — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — NFL.com API — Game

## nfl_game_summaries

GET /football/v2/stats/live/game-summaries — one row per game (live state).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/game-summaries`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/game-summaries?season=2024&seasonType=REG&week=1](https://api.nfl.com/football/v2/stats/live/game-summaries?season=2024&seasonType=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |

### Returns {#nfl_game_summaries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | NFL.com Shield GUID for the game. |
| `offset` | integer | Live-feed sequence offset for the summary snapshot. |
| `attendance` | integer | Announced game attendance. |
| `clock` | character | Game clock at the snapshot (MM:SS). |
| `distance` | integer | Yards to gain for a first down at the snapshot. |
| `down` | integer | Current down (1-4) at the snapshot. |
| `game_book_url` | character | URL of the official game book image. |
| `is_goal_to_go` | logical | Whether the current situation is goal-to-go. |
| `is_red_zone` | logical | Whether the ball is in the red zone. |
| `phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `start_time` | character | ISO 8601 kickoff timestamp. |
| `weather` | character | Weather summary string (temperature, humidity, wind). |
| `yard_line` | character | Current line of scrimmage (e.g. "KC 10"). |
| `away_team_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_has_possession` | logical | Whether the away team has possession at the snapshot. |
| `away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `away_team_score_ot` | integer | Away team points scored in overtime. |
| `away_team_score_total` | integer | Away team total points. |
| `away_team_timeouts_remaining` | integer | Away team timeouts remaining at the snapshot. |
| `away_team_timeouts_used` | integer | Away team timeouts used at the snapshot. |
| `home_team_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_has_possession` | logical | Whether the home team has possession at the snapshot. |
| `home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `home_team_score_ot` | integer | Home team points scored in overtime. |
| `home_team_score_total` | integer | Home team total points. |
| `home_team_timeouts_remaining` | integer | Home team timeouts remaining at the snapshot. |
| `home_team_timeouts_used` | integer | Home team timeouts used at the snapshot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_game_summaries-example}

```python
nfl_game_summaries(season=2024, season_type='REG', week=1)
```

_Last validated n/a._

## nfl_game_details_v2

GET /experience/v2/gamedetails/{game_id} — one row: the flat v2 game detail (game, summary, optional drive chart / replays / standings).

**Endpoint URL:** `GET https://api.nfl.com/experience/v2/gamedetails/{game_id}`

**Valid URL:** [https://api.nfl.com/experience/v2/gamedetails/a9a890ed-4feb-11f1-abca-2c54536568a9?includeDriveChart=false&includeReplays=false&includeStandings=false&includeTaggedVideos=false](https://api.nfl.com/experience/v2/gamedetails/a9a890ed-4feb-11f1-abca-2c54536568a9?includeDriveChart=false&includeReplays=false&includeStandings=false&includeTaggedVideos=false)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Shield uuid game id -- the ``id`` column of the week games and weekly game details listings. |
| `includeDriveChart` | `include_drive_chart` |  |  | `Y` | includeDriveChart query parameter. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |
| `includeStandings` | `include_standings` |  |  | `Y` | includeStandings query parameter. |
| `includeTaggedVideos` | `include_tagged_videos` |  |  | `Y` | includeTaggedVideos query parameter. |

### Returns {#nfl_game_details_v2-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the game. |
| `category` | character | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | character | Game date (YYYY-MM-DD). |
| `time` | character | ISO 8601 kickoff timestamp. |
| `game_type` | character | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | logical | Whether the game is played at an international venue. |
| `neutral_site` | logical | Whether the game is played at a neutral site. |
| `season` | integer | Season (year) the date falls in. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `status` | character | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | integer | Week number that the queried date falls within. |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | character | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | character | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | character | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | character | JSON-stringified array of extension objects (empty when none). |
| `version` | integer | Record version number. |
| `replays` | character | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `tagged_videos` | character | JSON-stringified array of tagged-video objects (populated only when include_tagged_videos=true). |
| `home_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | character | Templated URL of the home team's current logo. |
| `home_team_full_name` | character | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | character | Templated URL of the away team's current logo. |
| `away_team_full_name` | character | Full away-team name (e.g. "Baltimore Ravens"). |
| `broadcast_info_home_network_channels` | character | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | character | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | character | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | character | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | character | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | character | JSON-stringified array of audio-broadcast network objects. |
| `venue_id` | character | NFL.com Shield GUID of the venue. |
| `venue_name` | character | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | character | Venue city. |
| `venue_country` | character | Venue country. |
| `summary_game_id` | character | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | integer | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | integer | Announced game attendance (from the embedded summary). |
| `summary_clock` | character | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | integer | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | integer | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | character | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | logical | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | logical | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | character | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | character | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | character | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | character | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | logical | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | integer | Away team points scored in overtime. |
| `summary_away_team_score_total` | integer | Away team total points. |
| `summary_away_team_timeouts_remaining` | integer | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | integer | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | character | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | logical | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | integer | Home team points scored in overtime. |
| `summary_home_team_score_total` | integer | Home team total points. |
| `summary_home_team_timeouts_remaining` | integer | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | integer | Home team timeouts used at the summary snapshot. |
| `away_team_standings_team_id` | character | Away team standings: nFL.com Shield GUID for the team. |
| `away_team_standings_team_current_logo` | character | Away team standings: templated URL of the team's current logo. |
| `away_team_standings_team_full_name` | character | Away team standings: full team name (e.g. "Arizona Cardinals"). |
| `away_team_standings_clinched_bye` | logical | Away team standings: whether the team has clinched a first-round playoff bye. |
| `away_team_standings_clinched_division` | logical | Away team standings: whether the team has clinched its division. |
| `away_team_standings_clinched_eliminated` | logical | Away team standings: whether the team has been mathematically eliminated from playoff contention. |
| `away_team_standings_clinched_home_field` | logical | Away team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `away_team_standings_clinched_playoff` | logical | Away team standings: whether the team has clinched a playoff berth. |
| `away_team_standings_clinched_wild_card` | logical | Away team standings: whether the team has clinched a wild-card playoff berth. |
| `away_team_standings_close_games_wins` | integer | Away team standings: wins in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_losses` | integer | Away team standings: losses in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_ties` | integer | Away team standings: ties in close games. |
| `away_team_standings_conference_wins` | integer | Away team standings: wins against conference (AFC/NFC) opponents. |
| `away_team_standings_conference_win_pct` | numeric | Away team standings: win percentage against conference opponents. |
| `away_team_standings_conference_losses` | integer | Away team standings: losses against conference opponents. |
| `away_team_standings_conference_ties` | integer | Away team standings: ties against conference opponents. |
| `away_team_standings_conference_rank` | integer | Away team standings: standings rank within the conference. |
| `away_team_standings_conference_points_for` | integer | Away team standings: points scored in conference games. |
| `away_team_standings_conference_points_against` | integer | Away team standings: points allowed in conference games. |
| `away_team_standings_division_wins` | integer | Away team standings: wins against division opponents. |
| `away_team_standings_division_win_pct` | numeric | Away team standings: win percentage against division opponents. |
| `away_team_standings_division_losses` | integer | Away team standings: losses against division opponents. |
| `away_team_standings_division_ties` | integer | Away team standings: ties against division opponents. |
| `away_team_standings_division_rank` | integer | Away team standings: standings rank within the division. |
| `away_team_standings_division_points_for` | integer | Away team standings: points scored in division games. |
| `away_team_standings_division_points_against` | integer | Away team standings: points allowed in division games. |
| `away_team_standings_home_wins` | integer | Away team standings: wins in home games. |
| `away_team_standings_home_win_pct` | numeric | Away team standings: win percentage in home games. |
| `away_team_standings_home_losses` | integer | Away team standings: losses in home games. |
| `away_team_standings_home_ties` | integer | Away team standings: ties in home games. |
| `away_team_standings_home_points_for` | integer | Away team standings: points scored in home games. |
| `away_team_standings_home_points_against` | integer | Away team standings: points allowed in home games. |
| `away_team_standings_last5_wins` | integer | Away team standings: wins over the last five games. |
| `away_team_standings_last5_win_pct` | numeric | Away team standings: win percentage over the last five games. |
| `away_team_standings_last5_losses` | integer | Away team standings: losses over the last five games. |
| `away_team_standings_last5_ties` | integer | Away team standings: ties over the last five games. |
| `away_team_standings_last5_points_for` | integer | Away team standings: points scored over the last five games. |
| `away_team_standings_last5_points_against` | integer | Away team standings: points allowed over the last five games. |
| `away_team_standings_overall_games` | integer | Away team standings: total games played. |
| `away_team_standings_overall_wins` | integer | Away team standings: total wins. |
| `away_team_standings_overall_win_pct` | numeric | Away team standings: overall win percentage. |
| `away_team_standings_overall_losses` | integer | Away team standings: total losses. |
| `away_team_standings_overall_ties` | integer | Away team standings: total ties. |
| `away_team_standings_overall_points_for` | integer | Away team standings: total points scored. |
| `away_team_standings_overall_points_against` | integer | Away team standings: total points allowed. |
| `away_team_standings_overall_streak_type` | character | Away team standings: current streak type ("W" for winning, "L" for losing). |
| `away_team_standings_overall_streak_length` | integer | Away team standings: length of the current win/loss streak. |
| `away_team_standings_road_wins` | integer | Away team standings: wins in road (away) games. |
| `away_team_standings_road_win_pct` | numeric | Away team standings: win percentage in road games. |
| `away_team_standings_road_losses` | integer | Away team standings: losses in road games. |
| `away_team_standings_road_ties` | integer | Away team standings: ties in road games. |
| `away_team_standings_road_points_for` | integer | Away team standings: points scored in road games. |
| `away_team_standings_road_points_against` | integer | Away team standings: points allowed in road games. |
| `home_team_standings_team_id` | character | Home team standings: nFL.com Shield GUID for the team. |
| `home_team_standings_team_current_logo` | character | Home team standings: templated URL of the team's current logo. |
| `home_team_standings_team_full_name` | character | Home team standings: full team name (e.g. "Arizona Cardinals"). |
| `home_team_standings_clinched_bye` | logical | Home team standings: whether the team has clinched a first-round playoff bye. |
| `home_team_standings_clinched_division` | logical | Home team standings: whether the team has clinched its division. |
| `home_team_standings_clinched_eliminated` | logical | Home team standings: whether the team has been mathematically eliminated from playoff contention. |
| `home_team_standings_clinched_home_field` | logical | Home team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `home_team_standings_clinched_playoff` | logical | Home team standings: whether the team has clinched a playoff berth. |
| `home_team_standings_clinched_wild_card` | logical | Home team standings: whether the team has clinched a wild-card playoff berth. |
| `home_team_standings_close_games_wins` | integer | Home team standings: wins in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_losses` | integer | Home team standings: losses in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_ties` | integer | Home team standings: ties in close games. |
| `home_team_standings_conference_wins` | integer | Home team standings: wins against conference (AFC/NFC) opponents. |
| `home_team_standings_conference_win_pct` | numeric | Home team standings: win percentage against conference opponents. |
| `home_team_standings_conference_losses` | integer | Home team standings: losses against conference opponents. |
| `home_team_standings_conference_ties` | integer | Home team standings: ties against conference opponents. |
| `home_team_standings_conference_rank` | integer | Home team standings: standings rank within the conference. |
| `home_team_standings_conference_points_for` | integer | Home team standings: points scored in conference games. |
| `home_team_standings_conference_points_against` | integer | Home team standings: points allowed in conference games. |
| `home_team_standings_division_wins` | integer | Home team standings: wins against division opponents. |
| `home_team_standings_division_win_pct` | numeric | Home team standings: win percentage against division opponents. |
| `home_team_standings_division_losses` | integer | Home team standings: losses against division opponents. |
| `home_team_standings_division_ties` | integer | Home team standings: ties against division opponents. |
| `home_team_standings_division_rank` | integer | Home team standings: standings rank within the division. |
| `home_team_standings_division_points_for` | integer | Home team standings: points scored in division games. |
| `home_team_standings_division_points_against` | integer | Home team standings: points allowed in division games. |
| `home_team_standings_home_wins` | integer | Home team standings: wins in home games. |
| `home_team_standings_home_win_pct` | numeric | Home team standings: win percentage in home games. |
| `home_team_standings_home_losses` | integer | Home team standings: losses in home games. |
| `home_team_standings_home_ties` | integer | Home team standings: ties in home games. |
| `home_team_standings_home_points_for` | integer | Home team standings: points scored in home games. |
| `home_team_standings_home_points_against` | integer | Home team standings: points allowed in home games. |
| `home_team_standings_last5_wins` | integer | Home team standings: wins over the last five games. |
| `home_team_standings_last5_win_pct` | numeric | Home team standings: win percentage over the last five games. |
| `home_team_standings_last5_losses` | integer | Home team standings: losses over the last five games. |
| `home_team_standings_last5_ties` | integer | Home team standings: ties over the last five games. |
| `home_team_standings_last5_points_for` | integer | Home team standings: points scored over the last five games. |
| `home_team_standings_last5_points_against` | integer | Home team standings: points allowed over the last five games. |
| `home_team_standings_overall_games` | integer | Home team standings: total games played. |
| `home_team_standings_overall_wins` | integer | Home team standings: total wins. |
| `home_team_standings_overall_win_pct` | numeric | Home team standings: overall win percentage. |
| `home_team_standings_overall_losses` | integer | Home team standings: total losses. |
| `home_team_standings_overall_ties` | integer | Home team standings: total ties. |
| `home_team_standings_overall_points_for` | integer | Home team standings: total points scored. |
| `home_team_standings_overall_points_against` | integer | Home team standings: total points allowed. |
| `home_team_standings_overall_streak_type` | character | Home team standings: current streak type ("W" for winning, "L" for losing). |
| `home_team_standings_overall_streak_length` | integer | Home team standings: length of the current win/loss streak. |
| `home_team_standings_road_wins` | integer | Home team standings: wins in road (away) games. |
| `home_team_standings_road_win_pct` | numeric | Home team standings: win percentage in road games. |
| `home_team_standings_road_losses` | integer | Home team standings: losses in road games. |
| `home_team_standings_road_ties` | integer | Home team standings: ties in road games. |
| `home_team_standings_road_points_for` | integer | Home team standings: points scored in road games. |
| `home_team_standings_road_points_against` | integer | Home team standings: points allowed in road games. |
| `drive_chart_game_id` | character | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | integer | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | character | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | character | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | character | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_game_details_v2-example}

```python
nfl_game_details_v2(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

_Last validated n/a._

## nfl_game_details_by_slug

GET /experience/v1/gamedetailsbyslug/{slug} — one row: the flat game detail looked up by nfl.com slug.

**Endpoint URL:** `GET https://api.nfl.com/experience/v1/gamedetailsbyslug/{slug}`

**Valid URL:** [https://api.nfl.com/experience/v1/gamedetailsbyslug/broncos-at-chiefs-2026-reg-1?includeReplays=false](https://api.nfl.com/experience/v1/gamedetailsbyslug/broncos-at-chiefs-2026-reg-1?includeReplays=false)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | nfl.com game slug, e.g. ``broncos-at-chiefs-2026-reg-1`` -- the last segment of the nfl.com game page URL and the ``slug`` external id. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |

### Returns {#nfl_game_details_by_slug-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the game. |
| `category` | character | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | character | Game date (YYYY-MM-DD). |
| `time` | character | ISO 8601 kickoff timestamp. |
| `game_type` | character | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | logical | Whether the game is played at an international venue. |
| `neutral_site` | logical | Whether the game is played at a neutral site. |
| `season` | integer | Season (year) the date falls in. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `status` | character | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | integer | Week number that the queried date falls within. |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | character | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | character | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | character | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | character | JSON-stringified array of extension objects (empty when none). |
| `version` | integer | Record version number. |
| `replays` | character | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `home_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | character | Templated URL of the home team's current logo. |
| `home_team_full_name` | character | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | character | Templated URL of the away team's current logo. |
| `away_team_full_name` | character | Full away-team name (e.g. "Baltimore Ravens"). |
| `broadcast_info_home_network_channels` | character | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | character | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | character | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | character | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | character | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | character | JSON-stringified array of audio-broadcast network objects. |
| `venue_id` | character | NFL.com Shield GUID of the venue. |
| `venue_name` | character | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | character | Venue city. |
| `venue_country` | character | Venue country. |
| `summary_game_id` | character | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | integer | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | integer | Announced game attendance (from the embedded summary). |
| `summary_clock` | character | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | integer | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | integer | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | character | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | logical | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | logical | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | character | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | character | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | character | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | character | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | logical | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | integer | Away team points scored in overtime. |
| `summary_away_team_score_total` | integer | Away team total points. |
| `summary_away_team_timeouts_remaining` | integer | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | integer | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | character | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | logical | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | integer | Home team points scored in overtime. |
| `summary_home_team_score_total` | integer | Home team total points. |
| `summary_home_team_timeouts_remaining` | integer | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | integer | Home team timeouts used at the summary snapshot. |
| `away_team_standings_team_id` | character | Away team standings: nFL.com Shield GUID for the team. |
| `away_team_standings_team_current_logo` | character | Away team standings: templated URL of the team's current logo. |
| `away_team_standings_team_full_name` | character | Away team standings: full team name (e.g. "Arizona Cardinals"). |
| `away_team_standings_clinched_bye` | logical | Away team standings: whether the team has clinched a first-round playoff bye. |
| `away_team_standings_clinched_division` | logical | Away team standings: whether the team has clinched its division. |
| `away_team_standings_clinched_eliminated` | logical | Away team standings: whether the team has been mathematically eliminated from playoff contention. |
| `away_team_standings_clinched_home_field` | logical | Away team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `away_team_standings_clinched_playoff` | logical | Away team standings: whether the team has clinched a playoff berth. |
| `away_team_standings_clinched_wild_card` | logical | Away team standings: whether the team has clinched a wild-card playoff berth. |
| `away_team_standings_close_games_wins` | integer | Away team standings: wins in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_losses` | integer | Away team standings: losses in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_ties` | integer | Away team standings: ties in close games. |
| `away_team_standings_conference_wins` | integer | Away team standings: wins against conference (AFC/NFC) opponents. |
| `away_team_standings_conference_win_pct` | numeric | Away team standings: win percentage against conference opponents. |
| `away_team_standings_conference_losses` | integer | Away team standings: losses against conference opponents. |
| `away_team_standings_conference_ties` | integer | Away team standings: ties against conference opponents. |
| `away_team_standings_conference_rank` | integer | Away team standings: standings rank within the conference. |
| `away_team_standings_conference_points_for` | integer | Away team standings: points scored in conference games. |
| `away_team_standings_conference_points_against` | integer | Away team standings: points allowed in conference games. |
| `away_team_standings_division_wins` | integer | Away team standings: wins against division opponents. |
| `away_team_standings_division_win_pct` | numeric | Away team standings: win percentage against division opponents. |
| `away_team_standings_division_losses` | integer | Away team standings: losses against division opponents. |
| `away_team_standings_division_ties` | integer | Away team standings: ties against division opponents. |
| `away_team_standings_division_rank` | integer | Away team standings: standings rank within the division. |
| `away_team_standings_division_points_for` | integer | Away team standings: points scored in division games. |
| `away_team_standings_division_points_against` | integer | Away team standings: points allowed in division games. |
| `away_team_standings_home_wins` | integer | Away team standings: wins in home games. |
| `away_team_standings_home_win_pct` | numeric | Away team standings: win percentage in home games. |
| `away_team_standings_home_losses` | integer | Away team standings: losses in home games. |
| `away_team_standings_home_ties` | integer | Away team standings: ties in home games. |
| `away_team_standings_home_points_for` | integer | Away team standings: points scored in home games. |
| `away_team_standings_home_points_against` | integer | Away team standings: points allowed in home games. |
| `away_team_standings_last5_wins` | integer | Away team standings: wins over the last five games. |
| `away_team_standings_last5_win_pct` | numeric | Away team standings: win percentage over the last five games. |
| `away_team_standings_last5_losses` | integer | Away team standings: losses over the last five games. |
| `away_team_standings_last5_ties` | integer | Away team standings: ties over the last five games. |
| `away_team_standings_last5_points_for` | integer | Away team standings: points scored over the last five games. |
| `away_team_standings_last5_points_against` | integer | Away team standings: points allowed over the last five games. |
| `away_team_standings_overall_games` | integer | Away team standings: total games played. |
| `away_team_standings_overall_wins` | integer | Away team standings: total wins. |
| `away_team_standings_overall_win_pct` | numeric | Away team standings: overall win percentage. |
| `away_team_standings_overall_losses` | integer | Away team standings: total losses. |
| `away_team_standings_overall_ties` | integer | Away team standings: total ties. |
| `away_team_standings_overall_points_for` | integer | Away team standings: total points scored. |
| `away_team_standings_overall_points_against` | integer | Away team standings: total points allowed. |
| `away_team_standings_overall_streak_type` | character | Away team standings: current streak type ("W" for winning, "L" for losing). |
| `away_team_standings_overall_streak_length` | integer | Away team standings: length of the current win/loss streak. |
| `away_team_standings_road_wins` | integer | Away team standings: wins in road (away) games. |
| `away_team_standings_road_win_pct` | numeric | Away team standings: win percentage in road games. |
| `away_team_standings_road_losses` | integer | Away team standings: losses in road games. |
| `away_team_standings_road_ties` | integer | Away team standings: ties in road games. |
| `away_team_standings_road_points_for` | integer | Away team standings: points scored in road games. |
| `away_team_standings_road_points_against` | integer | Away team standings: points allowed in road games. |
| `home_team_standings_team_id` | character | Home team standings: nFL.com Shield GUID for the team. |
| `home_team_standings_team_current_logo` | character | Home team standings: templated URL of the team's current logo. |
| `home_team_standings_team_full_name` | character | Home team standings: full team name (e.g. "Arizona Cardinals"). |
| `home_team_standings_clinched_bye` | logical | Home team standings: whether the team has clinched a first-round playoff bye. |
| `home_team_standings_clinched_division` | logical | Home team standings: whether the team has clinched its division. |
| `home_team_standings_clinched_eliminated` | logical | Home team standings: whether the team has been mathematically eliminated from playoff contention. |
| `home_team_standings_clinched_home_field` | logical | Home team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `home_team_standings_clinched_playoff` | logical | Home team standings: whether the team has clinched a playoff berth. |
| `home_team_standings_clinched_wild_card` | logical | Home team standings: whether the team has clinched a wild-card playoff berth. |
| `home_team_standings_close_games_wins` | integer | Home team standings: wins in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_losses` | integer | Home team standings: losses in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_ties` | integer | Home team standings: ties in close games. |
| `home_team_standings_conference_wins` | integer | Home team standings: wins against conference (AFC/NFC) opponents. |
| `home_team_standings_conference_win_pct` | numeric | Home team standings: win percentage against conference opponents. |
| `home_team_standings_conference_losses` | integer | Home team standings: losses against conference opponents. |
| `home_team_standings_conference_ties` | integer | Home team standings: ties against conference opponents. |
| `home_team_standings_conference_rank` | integer | Home team standings: standings rank within the conference. |
| `home_team_standings_conference_points_for` | integer | Home team standings: points scored in conference games. |
| `home_team_standings_conference_points_against` | integer | Home team standings: points allowed in conference games. |
| `home_team_standings_division_wins` | integer | Home team standings: wins against division opponents. |
| `home_team_standings_division_win_pct` | numeric | Home team standings: win percentage against division opponents. |
| `home_team_standings_division_losses` | integer | Home team standings: losses against division opponents. |
| `home_team_standings_division_ties` | integer | Home team standings: ties against division opponents. |
| `home_team_standings_division_rank` | integer | Home team standings: standings rank within the division. |
| `home_team_standings_division_points_for` | integer | Home team standings: points scored in division games. |
| `home_team_standings_division_points_against` | integer | Home team standings: points allowed in division games. |
| `home_team_standings_home_wins` | integer | Home team standings: wins in home games. |
| `home_team_standings_home_win_pct` | numeric | Home team standings: win percentage in home games. |
| `home_team_standings_home_losses` | integer | Home team standings: losses in home games. |
| `home_team_standings_home_ties` | integer | Home team standings: ties in home games. |
| `home_team_standings_home_points_for` | integer | Home team standings: points scored in home games. |
| `home_team_standings_home_points_against` | integer | Home team standings: points allowed in home games. |
| `home_team_standings_last5_wins` | integer | Home team standings: wins over the last five games. |
| `home_team_standings_last5_win_pct` | numeric | Home team standings: win percentage over the last five games. |
| `home_team_standings_last5_losses` | integer | Home team standings: losses over the last five games. |
| `home_team_standings_last5_ties` | integer | Home team standings: ties over the last five games. |
| `home_team_standings_last5_points_for` | integer | Home team standings: points scored over the last five games. |
| `home_team_standings_last5_points_against` | integer | Home team standings: points allowed over the last five games. |
| `home_team_standings_overall_games` | integer | Home team standings: total games played. |
| `home_team_standings_overall_wins` | integer | Home team standings: total wins. |
| `home_team_standings_overall_win_pct` | numeric | Home team standings: overall win percentage. |
| `home_team_standings_overall_losses` | integer | Home team standings: total losses. |
| `home_team_standings_overall_ties` | integer | Home team standings: total ties. |
| `home_team_standings_overall_points_for` | integer | Home team standings: total points scored. |
| `home_team_standings_overall_points_against` | integer | Home team standings: total points allowed. |
| `home_team_standings_overall_streak_type` | character | Home team standings: current streak type ("W" for winning, "L" for losing). |
| `home_team_standings_overall_streak_length` | integer | Home team standings: length of the current win/loss streak. |
| `home_team_standings_road_wins` | integer | Home team standings: wins in road (away) games. |
| `home_team_standings_road_win_pct` | numeric | Home team standings: win percentage in road games. |
| `home_team_standings_road_losses` | integer | Home team standings: losses in road games. |
| `home_team_standings_road_ties` | integer | Home team standings: ties in road games. |
| `home_team_standings_road_points_for` | integer | Home team standings: points scored in road games. |
| `home_team_standings_road_points_against` | integer | Home team standings: points allowed in road games. |
| `drive_chart_game_id` | character | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | integer | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | character | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | character | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | character | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_game_details_by_slug-example}

```python
nfl_game_details_by_slug(slug='broncos-at-chiefs-2026-reg-1')
```

_Last validated n/a._
