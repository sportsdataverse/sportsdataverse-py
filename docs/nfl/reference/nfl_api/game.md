# NFL — NFL.com API — Game

> NFL — NFL.com API — Game — function reference in sdv-py, the SportsDataverse Python package.

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
| `game_id` | String | NFL.com Shield GUID for the game. |
| `offset` | Int64 | Live-feed sequence offset for the summary snapshot. |
| `attendance` | Int64 | Announced game attendance. |
| `clock` | String | Game clock at the snapshot (MM:SS). |
| `distance` | Int64 | Yards to gain for a first down at the snapshot. |
| `down` | Int64 | Current down (1-4) at the snapshot. |
| `game_book_url` | String | URL of the official game book image. |
| `is_goal_to_go` | Boolean | Whether the current situation is goal-to-go. |
| `is_red_zone` | Boolean | Whether the ball is in the red zone. |
| `phase` | String | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `quarter` | String | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `start_time` | String | ISO 8601 kickoff timestamp. |
| `weather` | String | Weather summary string (temperature, humidity, wind). |
| `yard_line` | String | Current line of scrimmage (e.g. "KC 10"). |
| `away_team_team_id` | String | NFL.com Shield GUID of the away team. |
| `away_team_has_possession` | Boolean | Whether the away team has possession at the snapshot. |
| `away_team_score_q1` | Int64 | Away team points scored in the first quarter. |
| `away_team_score_q2` | Int64 | Away team points scored in the second quarter. |
| `away_team_score_q3` | Int64 | Away team points scored in the third quarter. |
| `away_team_score_q4` | Int64 | Away team points scored in the fourth quarter. |
| `away_team_score_ot` | Int64 | Away team points scored in overtime. |
| `away_team_score_total` | Int64 | Away team total points. |
| `away_team_timeouts_remaining` | Int64 | Away team timeouts remaining at the snapshot. |
| `away_team_timeouts_used` | Int64 | Away team timeouts used at the snapshot. |
| `home_team_team_id` | String | NFL.com Shield GUID of the home team. |
| `home_team_has_possession` | Boolean | Whether the home team has possession at the snapshot. |
| `home_team_score_q1` | Int64 | Home team points scored in the first quarter. |
| `home_team_score_q2` | Int64 | Home team points scored in the second quarter. |
| `home_team_score_q3` | Int64 | Home team points scored in the third quarter. |
| `home_team_score_q4` | Int64 | Home team points scored in the fourth quarter. |
| `home_team_score_ot` | Int64 | Home team points scored in overtime. |
| `home_team_score_total` | Int64 | Home team total points. |
| `home_team_timeouts_remaining` | Int64 | Home team timeouts remaining at the snapshot. |
| `home_team_timeouts_used` | Int64 | Home team timeouts used at the snapshot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_game_summaries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_game_summaries-example}

```python
nfl_game_summaries(season=2024, season_type='REG', week=1)
```

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
| `id` | String | NFL.com Shield GUID for the game. |
| `category` | String | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | String | Game date (YYYY-MM-DD). |
| `time` | String | ISO 8601 kickoff timestamp. |
| `game_type` | String | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | Boolean | Whether the game is played at an international venue. |
| `neutral_site` | Boolean | Whether the game is played at a neutral site. |
| `season` | Int64 | Season (year) the date falls in. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `status` | String | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | Int64 | Week number that the queried date falls within. |
| `week_type` | String | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | String | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | String | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | String | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | String | JSON-stringified array of extension objects (empty when none). |
| `version` | Int64 | Record version number. |
| `replays` | String | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `tagged_videos` | String | JSON-stringified array of tagged-video objects (populated only when include_tagged_videos=true). |
| `home_team_id` | String | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | String | Templated URL of the home team's current logo. |
| `home_team_full_name` | String | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | String | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | String | Templated URL of the away team's current logo. |
| `away_team_full_name` | String | Full away-team name (e.g. "Baltimore Ravens"). |
| `broadcast_info_home_network_channels` | String | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | String | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | String | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | String | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | String | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | String | JSON-stringified array of audio-broadcast network objects. |
| `venue_id` | String | NFL.com Shield GUID of the venue. |
| `venue_name` | String | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | String | Venue city. |
| `venue_country` | String | Venue country. |
| `summary_game_id` | String | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | Int64 | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | Int64 | Announced game attendance (from the embedded summary). |
| `summary_clock` | String | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | Int64 | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | Int64 | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | String | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | Boolean | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | Boolean | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | String | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | String | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | String | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | String | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | String | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | String | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | Boolean | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | Int64 | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | Int64 | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | Int64 | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | Int64 | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | Int64 | Away team points scored in overtime. |
| `summary_away_team_score_total` | Int64 | Away team total points. |
| `summary_away_team_timeouts_remaining` | Int64 | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | Int64 | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | String | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | Boolean | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | Int64 | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | Int64 | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | Int64 | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | Int64 | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | Int64 | Home team points scored in overtime. |
| `summary_home_team_score_total` | Int64 | Home team total points. |
| `summary_home_team_timeouts_remaining` | Int64 | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | Int64 | Home team timeouts used at the summary snapshot. |
| `away_team_standings_team_id` | String | Away team standings: nFL.com Shield GUID for the team. |
| `away_team_standings_team_current_logo` | String | Away team standings: templated URL of the team's current logo. |
| `away_team_standings_team_full_name` | String | Away team standings: full team name (e.g. "Arizona Cardinals"). |
| `away_team_standings_clinched_bye` | Boolean | Away team standings: whether the team has clinched a first-round playoff bye. |
| `away_team_standings_clinched_division` | Boolean | Away team standings: whether the team has clinched its division. |
| `away_team_standings_clinched_eliminated` | Boolean | Away team standings: whether the team has been mathematically eliminated from playoff contention. |
| `away_team_standings_clinched_home_field` | Boolean | Away team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `away_team_standings_clinched_playoff` | Boolean | Away team standings: whether the team has clinched a playoff berth. |
| `away_team_standings_clinched_wild_card` | Boolean | Away team standings: whether the team has clinched a wild-card playoff berth. |
| `away_team_standings_close_games_wins` | Int64 | Away team standings: wins in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_losses` | Int64 | Away team standings: losses in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_ties` | Int64 | Away team standings: ties in close games. |
| `away_team_standings_conference_wins` | Int64 | Away team standings: wins against conference (AFC/NFC) opponents. |
| `away_team_standings_conference_win_pct` | Float64 | Away team standings: win percentage against conference opponents. |
| `away_team_standings_conference_losses` | Int64 | Away team standings: losses against conference opponents. |
| `away_team_standings_conference_ties` | Int64 | Away team standings: ties against conference opponents. |
| `away_team_standings_conference_rank` | Int64 | Away team standings: standings rank within the conference. |
| `away_team_standings_conference_points_for` | Int64 | Away team standings: points scored in conference games. |
| `away_team_standings_conference_points_against` | Int64 | Away team standings: points allowed in conference games. |
| `away_team_standings_division_wins` | Int64 | Away team standings: wins against division opponents. |
| `away_team_standings_division_win_pct` | Float64 | Away team standings: win percentage against division opponents. |
| `away_team_standings_division_losses` | Int64 | Away team standings: losses against division opponents. |
| `away_team_standings_division_ties` | Int64 | Away team standings: ties against division opponents. |
| `away_team_standings_division_rank` | Int64 | Away team standings: standings rank within the division. |
| `away_team_standings_division_points_for` | Int64 | Away team standings: points scored in division games. |
| `away_team_standings_division_points_against` | Int64 | Away team standings: points allowed in division games. |
| `away_team_standings_home_wins` | Int64 | Away team standings: wins in home games. |
| `away_team_standings_home_win_pct` | Float64 | Away team standings: win percentage in home games. |
| `away_team_standings_home_losses` | Int64 | Away team standings: losses in home games. |
| `away_team_standings_home_ties` | Int64 | Away team standings: ties in home games. |
| `away_team_standings_home_points_for` | Int64 | Away team standings: points scored in home games. |
| `away_team_standings_home_points_against` | Int64 | Away team standings: points allowed in home games. |
| `away_team_standings_last5_wins` | Int64 | Away team standings: wins over the last five games. |
| `away_team_standings_last5_win_pct` | Float64 | Away team standings: win percentage over the last five games. |
| `away_team_standings_last5_losses` | Int64 | Away team standings: losses over the last five games. |
| `away_team_standings_last5_ties` | Int64 | Away team standings: ties over the last five games. |
| `away_team_standings_last5_points_for` | Int64 | Away team standings: points scored over the last five games. |
| `away_team_standings_last5_points_against` | Int64 | Away team standings: points allowed over the last five games. |
| `away_team_standings_overall_games` | Int64 | Away team standings: total games played. |
| `away_team_standings_overall_wins` | Int64 | Away team standings: total wins. |
| `away_team_standings_overall_win_pct` | Float64 | Away team standings: overall win percentage. |
| `away_team_standings_overall_losses` | Int64 | Away team standings: total losses. |
| `away_team_standings_overall_ties` | Int64 | Away team standings: total ties. |
| `away_team_standings_overall_points_for` | Int64 | Away team standings: total points scored. |
| `away_team_standings_overall_points_against` | Int64 | Away team standings: total points allowed. |
| `away_team_standings_overall_streak_type` | String | Away team standings: current streak type ("W" for winning, "L" for losing). |
| `away_team_standings_overall_streak_length` | Int64 | Away team standings: length of the current win/loss streak. |
| `away_team_standings_road_wins` | Int64 | Away team standings: wins in road (away) games. |
| `away_team_standings_road_win_pct` | Float64 | Away team standings: win percentage in road games. |
| `away_team_standings_road_losses` | Int64 | Away team standings: losses in road games. |
| `away_team_standings_road_ties` | Int64 | Away team standings: ties in road games. |
| `away_team_standings_road_points_for` | Int64 | Away team standings: points scored in road games. |
| `away_team_standings_road_points_against` | Int64 | Away team standings: points allowed in road games. |
| `home_team_standings_team_id` | String | Home team standings: nFL.com Shield GUID for the team. |
| `home_team_standings_team_current_logo` | String | Home team standings: templated URL of the team's current logo. |
| `home_team_standings_team_full_name` | String | Home team standings: full team name (e.g. "Arizona Cardinals"). |
| `home_team_standings_clinched_bye` | Boolean | Home team standings: whether the team has clinched a first-round playoff bye. |
| `home_team_standings_clinched_division` | Boolean | Home team standings: whether the team has clinched its division. |
| `home_team_standings_clinched_eliminated` | Boolean | Home team standings: whether the team has been mathematically eliminated from playoff contention. |
| `home_team_standings_clinched_home_field` | Boolean | Home team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `home_team_standings_clinched_playoff` | Boolean | Home team standings: whether the team has clinched a playoff berth. |
| `home_team_standings_clinched_wild_card` | Boolean | Home team standings: whether the team has clinched a wild-card playoff berth. |
| `home_team_standings_close_games_wins` | Int64 | Home team standings: wins in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_losses` | Int64 | Home team standings: losses in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_ties` | Int64 | Home team standings: ties in close games. |
| `home_team_standings_conference_wins` | Int64 | Home team standings: wins against conference (AFC/NFC) opponents. |
| `home_team_standings_conference_win_pct` | Float64 | Home team standings: win percentage against conference opponents. |
| `home_team_standings_conference_losses` | Int64 | Home team standings: losses against conference opponents. |
| `home_team_standings_conference_ties` | Int64 | Home team standings: ties against conference opponents. |
| `home_team_standings_conference_rank` | Int64 | Home team standings: standings rank within the conference. |
| `home_team_standings_conference_points_for` | Int64 | Home team standings: points scored in conference games. |
| `home_team_standings_conference_points_against` | Int64 | Home team standings: points allowed in conference games. |
| `home_team_standings_division_wins` | Int64 | Home team standings: wins against division opponents. |
| `home_team_standings_division_win_pct` | Float64 | Home team standings: win percentage against division opponents. |
| `home_team_standings_division_losses` | Int64 | Home team standings: losses against division opponents. |
| `home_team_standings_division_ties` | Int64 | Home team standings: ties against division opponents. |
| `home_team_standings_division_rank` | Int64 | Home team standings: standings rank within the division. |
| `home_team_standings_division_points_for` | Int64 | Home team standings: points scored in division games. |
| `home_team_standings_division_points_against` | Int64 | Home team standings: points allowed in division games. |
| `home_team_standings_home_wins` | Int64 | Home team standings: wins in home games. |
| `home_team_standings_home_win_pct` | Float64 | Home team standings: win percentage in home games. |
| `home_team_standings_home_losses` | Int64 | Home team standings: losses in home games. |
| `home_team_standings_home_ties` | Int64 | Home team standings: ties in home games. |
| `home_team_standings_home_points_for` | Int64 | Home team standings: points scored in home games. |
| `home_team_standings_home_points_against` | Int64 | Home team standings: points allowed in home games. |
| `home_team_standings_last5_wins` | Int64 | Home team standings: wins over the last five games. |
| `home_team_standings_last5_win_pct` | Float64 | Home team standings: win percentage over the last five games. |
| `home_team_standings_last5_losses` | Int64 | Home team standings: losses over the last five games. |
| `home_team_standings_last5_ties` | Int64 | Home team standings: ties over the last five games. |
| `home_team_standings_last5_points_for` | Int64 | Home team standings: points scored over the last five games. |
| `home_team_standings_last5_points_against` | Int64 | Home team standings: points allowed over the last five games. |
| `home_team_standings_overall_games` | Int64 | Home team standings: total games played. |
| `home_team_standings_overall_wins` | Int64 | Home team standings: total wins. |
| `home_team_standings_overall_win_pct` | Float64 | Home team standings: overall win percentage. |
| `home_team_standings_overall_losses` | Int64 | Home team standings: total losses. |
| `home_team_standings_overall_ties` | Int64 | Home team standings: total ties. |
| `home_team_standings_overall_points_for` | Int64 | Home team standings: total points scored. |
| `home_team_standings_overall_points_against` | Int64 | Home team standings: total points allowed. |
| `home_team_standings_overall_streak_type` | String | Home team standings: current streak type ("W" for winning, "L" for losing). |
| `home_team_standings_overall_streak_length` | Int64 | Home team standings: length of the current win/loss streak. |
| `home_team_standings_road_wins` | Int64 | Home team standings: wins in road (away) games. |
| `home_team_standings_road_win_pct` | Float64 | Home team standings: win percentage in road games. |
| `home_team_standings_road_losses` | Int64 | Home team standings: losses in road games. |
| `home_team_standings_road_ties` | Int64 | Home team standings: ties in road games. |
| `home_team_standings_road_points_for` | Int64 | Home team standings: points scored in road games. |
| `home_team_standings_road_points_against` | Int64 | Home team standings: points allowed in road games. |
| `drive_chart_game_id` | String | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | Int64 | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | String | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | String | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | String | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_game_details_v2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_game_details_v2-example}

```python
nfl_game_details_v2(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

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
| `id` | String | NFL.com Shield GUID for the game. |
| `category` | String | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | String | Game date (YYYY-MM-DD). |
| `time` | String | ISO 8601 kickoff timestamp. |
| `game_type` | String | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | Boolean | Whether the game is played at an international venue. |
| `neutral_site` | Boolean | Whether the game is played at a neutral site. |
| `season` | Int64 | Season (year) the date falls in. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `status` | String | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | Int64 | Week number that the queried date falls within. |
| `week_type` | String | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | String | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | String | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | String | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | String | JSON-stringified array of extension objects (empty when none). |
| `version` | Int64 | Record version number. |
| `replays` | String | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `home_team_id` | String | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | String | Templated URL of the home team's current logo. |
| `home_team_full_name` | String | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | String | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | String | Templated URL of the away team's current logo. |
| `away_team_full_name` | String | Full away-team name (e.g. "Baltimore Ravens"). |
| `broadcast_info_home_network_channels` | String | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | String | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | String | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | String | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | String | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | String | JSON-stringified array of audio-broadcast network objects. |
| `venue_id` | String | NFL.com Shield GUID of the venue. |
| `venue_name` | String | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | String | Venue city. |
| `venue_country` | String | Venue country. |
| `summary_game_id` | String | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | Int64 | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | Int64 | Announced game attendance (from the embedded summary). |
| `summary_clock` | String | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | Int64 | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | Int64 | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | String | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | Boolean | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | Boolean | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | String | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | String | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | String | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | String | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | String | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | String | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | Boolean | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | Int64 | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | Int64 | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | Int64 | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | Int64 | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | Int64 | Away team points scored in overtime. |
| `summary_away_team_score_total` | Int64 | Away team total points. |
| `summary_away_team_timeouts_remaining` | Int64 | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | Int64 | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | String | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | Boolean | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | Int64 | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | Int64 | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | Int64 | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | Int64 | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | Int64 | Home team points scored in overtime. |
| `summary_home_team_score_total` | Int64 | Home team total points. |
| `summary_home_team_timeouts_remaining` | Int64 | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | Int64 | Home team timeouts used at the summary snapshot. |
| `away_team_standings_team_id` | String | Away team standings: nFL.com Shield GUID for the team. |
| `away_team_standings_team_current_logo` | String | Away team standings: templated URL of the team's current logo. |
| `away_team_standings_team_full_name` | String | Away team standings: full team name (e.g. "Arizona Cardinals"). |
| `away_team_standings_clinched_bye` | Boolean | Away team standings: whether the team has clinched a first-round playoff bye. |
| `away_team_standings_clinched_division` | Boolean | Away team standings: whether the team has clinched its division. |
| `away_team_standings_clinched_eliminated` | Boolean | Away team standings: whether the team has been mathematically eliminated from playoff contention. |
| `away_team_standings_clinched_home_field` | Boolean | Away team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `away_team_standings_clinched_playoff` | Boolean | Away team standings: whether the team has clinched a playoff berth. |
| `away_team_standings_clinched_wild_card` | Boolean | Away team standings: whether the team has clinched a wild-card playoff berth. |
| `away_team_standings_close_games_wins` | Int64 | Away team standings: wins in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_losses` | Int64 | Away team standings: losses in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_ties` | Int64 | Away team standings: ties in close games. |
| `away_team_standings_conference_wins` | Int64 | Away team standings: wins against conference (AFC/NFC) opponents. |
| `away_team_standings_conference_win_pct` | Float64 | Away team standings: win percentage against conference opponents. |
| `away_team_standings_conference_losses` | Int64 | Away team standings: losses against conference opponents. |
| `away_team_standings_conference_ties` | Int64 | Away team standings: ties against conference opponents. |
| `away_team_standings_conference_rank` | Int64 | Away team standings: standings rank within the conference. |
| `away_team_standings_conference_points_for` | Int64 | Away team standings: points scored in conference games. |
| `away_team_standings_conference_points_against` | Int64 | Away team standings: points allowed in conference games. |
| `away_team_standings_division_wins` | Int64 | Away team standings: wins against division opponents. |
| `away_team_standings_division_win_pct` | Float64 | Away team standings: win percentage against division opponents. |
| `away_team_standings_division_losses` | Int64 | Away team standings: losses against division opponents. |
| `away_team_standings_division_ties` | Int64 | Away team standings: ties against division opponents. |
| `away_team_standings_division_rank` | Int64 | Away team standings: standings rank within the division. |
| `away_team_standings_division_points_for` | Int64 | Away team standings: points scored in division games. |
| `away_team_standings_division_points_against` | Int64 | Away team standings: points allowed in division games. |
| `away_team_standings_home_wins` | Int64 | Away team standings: wins in home games. |
| `away_team_standings_home_win_pct` | Float64 | Away team standings: win percentage in home games. |
| `away_team_standings_home_losses` | Int64 | Away team standings: losses in home games. |
| `away_team_standings_home_ties` | Int64 | Away team standings: ties in home games. |
| `away_team_standings_home_points_for` | Int64 | Away team standings: points scored in home games. |
| `away_team_standings_home_points_against` | Int64 | Away team standings: points allowed in home games. |
| `away_team_standings_last5_wins` | Int64 | Away team standings: wins over the last five games. |
| `away_team_standings_last5_win_pct` | Float64 | Away team standings: win percentage over the last five games. |
| `away_team_standings_last5_losses` | Int64 | Away team standings: losses over the last five games. |
| `away_team_standings_last5_ties` | Int64 | Away team standings: ties over the last five games. |
| `away_team_standings_last5_points_for` | Int64 | Away team standings: points scored over the last five games. |
| `away_team_standings_last5_points_against` | Int64 | Away team standings: points allowed over the last five games. |
| `away_team_standings_overall_games` | Int64 | Away team standings: total games played. |
| `away_team_standings_overall_wins` | Int64 | Away team standings: total wins. |
| `away_team_standings_overall_win_pct` | Float64 | Away team standings: overall win percentage. |
| `away_team_standings_overall_losses` | Int64 | Away team standings: total losses. |
| `away_team_standings_overall_ties` | Int64 | Away team standings: total ties. |
| `away_team_standings_overall_points_for` | Int64 | Away team standings: total points scored. |
| `away_team_standings_overall_points_against` | Int64 | Away team standings: total points allowed. |
| `away_team_standings_overall_streak_type` | String | Away team standings: current streak type ("W" for winning, "L" for losing). |
| `away_team_standings_overall_streak_length` | Int64 | Away team standings: length of the current win/loss streak. |
| `away_team_standings_road_wins` | Int64 | Away team standings: wins in road (away) games. |
| `away_team_standings_road_win_pct` | Float64 | Away team standings: win percentage in road games. |
| `away_team_standings_road_losses` | Int64 | Away team standings: losses in road games. |
| `away_team_standings_road_ties` | Int64 | Away team standings: ties in road games. |
| `away_team_standings_road_points_for` | Int64 | Away team standings: points scored in road games. |
| `away_team_standings_road_points_against` | Int64 | Away team standings: points allowed in road games. |
| `home_team_standings_team_id` | String | Home team standings: nFL.com Shield GUID for the team. |
| `home_team_standings_team_current_logo` | String | Home team standings: templated URL of the team's current logo. |
| `home_team_standings_team_full_name` | String | Home team standings: full team name (e.g. "Arizona Cardinals"). |
| `home_team_standings_clinched_bye` | Boolean | Home team standings: whether the team has clinched a first-round playoff bye. |
| `home_team_standings_clinched_division` | Boolean | Home team standings: whether the team has clinched its division. |
| `home_team_standings_clinched_eliminated` | Boolean | Home team standings: whether the team has been mathematically eliminated from playoff contention. |
| `home_team_standings_clinched_home_field` | Boolean | Home team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `home_team_standings_clinched_playoff` | Boolean | Home team standings: whether the team has clinched a playoff berth. |
| `home_team_standings_clinched_wild_card` | Boolean | Home team standings: whether the team has clinched a wild-card playoff berth. |
| `home_team_standings_close_games_wins` | Int64 | Home team standings: wins in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_losses` | Int64 | Home team standings: losses in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_ties` | Int64 | Home team standings: ties in close games. |
| `home_team_standings_conference_wins` | Int64 | Home team standings: wins against conference (AFC/NFC) opponents. |
| `home_team_standings_conference_win_pct` | Float64 | Home team standings: win percentage against conference opponents. |
| `home_team_standings_conference_losses` | Int64 | Home team standings: losses against conference opponents. |
| `home_team_standings_conference_ties` | Int64 | Home team standings: ties against conference opponents. |
| `home_team_standings_conference_rank` | Int64 | Home team standings: standings rank within the conference. |
| `home_team_standings_conference_points_for` | Int64 | Home team standings: points scored in conference games. |
| `home_team_standings_conference_points_against` | Int64 | Home team standings: points allowed in conference games. |
| `home_team_standings_division_wins` | Int64 | Home team standings: wins against division opponents. |
| `home_team_standings_division_win_pct` | Float64 | Home team standings: win percentage against division opponents. |
| `home_team_standings_division_losses` | Int64 | Home team standings: losses against division opponents. |
| `home_team_standings_division_ties` | Int64 | Home team standings: ties against division opponents. |
| `home_team_standings_division_rank` | Int64 | Home team standings: standings rank within the division. |
| `home_team_standings_division_points_for` | Int64 | Home team standings: points scored in division games. |
| `home_team_standings_division_points_against` | Int64 | Home team standings: points allowed in division games. |
| `home_team_standings_home_wins` | Int64 | Home team standings: wins in home games. |
| `home_team_standings_home_win_pct` | Float64 | Home team standings: win percentage in home games. |
| `home_team_standings_home_losses` | Int64 | Home team standings: losses in home games. |
| `home_team_standings_home_ties` | Int64 | Home team standings: ties in home games. |
| `home_team_standings_home_points_for` | Int64 | Home team standings: points scored in home games. |
| `home_team_standings_home_points_against` | Int64 | Home team standings: points allowed in home games. |
| `home_team_standings_last5_wins` | Int64 | Home team standings: wins over the last five games. |
| `home_team_standings_last5_win_pct` | Float64 | Home team standings: win percentage over the last five games. |
| `home_team_standings_last5_losses` | Int64 | Home team standings: losses over the last five games. |
| `home_team_standings_last5_ties` | Int64 | Home team standings: ties over the last five games. |
| `home_team_standings_last5_points_for` | Int64 | Home team standings: points scored over the last five games. |
| `home_team_standings_last5_points_against` | Int64 | Home team standings: points allowed over the last five games. |
| `home_team_standings_overall_games` | Int64 | Home team standings: total games played. |
| `home_team_standings_overall_wins` | Int64 | Home team standings: total wins. |
| `home_team_standings_overall_win_pct` | Float64 | Home team standings: overall win percentage. |
| `home_team_standings_overall_losses` | Int64 | Home team standings: total losses. |
| `home_team_standings_overall_ties` | Int64 | Home team standings: total ties. |
| `home_team_standings_overall_points_for` | Int64 | Home team standings: total points scored. |
| `home_team_standings_overall_points_against` | Int64 | Home team standings: total points allowed. |
| `home_team_standings_overall_streak_type` | String | Home team standings: current streak type ("W" for winning, "L" for losing). |
| `home_team_standings_overall_streak_length` | Int64 | Home team standings: length of the current win/loss streak. |
| `home_team_standings_road_wins` | Int64 | Home team standings: wins in road (away) games. |
| `home_team_standings_road_win_pct` | Float64 | Home team standings: win percentage in road games. |
| `home_team_standings_road_losses` | Int64 | Home team standings: losses in road games. |
| `home_team_standings_road_ties` | Int64 | Home team standings: ties in road games. |
| `home_team_standings_road_points_for` | Int64 | Home team standings: points scored in road games. |
| `home_team_standings_road_points_against` | Int64 | Home team standings: points allowed in road games. |
| `drive_chart_game_id` | String | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | Int64 | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | String | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | String | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | String | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_game_details_by_slug-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_game_details_by_slug-example}

```python
nfl_game_details_by_slug(slug='broncos-at-chiefs-2026-reg-1')
```
