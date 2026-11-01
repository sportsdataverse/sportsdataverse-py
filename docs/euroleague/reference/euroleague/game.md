# EuroLeague — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Game

> EuroLeague — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Game — function reference in sdv-py, the SportsDataverse Python package.

## euroleague_game_boxscore

Box score of one game: per-player and team totals per side, by-quarter scores, referees, attendance.

**Endpoint URL:** `GET https://live.euroleague.net/api/Boxscore`

**Valid URL:** [https://live.euroleague.net/api/Boxscore?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/Boxscore?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  | `Y` |  | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  | `Y` |  | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `row_type` | String | Row kind: player, team (team-only rebounds) or total (side totals). |
| `team_name` | String | Club display name. |
| `coach` | String | Head coach name. |
| `attendance` | String | Attendance, as reported by the box score (repeated on every row). |
| `referees` | String | Referees of the game, comma-separated SURNAME, GIVEN NAME (repeated on every row). |
| `by_quarter_q1` | Int64 | Points the row's team scored in quarter 1 (from the box score's ByQuarter block). |
| `by_quarter_q2` | Int64 | Points the row's team scored in quarter 2 (from the box score's ByQuarter block). |
| `by_quarter_q3` | Int64 | Points the row's team scored in quarter 3 (from the box score's ByQuarter block). |
| `by_quarter_q4` | Int64 | Points the row's team scored in quarter 4 (from the box score's ByQuarter block). |
| `end_of_quarter_q1` | Int64 | Score of the row's team at the end of quarter 1 (cumulative; from the box score's EndOfQuarter block). |
| `end_of_quarter_q2` | Int64 | Score of the row's team at the end of quarter 2 (cumulative; from the box score's EndOfQuarter block). |
| `end_of_quarter_q3` | Int64 | Score of the row's team at the end of quarter 3 (cumulative; from the box score's EndOfQuarter block). |
| `end_of_quarter_q4` | Int64 | Score of the row's team at the end of quarter 4 (cumulative; from the box score's EndOfQuarter block). |
| `player_id` | String | EuroLeague player code (Utf8 join key; space padding stripped; blank on team rows). |
| `is_starter` | Int64 | Whether the player started (1 / 0). |
| `is_playing` | Int64 | Whether the player appeared in the game (1 / 0). |
| `team` | String | EuroLeague club code of the team (Utf8 join key; space padding stripped). |
| `dorsal` | String | Jersey number as displayed. |
| `player` | String | Player display name (SURNAME, GIVEN NAME). |
| `minutes` | String | Minutes played (mm:ss). |
| `points` | Int64 | Points. |
| `field_goals_made2` | Int64 | Two-point field goals made. |
| `field_goals_attempted2` | Int64 | Two-point field goals attempted. |
| `field_goals_made3` | Int64 | Three-point field goals made. |
| `field_goals_attempted3` | Int64 | Three-point field goals attempted. |
| `free_throws_made` | Int64 | Free throws made. |
| `free_throws_attempted` | Int64 | Free throws attempted. |
| `offensive_rebounds` | Int64 | Offensive rebounds. |
| `defensive_rebounds` | Int64 | Defensive rebounds. |
| `total_rebounds` | Int64 | Total rebounds. |
| `assistances` | Int64 | Assists. |
| `steals` | Int64 | Steals. |
| `turnovers` | Int64 | Turnovers. |
| `blocks_favour` | Int64 | Blocks made. |
| `blocks_against` | Int64 | Shots blocked by the opponent. |
| `fouls_commited` | Int64 | Personal fouls committed. |
| `fouls_received` | Int64 | Fouls drawn. |
| `valuation` | Int64 | Performance index rating (PIR). |
| `plusminus` | Int64 | Plus/minus. |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Raises {#euroleague_game_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_game_boxscore-example}

```python
euroleague_game_boxscore(game_code=1, season_code='E2025')
```

## euroleague_game_header

Game header: teams, codes, coaches, score by quarter, venue, referees.

**Endpoint URL:** `GET https://live.euroleague.net/api/Header`

**Valid URL:** [https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  | `Y` |  | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  | `Y` |  | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `live` | Boolean | Whether the game is in progress. |
| `round` | Int64 | Round number (Int64, as the v2 rounds and games routes carry it). |
| `date` | String | Game date (venue local, dd/mm/yyyy). |
| `hour` | String | Scheduled tip-off time (venue local, HH:MM). |
| `stadium` | String | Venue name. |
| `capacity` | String | Capacity as reported by the API (equals the box score's attendance on the captured game; semantics unverified). |
| `team_a` | String | Display name of team A (= the home side, measured on one game). |
| `team_b` | String | Display name of team B (= the away side, measured on one game). |
| `code_team_a` | String | EuroLeague club code of team A (= the home side, measured on one game); Utf8 join key. |
| `tv_code_a` | String | Three-letter broadcast abbreviation of team A. |
| `code_team_b` | String | EuroLeague club code of team B (= the away side, measured on one game); Utf8 join key. |
| `tv_code_b` | String | Three-letter broadcast abbreviation of team B. |
| `im_a` | String | Crest image file name of team A. |
| `im_b` | String | Crest image file name of team B. |
| `score_a` | String | Score of team A (= the home side, measured on one game). |
| `score_b` | String | Score of team B (= the away side, measured on one game). |
| `coach_a` | String | Head coach of team A. |
| `coach_b` | String | Head coach of team B. |
| `game_time` | String | Elapsed game time (mm:ss). |
| `remaining_partial_time` | String | Time remaining in the current period (mm:ss). |
| `wid` | String | Live-feed widget identifier of the game. |
| `quarter` | String | Current period of the game. |
| `foults_a` | String | Team fouls of team A in the current period (sic: the API spells it this way). |
| `foults_b` | String | Team fouls of team B in the current period (sic: the API spells it this way). |
| `timeouts_a` | String | Timeouts used by team A. |
| `timeouts_b` | String | Timeouts used by team B. |
| `score_quarter1_a` | Int64 | Score of team A at the end of quarter 1 (cumulative). |
| `score_quarter2_a` | Int64 | Score of team A at the end of quarter 2 (cumulative). |
| `score_quarter3_a` | Int64 | Score of team A at the end of quarter 3 (cumulative). |
| `score_quarter4_a` | Int64 | Score of team A at the end of quarter 4 (cumulative). |
| `score_extra_time_a` | Int64 | Points scored by team A in overtime (0 when none). |
| `score_quarter1_b` | Int64 | Score of team B at the end of quarter 1 (cumulative). |
| `score_quarter2_b` | Int64 | Score of team B at the end of quarter 2 (cumulative). |
| `score_quarter3_b` | Int64 | Score of team B at the end of quarter 3 (cumulative). |
| `score_quarter4_b` | Int64 | Score of team B at the end of quarter 4 (cumulative). |
| `score_extra_time_b` | Int64 | Points scored by team B in overtime (0 when none). |
| `phase` | String | Phase name (Regular Season, Playoffs, ...). |
| `phase_reduced_name` | String | Short phase name. |
| `competition` | String | Competition name. |
| `competition_reduced_name` | String | Short competition name. |
| `pcom` | String | Competition code of the live feed (E = EuroLeague, U = EuroCup). |
| `referee1` | String | First referee. |
| `referee2` | String | Second referee. |
| `referee3` | String | Third referee. |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Raises {#euroleague_game_header-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_game_header-example}

```python
euroleague_game_header(game_code=1, season_code='E2025')
```

## euroleague_game_pbp

Play-by-play of one game, one array per quarter (FirstQuarter ... ForthQuarter, ExtraTime).

**Endpoint URL:** `GET https://live.euroleague.net/api/PlayByPlay`

**Valid URL:** [https://live.euroleague.net/api/PlayByPlay?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/PlayByPlay?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  | `Y` |  | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  | `Y` |  | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `quarter` | Int64 | Period of the play: 1-4, or 5 for every overtime period. |
| `team_a` | String | Display name of team A (= the home side, measured on one game). |
| `team_b` | String | Display name of team B (= the away side, measured on one game). |
| `code_team_a` | String | EuroLeague club code of team A (= the home side, measured on one game); Utf8 join key. |
| `code_team_b` | String | EuroLeague club code of team B (= the away side, measured on one game); Utf8 join key. |
| `type` | Int64 | Play type (engine integer). |
| `numberofplay` | Int64 | Sequence number of the play within the game. |
| `codeteam` | String | EuroLeague club code of the team on the play (Utf8 join key; blank on administrative plays). |
| `player_id` | String | EuroLeague player code (Utf8 join key; space padding stripped; blank on team rows). |
| `playtype` | String | Play type code (BP = begin period, 2FGM, 3FGA, FTM, AS = assist, TO, RV, CM, ...). |
| `player` | String | Player display name (SURNAME, GIVEN NAME). |
| `team` | String | Display name of the team on the play (null on administrative plays). |
| `dorsal` | String | Jersey number as displayed. |
| `minute` | Int64 | Game minute of the event (1-based; 41+ in overtime). |
| `markertime` | String | Game clock at the play (mm:ss remaining in the period). |
| `points_a` | Int64 | Running score of team A (= the home side, measured on one game) after the event. |
| `points_b` | Int64 | Running score of team B (= the away side, measured on one game) after the event. |
| `comment` | String | Free-text annotation of the play. |
| `playinfo` | String | Play description. |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Raises {#euroleague_game_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_game_pbp-example}

```python
euroleague_game_pbp(game_code=1, season_code='E2025')
```

## euroleague_game_points

Shot chart of one game: one row per made/missed FG and made FT, with COORD_X/COORD_Y in cm from the hoop (the shot-chart source).

**Endpoint URL:** `GET https://live.euroleague.net/api/Points`

**Valid URL:** [https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  | `Y` |  | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  | `Y` |  | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_points-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `num_anot` | Int64 | Sequence number of the scoring annotation within the game. |
| `team` | String | EuroLeague club code of the team (Utf8 join key; space padding stripped). |
| `id_player` | String | EuroLeague player code (Utf8 join key; space padding stripped). |
| `player` | String | Player display name (SURNAME, GIVEN NAME). |
| `id_action` | String | Shot type code: 2FGM / 2FGA / 3FGM / 3FGA (made / attempted field goal) or FTM (made free throw). |
| `action` | String | Shot type label (Two Pointer, Three Pointer, Free Throw In, ...). |
| `points` | Int64 | Points the shot is worth (1, 2 or 3). |
| `coord_x` | Int64 | Shot x in integer centimeters from the hoop, signed left/right of it (-683 to 696 cm measured): both teams are mapped onto one basket; 3-point zone H is x < 0 and I is x > 0; which sideline is positive (from the shooter's view or from the scorer's table) is UNVERIFIED from the data alone. Made free throws (FTM) carry the -1 sentinel. |
| `coord_y` | Int64 | Shot y in integer centimeters from the hoop, growing away from the baseline toward the court (-6 to 865 cm measured; 3FG rows at 414-865): both teams are mapped onto one basket, negative y is behind the hoop. Made free throws (FTM) carry the -1 sentinel. |
| `zone` | String | Court zone letter: A rim, B/C close left/right, D/E mid, F/G long two, H/I three; blank on free throws. |
| `fastbreak` | String | Whether the shot came on a fast break (0 / 1 as a string). |
| `second_chance` | String | Whether the shot was a second-chance attempt (0 / 1 as a string). |
| `points_off_turnover` | String | Whether the shot came off a turnover (0 / 1 as a string). |
| `minute` | Int64 | Game minute of the event (1-based; 41+ in overtime). |
| `console` | String | Game clock at the event (mm:ss remaining in the period). |
| `points_a` | Int64 | Running score of team A (= the home side, measured on one game) after the event. |
| `points_b` | Int64 | Running score of team B (= the away side, measured on one game) after the event. |
| `utc` | String | UTC timestamp of the event (yyyymmddHHMMSS). |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Raises {#euroleague_game_points-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_game_points-example}

```python
euroleague_game_points(game_code=1, season_code='E2025')
```

## euroleague_game_report

Game report: date, round, phase, both clubs with score and last-5 form.

**Endpoint URL:** `GET https://api-live.euroleague.net/v3/competitions/{competition_code}/seasons/{season_code}/games/{game_code}/report`

**Valid URL:** [https://api-live.euroleague.net/v3/competitions/E/seasons/E2025/games/1/report](https://api-live.euroleague.net/v3/competitions/E/seasons/E2025/games/1/report)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |
| `game_code` | `game_code` |  | `Y` |  | Game number within the season (1-based). |

### Returns {#euroleague_game_report-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_code` | String | Game number within the season (1-based; Utf8 join key). |
| `round` | Int64 | Round number within the season. |
| `round_alias` | String | Short alias of the round. |
| `round_name` | String | Display name of the round. |
| `played` | Boolean | Whether the game has been played. |
| `date` | String | Scheduled tip-off (ISO 8601, venue local time). |
| `confirmed_date` | Boolean | Whether the game date is confirmed. |
| `confirmed_hour` | Boolean | Whether the tip-off time is confirmed. |
| `local_time_zone` | Int64 | UTC offset of the venue, in hours. |
| `local_date` | String | Scheduled tip-off in venue local time (ISO 8601). |
| `utc_date` | String | Scheduled tip-off in UTC (ISO 8601). |
| `local_last5_form` | String | Home side: results of the last five games, oldest first (W / L), JSON-encoded. |
| `road_last5_form` | String | Away side: results of the last five games, oldest first (W / L), JSON-encoded. |
| `season_name` | String | Season: display name. |
| `season_code` | String | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `season_alias` | String | Season: short display alias. |
| `season_competition_code` | String | Season: competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `season_year` | Int64 | Season: start year of the season. |
| `season_start_date` | String | Season: start date (ISO 8601). |
| `group_id` | String | Group: provider identifier for the entity (Utf8 join key). |
| `group_order` | Int64 | Group: display order within the roster. |
| `group_name` | String | Group: display name. |
| `group_raw_name` | String | Group: group name as stored by the engine. |
| `phase_type_code` | String | Phase type code (RS = regular season, PO = playoffs, ...). |
| `phase_type_alias` | String | Phase type: short display alias. |
| `phase_type_name` | String | Phase type: display name. |
| `phase_type_is_group_phase` | Boolean | Phase type: whether the phase is played in groups. |
| `local_club_code` | String | Home side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `local_club_name` | String | Home side: club: display name. |
| `local_club_abbreviated_name` | String | Home side: club: abbreviated display name. |
| `local_club_editorial_name` | String | Home side: club: editorial (long-form) display name. |
| `local_club_tv_code` | String | Home side: club: three-letter broadcast abbreviation of the club. |
| `local_club_is_virtual` | Boolean | Home side: club: whether the club is a placeholder rather than a real club. |
| `local_club_images_crest` | String | Home side: club: URL of the club crest image. |
| `local_score` | Int64 | Home side: final score of the side. |
| `local_standings_score` | Int64 | Home side: score of the side as counted for the standings. |
| `road_club_code` | String | Away side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `road_club_name` | String | Away side: club: display name. |
| `road_club_abbreviated_name` | String | Away side: club: abbreviated display name. |
| `road_club_editorial_name` | String | Away side: club: editorial (long-form) display name. |
| `road_club_tv_code` | String | Away side: club: three-letter broadcast abbreviation of the club. |
| `road_club_is_virtual` | Boolean | Away side: club: whether the club is a placeholder rather than a real club. |
| `road_club_images_crest` | String | Away side: club: URL of the club crest image. |
| `road_score` | Int64 | Away side: final score of the side. |
| `road_standings_score` | Int64 | Away side: score of the side as counted for the standings. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_game_report-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_game_report-example}

```python
euroleague_game_report(competition_code='E', game_code=1, season_code='E2025')
```

## euroleague_game_stats

Box score of one game.

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons/{season_code}/games/{game_code}/stats`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games/1/stats](https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games/1/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |
| `game_code` | `game_code` |  | `Y` |  | Game number within the season (1-based). |

### Returns {#euroleague_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `local_coach_code` | String | Home side: head coach code (Utf8 join key). |
| `local_coach_name` | String | Home side: head coach name. |
| `local_players` | String | Home side: per-player box-score rows for the side, JSON-encoded. |
| `local_team_time_played` | Float64 | Home side: team-only (not attributed to a player): seconds played. |
| `local_team_valuation` | Float64 | Home side: team-only (not attributed to a player): performance index rating (PIR). |
| `local_team_points` | Float64 | Home side: team-only (not attributed to a player): points. |
| `local_team_field_goals_made2` | Float64 | Home side: team-only (not attributed to a player): two-point field goals made. |
| `local_team_field_goals_attempted2` | Float64 | Home side: team-only (not attributed to a player): two-point field goals attempted. |
| `local_team_field_goals_made3` | Float64 | Home side: team-only (not attributed to a player): three-point field goals made. |
| `local_team_field_goals_attempted3` | Float64 | Home side: team-only (not attributed to a player): three-point field goals attempted. |
| `local_team_free_throws_made` | Float64 | Home side: team-only (not attributed to a player): free throws made. |
| `local_team_free_throws_attempted` | Float64 | Home side: team-only (not attributed to a player): free throws attempted. |
| `local_team_field_goals_made_total` | Float64 | Home side: team-only (not attributed to a player): field goals made. |
| `local_team_field_goals_attempted_total` | Float64 | Home side: team-only (not attributed to a player): field goals attempted. |
| `local_team_accuracy_made` | Float64 | Home side: team-only (not attributed to a player): shots made (accuracy numerator). |
| `local_team_accuracy_attempted` | Float64 | Home side: team-only (not attributed to a player): shots attempted (accuracy denominator). |
| `local_team_total_rebounds` | Float64 | Home side: team-only (not attributed to a player): total rebounds. |
| `local_team_defensive_rebounds` | Float64 | Home side: team-only (not attributed to a player): defensive rebounds. |
| `local_team_offensive_rebounds` | Float64 | Home side: team-only (not attributed to a player): offensive rebounds. |
| `local_team_assistances` | Float64 | Home side: team-only (not attributed to a player): assists. |
| `local_team_steals` | Float64 | Home side: team-only (not attributed to a player): steals. |
| `local_team_turnovers` | Float64 | Home side: team-only (not attributed to a player): turnovers. |
| `local_team_blocks_favour` | Float64 | Home side: team-only (not attributed to a player): blocks made. |
| `local_team_blocks_against` | Float64 | Home side: team-only (not attributed to a player): shots blocked by the opponent. |
| `local_team_fouls_commited` | Float64 | Home side: team-only (not attributed to a player): personal fouls committed. |
| `local_team_fouls_received` | Float64 | Home side: team-only (not attributed to a player): fouls drawn. |
| `local_team_plus_minus` | Float64 | Home side: team-only (not attributed to a player): plus/minus. |
| `local_total_time_played` | Float64 | Home side: totals: seconds played. |
| `local_total_valuation` | Float64 | Home side: totals: performance index rating (PIR). |
| `local_total_points` | Float64 | Home side: totals: points. |
| `local_total_field_goals_made2` | Float64 | Home side: totals: two-point field goals made. |
| `local_total_field_goals_attempted2` | Float64 | Home side: totals: two-point field goals attempted. |
| `local_total_field_goals_made3` | Float64 | Home side: totals: three-point field goals made. |
| `local_total_field_goals_attempted3` | Float64 | Home side: totals: three-point field goals attempted. |
| `local_total_free_throws_made` | Float64 | Home side: totals: free throws made. |
| `local_total_free_throws_attempted` | Float64 | Home side: totals: free throws attempted. |
| `local_total_field_goals_made_total` | Float64 | Home side: totals: field goals made. |
| `local_total_field_goals_attempted_total` | Float64 | Home side: totals: field goals attempted. |
| `local_total_accuracy_made` | Float64 | Home side: totals: shots made (accuracy numerator). |
| `local_total_accuracy_attempted` | Float64 | Home side: totals: shots attempted (accuracy denominator). |
| `local_total_total_rebounds` | Float64 | Home side: totals: total rebounds. |
| `local_total_defensive_rebounds` | Float64 | Home side: totals: defensive rebounds. |
| `local_total_offensive_rebounds` | Float64 | Home side: totals: offensive rebounds. |
| `local_total_assistances` | Float64 | Home side: totals: assists. |
| `local_total_steals` | Float64 | Home side: totals: steals. |
| `local_total_turnovers` | Float64 | Home side: totals: turnovers. |
| `local_total_blocks_favour` | Float64 | Home side: totals: blocks made. |
| `local_total_blocks_against` | Float64 | Home side: totals: shots blocked by the opponent. |
| `local_total_fouls_commited` | Float64 | Home side: totals: personal fouls committed. |
| `local_total_fouls_received` | Float64 | Home side: totals: fouls drawn. |
| `local_total_plus_minus` | Float64 | Home side: totals: plus/minus. |
| `road_coach_code` | String | Away side: head coach code (Utf8 join key). |
| `road_coach_name` | String | Away side: head coach name. |
| `road_players` | String | Away side: per-player box-score rows for the side, JSON-encoded. |
| `road_team_time_played` | Float64 | Away side: team-only (not attributed to a player): seconds played. |
| `road_team_valuation` | Float64 | Away side: team-only (not attributed to a player): performance index rating (PIR). |
| `road_team_points` | Float64 | Away side: team-only (not attributed to a player): points. |
| `road_team_field_goals_made2` | Float64 | Away side: team-only (not attributed to a player): two-point field goals made. |
| `road_team_field_goals_attempted2` | Float64 | Away side: team-only (not attributed to a player): two-point field goals attempted. |
| `road_team_field_goals_made3` | Float64 | Away side: team-only (not attributed to a player): three-point field goals made. |
| `road_team_field_goals_attempted3` | Float64 | Away side: team-only (not attributed to a player): three-point field goals attempted. |
| `road_team_free_throws_made` | Float64 | Away side: team-only (not attributed to a player): free throws made. |
| `road_team_free_throws_attempted` | Float64 | Away side: team-only (not attributed to a player): free throws attempted. |
| `road_team_field_goals_made_total` | Float64 | Away side: team-only (not attributed to a player): field goals made. |
| `road_team_field_goals_attempted_total` | Float64 | Away side: team-only (not attributed to a player): field goals attempted. |
| `road_team_accuracy_made` | Float64 | Away side: team-only (not attributed to a player): shots made (accuracy numerator). |
| `road_team_accuracy_attempted` | Float64 | Away side: team-only (not attributed to a player): shots attempted (accuracy denominator). |
| `road_team_total_rebounds` | Float64 | Away side: team-only (not attributed to a player): total rebounds. |
| `road_team_defensive_rebounds` | Float64 | Away side: team-only (not attributed to a player): defensive rebounds. |
| `road_team_offensive_rebounds` | Float64 | Away side: team-only (not attributed to a player): offensive rebounds. |
| `road_team_assistances` | Float64 | Away side: team-only (not attributed to a player): assists. |
| `road_team_steals` | Float64 | Away side: team-only (not attributed to a player): steals. |
| `road_team_turnovers` | Float64 | Away side: team-only (not attributed to a player): turnovers. |
| `road_team_blocks_favour` | Float64 | Away side: team-only (not attributed to a player): blocks made. |
| `road_team_blocks_against` | Float64 | Away side: team-only (not attributed to a player): shots blocked by the opponent. |
| `road_team_fouls_commited` | Float64 | Away side: team-only (not attributed to a player): personal fouls committed. |
| `road_team_fouls_received` | Float64 | Away side: team-only (not attributed to a player): fouls drawn. |
| `road_team_plus_minus` | Float64 | Away side: team-only (not attributed to a player): plus/minus. |
| `road_total_time_played` | Float64 | Away side: totals: seconds played. |
| `road_total_valuation` | Float64 | Away side: totals: performance index rating (PIR). |
| `road_total_points` | Float64 | Away side: totals: points. |
| `road_total_field_goals_made2` | Float64 | Away side: totals: two-point field goals made. |
| `road_total_field_goals_attempted2` | Float64 | Away side: totals: two-point field goals attempted. |
| `road_total_field_goals_made3` | Float64 | Away side: totals: three-point field goals made. |
| `road_total_field_goals_attempted3` | Float64 | Away side: totals: three-point field goals attempted. |
| `road_total_free_throws_made` | Float64 | Away side: totals: free throws made. |
| `road_total_free_throws_attempted` | Float64 | Away side: totals: free throws attempted. |
| `road_total_field_goals_made_total` | Float64 | Away side: totals: field goals made. |
| `road_total_field_goals_attempted_total` | Float64 | Away side: totals: field goals attempted. |
| `road_total_accuracy_made` | Float64 | Away side: totals: shots made (accuracy numerator). |
| `road_total_accuracy_attempted` | Float64 | Away side: totals: shots attempted (accuracy denominator). |
| `road_total_total_rebounds` | Float64 | Away side: totals: total rebounds. |
| `road_total_defensive_rebounds` | Float64 | Away side: totals: defensive rebounds. |
| `road_total_offensive_rebounds` | Float64 | Away side: totals: offensive rebounds. |
| `road_total_assistances` | Float64 | Away side: totals: assists. |
| `road_total_steals` | Float64 | Away side: totals: steals. |
| `road_total_turnovers` | Float64 | Away side: totals: turnovers. |
| `road_total_blocks_favour` | Float64 | Away side: totals: blocks made. |
| `road_total_blocks_against` | Float64 | Away side: totals: shots blocked by the opponent. |
| `road_total_fouls_commited` | Float64 | Away side: totals: personal fouls committed. |
| `road_total_fouls_received` | Float64 | Away side: totals: fouls drawn. |
| `road_total_plus_minus` | Float64 | Away side: totals: plus/minus. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_game_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_game_stats-example}

```python
euroleague_game_stats(competition_code='E', game_code=1, season_code='E2025')
```
