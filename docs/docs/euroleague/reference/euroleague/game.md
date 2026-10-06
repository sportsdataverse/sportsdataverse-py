---
title: "EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Game

## euroleague_game_boxscore

Box score of one game: per-player and team totals per side, by-quarter scores, referees, attendance.

**Endpoint URL:** `GET https://live.euroleague.net/api/Boxscore`

**Valid URL:** [https://live.euroleague.net/api/Boxscore?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/Boxscore?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  |  | `Y` | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  |  | `Y` | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `row_type` | character | Row kind: player, team (team-only rebounds) or total (side totals). |
| `team_name` | character | Club display name. |
| `coach` | character | Head coach name. |
| `player_id` | character | EuroLeague player code (Utf8 join key; space padding stripped; blank on team rows). |
| `is_starter` | numeric | Whether the player started (1 / 0). |
| `is_playing` | numeric | Whether the player appeared in the game (1 / 0). |
| `team` | character | EuroLeague club code of the team (Utf8 join key; space padding stripped). |
| `dorsal` | character | Jersey number as displayed. |
| `player` | character | Player display name (SURNAME, GIVEN NAME). |
| `minutes` | character | Minutes played (mm:ss). |
| `points` | integer | Points. |
| `field_goals_made2` | integer | Two-point field goals made. |
| `field_goals_attempted2` | integer | Two-point field goals attempted. |
| `field_goals_made3` | integer | Three-point field goals made. |
| `field_goals_attempted3` | integer | Three-point field goals attempted. |
| `free_throws_made` | integer | Free throws made. |
| `free_throws_attempted` | integer | Free throws attempted. |
| `offensive_rebounds` | integer | Offensive rebounds. |
| `defensive_rebounds` | integer | Defensive rebounds. |
| `total_rebounds` | integer | Total rebounds. |
| `assistances` | integer | Assists. |
| `steals` | integer | Steals. |
| `turnovers` | integer | Turnovers. |
| `blocks_favour` | integer | Blocks made. |
| `blocks_against` | integer | Shots blocked by the opponent. |
| `fouls_commited` | integer | Personal fouls committed. |
| `fouls_received` | integer | Fouls drawn. |
| `valuation` | integer | Performance index rating (PIR). |
| `plusminus` | numeric | Plus/minus. |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Example {#euroleague_game_boxscore-example}

```python
euroleague_game_boxscore(game_code=1, season_code='E2025')
```

_Last validated n/a._

## euroleague_game_header

Game header: teams, codes, coaches, score by quarter, venue, referees.

**Endpoint URL:** `GET https://live.euroleague.net/api/Header`

**Valid URL:** [https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  |  | `Y` | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  |  | `Y` | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `live` | logical | Whether the game is in progress. |
| `round` | character | Round label (e.g. Round 1). |
| `date` | character | Game date (venue local, dd/mm/yyyy). |
| `hour` | character | Scheduled tip-off time (venue local, HH:MM). |
| `stadium` | character | Venue name. |
| `capacity` | character | Seating capacity of the venue. |
| `team_a` | character | Display name of team A (the home side). |
| `team_b` | character | Display name of team B (the away side). |
| `code_team_a` | character | EuroLeague club code of team A (the home side; Utf8 join key). |
| `tv_code_a` | character | Three-letter broadcast abbreviation of team A. |
| `code_team_b` | character | EuroLeague club code of team B (the away side; Utf8 join key). |
| `tv_code_b` | character | Three-letter broadcast abbreviation of team B. |
| `im_a` | character | Crest image file name of team A. |
| `im_b` | character | Crest image file name of team B. |
| `score_a` | character | Score of team A (the home side). |
| `score_b` | character | Score of team B (the away side). |
| `coach_a` | character | Head coach of team A. |
| `coach_b` | character | Head coach of team B. |
| `game_time` | character | Elapsed game time (mm:ss). |
| `remaining_partial_time` | character | Time remaining in the current period (mm:ss). |
| `wid` | character | Live-feed widget identifier of the game. |
| `quarter` | character | Current period of the game. |
| `foults_a` | character | Team fouls of team A in the current period (sic: the API spells it this way). |
| `foults_b` | character | Team fouls of team B in the current period (sic: the API spells it this way). |
| `timeouts_a` | character | Timeouts used by team A. |
| `timeouts_b` | character | Timeouts used by team B. |
| `score_quarter1_a` | integer | Points scored by team A (the home side) in quarter 1. |
| `score_quarter2_a` | integer | Points scored by team A (the home side) in quarter 2. |
| `score_quarter3_a` | integer | Points scored by team A (the home side) in quarter 3. |
| `score_quarter4_a` | integer | Points scored by team A (the home side) in quarter 4. |
| `score_extra_time_a` | integer | Points scored by team A in overtime. |
| `score_quarter1_b` | integer | Points scored by team B (the away side) in quarter 1. |
| `score_quarter2_b` | integer | Points scored by team B (the away side) in quarter 2. |
| `score_quarter3_b` | integer | Points scored by team B (the away side) in quarter 3. |
| `score_quarter4_b` | integer | Points scored by team B (the away side) in quarter 4. |
| `score_extra_time_b` | integer | Points scored by team B in overtime. |
| `phase` | character | Phase name (Regular Season, Playoffs, ...). |
| `phase_reduced_name` | character | Short phase name. |
| `competition` | character | Competition name. |
| `competition_reduced_name` | character | Short competition name. |
| `pcom` | character | Competition code of the live feed (E = EuroLeague, U = EuroCup). |
| `referee1` | character | First referee. |
| `referee2` | character | Second referee. |
| `referee3` | character | Third referee. |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Example {#euroleague_game_header-example}

```python
euroleague_game_header(game_code=1, season_code='E2025')
```

_Last validated n/a._

## euroleague_game_pbp

Play-by-play of one game, one array per quarter (FirstQuarter ... ForthQuarter, ExtraTime).

**Endpoint URL:** `GET https://live.euroleague.net/api/PlayByPlay`

**Valid URL:** [https://live.euroleague.net/api/PlayByPlay?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/PlayByPlay?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  |  | `Y` | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  |  | `Y` | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `quarter` | integer | Period of the play: 1-4, or 5 for every overtime period. |
| `team_a` | character | Display name of team A (the home side). |
| `team_b` | character | Display name of team B (the away side). |
| `code_team_a` | character | EuroLeague club code of team A (the home side; Utf8 join key). |
| `code_team_b` | character | EuroLeague club code of team B (the away side; Utf8 join key). |
| `type` | integer | Play type (engine integer). |
| `numberofplay` | integer | Sequence number of the play within the game. |
| `codeteam` | character | EuroLeague club code of the team on the play (Utf8 join key; blank on administrative plays). |
| `player_id` | character | EuroLeague player code (Utf8 join key; space padding stripped; blank on team rows). |
| `playtype` | character | Play type code (BP = begin period, 2FGM, 3FGA, FTM, AS = assist, TO, RV, CM, ...). |
| `player` | character | Player display name (SURNAME, GIVEN NAME). |
| `team` | character | Display name of the team on the play (null on administrative plays). |
| `dorsal` | character | Jersey number as displayed. |
| `minute` | integer | Game minute of the event (1-based; 41+ in overtime). |
| `markertime` | character | Game clock at the play (mm:ss remaining in the period). |
| `points_a` | integer | Running score of team A (the home side) after the event. |
| `points_b` | integer | Running score of team B (the away side) after the event. |
| `comment` | character | Free-text annotation of the play. |
| `playinfo` | character | Play description. |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Example {#euroleague_game_pbp-example}

```python
euroleague_game_pbp(game_code=1, season_code='E2025')
```

_Last validated n/a._

## euroleague_game_points

Shot chart of one game: one row per made/missed FG and made FT, with COORD_X/COORD_Y in cm from the hoop (the shot-chart source).

**Endpoint URL:** `GET https://live.euroleague.net/api/Points`

**Valid URL:** [https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2025](https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gamecode` | `game_code` |  |  | `Y` | Required. Game number within the season (1-based). |
| `seasoncode` | `season_code` |  |  | `Y` | Required. Competition code + start year: E2025 (EuroLeague 2025-26), U2025 (EuroCup). |

### Returns {#euroleague_game_points-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `num_anot` | integer | Sequence number of the scoring annotation within the game. |
| `team` | character | EuroLeague club code of the team (Utf8 join key; space padding stripped). |
| `id_player` | character | EuroLeague player code (Utf8 join key; space padding stripped). |
| `player` | character | Player display name (SURNAME, GIVEN NAME). |
| `id_action` | character | Shot type code: 2FGM / 2FGA / 3FGM / 3FGA (made / attempted field goal) or FTM (made free throw). |
| `action` | character | Shot type label (Two Pointer, Three Pointer, Free Throw In, ...). |
| `points` | integer | Points the shot is worth (1, 2 or 3). |
| `coord_x` | integer | Shot x in integer centimeters from the hoop, signed left/right of it (-683 to 696 cm measured): both teams are mapped onto one basket; 3-point zone H is x < 0 and I is x > 0; which sideline is positive (from the shooter's view or from the scorer's table) is UNVERIFIED from the data alone. Made free throws (FTM) carry the -1 sentinel. |
| `coord_y` | integer | Shot y in integer centimeters from the hoop, growing away from the baseline toward the court (-6 to 865 cm measured; 3FG rows at 414-865): both teams are mapped onto one basket, negative y is behind the hoop. Made free throws (FTM) carry the -1 sentinel. |
| `zone` | character | Court zone letter: A rim, B/C close left/right, D/E mid, F/G long two, H/I three; blank on free throws. |
| `fastbreak` | character | Whether the shot came on a fast break (0 / 1 as a string). |
| `second_chance` | character | Whether the shot was a second-chance attempt (0 / 1 as a string). |
| `points_off_turnover` | character | Whether the shot came off a turnover (0 / 1 as a string). |
| `minute` | integer | Game minute of the event (1-based; 41+ in overtime). |
| `console` | character | Game clock at the event (mm:ss remaining in the period). |
| `points_a` | integer | Running score of team A (the home side) after the event. |
| `points_b` | integer | Running score of team B (the away side) after the event. |
| `utc` | character | UTC timestamp of the event (yyyymmddHHMMSS). |

**`return_parsed=False`** — the raw JSON `Dict` (`{}` when the live API answers its empty-body "no such game" sentinel, which the parser turns into a zero-row frame).

### Example {#euroleague_game_points-example}

```python
euroleague_game_points(game_code=1, season_code='E2025')
```

_Last validated n/a._

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
| `game_code` | character | Game number within the season (1-based; Utf8 join key). |
| `round` | integer | Round number within the season. |
| `round_alias` | character | Short alias of the round. |
| `round_name` | character | Display name of the round. |
| `played` | logical | Whether the game has been played. |
| `date` | character | Scheduled tip-off (ISO 8601, venue local time). |
| `confirmed_date` | logical | Whether the game date is confirmed. |
| `confirmed_hour` | logical | Whether the tip-off time is confirmed. |
| `local_time_zone` | integer | UTC offset of the venue, in hours. |
| `local_date` | character | Scheduled tip-off in venue local time (ISO 8601). |
| `utc_date` | character | Scheduled tip-off in UTC (ISO 8601). |
| `local_last5_form` | character | Home side: results of the last five games, oldest first (W / L), JSON-encoded. |
| `road_last5_form` | character | Away side: results of the last five games, oldest first (W / L), JSON-encoded. |
| `season_name` | character | Season: display name. |
| `season_code` | character | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `season_alias` | character | Season: short display alias. |
| `season_competition_code` | character | Season: competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `season_year` | integer | Season: start year of the season. |
| `season_start_date` | character | Season: start date (ISO 8601). |
| `group_id` | character | Group: provider identifier for the entity (Utf8 join key). |
| `group_order` | integer | Group: display order within the roster. |
| `group_name` | character | Group: display name. |
| `group_raw_name` | character | Group: group name as stored by the engine. |
| `phase_type_code` | character | Phase type code (RS = regular season, PO = playoffs, ...). |
| `phase_type_alias` | character | Phase type: short display alias. |
| `phase_type_name` | character | Phase type: display name. |
| `phase_type_is_group_phase` | logical | Phase type: whether the phase is played in groups. |
| `local_club_code` | character | Home side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `local_club_name` | character | Home side: club: display name. |
| `local_club_abbreviated_name` | character | Home side: club: abbreviated display name. |
| `local_club_editorial_name` | character | Home side: club: editorial (long-form) display name. |
| `local_club_tv_code` | character | Home side: club: three-letter broadcast abbreviation of the club. |
| `local_club_is_virtual` | logical | Home side: club: whether the club is a placeholder rather than a real club. |
| `local_club_images_crest` | character | Home side: club: URL of the club crest image. |
| `local_score` | integer | Home side: final score of the side. |
| `local_standings_score` | integer | Home side: score of the side as counted for the standings. |
| `road_club_code` | character | Away side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `road_club_name` | character | Away side: club: display name. |
| `road_club_abbreviated_name` | character | Away side: club: abbreviated display name. |
| `road_club_editorial_name` | character | Away side: club: editorial (long-form) display name. |
| `road_club_tv_code` | character | Away side: club: three-letter broadcast abbreviation of the club. |
| `road_club_is_virtual` | logical | Away side: club: whether the club is a placeholder rather than a real club. |
| `road_club_images_crest` | character | Away side: club: URL of the club crest image. |
| `road_score` | integer | Away side: final score of the side. |
| `road_standings_score` | integer | Away side: score of the side as counted for the standings. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_game_report-example}

```python
euroleague_game_report(competition_code='E', game_code=1, season_code='E2025')
```

_Last validated n/a._

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
| `local_coach_code` | character | Home side: head coach code (Utf8 join key). |
| `local_coach_name` | character | Home side: head coach name. |
| `local_players` | character | Home side: per-player box-score rows for the side, JSON-encoded. |
| `local_team_time_played` | numeric | Home side: team-only (not attributed to a player): seconds played. |
| `local_team_valuation` | numeric | Home side: team-only (not attributed to a player): performance index rating (PIR). |
| `local_team_points` | numeric | Home side: team-only (not attributed to a player): points. |
| `local_team_field_goals_made2` | numeric | Home side: team-only (not attributed to a player): two-point field goals made. |
| `local_team_field_goals_attempted2` | numeric | Home side: team-only (not attributed to a player): two-point field goals attempted. |
| `local_team_field_goals_made3` | numeric | Home side: team-only (not attributed to a player): three-point field goals made. |
| `local_team_field_goals_attempted3` | numeric | Home side: team-only (not attributed to a player): three-point field goals attempted. |
| `local_team_free_throws_made` | numeric | Home side: team-only (not attributed to a player): free throws made. |
| `local_team_free_throws_attempted` | numeric | Home side: team-only (not attributed to a player): free throws attempted. |
| `local_team_field_goals_made_total` | numeric | Home side: team-only (not attributed to a player): field goals made. |
| `local_team_field_goals_attempted_total` | numeric | Home side: team-only (not attributed to a player): field goals attempted. |
| `local_team_accuracy_made` | numeric | Home side: team-only (not attributed to a player): shots made (accuracy numerator). |
| `local_team_accuracy_attempted` | numeric | Home side: team-only (not attributed to a player): shots attempted (accuracy denominator). |
| `local_team_total_rebounds` | numeric | Home side: team-only (not attributed to a player): total rebounds. |
| `local_team_defensive_rebounds` | numeric | Home side: team-only (not attributed to a player): defensive rebounds. |
| `local_team_offensive_rebounds` | numeric | Home side: team-only (not attributed to a player): offensive rebounds. |
| `local_team_assistances` | numeric | Home side: team-only (not attributed to a player): assists. |
| `local_team_steals` | numeric | Home side: team-only (not attributed to a player): steals. |
| `local_team_turnovers` | numeric | Home side: team-only (not attributed to a player): turnovers. |
| `local_team_blocks_favour` | numeric | Home side: team-only (not attributed to a player): blocks made. |
| `local_team_blocks_against` | numeric | Home side: team-only (not attributed to a player): shots blocked by the opponent. |
| `local_team_fouls_commited` | numeric | Home side: team-only (not attributed to a player): personal fouls committed. |
| `local_team_fouls_received` | numeric | Home side: team-only (not attributed to a player): fouls drawn. |
| `local_team_plus_minus` | numeric | Home side: team-only (not attributed to a player): plus/minus. |
| `local_total_time_played` | numeric | Home side: totals: seconds played. |
| `local_total_valuation` | numeric | Home side: totals: performance index rating (PIR). |
| `local_total_points` | numeric | Home side: totals: points. |
| `local_total_field_goals_made2` | numeric | Home side: totals: two-point field goals made. |
| `local_total_field_goals_attempted2` | numeric | Home side: totals: two-point field goals attempted. |
| `local_total_field_goals_made3` | numeric | Home side: totals: three-point field goals made. |
| `local_total_field_goals_attempted3` | numeric | Home side: totals: three-point field goals attempted. |
| `local_total_free_throws_made` | numeric | Home side: totals: free throws made. |
| `local_total_free_throws_attempted` | numeric | Home side: totals: free throws attempted. |
| `local_total_field_goals_made_total` | numeric | Home side: totals: field goals made. |
| `local_total_field_goals_attempted_total` | numeric | Home side: totals: field goals attempted. |
| `local_total_accuracy_made` | numeric | Home side: totals: shots made (accuracy numerator). |
| `local_total_accuracy_attempted` | numeric | Home side: totals: shots attempted (accuracy denominator). |
| `local_total_total_rebounds` | numeric | Home side: totals: total rebounds. |
| `local_total_defensive_rebounds` | numeric | Home side: totals: defensive rebounds. |
| `local_total_offensive_rebounds` | numeric | Home side: totals: offensive rebounds. |
| `local_total_assistances` | numeric | Home side: totals: assists. |
| `local_total_steals` | numeric | Home side: totals: steals. |
| `local_total_turnovers` | numeric | Home side: totals: turnovers. |
| `local_total_blocks_favour` | numeric | Home side: totals: blocks made. |
| `local_total_blocks_against` | numeric | Home side: totals: shots blocked by the opponent. |
| `local_total_fouls_commited` | numeric | Home side: totals: personal fouls committed. |
| `local_total_fouls_received` | numeric | Home side: totals: fouls drawn. |
| `local_total_plus_minus` | numeric | Home side: totals: plus/minus. |
| `road_coach_code` | character | Away side: head coach code (Utf8 join key). |
| `road_coach_name` | character | Away side: head coach name. |
| `road_players` | character | Away side: per-player box-score rows for the side, JSON-encoded. |
| `road_team_time_played` | numeric | Away side: team-only (not attributed to a player): seconds played. |
| `road_team_valuation` | numeric | Away side: team-only (not attributed to a player): performance index rating (PIR). |
| `road_team_points` | numeric | Away side: team-only (not attributed to a player): points. |
| `road_team_field_goals_made2` | numeric | Away side: team-only (not attributed to a player): two-point field goals made. |
| `road_team_field_goals_attempted2` | numeric | Away side: team-only (not attributed to a player): two-point field goals attempted. |
| `road_team_field_goals_made3` | numeric | Away side: team-only (not attributed to a player): three-point field goals made. |
| `road_team_field_goals_attempted3` | numeric | Away side: team-only (not attributed to a player): three-point field goals attempted. |
| `road_team_free_throws_made` | numeric | Away side: team-only (not attributed to a player): free throws made. |
| `road_team_free_throws_attempted` | numeric | Away side: team-only (not attributed to a player): free throws attempted. |
| `road_team_field_goals_made_total` | numeric | Away side: team-only (not attributed to a player): field goals made. |
| `road_team_field_goals_attempted_total` | numeric | Away side: team-only (not attributed to a player): field goals attempted. |
| `road_team_accuracy_made` | numeric | Away side: team-only (not attributed to a player): shots made (accuracy numerator). |
| `road_team_accuracy_attempted` | numeric | Away side: team-only (not attributed to a player): shots attempted (accuracy denominator). |
| `road_team_total_rebounds` | numeric | Away side: team-only (not attributed to a player): total rebounds. |
| `road_team_defensive_rebounds` | numeric | Away side: team-only (not attributed to a player): defensive rebounds. |
| `road_team_offensive_rebounds` | numeric | Away side: team-only (not attributed to a player): offensive rebounds. |
| `road_team_assistances` | numeric | Away side: team-only (not attributed to a player): assists. |
| `road_team_steals` | numeric | Away side: team-only (not attributed to a player): steals. |
| `road_team_turnovers` | numeric | Away side: team-only (not attributed to a player): turnovers. |
| `road_team_blocks_favour` | numeric | Away side: team-only (not attributed to a player): blocks made. |
| `road_team_blocks_against` | numeric | Away side: team-only (not attributed to a player): shots blocked by the opponent. |
| `road_team_fouls_commited` | numeric | Away side: team-only (not attributed to a player): personal fouls committed. |
| `road_team_fouls_received` | numeric | Away side: team-only (not attributed to a player): fouls drawn. |
| `road_team_plus_minus` | numeric | Away side: team-only (not attributed to a player): plus/minus. |
| `road_total_time_played` | numeric | Away side: totals: seconds played. |
| `road_total_valuation` | numeric | Away side: totals: performance index rating (PIR). |
| `road_total_points` | numeric | Away side: totals: points. |
| `road_total_field_goals_made2` | numeric | Away side: totals: two-point field goals made. |
| `road_total_field_goals_attempted2` | numeric | Away side: totals: two-point field goals attempted. |
| `road_total_field_goals_made3` | numeric | Away side: totals: three-point field goals made. |
| `road_total_field_goals_attempted3` | numeric | Away side: totals: three-point field goals attempted. |
| `road_total_free_throws_made` | numeric | Away side: totals: free throws made. |
| `road_total_free_throws_attempted` | numeric | Away side: totals: free throws attempted. |
| `road_total_field_goals_made_total` | numeric | Away side: totals: field goals made. |
| `road_total_field_goals_attempted_total` | numeric | Away side: totals: field goals attempted. |
| `road_total_accuracy_made` | numeric | Away side: totals: shots made (accuracy numerator). |
| `road_total_accuracy_attempted` | numeric | Away side: totals: shots attempted (accuracy denominator). |
| `road_total_total_rebounds` | numeric | Away side: totals: total rebounds. |
| `road_total_defensive_rebounds` | numeric | Away side: totals: defensive rebounds. |
| `road_total_offensive_rebounds` | numeric | Away side: totals: offensive rebounds. |
| `road_total_assistances` | numeric | Away side: totals: assists. |
| `road_total_steals` | numeric | Away side: totals: steals. |
| `road_total_turnovers` | numeric | Away side: totals: turnovers. |
| `road_total_blocks_favour` | numeric | Away side: totals: blocks made. |
| `road_total_blocks_against` | numeric | Away side: totals: shots blocked by the opponent. |
| `road_total_fouls_commited` | numeric | Away side: totals: personal fouls committed. |
| `road_total_fouls_received` | numeric | Away side: totals: fouls drawn. |
| `road_total_plus_minus` | numeric | Away side: totals: plus/minus. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_game_stats-example}

```python
euroleague_game_stats(competition_code='E', game_code=1, season_code='E2025')
```

_Last validated n/a._
