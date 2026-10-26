---
title: "TPBL — TPBL API (api.tpbl.basketball, Taiwan) — Box"
sidebar_label: "Box"
sidebar_position: 1
description: "TPBL — TPBL API (api.tpbl.basketball, Taiwan) — Box — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# TPBL — TPBL API (api.tpbl.basketball, Taiwan) — Box

## tpbl_box_players

Whole-game box score lines of every player on either team sheet, keyed by GoHoops roster id.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/games/{game_id}/stats`

**Valid URL:** [https://api.tpbl.basketball/api/games/6/stats](https://api.tpbl.basketball/api/games/6/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served. |

### Returns {#tpbl_box_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `side` | String | Side of the game: home or away (the API lists the home team first and never infers it from the venue). |
| `team_id` | Int64 | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `team` | String | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `gohoops_squad_id` | Int64 | GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `player_id` | Int64 | TPBL player id (key space: TPBL players; one per person across seasons and teams): the {player_id} of the player routes. Not the GoHoops person id and not the roster id. |
| `gohoops_person_id` | Int64 | GoHoops person id (key space: GoHoops persons; one per human). Not the TPBL player id and not the roster id. |
| `gohoops_roster_id` | Int64 | GoHoops roster id (key space: GoHoops rosters = one player registered with one squad in one season; it changes when the player changes season or team). The key of the box score players: join tpbl_box_players to tpbl_roster on it. Not the TPBL player id and not the person id. |
| `player` | String | Player name (Traditional Chinese, or a transliteration for foreign players). |
| `number` | String | Jersey number as text (so '0' and '00' stay distinct). |
| `position` | String | Playing position as a PascalCase enum: PointGuard, ShootingGuard, SmallForward, PowerForward, Center, and the combinations PointGuardAndShootingGuard, ShootingGuardAndSmallForward, SmallForwardAndPowerForward and PowerForwardAndCenter; '' when unset. |
| `status` | String | Registration status of the player on the squad as of the request, not at game time: 'official', or 'withdrawn' / 'WITHDRAWN' (one state in two spellings: the player left the squad later; most withdrawn players still played). |
| `roster_status` | String | Roster registration status as an upper-case enum: 'OFFICIAL' in the 2026-27 preseason game; null in the 2024-25 and 2025-26 box scores (an era difference in the feed). |
| `is_starting` | Boolean | True when the player was in the starting five. |
| `entered` | Boolean | True when the player has a stat line (he entered the game; a few such players have 0 seconds on court); false for a registered player who did not play, whose stats are all null. |
| `time_on_court` | Int64 | Seconds on court by the player over the whole game (436 = 7:16). A team quarter sums its five players: 3600 = 5 x 720 s (12-minute quarter), 1500 = 5 x 300 s (5-minute overtime). |
| `score` | Int64 | Points scored by the player over the whole game. The stat keys are absent for a player who did not enter the game (11 keys in all); a few players who did not play carry them with time_on_court 0. |
| `plus_minus` | Int64 | Plus/minus: the team's point differential while the player was on court (standard definition; not independently checked). |
| `field_goals_made` | Int64 | Field goals made by the player over the whole game. |
| `field_goals_missed` | Int64 | Field goals missed by the player over the whole game. |
| `field_goals_attempted` | Int64 | Field goals attempted by the player over the whole game. |
| `field_goals_percentage` | Float64 | Field goals percentage by the player over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `two_pointers_made` | Int64 | 2-point field goals made by the player over the whole game. |
| `two_pointers_missed` | Int64 | 2-point field goals missed by the player over the whole game. |
| `two_pointers_attempted` | Int64 | 2-point field goals attempted by the player over the whole game. |
| `two_pointers_percentage` | Float64 | 2-point field goals percentage by the player over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `three_pointers_made` | Int64 | 3-point field goals made by the player over the whole game. |
| `three_pointers_missed` | Int64 | 3-point field goals missed by the player over the whole game. |
| `three_pointers_attempted` | Int64 | 3-point field goals attempted by the player over the whole game. |
| `three_pointers_percentage` | Float64 | 3-point field goals percentage by the player over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `free_throws_made` | Int64 | Free throws made by the player over the whole game. |
| `free_throws_missed` | Int64 | Free throws missed by the player over the whole game. |
| `free_throws_attempted` | Int64 | Free throws attempted by the player over the whole game. |
| `free_throws_percentage` | Float64 | Free throws percentage by the player over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `rebounds` | Int64 | Total rebounds by the player over the whole game (offensive + defensive). |
| `defensive_rebounds` | Int64 | Defensive rebounds by the player over the whole game. |
| `offensive_rebounds` | Int64 | Offensive rebounds by the player over the whole game. |
| `defensive_fouls` | Int64 | Fouls committed on defense by the player over the whole game. |
| `offensive_fouls` | Int64 | Fouls committed on offense by the player over the whole game. |
| `personal_fouls` | Int64 | Personal fouls by the player over the whole game (excludes technical, unsportsmanlike and disqualifying fouls). |
| `fouls` | Int64 | Fouls charged by the player over the whole game: personal + technical + unsportsmanlike + disqualifying; equals defensive_fouls + offensive_fouls (checked on 4,237 played player lines of 189 games). |
| `assists` | Int64 | Assists by the player over the whole game. |
| `steals` | Int64 | Steals by the player over the whole game. |
| `blocks` | Int64 | Blocked shots by the player over the whole game. |
| `turnovers` | Int64 | Turnovers committed by the player over the whole game; equals the sum of the *_turnovers type columns (checked on 4,237 played player lines of 189 games). |
| `points_in_paint` | Int64 | Points scored in the paint by the player over the whole game. |
| `second_chance_points` | Int64 | Points scored on second chances (after an offensive rebound) by the player over the whole game. |
| `fast_break_points` | Int64 | Points scored on the fast break by the player over the whole game. |
| `efficiency` | Int64 | Efficiency rating by the player over the whole game: (points + rebounds + assists + steals + blocks) - (missed field goals + missed free throws + turnovers); verified on all 4,237 played player lines of 189 games. |
| `efg` | Float64 | Effective field-goal percentage by the player over the whole game, 0-100 as a string with one decimal rounded half up: (FGM + 0.5 x 3PM) / FGA; '0.0' without attempts. |
| `tsp` | Float64 | True shooting percentage by the player over the whole game, 0-100 as a string with one decimal rounded half up: points / (2 x (FGA + 0.44 x FTA)); '0.0' without attempts. |
| `pir` | Int64 | Performance Index Rating by the player over the whole game: efficiency - fouls (verified on all 4,237 played player lines of 189 games). |
| `field_goals_made_in_the_paint` | Int64 | Field goals made in the paint by the player over the whole game. |
| `field_goals_missed_in_the_paint` | Int64 | Field goals missed in the paint by the player over the whole game. |
| `field_goals_attempted_in_the_paint` | Int64 | Field goals attempted in the paint by the player over the whole game. |
| `field_goals_made_on_fast_break` | Int64 | Field goals made on the fast break by the player over the whole game. |
| `field_goals_missed_on_fast_break` | Int64 | Field goals missed on the fast break by the player over the whole game. |
| `field_goals_attempted_on_fast_break` | Int64 | Field goals attempted on the fast break by the player over the whole game. |
| `field_goals_made_on_second_chance` | Int64 | Field goals made on a second chance (after an offensive rebound) by the player over the whole game. |
| `field_goals_missed_on_second_chance` | Int64 | Field goals missed on a second chance (after an offensive rebound) by the player over the whole game. |
| `field_goals_attempted_on_second_chance` | Int64 | Field goals attempted on a second chance (after an offensive rebound) by the player over the whole game. |
| `technical_fouls` | Int64 | Technical fouls by the player over the whole game. |
| `unsportsmanlike_fouls` | Int64 | Unsportsmanlike fouls by the player over the whole game. |
| `disqualifying_fouls` | Int64 | Disqualifying fouls by the player over the whole game. |
| `bad_pass_turnovers` | Int64 | Turnovers of type 'bad pass' by the player over the whole game. |
| `ball_handling_turnovers` | Int64 | Turnovers of type 'ball-handling error' by the player over the whole game. |
| `out_of_bounds_turnovers` | Int64 | Turnovers of type 'out of bounds' by the player over the whole game. |
| `travel_turnovers` | Int64 | Turnovers of type 'traveling' by the player over the whole game. |
| `three_seconds_turnovers` | Int64 | Turnovers of type 'three-second violation' by the player over the whole game. |
| `five_seconds_turnovers` | Int64 | Turnovers of type 'five-second violation' by the player over the whole game. |
| `back_court_turnovers` | Int64 | Turnovers of type 'back-court violation' by the player over the whole game. |
| `offensive_goal_tending_turnovers` | Int64 | Turnovers of type 'offensive goaltending' by the player over the whole game. |
| `double_dribble_turnovers` | Int64 | Turnovers of type 'double dribble' by the player over the whole game. |
| `carry_turnovers` | Int64 | Turnovers of type 'carrying' by the player over the whole game. |
| `offensive_foul_turnovers` | Int64 | Turnovers of type 'offensive foul' by the player over the whole game. |
| `other_turnovers` | Int64 | Turnovers of type 'other' by the player over the whole game. |
| `points_off_turnovers` | Int64 | Points scored off opponent turnovers by the player over the whole game. |
| `two_pointers_jump_shot_made` | Int64 | 2-point field goals made that were a jump shot (standing or catch-and-shoot) by the player over the whole game. |
| `two_pointers_jump_shot_missed` | Int64 | 2-point field goals missed that were a jump shot (standing or catch-and-shoot) by the player over the whole game. |
| `two_pointers_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a jump shot (standing or catch-and-shoot) by the player over the whole game. |
| `three_pointers_jump_shot_made` | Int64 | 3-point field goals made that were a jump shot (standing or catch-and-shoot) by the player over the whole game. |
| `three_pointers_jump_shot_missed` | Int64 | 3-point field goals missed that were a jump shot (standing or catch-and-shoot) by the player over the whole game. |
| `three_pointers_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a jump shot (standing or catch-and-shoot) by the player over the whole game. |
| `two_pointers_layup_made` | Int64 | 2-point field goals made that were a layup by the player over the whole game. |
| `two_pointers_layup_missed` | Int64 | 2-point field goals missed that were a layup by the player over the whole game. |
| `two_pointers_layup_attempted` | Int64 | 2-point field goals attempted that were a layup by the player over the whole game. |
| `two_pointers_driving_layup_made` | Int64 | 2-point field goals made that were a driving layup by the player over the whole game. |
| `two_pointers_driving_layup_missed` | Int64 | 2-point field goals missed that were a driving layup by the player over the whole game. |
| `two_pointers_driving_layup_attempted` | Int64 | 2-point field goals attempted that were a driving layup by the player over the whole game. |
| `two_pointers_dunk_made` | Int64 | 2-point field goals made that were a dunk by the player over the whole game. |
| `two_pointers_dunk_missed` | Int64 | 2-point field goals missed that were a dunk by the player over the whole game. |
| `two_pointers_dunk_attempted` | Int64 | 2-point field goals attempted that were a dunk by the player over the whole game. |
| `two_pointers_putback_dunk_made` | Int64 | 2-point field goals made that were a putback dunk (off an offensive rebound) by the player over the whole game. |
| `two_pointers_putback_dunk_missed` | Int64 | 2-point field goals missed that were a putback dunk (off an offensive rebound) by the player over the whole game. |
| `two_pointers_putback_dunk_attempted` | Int64 | 2-point field goals attempted that were a putback dunk (off an offensive rebound) by the player over the whole game. |
| `two_pointers_putback_tip_in_made` | Int64 | 2-point field goals made that were a putback tip-in (off an offensive rebound) by the player over the whole game. |
| `two_pointers_putback_tip_in_missed` | Int64 | 2-point field goals missed that were a putback tip-in (off an offensive rebound) by the player over the whole game. |
| `two_pointers_putback_tip_in_attempted` | Int64 | 2-point field goals attempted that were a putback tip-in (off an offensive rebound) by the player over the whole game. |
| `two_pointers_alley_oop_made` | Int64 | 2-point field goals made that were a alley-oop by the player over the whole game. |
| `two_pointers_alley_oop_missed` | Int64 | 2-point field goals missed that were a alley-oop by the player over the whole game. |
| `two_pointers_alley_oop_attempted` | Int64 | 2-point field goals attempted that were a alley-oop by the player over the whole game. |
| `two_pointers_hook_shot_made` | Int64 | 2-point field goals made that were a hook shot by the player over the whole game. |
| `two_pointers_hook_shot_missed` | Int64 | 2-point field goals missed that were a hook shot by the player over the whole game. |
| `two_pointers_hook_shot_attempted` | Int64 | 2-point field goals attempted that were a hook shot by the player over the whole game. |
| `three_pointers_hook_shot_made` | Int64 | 3-point field goals made that were a hook shot by the player over the whole game. |
| `three_pointers_hook_shot_missed` | Int64 | 3-point field goals missed that were a hook shot by the player over the whole game. |
| `three_pointers_hook_shot_attempted` | Int64 | 3-point field goals attempted that were a hook shot by the player over the whole game. |
| `two_pointers_floating_jump_shot_made` | Int64 | 2-point field goals made that were a floating jump shot (floater) by the player over the whole game. |
| `two_pointers_floating_jump_shot_missed` | Int64 | 2-point field goals missed that were a floating jump shot (floater) by the player over the whole game. |
| `two_pointers_floating_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a floating jump shot (floater) by the player over the whole game. |
| `three_pointers_floating_jump_shot_made` | Int64 | 3-point field goals made that were a floating jump shot (floater) by the player over the whole game. |
| `three_pointers_floating_jump_shot_missed` | Int64 | 3-point field goals missed that were a floating jump shot (floater) by the player over the whole game. |
| `three_pointers_floating_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a floating jump shot (floater) by the player over the whole game. |
| `two_pointers_fadeaway_jump_shot_made` | Int64 | 2-point field goals made that were a fadeaway jump shot by the player over the whole game. |
| `two_pointers_fadeaway_jump_shot_missed` | Int64 | 2-point field goals missed that were a fadeaway jump shot by the player over the whole game. |
| `two_pointers_fadeaway_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a fadeaway jump shot by the player over the whole game. |
| `three_pointers_fadeaway_jump_shot_made` | Int64 | 3-point field goals made that were a fadeaway jump shot by the player over the whole game. |
| `three_pointers_fadeaway_jump_shot_missed` | Int64 | 3-point field goals missed that were a fadeaway jump shot by the player over the whole game. |
| `three_pointers_fadeaway_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a fadeaway jump shot by the player over the whole game. |
| `two_pointers_turnaround_jump_shot_made` | Int64 | 2-point field goals made that were a turnaround jump shot by the player over the whole game. |
| `two_pointers_turnaround_jump_shot_missed` | Int64 | 2-point field goals missed that were a turnaround jump shot by the player over the whole game. |
| `two_pointers_turnaround_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a turnaround jump shot by the player over the whole game. |
| `three_pointers_turnaround_jump_shot_made` | Int64 | 3-point field goals made that were a turnaround jump shot by the player over the whole game. |
| `three_pointers_turnaround_jump_shot_missed` | Int64 | 3-point field goals missed that were a turnaround jump shot by the player over the whole game. |
| `three_pointers_turnaround_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a turnaround jump shot by the player over the whole game. |
| `two_pointers_step_back_jump_shot_made` | Int64 | 2-point field goals made that were a step-back jump shot by the player over the whole game. |
| `two_pointers_step_back_jump_shot_missed` | Int64 | 2-point field goals missed that were a step-back jump shot by the player over the whole game. |
| `two_pointers_step_back_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a step-back jump shot by the player over the whole game. |
| `three_pointers_step_back_jump_shot_made` | Int64 | 3-point field goals made that were a step-back jump shot by the player over the whole game. |
| `three_pointers_step_back_jump_shot_missed` | Int64 | 3-point field goals missed that were a step-back jump shot by the player over the whole game. |
| `three_pointers_step_back_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a step-back jump shot by the player over the whole game. |
| `two_pointers_pull_up_jump_shot_made` | Int64 | 2-point field goals made that were a pull-up jump shot by the player over the whole game. |
| `two_pointers_pull_up_jump_shot_missed` | Int64 | 2-point field goals missed that were a pull-up jump shot by the player over the whole game. |
| `two_pointers_pull_up_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a pull-up jump shot by the player over the whole game. |
| `three_pointers_pull_up_jump_shot_made` | Int64 | 3-point field goals made that were a pull-up jump shot by the player over the whole game. |
| `three_pointers_pull_up_jump_shot_missed` | Int64 | 3-point field goals missed that were a pull-up jump shot by the player over the whole game. |
| `three_pointers_pull_up_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a pull-up jump shot by the player over the whole game. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_box_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_box_players-example}

```python
tpbl_box_players(game_id=6)
```

## tpbl_box_player_periods

Per-quarter box score lines of the players who were on court, overtimes included.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/games/{game_id}/stats`

**Valid URL:** [https://api.tpbl.basketball/api/games/6/stats](https://api.tpbl.basketball/api/games/6/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served. |

### Returns {#tpbl_box_player_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `side` | String | Side of the game: home or away (the API lists the home team first and never infers it from the venue). |
| `team_id` | Int64 | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `team` | String | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `gohoops_squad_id` | Int64 | GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `period` | Int64 | Quarter number of the row: 1-4 regulation, 5 = the first overtime, 6 = the second. |
| `player_id` | Int64 | TPBL player id (key space: TPBL players; one per person across seasons and teams): the {player_id} of the player routes. Not the GoHoops person id and not the roster id. |
| `gohoops_person_id` | Int64 | GoHoops person id (key space: GoHoops persons; one per human). Not the TPBL player id and not the roster id. |
| `gohoops_roster_id` | Int64 | GoHoops roster id (key space: GoHoops rosters = one player registered with one squad in one season; it changes when the player changes season or team). The key of the box score players: join tpbl_box_players to tpbl_roster on it. Not the TPBL player id and not the person id. |
| `player` | String | Player name (Traditional Chinese, or a transliteration for foreign players). |
| `number` | String | Jersey number as text (so '0' and '00' stay distinct). |
| `position` | String | Playing position as a PascalCase enum: PointGuard, ShootingGuard, SmallForward, PowerForward, Center, and the combinations PointGuardAndShootingGuard, ShootingGuardAndSmallForward, SmallForwardAndPowerForward and PowerForwardAndCenter; '' when unset. |
| `status` | String | Registration status of the player on the squad as of the request, not at game time: 'official', or 'withdrawn' / 'WITHDRAWN' (one state in two spellings: the player left the squad later; most withdrawn players still played). |
| `roster_status` | String | Roster registration status as an upper-case enum: 'OFFICIAL' in the 2026-27 preseason game; null in the 2024-25 and 2025-26 box scores (an era difference in the feed). |
| `is_starting` | Boolean | True when the player was in the starting five of the game. |
| `entered` | Boolean | Always true here: only quarters in which the player was on court are rows. |
| `time_on_court` | Int64 | Seconds on court by the player in this quarter (436 = 7:16). A team quarter sums its five players: 3600 = 5 x 720 s (12-minute quarter), 1500 = 5 x 300 s (5-minute overtime). |
| `score` | Int64 | Points scored by the player in this quarter. The stat keys are absent for a player who did not enter the game (11 keys in all); a few players who did not play carry them with time_on_court 0. |
| `plus_minus` | Int64 | Plus/minus: the team's point differential while the player was on court (standard definition; not independently checked). |
| `field_goals_made` | Int64 | Field goals made by the player in this quarter. |
| `field_goals_missed` | Int64 | Field goals missed by the player in this quarter. |
| `field_goals_attempted` | Int64 | Field goals attempted by the player in this quarter. |
| `field_goals_percentage` | Float64 | Field goals percentage by the player in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `two_pointers_made` | Int64 | 2-point field goals made by the player in this quarter. |
| `two_pointers_missed` | Int64 | 2-point field goals missed by the player in this quarter. |
| `two_pointers_attempted` | Int64 | 2-point field goals attempted by the player in this quarter. |
| `two_pointers_percentage` | Float64 | 2-point field goals percentage by the player in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `three_pointers_made` | Int64 | 3-point field goals made by the player in this quarter. |
| `three_pointers_missed` | Int64 | 3-point field goals missed by the player in this quarter. |
| `three_pointers_attempted` | Int64 | 3-point field goals attempted by the player in this quarter. |
| `three_pointers_percentage` | Float64 | 3-point field goals percentage by the player in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `free_throws_made` | Int64 | Free throws made by the player in this quarter. |
| `free_throws_missed` | Int64 | Free throws missed by the player in this quarter. |
| `free_throws_attempted` | Int64 | Free throws attempted by the player in this quarter. |
| `free_throws_percentage` | Float64 | Free throws percentage by the player in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `rebounds` | Int64 | Total rebounds by the player in this quarter (offensive + defensive). |
| `defensive_rebounds` | Int64 | Defensive rebounds by the player in this quarter. |
| `offensive_rebounds` | Int64 | Offensive rebounds by the player in this quarter. |
| `defensive_fouls` | Int64 | Fouls committed on defense by the player in this quarter. |
| `offensive_fouls` | Int64 | Fouls committed on offense by the player in this quarter. |
| `personal_fouls` | Int64 | Personal fouls by the player in this quarter (excludes technical, unsportsmanlike and disqualifying fouls). |
| `fouls` | Int64 | Fouls charged by the player in this quarter: personal + technical + unsportsmanlike + disqualifying; equals defensive_fouls + offensive_fouls (checked on 4,237 played player lines of 189 games). |
| `assists` | Int64 | Assists by the player in this quarter. |
| `steals` | Int64 | Steals by the player in this quarter. |
| `blocks` | Int64 | Blocked shots by the player in this quarter. |
| `turnovers` | Int64 | Turnovers committed by the player in this quarter; equals the sum of the *_turnovers type columns (checked on 4,237 played player lines of 189 games). |
| `points_in_paint` | Int64 | Points scored in the paint by the player in this quarter. |
| `second_chance_points` | Int64 | Points scored on second chances (after an offensive rebound) by the player in this quarter. |
| `fast_break_points` | Int64 | Points scored on the fast break by the player in this quarter. |
| `efg` | Float64 | Effective field-goal percentage by the player in this quarter, 0-100 as a string with one decimal rounded half up: (FGM + 0.5 x 3PM) / FGA; '0.0' without attempts. |
| `tsp` | Float64 | True shooting percentage by the player in this quarter, 0-100 as a string with one decimal rounded half up: points / (2 x (FGA + 0.44 x FTA)); '0.0' without attempts. |
| `pir` | Int64 | Performance Index Rating by the player in this quarter: efficiency - fouls (verified on all 4,237 played player lines of 189 games). |
| `field_goals_made_in_the_paint` | Int64 | Field goals made in the paint by the player in this quarter. |
| `field_goals_missed_in_the_paint` | Int64 | Field goals missed in the paint by the player in this quarter. |
| `field_goals_attempted_in_the_paint` | Int64 | Field goals attempted in the paint by the player in this quarter. |
| `field_goals_made_on_fast_break` | Int64 | Field goals made on the fast break by the player in this quarter. |
| `field_goals_missed_on_fast_break` | Int64 | Field goals missed on the fast break by the player in this quarter. |
| `field_goals_attempted_on_fast_break` | Int64 | Field goals attempted on the fast break by the player in this quarter. |
| `field_goals_made_on_second_chance` | Int64 | Field goals made on a second chance (after an offensive rebound) by the player in this quarter. |
| `field_goals_missed_on_second_chance` | Int64 | Field goals missed on a second chance (after an offensive rebound) by the player in this quarter. |
| `field_goals_attempted_on_second_chance` | Int64 | Field goals attempted on a second chance (after an offensive rebound) by the player in this quarter. |
| `technical_fouls` | Int64 | Technical fouls by the player in this quarter. |
| `unsportsmanlike_fouls` | Int64 | Unsportsmanlike fouls by the player in this quarter. |
| `disqualifying_fouls` | Int64 | Disqualifying fouls by the player in this quarter. |
| `bad_pass_turnovers` | Int64 | Turnovers of type 'bad pass' by the player in this quarter. |
| `ball_handling_turnovers` | Int64 | Turnovers of type 'ball-handling error' by the player in this quarter. |
| `out_of_bounds_turnovers` | Int64 | Turnovers of type 'out of bounds' by the player in this quarter. |
| `travel_turnovers` | Int64 | Turnovers of type 'traveling' by the player in this quarter. |
| `three_seconds_turnovers` | Int64 | Turnovers of type 'three-second violation' by the player in this quarter. |
| `five_seconds_turnovers` | Int64 | Turnovers of type 'five-second violation' by the player in this quarter. |
| `back_court_turnovers` | Int64 | Turnovers of type 'back-court violation' by the player in this quarter. |
| `offensive_goal_tending_turnovers` | Int64 | Turnovers of type 'offensive goaltending' by the player in this quarter. |
| `double_dribble_turnovers` | Int64 | Turnovers of type 'double dribble' by the player in this quarter. |
| `carry_turnovers` | Int64 | Turnovers of type 'carrying' by the player in this quarter. |
| `offensive_foul_turnovers` | Int64 | Turnovers of type 'offensive foul' by the player in this quarter. |
| `other_turnovers` | Int64 | Turnovers of type 'other' by the player in this quarter. |
| `points_off_turnovers` | Int64 | Points scored off opponent turnovers by the player in this quarter. |
| `two_pointers_jump_shot_made` | Int64 | 2-point field goals made that were a jump shot (standing or catch-and-shoot) by the player in this quarter. |
| `two_pointers_jump_shot_missed` | Int64 | 2-point field goals missed that were a jump shot (standing or catch-and-shoot) by the player in this quarter. |
| `two_pointers_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a jump shot (standing or catch-and-shoot) by the player in this quarter. |
| `three_pointers_jump_shot_made` | Int64 | 3-point field goals made that were a jump shot (standing or catch-and-shoot) by the player in this quarter. |
| `three_pointers_jump_shot_missed` | Int64 | 3-point field goals missed that were a jump shot (standing or catch-and-shoot) by the player in this quarter. |
| `three_pointers_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a jump shot (standing or catch-and-shoot) by the player in this quarter. |
| `two_pointers_layup_made` | Int64 | 2-point field goals made that were a layup by the player in this quarter. |
| `two_pointers_layup_missed` | Int64 | 2-point field goals missed that were a layup by the player in this quarter. |
| `two_pointers_layup_attempted` | Int64 | 2-point field goals attempted that were a layup by the player in this quarter. |
| `two_pointers_driving_layup_made` | Int64 | 2-point field goals made that were a driving layup by the player in this quarter. |
| `two_pointers_driving_layup_missed` | Int64 | 2-point field goals missed that were a driving layup by the player in this quarter. |
| `two_pointers_driving_layup_attempted` | Int64 | 2-point field goals attempted that were a driving layup by the player in this quarter. |
| `two_pointers_dunk_made` | Int64 | 2-point field goals made that were a dunk by the player in this quarter. |
| `two_pointers_dunk_missed` | Int64 | 2-point field goals missed that were a dunk by the player in this quarter. |
| `two_pointers_dunk_attempted` | Int64 | 2-point field goals attempted that were a dunk by the player in this quarter. |
| `two_pointers_putback_dunk_made` | Int64 | 2-point field goals made that were a putback dunk (off an offensive rebound) by the player in this quarter. |
| `two_pointers_putback_dunk_missed` | Int64 | 2-point field goals missed that were a putback dunk (off an offensive rebound) by the player in this quarter. |
| `two_pointers_putback_dunk_attempted` | Int64 | 2-point field goals attempted that were a putback dunk (off an offensive rebound) by the player in this quarter. |
| `two_pointers_putback_tip_in_made` | Int64 | 2-point field goals made that were a putback tip-in (off an offensive rebound) by the player in this quarter. |
| `two_pointers_putback_tip_in_missed` | Int64 | 2-point field goals missed that were a putback tip-in (off an offensive rebound) by the player in this quarter. |
| `two_pointers_putback_tip_in_attempted` | Int64 | 2-point field goals attempted that were a putback tip-in (off an offensive rebound) by the player in this quarter. |
| `two_pointers_alley_oop_made` | Int64 | 2-point field goals made that were a alley-oop by the player in this quarter. |
| `two_pointers_alley_oop_missed` | Int64 | 2-point field goals missed that were a alley-oop by the player in this quarter. |
| `two_pointers_alley_oop_attempted` | Int64 | 2-point field goals attempted that were a alley-oop by the player in this quarter. |
| `two_pointers_hook_shot_made` | Int64 | 2-point field goals made that were a hook shot by the player in this quarter. |
| `two_pointers_hook_shot_missed` | Int64 | 2-point field goals missed that were a hook shot by the player in this quarter. |
| `two_pointers_hook_shot_attempted` | Int64 | 2-point field goals attempted that were a hook shot by the player in this quarter. |
| `three_pointers_hook_shot_made` | Int64 | 3-point field goals made that were a hook shot by the player in this quarter. |
| `three_pointers_hook_shot_missed` | Int64 | 3-point field goals missed that were a hook shot by the player in this quarter. |
| `three_pointers_hook_shot_attempted` | Int64 | 3-point field goals attempted that were a hook shot by the player in this quarter. |
| `two_pointers_floating_jump_shot_made` | Int64 | 2-point field goals made that were a floating jump shot (floater) by the player in this quarter. |
| `two_pointers_floating_jump_shot_missed` | Int64 | 2-point field goals missed that were a floating jump shot (floater) by the player in this quarter. |
| `two_pointers_floating_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a floating jump shot (floater) by the player in this quarter. |
| `three_pointers_floating_jump_shot_made` | Int64 | 3-point field goals made that were a floating jump shot (floater) by the player in this quarter. |
| `three_pointers_floating_jump_shot_missed` | Int64 | 3-point field goals missed that were a floating jump shot (floater) by the player in this quarter. |
| `three_pointers_floating_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a floating jump shot (floater) by the player in this quarter. |
| `two_pointers_fadeaway_jump_shot_made` | Int64 | 2-point field goals made that were a fadeaway jump shot by the player in this quarter. |
| `two_pointers_fadeaway_jump_shot_missed` | Int64 | 2-point field goals missed that were a fadeaway jump shot by the player in this quarter. |
| `two_pointers_fadeaway_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a fadeaway jump shot by the player in this quarter. |
| `three_pointers_fadeaway_jump_shot_made` | Int64 | 3-point field goals made that were a fadeaway jump shot by the player in this quarter. |
| `three_pointers_fadeaway_jump_shot_missed` | Int64 | 3-point field goals missed that were a fadeaway jump shot by the player in this quarter. |
| `three_pointers_fadeaway_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a fadeaway jump shot by the player in this quarter. |
| `two_pointers_turnaround_jump_shot_made` | Int64 | 2-point field goals made that were a turnaround jump shot by the player in this quarter. |
| `two_pointers_turnaround_jump_shot_missed` | Int64 | 2-point field goals missed that were a turnaround jump shot by the player in this quarter. |
| `two_pointers_turnaround_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a turnaround jump shot by the player in this quarter. |
| `three_pointers_turnaround_jump_shot_made` | Int64 | 3-point field goals made that were a turnaround jump shot by the player in this quarter. |
| `three_pointers_turnaround_jump_shot_missed` | Int64 | 3-point field goals missed that were a turnaround jump shot by the player in this quarter. |
| `three_pointers_turnaround_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a turnaround jump shot by the player in this quarter. |
| `two_pointers_step_back_jump_shot_made` | Int64 | 2-point field goals made that were a step-back jump shot by the player in this quarter. |
| `two_pointers_step_back_jump_shot_missed` | Int64 | 2-point field goals missed that were a step-back jump shot by the player in this quarter. |
| `two_pointers_step_back_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a step-back jump shot by the player in this quarter. |
| `three_pointers_step_back_jump_shot_made` | Int64 | 3-point field goals made that were a step-back jump shot by the player in this quarter. |
| `three_pointers_step_back_jump_shot_missed` | Int64 | 3-point field goals missed that were a step-back jump shot by the player in this quarter. |
| `three_pointers_step_back_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a step-back jump shot by the player in this quarter. |
| `two_pointers_pull_up_jump_shot_made` | Int64 | 2-point field goals made that were a pull-up jump shot by the player in this quarter. |
| `two_pointers_pull_up_jump_shot_missed` | Int64 | 2-point field goals missed that were a pull-up jump shot by the player in this quarter. |
| `two_pointers_pull_up_jump_shot_attempted` | Int64 | 2-point field goals attempted that were a pull-up jump shot by the player in this quarter. |
| `three_pointers_pull_up_jump_shot_made` | Int64 | 3-point field goals made that were a pull-up jump shot by the player in this quarter. |
| `three_pointers_pull_up_jump_shot_missed` | Int64 | 3-point field goals missed that were a pull-up jump shot by the player in this quarter. |
| `three_pointers_pull_up_jump_shot_attempted` | Int64 | 3-point field goals attempted that were a pull-up jump shot by the player in this quarter. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_box_player_periods-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_box_player_periods-example}

```python
tpbl_box_player_periods(game_id=6)
```

## tpbl_box_teams

Whole-game team totals of both teams: shooting, rebounds, fouls, assists, turnovers and paint points.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/games/{game_id}/stats`

**Valid URL:** [https://api.tpbl.basketball/api/games/6/stats](https://api.tpbl.basketball/api/games/6/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served. |

### Returns {#tpbl_box_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `side` | String | Side of the game: home or away (the API lists the home team first and never infers it from the venue). |
| `team_id` | Int64 | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `team` | String | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `gohoops_squad_id` | Int64 | GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `opponent_team_id` | Int64 | TPBL team id of the other side of the game (franchise, 2-8). Joins team_id. |
| `field_goals_made` | Int64 | Field goals made by the team over the whole game. |
| `field_goals_missed` | Int64 | Field goals missed by the team over the whole game. |
| `field_goals_attempted` | Int64 | Field goals attempted by the team over the whole game. |
| `field_goals_percentage` | Float64 | Field goals percentage by the team over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `two_pointers_made` | Int64 | 2-point field goals made by the team over the whole game. |
| `two_pointers_missed` | Int64 | 2-point field goals missed by the team over the whole game. |
| `two_pointers_attempted` | Int64 | 2-point field goals attempted by the team over the whole game. |
| `two_pointers_percentage` | Float64 | 2-point field goals percentage by the team over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `three_pointers_made` | Int64 | 3-point field goals made by the team over the whole game. |
| `three_pointers_missed` | Int64 | 3-point field goals missed by the team over the whole game. |
| `three_pointers_attempted` | Int64 | 3-point field goals attempted by the team over the whole game. |
| `three_pointers_percentage` | Float64 | 3-point field goals percentage by the team over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `free_throws_made` | Int64 | Free throws made by the team over the whole game. |
| `free_throws_missed` | Int64 | Free throws missed by the team over the whole game. |
| `free_throws_attempted` | Int64 | Free throws attempted by the team over the whole game. |
| `free_throws_percentage` | Float64 | Free throws percentage by the team over the whole game, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `won_score` | Int64 | Team-row field: points the team scored in the whole game (the final score). |
| `total_won_score` | Int64 | Team-row field: points the team scored in the whole game (the final score; equals won_score on this line). |
| `lost_score` | Int64 | Team-row field: points the team conceded in the whole game (the opponent's final score). |
| `total_lost_score` | Int64 | Team-row field: points the team conceded in the whole game (equals lost_score on this line). |
| `rebounds` | Int64 | Total rebounds by the team over the whole game (offensive + defensive). |
| `defensive_rebounds` | Int64 | Defensive rebounds by the team over the whole game. |
| `offensive_rebounds` | Int64 | Offensive rebounds by the team over the whole game. |
| `defensive_fouls` | Int64 | Fouls committed on defense by the team over the whole game. |
| `offensive_fouls` | Int64 | Fouls committed on offense by the team over the whole game. |
| `personal_fouls` | Int64 | Personal fouls by the team over the whole game (excludes technical, unsportsmanlike and disqualifying fouls). |
| `fouls` | Int64 | Fouls charged by the team over the whole game: personal + technical + unsportsmanlike + disqualifying; equals defensive_fouls + offensive_fouls (checked on 4,237 played player lines of 189 games). |
| `assists` | Int64 | Assists by the team over the whole game. |
| `steals` | Int64 | Steals by the team over the whole game. |
| `blocks` | Int64 | Blocked shots by the team over the whole game. |
| `turnovers` | Int64 | Turnovers committed by the team over the whole game; equals the sum of the *_turnovers type columns (checked on 4,237 played player lines of 189 games). |
| `points_in_paint` | Int64 | Points scored in the paint by the team over the whole game. |
| `second_chance_points` | Int64 | Points scored on second chances (after an offensive rebound) by the team over the whole game. |
| `fast_break_points` | Int64 | Points scored on the fast break by the team over the whole game. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_box_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_box_teams-example}

```python
tpbl_box_teams(game_id=6)
```

## tpbl_box_team_periods

Per-quarter team lines of both teams: points scored and conceded in the quarter, running totals and stats.

**Endpoint URL:** `GET https://api.tpbl.basketball/api/games/{game_id}/stats`

**Valid URL:** [https://api.tpbl.basketball/api/games/6/stats](https://api.tpbl.basketball/api/games/6/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served. |

### Returns {#tpbl_box_team_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a box frame parsed without the request stamp has a null game_id. |
| `side` | String | Side of the game: home or away (the API lists the home team first and never infers it from the venue). |
| `team_id` | Int64 | TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the division team routes. Not the GoHoops franchise id and not the squad id. |
| `team` | String | Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars). |
| `gohoops_squad_id` | Int64 | GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in 2025-26, 15536-15542 in 2026-27; it changes every season). |
| `period` | Int64 | Quarter number of the row: 1-4 regulation, 5 = the first overtime, 6 = the second. |
| `won_score` | Int64 | Team-row field: points the team scored in this quarter alone. Player stat lines carry no value for it. |
| `time_on_court` | Int64 | Seconds on court by the team in this quarter (436 = 7:16). A team quarter sums its five players: 3600 = 5 x 720 s (12-minute quarter), 1500 = 5 x 300 s (5-minute overtime). |
| `lost_score` | Int64 | Team-row field: points the team conceded in this quarter alone. Not a player stat. |
| `total_won_score` | Int64 | Team-row field: points the team has scored up to and including this quarter (the last quarter's value is the final score). Not a player stat. |
| `total_lost_score` | Int64 | Team-row field: points the team has conceded up to and including this quarter. Not a player stat. |
| `field_goals_made` | Int64 | Field goals made by the team in this quarter. |
| `field_goals_missed` | Int64 | Field goals missed by the team in this quarter. |
| `field_goals_attempted` | Int64 | Field goals attempted by the team in this quarter. |
| `field_goals_percentage` | Float64 | Field goals percentage by the team in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `two_pointers_made` | Int64 | 2-point field goals made by the team in this quarter. |
| `two_pointers_missed` | Int64 | 2-point field goals missed by the team in this quarter. |
| `two_pointers_attempted` | Int64 | 2-point field goals attempted by the team in this quarter. |
| `two_pointers_percentage` | Float64 | 2-point field goals percentage by the team in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `three_pointers_made` | Int64 | 3-point field goals made by the team in this quarter. |
| `three_pointers_missed` | Int64 | 3-point field goals missed by the team in this quarter. |
| `three_pointers_attempted` | Int64 | 3-point field goals attempted by the team in this quarter. |
| `three_pointers_percentage` | Float64 | 3-point field goals percentage by the team in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `free_throws_made` | Int64 | Free throws made by the team in this quarter. |
| `free_throws_missed` | Int64 | Free throws missed by the team in this quarter. |
| `free_throws_attempted` | Int64 | Free throws attempted by the team in this quarter. |
| `free_throws_percentage` | Float64 | Free throws percentage by the team in this quarter, 0-100 as a string with one decimal ('57.1'); '0.0' without attempts. |
| `rebounds` | Int64 | Total rebounds by the team in this quarter (offensive + defensive). |
| `defensive_rebounds` | Int64 | Defensive rebounds by the team in this quarter. |
| `offensive_rebounds` | Int64 | Offensive rebounds by the team in this quarter. |
| `defensive_fouls` | Int64 | Fouls committed on defense by the team in this quarter. |
| `offensive_fouls` | Int64 | Fouls committed on offense by the team in this quarter. |
| `personal_fouls` | Int64 | Personal fouls by the team in this quarter (excludes technical, unsportsmanlike and disqualifying fouls). |
| `fouls` | Int64 | Fouls charged by the team in this quarter: personal + technical + unsportsmanlike + disqualifying; equals defensive_fouls + offensive_fouls (checked on 4,237 played player lines of 189 games). |
| `assists` | Int64 | Assists by the team in this quarter. |
| `steals` | Int64 | Steals by the team in this quarter. |
| `blocks` | Int64 | Blocked shots by the team in this quarter. |
| `turnovers` | Int64 | Turnovers committed by the team in this quarter; equals the sum of the *_turnovers type columns (checked on 4,237 played player lines of 189 games). |
| `points_in_paint` | Int64 | Points scored in the paint by the team in this quarter. |
| `second_chance_points` | Int64 | Points scored on second chances (after an offensive rebound) by the team in this quarter. |
| `fast_break_points` | Int64 | Points scored on the fast break by the team in this quarter. |

**`return_parsed=False`** — the raw body (`dict`): the API's JSON object, or a list body wrapped as `{'data': [...]}`, plus `_request` (the season, game, division, team or player asked for and the ids the runtime resolved).

### Raises {#tpbl_box_team_periods-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a season it does not list, an empty list such as the standings of a playoff division). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a box score or list that disagrees with the game it was asked for, an error envelope, a non-JSON body, a server error). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tpbl_box_team_periods-example}

```python
tpbl_box_team_periods(game_id=6)
```
