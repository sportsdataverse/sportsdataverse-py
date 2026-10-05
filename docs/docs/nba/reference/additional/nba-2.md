---
title: "NBA — additional Python functions — Nba (2)"
sidebar_label: "Nba (2)"
sidebar_position: 5
description: "NBA — additional Python functions — Nba (2) — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Nba (2)

### nba_live_boxscore {#nba_live_boxscore}

`nba_live_boxscore(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict[str, str] | None' = None) -> 'Any'`

Fetch and parse NBA cdn.nba.com liveData boxscore for a game.

Retrieves `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json`
and parses it via `parse_nba_live_boxscore` into six tables (game,
officials, home/away players, home/away team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | NBA game ID (int or str). Zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict[str, str] \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a dict of six DataFrames (`game`, `officials`, `home_players`, `away_players`, `home_team`, `away_team`) parsed by `parse_nba_live_boxscore`. Their core columns are guaranteed on every frame, even a zero-row one, at their declared dtypes: `game_id` on all six, plus `game_status`, `game_time_utc`, `home_team_id`, `away_team_id`, and `attendance` on `game`; `person_id`, `name`, `jersey_num`, and `assignment` on `officials`; `team_id`, `person_id`, `name`, `jersey_num`, `position`, `starter`, and `played` on the player frames; and `team_id`, `team_tricode`, and `score` on the team frames. Every other liveData field is passed through, snake-cased, with each nested `statistics` object flattened into `statistics_*` columns; a field only some players carry, such as `not_playing_reason`, is present only when a player on that side has it.

| col_name | type | description |
|---|---|---|
| `game.game_id` | character | 10-digit NBA/WNBA game id (zero-padded), from the payload's game.gameId. |
| `game.game_status` | integer | Numeric game status code from the feed: 1 scheduled, 2 in progress, 3 final. |
| `game.game_time_utc` | character | Scheduled tip-off time in UTC (ISO-8601 timestamp). |
| `game.home_team_id` | integer | NBA/WNBA team id of the home team, taken from the payload's homeTeam object. |
| `game.away_team_id` | integer | NBA/WNBA team id of the away team, taken from the payload's awayTeam object. |
| `game.attendance` | integer | Reported attendance figure for the game, when published by the feed. |
| `game.game_time_local` | character | Scheduled tip-off in the arena's local time, ISO-8601 with UTC offset, e.g. 2025-10-21T18:30:00-05:00. |
| `game.game_time_home` | character | Scheduled tip-off in the home team's local time, ISO-8601 with UTC offset; the same as game_time_local on the capture, whose two teams share the arena's time zone. |
| `game.game_time_away` | character | Scheduled tip-off in the away team's local time, ISO-8601 with UTC offset; the same as game_time_local on the capture, whose two teams share the arena's time zone. |
| `game.game_et` | character | Scheduled tip-off in US Eastern time, ISO-8601 with UTC offset, e.g. 2025-10-21T19:30:00-04:00. |
| `game.duration` | integer | Wall-clock length of the game in whole minutes, opening tip to final buzzer, e.g. 196 on the double-overtime capture, whose first and last play-by-play actions are 3h16m apart. |
| `game.game_code` | character | Game code written as the game date (YYYYMMDD), a slash, then the away and home team tricodes, e.g. 20251021/HOUOKC. |
| `game.game_status_text` | character | Game status as display text, e.g. Final. |
| `game.regulation_periods` | integer | Number of regulation periods in the game, 4. |
| `game.period` | integer | Current period, or the last period played on a final game; 5 and up are overtime periods (6 on the double-overtime capture). |
| `game.game_clock` | character | Time left in the current period as an ISO-8601 duration, e.g. PT00M00.00S on a final game. |
| `game.sellout` | character | Sellout flag as a string, "1" when the game sold out; "1" on the capture. |
| `officials.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) this officiating crew worked. |
| `officials.person_id` | integer | NBA/WNBA person id of the on-court official. |
| `officials.name` | character | Official's display name. |
| `officials.jersey_num` | character | Official's jersey number as a string. |
| `officials.assignment` | character | Crew role label from the feed (OFFICIAL1, OFFICIAL2 or OFFICIAL3, listed in no fixed order), or ALTERNATE for the extra official listed in playoff games. |
| `officials.name_i` | character | Official's first initial and last name, e.g. Z. Zarba. |
| `officials.first_name` | character | Official's first name as the feed writes it. |
| `officials.family_name` | character | Official's last name as the feed writes it. |
| `home_players.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's player entry. |
| `home_players.team_id` | integer | NBA/WNBA team id of the side (home or away) the player belongs to. |
| `home_players.person_id` | integer | NBA/WNBA player id. |
| `home_players.name` | character | Player's display name. |
| `home_players.jersey_num` | character | Player's jersey number as a string. |
| `home_players.position` | character | Starting-lineup slot, one each of SF, PF, C, SG, and PG across the five starters (order 1-5) rather than the player's roster position; null for non-starters. |
| `home_players.starter` | character | Feed's starter flag as a string ("1"/"0") for whether the player started the game. |
| `home_players.played` | character | Feed's flag as a string for whether the player recorded any playing time in the game ("1"/"0"). |
| `home_players.status` | character | Roster status, ACTIVE or INACTIVE (ruled out before the game); an ACTIVE player can still sit out, which played and not_playing_reason show. |
| `home_players.order` | integer | Player's place in the feed's roster listing for the team, from 1; the five starters are 1-5 (SF, PF, C, SG, PG), then the rest of the active roster, then inactive players. |
| `home_players.oncourt` | character | Flag as a string ("1"/"0") for whether the player is on the floor as of the capture; on a final game, the five on the floor at the final buzzer. |
| `home_players.name_i` | character | Player's first initial and last name, e.g. L. Dort or J. Smith Jr. |
| `home_players.first_name` | character | Player's first name as the feed writes it. |
| `home_players.family_name` | character | Player's last name including any suffix, e.g. Gilgeous-Alexander or Smith Jr. |
| `home_players.statistics_assists` | integer | Assists credited to the player. |
| `home_players.statistics_blocks` | integer | Opponent shots the player blocked. |
| `home_players.statistics_blocks_received` | integer | Player's shot attempts that an opponent blocked. |
| `home_players.statistics_field_goals_attempted` | integer | Field-goal attempts, statistics_two_pointers_attempted plus statistics_three_pointers_attempted. |
| `home_players.statistics_field_goals_made` | integer | Field goals made, statistics_two_pointers_made plus statistics_three_pointers_made. |
| `home_players.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted; 0 when the player took no shot. |
| `home_players.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `home_players.statistics_fouls_drawn` | integer | Fouls opponents committed on the player. |
| `home_players.statistics_fouls_personal` | integer | Personal fouls committed, offensive fouls included; technical fouls excluded. |
| `home_players.statistics_fouls_technical` | integer | Technical fouls charged to the player. |
| `home_players.statistics_free_throws_attempted` | integer | Free-throw attempts by the player. |
| `home_players.statistics_free_throws_made` | integer | Free throws the player made. |
| `home_players.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted; 0 when the player attempted none. |
| `home_players.statistics_minus` | double | Points the opponent scored while the player was on the floor, as a float (e.g. 92.0); 0.0 for a player who did not play. |
| `home_players.statistics_minutes` | character | Playing time as an ISO-8601 duration to the hundredth of a second, e.g. PT45M15.10S; PT00M00.00S for a player who did not play. |
| `home_players.statistics_minutes_calculated` | character | Playing time in whole minutes as an ISO-8601 duration, e.g. PT45M; usually statistics_minutes rounded to the nearest minute, but adjusted so the team's players add up to the team's statistics_minutes_calculated (a 20:32 stint shows PT20M and a 0:05.6 one PT01M). |
| `home_players.statistics_plus` | double | Points the player's team scored while the player was on the floor, as a float (e.g. 94.0); 0.0 for a player who did not play. |
| `home_players.statistics_plus_minus_points` | double | Plus-minus, statistics_plus minus statistics_minus, as a float (e.g. 2.0). |
| `home_players.statistics_points` | integer | Points the player scored. |
| `home_players.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `home_players.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `home_players.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `home_players.statistics_rebounds_defensive` | integer | Defensive rebounds the player grabbed. |
| `home_players.statistics_rebounds_offensive` | integer | Offensive rebounds the player grabbed. |
| `home_players.statistics_rebounds_total` | integer | Total rebounds, statistics_rebounds_offensive plus statistics_rebounds_defensive. |
| `home_players.statistics_steals` | integer | Steals credited to the player. |
| `home_players.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts. |
| `home_players.statistics_three_pointers_made` | integer | Three-point field goals made. |
| `home_players.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted; 0 when the player attempted none. |
| `home_players.statistics_turnovers` | integer | Turnovers charged to the player. |
| `home_players.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts. |
| `home_players.statistics_two_pointers_made` | integer | Two-point field goals made. |
| `home_players.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted; 0 when the player attempted none. |
| `home_players.not_playing_reason` | character | Code for why the player did not play, e.g. INACTIVE_INJURY, INACTIVE_GLEAGUE_TWOWAY, or DND_INJURY; null for everyone else, including some players who sat out with no stated reason. The column is present only when a player on that side has one. |
| `home_players.not_playing_description` | character | Free-text detail for not_playing_reason, for an injury the body part and the injury, e.g. "Left Knee; Contusion" (sometimes with a trailing space); null when none is given, as on INACTIVE_GLEAGUE_TWOWAY. The column is present only when a player on that side has one. |
| `away_players.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's player entry. |
| `away_players.team_id` | integer | NBA/WNBA team id of the side (home or away) the player belongs to. |
| `away_players.person_id` | integer | NBA/WNBA player id. |
| `away_players.name` | character | Player's display name. |
| `away_players.jersey_num` | character | Player's jersey number as a string. |
| `away_players.position` | character | Starting-lineup slot, one each of SF, PF, C, SG, and PG across the five starters (order 1-5) rather than the player's roster position; null for non-starters. |
| `away_players.starter` | character | Feed's starter flag as a string ("1"/"0") for whether the player started the game. |
| `away_players.played` | character | Feed's flag as a string for whether the player recorded any playing time in the game ("1"/"0"). |
| `away_players.status` | character | Roster status, ACTIVE or INACTIVE (ruled out before the game); an ACTIVE player can still sit out, which played and not_playing_reason show. |
| `away_players.order` | integer | Player's place in the feed's roster listing for the team, from 1; the five starters are 1-5 (SF, PF, C, SG, PG), then the rest of the active roster, then inactive players. |
| `away_players.oncourt` | character | Flag as a string ("1"/"0") for whether the player is on the floor as of the capture; on a final game, the five on the floor at the final buzzer. |
| `away_players.name_i` | character | Player's first initial and last name, e.g. L. Dort or J. Smith Jr. |
| `away_players.first_name` | character | Player's first name as the feed writes it. |
| `away_players.family_name` | character | Player's last name including any suffix, e.g. Gilgeous-Alexander or Smith Jr. |
| `away_players.statistics_assists` | integer | Assists credited to the player. |
| `away_players.statistics_blocks` | integer | Opponent shots the player blocked. |
| `away_players.statistics_blocks_received` | integer | Player's shot attempts that an opponent blocked. |
| `away_players.statistics_field_goals_attempted` | integer | Field-goal attempts, statistics_two_pointers_attempted plus statistics_three_pointers_attempted. |
| `away_players.statistics_field_goals_made` | integer | Field goals made, statistics_two_pointers_made plus statistics_three_pointers_made. |
| `away_players.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted; 0 when the player took no shot. |
| `away_players.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `away_players.statistics_fouls_drawn` | integer | Fouls opponents committed on the player. |
| `away_players.statistics_fouls_personal` | integer | Personal fouls committed, offensive fouls included; technical fouls excluded. |
| `away_players.statistics_fouls_technical` | integer | Technical fouls charged to the player. |
| `away_players.statistics_free_throws_attempted` | integer | Free-throw attempts by the player. |
| `away_players.statistics_free_throws_made` | integer | Free throws the player made. |
| `away_players.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted; 0 when the player attempted none. |
| `away_players.statistics_minus` | double | Points the opponent scored while the player was on the floor, as a float (e.g. 92.0); 0.0 for a player who did not play. |
| `away_players.statistics_minutes` | character | Playing time as an ISO-8601 duration to the hundredth of a second, e.g. PT45M15.10S; PT00M00.00S for a player who did not play. |
| `away_players.statistics_minutes_calculated` | character | Playing time in whole minutes as an ISO-8601 duration, e.g. PT45M; usually statistics_minutes rounded to the nearest minute, but adjusted so the team's players add up to the team's statistics_minutes_calculated (a 20:32 stint shows PT20M and a 0:05.6 one PT01M). |
| `away_players.statistics_plus` | double | Points the player's team scored while the player was on the floor, as a float (e.g. 94.0); 0.0 for a player who did not play. |
| `away_players.statistics_plus_minus_points` | double | Plus-minus, statistics_plus minus statistics_minus, as a float (e.g. 2.0). |
| `away_players.statistics_points` | integer | Points the player scored. |
| `away_players.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `away_players.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `away_players.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `away_players.statistics_rebounds_defensive` | integer | Defensive rebounds the player grabbed. |
| `away_players.statistics_rebounds_offensive` | integer | Offensive rebounds the player grabbed. |
| `away_players.statistics_rebounds_total` | integer | Total rebounds, statistics_rebounds_offensive plus statistics_rebounds_defensive. |
| `away_players.statistics_steals` | integer | Steals credited to the player. |
| `away_players.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts. |
| `away_players.statistics_three_pointers_made` | integer | Three-point field goals made. |
| `away_players.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted; 0 when the player attempted none. |
| `away_players.statistics_turnovers` | integer | Turnovers charged to the player. |
| `away_players.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts. |
| `away_players.statistics_two_pointers_made` | integer | Two-point field goals made. |
| `away_players.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted; 0 when the player attempted none. |
| `away_players.not_playing_reason` | character | Code for why the player did not play, e.g. INACTIVE_INJURY, INACTIVE_GLEAGUE_TWOWAY, or DND_INJURY; null for everyone else, including some players who sat out with no stated reason. The column is present only when a player on that side has one. |
| `away_players.not_playing_description` | character | Free-text detail for not_playing_reason, for an injury the body part and the injury, e.g. "Left Knee; Contusion" (sometimes with a trailing space); null when none is given, as on INACTIVE_GLEAGUE_TWOWAY. The column is present only when a player on that side has one. |
| `home_team.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's team entry. |
| `home_team.team_id` | integer | NBA/WNBA team id of the side (home or away). |
| `home_team.team_tricode` | character | Three-letter team code, e.g. OKC or HOU. |
| `home_team.score` | integer | Team's current or final score. |
| `home_team.team_name` | character | Team nickname, e.g. Thunder or Rockets. |
| `home_team.team_city` | character | Team's location label, e.g. Oklahoma City or Houston. |
| `home_team.in_bonus` | character | Flag as a string ("1"/"0") for whether the team is in the bonus as of the capture, i.e. the opponent has reached the team-foul penalty in the current period (the last period on a final game). |
| `home_team.timeouts_remaining` | integer | Timeouts the team has left as of the capture (at the end of the game on a final). |
| `home_team.statistics_assists` | integer | Team assists, the players' assists summed. |
| `home_team.statistics_assists_turnover_ratio` | double | Assists divided by statistics_turnovers_total (team turnovers included), e.g. 29 / 12 = 2.4167. |
| `home_team.statistics_bench_points` | integer | Points scored by the players who did not start. |
| `home_team.statistics_biggest_lead` | integer | Team's largest lead in points during the game. |
| `home_team.statistics_biggest_lead_score` | character | Score when the team first reached its biggest lead, written away-home, e.g. "104-110" for the home team's 6-point lead. |
| `home_team.statistics_biggest_scoring_run` | integer | Team's longest run of unanswered points in the game. |
| `home_team.statistics_biggest_scoring_run_score` | character | Score when the team's longest run ended, written away-home, e.g. "104-110". |
| `home_team.statistics_blocks` | integer | Opponent shots the team blocked. |
| `home_team.statistics_blocks_received` | integer | Team's shot attempts that were blocked, equal to the opponent's blocks. |
| `home_team.statistics_fast_break_points_attempted` | integer | Fast-break field-goal attempts; free throws are not counted. |
| `home_team.statistics_fast_break_points_made` | integer | Fast-break field goals made; free throws are not counted. |
| `home_team.statistics_fast_break_points_percentage` | double | statistics_fast_break_points_made / statistics_fast_break_points_attempted as a 0-1 fraction. |
| `home_team.statistics_field_goals_attempted` | integer | Field-goal attempts by the team's players, the sum of the player rows; a heave counted in statistics_team_field_goal_attempts is not included. |
| `home_team.statistics_field_goals_effective_adjusted` | double | Effective field-goal percentage as a 0-1 fraction, (statistics_field_goals_made + 0.5 * statistics_three_pointers_made) / statistics_field_goals_attempted. |
| `home_team.statistics_field_goals_made` | integer | Field goals made by the team's players. |
| `home_team.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted. |
| `home_team.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `home_team.statistics_fouls_drawn` | integer | Fouls drawn, equal to the opponent's statistics_fouls_personal on the capture. |
| `home_team.statistics_fouls_personal` | integer | Personal fouls committed by the team's players, offensive fouls included and technical fouls excluded. |
| `home_team.statistics_fouls_team` | integer | Team fouls, statistics_fouls_personal minus statistics_fouls_offensive on the capture. |
| `home_team.statistics_fouls_technical` | integer | Technical fouls charged to the team's players (0 on the capture). |
| `home_team.statistics_fouls_team_technical` | integer | Technical fouls charged to the team rather than a player (0 on the capture). |
| `home_team.statistics_free_throws_attempted` | integer | Free-throw attempts by the team's players. |
| `home_team.statistics_free_throws_made` | integer | Free throws made by the team's players. |
| `home_team.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted. |
| `home_team.statistics_lead_changes` | integer | Number of lead changes in the game, the same on both teams' rows. |
| `home_team.statistics_minutes` | character | Total playing time of the team's players as an ISO-8601 duration, five times the game length, e.g. PT290M00.00S for a double-overtime game. |
| `home_team.statistics_minutes_calculated` | character | Total playing time in whole minutes as an ISO-8601 duration, e.g. PT290M; the players' statistics_minutes_calculated add up to it. |
| `home_team.statistics_points` | integer | Team points, equal to score on the capture. |
| `home_team.statistics_points_against` | integer | Points the opponent scored. |
| `home_team.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `home_team.statistics_points_from_turnovers` | integer | Points scored off the opponent's turnovers, free throws included. |
| `home_team.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `home_team.statistics_points_in_the_paint_attempted` | integer | Field-goal attempts in the paint. |
| `home_team.statistics_points_in_the_paint_made` | integer | Field goals made in the paint. |
| `home_team.statistics_points_in_the_paint_percentage` | double | statistics_points_in_the_paint_made / statistics_points_in_the_paint_attempted as a 0-1 fraction. |
| `home_team.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `home_team.statistics_rebounds_defensive` | integer | Defensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_defensive. |
| `home_team.statistics_rebounds_offensive` | integer | Offensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_offensive. |
| `home_team.statistics_rebounds_personal` | integer | Rebounds by the team's players, statistics_rebounds_defensive plus statistics_rebounds_offensive. |
| `home_team.statistics_rebounds_team` | integer | Team rebounds credited to the team rather than a player, statistics_rebounds_team_defensive plus statistics_rebounds_team_offensive. |
| `home_team.statistics_rebounds_team_defensive` | integer | Defensive rebounds credited to the team rather than a player. |
| `home_team.statistics_rebounds_team_offensive` | integer | Offensive rebounds credited to the team rather than a player. |
| `home_team.statistics_rebounds_total` | integer | All rebounds, statistics_rebounds_personal plus statistics_rebounds_team. |
| `home_team.statistics_second_chance_points_attempted` | integer | Second-chance field-goal attempts; free throws are not counted. |
| `home_team.statistics_second_chance_points_made` | integer | Second-chance field goals made; free throws are not counted. |
| `home_team.statistics_second_chance_points_percentage` | double | statistics_second_chance_points_made / statistics_second_chance_points_attempted as a 0-1 fraction. |
| `home_team.statistics_steals` | integer | Steals by the team's players. |
| `home_team.statistics_team_field_goal_attempts` | integer | Field-goal attempts credited to the team rather than a player and left out of statistics_field_goals_attempted, e.g. an end-of-quarter heave (1 for Houston on the capture, whose play-by-play logs one heave). |
| `home_team.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts by the team's players. |
| `home_team.statistics_three_pointers_made` | integer | Three-point field goals made by the team's players. |
| `home_team.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted. |
| `home_team.statistics_time_leading` | character | Game-clock time the team held the lead, as an ISO-8601 duration, e.g. PT10M18.70S. |
| `home_team.statistics_times_tied` | integer | Number of times the score was tied after 0-0, the same on both teams' rows. |
| `home_team.statistics_true_shooting_attempts` | double | True-shooting attempts, statistics_field_goals_attempted + 0.44 * statistics_free_throws_attempted. |
| `home_team.statistics_true_shooting_percentage` | double | True-shooting percentage as a 0-1 fraction, statistics_points / (2 * statistics_true_shooting_attempts). |
| `home_team.statistics_turnovers` | integer | Turnovers by the team's players; team turnovers are in statistics_turnovers_team. |
| `home_team.statistics_turnovers_team` | integer | Turnovers charged to the team rather than a player, e.g. a shot-clock violation. |
| `home_team.statistics_turnovers_total` | integer | All turnovers, statistics_turnovers plus statistics_turnovers_team. |
| `home_team.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts by the team's players. |
| `home_team.statistics_two_pointers_made` | integer | Two-point field goals made by the team's players. |
| `home_team.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted. |
| `away_team.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's team entry. |
| `away_team.team_id` | integer | NBA/WNBA team id of the side (home or away). |
| `away_team.team_tricode` | character | Three-letter team code, e.g. OKC or HOU. |
| `away_team.score` | integer | Team's current or final score. |
| `away_team.team_name` | character | Team nickname, e.g. Thunder or Rockets. |
| `away_team.team_city` | character | Team's location label, e.g. Oklahoma City or Houston. |
| `away_team.in_bonus` | character | Flag as a string ("1"/"0") for whether the team is in the bonus as of the capture, i.e. the opponent has reached the team-foul penalty in the current period (the last period on a final game). |
| `away_team.timeouts_remaining` | integer | Timeouts the team has left as of the capture (at the end of the game on a final). |
| `away_team.statistics_assists` | integer | Team assists, the players' assists summed. |
| `away_team.statistics_assists_turnover_ratio` | double | Assists divided by statistics_turnovers_total (team turnovers included), e.g. 29 / 12 = 2.4167. |
| `away_team.statistics_bench_points` | integer | Points scored by the players who did not start. |
| `away_team.statistics_biggest_lead` | integer | Team's largest lead in points during the game. |
| `away_team.statistics_biggest_lead_score` | character | Score when the team first reached its biggest lead, written away-home, e.g. "104-110" for the home team's 6-point lead. |
| `away_team.statistics_biggest_scoring_run` | integer | Team's longest run of unanswered points in the game. |
| `away_team.statistics_biggest_scoring_run_score` | character | Score when the team's longest run ended, written away-home, e.g. "104-110". |
| `away_team.statistics_blocks` | integer | Opponent shots the team blocked. |
| `away_team.statistics_blocks_received` | integer | Team's shot attempts that were blocked, equal to the opponent's blocks. |
| `away_team.statistics_fast_break_points_attempted` | integer | Fast-break field-goal attempts; free throws are not counted. |
| `away_team.statistics_fast_break_points_made` | integer | Fast-break field goals made; free throws are not counted. |
| `away_team.statistics_fast_break_points_percentage` | double | statistics_fast_break_points_made / statistics_fast_break_points_attempted as a 0-1 fraction. |
| `away_team.statistics_field_goals_attempted` | integer | Field-goal attempts by the team's players, the sum of the player rows; a heave counted in statistics_team_field_goal_attempts is not included. |
| `away_team.statistics_field_goals_effective_adjusted` | double | Effective field-goal percentage as a 0-1 fraction, (statistics_field_goals_made + 0.5 * statistics_three_pointers_made) / statistics_field_goals_attempted. |
| `away_team.statistics_field_goals_made` | integer | Field goals made by the team's players. |
| `away_team.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted. |
| `away_team.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `away_team.statistics_fouls_drawn` | integer | Fouls drawn, equal to the opponent's statistics_fouls_personal on the capture. |
| `away_team.statistics_fouls_personal` | integer | Personal fouls committed by the team's players, offensive fouls included and technical fouls excluded. |
| `away_team.statistics_fouls_team` | integer | Team fouls, statistics_fouls_personal minus statistics_fouls_offensive on the capture. |
| `away_team.statistics_fouls_technical` | integer | Technical fouls charged to the team's players (0 on the capture). |
| `away_team.statistics_fouls_team_technical` | integer | Technical fouls charged to the team rather than a player (0 on the capture). |
| `away_team.statistics_free_throws_attempted` | integer | Free-throw attempts by the team's players. |
| `away_team.statistics_free_throws_made` | integer | Free throws made by the team's players. |
| `away_team.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted. |
| `away_team.statistics_lead_changes` | integer | Number of lead changes in the game, the same on both teams' rows. |
| `away_team.statistics_minutes` | character | Total playing time of the team's players as an ISO-8601 duration, five times the game length, e.g. PT290M00.00S for a double-overtime game. |
| `away_team.statistics_minutes_calculated` | character | Total playing time in whole minutes as an ISO-8601 duration, e.g. PT290M; the players' statistics_minutes_calculated add up to it. |
| `away_team.statistics_points` | integer | Team points, equal to score on the capture. |
| `away_team.statistics_points_against` | integer | Points the opponent scored. |
| `away_team.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `away_team.statistics_points_from_turnovers` | integer | Points scored off the opponent's turnovers, free throws included. |
| `away_team.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `away_team.statistics_points_in_the_paint_attempted` | integer | Field-goal attempts in the paint. |
| `away_team.statistics_points_in_the_paint_made` | integer | Field goals made in the paint. |
| `away_team.statistics_points_in_the_paint_percentage` | double | statistics_points_in_the_paint_made / statistics_points_in_the_paint_attempted as a 0-1 fraction. |
| `away_team.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `away_team.statistics_rebounds_defensive` | integer | Defensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_defensive. |
| `away_team.statistics_rebounds_offensive` | integer | Offensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_offensive. |
| `away_team.statistics_rebounds_personal` | integer | Rebounds by the team's players, statistics_rebounds_defensive plus statistics_rebounds_offensive. |
| `away_team.statistics_rebounds_team` | integer | Team rebounds credited to the team rather than a player, statistics_rebounds_team_defensive plus statistics_rebounds_team_offensive. |
| `away_team.statistics_rebounds_team_defensive` | integer | Defensive rebounds credited to the team rather than a player. |
| `away_team.statistics_rebounds_team_offensive` | integer | Offensive rebounds credited to the team rather than a player. |
| `away_team.statistics_rebounds_total` | integer | All rebounds, statistics_rebounds_personal plus statistics_rebounds_team. |
| `away_team.statistics_second_chance_points_attempted` | integer | Second-chance field-goal attempts; free throws are not counted. |
| `away_team.statistics_second_chance_points_made` | integer | Second-chance field goals made; free throws are not counted. |
| `away_team.statistics_second_chance_points_percentage` | double | statistics_second_chance_points_made / statistics_second_chance_points_attempted as a 0-1 fraction. |
| `away_team.statistics_steals` | integer | Steals by the team's players. |
| `away_team.statistics_team_field_goal_attempts` | integer | Field-goal attempts credited to the team rather than a player and left out of statistics_field_goals_attempted, e.g. an end-of-quarter heave (1 for Houston on the capture, whose play-by-play logs one heave). |
| `away_team.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts by the team's players. |
| `away_team.statistics_three_pointers_made` | integer | Three-point field goals made by the team's players. |
| `away_team.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted. |
| `away_team.statistics_time_leading` | character | Game-clock time the team held the lead, as an ISO-8601 duration, e.g. PT10M18.70S. |
| `away_team.statistics_times_tied` | integer | Number of times the score was tied after 0-0, the same on both teams' rows. |
| `away_team.statistics_true_shooting_attempts` | double | True-shooting attempts, statistics_field_goals_attempted + 0.44 * statistics_free_throws_attempted. |
| `away_team.statistics_true_shooting_percentage` | double | True-shooting percentage as a 0-1 fraction, statistics_points / (2 * statistics_true_shooting_attempts). |
| `away_team.statistics_turnovers` | integer | Turnovers by the team's players; team turnovers are in statistics_turnovers_team. |
| `away_team.statistics_turnovers_team` | integer | Turnovers charged to the team rather than a player, e.g. a shot-clock violation. |
| `away_team.statistics_turnovers_total` | integer | All turnovers, statistics_turnovers plus statistics_turnovers_team. |
| `away_team.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts by the team's players. |
| `away_team.statistics_two_pointers_made` | integer | Two-point field goals made by the team's players. |
| `away_team.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted. |

**Example**

```python
from sportsdataverse.nba.nba_live import nba_live_boxscore
result = nba_live_boxscore("0022500001")
officials = result["officials"]
print(officials.select("person_id", "name", "assignment"))
```

### nba_live_pbp {#nba_live_pbp}

`nba_live_pbp(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict[str, str] | None' = None) -> 'Any'`

Fetch and parse NBA cdn.nba.com liveData play-by-play for a game.

Retrieves `https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json`
and parses it via `parse_nba_live_pbp`. Unlike stats.nba.com's
play-by-play, this feed carries per-whistle referee ids (`official_id`,
populated on every foul since the 2019-20 season) and wall-clock timestamps
(`time_actual`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | NBA game ID (int or str). Zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of a DataFrame. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame instead of polars. |
| `proxy` | `dict[str, str] \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a DataFrame with one row per action, parsed by `parse_nba_live_pbp`. Its 13 core columns (`game_id`, `action_number`, `period`, `clock`, `time_actual`, `action_type`, `sub_type`, `team_id`, `person_id`, `official_id`, `x_legacy`, `y_legacy`, `description`) are guaranteed on every frame, even a zero-row one, at their declared dtypes. Every other liveData action field is passed through, snake-cased, when the payload carries it, so an event-specific column such as `block_person_id` or `foul_drawn_person_id` is present only when the game had that event.

| col_name | type | description |
|---|---|---|
| `game_id` | character | 10-digit NBA/WNBA game id (zero-padded), stamped onto every action from the payload's game.gameId. |
| `action_number` | integer | Action id from the liveData feed's actionNumber field, assigned in the order actions are logged rather than strictly in game order (an action logged late gets a higher number than plays that followed it); sort by order_number for game order. |
| `period` | integer | Period of the game: 1-4 for quarters, 5+ for overtime periods. |
| `clock` | character | ISO-8601 duration game clock at the action, e.g. "PT11M25.00S", not yet converted to MM:SS. |
| `time_actual` | character | Wall-clock UTC timestamp when the action occurred, letting plays be matched to real elapsed time. |
| `action_type` | character | Action category from the feed, lower-cased, e.g. 2pt, 3pt, foul, substitution, or turnover. |
| `sub_type` | character | Action sub-type as the feed writes it, e.g. personal or offensive for a foul, Jump Shot or Layup for a 2pt/3pt shot, 1 of 2 for a free throw. |
| `team_id` | integer | NBA/WNBA team id of the team associated with the action, when applicable. |
| `person_id` | integer | NBA/WNBA player id of the primary person involved in the action, when applicable. |
| `official_id` | integer | Referee's person id for the whistle on this action; populated on every foul since the 2019-20 season, and also on whistled turnovers and violations such as traveling, out-of-bounds, or kicked ball (null on live-ball turnovers like a bad pass). |
| `x_legacy` | double | Shot x-coordinate in the legacy stats.nba.com coordinate system; populated only on 2pt/3pt shots (fouls and blocks carry a court zone in area/area_detail instead). |
| `y_legacy` | double | Shot y-coordinate in the legacy stats.nba.com coordinate system; populated only on 2pt/3pt shots (fouls and blocks carry a court zone in area/area_detail instead). |
| `description` | character | Long-form human-readable description of the action, as shown on NBA.com's live scoreboard. |
| `period_type` | character | Period kind from the feed, REGULAR for the four quarters and OVERTIME for an overtime period. |
| `possession` | integer | Team id of the team with the ball as of the action (on a rebound or steal, the team that gained it); 0 on game and period start/end rows. |
| `score_home` | character | Home team's running score after the action, stored as a string. |
| `score_away` | character | Away team's running score after the action, stored as a string. |
| `order_number` | integer | The feed's sort key for the action (actions ship in this order); sorting by it gives game order even where action_number does not, since an action logged late is slotted in at its place in the game. |
| `edited` | character | UTC timestamp (ISO-8601, whole seconds) of the feed's last edit to the action, never earlier than time_actual. |
| `qualifiers` | list | List of context tags on the action, e.g. pointsinthepaint, fastbreak, 2ndchance, or fromturnover on a shot, 2freethrow or inpenalty on a foul, and team on a team rebound or turnover; empty when none apply. |
| `descriptor` | character | Extra detail on sub_type, e.g. driving or step back on a shot, shooting or loose ball on a foul, bad pass on a turnover, heldball or startperiod on a jump ball; null when absent. |
| `is_target_score_last_period` | logical | Feed flag for a last period played to a target score instead of the game clock (the Elam-ending format); False on every action of a regular game. |
| `team_tricode` | character | Three-letter code of the team in team_id, e.g. OKC or HOU. |
| `player_name` | character | Last name of the action's primary player (person_id) as the feed writes it, e.g. Gilgeous-Alexander; null on team and administrative actions. |
| `player_name_i` | character | First initial and last name of the action's primary player (person_id), e.g. S. Gilgeous-Alexander; null on team and administrative actions. |
| `person_ids_filter` | list | List of every player id on the action, i.e. person_id plus any assist, block, steal, foul-drawn, or jump-ball participant; empty when no player is involved. |
| `is_field_goal` | integer | Field-goal attempt flag, 1 on a 2pt/3pt shot and 0 on every other action (including a heave). |
| `shot_result` | character | Made or Missed; set on 2pt/3pt shots and free throws. |
| `shot_distance` | double | Shot distance from the basket in feet; set on 2pt/3pt shots and heaves. |
| `x` | double | Shot location along the court's length on a 0-100 scale from the left baseline; populated only on 2pt/3pt shots, like x_legacy. |
| `y` | double | Shot location across the court's width on a 0-100 scale, 50 at the baskets' centerline; populated only on 2pt/3pt shots, like y_legacy. |
| `side` | character | Court half of the shot, left or right (left where x is below 50); populated only on 2pt/3pt shots. |
| `area` | character | Court zone of the action, e.g. Restricted Area, In The Paint (Non-RA), Mid-Range, Left Corner 3, Right Corner 3, or Above the Break 3; set on 2pt/3pt shots and also on heaves, fouls, blocks, steals, turnovers, and most rebounds, which carry no x/y coordinates. |
| `area_detail` | character | Finer zone for the same actions as area, written as a distance band in feet plus a direction, e.g. "0-8 Center", "16-24 Left Center", or "24+ Right". |
| `points_total` | integer | Scorer's running point total in the game, including this basket; set on made shots and made free throws. |
| `assist_person_id` | integer | Player id credited with the assist on a made 2pt/3pt shot; null on unassisted and missed shots. |
| `assist_player_name_initial` | character | First initial and last name of the player credited with the assist on a made 2pt/3pt shot, e.g. L. Dort. |
| `assist_total` | integer | Assisting player's running assist total in the game, including this assist. |
| `shot_action_number` | integer | On a rebound, the action_number of the missed shot or free throw being rebounded. |
| `rebound_total` | integer | Rebounder's running rebound total in the game, including this rebound (rebound_offensive_total plus rebound_defensive_total); null on a team rebound. |
| `rebound_offensive_total` | integer | Rebounder's running offensive-rebound total in the game as of this rebound; null on a team rebound. |
| `rebound_defensive_total` | integer | Rebounder's running defensive-rebound total in the game as of this rebound; null on a team rebound. |
| `foul_drawn_person_id` | integer | Player id of the opponent who drew the foul; null when no player drew it (e.g. a technical foul). |
| `foul_drawn_player_name` | character | Last name of the opponent who drew the foul; null when no player drew it (e.g. a technical foul). |
| `foul_personal_total` | integer | Fouling player's running personal-foul count in the game as of this foul (a technical foul leaves it unchanged); null on a team foul. |
| `foul_technical_total` | integer | Fouling player's running technical-foul count in the game as of this foul; null on a team foul. |
| `turnover_total` | integer | Player's running turnover count in the game, including this turnover; null on a team turnover. |
| `steal_person_id` | integer | Player id credited with the steal, on the turnover row it forced; the steal is also logged as its own steal action. |
| `steal_player_name` | character | Last name of the player credited with the steal, on the turnover row it forced. |
| `block_person_id` | integer | Player id credited with the block, on the blocked (missed) shot's row; the block is also logged as its own block action. |
| `block_player_name` | character | Last name of the player credited with the block, on the blocked (missed) shot's row. |
| `jump_ball_won_person_id` | integer | Player id of the jumper who won the tip on a jump ball. |
| `jump_ball_won_player_name` | character | Last name of the jumper who won the tip on a jump ball. |
| `jump_ball_lost_person_id` | integer | Player id of the jumper who lost the tip on a jump ball. |
| `jump_ball_lost_player_name` | character | Last name of the jumper who lost the tip on a jump ball. |
| `jump_ball_recoverd_person_id` | integer | Player id of the player who recovered the tip on a jump ball, null when a team recovered it; the column keeps the feed's own spelling (jumpBallRecoverdPersonId). |
| `jump_ball_recovered_name` | character | First initial and last name of the player who recovered the tip on a jump ball, or a team label such as "Team (OKC)" when a team recovered it. |

**Example**

```python
from sportsdataverse.nba.nba_live import nba_live_pbp
pbp = nba_live_pbp("0022500001")
print(pbp.filter(pbp["action_type"] == "foul").height)

# Pipeline next step (fouls with a referee id)

fouls = pbp.filter(pbp["action_type"] == "foul").select("official_id", "time_actual")
```

### nba_matchup_drapm {#nba_matchup_drapm}

`nba_matchup_drapm(season: 'str', *, league_id: 'str' = '00', matchups: 'Optional[pl.DataFrame]' = None, config: 'Optional[PlaytypeConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Matchup-based defensive RAPM (offense-quality-controlled).

Fits `points_allowed_per_100 ~ defender_FE + offense_FE` via
`~sklearn.linear_model.RidgeCV` (weighted by matchup possessions),
reusing the shipped RAPM ridge machinery on the
`build_matchup_drapm_design` two-way-FE design.

**Sign + scale:** the design target `y` is already points-allowed *per 100*
matchup possessions (`100 * player_pts / partial_poss`), so the defender
coefficient is already on the per-100 scale -- `matchup_drapm =
-(beta_defender - mean_beta_defender)` (centered, NO extra ×100, unlike
`~sportsdataverse.nba.nba_rapm.nba_rapm` whose `y` is per-*possession*
and needs the ×100). Sign is negated so higher = better defense (fewer points
allowed), matching the `d_rapm` convention. Typical magnitudes are a few to
low-double-digit points per 100 vs the league defender average.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig`; defaults to a fresh instance (`ridge_alphas` = the shared RAPM grid, `min_matchup_poss` = 25.0 inclusion floor). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per defender: `player_id` (Int64), `matchup_drapm` (Float64, points-allowed-per-100 estimate, higher = better defense), `matchup_poss` (Float64, total matchup possessions guarded). Returns a zero-row frame with this schema when the upstream fetch/injection is empty or no row survives the `min_matchup_poss` floor (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_matchup_drapm
d = nba_matchup_drapm("2023-24")
print(d.sort("matchup_drapm", descending=True).head(10))

# Injected offline (oracle / test) path

d = nba_matchup_drapm("2023-24", matchups=matchups_df)

# Pipeline next step

d.filter(pl.col("matchup_poss") >= 200).sort("matchup_drapm", descending=True)
```

### nba_pbp_disk {#nba_pbp_disk}

`nba_pbp_disk(game_id, path_to_json)`

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads `{path_to_json}/{game_id}.json`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `path_to_json` | `str` |  | Directory containing the cached JSON file. |

**Returns**

Parsed JSON contents.

**Example**

```python
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

### nba_play_context {#nba_play_context}

`nba_play_context(game_id: 'str', league_id: 'str' = '00', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch one game and return its possessions with the full CTG play-context surface.

Single live call to `nba_stats_playbyplayv3`, then
`add_play_context`. Works for NBA (`league_id="00"`), WNBA and the
G-League — `stats.wnba.com` ships the same play-by-play shapes, and every
threshold here is league-agnostic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | Ten-character game identifier (e.g. `"0022200001"`). |
| `league_id` | `str` | `'00'` | League identifier (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Possession frame with the play-context columns. Empty/malformed payloads return a zero-row frame — never raises.

**Example**

```python
from sportsdataverse.nba.nba_play_context import nba_play_context
poss = nba_play_context("0022200001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate for the game

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
)
print(clean["is_transition"].mean())
```

### nba_player_ages {#nba_player_ages}

`nba_player_ages(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Per-player age for a season (bulk), for the DARKO aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguedashplayerbiostats` replacement for offline tests. |

**Returns**

Frame `player_id:Int64, age:Float64`.

**Example**

```python
from sportsdataverse.nba import nba_player_ages
ages = nba_player_ages("2023-24")
print(ages.head())
```

### nba_player_crosswalk {#nba_player_crosswalk}

`nba_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source player crosswalk (ESPN / NBA Stats / Fox).

One row per ESPN athlete per team. `match_method` / `match_confidence`
describe the **Stats API** match (normalized exact name, then
Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
`fox_athlete_id` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention. Defaults to the most recent NBA season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 21 columns.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_name` | character | Player name. |
| `espn_athlete_id` | character | ESPN athlete id. |
| `espn_full_name` | character | ESPN full name. |
| `espn_jersey` | character | ESPN jersey number. |
| `espn_position` | character | ESPN position abbreviation. |
| `nba_player_id` | character | NBA Stats API (stats.nba.com) player id as a string, matched to the ESPN athlete within the same team by normalized exact name, then Jaro-Winkler fuzzy name match (min_confidence, default 0.92) with jersey and birth-date tiebreaks; null when the athlete had no Stats match. |
| `nba_player_name` | character | Player name from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `nba_jersey_num` | character | Jersey number as a string from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `nba_position` | character | Position from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `fox_athlete_id` | character | Fox athlete id (NA if unmatched). |
| `fox_player` | character | Fox player name (NA if unmatched). |
| `fox_jersey` | character | Fox jersey number (NA if unmatched). |
| `fox_position_group` | character | Fox position group label (NA if unmatched). |
| `yahoo_player_id` | character | Yahoo player id (NA placeholder). |
| `yahoo_player_name` | character | Yahoo player name (NA placeholder). |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | double | Jaro-Winkler score or 1 for exact (NA if none). |
| `match_keys` | character | NA (reserved for future use). |

**Example**

```python
from sportsdataverse.nba import nba_player_crosswalk
df = nba_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = nba_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### nba_player_identity {#nba_player_identity}

`nba_player_identity(player_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Human-readable identity for every player in a season's box logs.

Model outputs key on `player_id` alone, which makes them unusable without a
second lookup -- a leaderboard reads `1628983` instead of
`Shai Gilgeous-Alexander`. This derives the display columns from the season's
own game logs, so they are **season-accurate**: a player's team is what he
actually played for that year, not his current one (which is what a player
directory would give and would silently mislabel every historical season).

A traded player has rows for several teams. `team_*` is his **primary** team
by minutes -- the one a reader means when they say "his team that season" --
and `teams` lists every abbreviation he appeared for, in descending minutes,
so a trade is visible rather than silently collapsed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game rows from `leaguegamelog` (the `player_or_team="P"` variant), carrying `player_id`, `player_name`, `team_id`, `team_abbreviation`, `team_name` and `min`. |

**Returns**

One row per `player_id` with `PLAYER_IDENTITY_SCHEMA`. An empty input -- or one missing any required column, `min` included -- gives the zero-row frame with that schema, so callers can join unconditionally. `min` is required rather than optional: without it every team totals zero minutes and "primary team" quietly degrades to whichever `team_id` sorts first, which looks like an answer but is not one.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_player_identity

logs = pl.DataFrame({
    "player_id": [1628983],
    "player_name": ["Shai Gilgeous-Alexander"],
    "team_id": [1610612760],
    "team_abbreviation": ["OKC"],
    "team_name": ["Oklahoma City Thunder"],
    "min": [34.0],
})
ratings = pl.DataFrame({"player_id": [1628983], "war": [21.9]})
named = ratings.join(nba_player_identity(logs), on="player_id", how="left")
print(named.select("player_name", "team_name", "war"))
```

### nba_player_positions {#nba_player_positions}

`nba_player_positions(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Fetch league-wide listed positions for a season as numeric 1-5.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_playerindex` replacement for offline tests. |

**Returns**

Frame with columns `player_id:Int64, position_num:Float64`.

**Example**

```python
from sportsdataverse.nba import nba_player_positions
pos = nba_player_positions("2023-24")
print(pos.head())

# Offline / injectable fetch for testing

import polars as pl
stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
pos = nba_player_positions("2023-24", fetch=stub)
```

### nba_player_props {#nba_player_props}

`nba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player expected prop lines + team pace projection for a matchup.

Loads the season's player box logs + team ratings, computes per-minute
`player_rates`, and projects each player's line onto their mean
minutes and the matchup's pace factor (`exp_poss / avg_pace`). Only the
two teams in the matchup are returned.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024`). |
| `game_id` | `str` |  | The game id (passed through for the caller's join; not used to filter historical rates). |
| `home_team_id` | `str` |  | Home team id. |
| `away_team_id` | `str` |  | Away team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player on either team: `player_id, team_id, stat_pts_exp, stat_reb_exp, stat_ast_exp, stat_fg3m_exp, pace_proj`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_player_props import nba_player_props
props = nba_player_props(2024, "401585828", "2", "6")
```

### nba_playtype_ratings {#nba_playtype_ratings}

`nba_playtype_ratings(season: 'str', *, league_id: 'str' = '00', off_team: 'Optional[pl.DataFrame]' = None, def_team: 'Optional[pl.DataFrame]' = None, schedule: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season play-type-adjusted offensive/defensive team ratings.

Fetches (or uses injected) Synergy offensive/defensive team frames plus the
league schedule, computes raw per-type efficiency
(`raw_playtype_efficiency`), opponent-adjusts it
(`adjust_playtype_efficiency`), then rolls up to one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id` (Int64), `adj_off`/`adj_def`/`adj_net` (Float64) roll-ups, plus per-type wide columns `adj_off_ppp_<playtype>`/`adj_def_ppp_<playtype>`/`off_freq_<playtype>` (Float64) for each play type present in the data. `adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100` (symmetric for `adj_def` off `def_freq_t`); `adj_net = adj_off - adj_def`. Returns a zero-row frame with the base roll-up schema when the upstream fetch is empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_playtype_ratings
r = nba_playtype_ratings("2023-24")
print(r.sort("adj_off", descending=True).head(10))

# Injected offline (oracle / test) path

r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

# Pipeline next step

r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)
```

### nba_predict_games {#nba_predict_games}

`nba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home/away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` / `predict_total`
math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace` (the `~sportsdataverse.nba.nba_team_ratings.nba_team_ratings` output for one season/as-of date). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
preds = nba_predict_games(games, nba_team_ratings(2024))
```

### nba_ratings_panel {#nba_ratings_panel}

`nba_ratings_panel(model: 'AnyModel', possessions: 'pl.DataFrame', dates: 'Optional[Sequence[datetime.date]]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Player-ratings-through-date long panel: one row per (player_id, date).

Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
calls `ratings_as_of` independently, so the panel is leakage-free by
construction: a possession dated after a given checkpoint can never affect
that checkpoint's row, no matter what other dates are also being computed
or what future rows exist in `possessions`. Cost is a full refit per
checkpoint date; for a season's sparse RAPM-family design this is seconds
per date, not minutes — acceptable for a nightly/daily cadence but not for
live in-game updating (out of scope; see spec non-goals).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel`. |
| `possessions` | `DataFrame` |  | A possession+lineup frame with a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `dates` | `Optional[Sequence[date]]` | `None` | Checkpoint dates to compute. `None` (default) uses every distinct `game_date` present in `possessions`, sorted ascending — a rating for every game day, matching what EPM/LEBRON publish nightly. Duplicates are deduped; input order does not matter (the output is always sorted by date). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Long frame with `RATINGS_PANEL_SCHEMA` columns (`player_id`, `date`, `o_rating`, `d_rating`, `rating`). Zero-row (that schema) when `possessions` is empty or no date yields any players.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
print(panel.filter(pl.col("player_id") == 201939).sort("date"))

# Every game day, no explicit grid

panel = nba_ratings_panel(RidgeRapmModel(), season_poss)
```

### nba_raw_store_season_frame {#nba_raw_store_season_frame}

`nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"`

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **END** year (2024 = 2023-24) -- the key both halves of the store are filed under. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```
