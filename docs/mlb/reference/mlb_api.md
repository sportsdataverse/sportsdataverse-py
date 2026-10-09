# MLB — MLB Stats API

> MLB — MLB Stats API — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.mlb` — 64 endpoints.

## All

| Function | Summary |
|---|---|
| [mlb_all_star_ballot](mlb_api/all.md#mlb_all_star_ballot) | View All-Star Ballots per league. |
| [mlb_all_star_write_ins](mlb_api/all.md#mlb_all_star_write_ins) | View All-Star Write-ins per league. |
| [mlb_all_star_final_vote](mlb_api/all.md#mlb_all_star_final_vote) | View All-Star Final Vote per league. |

## Draft

| Function | Summary |
|---|---|
| [mlb_draft](mlb_api/draft.md#mlb_draft) | GET /api/v1/draft/{year} — draft results for a year (optionally one round). |
| [mlb_draft_latest](mlb_api/draft.md#mlb_draft_latest) | View latest player drafted, endpoint best used when draft is currently open. |

## Game

| Function | Summary |
|---|---|
| [mlb_game_context_metrics](mlb_api/game.md#mlb_game_context_metrics) | GET /api/v1/game/{gamePk}/contextMetrics — WP, leverage index, in-game context. |
| [mlb_game_content](mlb_api/game.md#mlb_game_content) | GET /api/v1/game/{gamePk}/content — articles, highlights, editorial content. |
| [mlb_game_timestamps](mlb_api/game.md#mlb_game_timestamps) | Retrieve all of the play timecodes for a game in GUMBO feed. |
| [mlb_game_changes](mlb_api/game.md#mlb_game_changes) | View corrected non Statcast information for games |
| [mlb_game_guids](mlb_api/game.md#mlb_game_guids) | View Statcast data for a specific game. |
| [mlb_game_color](mlb_api/game.md#mlb_game_color) | View game color commentary info. |
| [mlb_game_color_diff](mlb_api/game.md#mlb_game_color_diff) | View game color feed. |
| [mlb_game_color_timestamps](mlb_api/game.md#mlb_game_color_timestamps) | View all of the color timecodes for a game. |
| [mlb_game_pace](mlb_api/game.md#mlb_game_pace) | View time of game info. |

## Home

| Function | Summary |
|---|---|
| [mlb_home_run_derby](mlb_api/home.md#mlb_home_run_derby) | View a home run derby object based on gamePk. |
| [mlb_home_run_derby_bracket](mlb_api/home.md#mlb_home_run_derby_bracket) | View a home run derby object based on bracket. |
| [mlb_home_run_derby_pool](mlb_api/home.md#mlb_home_run_derby_pool) | View a home run derby object based on pool. |

## Play

| Function | Summary |
|---|---|
| [mlb_play_by_play](mlb_api/play.md#mlb_play_by_play) | GET /api/v1/game/{gamePk}/playByPlay — play-by-play with at-bat detail. |
| [mlb_play_analytics](mlb_api/play.md#mlb_play_analytics) | View Statcast data for a specific play. |
| [mlb_play_context_metrics_averages](mlb_api/play.md#mlb_play_context_metrics_averages) | View Statcast contextMetrics data for a specific play. |

## Schedule

| Function | Summary |
|---|---|
| [mlb_schedule_postseason](mlb_api/schedule.md#mlb_schedule_postseason) | GET /api/v1/schedule/postseason — postseason-only schedule for a season. |
| [mlb_schedule_tied](mlb_api/schedule.md#mlb_schedule_tied) | View tied game schedule info. |
| [mlb_schedule_postseason_series](mlb_api/schedule.md#mlb_schedule_postseason_series) | View schedule info for postseason based on series. |
| [mlb_schedule_postseason_tunein](mlb_api/schedule.md#mlb_schedule_postseason_tunein) | View schedule info for the tuneIn application. |

## Team

| Function | Summary |
|---|---|
| [mlb_team](mlb_api/team.md#mlb_team) | GET /api/v1/teams/{teamId} — single team detail. |
| [mlb_team_roster](mlb_api/team.md#mlb_team_roster) | GET /api/v1/teams/{teamId}/roster — team roster. |
| [mlb_team_alumni](mlb_api/team.md#mlb_team_alumni) | GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season. |
| [mlb_team_affiliates](mlb_api/team.md#mlb_team_affiliates) | GET /api/v1/teams/affiliates — org affiliates (MLB parent → minor league chain). |
| [mlb_teams_history](mlb_api/team.md#mlb_teams_history) | View historical records for a list of teams. |
| [mlb_teams_stats](mlb_api/team.md#mlb_teams_stats) | View team stats. |
| [mlb_teams_stats_leaders](mlb_api/team.md#mlb_teams_stats_leaders) | View leaders for a statistic. |
| [mlb_team_coaches](mlb_api/team.md#mlb_team_coaches) | View biographical  information on all coaches for a given club. |
| [mlb_team_personnel](mlb_api/team.md#mlb_team_personnel) | View biographical  information on all personnel for a given club. |
| [mlb_team_roster_type](mlb_api/team.md#mlb_team_roster_type) | View biographical and statistical information for a club's roster based on roster type. |

## Other

| Function | Summary |
|---|---|
| [mlb_pbp](mlb_api/other.md#mlb_pbp) | GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1). |
| [mlb_boxscore](mlb_api/other.md#mlb_boxscore) | GET /api/v1/game/{gamePk}/boxscore — team + player boxscore for one game. |
| [mlb_linescore](mlb_api/other.md#mlb_linescore) | GET /api/v1/game/{gamePk}/linescore — inning-by-inning + current game state. |
| [mlb_win_probability](mlb_api/other.md#mlb_win_probability) | GET /api/v1/game/{gamePk}/winProbability — per-play WP timeline. |
| [mlb_people](mlb_api/other.md#mlb_people) | GET /api/v1/people?personIds=... — bulk person lookup by MLBAM id. |
| [mlb_person](mlb_api/other.md#mlb_person) | GET /api/v1/people/{personId} — single person detail. |
| [mlb_person_game_stats](mlb_api/other.md#mlb_person_game_stats) | GET /api/v1/people/{personId}/stats/game/{gamePk} — one player, one game. |
| [mlb_sport_players](mlb_api/other.md#mlb_sport_players) | GET /api/v1/sports/{sportId}/players — every player in a sport for a season. |
| [mlb_sports](mlb_api/other.md#mlb_sports) | GET /api/v1/sports — list known sports (MLB, MiLB, KBO, NPB, …). |
| [mlb_leagues](mlb_api/other.md#mlb_leagues) | GET /api/v1/leagues — list leagues. |
| [mlb_season](mlb_api/other.md#mlb_season) | GET /api/v1/seasons/{seasonId} — single season detail. |
| [mlb_venues](mlb_api/other.md#mlb_venues) | GET /api/v1/venues — list venues. |
| [mlb_venue](mlb_api/other.md#mlb_venue) | GET /api/v1/venues/{venueId} — single venue detail. |
| [mlb_meta](mlb_api/other.md#mlb_meta) | GET /api/v1/{metaType} — enum lookup (the API's self-describing surface). |
| [mlb_awards](mlb_api/other.md#mlb_awards) | GET /api/v1/awards — list award IDs (call with no params to enumerate). |
| [mlb_award_recipients](mlb_api/other.md#mlb_award_recipients) | GET /api/v1/awards/{awardId}/recipients — historical winners of one award. |
| [mlb_umpires](mlb_api/other-2.md#mlb_umpires) | GET /api/v1/jobs/umpires — current umpire crew assignments. |
| [mlb_conferences](mlb_api/other-2.md#mlb_conferences) | View all PCL conferences. |
| [mlb_conference](mlb_api/other-2.md#mlb_conference) | View PCL conferences by conferenceId. |
| [mlb_analytics_games](mlb_api/other-2.md#mlb_analytics_games) | View timestamps of most recent data corrections made to games. |
| [mlb_analytics_guids](mlb_api/other-2.md#mlb_analytics_guids) | View timestamps of most recent data corrections made to GUIDs. |
| [mlb_high_low](mlb_api/other-2.md#mlb_high_low) | View high/low stats by player or team. |
| [mlb_free_agents](mlb_api/other-2.md#mlb_free_agents) | View biographical information and stats for Free Agents. |
| [mlb_jobs](mlb_api/other-2.md#mlb_jobs) | View directory by jobType. |
| [mlb_datacasters](mlb_api/other-2.md#mlb_datacasters) | View datacasters directory. |
| [mlb_official_scorers](mlb_api/other-2.md#mlb_official_scorers) | View official scorer directory. |
| [mlb_umpire_games](mlb_api/other-2.md#mlb_umpire_games) | Get umpires and associated game for umpireId. |
| [mlb_seasons_all](mlb_api/other-2.md#mlb_seasons_all) | View information for all seasons based on id. |
| [mlb_sport](mlb_api/other-2.md#mlb_sport) | View information for any given sportId. |
| [mlb_stats_metrics](mlb_api/other-2.md#mlb_stats_metrics) | View Statcast stats. |
