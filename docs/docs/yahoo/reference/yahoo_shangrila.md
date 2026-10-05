---
title: YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com)
sidebar_label: Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com)
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com)

`sportsdataverse.yahoo` — 107 endpoints.

## Editorial

| Function | Summary |
|---|---|
| [yahoo_editorial_boxscore](yahoo_shangrila/editorial.md#yahoo_editorial_boxscore) | Full game box score + play-by-play (normalized stat dictionaries) |
| [yahoo_editorial_scoreboard](yahoo_shangrila/editorial-2.md#yahoo_editorial_scoreboard) | Scoreboard: games + teams + leagues + odds (fat payload) |

## Game

| Function | Summary |
|---|---|
| [yahoo_game_prop_bets](yahoo_shangrila/game.md#yahoo_game_prop_bets) | Yahoo shangrila persisted query `gamePropBets` -> one row per `games` entry |
| [yahoo_game_stats_leaders](yahoo_shangrila/game.md#yahoo_game_stats_leaders) | Yahoo shangrila persisted query `gameStatsLeaders` -> one row per `games` entry |

## League

| Function | Summary |
|---|---|
| [yahoo_league_conferences](yahoo_shangrila/league.md#yahoo_league_conferences) | Yahoo shangrila persisted query `leagueConferences` -> one row per `leagues` entry |
| [yahoo_league_filters_data](yahoo_shangrila/league.md#yahoo_league_filters_data) | Yahoo shangrila persisted query `leagueFiltersData` -> one row per `leagues` entry |
| [yahoo_league_future_odds](yahoo_shangrila/league.md#yahoo_league_future_odds) | Yahoo shangrila persisted query `leagueFutureOdds` -> one row per `leagues` entry |
| [yahoo_league_game_ids](yahoo_shangrila/league.md#yahoo_league_game_ids) | Yahoo shangrila persisted query `leagueGameIds` -> one row per `leagues` entry |
| [yahoo_league_game_ids_by_date](yahoo_shangrila/league.md#yahoo_league_game_ids_by_date) | Yahoo shangrila persisted query `leagueGameIdsByDate` -> one row per `leagues` entry |
| [yahoo_league_games_by_round](yahoo_shangrila/league.md#yahoo_league_games_by_round) | Yahoo shangrila persisted query `leagueGamesByRound` -> one row per `leagues` entry |
| [yahoo_league_info](yahoo_shangrila/league.md#yahoo_league_info) | Yahoo shangrila persisted query `leagueInfo` -> one row per `leagues` entry |
| [yahoo_league_injuries](yahoo_shangrila/league.md#yahoo_league_injuries) | Yahoo shangrila persisted query `leagueInjuries` -> one row per `leagues.teams` entry |
| [yahoo_league_names](yahoo_shangrila/league.md#yahoo_league_names) | Yahoo shangrila persisted query `leagueNames` -> one row per `leagues` entry |
| [yahoo_league_prop_odds](yahoo_shangrila/league.md#yahoo_league_prop_odds) | Yahoo shangrila persisted query `leaguePropOdds` -> one row per `leagues` entry |
| [yahoo_league_standings](yahoo_shangrila/league.md#yahoo_league_standings) | Yahoo shangrila persisted query `leagueStandings` -> one row per `leagues` entry |
| [yahoo_league_stats_by_team](yahoo_shangrila/league.md#yahoo_league_stats_by_team) | Yahoo shangrila persisted query `leagueStatsByTeam` -> one row per `leagues` entry |
| [yahoo_league_stats_individual](yahoo_shangrila/league.md#yahoo_league_stats_individual) | Yahoo shangrila persisted query `leagueStatsIndividual` -> one row per `leagues` entry |
| [yahoo_league_stats_overview](yahoo_shangrila/league.md#yahoo_league_stats_overview) | Yahoo shangrila persisted query `leagueStatsOverview` (response body not captured; shape unknown) |
| [yahoo_league_stats_weekly](yahoo_shangrila/league.md#yahoo_league_stats_weekly) | Yahoo shangrila persisted query `leagueStatsWeekly` -> one row per `leagues` entry |
| [yahoo_league_team_ids](yahoo_shangrila/league.md#yahoo_league_team_ids) | Yahoo shangrila persisted query `leagueTeamIds` -> one row per `leagues` entry |
| [yahoo_league_teams](yahoo_shangrila/league.md#yahoo_league_teams) | Yahoo shangrila persisted query `leagueTeams` -> one row per `leagues` entry |
| [yahoo_leagues_season_states](yahoo_shangrila/league.md#yahoo_leagues_season_states) | Yahoo shangrila persisted query `leaguesSeasonStates` -> one row per `leagues` entry |

## Playbook

| Function | Summary |
|---|---|
| [yahoo_playbook_boxscore](yahoo_shangrila/playbook.md#yahoo_playbook_boxscore) | Yahoo shangrila persisted query `playbookBoxscore` -> tables: football_positions, football_stat_types, games |
| [yahoo_playbook_boxscore_poll](yahoo_shangrila/playbook.md#yahoo_playbook_boxscore_poll) | Yahoo shangrila persisted query `playbookBoxscorePoll` -> tables: football_positions, football_stat_types, games |
| [yahoo_playbook_boxscore_social_share](yahoo_shangrila/playbook.md#yahoo_playbook_boxscore_social_share) | Yahoo shangrila persisted query `playbookBoxscoreSocialShare` -> one row per `games` entry |
| [yahoo_playbook_combat_match](yahoo_shangrila/playbook.md#yahoo_playbook_combat_match) | Yahoo shangrila persisted query `playbookCombatMatch` -> one row per `games` entry |
| [yahoo_playbook_game](yahoo_shangrila/playbook.md#yahoo_playbook_game) | Yahoo shangrila persisted query `playbookGame` -> one row per `games` entry |
| [yahoo_playbook_game_odds_poll](yahoo_shangrila/playbook.md#yahoo_playbook_game_odds_poll) | Yahoo shangrila persisted query `playbookGameOddsPoll` -> one row per `games` entry |
| [yahoo_playbook_golf_tournament](yahoo_shangrila/playbook.md#yahoo_playbook_golf_tournament) | Yahoo shangrila persisted query `playbookGolfTournament` (response body not captured; shape unknown) |
| [yahoo_playbook_league_odds](yahoo_shangrila/playbook.md#yahoo_playbook_league_odds) | Yahoo shangrila persisted query `playbookLeagueOdds` -> one row per `leagues` entry |
| [yahoo_playbook_player](yahoo_shangrila/playbook.md#yahoo_playbook_player) | Yahoo shangrila persisted query `playbookPlayer` -> one row per `players` entry |
| [yahoo_playbook_player_social_share](yahoo_shangrila/playbook.md#yahoo_playbook_player_social_share) | Yahoo shangrila persisted query `playbookPlayerSocialShare` -> one row per `players` entry |
| [yahoo_playbook_race](yahoo_shangrila/playbook.md#yahoo_playbook_race) | Yahoo shangrila persisted query `playbookRace` -> one row per `games` entry |
| [yahoo_playbook_team](yahoo_shangrila/playbook.md#yahoo_playbook_team) | Yahoo shangrila persisted query `playbookTeam` -> tables: teams, leagues |
| [yahoo_playbook_team_basic](yahoo_shangrila/playbook.md#yahoo_playbook_team_basic) | Yahoo shangrila persisted query `playbookTeamBasic` -> one row per `teams` entry |
| [yahoo_playbook_team_social_share](yahoo_shangrila/playbook.md#yahoo_playbook_team_social_share) | Yahoo shangrila persisted query `playbookTeamSocialShare` -> one row per `teams` entry |
| [yahoo_playbook_tennis_match](yahoo_shangrila/playbook-2.md#yahoo_playbook_tennis_match) | Yahoo shangrila persisted query `playbookTennisMatch` -> one row per `events` entry |

## Player

| Function | Summary |
|---|---|
| [yahoo_player_basic](yahoo_shangrila/player.md#yahoo_player_basic) | Yahoo shangrila persisted query `playerBasic` -> tables: players, leagues |
| [yahoo_player_career_stats](yahoo_shangrila/player.md#yahoo_player_career_stats) | Yahoo shangrila persisted query `playerCareerStats` -> one row per `players` entry |
| [yahoo_player_game_log](yahoo_shangrila/player.md#yahoo_player_game_log) | Yahoo shangrila persisted query `playerGameLog` -> one row per `players` entry |
| [yahoo_player_props](yahoo_shangrila/player.md#yahoo_player_props) | Yahoo shangrila persisted query `playerProps` -> one row per `players` entry |
| [yahoo_player_search](yahoo_shangrila/player.md#yahoo_player_search) | Yahoo shangrila persisted query `playerSearch` -> one row per `leagues.players` entry |
| [yahoo_player_season_stats](yahoo_shangrila/player.md#yahoo_player_season_stats) | Yahoo shangrila persisted query `playerSeasonStats` -> one row per `players` entry |

## Season

| Function | Summary |
|---|---|
| [yahoo_season_stats_football_defense_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_defense_ncaaf) | Legacy player season Defense leaders (NCAAF) |
| [yahoo_season_stats_football_kicking_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_kicking_ncaaf) | Legacy player season Kicking leaders (NCAAF) |
| [yahoo_season_stats_football_passing_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_passing_ncaaf) | Legacy player season Passing leaders (NCAAF) |
| [yahoo_season_stats_football_punting_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_punting_ncaaf) | Legacy player season Punting leaders (NCAAF) |
| [yahoo_season_stats_football_receiving_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_receiving_ncaaf) | Legacy player season Receiving leaders (NCAAF) |
| [yahoo_season_stats_football_returns_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_returns_ncaaf) | Legacy player season Returns leaders (NCAAF) |
| [yahoo_season_stats_football_rushing_ncaaf](yahoo_shangrila/season.md#yahoo_season_stats_football_rushing_ncaaf) | Legacy player season Rushing leaders (NCAAF) |
| [yahoo_season_team_stats_football_defense](yahoo_shangrila/season.md#yahoo_season_team_stats_football_defense) | Legacy team season Defense (NCAAF) |
| [yahoo_season_team_stats_football_kicking](yahoo_shangrila/season.md#yahoo_season_team_stats_football_kicking) | Legacy team season Kicking (NCAAF) |
| [yahoo_season_team_stats_football_kickoffs](yahoo_shangrila/season.md#yahoo_season_team_stats_football_kickoffs) | Legacy team season Kickoffs (NCAAF) |
| [yahoo_season_team_stats_football_offense](yahoo_shangrila/season.md#yahoo_season_team_stats_football_offense) | Legacy team season Offense (NCAAF) |
| [yahoo_season_team_stats_football_passing](yahoo_shangrila/season.md#yahoo_season_team_stats_football_passing) | Legacy team season Passing (NCAAF) |
| [yahoo_season_team_stats_football_passing_defense](yahoo_shangrila/season.md#yahoo_season_team_stats_football_passing_defense) | Legacy team Passing defense allowed (NCAAF) |
| [yahoo_season_team_stats_football_punting](yahoo_shangrila/season.md#yahoo_season_team_stats_football_punting) | Legacy team season Punting (NCAAF) |
| [yahoo_season_team_stats_football_receiving](yahoo_shangrila/season.md#yahoo_season_team_stats_football_receiving) | Legacy team season Receiving (NCAAF) |
| [yahoo_season_team_stats_football_receiving_defense](yahoo_shangrila/season.md#yahoo_season_team_stats_football_receiving_defense) | Legacy team Receiving defense allowed (NCAAF) |
| [yahoo_season_team_stats_football_returns](yahoo_shangrila/season.md#yahoo_season_team_stats_football_returns) | Legacy team season Returns (NCAAF) |
| [yahoo_season_team_stats_football_rushing](yahoo_shangrila/season.md#yahoo_season_team_stats_football_rushing) | Legacy team season Rushing (NCAAF) |
| [yahoo_season_team_stats_football_rushing_defense](yahoo_shangrila/season.md#yahoo_season_team_stats_football_rushing_defense) | Legacy team Rushing defense allowed (NCAAF) |

## Team

| Function | Summary |
|---|---|
| [yahoo_team_injuries](yahoo_shangrila/team.md#yahoo_team_injuries) | Yahoo shangrila persisted query `teamInjuries` -> one row per `teams` entry |
| [yahoo_team_playoff_series](yahoo_shangrila/team.md#yahoo_team_playoff_series) | Yahoo shangrila persisted query `teamPlayoffSeries` -> one row per `teams.playoffSeries` entry |
| [yahoo_team_roster](yahoo_shangrila/team.md#yahoo_team_roster) | Yahoo shangrila persisted query `teamRoster` -> one row per `teams` entry |
| [yahoo_team_schedule_by_season](yahoo_shangrila/team.md#yahoo_team_schedule_by_season) | Yahoo shangrila persisted query `teamScheduleBySeason` -> one row per `teams` entry |
| [yahoo_team_search](yahoo_shangrila/team.md#yahoo_team_search) | Yahoo shangrila persisted query `teamSearch` -> one row per `teams` entry |
| [yahoo_team_stats_leaders_v2](yahoo_shangrila/team.md#yahoo_team_stats_leaders_v2) | Yahoo shangrila persisted query `teamStatsLeadersV2` -> tables: leagues, teams |
| [yahoo_team_transactions](yahoo_shangrila/team.md#yahoo_team_transactions) | Yahoo shangrila persisted query `teamTransactions` -> one row per `teams` entry |
| [yahoo_teams_basic](yahoo_shangrila/team.md#yahoo_teams_basic) | Yahoo shangrila persisted query `teamsBasic` -> one row per `teams` entry |

## Other

| Function | Summary |
|---|---|
| [yahoo_oly_medal_count](yahoo_shangrila/other.md#yahoo_oly_medal_count) | Yahoo shangrila persisted query `OlyMedalCount` -> one row per `olympics` entry |
| [yahoo_oly_seasons](yahoo_shangrila/other.md#yahoo_oly_seasons) | Yahoo shangrila persisted query `OlySeasons` -> one row per `olympics` entry |
| [yahoo_alias](yahoo_shangrila/other.md#yahoo_alias) | Yahoo shangrila persisted query `alias` -> one row per `pageMetaData` entry |
| [yahoo_article_list_card_players](yahoo_shangrila/other.md#yahoo_article_list_card_players) | Yahoo shangrila persisted query `articleListCardPlayers` -> one row per `players` entry |
| [yahoo_article_list_card_teams](yahoo_shangrila/other.md#yahoo_article_list_card_teams) | Yahoo shangrila persisted query `articleListCardTeams` -> one row per `teams` entry |
| [yahoo_basic_players](yahoo_shangrila/other.md#yahoo_basic_players) | Yahoo shangrila persisted query `basicPlayers` -> one row per `players` entry |
| [yahoo_betting_disclaimer](yahoo_shangrila/other.md#yahoo_betting_disclaimer) | Yahoo shangrila persisted query `bettingDisclaimer` -> one row per `bettingDisclaimers` entry |
| [yahoo_combat_event_fights](yahoo_shangrila/other.md#yahoo_combat_event_fights) | Yahoo shangrila persisted query `combatEventFights` -> one row per `leagues` entry |
| [yahoo_combat_schedule](yahoo_shangrila/other.md#yahoo_combat_schedule) | Yahoo shangrila persisted query `combatSchedule` -> one row per `leagues` entry |
| [yahoo_common_pills](yahoo_shangrila/other.md#yahoo_common_pills) | Yahoo shangrila persisted query `common/pills` (response body not captured; shape unknown) |
| [yahoo_consensus_rankings_php](yahoo_shangrila/other.md#yahoo_consensus_rankings_php) | Yahoo shangrila persisted query `consensus-rankings.php` (response body not captured; shape unknown) |
| [yahoo_draft](yahoo_shangrila/other.md#yahoo_draft) | Yahoo shangrila persisted query `draft` -> one row per `leagues` entry |
| [yahoo_draft_prospects](yahoo_shangrila/other.md#yahoo_draft_prospects) | Yahoo shangrila persisted query `draftProspects` -> one row per `leagues` entry |
| [yahoo_driver_results](yahoo_shangrila/other.md#yahoo_driver_results) | Yahoo shangrila persisted query `driverResults` -> one row per `players` entry |
| [yahoo_driver_splits](yahoo_shangrila/other.md#yahoo_driver_splits) | Yahoo shangrila persisted query `driverSplits` -> one row per `players` entry |
| [yahoo_featured_game_ids](yahoo_shangrila/other.md#yahoo_featured_game_ids) | Yahoo shangrila persisted query `featuredGameIds` -> one row per `featuredGames` entry |
| [yahoo_gametime_game](yahoo_shangrila/other.md#yahoo_gametime_game) | Yahoo shangrila persisted query `gametimeGame` -> one row per `games` entry |
| [yahoo_gametime_team](yahoo_shangrila/other.md#yahoo_gametime_team) | Yahoo shangrila persisted query `gametimeTeam` -> one row per `teams` entry |
| [yahoo_golf_tournament_seasons](yahoo_shangrila/other.md#yahoo_golf_tournament_seasons) | Yahoo shangrila persisted query `golfTournamentSeasons` (response body not captured; shape unknown) |
| [yahoo_golf_tournaments](yahoo_shangrila/other.md#yahoo_golf_tournaments) | Yahoo shangrila persisted query `golfTournaments` -> one row per `golfTournaments` entry |
| [yahoo_golf_tournaments_basic](yahoo_shangrila/other.md#yahoo_golf_tournaments_basic) | Yahoo shangrila persisted query `golfTournamentsBasic` -> one row per `golfTournaments` entry |
| [yahoo_module_game](yahoo_shangrila/other.md#yahoo_module_game) | Yahoo shangrila persisted query `moduleGame` -> one row per `games` entry |
| [yahoo_motorsport_standings](yahoo_shangrila/other.md#yahoo_motorsport_standings) | Yahoo shangrila persisted query `motorsportStandings` -> one row per `leagues` entry |
| [yahoo_nascar_drivers](yahoo_shangrila/other.md#yahoo_nascar_drivers) | Yahoo shangrila persisted query `nascarDrivers` -> one row per `leagues` entry |
| [yahoo_nav_dropdown_tray](yahoo_shangrila/other.md#yahoo_nav_dropdown_tray) | Yahoo shangrila persisted query `navDropdownTray` -> tables: nfl, nhl, nba, mlb, wnba, ncaab, ncaaf, ncaaw, sportsbook_legal_states |
| [yahoo_pick_distribution](yahoo_shangrila/other.md#yahoo_pick_distribution) | Yahoo shangrila persisted query `pickDistribution` -> one row per `leagues` entry |
| [yahoo_playoff_bracket](yahoo_shangrila/other.md#yahoo_playoff_bracket) | Yahoo shangrila persisted query `playoffBracket` -> one row per `leagues.bracketSlots` entry |
| [yahoo_playoff_series_game](yahoo_shangrila/other.md#yahoo_playoff_series_game) | Yahoo shangrila persisted query `playoffSeriesGame` -> one row per `games` entry |
| [yahoo_polymarket_game](yahoo_shangrila/other.md#yahoo_polymarket_game) | Yahoo shangrila persisted query `polymarketGame` -> one row per `games` entry |
| [yahoo_racing_schedule](yahoo_shangrila/other.md#yahoo_racing_schedule) | Yahoo shangrila persisted query `racingSchedule` -> one row per `leagues` entry |
| [yahoo_scoreboard_game](yahoo_shangrila/other.md#yahoo_scoreboard_game) | Yahoo shangrila persisted query `scoreboardGame` -> one row per `games` entry |
| [yahoo_tennis_matches_by_date](yahoo_shangrila/other.md#yahoo_tennis_matches_by_date) | Yahoo shangrila persisted query `tennisMatchesByDate` -> one row per `tennisTournaments` entry |
| [yahoo_tennis_tournament](yahoo_shangrila/other.md#yahoo_tennis_tournament) | Yahoo shangrila persisted query `tennisTournament` -> one row per `tennisTournaments` entry |
| [yahoo_tennis_tournaments](yahoo_shangrila/other.md#yahoo_tennis_tournaments) | Yahoo shangrila persisted query `tennisTournaments` -> one row per `tennisTournaments` entry |
| [yahoo_tennis_tournaments_by_date](yahoo_shangrila/other-2.md#yahoo_tennis_tournaments_by_date) | Yahoo shangrila persisted query `tennisTournamentsByDate` -> one row per `tennisTournaments` entry |
| [yahoo_trending_event_ids](yahoo_shangrila/other-2.md#yahoo_trending_event_ids) | Yahoo shangrila persisted query `trendingEventIds` -> one row per `trendingEvents` entry |
| [yahoo_trending_game_ids](yahoo_shangrila/other-2.md#yahoo_trending_game_ids) | Yahoo shangrila persisted query `trendingGameIds` -> one row per `trendingGames` entry |
