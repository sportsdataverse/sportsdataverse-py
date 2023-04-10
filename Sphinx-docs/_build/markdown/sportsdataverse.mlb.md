# sportsdataverse.mlb package

## Submodules

## sportsdataverse.mlb.mlb_loaders module


### sportsdataverse.mlb.mlb_loaders.mlbam_copyright_info(saveFile=False, returnFile=False)
Displays the copyright info for the MLBAM API.

Args:

    saveFile (boolean) = False
    If saveFile is set to True, the copyright file generated is saved.

    returnFile (boolean) = False
    If returnFile is set to True, the copyright file is returned.

## sportsdataverse.mlb.mlbam_games module


### sportsdataverse.mlb.mlbam_games.mlbam_schedule(season: int, gameType='R')
Retrieves the start and end date for games for every league, and the MLB,for a given season.
This function does not get individual games.

Args:

    season (int):

        Required parameter. Indicates the season you are trying to find the games for.

    gameType (string) = “R”:

        Optional parameter. If there’s no input, this function will get the info for the regular season.

        Other parts of the season are indicated as follows in the MLBAM API:

        ‘S’ - Spring Training
        ‘E’ - Exhibition
        ‘A’ - All Star Game
        ‘D’ - Division Series
        ‘F’ - First Round (Wild Card)
        ‘L’ - League Championship
        ‘W’ - World Series

Returns:

    A pandas dataframe containing MLB scheduled games.

## sportsdataverse.mlb.mlbam_players module


### sportsdataverse.mlb.mlbam_players.mlbam_player_info(playerID: int)
Retrieves the player info for an MLB player, given a proper MLBAM ID

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

Returns:

    A pandas dataframe cointaining player information for the specified MLBAM player ID.


### sportsdataverse.mlb.mlbam_players.mlbam_player_teams(playerID: int, season: int)
Retrieves the info regarding which teams that player played for in a given season, or in the player’s career.

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

    season (int):

        Required parameter. If provided, the search will only look for teams
        that player played for in that season.

Returns:

    A pandas dataframe containing teams a player played for in that season.


### sportsdataverse.mlb.mlbam_players.mlbam_search_mlb_players(search: str, isActive='')
Searches for an MLB player in the MLBAM API.

Args:

    search (string):

        Inputted string of the player(s) the user is intending to search.
        If there is nothing inputted, nothing will be searched.

    isActive (string, optional):

        If called, it will specify if you want active players, or inactive players
        in your search.

        If you want active players, set isActive to “Y” or “Yes”.

        If you want inactive players, set isActive to “N” or “No”.

Returns:

    A pandas dataframe containing MLBAM players whose name(s) matches the input string.

## sportsdataverse.mlb.mlbam_reports module


### sportsdataverse.mlb.mlbam_reports.mlbam_broadcast_info(season: int, home_away='e')
Retrieves the broadcasters (radio and TV) involved with certain games.

Args:

    season (int):

        Required parameter. If no season is provided, the function wil not work.

    home_away (string):

        Optional parameter. Used to get broadcasters from either the home OR the away side.
        Leave blank if you want both home and away broadcasters.

        If you want home broadcasters only, set home_away=’H’ or home_away=’a’.

        If you want away broadcasters only, set home_away=’A’ or home_away=’a’.

        If you want both home and away broadcasters, set home_away=’E’ or home_away=’e’.

Returns:

    A pandas dataframe containing TV and radio broadcast information for various MLB games.


### sportsdataverse.mlb.mlbam_reports.mlbam_transactions(startDate: str, endDate: str)
Retrieves all transactions in a given range of dates.
You MUST provide two dates for this function to work, and both dates must be in MM/DD/YYYY format.
For example, December 31st, 2021 would be represented as 12/31/2021.

Args:

    startDate (int):

        Required parameter. If no startDate is provided, the function wil not work.
        Additionally, startDate must be in MM/DD/YYYY format.

    endDate (int):

        Required parameter. If no endDate is provided, the function wil not work.
        Additionally, endDate must be in MM/DD/YYYY format.

Returns:

    A pandas dataframe containing MLB transactions between two dates.

## sportsdataverse.mlb.mlbam_stats module


### sportsdataverse.mlb.mlbam_stats.mlbam_player_career_hitting_stats(playerID: int, gameType='R')
Retrieves the career hitting stats for an MLB player, given a proper MLBAM ID

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

    gameType (string) = “R”:

        Optional parameter. If there’s no input, this function will get the info for the regular season.

        Other parts of the season are indicated as follows in the MLBAM API:

        ‘S’ - Spring Training
        ‘E’ - Exhibition
        ‘A’ - All Star Game
        ‘D’ - Division Series
        ‘F’ - First Round (Wild Card)
        ‘L’ - League Championship
        ‘W’ - World Series

Returns:

    A pandas dataframe containing hitting stats for an MLB player in a given season.


### sportsdataverse.mlb.mlbam_stats.mlbam_player_career_pitching_stats(playerID: int, gameType='R')
Retrieves the career pitching stats for an MLB player, given a proper MLBAM ID

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

    gameType (string) = “R”:

        Optional parameter. If there’s no input, this function will get the info for the regular season.

        Other parts of the season are indicated as follows in the MLBAM API:

        ‘S’ - Spring Training
        ‘E’ - Exhibition
        ‘A’ - All Star Game
        ‘D’ - Division Series
        ‘F’ - First Round (Wild Card)
        ‘L’ - League Championship
        ‘W’ - World Series

Returns:

    A pandas dataframe containing career pitching stats for an MLB player.


### sportsdataverse.mlb.mlbam_stats.mlbam_player_season_hitting_stats(playerID: int, season: int, gameType='R')
Retrieves the hitting stats for an MLB player in a given season, given a proper MLBAM ID.

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

    season (int):

        Required parameter. Indicates the season you are trying to find the games for.

    gameType (string) = “R”:

        Optional parameter. If there’s no input, this function will get the info for the regular season.

        Other parts of the season are indicated as follows in the MLBAM API:

        ‘S’ - Spring Training
        ‘E’ - Exhibition
        ‘A’ - All Star Game
        ‘D’ - Division Series
        ‘F’ - First Round (Wild Card)
        ‘L’ - League Championship
        ‘W’ - World Series

Returns:

    A pandas dataframe containing career hitting stats for an MLB player.


### sportsdataverse.mlb.mlbam_stats.mlbam_player_season_pitching_stats(playerID: int, season: int, gameType='R')
Retrieves the pitching stats for an MLB player in a given season, given a proper MLBAM ID

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

    season (int):

        Required parameter. Indicates the season you are trying to find the games for.

    gameType (string) = “R”:

        Optional parameter. If there’s no input, this function will get the info for the regular season.

        Other parts of the season are indicated as follows in the MLBAM API:

        ‘S’ - Spring Training
        ‘E’ - Exhibition
        ‘A’ - All Star Game
        ‘D’ - Division Series
        ‘F’ - First Round (Wild Card)
        ‘L’ - League Championship
        ‘W’ - World Series

Returns:

    A pandas dataframe containing pitching stats for an MLB player in a given season.

## sportsdataverse.mlb.mlbam_teams module


### sportsdataverse.mlb.mlbam_teams.mlbam_40_man_roster(teamID: int)
Retrieves the current 40-man roster for a team, given a proper MLBAM team ID.

Args:

teamID (int):
Required parameter. This should be the MLBAM team ID for the MLB team you want a 40-man roster from.

Returns:

    A pandas dataframe containing the current 40-man roster for the given MLBAM team ID.


### sportsdataverse.mlb.mlbam_teams.mlbam_team_roster(teamID: int, startSeason: int, endSeason: int)
Retrieves the cumulative roster for a MLB team in a specified timeframe.

Args:

    teamID (int):

        Required parameter. This should be the number MLBAM associates for an MLB team.
        For example, the Cincinnati Reds have an MLBAM team ID of 113.

    startSeason (int):

        Required parameter. This value must be less than endSeason for this function to work.

    endSeason (int):

        Required parameter. This value must be greater than startSeason for this function to work.

Returns:

    A pandas dataframe containg the roster(s) for the MLB team.


### sportsdataverse.mlb.mlbam_teams.mlbam_teams(season: int, retriveAllStarRosters=False)
Retrieves the player info for an MLB team, given an MLB season.

Args:

    season (int):

        Required parameter. If no season is provided, the function wil not work.

    retriveAllStarRosters (boolean):

        Optional parameter. If set to ‘True’, MLB All-Star rosters will be returned when
        running this function.

Returns:

    A pandas dataframe containing information about MLB teams that played in that season.

## sportsdataverse.mlb.retrosheet module

RETROSHEET NOTICE:

> The information used here was obtained free of
> charge from and is copyrighted by Retrosheet.  Interested
> parties may contact Retrosheet at “www.retrosheet.org”.


### sportsdataverse.mlb.retrosheet.retrosheet_ballparks()
Retrives the current TEAMABR.txt file from the Retrosheet website, and then returns the current file as a pandas dataframe.

Args:

    None

Returns:

    A pandas Dataframe with the biographical information of notable major leauge teams.


### sportsdataverse.mlb.retrosheet.retrosheet_ejections()
Retrives the current Ejecdata.txt file from the  Retrosheet website, and then returns the current file as a pandas dataframe.

Args:

    None

Returns:

    A pandas Dataframe with the biographical information of known MLB ejections.


### sportsdataverse.mlb.retrosheet.retrosheet_franchises()
Retrives the current TEAMABR.txt file from the  Retrosheet website, and then returns the current file as  a pandas dataframe.

Args:

    None

Returns:

    A pandas Dataframe with the biographical information of notable major leauge teams.


### sportsdataverse.mlb.retrosheet.retrosheet_game_logs_team()
Retrives the team-level stats for MLB games in a season, or range of seasons.
THIS DOES NOT GET PLAYER STATS!
Use retrosplits_game_logs_player() for player-level game stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

    game_type (str):

        Optional parameter. By default, this is set to “regular”, or to put it in another way, this function call will return only regular season games.

        The full list of supported keywards for game_type are as follows. Case does not matter (you can set game_type to “rEgUlAr”, and the function call will still work):

        >
        > * “regular”: Regular season games.


        > * “asg”: All-Star games.


        > * “playoffs”: Playoff games.

    filter_out_seasons (bool):

        If game_type is set to either “asg” or “playoffs”, and filter_out_seasons is set to true, this function will filter out seasons that do not match the inputted season and/or the range of seasons. By default, this is set to True.

Returns:

    A pandas dataframe containing team-level stats for MLB games.


### sportsdataverse.mlb.retrosheet.retrosheet_people()
Retrives the current BioFile.txt file from the Retrosheet website, and then returns the current file as a pandas dataframe.

Args:

    None

Returns:

    A pandas Dataframe with the biographical information of various individuals who have played baseball.


### sportsdataverse.mlb.retrosheet.retrosheet_schedule()
Retrives the scheduled games of an MLB season, or MLB seasons.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

    original_2020_schedule (bool):

        Retrosheet keeps a record of the orignial 2020 MLB season, before the season was delayed due to the COVID-19 pandemic.


        * If this is set to True, this function will return the original 2020 MLB season, before it was altered due to the COVID-19 pandemic, if the user wants this function to return the schedule for the 2020 MLB season.


        * If this is set to False, this function will return the altered 2020 MLB season, after it was altered due to the COVID-19 pandemic, if the user wants this function to return the schedule for the 2020 MLB season.

Returns:

    A pandas dataframe containg historical MLB schedules.

## sportsdataverse.mlb.retrosplits module

RETROSHEET NOTICE:

> The information used here was obtained free of
> charge from and is copyrighted by Retrosheet.  Interested
> parties may contact Retrosheet at “www.retrosheet.org”.


### sportsdataverse.mlb.retrosplits.retrosplits_game_logs_player()
Retrives game-level player stats from the Retrosplits project.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing game-level player stats from historical MLB games.


### sportsdataverse.mlb.retrosplits.retrosplits_game_logs_team()
Retrives game-level team stats from the Retrosplits project.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing game-level team stats from historical MLB games.


### sportsdataverse.mlb.retrosplits.retrosplits_player_batting_by_platoon()
Retrives player-level, batting by platoon (left/right hitting vs. left/right pitching) split stats from the Retrosplits project.
The stats are batting stats, based off of the handedness of the batter vs the handedness of the pitcher.
The stats returned by this function are season-level stats, not game-level stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing player-level, batting by platoon stats for batters.


### sportsdataverse.mlb.retrosplits.retrosplits_player_batting_by_position()
Retrives player-level, batting by position split stats from the Retrosplits project.
The stats returned by this function are season-level stats, not game-level stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing batting by position split stats for MLB players.


### sportsdataverse.mlb.retrosplits.retrosplits_player_batting_by_runners()
Retrives player-level, batting by runners split stats from the Retrosplits project.
The stats are batting stats, based off of how many runners are on base at the time of the at bat.
The stats returned by this function are season-level stats, not game-level stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing player-level, batting by runners split stats.


### sportsdataverse.mlb.retrosplits.retrosplits_player_head_to_head_stats()
Retrives batter vs. pitcher stats from the Retrosplits project.
The stats are batting stats, based off of the preformance of that specific batter agianst a specific pitcher for the durration of that specific season.
The stats returned by this function are season-level stats, not game-level stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing batter vs. pitcher stats for a season, or for a range of seasons.


### sportsdataverse.mlb.retrosplits.retrosplits_player_pitching_by_platoon()
Retrives player-level, pitching by platoon (left/right pitching vs. left/right hitting) split stats from the Retrosplits project.
The stats are pitching stats, based off of the handedness of the pitcher vs the handedness of the batter.
The stats returned by this function are season-level stats, not game-level stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing player-level, pitching by platoon stats for pitchers.


### sportsdataverse.mlb.retrosplits.retrosplits_player_pitching_by_runners()
Retrives player-level, pitching by runners split stats from the Retrosplits project.
The stats are pitching stats, based off of how many runners are on base at the time of the at bat.
The stats returned by this function are season-level stats, not game-level stats.

Args:

    first_season (int):

        Required parameter. Indicates the season you are trying to find the games for, or the first season you are trying to find games for, if you want games from a range of seasons.

    last_season (int):

        Optional parameter. If you want to get games from a range of seasons, set this variable to the last season you want games from.

Returns:

    A pandas dataframe containing pitching by runners split stats for a season, or for a range of seasons.

## Module contents
