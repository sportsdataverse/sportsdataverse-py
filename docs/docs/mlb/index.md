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

### sportsdataverse.mlb.mlbam_games.load_mlbam_schedule(season: int, gameType='R')

Retrieves the start and end date for games for every league, and the MLB,
for a given season.

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

## sportsdataverse.mlb.mlbam_players module

### sportsdataverse.mlb.mlbam_players.load_mlbam_player_info(playerID: int)

Retrieves the player info for an MLB player, given a proper MLBAM ID

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

### sportsdataverse.mlb.mlbam_players.getPlayerTeams(playerID: int, season: int)

Retrieves the info regarding which teams that player played for in a given
season, or in the player’s career

Args:

    playerID (int):

        Required parameter. If no playerID is provided, the function wil not work.

    season (int):

        Required parameter. If provided, the search will only look for teams
        that player played for in that season.

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

## sportsdataverse.mlb.mlbam_reports module

### sportsdataverse.mlb.mlbam_reports.getBroadcastInfo(season: int, home_away='e')

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

### sportsdataverse.mlb.mlbam_reports.getTransactionsInRange(startDate: str, endDate: str)

Retrieves all transactions in a given range of dates.
You MUST provide two dates for this function to work, and both dates must
be in YYYYMMDD format. For example, December 31st, 2021 would be represented
as 20211231

Args:

    startDate (int):

        Required parameter. If no startDate is provided, the function wil not work.
        Additionally, startDate must be in YYYYMMDD format.

    endDate (int):

        Required parameter. If no endDate is provided, the function wil not work.
        Additionally, endDate must be in YYYYMMDD format.

## sportsdataverse.mlb.mlbam_stats module

### sportsdataverse.mlb.mlbam_stats.getCareerHittingStats(playerID: int, gameType='R')

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

### sportsdataverse.mlb.mlbam_stats.getCareerPitchingStats(playerID: int, gameType='R')

Retrieves the career pitching stats for an MLB player, given a proper MLBAM ID

Args:

> playerID (int):

>     Required parameter. If no playerID is provided, the function wil not work.

> gameType (string) = “R”:

>     Optional parameter. If there’s no input, this function will get the info for the regular season.

>     Other parts of the season are indicated as follows in the MLBAM API:

>     ‘S’ - Spring Training
>     ‘E’ - Exhibition
>     ‘A’ - All Star Game
>     ‘D’ - Division Series
>     ‘F’ - First Round (Wild Card)
>     ‘L’ - League Championship
>     ‘W’ - World Series

### sportsdataverse.mlb.mlbam_stats.getSeasonHittingStats(playerID: int, season: int, gameType='R')

Retrieves the hitting stats for an MLB player in a given season, given a proper MLBAM ID

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

### sportsdataverse.mlb.mlbam_stats.getSeasonPitchingStats(playerID: int, season: int, gameType='R')

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

## sportsdataverse.mlb.mlbam_teams module

### sportsdataverse.mlb.mlbam_teams.get40ManRoster(teamID: int)

Retrieves the player info for an MLB player, given a proper MLBAM ID

Args:

    teamID (int):

        Required parameter. This should be the number MLBAM associates for an MLB team.
        For example, the Cincinnati Reds have an MLBAM team ID of 113.

### sportsdataverse.mlb.mlbam_teams.getAllTimeRoster(teamID: int, startSeason: int, endSeason: int)

Retrieves the cumulative roster for a MLB team in a specified timeframe.

Args:

    teamID (int):

        Required parameter. This should be the number MLBAM associates for an MLB team.
        For example, the Cincinnati Reds have an MLBAM team ID of 113.

    startSeason (int):

        Required parameter. This value must be less than endSeason for this function to work.

    endSeason (int):

        Required parameter. This value must be greater than startSeason for this function to work.

### sportsdataverse.mlb.mlbam_teams.getTeamData(season: int, retriveAllStarRosters=False)

Retrieves the player info for an MLB team, given an MLB season

Args:

    season (int):

        Required parameter. If no season is provided, the function wil not work.

    retriveAllStarRosters (boolean):

        Optional parameter. If set to ‘True’, MLB All-Star rosters will be returned when
        running this function.

## Module contents
