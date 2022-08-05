import sportsdataverse as sdv

"""
mlbam_copyright_info(saveFile=False,returnFile=False):

Displays the copyright info for the MLBAM API.

Args:
saveFile (boolean) = False
    If saveFile is set to True, the copyright file generated is saved.

returnFile (boolean) = False
    If returnFile is set to True, the copyright file is returned.

Example:

import sportsdataverse as sdv
MLBAM_copyright_info = sdv.mlb.mlbam_copyright_info()
print(MLBAM_copyright_info)
"""
print('mlbam_copyright_info')
print(sdv.mlb.mlbam_copyright_info())
print('')
"""
mlbam_schedule(season:int,gameType="R"):

Retrieves the start and end date for games for every league, and the MLB,
for a given season.

This function does not get individual games.

Args:

season (int):
    Required parameter. Indicates the season you are trying to find the games for.

gameType (string) = "R":
    Optional parameter. If there's no input, this function will get the info for the regular season.

    Other parts of the season are indicated as follows in the MLBAM API:

    'S' - Spring Training
    'E' - Exhibition
    'A' - All Star Game
    'D' - Division Series
    'F' - First Round (Wild Card)
    'L' - League Championship
    'W' - World Series

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_schedule(2020)
print(df)
"""
print('mlbam_schedule')
print(sdv.mlb.mlbam_schedule(2017))
print('')
"""
mlbam_search_mlb_players(search:str,isActive=""):

Searches for an MLB player in the MLBAM API.

Args:
search (string):
    Inputted string of the player(s) the user is intending to search.
    If there is nothing inputted, nothing will be searched.

isActive (string, optional):
    If called, it will specify if you want active players, or innactive players
    in your search.

    If you want active players, set isActive to "Y" or "Yes".

    If you want inactive players, set isActive to "N" or "No".

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_search_mlb_players(search="Votto",isActive="y")
print(df)
"""
print('mlbam_search_mlb_players')
print(sdv.mlb.mlbam_search_mlb_players(search="Votto",isActive="y"))
print(sdv.mlb.mlbam_search_mlb_players(search="Joe",isActive="y"))
print('')
"""
mlbam_player_info(playerID:int):

Retrieves the player info for an MLB player, given a proper MLBAM ID

Args:

playerID (int):
    Required parameter. If no playerID is provided, the function wil not work.

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_player_info(playerID=458015)
print(df)
"""
print('getPlayerInfo')
print(sdv.mlb.mlbam_player_info(playerID=458015))
print('')
"""
def mlbam_player_teams(playerID:int,season:int):

Retrieves the info regarding which teams that player played for in a given
season, or in the player's career

Args:

playerID (int):
    Required parameter. If no playerID is provided, the function wil not work.

season (int):
    Optional parameter. If provided, the search will only look for teams
    that player played for in that season.

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_player_teams(playerID=523260,season=2014)
print(df)
"""
print('getPlayerTeams')
print(sdv.mlb.mlbam_player_teams(playerID=523260,season=2014))
print('')
"""
def mlbam_player_season_hitting_stats(playerID:int,season:int,gameType="R"):

Retrieves the hitting stats for an MLB player in a given season, given a proper MLBAM ID

Args:

playerID (int):
    Required parameter. If no playerID is provided, the function wil not work.

season (int):
    Required parameter. Indicates the season you are trying to find the games for.

gameType (string) = "R":
    Optional parameter. If there's no input, this function will get the info for the regular season.

    Other parts of the season are indicated as follows in the MLBAM API:

    'S' - Spring Training
    'E' - Exhibition
    'A' - All Star Game
    'D' - Division Series
    'F' - First Round (Wild Card)
    'L' - League Championship
    'W' - World Series

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_player_season_hitting_stats(playerID=458015,season=2021,gameType="R")
print(df)
"""
print('mlbam_player_season_hitting_stats')
print(sdv.mlb.mlbam_player_season_hitting_stats(playerID=458015,season=2021,gameType="R"))
print('')
"""
def mlbam_player_season_pitching_stats(playerID:int,season:int,gameType="R"):

Retrieves the pitching stats for an MLB player in a given season, given a proper MLBAM ID

Args:

playerID (int):
    Required parameter. If no playerID is provided, the function wil not work.

season (int):
    Required parameter. Indicates the season you are trying to find the games for.

gameType (string) = "R":
    Optional parameter. If there's no input, this function will get the info for the regular season.

    Other parts of the season are indicated as follows in the MLBAM API:

    'S' - Spring Training
    'E' - Exhibition
    'A' - All Star Game
    'D' - Division Series
    'F' - First Round (Wild Card)
    'L' - League Championship
    'W' - World Series

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_player_season_pitching_stats(playerID=642840,season=2019,gameType="R")
print(df)
"""
print('mlbam_player_season_pitching_stats')
print(sdv.mlb.mlbam_player_season_pitching_stats(playerID=642840,season=2019,gameType="R"))
print('')
"""
def mlbam_player_career_hitting_stats(playerID:int,gameType="R"):

Retrieves the career hitting stats for an MLB player, given a proper MLBAM ID

Args:

playerID (int):
    Required parameter. If no playerID is provided, the function wil not work.

season (int):
    Required parameter. Indicates the season you are trying to find the games for.

gameType (string) = "R":
    Optional parameter. If there's no input, this function will get the info for the regular season.

    Other parts of the season are indicated as follows in the MLBAM API:

    'S' - Spring Training
    'E' - Exhibition
    'A' - All Star Game
    'D' - Division Series
    'F' - First Round (Wild Card)
    'L' - League Championship
    'W' - World Series

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_player_career_hitting_stats(playerID=458015,gameType="R")
print(df)
"""
print('mlbam_player_career_hitting_stats')
print(sdv.mlb.mlbam_player_career_hitting_stats(playerID=458015,gameType="R"))
print('')
"""
mlbam_player_career_pitching_stats(playerID:int,gameType="R"):

Retrieves the career pitching stats for an MLB player, given a proper MLBAM ID

Args:

playerID (int):
    Required parameter. If no playerID is provided, the function wil not work.

gameType (string) = "R":
    Optional parameter. If there's no input, this function will get the info for the regular season.

    Other parts of the season are indicated as follows in the MLBAM API:

    'S' - Spring Training
    'E' - Exhibition
    'A' - All Star Game
    'D' - Division Series
    'F' - First Round (Wild Card)
    'L' - League Championship
    'W' - World Series

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_player_career_pitching_stats(playerID=642840,gameType="R")
print(df)
"""
print('mlbam_player_career_pitching_stats')
print(sdv.mlb.mlbam_player_career_pitching_stats(playerID=642840,gameType="R"))
print('')
"""
mlbam_transactions(startDate=0,endDate=0):

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

Example:

import sportsdataverse as sdv
df =sdv.mlb.mlbam_transactions(startDate=20200901,endDate=20200914)
print(df)
"""
print('mlbam_transactions')
print(sdv.mlb.mlbam_transactions(startDate="09/01/2020",endDate="09/01/2020"))
print('')
"""
mlbam_broadcast_info(season:int,home_away="e"):

Retrieves the broadcasters (radio and TV) involved with certian games.

Args:

season (int):
    Required parameter. If no season is provided, the function wil not work.

home_away (string):
    Optional parameter. Used to get broadcasters from either the home OR the away side.
    Leave blank if you want both home and away broadcasters.

    If you want home broadcasters only, set home_away='H' or home_away='a'.

    If you want away broadcasters only, set home_away='A' or home_away='a'.

    If you want both home and away broadcasters, set home_away='E' or home_away='e'.

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_broadcast_info(season=2020,home_away="e")
print(df)
"""
print('mlbam_broadcast_info')
print(sdv.mlb.mlbam_broadcast_info(season=2020,home_away="e"))
print('')
"""
mlbam_teams(season:int,retriveAllStarRosters=False):

Retrieves the player info for an MLB player, given a proper MLBAM ID

Args:

season (int):
    Required parameter. If no season is provided, the function wil not work.

retriveAllStarRosters (boolean):
    Optional parameter. If set to 'True', MLB All-Star rosters will be returned when
    running this function.

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_teams(season=2020)
print(df)
"""
print('mlbam_teams')
print(sdv.mlb.mlbam_teams(season=2020))
print('')
"""
mlbam_40_man_roster(teamID=113):

Retrieves the current 40-man roster for a team, given a proper MLBAM Team ID

Args:

teamID (int):
    Required parameter. If no MLBAM Team ID is provided, the current 40-man roster for the Cincinnati Reds will be returned.

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_40_man_roster(teamID=113)
print(df)
"""
print('mlbam_40_man_roster')
print(sdv.mlb.mlbam_40_man_roster(teamID=113))
print('')
"""
mlbam_team_roster(teamID=113,startSeason=2020,endSeason=2021):

Retrieves the cumulative roster for a MLB team in a specified timeframe.

Args:

teamID (int):
    Required parameter. If no MLBAM Team ID is provided, the cumulative roster for the Cincinnati Reds will be returned.

startSeason (int):
    Required parameter. This value must be less than endSeason for this function to work.

endSeason (int):
    Required parameter. This value must be greater than startSeason for this function to work.

Example:

import sportsdataverse as sdv
df = sdv.mlb.mlbam_team_roster(teamID=113,startSeason=2020,endSeason=2021)
print(df)
"""
print('mlbam_team_roster')
df = sdv.mlb.mlbam_team_roster(teamID=113,startSeason=2020,endSeason=2021)
print(df)
print('')