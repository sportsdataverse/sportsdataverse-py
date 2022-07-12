# sportsdataverse.mlb package

## sportsdataverse.mlb.pullCopyrightInfo():

Displays the copyright info for the MLBAM API.

### Args:

saveFile (boolean) = False
If saveFile is set to True, the copyright file generated is saved.

### returnFile (boolean) = False

    If returnFile is set to True, the copyright file is returned.

### Example:

```
import sportsdataverse as sdv
MLBAM_copyright_info = sdv.mlb.pullCopyrightInfo()
print(MLBAM_copyright_info)
```

## getGamesInSeason(season:int,gameType="R"):

Retrives the start and end date for games for every leauge, and the MLB,
for a given season.

This function does not get individual games.

### Args:

season (int):
Required paramater. Indicates the season you are trying to find the games for.

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

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getGamesInSeason(2020)
print(df)
```

## searchMlbPlayers(search:str,isActive=""):

Searches for an MLB player in the MLBAM API.

### Args:

search (string):
Inputted string of the player(s) the user is intending to search.
If there is nothin inputted, nothing will be searched.

isActive (string, optional):
If called, it will specify if you want active players, or innactive players
in your search.

    If you want active players, set isActive to "Y" or "Yes".

    If you want inactive players, set isActive to "N" or "No".

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.searchMlbPlayers(search="Joe",isActive="y")
print(df)
```

## getPlayerInfo(playerID:int):

Retrives the player info for an MLB player, given a proper MLBAM ID

### Args:

playerID (int):
Required paramater. If no playerID is provided, the function wil not work.

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getPlayerInfo(playerID=458015)
print(df)
```

## getPlayerTeams(playerID:int,season:int):

Retrives the info regarding which teams that player played for in a given
season, or in the player's career

### Args:

playerID (int):
Required paramater. If no playerID is provided, the function wil not work.

season (int):
Required parameter. If provided, the search will only look for teams
that player played for in that season.

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getPlayerTeams(playerID=523260,season=2014)
print(df)
```

## def getSeasonHittingStats(playerID:int,season:int,gameType="R"):

Retrives the hitting stats for an MLB player in a given season, given a proper MLBAM ID

### Args:

playerID (int):
Required paramater. If no playerID is provided, the function wil not work.

season (int):
Required paramater. Indicates the season you are trying to find the games for.

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

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getSeasonHittingStats(playerID=458015,season=2021,gameType="R")
print(df)
```

## getSeasonPitchingStats(playerID=0,gameType="R",season=0):

Retrives the pitching stats for an MLB player in a given season, given a proper MLBAM ID

### Args:

playerID (int):
Required paramater. If no playerID is provided, the function wil not work.

season (int):
Required paramater. Indicates the season you are trying to find the games for.

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

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getSeasonPitchingStats(playerID=642840,season=2019,gameType="R")
print(df)
```

## getCareerHittingStats(playerID:int,gameType="R"):

Retrives the career hitting stats for an MLB player, given a proper MLBAM ID

### Args:

playerID (int):
Required paramater. If no playerID is provided, the function wil not work.

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

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getCareerHittingStats(playerID=458015,gameType="R")
print(df)
```

## getCareerPitchingStats(playerID:int,gameType="R"):

Retrives the career pitching stats for an MLB player, given a proper MLBAM ID

### Args:

playerID (int):
Required paramater. If no playerID is provided, the function wil not work.

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

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getCareerPitchingStats(playerID=642840,gameType="R")
print(df)
```

## getTransactionsInRange(startDate:str,endDate:str):

Retrives all transactions in a given range of dates.
You MUST provide two dates for this function to work, and both dates must
be in YYYYMMDD format. For example, December 31st, 2021 would be represented
as 20211231

### Args:

startDate (int):
Required paramater. If no startDate is provided, the function wil not work.
Additionally, startDate must be in YYYYMMDD format.

endDate (int):
Required paramater. If no endDate is provided, the function wil not work.
Additionally, endDate must be in YYYYMMDD format.

### Example:

```
import sportsdataverse as sdv
df =sdv.mlb.getTransactionsInRange(startDate="09/01/2020",endDate="09/14/2020")
print(df)
```

## getBroadcastInfo(season:int,home_away="e"):

Retrives the broadcasters (radio and TV) involved with certian games.

### Args:

season (int):
Required paramater. If no season is provided, the function wil not work.

home_away (string):
Optional paramater. Used to get broadcasters from either the home OR the away side.
Leave blank if you want both home and away broadcasters.

    If you want home broadcasters only, set home_away='H' or home_away='a'.

    If you want away broadcasters only, set home_away='A' or home_away='a'.

    If you want both home and away broadcasters, set home_away='E' or home_away='e'.

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getBroadcastInfo(season=2020,home_away="e")
print(df)
```

## getTeamData(season:int,retriveAllStarRosters=False):

Retrives the player info for an MLB player, given a proper MLBAM ID

### Args:

season (int):
Required paramater. If no season is provided, the function wil not work.

retriveAllStarRosters (boolean):
Optional parameter. If set to 'True', MLB All-Star rosters will be returned when
running this function.

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getTeamData(season=2020)
print(df)
```

## get40ManRoster(teamID:int):

Retrives the current 40-man roster for a team, given a proper MLBAM Team ID.

### Args:

teamID (int):
Required paramater. This should be the number MLBAM associates for an MLB team.
For example, the Cincinnati Reds have an MLBAM team ID of 113.

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.get40ManRoster(teamID=113)
print(df)
```

## getAllTimeRoster(teamID:int,startSeason:int,endSeason:int):

Retrives the cumulative roster for a MLB team in a specified timeframe.

### Args:

teamID (int):
Required paramater. If no MLBAM Team ID is provided, the cumulative roster for the Cincinnati Reds will be returned.

startSeason (int):
Required parameter. This value must be less than endSeason for this function to work.

endSeason (int):
Required parameter. This value must be greater than startSeason for this function to work.

### Example:

```
import sportsdataverse as sdv
df = sdv.mlb.getAllTimeRoster(teamID=113,startSeason=2020,endSeason=2021)
print(df)
```
