# sportsdataverse.nfl package

## Submodules

## sportsdataverse.nfl.model_vars module

## sportsdataverse.nfl.nfl_loaders module


### sportsdataverse.nfl.nfl_loaders.load_nfl_pbp(seasons: List[int])
Load NFL play by play data going back to 1999

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pbp(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


### sportsdataverse.nfl.nfl_loaders.load_nfl_player_stats()
Load NFL player stats data

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_player_stats()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing player stats.


### sportsdataverse.nfl.nfl_loaders.load_nfl_rosters()
Load NFL roster data for all seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_rosters(seasons=range(1999,2021))

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_schedule(seasons: List[int])
Load NFL schedule data

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_schedule(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


### sportsdataverse.nfl.nfl_loaders.load_nfl_teams()
Load NFL team ID information and logos

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_teams()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.

## sportsdataverse.nfl.nfl_pbp module


### class sportsdataverse.nfl.nfl_pbp.NFLPlayProcess(gameId=0, raw=False, path_to_json='/')
Bases: `object`


#### \__init__(gameId=0, raw=False, path_to_json='/')
Initialize self.  See help(type(self)) for accurate signature.


#### create_box_score()

#### espn_nfl_pbp()
espn_nfl_pbp() - Pull the game by id. Data from API endpoints: nfl/playbyplay, nfl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nfl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
        “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
        “gameInfo”, “season”

Example:

    nfl_df = sportsdataverse.nfl.NFLPlayProcess(gameId=401256137).espn_nfl_pbp()


#### gameId( = 0)

#### nfl_pbp_disk()

#### path_to_json( = '/')

#### ran_cleaning_pipeline( = False)

#### ran_pipeline( = False)

#### raw( = False)

#### run_cleaning_pipeline()

#### run_processing_pipeline()
## sportsdataverse.nfl.nfl_schedule module


### sportsdataverse.nfl.nfl_schedule.espn_nfl_calendar(season=None, ondays=None)
espn_nfl_calendar - look up the NFL calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    ondays (boolean): Used to return dates for calendar ondays

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nfl.nfl_schedule.espn_nfl_schedule(dates=None, week=None, season_type=None, limit=500)
espn_nfl_schedule - look up the NFL schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    week (int): Week of the schedule.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

## sportsdataverse.nfl.nfl_teams module


### sportsdataverse.nfl.nfl_teams.espn_nfl_teams()
espn_nfl_teams - look up NFL teams

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

## Module contents
