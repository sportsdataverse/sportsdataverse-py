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


### class sportsdataverse.nfl.nfl_pbp.NFLPlayProcess(gameId=0)
Bases: `object`


#### \__init__(gameId=0)
Initialize self.  See help(type(self)) for accurate signature.


#### create_box_score()

#### espn_nfl_pbp()
espn_nfl_pbp() - Pull the game by id - Data from API endpoints - nfl/playbyplay, nfl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nfl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”, “videos”,

        “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”, “pickcenter”,
        “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”, “gameInfo”, “season”

Example:

    nfl_df = sportsdataverse.nfl.NFLPlayProcess(game_id=401220403).espn_nfl_pbp()


#### gameId( = 0)

#### ran_pipeline( = False)

#### run_processing_pipeline()

### sportsdataverse.nfl.nfl_pbp.safe_retrieve_key(tmp_dict, key, default_value)
## sportsdataverse.nfl.nfl_schedule module


### sportsdataverse.nfl.nfl_schedule.espn_nfl_calendar(season=None)
espn_nfl_calendar - look up the NFL calendar for a given season from ESPN

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nfl.nfl_schedule.espn_nfl_schedule(dates=None, week=None, season_type=None)
espn_nfl_schedule - look up the NFL schedule for a given date from ESPN

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    week (int): Used to define different weeks.
    season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season

Returns:

    pd.DataFrame: Pandas dataframe containing
    schedule events for the requested season.

## sportsdataverse.nfl.nfl_teams module


### sportsdataverse.nfl.nfl_teams.espn_nfl_teams()
espn_nfl_teams - look up NFL teams

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

## Module contents
