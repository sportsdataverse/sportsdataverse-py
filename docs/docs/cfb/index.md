# CFB Function Index

## sportsdataverse.cfb.cfb_teams()
Load college football team ID information and logos

Example:

    cfb_df = sportsdataverse.cfb.cfb_teams()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.

----------------------------------------------------------------------------------------

# CFB Loaders
### sportsdataverse.cfb.load_cfb_pbp(seasons: List[int])
Load college football play by play data going back to 2003

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2003,2021))

Args:

    seasons (list): Used to define different seasons. 2003 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2003.


### sportsdataverse.cfb.load_cfb_rosters(seasons: List[int])
Load roster data

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2021))

Args:

    seasons (list): Used to define different seasons. 2014 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

Raises:

    ValueError: If season is less than 2014.


### sportsdataverse.cfb.load_cfb_schedule(seasons: List[int])
Load college football schedule data

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.cfb.load_cfb_team_info(seasons: List[int])
Load college football team info

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the team info available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

### class sportsdataverse.cfb.CFBPlayProcess(gameId=0)
Bases: `object`


#### \__init__(gameId=0)
Initialize self.  See help(type(self)) for accurate signature.


#### cfb_pbp()
cfb_pbp() - Pull the game by id. Data from API endpoints: college-football/playbyplay, college-football/summary

Args:

    game_id (int): Unique game_id, can be obtained from cfb_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
        “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
        “gameInfo”, “season”

Example:

    cfb_df = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).cfb_pbp()


#### create_box_score()

#### gameId( = 0)

#### ran_pipeline( = False)

#### run_processing_pipeline()

### sportsdataverse.cfb.cfb_calendar(season=None, groups=None)
cfb_calendar - look up the men’s college football calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.cfb.cfb_schedule(dates=None, week=None, season_type=None, groups=None)
cfb_schedule - look up the college football schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    week (int): Week of the schedule.
    groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

