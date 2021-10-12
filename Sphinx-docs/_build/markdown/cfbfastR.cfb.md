# cfbfastR.cfb package

## Submodules

## cfbfastR.cfb.pbp module

## Module contents


### cfbfastR.cfb.cfb_calendar(season: int)
cfb_calendar - look up the men’s college football calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### cfbfastR.cfb.cfb_teams()
Load college football team ID information and logos

Example:

    cfb_df = cfbfastR.cfb.cfb_teams()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.


### cfbfastR.cfb.load_cfb_pbp(seasons: List[int])
Load college football play by play data going back to 2003

Example:

    cfb_df = cfbfastR.cfb.load_cfb_pbp(seasons=[range(2003,2021)])

Args:

    seasons (list): Used to define different seasons. 2003 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2003.


### cfbfastR.cfb.load_cfb_rosters(seasons: List[int])
Load roster data

Example:

    cfb_df = cfbfastR.cfb.load_cfb_rosters(seasons=[range(2014,2021)])

Args:

    seasons (list): Used to define different seasons. 2014 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

Raises:

    ValueError: If season is less than 2014.


### cfbfastR.cfb.load_cfb_schedule(seasons: List[int])
Load college football schedule data

Example:

    cfb_df = cfbfastR.cfb.load_cfb_schedule(seasons=[range(2002,2021)])

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### cfbfastR.cfb.load_cfb_team_info(seasons: List[int])
Load college football team info

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the team info available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.
