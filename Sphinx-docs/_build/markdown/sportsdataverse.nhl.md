# sportsdataverse.nhl package

## Submodules

## sportsdataverse.nhl.nhl_loaders module


### sportsdataverse.nhl.nhl_loaders.load_nhl_pbp(seasons: List[int])
Load NHL play by play data going back to 1999

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_pbp(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


### sportsdataverse.nhl.nhl_loaders.load_nhl_player_stats()
Load NHL player stats data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_player_stats()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing player stats.


### sportsdataverse.nhl.nhl_loaders.load_nhl_rosters()
Load NHL roster data for all seasons

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_rosters(seasons=range(1999,2021))

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.


### sportsdataverse.nhl.nhl_loaders.load_nhl_schedule(seasons: List[int])
Load NHL schedule data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_schedule(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


### sportsdataverse.nhl.nhl_loaders.nhl_teams()
Load NHL team ID information and logos

Example:

    nhl_df = sportsdataverse.nhl.nhl_teams()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.

## sportsdataverse.nhl.nhl_pbp module


### sportsdataverse.nhl.nhl_pbp.nhl_pbp(game_id: int)
nhl_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
        “odds”, “onIce”, “gameInfo”, “season”

Example:

    nhl_df = sportsdataverse.nhl.nhl_pbp(game_id=401247153)

## sportsdataverse.nhl.nhl_schedule module


### sportsdataverse.nhl.nhl_schedule.nhl_calendar(season=None)
nhl_calendar - look up the NHL calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nhl.nhl_schedule.nhl_schedule(dates=None, season_type=None)
nhl_schedule - look up the NHL schedule for a given date

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season

Returns:

    pd.DataFrame: Pandas dataframe containing
    schedule events for the requested season.

## Module contents
