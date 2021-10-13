# sportsdataverse.mbb package

## Submodules

## sportsdataverse.mbb.mbb_loaders module


### sportsdataverse.mbb.mbb_loaders.load_mbb_pbp(seasons: List[int])
Load men’s college basketball play by play data going back to 2002

Example:

    mbb_df = sportsdataverse.mbb.load_mbb_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.mbb.mbb_loaders.load_mbb_player_boxscore(seasons: List[int])
Load men’s college basketball player boxscore data

Example:

    mbb_df = sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.mbb.mbb_loaders.load_mbb_schedule(seasons: List[int])
Load men’s college basketball schedule data

Example:

    mbb_df = sportsdataverse.mbb.load_mbb_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.mbb.mbb_loaders.load_mbb_team_boxscore(seasons: List[int])
Load men’s college basketball team boxscore data

Example:

    mbb_df = sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.mbb.mbb_pbp module


### sportsdataverse.mbb.mbb_pbp.mbb_pbp(game_id: int)
mbb_pbp() - Pull the game by id. Data from API endpoints: mens-college-basketball/playbyplay, mens-college-basketball/summary

Args:

    game_id (int): Unique game_id, can be obtained from mbb_schedule().

Returns:

    Dict: Dictionary of game data with keys: “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
    “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”, “againstTheSpread”, “odds”, “predictor”,
    “espnWP”, “gameInfo”, “season”

Example:

    mbb_df = sportsdataverse.mbb.mbb_pbp(game_id=401265031)

## sportsdataverse.mbb.mbb_schedule module


### sportsdataverse.mbb.mbb_schedule.mbb_calendar(season=None)
mbb_calendar - look up the men’s college basketball calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.


### sportsdataverse.mbb.mbb_schedule.mbb_schedule(dates=None, groups=None, season_type=None)
mbb_schedule - look up the men’s college basketball scheduler for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II, 52 is Division III.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

## Module contents
