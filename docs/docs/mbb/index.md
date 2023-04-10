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


### sportsdataverse.mbb.mbb_pbp.espn_mbb_pbp(game_id: int, raw=False)
espn_mbb_pbp() - Pull the game by id. Data from API endpoints: mens-college-basketball/playbyplay, mens-college-basketball/summary

Args:

    game_id (int): Unique game_id, can be obtained from mbb_schedule().

Returns:

    Dict: Dictionary of game data with keys: “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
    “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”, “againstTheSpread”, “odds”, “predictor”,
    “espnWP”, “gameInfo”, “season”

Example:

    mbb_df = sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031)


### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp(game_id, pbp_txt)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp_features(game_id, pbp_txt, gameSpread, homeFavorite, gameSpreadAvailable, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pickcenter(pbp_txt)

### sportsdataverse.mbb.mbb_pbp.mbb_pbp_disk(game_id, path_to_json)
## sportsdataverse.mbb.mbb_schedule module


### sportsdataverse.mbb.mbb_schedule.espn_mbb_calendar(season=None)
espn_mbb_calendar - look up the men’s college basketball calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.


### sportsdataverse.mbb.mbb_schedule.espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500)
espn_mbb_schedule - look up the men’s college basketball scheduler for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

## sportsdataverse.mbb.mbb_teams module


### sportsdataverse.mbb.mbb_teams.espn_mbb_teams(groups=None)
espn_mbb_teams - look up the men’s college basketball teams

Args:

    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

## Module contents
