# sportsdataverse.nba package

## Submodules

## sportsdataverse.nba.nba_loaders module


### sportsdataverse.nba.nba_loaders.load_nba_pbp(seasons: List[int])
Load NBA play by play data going back to 2002

Example:

    nba_df = sportsdataverse.nba.load_nba_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_loaders.load_nba_player_boxscore(seasons: List[int])
Load NBA player boxscore data

Example:

    nba_df = sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_loaders.load_nba_schedule(seasons: List[int])
Load NBA schedule data

Example:

    nba_df = sportsdataverse.nba.load_nba_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_loaders.load_nba_team_boxscore(seasons: List[int])
Load NBA team boxscore data

Example:

    nba_df = sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.nba.nba_pbp module


### sportsdataverse.nba.nba_pbp.espn_nba_pbp(game_id: int, raw=False)
espn_nba_pbp() - Pull the game by id - Data from API endpoints - nba/playbyplay, nba/summary

Args:

    game_id (int): Unique game_id, can be obtained from nba_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”, “pickcenter”, “againstTheSpread”,
        “odds”, “predictor”, “espnWP”, “gameInfo”, “season”

Example:

    nba_df = sportsdataverse.nba.espn_nba_pbp(game_id=401307514)


### sportsdataverse.nba.nba_pbp.helper_nba_pbp(game_id, pbp_txt)

### sportsdataverse.nba.nba_pbp.helper_nba_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable)

### sportsdataverse.nba.nba_pbp.helper_nba_pickcenter(pbp_txt)

### sportsdataverse.nba.nba_pbp.nba_pbp_disk(game_id, path_to_json)
## sportsdataverse.nba.nba_schedule module


### sportsdataverse.nba.nba_schedule.espn_nba_calendar(season=None)
espn_nba_calendar - look up the NBA calendar for a given season from ESPN

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing
    calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_schedule.espn_nba_schedule(dates=None, season_type=None, limit=500)
espn_nba_schedule - look up the NBA schedule for a given date from ESPN

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
    limit (int): number of records to return, default: 500.

Returns:

    pd.DataFrame: Pandas dataframe containing
    schedule events for the requested season.

## sportsdataverse.nba.nba_teams module


### sportsdataverse.nba.nba_teams.espn_nba_teams()
espn_nba_teams - look up NBA teams

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

## Module contents
