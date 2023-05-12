# sportsdataverse.nhl package

## Submodules

## sportsdataverse.nhl.nhl_api module


### sportsdataverse.nhl.nhl_api.nhl_api_pbp(game_id: int)
nhl_api_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
        “odds”, “onIce”, “gameInfo”, “season”

Example:

    nhl_df = sportsdataverse.nhl.nhl_api_pbp(game_id=2021020079)


### sportsdataverse.nhl.nhl_api.nhl_api_schedule(start_date: str, end_date: str)
nhl_api_schedule() - Pull the game by id. Data from API endpoints - nhl/schedule

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    Dict:

Example:

    nhl_sched_df = sportsdataverse.nhl.nhl_api_schedule(start_date=2021-10-23, end_date=2021-10-28)

## sportsdataverse.nhl.nhl_loaders module


### sportsdataverse.nhl.nhl_loaders.load_nhl_pbp(seasons: List[int])
Load NHL play by play data going back to 2011

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2011,2021))

Args:

    seasons (list): Used to define different seasons. 2011 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2011.


### sportsdataverse.nhl.nhl_loaders.load_nhl_player_boxscore(seasons: List[int])
Load NHL player boxscore data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_player_boxscore(seasons=range(2011,2022))

Args:

    seasons (list): Used to define different seasons. 2011 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2011.


### sportsdataverse.nhl.nhl_loaders.load_nhl_schedule(seasons: List[int])
Load NHL schedule data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nhl.nhl_loaders.load_nhl_team_boxscore(seasons: List[int])
Load NHL team boxscore data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_team_boxscore(seasons=range(2011,2022))

Args:

    seasons (list): Used to define different seasons. 2011 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2011.


### sportsdataverse.nhl.nhl_loaders.nhl_teams()
Load NHL team ID information and logos

Example:

    nhl_df = sportsdataverse.nhl.nhl_teams()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.

## sportsdataverse.nhl.nhl_pbp module


### sportsdataverse.nhl.nhl_pbp.espn_nhl_pbp(game_id: int, raw=False)
espn_nhl_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
        “odds”, “onIce”, “gameInfo”, “season”

Example:

    nhl_df = sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)


### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp(game_id, pbp_txt)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pickcenter(pbp_txt)

### sportsdataverse.nhl.nhl_pbp.nhl_pbp_disk(game_id, path_to_json)
## sportsdataverse.nhl.nhl_schedule module


### sportsdataverse.nhl.nhl_schedule.espn_nhl_calendar(season=None)
espn_nhl_calendar - look up the NHL calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nhl.nhl_schedule.espn_nhl_schedule(dates=None, season_type=None, limit=500)
espn_nhl_schedule - look up the NHL schedule for a given date

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
    limit (int): number of records to return, default: 500.

Returns:

    pd.DataFrame: Pandas dataframe containing
    schedule events for the requested season.


### sportsdataverse.nhl.nhl_schedule.most_recent_nhl_season()

### sportsdataverse.nhl.nhl_schedule.year_to_season(year)
## sportsdataverse.nhl.nhl_teams module


### sportsdataverse.nhl.nhl_teams.espn_nhl_teams()
espn_nhl_teams - look up NHL teams

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

## Module contents
