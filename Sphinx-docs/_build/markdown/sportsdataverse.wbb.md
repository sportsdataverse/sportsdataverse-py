# sportsdataverse.wbb package

## Submodules

## sportsdataverse.wbb.wbb_loaders module


### sportsdataverse.wbb.wbb_loaders.load_wbb_pbp(seasons: List[int])
Load women’s college basketball play by play data going back to 2002

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_loaders.load_wbb_player_boxscore(seasons: List[int])
Load women’s college basketball player boxscore data

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_loaders.load_wbb_schedule(seasons: List[int])
Load women’s college basketball schedule data

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_loaders.load_wbb_team_boxscore(seasons: List[int])
Load women’s college basketball team boxscore data

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.wbb.wbb_pbp module


### sportsdataverse.wbb.wbb_pbp.espn_wbb_pbp(game_id: int, raw=False)
espn_wbb_pbp() - Pull the game by id. Data from API endpoints - womens-college-basketball/playbyplay, womens-college-basketball/summary

Args:

    game_id (int): Unique game_id, can be obtained from wbb_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”, “againstTheSpread”, “odds”, “predictor”,
        “espnWP”, “gameInfo”, “season”

Example:

    wbb_df = sportsdataverse.wb.espn_wbb_pbp(game_id=401266534)


### sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp(game_id, pbp_txt)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp_features(game_id, pbp_txt, homeTeamId, awayTeamId, homeTeamMascot, awayTeamMascot, homeTeamName, awayTeamName, homeTeamAbbrev, awayTeamAbbrev, homeTeamNameAlt, awayTeamNameAlt, gameSpread, homeFavorite, gameSpreadAvailable)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pickcenter(pbp_txt)
## sportsdataverse.wbb.wbb_schedule module


### sportsdataverse.wbb.wbb_schedule.espn_wbb_calendar(season=None)
espn_wbb_calendar - look up the women’s college basketball calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing
    calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_schedule.espn_wbb_schedule(dates=None, groups=None, season_type=None, limit=500)
espn_wbb_schedule - look up the women’s college basketball schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

## sportsdataverse.wbb.wbb_teams module


### sportsdataverse.wbb.wbb_teams.espn_wbb_teams(groups=None)
espn_wbb_teams - look up the women’s college basketball teams

Args:

    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

## Module contents
