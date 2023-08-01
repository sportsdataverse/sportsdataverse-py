# sportsdataverse.nba package

## Submodules

## sportsdataverse.nba.nba_game_rosters module


### sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs)
espn_nba_game_rosters() - Pull the game by id.

Args:

    game_id (int): Unique game_id, can be obtained from espn_nba_schedule().
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Data frame of game roster data with columns:
    ‘athlete_id’, ‘athlete_uid’, ‘athlete_guid’, ‘athlete_type’,
    ‘first_name’, ‘last_name’, ‘full_name’, ‘athlete_display_name’,
    ‘short_name’, ‘weight’, ‘display_weight’, ‘height’, ‘display_height’,
    ‘age’, ‘date_of_birth’, ‘slug’, ‘jersey’, ‘linked’, ‘active’,
    ‘alternate_ids_sdr’, ‘birth_place_city’, ‘birth_place_state’,
    ‘birth_place_country’, ‘headshot_href’, ‘headshot_alt’,
    ‘experience_years’, ‘experience_display_value’,
    ‘experience_abbreviation’, ‘status_id’, ‘status_name’, ‘status_type’,
    ‘status_abbreviation’, ‘hand_type’, ‘hand_abbreviation’,
    ‘hand_display_value’, ‘draft_display_text’, ‘draft_round’, ‘draft_year’,
    ‘draft_selection’, ‘player_id’, ‘starter’, ‘valid’, ‘did_not_play’,
    ‘display_name’, ‘ejected’, ‘athlete_href’, ‘position_href’,
    ‘statistics_href’, ‘team_id’, ‘team_guid’, ‘team_uid’, ‘team_slug’,
    ‘team_location’, ‘team_name’, ‘team_abbreviation’,
    ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
    ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
    ‘logo_href’, ‘logo_dark_href’, ‘game_id’

Example:

    nba_df = sportsdataverse.nba.espn_nba_game_rosters(game_id=401307514)


### sportsdataverse.nba.nba_game_rosters.helper_nba_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.nba.nba_game_rosters.helper_nba_game_items(summary)

### sportsdataverse.nba.nba_game_rosters.helper_nba_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.nba.nba_game_rosters.helper_nba_team_items(items, \*\*kwargs)
## sportsdataverse.nba.nba_loaders module


### sportsdataverse.nba.nba_loaders.load_nba_pbp(seasons: List[int], return_as_pandas=False)
Load NBA play by play data going back to 2002

Example:

    nba_df = sportsdataverse.nba.load_nba_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_loaders.load_nba_player_boxscore(seasons: List[int], return_as_pandas=False)
Load NBA player boxscore data

Example:

    nba_df = sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_loaders.load_nba_schedule(seasons: List[int], return_as_pandas=False)
Load NBA schedule data

Example:

    nba_df = sportsdataverse.nba.load_nba_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_loaders.load_nba_team_boxscore(seasons: List[int], return_as_pandas=False)
Load NBA team boxscore data

Example:

    nba_df = sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.nba.nba_pbp module


### sportsdataverse.nba.nba_pbp.espn_nba_pbp(game_id: int, raw=False, \*\*kwargs)
espn_nba_pbp() - Pull the game by id - Data from API endpoints - nba/playbyplay, nba/summary

Args:

    game_id (int): Unique game_id, can be obtained from nba_schedule().
    raw (bool): If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”, “pickcenter”, “againstTheSpread”,
        “odds”, “predictor”, “espnWP”, “gameInfo”, “season”

Example:

    nba_df = sportsdataverse.nba.espn_nba_pbp(game_id=401307514)


### sportsdataverse.nba.nba_pbp.helper_nba_game_data(pbp_txt, init)

### sportsdataverse.nba.nba_pbp.helper_nba_pbp(game_id, pbp_txt)

### sportsdataverse.nba.nba_pbp.helper_nba_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.nba.nba_pbp.helper_nba_pickcenter(pbp_txt)

### sportsdataverse.nba.nba_pbp.nba_pbp_disk(game_id, path_to_json)
## sportsdataverse.nba.nba_schedule module


### sportsdataverse.nba.nba_schedule.espn_nba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs)
espn_nba_calendar - look up the NBA calendar for a given season from ESPN

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    ondays (boolean): Used to return dates for calendar ondays
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nba.nba_schedule.espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs)
espn_nba_schedule - look up the NBA schedule for a given date from ESPN

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season,
    4 for all-star, 5 for off-season
    limit (int): number of records to return, default: 500.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games


### sportsdataverse.nba.nba_schedule.most_recent_nba_season()

### sportsdataverse.nba.nba_schedule.year_to_season(year)
## sportsdataverse.nba.nba_teams module


### sportsdataverse.nba.nba_teams.espn_nba_teams(return_as_pandas=False, \*\*kwargs)
espn_nba_teams - look up NBA teams

Args:

    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

Example:

    nba_df = sportsdataverse.nba.espn_nba_teams()

## Module contents
