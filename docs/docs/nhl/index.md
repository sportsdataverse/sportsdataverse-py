# sportsdataverse.nhl package

## Submodules

## sportsdataverse.nhl.nhl_api module


### sportsdataverse.nhl.nhl_api.nhl_api_pbp(game_id: int, \*\*kwargs)
nhl_api_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
        “odds”, “onIce”, “gameInfo”, “season”

Example:

    nhl_df = sportsdataverse.nhl.nhl_api_pbp(game_id=2021020079)


### sportsdataverse.nhl.nhl_api.nhl_api_schedule(start_date: str, end_date: str, return_as_pandas=False)
nhl_api_schedule() - Pull the game by id. Data from API endpoints - nhl/schedule

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Example:

    nhl_sched_df = sportsdataverse.nhl.nhl_api_schedule(start_date=2021-10-23, end_date=2021-10-28)

## sportsdataverse.nhl.nhl_game_rosters module


### sportsdataverse.nhl.nhl_game_rosters.espn_nhl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs)
espn_nhl_game_rosters() - Pull the game by id.

Args:

    game_id (int): Unique game_id, can be obtained from espn_nhl_schedule().
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

    nhl_df = sportsdataverse.nhl.espn_nhl_game_rosters(game_id=401247153)


### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_game_items(summary)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_team_items(items, \*\*kwargs)
## sportsdataverse.nhl.nhl_loaders module


### sportsdataverse.nhl.nhl_loaders.load_nhl_pbp(seasons: List[int], return_as_pandas=False)
Load NHL play by play data going back to 2011

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2011,2021))

Args:

    seasons (list): Used to define different seasons. 2011 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2011.


### sportsdataverse.nhl.nhl_loaders.load_nhl_player_boxscore(seasons: List[int], return_as_pandas=False)
Load NHL player boxscore data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_player_boxscore(seasons=range(2011,2022))

Args:

    seasons (list): Used to define different seasons. 2011 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2011.


### sportsdataverse.nhl.nhl_loaders.load_nhl_schedule(seasons: List[int], return_as_pandas=False)
Load NHL schedule data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nhl.nhl_loaders.load_nhl_team_boxscore(seasons: List[int], return_as_pandas=False)
Load NHL team boxscore data

Example:

    nhl_df = sportsdataverse.nhl.load_nhl_team_boxscore(seasons=range(2011,2022))

Args:

    seasons (list): Used to define different seasons. 2011 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2011.


### sportsdataverse.nhl.nhl_loaders.nhl_teams(return_as_pandas=False)
Load NHL team ID information and logos

Example:

    nhl_df = sportsdataverse.nhl.nhl_teams()

Args:

    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.

## sportsdataverse.nhl.nhl_pbp module


### sportsdataverse.nhl.nhl_pbp.espn_nhl_pbp(game_id: int, raw=False, \*\*kwargs)
espn_nhl_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nhl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
        “odds”, “onIce”, “gameInfo”, “season”

Example:

    nhl_df = sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)


### sportsdataverse.nhl.nhl_pbp.helper_nhl_game_data(pbp_txt, init)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp(game_id, pbp_txt)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pickcenter(pbp_txt)

### sportsdataverse.nhl.nhl_pbp.nhl_pbp_disk(game_id, path_to_json)
## sportsdataverse.nhl.nhl_schedule module


### sportsdataverse.nhl.nhl_schedule.espn_nhl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs)
espn_nhl_calendar - look up the NHL calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    ondays (boolean): Used to return dates for calendar ondays
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nhl.nhl_schedule.espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs)
espn_nhl_schedule - look up the NHL schedule for a given date

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
    limit (int): number of records to return, default: 500.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games


### sportsdataverse.nhl.nhl_schedule.most_recent_nhl_season()

### sportsdataverse.nhl.nhl_schedule.year_to_season(year)
## sportsdataverse.nhl.nhl_teams module


### sportsdataverse.nhl.nhl_teams.espn_nhl_teams(return_as_pandas=False, \*\*kwargs)
espn_nhl_teams - look up NHL teams

Args:

    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

Example:

    nhl_df = sportsdataverse.nhl.espn_nhl_teams()

## Module contents
