# sportsdataverse.wnba package

## Submodules

## sportsdataverse.wnba.wnba_game_rosters module


### sportsdataverse.wnba.wnba_game_rosters.espn_wnba_game_rosters(game_id: int, raw=False, return_as_pandas=True, \*\*kwargs)
espn_wnba_game_rosters() - Pull the game by id.

Args:

    game_id (int): Unique game_id, can be obtained from espn_wnba_schedule().
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

    wnba_df = sportsdataverse.wnba.espn_wnba_game_rosters(game_id=401370395)


### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_game_items(summary)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_team_items(items, \*\*kwargs)
## sportsdataverse.wnba.wnba_loaders module


### sportsdataverse.wnba.wnba_loaders.load_wnba_pbp(seasons: List[int], return_as_pandas=True)
Load WNBA play by play data going back to 2002

Example:

    wnba_df = sportsdataverse.wnba.load_wnba_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wnba.wnba_loaders.load_wnba_player_boxscore(seasons: List[int], return_as_pandas=True)
Load WNBA player boxscore data

Example:

    wnba_df = sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wnba.wnba_loaders.load_wnba_schedule(seasons: List[int], return_as_pandas=True)
Load WNBA schedule data

Example:

    wnba_df = sportsdataverse.wnba.load_wnba_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wnba.wnba_loaders.load_wnba_team_boxscore(seasons: List[int], return_as_pandas=True)
Load WNBA team boxscore data

Example:

    wnba_df = sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.wnba.wnba_pbp module


### sportsdataverse.wnba.wnba_pbp.espn_wnba_pbp(game_id: int, raw=False, \*\*kwargs)
espn_wnba_pbp() - Pull the game by id. Data from API endpoints - wnba/playbyplay, wnba/summary

Args:

    game_id (int): Unique game_id, can be obtained from wnba_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”,

        “broadcasts”, “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”,
        “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “espnWP”, “gameInfo”, “season”

Example:

    wnba_df = sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395)


### sportsdataverse.wnba.wnba_pbp.helper_wnba_game_data(pbp_txt, init)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp(game_id, pbp_txt)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pickcenter(pbp_txt)

### sportsdataverse.wnba.wnba_pbp.wnba_pbp_disk(game_id, path_to_json)
## sportsdataverse.wnba.wnba_schedule module


### sportsdataverse.wnba.wnba_schedule.espn_wnba_calendar(season=None, ondays=None, return_as_pandas=True, \*\*kwargs)
espn_wnba_calendar - look up the WNBA calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    ondays (boolean): Used to return dates for calendar ondays

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wnba.wnba_schedule.espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=True, \*\*kwargs)
espn_wnba_schedule - look up the WNBA schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.


### sportsdataverse.wnba.wnba_schedule.most_recent_wnba_season()
## sportsdataverse.wnba.wnba_teams module


### sportsdataverse.wnba.wnba_teams.espn_wnba_teams(return_as_pandas=True, \*\*kwargs)
espn_wnba_teams - look up WNBA teams

Args:

    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

Example:

    wnba_df = sportsdataverse.wnba.espn_wnba_teams()

## Module contents
