# sportsdataverse.wbb package

## Submodules

## sportsdataverse.wbb.wbb_game_rosters module


### sportsdataverse.wbb.wbb_game_rosters.espn_wbb_game_rosters(game_id: int, raw=False, return_as_pandas=True, \*\*kwargs)
espn_wbb_game_rosters() - Pull the game by id.

Args:

    game_id (int): Unique game_id, can be obtained from wbb_schedule().
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
    ‘team_location’, ‘team_name’, ‘team_nickname’, ‘team_abbreviation’,
    ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
    ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
    ‘team_alternate_ids_sdr’, ‘logo_href’, ‘logo_dark_href’, ‘game_id’

Example:

    wbb_df = sportsdataverse.wbb.espn_wbb_game_rosters(game_id=401266534)


### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_game_items(summary)

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_team_items(items, \*\*kwargs)
## sportsdataverse.wbb.wbb_loaders module


### sportsdataverse.wbb.wbb_loaders.load_wbb_pbp(seasons: List[int], return_as_pandas=True)
Load women’s college basketball play by play data going back to 2002

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_pbp(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_loaders.load_wbb_player_boxscore(seasons: List[int], return_as_pandas=True)
Load women’s college basketball player boxscore data

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_loaders.load_wbb_schedule(seasons: List[int], return_as_pandas=True)
Load women’s college basketball schedule data

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_loaders.load_wbb_team_boxscore(seasons: List[int], return_as_pandas=True)
Load women’s college basketball team boxscore data

Example:

    wbb_df = sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.wbb.wbb_pbp module


### sportsdataverse.wbb.wbb_pbp.espn_wbb_pbp(game_id: int, raw=False, \*\*kwargs)
espn_wbb_pbp() - Pull the game by id. Data from API endpoints - womens-college-basketball/playbyplay,
womens-college-basketball/summary

Args:

    game_id (int): Unique game_id, can be obtained from wbb_schedule().

raw (bool): If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”,
    “broadcasts”, “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”,
    “againstTheSpread”, “odds”, “predictor”,”espnWP”, “gameInfo”, “season”

Example:

    wbb_df = sportsdataverse.wb.espn_wbb_pbp(game_id=401266534)


### sportsdataverse.wbb.wbb_pbp.helper_wbb_game_data(pbp_txt, init)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp(game_id, pbp_txt)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pickcenter(pbp_txt)

### sportsdataverse.wbb.wbb_pbp.mbb_pbp_disk(game_id, path_to_json)
## sportsdataverse.wbb.wbb_schedule module


### sportsdataverse.wbb.wbb_schedule.espn_wbb_calendar(season=None, ondays=None, return_as_pandas=True, \*\*kwargs)
espn_wbb_calendar - look up the women’s college basketball calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    ondays (boolean): Used to return dates for calendar ondays
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing
    calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.wbb.wbb_schedule.espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=True, \*\*kwargs)
espn_wbb_schedule - look up the women’s college basketball schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.


### sportsdataverse.wbb.wbb_schedule.most_recent_wbb_season()
## sportsdataverse.wbb.wbb_teams module


### sportsdataverse.wbb.wbb_teams.espn_wbb_teams(groups=None, return_as_pandas=True, \*\*kwargs)
espn_wbb_teams - look up the women’s college basketball teams

Args:

    groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

Example:

    wbb_df = sportsdataverse.wbb.espn_wbb_teams()

## Module contents
