# sportsdataverse.cfb package

## Submodules

## sportsdataverse.cfb.cfb_game_rosters module


### sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters(game_id: int, raw=False, return_as_pandas=True, \*\*kwargs)
espn_cfb_game_rosters() - Pull the game by id.

Args:

    game_id (int): Unique game_id, can be obtained from espn_cfb_schedule().
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

    cfb_df = sportsdataverse.cfb.espn_cfb_game_rosters(game_id=401256137)


### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items(summary)

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_team_items(items, \*\*kwargs)
## sportsdataverse.cfb.cfb_loaders module


### sportsdataverse.cfb.cfb_loaders.get_cfb_teams(return_as_pandas=True)
Load college football team ID information and logos

Example:

    cfb_df = sportsdataverse.cfb.cfb_teams()

Args:

    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams available for the requested seasons.


### sportsdataverse.cfb.cfb_loaders.load_cfb_pbp(seasons: List[int], return_as_pandas=True)
Load college football play by play data going back to 2003

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2003,2021))

Args:

    seasons (list): Used to define different seasons. 2003 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 2003.


### sportsdataverse.cfb.cfb_loaders.load_cfb_rosters(seasons: List[int], return_as_pandas=True)
Load roster data

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2021))

Args:

    seasons (list): Used to define different seasons. 2014 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.

Raises:

    ValueError: If season is less than 2014.


### sportsdataverse.cfb.cfb_loaders.load_cfb_schedule(seasons: List[int], return_as_pandas=True)
Load college football schedule data

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.cfb.cfb_loaders.load_cfb_team_info(seasons: List[int], return_as_pandas=True)
Load college football team info

Example:

    cfb_df = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the team info available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

## sportsdataverse.cfb.cfb_pbp module


### class sportsdataverse.cfb.cfb_pbp.CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)
Bases: `object`


#### \__init__(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)
Initialize self.  See help(type(self)) for accurate signature.


#### cfb_pbp_disk()

#### create_box_score(play_df)

#### espn_cfb_pbp(\*\*kwargs)
espn_cfb_pbp() - Pull the game by id. Data from API endpoints: college-football/playbyplay, college-football/summary

Args:

    game_id (int): Unique game_id, can be obtained from cfb_schedule().
    raw (bool): If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
        “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
        “gameInfo”, “season”

Example:

    cfb_df = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()


#### gameId( = 0)

#### path_to_json( = '/')

#### ran_cleaning_pipeline( = False)

#### ran_pipeline( = False)

#### raw( = False)

#### return_keys( = None)

#### run_cleaning_pipeline()

#### run_processing_pipeline()
## sportsdataverse.cfb.cfb_schedule module


### sportsdataverse.cfb.cfb_schedule.espn_cfb_calendar(season=None, groups=None, ondays=None, return_as_pandas=True, \*\*kwargs)
espn_cfb_calendar - look up the men’s college football calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
    ondays (boolean): Used to return dates for calendar ondays
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.cfb.cfb_schedule.espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=True, \*\*kwargs)
espn_cfb_schedule - look up the college football schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    week (int): Week of the schedule.
    groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.


### sportsdataverse.cfb.cfb_schedule.most_recent_cfb_season()
## sportsdataverse.cfb.cfb_teams module


### sportsdataverse.cfb.cfb_teams.espn_cfb_teams(groups=None, return_as_pandas=True, \*\*kwargs)
espn_cfb_teams - look up the college football teams

Args:

    groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

Example:

    cfb_df = sportsdataverse.cfb.espn_cfb_teams()

## sportsdataverse.cfb.model_vars module

## Module contents
