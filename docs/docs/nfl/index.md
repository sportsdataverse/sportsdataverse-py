# sportsdataverse.nfl package

## Submodules

## sportsdataverse.nfl.model_vars module

## sportsdataverse.nfl.nfl_game_rosters module


### sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs)
espn_nfl_game_rosters() - Pull the game by id.

Args:

    game_id (int): Unique game_id, can be obtained from espn_nfl_schedule().
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

    nfl_df = sportsdataverse.nfl.espn_nfl_game_rosters(game_id=401220403)


### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_game_items(summary)

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_team_items(items, \*\*kwargs)
## sportsdataverse.nfl.nfl_games module


### sportsdataverse.nfl.nfl_games.nfl_game_details()
Args:

    game_id (int): Game ID

Returns:

    Dict: Dictionary of odds and props data with keys

Example:

    nfl_df = nfl_game_details(
    game_id = ‘7ae87c4c-d24c-11ec-b23d-d15a91047884’
    )


### sportsdataverse.nfl.nfl_games.nfl_game_schedule()
Args:

    season (int): season
    season_type (str): season type - REG, POST
    week (int): week

Returns:

    Dict: Dictionary of odds and props data with keys

Example:



    ```
    `
    ```

    nfl_df = nfl_game_schedule(

        season = 2021, seasonType=’REG’, week=1

    )\`


### sportsdataverse.nfl.nfl_games.nfl_headers_gen()

### sportsdataverse.nfl.nfl_games.nfl_token_gen()
## sportsdataverse.nfl.nfl_loaders module


### sportsdataverse.nfl.nfl_loaders.load_nfl_combine(return_as_pandas=False)
Load NFL Combine information

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_combine()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing NFL combine data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_contracts(return_as_pandas=False)
Load NFL Historical contracts information

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_contracts()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing historical contracts available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_depth_charts(seasons: List[int], return_as_pandas=False)
Load NFL Depth Chart data for selected seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_depth_charts(seasons=range(2001,2021))

Args:

    seasons (list): Used to define different seasons. 2001 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing depth chart data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_draft_picks(return_as_pandas=False)
Load NFL Draft picks information

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_draft_picks()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing NFL Draft picks data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_injuries(seasons: List[int], return_as_pandas=False)
Load NFL injuries data for selected seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_injuries(seasons=range(2009,2021))

Args:

    seasons (list): Used to define different seasons. 2009 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing injuries data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_passing(return_as_pandas=False)
Load NFL NextGen Stats Passing data going back to 2016

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_ngs_passing()

Returns:

    pd.DataFrame: Pandas dataframe containing the NextGen Stats Passing data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_receiving(return_as_pandas=False)
Load NFL NextGen Stats Receiving data going back to 2016

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_ngs_receiving()

Returns:

    pd.DataFrame: Pandas dataframe containing the NextGen Stats Receiving data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_rushing(return_as_pandas=False)
Load NFL NextGen Stats Rushing data going back to 2016

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_ngs_rushing()

Returns:

    pd.DataFrame: Pandas dataframe containing the NextGen Stats Rushing data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_officials(return_as_pandas=False)
Load NFL Officials information

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_officials()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing officials available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pbp(seasons: List[int], return_as_pandas=False)
Load NFL play by play data going back to 1999

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pbp(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing the play-by-plays available for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pbp_participation(seasons: List[int], return_as_pandas=False)
Load NFL play-by-play participation data for selected seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pbp_participation(seasons=range(2016,2021))

Args:

    seasons (list): Used to define different seasons. 2016 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing play-by-play participation data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_def(return_as_pandas=False)
Load NFL Pro-Football Reference Advanced Defensive data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_def()

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced defensive stats data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_pass(return_as_pandas=False)
Load NFL Pro-Football Reference Advanced Passing data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_pass()

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced passing stats data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_rec(return_as_pandas=False)
Load NFL Pro-Football Reference Advanced Receiving data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_rec()

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced receiving stats data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_rush(return_as_pandas=False)
Load NFL Pro-Football Reference Advanced Rushing data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_rush()

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced rushing stats data available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_def(seasons: List[int], return_as_pandas=False)
Load NFL Pro-Football Reference Weekly Advanced Defensive data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_def(seasons=range(2018,2021))

Args:

    seasons (list): Used to define different seasons. 2018 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced defensive stats data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_pass(seasons: List[int], return_as_pandas=False)
Load NFL Pro-Football Reference Weekly Advanced Passing data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_pass(seasons=range(2018,2021))

Args:

    seasons (list): Used to define different seasons. 2018 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced passing stats data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_rec(seasons: List[int], return_as_pandas=False)
Load NFL Pro-Football Reference Weekly Advanced Receiving data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_rec(seasons=range(2018,2021))

Args:

    seasons (list): Used to define different seasons. 2018 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced receiving stats data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_rush(seasons: List[int], return_as_pandas=False)
Load NFL Pro-Football Reference Weekly Advanced Rushing data going back to 2018

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_pfr_weekly_rush(seasons=range(2018,2021))

Args:

    seasons (list): Used to define different seasons. 2018 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing Pro-Football Reference

        advanced rushing stats data available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_player_stats(kicking=False, return_as_pandas=False)
Load NFL player stats data

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_player_stats()

Args:

    kicking (bool): If True, load kicking stats. If False, load all other stats.

Returns:

    pd.DataFrame: Pandas dataframe containing player stats.


### sportsdataverse.nfl.nfl_loaders.load_nfl_players(return_as_pandas=False)
Load NFL Player ID information

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_players()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing players available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_rosters(seasons: List[int], return_as_pandas=False)
Load NFL roster data for all seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_rosters(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1920 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing rosters available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_schedule(seasons: List[int], return_as_pandas=False)
Load NFL schedule data

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_schedule(seasons=range(1999,2021))

Args:

    seasons (list): Used to define different seasons. 1999 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the schedule for the requested seasons.

Raises:

    ValueError: If season is less than 1999.


### sportsdataverse.nfl.nfl_loaders.load_nfl_snap_counts(seasons: List[int], return_as_pandas=False)
Load NFL snap counts data for selected seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_snap_counts(seasons=range(2012,2021))

Args:

    seasons (list): Used to define different seasons. 2012 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing snap counts available for the requested seasons.


### sportsdataverse.nfl.nfl_loaders.load_nfl_teams(return_as_pandas=False)
Load NFL team ID information and logos

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_teams()

Args:

Returns:

    pd.DataFrame: Pandas dataframe containing teams available.


### sportsdataverse.nfl.nfl_loaders.load_nfl_weekly_rosters(seasons: List[int], return_as_pandas=False)
Load NFL weekly roster data for selected seasons

Example:

    nfl_df = sportsdataverse.nfl.load_nfl_weekly_rosters(seasons=range(2002,2021))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing weekly rosters available for the requested seasons.

## sportsdataverse.nfl.nfl_pbp module


### class sportsdataverse.nfl.nfl_pbp.NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)
Bases: `object`


#### \__init__(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)
Initialize self.  See help(type(self)) for accurate signature.


#### create_box_score(play_df)

#### espn_nfl_pbp(\*\*kwargs)
espn_nfl_pbp() - Pull the game by id. Data from API endpoints: nfl/playbyplay, nfl/summary

Args:

    game_id (int): Unique game_id, can be obtained from nfl_schedule().

Returns:

    Dict: Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,

        “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
        “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
        “gameInfo”, “season”

Example:

    nfl_df = sportsdataverse.nfl.NFLPlayProcess(gameId=401220403).espn_nfl_pbp()


#### gameId( = 0)

#### nfl_pbp_disk()

#### path_to_json( = '/')

#### ran_cleaning_pipeline( = False)

#### ran_pipeline( = False)

#### raw( = False)

#### return_keys( = None)

#### run_cleaning_pipeline()

#### run_processing_pipeline()
## sportsdataverse.nfl.nfl_schedule module


### sportsdataverse.nfl.nfl_schedule.espn_nfl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs)
espn_nfl_calendar - look up the NFL calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.
    ondays (boolean): Used to return dates for calendar ondays
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.


### sportsdataverse.nfl.nfl_schedule.espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, \*\*kwargs)
espn_nfl_schedule - look up the NFL schedule for a given season

Args:

    dates (int): Used to define different seasons. 2002 is the earliest available season.
    week (int): Week of the schedule.
    season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
    limit (int): number of records to return, default: 500.
    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.


### sportsdataverse.nfl.nfl_schedule.get_current_week()

### sportsdataverse.nfl.nfl_schedule.most_recent_nfl_season()
## sportsdataverse.nfl.nfl_teams module


### sportsdataverse.nfl.nfl_teams.espn_nfl_teams(return_as_pandas=False, \*\*kwargs)
espn_nfl_teams - look up NFL teams

Args:

    return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:

    pd.DataFrame: Pandas dataframe containing teams for the requested league.

Example:

    nfl_df = sportsdataverse.nfl.espn_nfl_teams()

## Module contents
