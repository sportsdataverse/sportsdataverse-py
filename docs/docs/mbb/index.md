# sportsdataverse.mbb package

## Submodules

## sportsdataverse.mbb.mbb_game_rosters module

### sportsdataverse.mbb.mbb_game_rosters.espn_mbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs)

espn_mbb_game_rosters() - Pull the game by id.

Args:
: game_id (int): Unique game_id, can be obtained from mbb_schedule().
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe of game roster data with columns:
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
: mbb_df = sportsdataverse.mbb.espn_mbb_game_rosters(game_id=401265031)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_game_items(summary)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_team_items(items, \*\*kwargs)

## sportsdataverse.mbb.mbb_loaders module

### sportsdataverse.mbb.mbb_loaders.load_mbb_pbp(seasons: List[int], return_as_pandas=False)

Load men’s college basketball play by play data going back to 2002

Example:
: mbb_df = sportsdataverse.mbb.load_mbb_pbp(seasons=range(2002,2022))

Args:
: seasons (list): Used to define different seasons. 2002 is the earliest available season.
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing the
  play-by-plays available for the requested seasons.

Raises:
: ValueError: If season is less than 2002.

### sportsdataverse.mbb.mbb_loaders.load_mbb_player_boxscore(seasons: List[int], return_as_pandas=False)

Load men’s college basketball player boxscore data

Example:
: mbb_df = sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2002,2022))

Args:
: seasons (list): Used to define different seasons. 2002 is the earliest available season.
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing the
  player boxscores available for the requested seasons.

Raises:
: ValueError: If season is less than 2002.

### sportsdataverse.mbb.mbb_loaders.load_mbb_schedule(seasons: List[int], return_as_pandas=False)

Load men’s college basketball schedule data

Example:
: mbb_df = sportsdataverse.mbb.load_mbb_schedule(seasons=range(2002,2022))

Args:
: seasons (list): Used to define different seasons. 2002 is the earliest available season.
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing the
  schedule for  the requested seasons.

Raises:
: ValueError: If season is less than 2002.

### sportsdataverse.mbb.mbb_loaders.load_mbb_team_boxscore(seasons: List[int], return_as_pandas=False)

Load men’s college basketball team boxscore data

Example:
: mbb_df = sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002,2022))

Args:
: seasons (list): Used to define different seasons. 2002 is the earliest available season.
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing the
  team boxscores available for the requested seasons.

Raises:
: ValueError: If season is less than 2002.

## sportsdataverse.mbb.mbb_pbp module

### sportsdataverse.mbb.mbb_pbp.espn_mbb_pbp(game_id: int, raw=False, \*\*kwargs)

espn_mbb_pbp() - Pull the game by id. Data from API endpoints: mens-college-basketball/playbyplay, mens-college-basketball/summary

Args:
: game_id (int): Unique game_id, can be obtained from mbb_schedule().
  raw (bool): If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.

Returns:
: Dict: Dictionary of game data with keys: “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
  “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”, “againstTheSpread”, “odds”, “predictor”,
  “espnWP”, “gameInfo”, “season”

Example:
: mbb_df = sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_game_data(pbp_txt, init)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp(game_id, pbp_txt)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pickcenter(pbp_txt)

### sportsdataverse.mbb.mbb_pbp.mbb_pbp_disk(game_id, path_to_json)

## sportsdataverse.mbb.mbb_schedule module

### sportsdataverse.mbb.mbb_schedule.espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs)

espn_mbb_calendar - look up the men’s college basketball calendar for a given season

Args:
: season (int): Used to define different seasons. 2002 is the earliest available season.
  ondays (boolean): Used to return dates for calendar ondays
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

Raises:
: ValueError: If season is less than 2002.

### sportsdataverse.mbb.mbb_schedule.espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs)

espn_mbb_schedule - look up the men’s college basketball scheduler for a given season

Args:
: dates (int): Used to define different seasons. 2002 is the earliest available season.
  groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
  limit (int): number of records to return, default: 500.
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games

### sportsdataverse.mbb.mbb_schedule.most_recent_mbb_season()

### sportsdataverse.mbb.mbb_schedule.scoreboard_event_parsing(event)

## sportsdataverse.mbb.mbb_teams module

### sportsdataverse.mbb.mbb_teams.espn_mbb_teams(groups=None, return_as_pandas=False, \*\*kwargs)

espn_mbb_teams - look up the men’s college basketball teams

Args:
: groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

Returns:
: pl.DataFrame: Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.mbb.espn_mbb_teams.clear_cache().

Example:
: mbb_df = sportsdataverse.mbb.espn_mbb_teams()

## Module contents
