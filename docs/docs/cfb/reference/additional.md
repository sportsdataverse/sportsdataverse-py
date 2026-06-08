---
title: CFB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# CFB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.cfb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_cfb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_cfb_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer |  |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `full_name` | character |  |
| `athlete_display_name` | character |  |
| `short_name` | character |  |
| `weight` | double |  |
| `display_weight` | character |  |
| `height` | double |  |
| `display_height` | character |  |
| `slug` | character |  |
| `jersey` | character |  |
| `linked` | logical |  |
| `active` | logical |  |
| `alternate_ids_sdr` | character |  |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |
| `birth_place_country` | character |  |
| `birth_country_alternate_id` | character |  |
| `birth_country_abbreviation` | character |  |
| `headshot_href` | character |  |
| `headshot_alt` | character |  |
| `flag_href` | character |  |
| `flag_alt` | character |  |
| `flag_rel` | character |  |
| `experience_years` | integer |  |
| `experience_display_value` | character |  |
| `experience_abbreviation` | character |  |
| `status_id` | character |  |
| `status_name` | character |  |
| `status_type` | character |  |
| `status_abbreviation` | character |  |
| `hand_type` | character |  |
| `hand_abbreviation` | character |  |
| `hand_display_value` | character |  |
| `age` | integer |  |
| `date_of_birth` | character |  |
| `starter` | logical |  |
| `jersey_right` | character |  |
| `valid` | logical |  |
| `did_not_play` | logical |  |
| `display_name` | character |  |
| `athlete_href` | character |  |
| `position_href` | character |  |
| `statistics_href` | character |  |
| `team_id` | integer |  |
| `order` | integer |  |
| `home_away` | character |  |
| `winner` | logical |  |
| `team_guid` | character |  |
| `team_uid` | character |  |
| `team_slug` | character |  |
| `team_location` | character |  |
| `team_name` | character |  |
| `team_nickname` | character |  |
| `team_abbreviation` | character |  |
| `team_display_name` | character |  |
| `team_short_display_name` | character |  |
| `team_color` | character |  |
| `team_alternate_color` | character |  |
| `is_active` | logical |  |
| `is_all_star` | logical |  |
| `team_alternate_ids_sdr` | character |  |
| `logo_href` | character |  |
| `logo_dark_href` | character |  |
| `game_id` | integer |  |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)

Pandas round-trip::

rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()

Pipeline next step (filter to game starters)::

import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

### `espn_cfb_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull ESPN per-play participants for a college-football game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts (after following pagination) before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | If True (default), athletes that the ``cdn.espn.com`` sidecar omits are fetched one-by-one from their canonical ESPN ``$ref`` URL so the resulting frame has populated ``*_player_name`` / ``*_player_names`` columns wherever an ``*_player_id`` is non-null. Setting this to False skips the extra HTTP fan-out and reproduces the pre-enhancement behavior — rows may then ship with ``*_player_id`` populated but ``*_player_name`` null on the handful of athletes the sidecar misses (most visible on split sacks, multi-lateral returns, and older games). |
| `resolve_missing_max` | `int` | `50` | Hard cap on the number of per-athlete ``$ref`` requests issued for a single game. Defaults to 50, which comfortably covers every probed game (typical max is ≤8 unique missing athletes). If breached, a warning is logged and the remaining missing athletes are left with null names. Ignored when ``resolve_missing=False``. |

**Returns**

Polars (or pandas) DataFrame, one row per play. Columns include ``game_id``, ``play_id``, and TWO column families for every participant ``type`` ESPN ships for the game (typical types: ``passer``, ``rusher``, ``receiver``, ``tackler``, ``sacked_by``, ``forced_by``, ``pass_defender``, ``kicker``, ``punter``, ``returner``, ``recoverer``, ``scorer``, ``pat_scorer``, ``penalized``, ``assisted_by``): * **Scalar** — ``{type}_player_id`` / ``{type}_player_name``: the first occurrence of that participant type on the play. Backwards compatible with the legacy regex-extractor shape. * **List** — ``{type}_player_ids`` / ``{type}_player_names``: ``List(Utf8)`` columns containing **every** occurrence of that participant type on the play, in the order ESPN shipped them. Plays with no participant of a given type carry an empty list ``[]`` (not null) for downstream consumption simplicity. This family preserves multi-entry participant types (split sacks where ESPN ships two ``sackedBy`` entries, multi-tacklers, etc.) that the scalar family collapses to first-only. If ``raw=True``, returns the parsed JSON list of play dicts.

| col_name | type | description |
|---|---|---|
| `game_id` | integer |  |
| `play_id` | integer |  |
| `kicker_player_name` | character |  |
| `passer_player_name` | character |  |
| `receiver_player_name` | character |  |
| `rusher_player_name` | character |  |
| `scorer_player_name` | character |  |
| `returner_player_name` | character |  |
| `pass_defender_player_name` | character |  |
| `penalized_player_name` | character |  |
| `sacked_by_player_name` | character |  |
| `pat_scorer_player_name` | character |  |
| `punter_player_name` | character |  |
| `kicker_player_id` | character |  |
| `passer_player_id` | character |  |
| `receiver_player_id` | character |  |
| `rusher_player_id` | character |  |
| `scorer_player_id` | character |  |
| `returner_player_id` | character |  |
| `pass_defender_player_id` | character |  |
| `penalized_player_id` | character |  |
| `sacked_by_player_id` | character |  |
| `pat_scorer_player_id` | character |  |
| `punter_player_id` | character |  |
| `kicker_player_names` | character |  |
| `passer_player_names` | character |  |
| `receiver_player_names` | character |  |
| `rusher_player_names` | character |  |
| `scorer_player_names` | character |  |
| `returner_player_names` | character |  |
| `pass_defender_player_names` | character |  |
| `penalized_player_names` | character |  |
| `sacked_by_player_names` | character |  |
| `pat_scorer_player_names` | character |  |
| `punter_player_names` | character |  |
| `kicker_player_ids` | character |  |
| `passer_player_ids` | character |  |
| `receiver_player_ids` | character |  |
| `rusher_player_ids` | character |  |
| `scorer_player_ids` | character |  |
| `returner_player_ids` | character |  |
| `pass_defender_player_ids` | character |  |
| `penalized_player_ids` | character |  |
| `sacked_by_player_ids` | character |  |
| `pat_scorer_player_ids` | character |  |
| `punter_player_ids` | character |  |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_play_participants
participants = espn_cfb_play_participants(game_id=401628334)
print(participants.shape)

Skip the per-athlete fan-out for speed::

participants_fast = espn_cfb_play_participants(
    game_id=401628334,
    resolve_missing=False,
)

Pipeline next step (join onto play-by-play frame)::

from sportsdataverse.cfb import CFBPlayProcess
pbp = CFBPlayProcess(gameId=401628334).espn_cfb_pbp()
plays = pbp["plays"]
joined = plays.join(participants, how="left", left_on="id", right_on="play_id")
```

### `espn_cfb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a college-football athlete's ESPN **season** stat line.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns (for football: ``passing_*``, ``rushing_*``, ``receiving_*``, ``scoring_*``, ...), the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer multi-category web-v3 payload use :func:`sportsdataverse.cfb.espn_cfb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN college-football athlete identifier. |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When ``raw=True`` returns the raw statistics JSON ``dict``.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `season_type` | character |  |
| `total` | logical |  |
| `athlete_id` | integer |  |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `full_name` | character |  |
| `display_name` | character |  |
| `short_name` | character |  |
| `weight` | double |  |
| `display_weight` | character |  |
| `height` | double |  |
| `display_height` | character |  |
| `age` | integer |  |
| `date_of_birth` | character |  |
| `jersey` | character |  |
| `slug` | character |  |
| `active` | logical |  |
| `position_id` | integer |  |
| `position_name` | character |  |
| `position_display_name` | character |  |
| `position_abbreviation` | character |  |
| `college_name` | character |  |
| `status_id` | integer |  |
| `status_name` | character |  |
| `general_fumbles` | double |  |
| `general_fumbles_lost` | double |  |
| `general_fumbles_touchdowns` | double |  |
| `general_games_played` | double |  |
| `general_offensive_two_pt_returns` | double |  |
| `general_offensive_fumbles_touchdowns` | double |  |
| `general_defensive_fumbles_touchdowns` | double |  |
| `passing_avg_gain` | double |  |
| `passing_completion_pct` | double |  |
| `passing_completions` | double |  |
| `passing_espnqb_rating` | double |  |
| `passing_interception_pct` | double |  |
| `passing_interceptions` | double |  |
| `passing_long_passing` | double |  |
| `passing_net_passing_yards` | double |  |
| `passing_net_passing_yards_per_game` | double |  |
| `passing_net_total_yards` | double |  |
| `passing_net_yards_per_game` | double |  |
| `passing_passing_attempts` | double |  |
| `passing_passing_big_plays` | double |  |
| `passing_passing_first_downs` | double |  |
| `passing_passing_fumbles` | double |  |
| `passing_passing_fumbles_lost` | double |  |
| `passing_passing_touchdown_pct` | double |  |
| `passing_passing_touchdowns` | double |  |
| `passing_passing_yards` | double |  |
| `passing_passing_yards_after_catch` | double |  |
| `passing_passing_yards_at_catch` | double |  |
| `passing_passing_yards_per_game` | double |  |
| `passing_qb_rating` | double |  |
| `passing_sacks` | double |  |
| `passing_sack_yards_lost` | double |  |
| `passing_team_games_played` | double |  |
| `passing_total_offensive_plays` | double |  |
| `passing_total_points_per_game` | double |  |
| `passing_total_touchdowns` | double |  |
| `passing_total_yards` | double |  |
| `passing_total_yards_from_scrimmage` | double |  |
| `passing_two_point_pass_convs` | double |  |
| `passing_two_pt_pass` | double |  |
| `passing_two_pt_pass_attempts` | double |  |
| `passing_yards_from_scrimmage_per_game` | double |  |
| `passing_yards_per_completion` | double |  |
| `passing_yards_per_game` | double |  |
| `passing_yards_per_pass_attempt` | double |  |
| `passing_net_yards_per_pass_attempt` | double |  |
| `passing_qbr` | double |  |
| `passing_adj_qbr` | double |  |
| `passing_quarterback_rating` | double |  |
| `rushing_avg_gain` | double |  |
| `rushing_espnrb_rating` | double |  |
| `rushing_long_rushing` | double |  |
| `rushing_net_total_yards` | double |  |
| `rushing_net_yards_per_game` | double |  |
| `rushing_rushing_attempts` | double |  |
| `rushing_rushing_big_plays` | double |  |
| `rushing_rushing_first_downs` | double |  |
| `rushing_rushing_fumbles` | double |  |
| `rushing_rushing_fumbles_lost` | double |  |
| `rushing_rushing_touchdowns` | double |  |
| `rushing_rushing_yards` | double |  |
| `rushing_rushing_yards_per_game` | double |  |
| `rushing_stuffs` | double |  |
| `rushing_stuff_yards_lost` | double |  |
| `rushing_team_games_played` | double |  |
| `rushing_total_offensive_plays` | double |  |
| `rushing_total_points_per_game` | double |  |
| `rushing_total_touchdowns` | double |  |
| `rushing_total_yards` | double |  |
| `rushing_total_yards_from_scrimmage` | double |  |
| `rushing_two_point_rush_convs` | double |  |
| `rushing_two_pt_rush` | double |  |
| `rushing_two_pt_rush_attempts` | double |  |
| `rushing_yards_from_scrimmage_per_game` | double |  |
| `rushing_yards_per_game` | double |  |
| `rushing_yards_per_rush_attempt` | double |  |
| `receiving_avg_gain` | double |  |
| `receiving_espnwr_rating` | double |  |
| `receiving_long_reception` | double |  |
| `receiving_net_total_yards` | double |  |
| `receiving_net_yards_per_game` | double |  |
| `receiving_receiving_big_plays` | double |  |
| `receiving_receiving_first_downs` | double |  |
| `receiving_receiving_fumbles` | double |  |
| `receiving_receiving_fumbles_lost` | double |  |
| `receiving_receiving_targets` | double |  |
| `receiving_receiving_touchdowns` | double |  |
| `receiving_receiving_yards` | double |  |
| `receiving_receiving_yards_after_catch` | double |  |
| `receiving_receiving_yards_at_catch` | double |  |
| `receiving_receiving_yards_per_game` | double |  |
| `receiving_receptions` | double |  |
| `receiving_team_games_played` | double |  |
| `receiving_total_offensive_plays` | double |  |
| `receiving_total_points_per_game` | double |  |
| `receiving_total_touchdowns` | double |  |
| `receiving_total_yards` | double |  |
| `receiving_total_yards_from_scrimmage` | double |  |
| `receiving_two_point_rec_convs` | double |  |
| `receiving_two_pt_reception` | double |  |
| `receiving_two_pt_reception_attempts` | double |  |
| `receiving_yards_from_scrimmage_per_game` | double |  |
| `receiving_yards_per_game` | double |  |
| `receiving_yards_per_reception` | double |  |
| `scoring_defensive_points` | double |  |
| `scoring_field_goals` | double |  |
| `scoring_kick_extra_points` | double |  |
| `scoring_kick_extra_points_made` | double |  |
| `scoring_misc_points` | double |  |
| `scoring_passing_touchdowns` | double |  |
| `scoring_receiving_touchdowns` | double |  |
| `scoring_return_touchdowns` | double |  |
| `scoring_rushing_touchdowns` | double |  |
| `scoring_total_points` | double |  |
| `scoring_total_points_per_game` | double |  |
| `scoring_total_touchdowns` | double |  |
| `scoring_total_two_point_convs` | double |  |
| `scoring_two_point_pass_convs` | double |  |
| `scoring_two_point_rec_convs` | double |  |
| `scoring_two_point_rush_convs` | double |  |
| `scoring_one_pt_safeties_made` | double |  |
| `team_id` | integer |  |
| `team_uid` | character |  |
| `team_guid` | character |  |
| `team_slug` | character |  |
| `team_location` | character |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_display_name` | character |  |
| `team_short_display_name` | character |  |
| `team_color` | character |  |
| `team_alternate_color` | character |  |
| `team_is_active` | logical |  |
| `team_logo_href` | character |  |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_player_stats
df = espn_cfb_player_stats(athlete_id=4426338, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_schedule - look up the college football schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `week` | `int` | `None` | Week of the schedule. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `groups` | `int` | `None` | Used to define different divisions. 80 is FBS, 81 is FCS. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `uid` | character |  |
| `date` | character |  |
| `attendance` | integer |  |
| `time_valid` | logical |  |
| `date_valid` | logical |  |
| `neutral_site` | logical |  |
| `conference_competition` | logical |  |
| `play_by_play_available` | logical |  |
| `recent` | logical |  |
| `start_date` | character |  |
| `broadcast` | character |  |
| `highlights` | character |  |
| `notes_type` | character |  |
| `notes_headline` | character |  |
| `broadcast_market` | character |  |
| `broadcast_name` | character |  |
| `type_id` | character |  |
| `type_abbreviation` | character |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_address_city` | character |  |
| `venue_address_country` | character |  |
| `venue_indoor` | logical |  |
| `status_clock` | double |  |
| `status_display_clock` | character |  |
| `status_period` | integer |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `format_regulation_periods` | integer |  |
| `home_id` | character |  |
| `home_uid` | character |  |
| `home_location` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_short_display_name` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_is_active` | logical |  |
| `home_venue_id` | character |  |
| `home_logo` | character |  |
| `home_conference_id` | character |  |
| `home_score` | character |  |
| `home_current_rank` | integer |  |
| `home_linescores` | integer |  |
| `home_records` | character |  |
| `away_id` | character |  |
| `away_uid` | character |  |
| `away_location` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_short_display_name` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_is_active` | logical |  |
| `away_venue_id` | character |  |
| `away_logo` | character |  |
| `away_conference_id` | character |  |
| `away_score` | character |  |
| `away_current_rank` | integer |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer |  |
| `season` | integer |  |
| `season_type` | integer |  |
| `week` | integer |  |
| `venue_address_state` | character |  |
| `groups_id` | character |  |
| `groups_name` | character |  |
| `groups_short_name` | character |  |
| `groups_is_conference` | logical |  |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
slate = espn_cfb_schedule()
print(slate.shape if slate is not None else "no games")

Pull a specific week of FBS games::

week5 = espn_cfb_schedule(dates=2023, week=5, season_type=2)

Pipeline next step (extract finals only)::

import polars as pl
finals = espn_cfb_schedule(dates=2023, week=5).filter(
    pl.col("status_type_completed") == True
)
```

## Dataset loaders

### `load_cfb_betting_lines(return_as_pandas=False) -> 'pl.DataFrame'`

Load college football betting lines information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing betting lines available for the available seasons.

| col_name | type | description |
|---|---|---|
| `id` | double |  |
| `game_id` | integer |  |
| `season` | double |  |
| `game_desc` | character |  |
| `date_time` | character |  |
| `market_type` | character |  |
| `abbr` | character |  |
| `lines` | double |  |
| `odds` | integer |  |
| `opening_lines` | double |  |
| `opening_odds` | integer |  |
| `book` | character |  |
| `season_type` | character |  |
| `week` | integer |  |

**Example**

```python
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)

Pandas round-trip::

lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()

Pipeline next step (filter to one provider in 2023)::

import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

## Utilities & helpers

### `CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, odds_override=None, **kwargs)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` |  | `0` |  |
| `raw` |  | `False` |  |
| `path_to_json` |  | `'/'` |  |
| `return_keys` |  | `None` |  |
| `odds_override` |  | `None` |  |

### `most_recent_cfb_season()`

Return the most recent college football season year based on today's date.

The college football season starts in mid-August. If today is on or after August 15 (or any day in September or later), this returns the current calendar year. Otherwise, it returns the previous calendar year.

**Returns**

The most recent CFB season year.

**Example**

```python
from sportsdataverse.cfb import most_recent_cfb_season
year = most_recent_cfb_season()
print(year)

Combine with the loaders for a "current season" pull::

from sportsdataverse.cfb import load_cfb_schedule, most_recent_cfb_season
sched = load_cfb_schedule(seasons=[most_recent_cfb_season()])
```

## Other

### `espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_teams - look up the college football teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 80 is FBS, 81 is FCS. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.cfb.espn_cfb_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character |  |
| `team_alternate_color` | character |  |
| `team_color` | character |  |
| `team_display_name` | character |  |
| `team_id` | character |  |
| `team_is_active` | logical |  |
| `team_is_all_star` | logical |  |
| `team_location` | character |  |
| `team_logos` | integer |  |
| `team_name` | character |  |
| `team_nickname` | character |  |
| `team_short_display_name` | character |  |
| `team_slug` | character |  |
| `team_uid` | character |  |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)

Pull FCS teams (group 81)::

fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()

Pipeline next step (build an abbreviation lookup)::

teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### `get_cfb_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load college football team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_id` | integer |  |
| `school` | character |  |
| `mascot` | character |  |
| `abbreviation` | character |  |
| `alt_name1` | character |  |
| `alt_name2` | character |  |
| `alt_name3` | character |  |
| `conference` | character |  |
| `division` | character |  |
| `color` | character |  |
| `alt_color` | character |  |
| `logo` | character |  |
| `logo_dark` | character |  |

**Example**

```python
from sportsdataverse.cfb import get_cfb_teams
teams = get_cfb_teams()
print(teams.shape)

Pandas round-trip::

teams_pd = get_cfb_teams(return_as_pandas=True)
teams_pd.head()

Pipeline next step (build a team_id to logo URL map)::

teams = get_cfb_teams()
logo_map = dict(zip(teams["team_id"], teams["logo"]))
```

### `scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN scoreboard event dict into a shape

suitable for ``pd.json_normalize``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard ``events[*]`` entry from the ESPN college-football scoreboard API. |

**Returns**

The same event dict, mutated in place with ``home``/``away`` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```
