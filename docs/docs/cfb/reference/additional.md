---
title: CFB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# CFB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.cfb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_cfb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_cfb_game_rosters}

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
| `athlete_id` | integer | ESPN athlete id. |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `athlete_display_name` | character | Player display name; `athlete_detail = TRUE` only. |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `weight` | double | Listed weight (lbs). |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Listed height (inches). |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `slug` | character | URL slug for the team. |
| `jersey` | character | Jersey number. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_country_alternate_id` | character |  |
| `birth_country_abbreviation` | character | Birth country abbreviation. |
| `headshot_href` | character | URL of the athlete headshot image. |
| `headshot_alt` | character | Alternative-text label for the headshot. |
| `flag_href` | character |  |
| `flag_alt` | character |  |
| `flag_rel` | character |  |
| `experience_years` | integer | Years of experience. |
| `experience_display_value` | character | Experience display value. |
| `experience_abbreviation` | character | Experience abbreviation. |
| `status_id` | character | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Player date of birth (if published). |
| `starter` | logical | `TRUE` if the athlete started the game. |
| `jersey_right` | character |  |
| `valid` | logical | `TRUE` if the roster entry is flagged valid by ESPN. |
| `did_not_play` | logical | `TRUE` if the athlete did not play. |
| `display_name` | character | Human-readable metric name. |
| `athlete_href` | character |  |
| `position_href` | character |  |
| `statistics_href` | character |  |
| `team_id` | integer | ESPN team id. |
| `order` | integer | Team order within the competition (0 = first). |
| `home_away` | character | `home` or `away`. |
| `winner` | logical | `TRUE` if this team won the game. |
| `team_guid` | character | ESPN team GUID. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | character | Team slug for the stat row. |
| `team_location` | character | Team location / school name; `team_detail = TRUE` only. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_nickname` | character | Team nickname label; `team_detail = TRUE` only. |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `team_display_name` | character | Full team display name; `team_detail = TRUE` only. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_alternate_color` | character | Alternate team color; `team_detail = TRUE` only. |
| `is_active` | logical | Whether the team is currently active. |
| `is_all_star` | logical | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | character |  |
| `logo_href` | character | URL of the default team logo. |
| `logo_dark_href` | character | URL of the dark-variant team logo. |
| `game_id` | integer | ESPN game identifier. |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)

# Pandas round-trip

rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()

# Pipeline next step (filter to game starters)

import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

### `espn_cfb_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_cfb_play_participants}

Pull ESPN per-play participants for a college-football game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts (after following pagination) before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | If True (default), athletes that the `cdn.espn.com` sidecar omits are fetched one-by-one from their canonical ESPN `$ref` URL so the resulting frame has populated `*_player_name` / `*_player_names` columns wherever an `*_player_id` is non-null. Setting this to False skips the extra HTTP fan-out and reproduces the pre-enhancement behavior — rows may then ship with `*_player_id` populated but `*_player_name` null on the handful of athletes the sidecar misses (most visible on split sacks, multi-lateral returns, and older games). |
| `resolve_missing_max` | `int` | `50` | Hard cap on the number of per-athlete `$ref` requests issued for a single game. Defaults to 50, which comfortably covers every probed game (typical max is ≤8 unique missing athletes). If breached, a warning is logged and the remaining missing athletes are left with null names. Ignored when `resolve_missing=False`. |

**Returns**

Polars (or pandas) DataFrame, one row per play. Columns include `game_id`, `play_id`, and TWO column families for every participant `type` ESPN ships for the game (typical types: `passer`, `rusher`, `receiver`, `tackler`, `sacked_by`, `forced_by`, `pass_defender`, `kicker`, `punter`, `returner`, `recoverer`, `scorer`, `pat_scorer`, `penalized`, `assisted_by`): * **Scalar** — `{type}_player_id` / `{type}_player_name`: the first occurrence of that participant type on the play. Backwards compatible with the legacy regex-extractor shape. * **List** — `{type}_player_ids` / `{type}_player_names`: `List(Utf8)` columns containing **every** occurrence of that participant type on the play, in the order ESPN shipped them. Plays with no participant of a given type carry an empty list `[]` (not null) for downstream consumption simplicity. This family preserves multi-entry participant types (split sacks where ESPN ships two `sackedBy` entries, multi-tacklers, etc.) that the scalar family collapses to first-only. If `raw=True`, returns the parsed JSON list of play dicts.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game identifier. |
| `play_id` | integer | ESPN play id. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `passer_player_name` | character | Name of the passer on a passing play. |
| `receiver_player_name` | character | Name of the receiver on a passing play. |
| `rusher_player_name` | character | Name of the rusher on a rushing play. |
| `scorer_player_name` | character |  |
| `returner_player_name` | character |  |
| `pass_defender_player_name` | character |  |
| `penalized_player_name` | character |  |
| `sacked_by_player_name` | character |  |
| `pat_scorer_player_name` | character |  |
| `punter_player_name` | character | Name of the punter. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `scorer_player_id` | character |  |
| `returner_player_id` | character |  |
| `pass_defender_player_id` | character |  |
| `penalized_player_id` | character |  |
| `sacked_by_player_id` | character |  |
| `pat_scorer_player_id` | character |  |
| `punter_player_id` | character | Unique identifier for the punter. |
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

# Skip the per-athlete fan-out for speed

participants_fast = espn_cfb_play_participants(
    game_id=401628334,
    resolve_missing=False,
)

# Pipeline next step (join onto play-by-play frame)

from sportsdataverse.cfb import CFBPlayProcess
pbp = CFBPlayProcess(gameId=401628334).espn_cfb_pbp()
plays = pbp["plays"]
joined = plays.join(participants, how="left", left_on="id", right_on="play_id")
```

### `espn_cfb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_cfb_player_stats}

Pull a college-football athlete's ESPN **season** stat line.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns (for football: `passing_*`, `rushing_*`, `receiving_*`,
`scoring_*`, ...), the athlete / team metadata blocks, and the
`season_type` / `total` parameters. For the richer multi-category
web-v3 payload use `sportsdataverse.cfb.espn_cfb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN college-football athlete identifier. |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `season_type` | character | ESPN season type (2 = regular, 3 = postseason). |
| `total` | logical | Total. |
| `athlete_id` | integer | ESPN athlete id. |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `display_name` | character | Human-readable metric name. |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `weight` | double | Listed weight (lbs). |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Listed height (inches). |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Player date of birth (if published). |
| `jersey` | character | Jersey number. |
| `slug` | character | URL slug for the team. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `position_id` | integer | ESPN position id. |
| `position_name` | character | Position name (e.g. `Quarterback`); `position_detail = TRUE` only. |
| `position_display_name` | character | Human-readable position name; `position_detail = TRUE` only. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `college_name` | character | College name. |
| `status_id` | integer | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `general_fumbles` | double |  |
| `general_fumbles_lost` | double |  |
| `general_fumbles_touchdowns` | double |  |
| `general_games_played` | double | Games Played. |
| `general_offensive_two_pt_returns` | double |  |
| `general_offensive_fumbles_touchdowns` | double |  |
| `general_defensive_fumbles_touchdowns` | double |  |
| `passing_avg_gain` | double |  |
| `passing_completion_pct` | double |  |
| `passing_completions` | double | Pass completions (split from CFBD's `C/ATT` field). |
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
| `passing_qbr` | double | ESPN Quarterback Rating (QBR) for the player in this game. |
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
| `team_id` | integer | ESPN team id. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | character | ESPN team GUID. |
| `team_slug` | character | Team slug for the stat row. |
| `team_location` | character | Team location / school name; `team_detail = TRUE` only. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `team_display_name` | character | Full team display name; `team_detail = TRUE` only. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_alternate_color` | character | Alternate team color; `team_detail = TRUE` only. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_logo_href` | character | Default team logo URL; `team_detail = TRUE` only. |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_player_stats
df = espn_cfb_player_stats(athlete_id=4426338, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_cfb_schedule}

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
| `id` | character | 247Sports referencing id for the recruit. |
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Date of the poll release. |
| `attendance` | integer | Reported attendance at the game. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `date_valid` | logical |  |
| `neutral_site` | logical | TRUE/FALSE flag for if the game took place at a neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Whether the game is recent. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |
| `broadcast` | character | Broadcast network short name. |
| `highlights` | character | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Play-type id. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `venue_id` | character | Referencing venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_country` | character |  |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current period. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `format_regulation_periods` | integer | Format regulation periods. |
| `home_id` | character | Home team referencing id. |
| `home_uid` | character | Home team's uid. |
| `home_location` | character | Home team's location. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_short_display_name` | character | Home short display name. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_is_active` | logical | Home team's is active. |
| `home_venue_id` | character | Unique identifier for home venue. |
| `home_logo` | character | Home team logo URL. |
| `home_conference_id` | character | Unique identifier for home conference. |
| `home_score` | character | Home-team score after the play. |
| `home_current_rank` | integer |  |
| `home_linescores` | integer |  |
| `home_records` | character |  |
| `away_id` | character | Away team referencing id. |
| `away_uid` | character | Away team's uid. |
| `away_location` | character | Away team's location. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_short_display_name` | character | Away short display name. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_is_active` | logical | Away team's is active. |
| `away_venue_id` | character | Unique identifier for away venue. |
| `away_logo` | character | Away team logo URL. |
| `away_conference_id` | character | Unique identifier for away conference. |
| `away_score` | character | Away-team score after the play. |
| `away_current_rank` | integer |  |
| `away_linescores` | integer |  |
| `away_records` | character |  |
| `game_id` | integer | ESPN game identifier. |
| `season` | integer | Season (4-digit year). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |
| `venue_address_state` | character | Venue address state / region. |
| `groups_id` | character | Unique identifier for groups. |
| `groups_name` | character | Groups name. |
| `groups_short_name` | character | Groups short name. |
| `groups_is_conference` | logical | Groups is conference. |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
slate = espn_cfb_schedule()
print(slate.shape if slate is not None else "no games")

# Pull a specific week of FBS games

week5 = espn_cfb_schedule(dates=2023, week=5, season_type=2)

# Pipeline next step (extract finals only)

import polars as pl
finals = espn_cfb_schedule(dates=2023, week=5).filter(
    pl.col("status_type_completed") == True
)
```

## Dataset loaders

### `load_cfb_betting_lines(return_as_pandas=False) -> 'pl.DataFrame'` {#load_cfb_betting_lines}

Load college football betting lines information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing betting lines available for the available seasons.

| col_name | type | description |
|---|---|---|
| `id` | double | 247Sports referencing id for the recruit. |
| `game_id` | integer | ESPN game identifier. |
| `season` | double | Season (4-digit year). |
| `game_desc` | character |  |
| `date_time` | character |  |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `abbr` | character |  |
| `lines` | double |  |
| `odds` | integer |  |
| `opening_lines` | double |  |
| `opening_odds` | integer |  |
| `book` | character |  |
| `season_type` | character | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)

# Pandas round-trip

lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()

# Pipeline next step (filter to one provider in 2023)

import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

### `load_cfb_rosters_crosswalk(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_cfb_rosters_crosswalk}

Load the current ESPN x Fox CFB rosters crosswalk (single snapshot).

Unlike the per-season `load_cfb_teams_crosswalk` / `load_cfb_schedule_crosswalk`
loaders, this one is **season-less**: ESPN's and Fox's team-roster endpoints
only expose the *current* roster, so the published artifact is a single
snapshot rather than a historical per-season series. It is built by
`cfbfastR-cfb-data`'s `scripts/build_cfb_crosswalk.py` (which fans the
per-team `sportsdataverse.cfb.cfb_rosters_crosswalk` builder out over
the current season's ESPN<->Fox team-id pairs) and refreshed on that repo's
cadence.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

one row per matched player, carrying `espn_team_id` / `fox_team_id` provenance plus each provider's athlete id, name, jersey, position, and the `match_method` / `matched_sources` flags.

**Example**

```python
from sportsdataverse.cfb import load_cfb_rosters_crosswalk
xwalk = load_cfb_rosters_crosswalk()
print(xwalk.shape)

# Pandas round-trip

xwalk_pd = load_cfb_rosters_crosswalk(return_as_pandas=True)

# Pipeline next step (one team's ESPN<->Fox athlete map)

import polars as pl
osu = load_cfb_rosters_crosswalk().filter(pl.col("espn_team_id") == 194)
```

## Utilities & helpers

### `CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, odds_override=None, **kwargs)` {#CFBPlayProcess}

Process ESPN college-football play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `playbyplay` / `summary` endpoints (or a local JSON dump)
and pipes the result through a chain of feature-engineering steps --
down/distance, play-type flags, EPA, WPA, QBR, drive aggregation, and an
advanced box score. Use `run_processing_pipeline()` for the full feature
set or `run_cleaning_pipeline()` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` |  | `0` | ESPN game id. |
| `raw` |  | `False` | if True, espn_cfb_pbp() returns the (allowlisted) summary verbatim. |
| `path_to_json` |  | `'/'` | directory for cfb_pbp_disk() offline loads. |
| `return_keys` |  | `None` | optional subset of result keys to return. |
| `odds_override` |  | `None` | optional dict {gameSpread, overUnder, homeFavorite, gameSpreadAvailable} that short-circuits odds resolution (sets odds_source="injected") so offline rebuilds never hit the live core-odds endpoint or fall back to defaults. Validated + coerced here. |

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
proc = CFBPlayProcess(gameId=401628334)
proc.espn_cfb_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

# Offline replay from a JSON dump

proc = CFBPlayProcess(gameId=401628334, path_to_json="./pbp_dump")
proc.cfb_pbp_disk()
result = proc.run_processing_pipeline()
```

**Methods**

#### `CFBPlayProcess.cfb_pbp_disk()`

Load a previously cached ESPN summary JSON for this game from disk.

Reads `{path_to_json}/{gameId}.json` where `path_to_json` was passed
to the `CFBPlayProcess` constructor.

**Returns**

Parsed JSON contents, also stored on `self.json`.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334, path_to_json="./cache")
pbp = game.cfb_pbp_disk()
print(list(pbp.keys()))
```

#### `CFBPlayProcess.cfb_pbp_json(**kwargs)`

Return the JSON payload currently attached to this `CFBPlayProcess`

instance.

**Returns**

The cached JSON payload (`self.json`).

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cached = game.cfb_pbp_json()
```

#### `CFBPlayProcess.corrupt_pbp_check()`

Heuristic check for corrupt or incomplete play-by-play.

Flags games with zero plays, fewer than 50 plays for a completed game,
or more than 500 plays for a completed game -- all of which historically
indicate ESPN delivered a malformed PBP payload that should not be
processed downstream.

**Returns**

True if PBP looks corrupt and the processing pipeline should be skipped, False otherwise.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
if not game.corrupt_pbp_check():
    game.run_processing_pipeline()
```

#### `CFBPlayProcess.create_box_score(play_df)`

Build a per-team and per-player advanced box score from a processed

plays frame.

Triggers `run_processing_pipeline` first if it hasn't already run,
so the input `play_df` is expected to be the post-pipeline plays frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced by `run_processing_pipeline` (with EPA, WPA and play-type flags already populated). |

**Returns**

Box-score sections, each a list of records — `"pass"` / `"rush"` / `"receiver"` (per-player advanced + EPA lines), `"team"` and `"situational"` (per-team), `"defensive"` and `"defensive_players"` (team- and player-level havoc), `"specialists"` (kicking / punting / return players), `"turnover"`, `"drives"`, and the ESPN-sourced `"espn_team"` / `"espn_players"` totals.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
box = game.create_box_score(game.plays_json)
print(list(box.keys()))
```

#### `CFBPlayProcess.espn_cfb_pbp(**kwargs)`

espn_cfb_pbp() - Pull the game by id. Data from API endpoints: `college-football/playbyplay`,

`college-football/summary`

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder", "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
pbp = game.espn_cfb_pbp()
print(list(pbp.keys()))

# Pull only the raw ESPN summary payload (skip cleaning)

raw_pbp = CFBPlayProcess(gameId=401628334, raw=True).espn_cfb_pbp()

# Pipeline next step (run the full processing pipeline for advanced features)

game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()  # adds EPA, WPA, box score
```

#### `CFBPlayProcess.run_cleaning_pipeline()`

Run the lighter cleaning pipeline (no EPA/WPA/QBR/box-score).

Same per-play feature engineering as `run_processing_pipeline`
through add_spread_time`, but stops short of the modeling steps.
Use this when you only need cleaned plays and don't need expected
points or win probability columns.

**Returns**

Cleaned game payload (no `advBoxScore` key).

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cleaned = game.run_cleaning_pipeline()
print(len(cleaned["plays"]))
```

#### `CFBPlayProcess.run_processing_pipeline()`

Run the full play-by-play processing pipeline.

Applies every scoring/feature step in order: down detection, play type
flags, rush/pass flags, team score variables, new play types, penalty
setup, play category flags, yardage cols, player cols, after cols,
spread time, EPA, WPA, drive data, and QBR. Also produces an advanced
box score and stores it under `advBoxScore` on the returned dict.

Idempotent -- subsequent calls return the cached `self.json`.

**Returns**

The fully-processed game payload. If the constructor was given `return_keys`, only those keys are returned.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
print(processed["advBoxScore"].keys())

# Pipeline next step (return only selected keys)

game = CFBPlayProcess(gameId=401628334, return_keys=["plays", "advBoxScore"])
game.espn_cfb_pbp()
trimmed = game.run_processing_pipeline()
```

### `most_recent_cfb_season()` {#most_recent_cfb_season}

Return the most recent college football season year based on today's date.

The college football season starts in mid-August. If today is on or after
August 15 (or any day in September or later), this returns the current
calendar year. Otherwise, it returns the previous calendar year.

**Returns**

The most recent CFB season year.

**Example**

```python
from sportsdataverse.cfb import most_recent_cfb_season
year = most_recent_cfb_season()
print(year)

# Combine with the loaders for a "current season" pull

from sportsdataverse.cfb import load_cfb_schedule, most_recent_cfb_season
sched = load_cfb_schedule(seasons=[most_recent_cfb_season()])
```

## Other

### `cfb_odds_events_crosswalk(season: 'Optional[int]' = None, week: 'Optional[int]' = None, *, sport: 'str' = 'americanfootball_ncaaf', api_key: 'Optional[str]' = None, season_type: 'int' = 2, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_odds_events_crosswalk}

Match The Odds API CFB events to ESPN game ids.

Pulls the upcoming/live events for `sport` from The Odds API and the ESPN
scoreboard for `(season, week)`, then joins them on the order-independent
team matchup so each odds event id maps to its ESPN `event` id. Because
The Odds API only lists near-term events, this is most useful for the
current/upcoming week.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | ESPN season year for the schedule side. Defaults to the most recent CFB season. |
| `week` | `Optional[int]` | `None` | ESPN schedule week. When `None`, ESPN returns its default (current) slate. |
| `sport` | `str` | `'americanfootball_ncaaf'` | The Odds API sport key. Defaults to `"americanfootball_ncaaf"`. |
| `api_key` | `Optional[str]` | `None` | The Odds API key; falls back to the `ODDS_API_KEY` env var. |
| `season_type` | `int` | `2` | ESPN season type (`2` regular, `3` post-season). Defaults to `2`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`), one row per odds event, with columns `matchup_key`, `odds_event_id`, `espn_game_id`, `home_team`, `away_team`, `commence_time`, `espn_date`, `matched_sources`.

**Example**

```python
from sportsdataverse.cfb import cfb_odds_events_crosswalk
xwalk = cfb_odds_events_crosswalk(season=2024, week=5)
matched = xwalk.filter(pl.col("espn_game_id").is_not_null())
```

### `cfb_rosters_crosswalk(espn_team_id: 'Union[int, str]', fox_team_id: 'Union[int, str]', *, season: 'Optional[int]' = None, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_rosters_crosswalk}

Build the ESPN x Fox x Yahoo player-id crosswalk for one team.

Fetches the selected providers' players for the team, matches them on
normalized name (with jersey as a confidence signal), and returns each
player's ESPN, Fox, and Yahoo athlete ids side by side. Use
`cfb_teams_crosswalk` first to translate an ESPN team id into the
matching Fox team id.

ESPN and Fox provide full rosters, so the default is `("espn", "fox")`.
**Yahoo is opt-in** (pass `providers=("espn", "fox", "yahoo")`) because it
has no roster endpoint — its only player feed is the season stat-leaderboard
(`sportsdataverse.cfb.yahoo_cfb_player_season_stats`), which is the
league's top ~200 players (roughly one per team) and frequently includes no
player for a given team at all. When selected, the team is resolved by
matching Yahoo's (abbreviated) team name against the ESPN team's name; if it
can't be resolved, the Yahoo columns are simply null.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn_team_id` | `Union[int, str]` |  | ESPN team id (e.g. `194` for Ohio State). |
| `fox_team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `25` for Ohio State). |
| `season` | `Optional[int]` | `None` | Season year for the Yahoo player-stats leg. Defaults to the most recent CFB season. Unused when Yahoo isn't selected. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses `("espn", "fox")`; add `"yahoo"` explicitly for its (sparse) leg, or pass a single source. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `person_key`, `espn_athlete_id`, `fox_athlete_id`, `yahoo_athlete_id`, `name`, `espn_jersey`, `fox_jersey`, `espn_position`, `fox_position`, `yahoo_position`, `match_method`, `matched_sources`. `match_method` reflects the ESPN/Fox jersey agreement: `name_jersey` (agree), `name` (name only), `name_jersey_conflict` (jerseys differ — review), or `unmatched`.

**Example**

```python
from sportsdataverse.cfb import cfb_rosters_crosswalk
xwalk = cfb_rosters_crosswalk(espn_team_id=194, fox_team_id=25, season=2024)
matched = xwalk.filter(pl.col("matched_sources") == "espn+fox")

# Just ESPN vs Fox (skip Yahoo's partial leg)

espn_fox = cfb_rosters_crosswalk(194, 25, providers=("espn", "fox"))
```

### `cfb_schedule_crosswalk(season: 'int', week: 'Optional[int]' = None, *, season_type: 'int' = 2, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_schedule_crosswalk}

Build the ESPN x Fox x Yahoo CFB game-id crosswalk.

Each ESPN game is keyed by its order-independent team matchup, and the Fox
and Yahoo games are mapped onto it, so each row pairs the ESPN `event` id
with the Fox Bifrost event id and the Yahoo dotted game id. Where a provider
has no game, its columns are `None` and `matched_sources` records who
contributed — so regular season, conference championships, bowls, and the
CFP all flow through the same call, degrading gracefully when a source lacks
a game.

Two modes:

* **Full season** (`week` omitted): pulls every ESPN game (regular weeks +
  bowls + CFP), Fox's full season, and Yahoo's full season, and matches on
  team **+ date** (date disambiguates rematches — a regular-season game vs a
  conference-championship or CFP rematch of the same teams).
* **Single week** (`week` given): just that week's slate, matched on team.

Each provider leg is best-effort: a Fox outage, a Yahoo per-week parser
hiccup, or Fox's offseason-projected CFP matchups simply leave that
provider's columns null rather than failing the call.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`). |
| `week` | `Optional[int]` | `None` | Schedule week number for single-week mode; omit (`None`) for the whole season. |
| `season_type` | `int` | `2` | ESPN season type for single-week mode — `2` regular, `3` post-season (`week=1` bowls, `week=999` CFP). Ignored in full-season mode. Defaults to `2`. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses all three; pass a subset for a pairwise crosswalk (e.g. `("espn", "fox")`) or a single source. Unselected providers are not fetched and surface as null columns. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `matchup_key`, `espn_game_id`, `fox_game_id`, `yahoo_game_id`, `yahoo_global_game_id`, `home_team`, `away_team`, `espn_date`, `fox_date`, `yahoo_date`, `matched_sources`.

**Example**

```python
from sportsdataverse.cfb import cfb_schedule_crosswalk
full = cfb_schedule_crosswalk(2024)
all_three = full.filter(pl.col("matched_sources") == "espn+fox+yahoo")

# Or just one week

wk5 = cfb_schedule_crosswalk(2024, 5)
```

### `cfb_teams_crosswalk(*, season: 'Optional[int]' = None, week: 'int' = 1, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_teams_crosswalk}

Build the ESPN x Fox x Yahoo CFB team-id crosswalk.

Fetches the selected provider team directories, normalizes each team name to
a shared key, and full-outer-joins them so every row carries each provider's
id, name, and abbreviation (`None` where a provider has no match). The
`matched_sources` column records which providers contributed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year used only to fetch Yahoo's embedded team directory (Yahoo has no standalone teams endpoint). Defaults to the most recent CFB season. |
| `week` | `int` | `1` | Schedule week used for the Yahoo scoreboard fetch. Defaults to `1`. The embedded directory is the full league list regardless. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses all three; pass a subset for a pairwise crosswalk (e.g. `("espn", "fox")`) or a single source. Unselected providers are not fetched and surface as null columns. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `norm_key`, `espn_team_id`, `espn_team`, `espn_abbreviation`, `fox_team_id`, `fox_team`, `fox_abbreviation`, `yahoo_team_id`, `yahoo_team`, `yahoo_abbreviation`, `matched_sources`.

**Example**

```python
from sportsdataverse.cfb import cfb_teams_crosswalk
xwalk = cfb_teams_crosswalk(season=2024)
row = xwalk.filter(pl.col("espn_team_id") == 194)  # Ohio State

# Pairwise — just ESPN vs Fox

espn_fox = cfb_teams_crosswalk(providers=("espn", "fox"))
```

### `espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_cfb_teams}

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
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `team_alternate_color` | character | Alternate team color; `team_detail = TRUE` only. |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_display_name` | character | Full team display name; `team_detail = TRUE` only. |
| `team_id` | character | ESPN team id. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team location / school name; `team_detail = TRUE` only. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_nickname` | character | Team nickname label; `team_detail = TRUE` only. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_slug` | character | Team slug for the stat row. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)

# Pull FCS teams (group 81)

fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()

# Pipeline next step (build an abbreviation lookup)

teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### `fox_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_boxscore}

Fox Sports CFB boxscore (long: one row per player-stat).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data`
(the `boxscore` block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the per-team stat tables to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_boxscore
df = fox_cfb_boxscore("41616")
```

### `fox_cfb_league_leaders(category: 'str' = 'passing', who: 'str' = 'player', page: 'int' = 0, group_id: 'Union[int, str]' = '2', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_league_leaders}

Fox Sports CFB statistical leaders (one row per player/team).

Endpoint: `GET .../bifrost/v1/cfb/league/stats-con/{who}/{category}/{page}`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'passing'` | Stat category -- passing, rushing, receiving, defense, kicking, returning, scoring, yardage (team adds downs, turnovers). Defaults to `"passing"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `group_id` | `Union[int, str]` | `'2'` | Conference/group filter. Defaults to `"2"`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_leaders
df = fox_cfb_league_leaders("passing")
```

### `fox_cfb_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_odds}

Fox Sports CFB game odds six-pack (spread / to win / total per team).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/odds`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default; empty when no market is posted), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_odds
df = fox_cfb_odds("41616")
```

### `fox_cfb_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_pbp}

Fox Sports CFB play-by-play (one row per play).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`) -- not the ESPN id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the pbp layout to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_pbp
df = fox_cfb_pbp("41616")
```

### `fox_cfb_play_process(event_id, odds_override: 'Optional[Dict[str, Any]]' = None, process: 'bool' = True, raw: 'bool' = False, **kwargs) -> 'Dict[str, Any]'` {#fox_cfb_play_process}

Build a *processed* CFB play-by-play game from FoxSports as a backup to ESPN.

Where `~sportsdataverse.cfb.cfb_fox_ext.fox_cfb_pbp` returns the raw Fox
play-by-play rows, this runs Fox data through the full ESPN play processor:
it fetches FoxSports Bifrost `cfb/event/{event_id}/data`, adapts it into the
ESPN-`summary` shape via `fox_to_espn_summary`, and runs the same
`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess` pipeline ESPN games use
-- producing EPA / WPA / advanced box score. The result carries
`source="fox"` so downstream consumers know the provenance (and that
text-derived columns are lower fidelity than the ESPN path).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event_id` |  |  | FoxSports CFB event id (e.g. `41616`). |
| `odds_override` | `Optional[Dict[str, Any]]` | `None` | Optional `{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}` dict. Fox does not expose a clean pre-game spread, so when omitted a neutral pick'em line is used (EPA is unaffected; only the WP model's spread term is neutralized). |
| `process` | `bool` | `True` | If `True` (default) run the full `~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline` (EPA/WPA/box). If `False` run the lighter `~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_cleaning_pipeline`. |
| `raw` | `bool` | `False` | If `True` skip the processor entirely and return the adapted ESPN-summary dict (the input the processor would consume). |

**Returns**

The processed game payload (same keys as `CFBPlayProcess.run_processing_pipeline`) with an added `source="fox"` key. When `raw=True`, the adapted summary dict.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_play_process
game = fox_cfb_play_process(41616)
print(len(game["plays"]), game["source"])
```

### `fox_cfb_schedule(season: 'Optional[int]' = None, *, segment_id: 'Optional[str]' = None, group_id: 'Union[int, str]' = '2', return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_schedule}

Fox Sports CFB full-season schedule (one row per game).

Fox lists games behind a two-step *selector -> segment* flow: `scoreboard/main`
enumerates the season's segments (its `selectionGroupList`), and
`league/scores-segment/{segmentId}` returns the games for one segment.
Pass a `season` to scrape the **whole season** -- every regular week plus
conference championships, bowls, and every College Football Playoff round --
enumerated from the live selector and unioned, deduplicated by `game_id`.

Segment ids encode the phase, not an ESPN-style integer week:
`"{season}-{week}-1"` for a regular-season week, `"{season}-bowls-2"` for
the bowls, `"{season}-cfp-2"` for the CFP (conference championships fall in
the final regular-season week). Pass `segment_id` to fetch just one of them.

The numeric `game_id` is the Fox Bifrost event id that `fox_cfb_pbp` /
`fox_cfb_odds` accept; `week_label` is the section title.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year -> scrape the full season. Ignored when `segment_id` is given; if both are `None` the current segment is returned. |
| `segment_id` | `Optional[str]` | `None` | Explicit Fox segment id (e.g. `"2025-5-1"`, `"2025-cfp-2"`) -> fetch just that segment. |
| `group_id` | `Union[int, str]` | `'2'` | Conference/division group filter. Defaults to `"2"` (FBS). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON (a single segment's `dict`, or a `{segment_id: dict}` map in full-season mode). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with columns `game_id`, `date`, `status`, `week_label`, `home_team`, `home_team_id`, `away_team`, `away_team_id`, `segment_id`; a pandas DataFrame when `return_as_pandas=True`; or raw JSON when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_schedule
season = fox_cfb_schedule(2025)

# Fetch just one segment (a week, or the playoff)

wk5 = fox_cfb_schedule(segment_id="2025-5-1")
cfp = fox_cfb_schedule(segment_id="2025-cfp-2")
```

### `fox_cfb_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_standings}

Fox Sports CFB conference standings for a team's conference.

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/standings`
(the league-wide `league/standings` endpoint returns header-only tables, so
standings are keyed by team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_standings
df = fox_cfb_standings("11")
```

### `fox_cfb_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_team_gamelog}

Fox Sports CFB team game log -- tidy long: one row per (game, stat).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/gamelog`
The endpoint groups team per-game stats by category (passing, rushing,
defense, ...) and season-type split; this flattens to columns
`team_id, season_type, category, game_id, game_date, opponent, stat, value`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_gamelog
df = fox_cfb_team_gamelog("11")
```

### `fox_cfb_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_team_roster}

Fox Sports CFB team roster (one row per player).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/roster`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)); discover via the league team directory (`cfb/league/teamnav`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the position-group tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_roster
df = fox_cfb_team_roster("11")
```

### `fox_cfb_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_team_stats}

Fox Sports CFB team stat leaders (one row per category leader).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/stats`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_stats
df = fox_cfb_team_stats("11")
```

### `fox_cfb_teams(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_teams}

Fox Sports CFB team directory (one row per team).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/league/teamnav`

The team-nav payload is the canonical Fox directory: it maps every team's
Bifrost id to its abbreviation, full name, and web slug. This is the lookup
you need to translate a human team name into the numeric `team_id` the
other `fox_cfb_*` wrappers expect, and it is the Fox side of
`sportsdataverse.cfb.cfb_teams_crosswalk`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the nav items to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with columns `fox_team_id`, `abbreviation`, `name`, `slug`, `color`, `logo_url`; a pandas DataFrame when `return_as_pandas=True`; or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_teams
teams = fox_cfb_teams()
fox_id = dict(zip(teams["abbreviation"], teams["fox_team_id"]))
```

### `fox_to_espn_summary(fox_data: 'Dict[str, Any]') -> 'Dict[str, Any]'` {#fox_to_espn_summary}

Adapt a Fox `cfb/event/{id}/data` payload into the ESPN-summary shape.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fox_data` | `Dict[str, Any]` |  | Parsed JSON from `api.foxsports.com/bifrost/v1/cfb/event/{id}/data`. |

**Returns**

A dict shaped like ESPN's `college-football/summary` response (`header` + `drives` + stub `pickcenter`/`boxscore`/...), ready to assign onto `CFBPlayProcess(...).json`.

### `get_cfb_teams(return_as_pandas=False) -> 'pl.DataFrame'` {#get_cfb_teams}

Load college football team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `school` | character | Team name. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `alt_name1` | character | Team alternate name 1 (as it appears in `play_text`). |
| `alt_name2` | character | Team alternate name 2 (as it appears in `play_text`). |
| `alt_name3` | character | Team alternate name 3 (as it appears in `play_text`). |
| `conference` | character | Conference of the team. |
| `division` | character | Division in the conference for the team. |
| `color` | character | Primary team color (hex, no `#`). |
| `alt_color` | character | Team color (alternate). |
| `logo` | character | Team or league logo URL. |
| `logo_dark` | character | Dark-mode logo URL. |

**Example**

```python
from sportsdataverse.cfb import get_cfb_teams
teams = get_cfb_teams()
print(teams.shape)

# Pandas round-trip

teams_pd = get_cfb_teams(return_as_pandas=True)
teams_pd.head()

# Pipeline next step (build a team_id to logo URL map)

teams = get_cfb_teams()
logo_map = dict(zip(teams["team_id"], teams["logo"]))
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

Internal helper that flattens an ESPN scoreboard event dict into a shape

suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN college-football scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```

### `yahoo_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Dict[str, Any]'` {#yahoo_cfb_boxscore}

Yahoo CFB boxscore — raw JSON passthrough (parsing not yet implemented).

Wraps the editorial `boxscore/{game_id}` resource. The payload uses a
normalized decoder-dictionary schema
(`player_stats[playerId][variation][stat_type]=value` joined against the
`stat_types`/`stat_categories` dictionaries). Flattening that into
tidy frames is a follow-up; until then this returns the raw JSON `dict`
and **fails fast** if a parsed frame is requested rather than silently
ignoring `return_parsed`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Dotted Yahoo game id (e.g. `"ncaaf.g.202509200023"`). |
| `return_parsed` | `bool` | `False` | Must be `False` (the default). Passing `True` raises `NotImplementedError` because parsing is not implemented. |
| `return_as_pandas` | `bool` | `False` | Accepted for signature parity with the sibling wrappers; has no effect while only raw output is supported. |

**Returns**

The raw editorial boxscore JSON as a `dict` (`service.boxscore`).

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_boxscore
raw = yahoo_cfb_boxscore("ncaaf.g.202509200023")
```

### `yahoo_cfb_player_season_stats(season: 'int' = 2024, *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, qualified: 'bool' = False, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_player_season_stats}

Yahoo CFB player season stats (modern; one wide row per player).

Wraps the shangrila `leagueStatsIndividual` query, which returns every
stat group (passing/rushing/receiving/...) in one call, pivoted wide with
one column per `statId`. NCAAF data is available 2013-present.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of players to request. Defaults to `200`. |
| `qualified` | `bool` | `False` | Restrict to qualified leaders only. Defaults to `False`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes a self-describing `season` column.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_player_season_stats
df = yahoo_cfb_player_season_stats(season=2024)
```

### `yahoo_cfb_player_season_stats_legacy(season: 'int' = 2024, category: 'str' = 'Passing', sort_stat: 'str' = 'PASSING_YARDS', *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_player_season_stats_legacy}

Yahoo CFB legacy per-category player leaders (one wide row per player).

Wraps the legacy `seasonStatsFootball{Category}Ncaaf` query (one stat
category per call), pivoted wide with one column per `statId`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `category` | `str` | `'Passing'` | Stat category, one of `{"Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns"}`. Defaults to `"Passing"`. |
| `sort_stat` | `str` | `'PASSING_YARDS'` | Required `FootballStatId` to sort by (see the catalog vocab). Defaults to `"PASSING_YARDS"`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of players to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `category` columns.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_player_season_stats_legacy
df = yahoo_cfb_player_season_stats_legacy(
    season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
)
```

### `yahoo_cfb_scoreboard(season: 'int', week: 'int' = 1, *, count: 'int' = 500, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_scoreboard}

Yahoo CFB scoreboard (one row per game).

Wraps the editorial `scoreboard` resource and flattens the `games` map.
`season` is required — there is no meaningful default for a weekly
scoreboard and the API has no concept of "current season". The full raw
payload also carries teams/leagues/odds maps (use `return_parsed=False`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required). |
| `week` | `int` | `1` | Schedule week number. Defaults to `1`. |
| `count` | `int` | `500` | Maximum number of games to request. Defaults to `500`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the games map to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with one row per game, a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `week` columns.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_scoreboard
df = yahoo_cfb_scoreboard(season=2024, week=1)
```

### `yahoo_cfb_team_season_stats(season: 'int' = 2024, *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_team_season_stats}

Yahoo CFB team season stats (modern; one wide row per team).

Wraps the shangrila `leagueStatsByTeam` query (all stat groups in one
call, pivoted wide with one column per `statId`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of teams to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes a self-describing `season` column.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_team_season_stats
df = yahoo_cfb_team_season_stats(season=2024)
```

### `yahoo_cfb_team_season_stats_legacy(season: 'int' = 2024, category: 'str' = 'Passing', sort_stat: 'str' = 'PASSING_YARDS', *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_team_season_stats_legacy}

Yahoo CFB legacy per-category team stats (one wide row per team).

Wraps the legacy `seasonTeamStatsFootball{Category}` query (one stat
category per call), pivoted wide with one column per `statId`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `category` | `str` | `'Passing'` | Stat category, one of `{"Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns", "Kickoffs", "Offense"}`. Defaults to `"Passing"`. |
| `sort_stat` | `str` | `'PASSING_YARDS'` | Required `FootballStatId` to sort by. Defaults to `"PASSING_YARDS"`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of teams to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `category` columns.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_team_season_stats_legacy
df = yahoo_cfb_team_season_stats_legacy(
    season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
)
```

### `yahoo_cfb_teams(season: 'int', week: 'int' = 1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_teams}

Yahoo CFB team directory (one row per team).

Yahoo has no standalone teams resource (the documented
`sports.league.teams` resource 404s without auth). Instead the editorial
`scoreboard` payload is "fat": one call embeds the full ~186-team
directory under `service.scoreboard.teams` keyed by the dotted
`ncaaf.t.<id>` team id. This wrapper pulls that map for the requested
`(season, week)` and projects it to the directory columns -- it is the
Yahoo side of `sportsdataverse.cfb.cfb_teams_crosswalk`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required; the scoreboard is fetched to obtain the embedded teams map). |
| `week` | `int` | `1` | Schedule week used to fetch the scoreboard. Defaults to `1`. The embedded directory is the full league list regardless of week. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the teams map to a DataFrame; if `False` return the raw scoreboard JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with one row per team -- columns `team_id`, `abbreviation`, `display_name`, `full_name`, `location`, `nickname`, `conference`, `conference_abbreviation`, `conference_id`, `division`, `division_id`, `seatgeek_id` -- a pandas DataFrame when `return_as_pandas=True`, or the raw scoreboard JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_teams
teams = yahoo_cfb_teams(season=2024)
abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
```
