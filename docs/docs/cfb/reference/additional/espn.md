---
title: "CFB — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 2
description: "CFB — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — ESPN

### espn_cfb_game_rosters {#espn_cfb_game_rosters}

`espn_cfb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `athlete_display_name` | character | Player display name. |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `weight` | double | Listed weight (lbs). |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Listed height (inches). |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `slug` | character | URL slug for the team. |
| `jersey` | character | Jersey number. |
| `linked` | logical |  |
| `active` | logical | `TRUE` if the player was active for the game. |
| `alternate_ids_sdr` | character |  |
| `birth_place_city` | character |  |
| `birth_place_state` | character |  |
| `birth_place_country` | character |  |
| `birth_country_alternate_id` | character |  |
| `birth_country_abbreviation` | character |  |
| `headshot_href` | character | URL of the athlete headshot image. |
| `headshot_alt` | character |  |
| `flag_href` | character |  |
| `flag_alt` | character |  |
| `flag_rel` | character |  |
| `experience_years` | integer | Years of experience. |
| `experience_display_value` | character |  |
| `experience_abbreviation` | character |  |
| `status_id` | character | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | character | Status type. |
| `status_abbreviation` | character |  |
| `hand_type` | character |  |
| `hand_abbreviation` | character |  |
| `hand_display_value` | character |  |
| `age` | integer |  |
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
| `team_guid` | character |  |
| `team_uid` | character |  |
| `team_slug` | character | Team slug for the stat row. |
| `team_location` | character | Team location / school name. |
| `team_name` | character | Team nickname. |
| `team_nickname` | character | Team nickname label. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name. |
| `team_color` | character | Primary team color. |
| `team_alternate_color` | character | Alternate team color. |
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

### espn_cfb_play_participants {#espn_cfb_play_participants}

`espn_cfb_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull ESPN per-play participants for a college-football game.

The college-football entry point of the shared
`sportsdataverse.football.play_participants.espn_play_participants`;
see it for the column contract (`{type}_player_name` / `{type}_player_id`
scalars plus the `{type}_player_names` / `{type}_player_ids` lists per
participant type ESPN ships).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | Fetch athletes the sidecar omits from their `$ref`. |
| `resolve_missing_max` | `int` | `50` | Cap on those per-athlete requests (default 50). |

**Returns**

Polars (or pandas) DataFrame, one row per play; the raw play dicts when `raw=True`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game identifier. |
| `play_id` | integer | ESPN play id. |
| `kicker_player_name` | character |  |
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
| `kicker_position_id` | character |  |
| `passer_position_id` | character |  |
| `receiver_position_id` | character |  |
| `rusher_position_id` | character |  |
| `scorer_position_id` | character |  |
| `returner_position_id` | character |  |
| `pass_defender_position_id` | character |  |
| `penalized_position_id` | character |  |
| `sacked_by_position_id` | character |  |
| `pat_scorer_position_id` | character |  |
| `punter_position_id` | character |  |
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
```

### espn_cfb_teams {#espn_cfb_teams}

`espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

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
| `team_abbreviation` | character | Team abbreviation. |
| `team_alternate_color` | character | Alternate team color. |
| `team_color` | character | Primary team color. |
| `team_display_name` | character | Full team display name. |
| `team_id` | character | ESPN team id. |
| `team_is_active` | logical |  |
| `team_is_all_star` | logical |  |
| `team_location` | character | Team location / school name. |
| `team_logos` | integer |  |
| `team_name` | character | Team nickname. |
| `team_nickname` | character | Team nickname label. |
| `team_short_display_name` | character | Short team display name. |
| `team_slug` | character | Team slug for the stat row. |
| `team_uid` | character |  |

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

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

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
