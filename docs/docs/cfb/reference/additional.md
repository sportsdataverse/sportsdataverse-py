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

**Example**

```python
Quick start::

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

See Also:
    * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB rosters
    * `recruitR <https://github.com/sportsdataverse/recruitR>`_ -- recruiting data companion
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

**Example**

```python
Quick start::

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

See Also:
    * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB PBP
    * `nflverse <https://nflverse.nflverse.com>`_ -- companion data ecosystem for the NFL
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

**Example**

```python
Pull a player's 2023 season line as a single wide row::

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

**Example**

```python
Quick start (today's slate)::

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

See Also:
    * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB schedules
    * `nflverse <https://nflverse.nflverse.com>`_ -- companion data ecosystem for the NFL
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

**Example**

```python
Quick start::

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

See Also:
    * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB betting lines
    * `nflverse <https://nflverse.nflverse.com>`_ -- companion data ecosystem for the NFL
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
Quick start::

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

**Example**

```python
Quick start (FBS only by default)::

    from sportsdataverse.cfb import espn_cfb_teams
    teams = espn_cfb_teams()
    print(teams.shape)

Pull FCS teams (group 81)::

    fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
    fcs.head()

Pipeline next step (build an abbreviation lookup)::

    teams = espn_cfb_teams()
    abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))

See Also:
    * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB team data
    * `recruitR <https://github.com/sportsdataverse/recruitR>`_ -- recruiting data companion
```

### `get_cfb_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load college football team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

**Example**

```python
Quick start::

    from sportsdataverse.cfb import get_cfb_teams
    teams = get_cfb_teams()
    print(teams.shape)

Pandas round-trip::

    teams_pd = get_cfb_teams(return_as_pandas=True)
    teams_pd.head()

Pipeline next step (build a team_id to logo URL map)::

    teams = get_cfb_teams()
    logo_map = dict(zip(teams["team_id"], teams["logo"]))

See Also:
    * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB team metadata
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
Used internally by :func:`espn_cfb_schedule`::

from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```
