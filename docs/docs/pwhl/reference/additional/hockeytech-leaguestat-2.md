---
title: "PWHL — additional Python functions — HockeyTech / LeagueStat: team_roster–unit_ratings"
sidebar_label: "HockeyTech / LeagueStat: team_roster–unit_ratings"
sidebar_position: 3
description: "PWHL — additional Python functions — HockeyTech / LeagueStat: team_roster–unit_ratings — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — HockeyTech / LeagueStat: team_roster–unit_ratings

### pwhl_team_roster {#pwhl_team_roster}

`pwhl_team_roster(team_id: 'int', season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL team roster for a given team + season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  | The HockeyTech team id. |
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per rostered player: `player_id`, `person_id`, names (`first_name`, `last_name`, `display_name`), `position`, `tp_jersey_number`, `shoots` / `catches`, `height` / `weight`, `birthdate`, home and birth places, `rookie`, `veteran_status`, `draft_status` and `player_image` (String, as the feed ships them). A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `person_id` | integer | Unique person identifier. |
| `active` | character | Whether athlete is currently active. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `phonetic_name` | character | Phonetic spelling of the player name. |
| `display_name` | character | Player display name. |
| `shoots` | character | Shooting hand. |
| `hometown` | character | Prospect hometown. |
| `homeprov` | character | Player home province/state. |
| `homecntry` | character | Player home country. |
| `homeplace` | character | Player home place description. |
| `birthtown` | character | Player birth town. |
| `birthprov` | character | Player birth province/state. |
| `birthcntry` | character | Player birth country. |
| `birthplace` | character | City, province/state, or country where the player was born, as recorded in the PWHL HockeyTech roster. |
| `height` | character | Player height in inches. |
| `weight` | character | Player weight in pounds. |
| `height_hyphenated` | character | Player's height formatted as feet-inches with a hyphen separator (e.g., '5-9'), as supplied by the PWHL HockeyTech API. |
| `hidden` | character | Flag indicating whether the player's roster entry is suppressed from public-facing displays in the PWHL HockeyTech system. |
| `current_team` | character | Name or identifier of the PWHL team to which the player is currently rostered. |
| `player_id` | integer | Unique player identifier. |
| `status` | character | Status string (e.g. captain markers). |
| `birthdate` | character | Date of birth. |
| `birthdate_year` | character | Player birth year. |
| `rawbirthdate` | character | Player's birth date as a raw string in the format returned by the PWHL HockeyTech API, typically YYYY-MM-DD. |
| `latest_team_id` | integer | Most recent team identifier. |
| `veteran_status` | character | Player veteran status. |
| `veteran_description` | character | Text label or descriptor indicating the player's veteran status or experience classification in the PWHL. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `division` | character | Division identifier. |
| `tp_jersey_number` | character | Jersey number assigned to the player on the current PWHL team roster, as provided by the HockeyTech feed. |
| `rookie` | character | Whether the player is a rookie. |
| `position_id` | integer | Official position identifier. |
| `position` | character | Player position. |
| `nhlteam` | character | Name or identifier of the NHL organization that holds the player's NHL rights, if applicable. |
| `player_id_1` | character | Alternate or secondary HockeyTech player identifier, distinct from the primary person_id and player_id fields. |
| `is_rookie` | character | Whether the player is a rookie. |
| `h` | character |  |
| `w` | character |  |
| `draft_status` | character | Text description of the player's draft history or eligibility status (e.g., undrafted, drafted year and round). |
| `name` | character | Player full name, first then last (e.g. 'Megan Keller'). |
| `player_image` | character | URL of the player's official roster photograph from the PWHL HockeyTech feed. |
| `catches` | character | Catching hand (goalies). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_team_roster
df = pwhl_team_roster(team_id=1, season=2025)

# Next step

import polars as pl
df.filter(pl.col("position") == "G")
```

### pwhl_teams {#pwhl_teams}

`pwhl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL teams for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id`, `team_name`, `team_code`, `team_nickname`, `team_label`, `division` and `team_logo`; `team_id` is Int64, the rest String. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `team_name` | character | Team name. |
| `team_id` | integer | Unique team identifier. |
| `team_code` | character | Team abbreviation. |
| `team_nickname` | character | Team nickname. |
| `team_label` | character | Short city label. |
| `division` | character | Division identifier. |
| `team_logo` | character | URL to the team logo image. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_teams
df = pwhl_teams(season=2025)

# Next step

df.select("team_id", "team_name", "team_code")
```

### pwhl_transactions {#pwhl_transactions}

`pwhl_transactions(return_as_pandas: 'bool' = False) -> 'Any'`

PWHL roster transactions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per transaction on the feed's current page (the newest 20): `transaction_date` / `transaction_time`, `transaction_type` / `ttype_text`, `title`, `detail`, `player_id` / `player_name` / `position`, and `team_id` / `team_name` / `team_code` / `team_city` (`player_id` / `team_id` Int64, the rest String). A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_transactions
df = pwhl_transactions()

# Next step

df.head()
```

### pwhl_unit_ratings {#pwhl_unit_ratings}

`pwhl_unit_ratings(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, xg_model: 'Any' = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

⑤ PWHL line/pair ratings -- shim over `nhl_unit_ratings` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_pwhl_pbp` frame (adapted, see the module docstring) or a frame already shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | the matching `load_pwhl_shifts` frame, or one shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory for NHL-shaped input (the borrowed NHL xG boosters). |
| `xg_model` | `Any` | `None` | a fitted `PwhlCoordXGModel` scoring HockeyTech input; `None` fits one on `pbp` (see `sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg`). That default is an in-sample fit: the goals in `pbp` shape the xG the ratings are fit to. For a backtest, pass a model fit on prior seasons only. |

**Returns**

Same schema as `nhl_unit_ratings`: `team:Utf8, unit_ids:Utf8, unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64, on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: since #846 it runs on the published PWHL pbp and shift releases (tested on five real 2025 games) and returns the same schema as its NHL engine, which has no capture of its own.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A HockeyTech pbp/shift column the NHL contract needs is missing. |

**Example**

```python
from sportsdataverse.pwhl import load_pwhl_pbp, load_pwhl_shifts
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_unit_ratings
units = pwhl_unit_ratings(load_pwhl_pbp(2025), load_pwhl_shifts(2025))
```
