---
title: "NBA — additional Python functions — sportsdataverse-data releases"
sidebar_label: "sportsdataverse-data releases"
sidebar_position: 3
description: "NBA — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — sportsdataverse-data releases

### load_nba_stats_draft_combine_anthro {#load_nba_stats_draft_combine_anthro}

`load_nba_stats_draft_combine_anthro(seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load draft-combine anthropometric measurements (`draftcombineplayeranthro`).

One row per combine participant: height with and without shoes, weight,
wingspan, standing reach, body fat and hand size.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Draft year(s) of the combine, e.g. `2024` for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's START year; the asset is keyed by the END year and the loader translates). 2000 is the earliest combine on the tag. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per participant; `season` is the asset's END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | Int64 | Combine participant id; equals `player_id` in every row checked. |
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64. |
| `first_name` | String | Participant's first name. |
| `last_name` | String | Participant's last name. |
| `player_name` | String | Participant's full name. |
| `position` | String | Position listed by the combine (e.g. 'PG', 'SF-PF'). |
| `height_wo_shoes` | Float64 | Height without shoes, in inches. |
| `height_wo_shoes_ft_in` | String | Height without shoes as feet-and-inches text (e.g. "6' 5.75''"). |
| `height_w_shoes` | String | Height in shoes, in inches; null in recent combines, which stopped measuring it. |
| `height_w_shoes_ft_in` | String | Height in shoes as feet-and-inches text; null when not measured. |
| `weight` | String | Weight in pounds, as text (e.g. '227.80'). |
| `wingspan` | Float64 | Wingspan, in inches. |
| `wingspan_ft_in` | String | Wingspan as feet-and-inches text. |
| `standing_reach` | Float64 | Standing reach, in inches. |
| `standing_reach_ft_in` | String | Standing reach as feet-and-inches text. |
| `body_fat_pct` | String | Body-fat percentage; null in combines that did not publish it. |
| `hand_length` | Float64 | Hand length, in inches. |
| `hand_width` | Float64 | Hand width (span), in inches. |
| `season` | Int64 | END year of the season the draft class enters (2025 = the May 2024 combine for the 2024 draft) -- one more than the `seasons` argument. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If any requested season is below 2000. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_draft_combine_anthro
anthro = load_nba_stats_draft_combine_anthro(2024)
print(anthro.shape)

# Pipeline next step (largest hands)

anthro.sort("hand_length", descending=True).head()
```

### load_nba_stats_draft_combine_drills {#load_nba_stats_draft_combine_drills}

`load_nba_stats_draft_combine_drills(seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load draft-combine athletic drill results (`draftcombinedrillresults`).

One row per combine participant: vertical leaps, lane agility, the
three-quarter-court sprint and the bench press.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Draft year(s) of the combine, e.g. `2024` for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's START year; the asset is keyed by the END year and the loader translates). 2000 is the earliest combine on the tag. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per participant; `season` is the asset's END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | Int64 | Combine participant id; equals `player_id` in every row checked. |
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64. |
| `first_name` | String | Participant's first name. |
| `last_name` | String | Participant's last name. |
| `player_name` | String | Participant's full name. |
| `position` | String | Position listed by the combine (e.g. 'PG', 'SF-PF'). |
| `standing_vertical_leap` | Float64 | No-step vertical leap, in inches. |
| `max_vertical_leap` | Float64 | Running (max) vertical leap, in inches. |
| `lane_agility_time` | Float64 | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | Float64 | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | Float64 | Three-quarter-court sprint time, in seconds. |
| `bench_press` | String | Repetitions of the 185 lb bench press; null where not tested. |
| `season` | Int64 | END year of the season the draft class enters (2025 = the May 2024 combine for the 2024 draft) -- one more than the `seasons` argument. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If any requested season is below 2000. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_draft_combine_drills
drills = load_nba_stats_draft_combine_drills(2024)
print(drills.shape)

# Pipeline next step (highest max vertical)

drills.sort("max_vertical_leap", descending=True).head()
```

### load_nba_stats_draft_combine_nonstationary_shooting {#load_nba_stats_draft_combine_nonstationary_shooting}

`load_nba_stats_draft_combine_nonstationary_shooting(seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load draft-combine off-the-dribble and on-the-move shooting (`draftcombinenonstationaryshooting`).

One row per combine participant: made, attempted and percentage for the
off-the-dribble (15 feet and college three, three spots) and on-the-move
drills. Drills a year's combine did not run are null.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Draft year(s) of the combine, e.g. `2024` for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's START year; the asset is keyed by the END year and the loader translates). 2000 is the earliest combine on the tag. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per participant; `season` is the asset's END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | Int64 | Combine participant id; equals `player_id` in every row checked. |
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64. |
| `first_name` | String | Participant's first name. |
| `last_name` | String | Participant's last name. |
| `player_name` | String | Participant's full name. |
| `position` | String | Position listed by the combine (e.g. 'PG', 'SF-PF'). |
| `off_drib_fifteen_break_left_made` | String | Shots made in the off-the-dribble drill from the left break (wing) at 15-foot range; null if not run. |
| `off_drib_fifteen_break_left_attempt` | String | Shots attempted in the off-the-dribble drill from the left break (wing) at 15-foot range; null if not run. |
| `off_drib_fifteen_break_left_pct` | String | Shooting percentage (0-1) in the off-the-dribble drill from the left break (wing) at 15-foot range; null if not run. |
| `off_drib_fifteen_top_key_made` | String | Shots made in the off-the-dribble drill from the top of the key at 15-foot range; null if not run. |
| `off_drib_fifteen_top_key_attempt` | String | Shots attempted in the off-the-dribble drill from the top of the key at 15-foot range; null if not run. |
| `off_drib_fifteen_top_key_pct` | String | Shooting percentage (0-1) in the off-the-dribble drill from the top of the key at 15-foot range; null if not run. |
| `off_drib_fifteen_break_right_made` | String | Shots made in the off-the-dribble drill from the right break (wing) at 15-foot range; null if not run. |
| `off_drib_fifteen_break_right_attempt` | String | Shots attempted in the off-the-dribble drill from the right break (wing) at 15-foot range; null if not run. |
| `off_drib_fifteen_break_right_pct` | String | Shooting percentage (0-1) in the off-the-dribble drill from the right break (wing) at 15-foot range; null if not run. |
| `off_drib_college_break_left_made` | Int64 | Shots made in the off-the-dribble drill from the left break (wing) at college three-point range; null if not run. |
| `off_drib_college_break_left_attempt` | Int64 | Shots attempted in the off-the-dribble drill from the left break (wing) at college three-point range; null if not run. |
| `off_drib_college_break_left_pct` | Float64 | Shooting percentage (0-1) in the off-the-dribble drill from the left break (wing) at college three-point range; null if not run. |
| `off_drib_college_top_key_made` | String | Shots made in the off-the-dribble drill from the top of the key at college three-point range; null if not run. |
| `off_drib_college_top_key_attempt` | String | Shots attempted in the off-the-dribble drill from the top of the key at college three-point range; null if not run. |
| `off_drib_college_top_key_pct` | String | Shooting percentage (0-1) in the off-the-dribble drill from the top of the key at college three-point range; null if not run. |
| `off_drib_college_break_right_made` | String | Shots made in the off-the-dribble drill from the right break (wing) at college three-point range; null if not run. |
| `off_drib_college_break_right_attempt` | String | Shots attempted in the off-the-dribble drill from the right break (wing) at college three-point range; null if not run. |
| `off_drib_college_break_right_pct` | String | Shooting percentage (0-1) in the off-the-dribble drill from the right break (wing) at college three-point range; null if not run. |
| `on_move_fifteen_made` | String | Shots made in the on-the-move drill at 15-foot range; null if not run. |
| `on_move_fifteen_attempt` | String | Shots attempted in the on-the-move drill at 15-foot range; null if not run. |
| `on_move_fifteen_pct` | String | Shooting percentage (0-1) in the on-the-move drill at 15-foot range; null if not run. |
| `on_move_college_made` | Int64 | Shots made in the on-the-move drill at college three-point range; null if not run. |
| `on_move_college_attempt` | Int64 | Shots attempted in the on-the-move drill at college three-point range; null if not run. |
| `on_move_college_pct` | Float64 | Shooting percentage (0-1) in the on-the-move drill at college three-point range; null if not run. |
| `season` | Int64 | END year of the season the draft class enters (2025 = the May 2024 combine for the 2024 draft) -- one more than the `seasons` argument. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If any requested season is below 2000. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_draft_combine_nonstationary_shooting
moving = load_nba_stats_draft_combine_nonstationary_shooting(2024)
print(moving.shape)

# Pipeline next step (best on-the-move college-three shooters)

moving.sort("on_move_college_pct", descending=True, nulls_last=True).head()
```

### load_nba_stats_draft_combine_spot_shooting {#load_nba_stats_draft_combine_spot_shooting}

`load_nba_stats_draft_combine_spot_shooting(seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load draft-combine spot-up shooting drills (`draftcombinespotshooting`).

One row per combine participant: made, attempted and percentage from five
floor spots at 15 feet, the college three and the NBA three. Spots a year's
combine did not run are null.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Draft year(s) of the combine, e.g. `2024` for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's START year; the asset is keyed by the END year and the loader translates). 2000 is the earliest combine on the tag. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per participant; `season` is the asset's END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | Int64 | Combine participant id; equals `player_id` in every row checked. |
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64. |
| `first_name` | String | Participant's first name. |
| `last_name` | String | Participant's last name. |
| `player_name` | String | Participant's full name. |
| `position` | String | Position listed by the combine (e.g. 'PG', 'SF-PF'). |
| `fifteen_corner_left_made` | String | Shots made in the spot-up drill from the left corner at 15-foot range; null if not run. |
| `fifteen_corner_left_attempt` | String | Shots attempted in the spot-up drill from the left corner at 15-foot range; null if not run. |
| `fifteen_corner_left_pct` | String | Shooting percentage (0-1) in the spot-up drill from the left corner at 15-foot range; null if not run. |
| `fifteen_break_left_made` | String | Shots made in the spot-up drill from the left break (wing) at 15-foot range; null if not run. |
| `fifteen_break_left_attempt` | String | Shots attempted in the spot-up drill from the left break (wing) at 15-foot range; null if not run. |
| `fifteen_break_left_pct` | String | Shooting percentage (0-1) in the spot-up drill from the left break (wing) at 15-foot range; null if not run. |
| `fifteen_top_key_made` | String | Shots made in the spot-up drill from the top of the key at 15-foot range; null if not run. |
| `fifteen_top_key_attempt` | String | Shots attempted in the spot-up drill from the top of the key at 15-foot range; null if not run. |
| `fifteen_top_key_pct` | String | Shooting percentage (0-1) in the spot-up drill from the top of the key at 15-foot range; null if not run. |
| `fifteen_break_right_made` | String | Shots made in the spot-up drill from the right break (wing) at 15-foot range; null if not run. |
| `fifteen_break_right_attempt` | String | Shots attempted in the spot-up drill from the right break (wing) at 15-foot range; null if not run. |
| `fifteen_break_right_pct` | String | Shooting percentage (0-1) in the spot-up drill from the right break (wing) at 15-foot range; null if not run. |
| `fifteen_corner_right_made` | String | Shots made in the spot-up drill from the right corner at 15-foot range; null if not run. |
| `fifteen_corner_right_attempt` | String | Shots attempted in the spot-up drill from the right corner at 15-foot range; null if not run. |
| `fifteen_corner_right_pct` | String | Shooting percentage (0-1) in the spot-up drill from the right corner at 15-foot range; null if not run. |
| `college_corner_left_made` | Int64 | Shots made in the spot-up drill from the left corner at college three-point range; null if not run. |
| `college_corner_left_attempt` | Int64 | Shots attempted in the spot-up drill from the left corner at college three-point range; null if not run. |
| `college_corner_left_pct` | Float64 | Shooting percentage (0-1) in the spot-up drill from the left corner at college three-point range; null if not run. |
| `college_break_left_made` | String | Shots made in the spot-up drill from the left break (wing) at college three-point range; null if not run. |
| `college_break_left_attempt` | String | Shots attempted in the spot-up drill from the left break (wing) at college three-point range; null if not run. |
| `college_break_left_pct` | String | Shooting percentage (0-1) in the spot-up drill from the left break (wing) at college three-point range; null if not run. |
| `college_top_key_made` | String | Shots made in the spot-up drill from the top of the key at college three-point range; null if not run. |
| `college_top_key_attempt` | String | Shots attempted in the spot-up drill from the top of the key at college three-point range; null if not run. |
| `college_top_key_pct` | String | Shooting percentage (0-1) in the spot-up drill from the top of the key at college three-point range; null if not run. |
| `college_break_right_made` | String | Shots made in the spot-up drill from the right break (wing) at college three-point range; null if not run. |
| `college_break_right_attempt` | String | Shots attempted in the spot-up drill from the right break (wing) at college three-point range; null if not run. |
| `college_break_right_pct` | String | Shooting percentage (0-1) in the spot-up drill from the right break (wing) at college three-point range; null if not run. |
| `college_corner_right_made` | String | Shots made in the spot-up drill from the right corner at college three-point range; null if not run. |
| `college_corner_right_attempt` | String | Shots attempted in the spot-up drill from the right corner at college three-point range; null if not run. |
| `college_corner_right_pct` | String | Shooting percentage (0-1) in the spot-up drill from the right corner at college three-point range; null if not run. |
| `nba_corner_left_made` | String | Shots made in the spot-up drill from the left corner at NBA three-point range; null if not run. |
| `nba_corner_left_attempt` | String | Shots attempted in the spot-up drill from the left corner at NBA three-point range; null if not run. |
| `nba_corner_left_pct` | String | Shooting percentage (0-1) in the spot-up drill from the left corner at NBA three-point range; null if not run. |
| `nba_break_left_made` | String | Shots made in the spot-up drill from the left break (wing) at NBA three-point range; null if not run. |
| `nba_break_left_attempt` | String | Shots attempted in the spot-up drill from the left break (wing) at NBA three-point range; null if not run. |
| `nba_break_left_pct` | String | Shooting percentage (0-1) in the spot-up drill from the left break (wing) at NBA three-point range; null if not run. |
| `nba_top_key_made` | String | Shots made in the spot-up drill from the top of the key at NBA three-point range; null if not run. |
| `nba_top_key_attempt` | String | Shots attempted in the spot-up drill from the top of the key at NBA three-point range; null if not run. |
| `nba_top_key_pct` | String | Shooting percentage (0-1) in the spot-up drill from the top of the key at NBA three-point range; null if not run. |
| `nba_break_right_made` | String | Shots made in the spot-up drill from the right break (wing) at NBA three-point range; null if not run. |
| `nba_break_right_attempt` | String | Shots attempted in the spot-up drill from the right break (wing) at NBA three-point range; null if not run. |
| `nba_break_right_pct` | String | Shooting percentage (0-1) in the spot-up drill from the right break (wing) at NBA three-point range; null if not run. |
| `nba_corner_right_made` | String | Shots made in the spot-up drill from the right corner at NBA three-point range; null if not run. |
| `nba_corner_right_attempt` | String | Shots attempted in the spot-up drill from the right corner at NBA three-point range; null if not run. |
| `nba_corner_right_pct` | String | Shooting percentage (0-1) in the spot-up drill from the right corner at NBA three-point range; null if not run. |
| `season` | Int64 | END year of the season the draft class enters (2025 = the May 2024 combine for the 2024 draft) -- one more than the `seasons` argument. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If any requested season is below 2000. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_draft_combine_spot_shooting
spot = load_nba_stats_draft_combine_spot_shooting(2024)
print(spot.shape)

# Pipeline next step (best corner-three shooters)

spot.sort("college_corner_left_pct", descending=True, nulls_last=True).head()
```

### load_nba_stats_draft_combine_stats {#load_nba_stats_draft_combine_stats}

`load_nba_stats_draft_combine_stats(seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load the draft-combine summary table (`nba_stats_draft_combine`, `draftcombinestats`).

One row per combine participant: measurements, athletic testing and the
shooting drills summarised as `"made-attempted"` text.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Draft year(s) of the combine, e.g. `2024` for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's START year; the asset is keyed by the END year and the loader translates). 2000 is the earliest combine on the tag. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per participant; `season` is the asset's END year (`2025` for the 2024 combine). An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | END year of the season the draft class enters (2025 = the May 2024 combine for the 2024 draft) -- one more than the `seasons` argument. |
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64. |
| `first_name` | String | Participant's first name. |
| `last_name` | String | Participant's last name. |
| `player_name` | String | Participant's full name. |
| `position` | String | Position listed by the combine (e.g. 'PG', 'SF-PF'). |
| `height_wo_shoes` | Float64 | Height without shoes, in inches. |
| `height_wo_shoes_ft_in` | String | Height without shoes as feet-and-inches text (e.g. "6' 5.75''"). |
| `height_w_shoes` | String | Height in shoes, in inches; null in recent combines, which stopped measuring it. |
| `height_w_shoes_ft_in` | String | Height in shoes as feet-and-inches text; null when not measured. |
| `weight` | String | Weight in pounds, as text (e.g. '227.80'). |
| `wingspan` | Float64 | Wingspan, in inches. |
| `wingspan_ft_in` | String | Wingspan as feet-and-inches text. |
| `standing_reach` | Float64 | Standing reach, in inches. |
| `standing_reach_ft_in` | String | Standing reach as feet-and-inches text. |
| `body_fat_pct` | String | Body-fat percentage; null in combines that did not publish it. |
| `hand_length` | Float64 | Hand length, in inches. |
| `hand_width` | Float64 | Hand width (span), in inches. |
| `standing_vertical_leap` | Float64 | No-step vertical leap, in inches. |
| `max_vertical_leap` | Float64 | Running (max) vertical leap, in inches. |
| `lane_agility_time` | Float64 | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | Float64 | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | Float64 | Three-quarter-court sprint time, in seconds. |
| `bench_press` | String | Repetitions of the 185 lb bench press; null where not tested. |
| `spot_fifteen_corner_left` | String | Spot-up shooting from the left corner at 15-foot range, as 'made-attempted' text. |
| `spot_fifteen_break_left` | String | Spot-up shooting from the left break (wing) at 15-foot range, as 'made-attempted' text. |
| `spot_fifteen_top_key` | String | Spot-up shooting from the top of the key at 15-foot range, as 'made-attempted' text. |
| `spot_fifteen_break_right` | String | Spot-up shooting from the right break (wing) at 15-foot range, as 'made-attempted' text. |
| `spot_fifteen_corner_right` | String | Spot-up shooting from the right corner at 15-foot range, as 'made-attempted' text. |
| `spot_college_corner_left` | String | Spot-up shooting from the left corner at college three-point range, as 'made-attempted' text. |
| `spot_college_break_left` | String | Spot-up shooting from the left break (wing) at college three-point range, as 'made-attempted' text. |
| `spot_college_top_key` | String | Spot-up shooting from the top of the key at college three-point range, as 'made-attempted' text. |
| `spot_college_break_right` | String | Spot-up shooting from the right break (wing) at college three-point range, as 'made-attempted' text. |
| `spot_college_corner_right` | String | Spot-up shooting from the right corner at college three-point range, as 'made-attempted' text. |
| `spot_nba_corner_left` | String | Spot-up shooting from the left corner at NBA three-point range, as 'made-attempted' text. |
| `spot_nba_break_left` | String | Spot-up shooting from the left break (wing) at NBA three-point range, as 'made-attempted' text. |
| `spot_nba_top_key` | String | Spot-up shooting from the top of the key at NBA three-point range, as 'made-attempted' text. |
| `spot_nba_break_right` | String | Spot-up shooting from the right break (wing) at NBA three-point range, as 'made-attempted' text. |
| `spot_nba_corner_right` | String | Spot-up shooting from the right corner at NBA three-point range, as 'made-attempted' text. |
| `off_drib_fifteen_break_left` | String | Off-the-dribble shooting from the left break (wing) at 15-foot range, as 'made-attempted' text. |
| `off_drib_fifteen_top_key` | String | Off-the-dribble shooting from the top of the key at 15-foot range, as 'made-attempted' text. |
| `off_drib_fifteen_break_right` | String | Off-the-dribble shooting from the right break (wing) at 15-foot range, as 'made-attempted' text. |
| `off_drib_college_break_left` | String | Off-the-dribble shooting from the left break (wing) at college three-point range, as 'made-attempted' text. |
| `off_drib_college_top_key` | String | Off-the-dribble shooting from the top of the key at college three-point range, as 'made-attempted' text. |
| `off_drib_college_break_right` | String | Off-the-dribble shooting from the right break (wing) at college three-point range, as 'made-attempted' text. |
| `on_move_fifteen` | String | On-the-move shooting at 15-foot range, as 'made-attempted' text. |
| `on_move_college` | String | On-the-move shooting at college three-point range, as 'made-attempted' text. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If any requested season is below 2000. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_draft_combine_stats
combine = load_nba_stats_draft_combine_stats(2024)
print(combine.shape)

# Pipeline next step (longest wingspans)

combine.sort("wingspan", descending=True).head()
```

### load_nba_stats_hustle_player {#load_nba_stats_hustle_player}

`load_nba_stats_hustle_player(seasons: 'int | Iterable[int]', season_type: '_Choice' = None, per_mode: '_Choice' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load player hustle stats (the `nba_stats_hustle` release, player files).

`nba_stats_hustle` publishes eight files per season,
`leaguehustlestats{player|team}_{season_type}_{per_mode}_{END year}.parquet`.
Player and team files have different columns, so each entity has its own
loader; this one unions the four player files (or the requested subset).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Season START year(s), e.g. `2024` for 2024-25 (the asset is keyed by the END year; the loader translates). 2015 (2015-16) is the earliest season on the tag. |
| `season_type` | `str \| Iterable[str] \| None` | `None` | `"regular-season"` and/or `"playoffs"`; `None` reads both. |
| `per_mode` | `str \| Iterable[str] \| None` | `None` | `"pergame"` and/or `"totals"`; `None` reads both. Counting columns are Int64 in `totals` files and Float64 in `pergame` files, so reading both widens them to Float64. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per player per season type per per mode, with `season` as the END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64, joins to every other nba_stats loader. |
| `player_name` | String | Player's full name as stats.nba.com lists it (e.g. 'Nikola Jokic'). |
| `team_id` | Int64 | stats.nba.com team identifier; Int64. |
| `team_abbreviation` | String | Team tricode (e.g. 'BOS'). |
| `age` | Float64 | Player age in years at the season's reference date, as stats.nba.com reports it. |
| `g` | Int64 | Games played in the split. |
| `min` | Float64 | Minutes played (per game in 'pergame' files, season total in 'totals' files). |
| `contested_shots` | Float64 | Shots the player (or team) contested. |
| `contested_shots_2_pt` | Float64 | Two-point shots contested. |
| `contested_shots_3_pt` | Float64 | Three-point shots contested. |
| `deflections` | Float64 | Deflections of opponent passes or dribbles. |
| `charges_drawn` | Float64 | Offensive fouls (charges) drawn. |
| `screen_assists` | Float64 | Screens set that led directly to a teammate's made field goal. |
| `screen_ast_pts` | Float64 | Points scored on field goals created by the screen assists. |
| `off_loose_balls_recovered` | Float64 | Loose balls recovered on offense. |
| `def_loose_balls_recovered` | Float64 | Loose balls recovered on defense. |
| `loose_balls_recovered` | Float64 | Loose balls recovered (offense plus defense). |
| `pct_loose_balls_recovered_off` | Float64 | Share (0-1) of loose-ball recoveries made on offense. |
| `pct_loose_balls_recovered_def` | Float64 | Share (0-1) of loose-ball recoveries made on defense. |
| `off_boxouts` | Float64 | Box-outs on offense. |
| `def_boxouts` | Float64 | Box-outs on defense. |
| `box_outs` | Float64 | Box-outs executed (offense plus defense). |
| `box_out_player_team_rebs` | Float64 | Rebounds secured by the player's team on the player's box-outs. |
| `box_out_player_rebs` | Float64 | Rebounds secured by the player himself on his own box-outs. |
| `pct_box_outs_off` | Float64 | Share (0-1) of box-outs made on offense. |
| `pct_box_outs_def` | Float64 | Share (0-1) of box-outs made on defense. |
| `pct_box_outs_team_reb` | Float64 | Share (0-1) of the player's box-outs that ended in a team rebound. |
| `pct_box_outs_reb` | Float64 | Share (0-1) of the player's box-outs that ended in his own rebound. |
| `season` | Int64 | END year of the NBA season (2025 = 2024-25) -- the published asset's own stamp, one more than the `seasons` argument. |
| `season_type` | String | Season type of the source file: 'regular-season' or 'playoffs' (the release file-name token). |
| `per_mode` | String | Aggregation of the source file: 'pergame' (per-game averages) or 'totals' (season totals); stamped from the file name when the rows lack it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season_type` or `per_mode` is unknown. |
| `SeasonNotFoundError` | If any requested season is below 2015. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_hustle_player
hustle = load_nba_stats_hustle_player(2024, season_type="regular-season", per_mode="totals")
print(hustle.shape)

# Pipeline next step (league leaders in deflections)

hustle.sort("deflections", descending=True).head()
```

### load_nba_stats_hustle_team {#load_nba_stats_hustle_team}

`load_nba_stats_hustle_team(seasons: 'int | Iterable[int]', season_type: '_Choice' = None, per_mode: '_Choice' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load team hustle stats (the `nba_stats_hustle` release, team files).

The team half of `nba_stats_hustle`: unions the four
`leaguehustlestatsteam_{season_type}_{per_mode}_{END year}.parquet` files
(or the requested subset). See `load_nba_stats_hustle_player` for the
player files.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Season START year(s), e.g. `2024` for 2024-25 (the asset is keyed by the END year; the loader translates). 2015 (2015-16) is the earliest season on the tag. |
| `season_type` | `str \| Iterable[str] \| None` | `None` | `"regular-season"` and/or `"playoffs"`; `None` reads both. |
| `per_mode` | `str \| Iterable[str] \| None` | `None` | `"pergame"` and/or `"totals"`; `None` reads both. Counting columns are Int64 in `totals` files and Float64 in `pergame` files, so reading both widens them to Float64. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per team per season type per per mode, with `season` as the END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | stats.nba.com team identifier; Int64. |
| `team_name` | String | Team nickname (e.g. 'Celtics'). |
| `min` | Float64 | Minutes played (per game in 'pergame' files, season total in 'totals' files). |
| `contested_shots` | Float64 | Shots the team contested. |
| `contested_shots_2_pt` | Float64 | Two-point shots contested. |
| `contested_shots_3_pt` | Float64 | Three-point shots contested. |
| `deflections` | Float64 | Deflections of opponent passes or dribbles. |
| `charges_drawn` | Float64 | Offensive fouls (charges) drawn. |
| `screen_assists` | Float64 | Screens set that led directly to a teammate's made field goal. |
| `screen_ast_pts` | Float64 | Points scored on field goals created by the screen assists. |
| `off_loose_balls_recovered` | Float64 | Loose balls recovered on offense. |
| `def_loose_balls_recovered` | Float64 | Loose balls recovered on defense. |
| `loose_balls_recovered` | Float64 | Loose balls recovered (offense plus defense). |
| `pct_loose_balls_recovered_off` | Float64 | Share (0-1) of loose-ball recoveries made on offense. |
| `pct_loose_balls_recovered_def` | Float64 | Share (0-1) of loose-ball recoveries made on defense. |
| `off_boxouts` | Float64 | Box-outs on offense. |
| `def_boxouts` | Float64 | Box-outs on defense. |
| `box_outs` | Float64 | Box-outs executed (offense plus defense). |
| `pct_box_outs_off` | Float64 | Share (0-1) of box-outs made on offense. |
| `pct_box_outs_def` | Float64 | Share (0-1) of box-outs made on defense. |
| `season` | Int64 | END year of the NBA season (2025 = 2024-25) -- the published asset's own stamp, one more than the `seasons` argument. |
| `season_type` | String | Season type of the source file: 'regular-season' or 'playoffs' (the release file-name token). |
| `per_mode` | String | Aggregation of the source file: 'pergame' (per-game averages) or 'totals' (season totals); stamped from the file name when the rows lack it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season_type` or `per_mode` is unknown. |
| `SeasonNotFoundError` | If any requested season is below 2015. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_hustle_team
teams = load_nba_stats_hustle_team(2024, season_type="regular-season", per_mode="pergame")
print(teams.shape)

# Pipeline next step (most screen assists per game)

teams.sort("screen_assists", descending=True).head()
```

### load_nba_stats_leaguedash {#load_nba_stats_leaguedash}

`load_nba_stats_leaguedash(family: 'str', seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load one asset family of the `nba_stats_leaguedash` release.

`nba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as
`NBA_STATS_LEAGUEDASH_FAMILIES` -- import that tuple to discover them
rather than passing a bare string; an unknown family raises `ValueError`
listing every valid value.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `NBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. Seasons are END years (`2024` = the 2023-24 NBA season). 1996 is the earliest season on the tag; per-family coverage starts later (`lineups_*` 2008, most `player_tracking_*` 2014). A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `age` | Float64 | Player age (in years). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `e_off_rating` | Float64 |  |
| `off_rating` | Float64 |  |
| `sp_work_off_rating` | Float64 |  |
| `e_def_rating` | Float64 |  |
| `def_rating` | Float64 |  |
| `sp_work_def_rating` | Float64 |  |
| `e_net_rating` | Float64 |  |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `sp_work_net_rating` | Float64 |  |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 |  |
| `ast_ratio` | Float64 |  |
| `oreb_pct` | Float64 |  |
| `dreb_pct` | Float64 |  |
| `reb_pct` | Float64 |  |
| `tm_tov_pct` | Float64 |  |
| `e_tov_pct` | Float64 |  |
| `efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `usg_pct` | Float64 |  |
| `e_usg_pct` | Float64 |  |
| `e_pace` | Float64 |  |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `sp_work_pace` | Float64 |  |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `poss` | Int64 | Poss. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fgm_pg` | Float64 |  |
| `fga_pg` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `gp_rank` | Int64 |  |
| `w_rank` | Int64 |  |
| `l_rank` | Int64 |  |
| `w_pct_rank` | Int64 |  |
| `min_rank` | Int64 |  |
| `e_off_rating_rank` | Int64 |  |
| `off_rating_rank` | Int64 |  |
| `sp_work_off_rating_rank` | Int64 |  |
| `e_def_rating_rank` | Int64 |  |
| `def_rating_rank` | Int64 |  |
| `sp_work_def_rating_rank` | Int64 |  |
| `e_net_rating_rank` | Int64 |  |
| `net_rating_rank` | Int64 |  |
| `sp_work_net_rating_rank` | Int64 |  |
| `ast_pct_rank` | Int64 |  |
| `ast_to_rank` | Int64 |  |
| `ast_ratio_rank` | Int64 |  |
| `oreb_pct_rank` | Int64 |  |
| `dreb_pct_rank` | Int64 |  |
| `reb_pct_rank` | Int64 |  |
| `tm_tov_pct_rank` | Int64 |  |
| `e_tov_pct_rank` | Int64 |  |
| `efg_pct_rank` | Int64 |  |
| `ts_pct_rank` | Int64 |  |
| `usg_pct_rank` | Int64 |  |
| `e_usg_pct_rank` | Int64 |  |
| `e_pace_rank` | Int64 |  |
| `pace_rank` | Int64 |  |
| `sp_work_pace_rank` | Int64 |  |
| `pie_rank` | Int64 |  |
| `fgm_rank` | Int64 |  |
| `fga_rank` | Int64 |  |
| `fgm_pg_rank` | Int64 |  |
| `fga_pg_rank` | Int64 |  |
| `fg_pct_rank` | Int64 |  |
| `team_count` | Int64 |  |
| `season` | Int64 | Season year. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `family` is not one of `NBA_STATS_LEAGUEDASH_FAMILIES`. |
| `SeasonNotFoundError` | If any requested season is less than 1996. |
| `AssetFetchError` | If a release asset request fails (a 403 or an exhausted retry budget) -- distinct from an absent season, which is skipped. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_leaguedash
adv = load_nba_stats_leaguedash("player_stats_advanced", seasons=2024)
print(adv.shape)

# Discover the valid families

from sportsdataverse.nba import NBA_STATS_LEAGUEDASH_FAMILIES
print([f for f in NBA_STATS_LEAGUEDASH_FAMILIES if f.startswith("player_tracking_")])

# Multi-season, pandas round-trip

drives_pd = load_nba_stats_leaguedash(
    "player_tracking_drives", seasons=range(2020, 2025), return_as_pandas=True
)

# Pipeline next step (top usage rates in 2024)

import polars as pl
usage = load_nba_stats_leaguedash("player_stats_usage", seasons=2024)
usage.sort("usg_pct", descending=True).head()
```

### load_nba_stats_matchups {#load_nba_stats_matchups}

`load_nba_stats_matchups(seasons: 'int | Iterable[int]', per_mode: '_Choice' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load offensive-vs-defensive player matchups (`nba_stats_matchups`, season files).

Reads `leagueseasonmatchups_regular-season_{per_mode}_{END year}.parquet`
from `nba_stats_matchups`: one row per (offensive player, defensive player)
pair. The tag publishes this table for the regular season only; the
position roll-up lives in `load_nba_stats_matchups_rollup`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Season START year(s), e.g. `2024` for 2024-25 (the asset is keyed by the END year; the loader translates). 2017 (2017-18) is the earliest season on the tag. |
| `per_mode` | `str \| Iterable[str] \| None` | `None` | `"pergame"` and/or `"totals"`; `None` reads both. `matchup_min` is `"MM:SS"` text in `totals` files and decimal minutes in `pergame` files, so reading both makes it String; counting columns widen to Float64. Pass one `per_mode` to keep the native dtypes. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per matchup pair per per mode (about 140,000 per season and per mode). `season` is the END year and `season_id` stats.nba.com's code (`22024`). An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `season_id` | String | stats.nba.com season code: a season-type digit (2 = regular season, 4 = playoffs) followed by the season's START year, e.g. '22024' = the 2024-25 regular season. |
| `off_player_id` | Int64 | stats.nba.com identifier of the offensive player in the matchup; Int64. |
| `off_player_name` | String | Offensive player's display name. |
| `def_player_id` | Int64 | stats.nba.com identifier of the defender; Int64. |
| `def_player_name` | String | Defender's display name. |
| `gp` | Int64 | Games played in the split. |
| `matchup_min` | String | Time matched up: 'MM:SS' text in 'totals' files, decimal minutes in 'pergame' files (String when both are loaded together). |
| `partial_poss` | Float64 | Possessions the pair was matched up for, apportioned by time (fractional). |
| `player_pts` | Float64 | Points the offensive player scored while guarded by the defender. |
| `team_pts` | Float64 | Points the offensive player's team scored while the pair was matched up. |
| `matchup_ast` | Float64 | Assists by the offensive player while guarded by the defender. |
| `matchup_tov` | Float64 | Turnovers by the offensive player while guarded by the defender. |
| `matchup_blk` | Float64 | Blocks by the defender on the offensive player's shots. |
| `matchup_fgm` | Float64 | Field goals made by the offensive player against the defender. |
| `matchup_fga` | Float64 | Field goals attempted by the offensive player against the defender. |
| `matchup_fg_pct` | Float64 | Field-goal percentage (0-1) of the offensive player against the defender. |
| `matchup_fg3_m` | Float64 | Three-pointers made by the offensive player against the defender. |
| `matchup_fg3_a` | Float64 | Three-pointers attempted by the offensive player against the defender. |
| `matchup_fg3_pct` | Float64 | Three-point percentage (0-1) of the offensive player against the defender. |
| `help_blk` | Int64 | Help-defense blocks credited to the matchup; stats.nba.com ships 0 in every row checked. |
| `help_fgm` | Int64 | Field goals made against help defense; stats.nba.com ships 0 in every row checked. |
| `help_fga` | Int64 | Field goals attempted against help defense; stats.nba.com ships 0 in every row checked. |
| `help_fg_perc` | Int64 | Field-goal percentage against help defense; stats.nba.com ships 0 in every row checked. |
| `matchup_ftm` | Float64 | Free throws made by the offensive player on fouls by the defender. |
| `matchup_fta` | Float64 | Free throws attempted by the offensive player on fouls by the defender. |
| `sfl` | Float64 | Shooting fouls committed by the defender on the offensive player. |
| `season` | Int64 | END year of the NBA season (2025 = 2024-25) -- the published asset's own stamp, one more than the `seasons` argument. |
| `season_type` | String | Season type of the source file: 'regular-season' or 'playoffs' (the release file-name token). |
| `per_mode` | String | Aggregation of the source file: 'pergame' (per-game averages) or 'totals' (season totals); stamped from the file name when the rows lack it. |
| `matchup_time_sec` | Float64 | Time matched up in seconds; present only in early 'totals' files (e.g. 2017-18). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `per_mode` is unknown. |
| `SeasonNotFoundError` | If any requested season is below 2017. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_matchups
pairs = load_nba_stats_matchups(2024, per_mode="totals")
print(pairs.shape)

# Pipeline next step (who guarded a scorer the most)

import polars as pl
pairs.filter(pl.col("off_player_id") == 1628983).sort("partial_poss", descending=True).head()
```

### load_nba_stats_matchups_rollup {#load_nba_stats_matchups_rollup}

`load_nba_stats_matchups_rollup(seasons: 'int | Iterable[int]', season_type: '_Choice' = None, per_mode: '_Choice' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load defensive matchup roll-ups by position (`nba_stats_matchups`, rollup files).

Reads `matchupsrollup_{season_type}_{per_mode}_{END year}.parquet` from
`nba_stats_matchups`: one row per defender per offensive position
(`G`/`F`/`C`) plus a `TOTAL` row. Pair-level rows are in
`load_nba_stats_matchups`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Season START year(s), e.g. `2024` for 2024-25 (the asset is keyed by the END year; the loader translates). 2017 (2017-18) is the earliest season on the tag. |
| `season_type` | `str \| Iterable[str] \| None` | `None` | `"regular-season"` and/or `"playoffs"`; `None` reads both. |
| `per_mode` | `str \| Iterable[str] \| None` | `None` | `"pergame"` and/or `"totals"`; `None` reads both. `matchup_min` is `"MM:SS"` text in `totals` files and decimal minutes in `pergame` files, so reading both makes it String; counting columns widen to Float64. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per defender per position per season type per per mode, with `season` as the END year. An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `season_id` | String | stats.nba.com season code: a season-type digit (2 = regular season, 4 = playoffs) followed by the season's START year, e.g. '22024' = the 2024-25 regular season. |
| `position` | String | Offensive position bucket the defender guarded: 'G', 'F' or 'C', plus a 'TOTAL' row summing the three. |
| `percent_of_time` | Float64 | Share (0-1) of the defender's matchup time spent on this position ('TOTAL' = 1). |
| `def_player_id` | Int64 | stats.nba.com identifier of the defender; Int64. |
| `def_player_name` | String | Defender's display name. |
| `gp` | Int64 | Games played in the split. |
| `matchup_min` | String | Time the defender spent guarding this position: 'MM:SS' text in 'totals' files, decimal minutes in 'pergame' files (String when both are loaded together). |
| `partial_poss` | Float64 | Possessions the defender spent on this position, apportioned by time (fractional). |
| `player_pts` | Float64 | Points scored by the offensive players of this position while guarded by the defender. |
| `team_pts` | Float64 | Points scored by the opposing team while the defender guarded this position. |
| `matchup_ast` | Float64 | Assists by the guarded players of this position. |
| `matchup_tov` | Float64 | Turnovers by the guarded players of this position. |
| `matchup_blk` | Float64 | Blocks by the defender against this position. |
| `matchup_fgm` | Float64 | Field goals made against the defender by this position. |
| `matchup_fga` | Float64 | Field goals attempted against the defender by this position. |
| `matchup_fg_pct` | Float64 | Field-goal percentage (0-1) allowed to this position. |
| `matchup_fg3_m` | Float64 | Three-pointers made against the defender by this position. |
| `matchup_fg3_a` | Float64 | Three-pointers attempted against the defender by this position. |
| `matchup_fg3_pct` | Float64 | Three-point percentage (0-1) allowed to this position. |
| `matchup_ftm` | Float64 | Free throws made by this position on the defender's fouls. |
| `matchup_fta` | Float64 | Free throws attempted by this position on the defender's fouls. |
| `sfl` | Float64 | Shooting fouls the defender committed against this position. |
| `season` | Int64 | END year of the NBA season (2025 = 2024-25) -- the published asset's own stamp, one more than the `seasons` argument. |
| `season_type` | String | Season type of the source file: 'regular-season' or 'playoffs' (the release file-name token). |
| `per_mode` | String | Aggregation of the source file: 'pergame' (per-game averages) or 'totals' (season totals); stamped from the file name when the rows lack it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season_type` or `per_mode` is unknown. |
| `SeasonNotFoundError` | If any requested season is below 2017. |
| `AssetFetchError` | If a release asset request fails. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_matchups_rollup
rollup = load_nba_stats_matchups_rollup(2024, season_type="playoffs", per_mode="totals")
print(rollup.shape)

# Pipeline next step (share of time each defender spent on guards)

import polars as pl
rollup.filter(pl.col("position") == "G").sort("percent_of_time", descending=True).head()
```

### load_nba_stats_synergy {#load_nba_stats_synergy}

`load_nba_stats_synergy(seasons: 'int | Iterable[int]', season_type: '_Choice' = None, per_mode: '_Choice' = None, type_grouping: '_Choice' = None, play_type: '_Choice' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load Synergy play-type stats (the `nba_stats_synergy` release).

`nba_stats_synergy` publishes 72 files per season, one per
(season type, play type, offensive/defensive, per mode) combination, named
`{season_type}_{play_type}_{type_grouping}_{per_mode}_{END year}.parquet`.
This loader reads the requested combinations (all of them by default) and
unions them into one frame, one row per player per play type per split.
`season_type` and `per_mode` are stamped from the file name when a file's
rows do not carry them, so every row says which file it came from.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| Iterable[int]` |  | Season START year(s), e.g. `2024` for 2024-25 (the asset is keyed by the END year; the loader translates). 2015 (2015-16) is the earliest season on the tag. |
| `season_type` | `str \| Iterable[str] \| None` | `None` | `"regular-season"` and/or `"playoffs"`; `None` reads both. |
| `per_mode` | `str \| Iterable[str] \| None` | `None` | `"pergame"` and/or `"totals"`; `None` reads both. Counting columns (`poss`, `pts`, `fgm`, `fga`, `fgmx`) are Int64 in `totals` files and Float64 in `pergame` files, so reading both widens them to Float64. |
| `type_grouping` | `str \| Iterable[str] \| None` | `None` | `"offensive"` and/or `"defensive"`; `None` reads both. |
| `play_type` | `str \| Iterable[str] \| None` | `None` | Play type(s) from `NBA_STATS_SYNERGY_PLAY_TYPES` (e.g. `"isolation"`); `None` reads every play type of each requested type grouping. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per player per play type per type grouping per season type per per mode. `season` is the END year (`2025` for 2024-25), the published assets' own stamp; `season_id` is stats.nba.com's code (`22024`). An empty frame when no requested season is published.

| col_name | type | description |
|---|---|---|
| `season_id` | String | stats.nba.com season code: a season-type digit (2 = regular season, 4 = playoffs) followed by the season's START year, e.g. '22024' = the 2024-25 regular season. |
| `player_id` | Int64 | stats.nba.com player identifier (PERSON_ID); Int64, joins to every other nba_stats loader. |
| `player_name` | String | Player's full name as stats.nba.com lists it (e.g. 'Nikola Jokic'). |
| `team_id` | Int64 | stats.nba.com team identifier; Int64. |
| `team_abbreviation` | String | Team tricode (e.g. 'BOS'). |
| `team_name` | String | Team nickname (e.g. 'Celtics'). |
| `play_type` | String | Synergy play type as stats.nba.com labels it, e.g. 'Isolation', 'PRBallHandler', 'Spotup', 'Transition'. |
| `type_grouping` | String | 'Offensive' (the player's own possessions of this play type) or 'Defensive' (opponent possessions of this play type the player defended). |
| `percentile` | Float64 | Percentile rank (0-1) of the row's points per possession among players with this play type. |
| `gp` | Int64 | Games played in the split. |
| `poss_pct` | Float64 | Frequency: share (0-1) of the player's possessions that were this play type. |
| `ppp` | Float64 | Points per possession on this play type. |
| `fg_pct` | Float64 | Field-goal percentage (0-1) on this play type. |
| `ft_poss_pct` | Float64 | Share (0-1) of this play type's possessions that produced free throws. |
| `tov_poss_pct` | Float64 | Share (0-1) of this play type's possessions that ended in a turnover. |
| `sf_poss_pct` | Float64 | Share (0-1) of this play type's possessions that drew a shooting foul. |
| `plusone_poss_pct` | Float64 | Share (0-1) of this play type's possessions that ended in an and-one. |
| `score_poss_pct` | Float64 | Share (0-1) of this play type's possessions that scored at least one point. |
| `efg_pct` | Float64 | Effective field-goal percentage (0-1), weighting made threes by 1.5. |
| `poss` | Float64 | Possessions of this play type (Int64 in 'totals' files, Float64 per game). |
| `pts` | Float64 | Points scored on this play type. |
| `fgm` | Float64 | Field goals made on this play type. |
| `fga` | Float64 | Field goals attempted on this play type. |
| `fgmx` | Float64 | Field goals missed on this play type. |
| `season` | Int64 | END year of the NBA season (2025 = 2024-25) -- the published asset's own stamp, one more than the `seasons` argument. |
| `season_type` | String | Season type of the source file: 'regular-season' or 'playoffs' (the release file-name token). |
| `per_mode` | String | Aggregation of the source file: 'pergame' (per-game averages) or 'totals' (season totals); stamped from the file name when the rows lack it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a filter value is unknown, or if `play_type` and `type_grouping` select no published file (e.g. a defensive `cut`). |
| `SeasonNotFoundError` | If any requested season is below 2015. |
| `AssetFetchError` | If a release asset request fails (a 403 or an exhausted retry budget) -- distinct from an absent season, which is skipped. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_synergy
iso = load_nba_stats_synergy(2024, play_type="isolation", per_mode="totals")
print(iso.shape)

# Every play type for one split

off = load_nba_stats_synergy(2024, season_type="regular-season", type_grouping="offensive")

# Pipeline next step (most efficient isolation scorers)

import polars as pl
iso.filter(pl.col("poss") >= 100).sort("ppp", descending=True).head()
```
