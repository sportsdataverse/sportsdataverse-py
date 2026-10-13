# NBA — NBA Stats API (stats.nba.com) — Draft combine

> NBA — NBA Stats API (stats.nba.com) — Draft combine — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_draftcombinedrillresults

GET /stats/draftcombinedrillresults

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinedrillresults`

**Valid URL:** [https://stats.nba.com/stats/draftcombinedrillresults?LeagueID=00&SeasonYear=2024-25](https://stats.nba.com/stats/draftcombinedrillresults?LeagueID=00&SeasonYear=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_draftcombinedrillresults-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Temporary combine player identifier assigned by the NBA before a permanent player id exists. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `standing_vertical_leap` | numeric | Standing (no-step) vertical leap, in inches. |
| `max_vertical_leap` | numeric | Maximum (running) vertical leap, in inches. |
| `lane_agility_time` | numeric | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | numeric | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | numeric | Three-quarter-court sprint time, in seconds. |
| `bench_press` | character | Repetitions of 185 pounds completed on the bench press. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_draftcombinedrillresults-example}

```python
nba_stats_draftcombinedrillresults(league_id='00', season_year='2024-25')
```

_Last validated n/a._

## nba_stats_draftcombinenonstationaryshooting

GET /stats/draftcombinenonstationaryshooting

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinenonstationaryshooting`

**Valid URL:** [https://stats.nba.com/stats/draftcombinenonstationaryshooting?LeagueID=00&SeasonYear=2024-25](https://stats.nba.com/stats/draftcombinenonstationaryshooting?LeagueID=00&SeasonYear=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_draftcombinenonstationaryshooting-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Temporary combine player identifier assigned by the NBA before a permanent player id exists. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `off_drib_fifteen_break_left_made` | character | Shots made from the 15-foot left wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_fifteen_break_left_attempt` | character | Shots attempted from the 15-foot left wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_fifteen_break_left_pct` | character | Shooting percentage from the 15-foot left wing (break) spot in the off-the-dribble shooting drill, as a decimal. |
| `off_drib_fifteen_top_key_made` | character | Shots made from the 15-foot top of the key spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_fifteen_top_key_attempt` | character | Shots attempted from the 15-foot top of the key spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_fifteen_top_key_pct` | character | Shooting percentage from the 15-foot top of the key spot in the off-the-dribble shooting drill, as a decimal. |
| `off_drib_fifteen_break_right_made` | character | Shots made from the 15-foot right wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_fifteen_break_right_attempt` | character | Shots attempted from the 15-foot right wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_fifteen_break_right_pct` | character | Shooting percentage from the 15-foot right wing (break) spot in the off-the-dribble shooting drill, as a decimal. |
| `off_drib_college_break_left_made` | integer | Shots made from the college three-point left wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_college_break_left_attempt` | integer | Shots attempted from the college three-point left wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_college_break_left_pct` | numeric | Shooting percentage from the college three-point left wing (break) spot in the off-the-dribble shooting drill, as a decimal. |
| `off_drib_college_top_key_made` | character | Shots made from the college three-point top of the key spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_college_top_key_attempt` | character | Shots attempted from the college three-point top of the key spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_college_top_key_pct` | character | Shooting percentage from the college three-point top of the key spot in the off-the-dribble shooting drill, as a decimal. |
| `off_drib_college_break_right_made` | character | Shots made from the college three-point right wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_college_break_right_attempt` | character | Shots attempted from the college three-point right wing (break) spot in the off-the-dribble shooting drill at the combine. |
| `off_drib_college_break_right_pct` | character | Shooting percentage from the college three-point right wing (break) spot in the off-the-dribble shooting drill, as a decimal. |
| `on_move_fifteen_made` | character | Shots made from the 15-foot spot in the shooting-on-the-move drill at the combine. |
| `on_move_fifteen_attempt` | character | Shots attempted from the 15-foot spot in the shooting-on-the-move drill at the combine. |
| `on_move_fifteen_pct` | character | Shooting percentage from the 15-foot spot in the shooting-on-the-move drill, as a decimal. |
| `on_move_college_made` | integer | Shots made from the college three-point spot in the shooting-on-the-move drill at the combine. |
| `on_move_college_attempt` | integer | Shots attempted from the college three-point spot in the shooting-on-the-move drill at the combine. |
| `on_move_college_pct` | numeric | Shooting percentage from the college three-point spot in the shooting-on-the-move drill, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_draftcombinenonstationaryshooting-example}

```python
nba_stats_draftcombinenonstationaryshooting(league_id='00', season_year='2024-25')
```

_Last validated n/a._

## nba_stats_draftcombineplayeranthro

GET /stats/draftcombineplayeranthro

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombineplayeranthro`

**Valid URL:** [https://stats.nba.com/stats/draftcombineplayeranthro?LeagueID=00&SeasonYear=2024-25](https://stats.nba.com/stats/draftcombineplayeranthro?LeagueID=00&SeasonYear=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_draftcombineplayeranthro-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Temporary combine player identifier assigned by the NBA before a permanent player id exists. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | numeric | Height measured without shoes, in inches. |
| `height_wo_shoes_ft_in` | character | Height without shoes formatted as feet and inches. |
| `height_w_shoes` | character | Height measured with shoes, in inches. |
| `height_w_shoes_ft_in` | character | Height with shoes formatted as feet and inches. |
| `weight` | character | Player weight in pounds. |
| `wingspan` | numeric | Wingspan measured at the combine, in inches. |
| `wingspan_ft_in` | character | Wingspan formatted as feet and inches. |
| `standing_reach` | numeric | Standing reach measured at the combine, in inches. |
| `standing_reach_ft_in` | character | Standing reach formatted as feet and inches. |
| `body_fat_pct` | character | Body fat percentage measured at the combine. |
| `hand_length` | numeric | Hand length measured at the combine, in inches. |
| `hand_width` | numeric | Hand width measured at the combine, in inches. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_draftcombineplayeranthro-example}

```python
nba_stats_draftcombineplayeranthro(league_id='00', season_year='2024-25')
```

_Last validated n/a._

## nba_stats_draftcombinespotshooting

GET /stats/draftcombinespotshooting

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinespotshooting`

**Valid URL:** [https://stats.nba.com/stats/draftcombinespotshooting?LeagueID=00&SeasonYear=2024-25](https://stats.nba.com/stats/draftcombinespotshooting?LeagueID=00&SeasonYear=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_draftcombinespotshooting-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Temporary combine player identifier assigned by the NBA before a permanent player id exists. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `fifteen_corner_left_made` | character | Shots made from the 15-foot left corner spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_corner_left_attempt` | character | Shots attempted from the 15-foot left corner spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_corner_left_pct` | character | Shooting percentage from the 15-foot left corner spot in the stationary spot-up shooting drill, as a decimal. |
| `fifteen_break_left_made` | character | Shots made from the 15-foot left wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_break_left_attempt` | character | Shots attempted from the 15-foot left wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_break_left_pct` | character | Shooting percentage from the 15-foot left wing (break) spot in the stationary spot-up shooting drill, as a decimal. |
| `fifteen_top_key_made` | character | Shots made from the 15-foot top of the key spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_top_key_attempt` | character | Shots attempted from the 15-foot top of the key spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_top_key_pct` | character | Shooting percentage from the 15-foot top of the key spot in the stationary spot-up shooting drill, as a decimal. |
| `fifteen_break_right_made` | character | Shots made from the 15-foot right wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_break_right_attempt` | character | Shots attempted from the 15-foot right wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_break_right_pct` | character | Shooting percentage from the 15-foot right wing (break) spot in the stationary spot-up shooting drill, as a decimal. |
| `fifteen_corner_right_made` | character | Shots made from the 15-foot right corner spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_corner_right_attempt` | character | Shots attempted from the 15-foot right corner spot in the stationary spot-up shooting drill at the combine. |
| `fifteen_corner_right_pct` | character | Shooting percentage from the 15-foot right corner spot in the stationary spot-up shooting drill, as a decimal. |
| `college_corner_left_made` | integer | Shots made from the college three-point left corner spot in the stationary spot-up shooting drill at the combine. |
| `college_corner_left_attempt` | integer | Shots attempted from the college three-point left corner spot in the stationary spot-up shooting drill at the combine. |
| `college_corner_left_pct` | numeric | Shooting percentage from the college three-point left corner spot in the stationary spot-up shooting drill, as a decimal. |
| `college_break_left_made` | character | Shots made from the college three-point left wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `college_break_left_attempt` | character | Shots attempted from the college three-point left wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `college_break_left_pct` | character | Shooting percentage from the college three-point left wing (break) spot in the stationary spot-up shooting drill, as a decimal. |
| `college_top_key_made` | character | Shots made from the college three-point top of the key spot in the stationary spot-up shooting drill at the combine. |
| `college_top_key_attempt` | character | Shots attempted from the college three-point top of the key spot in the stationary spot-up shooting drill at the combine. |
| `college_top_key_pct` | character | Shooting percentage from the college three-point top of the key spot in the stationary spot-up shooting drill, as a decimal. |
| `college_break_right_made` | character | Shots made from the college three-point right wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `college_break_right_attempt` | character | Shots attempted from the college three-point right wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `college_break_right_pct` | character | Shooting percentage from the college three-point right wing (break) spot in the stationary spot-up shooting drill, as a decimal. |
| `college_corner_right_made` | character | Shots made from the college three-point right corner spot in the stationary spot-up shooting drill at the combine. |
| `college_corner_right_attempt` | character | Shots attempted from the college three-point right corner spot in the stationary spot-up shooting drill at the combine. |
| `college_corner_right_pct` | character | Shooting percentage from the college three-point right corner spot in the stationary spot-up shooting drill, as a decimal. |
| `nba_corner_left_made` | character | Shots made from the NBA three-point left corner spot in the stationary spot-up shooting drill at the combine. |
| `nba_corner_left_attempt` | character | Shots attempted from the NBA three-point left corner spot in the stationary spot-up shooting drill at the combine. |
| `nba_corner_left_pct` | character | Shooting percentage from the NBA three-point left corner spot in the stationary spot-up shooting drill, as a decimal. |
| `nba_break_left_made` | character | Shots made from the NBA three-point left wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `nba_break_left_attempt` | character | Shots attempted from the NBA three-point left wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `nba_break_left_pct` | character | Shooting percentage from the NBA three-point left wing (break) spot in the stationary spot-up shooting drill, as a decimal. |
| `nba_top_key_made` | character | Shots made from the NBA three-point top of the key spot in the stationary spot-up shooting drill at the combine. |
| `nba_top_key_attempt` | character | Shots attempted from the NBA three-point top of the key spot in the stationary spot-up shooting drill at the combine. |
| `nba_top_key_pct` | character | Shooting percentage from the NBA three-point top of the key spot in the stationary spot-up shooting drill, as a decimal. |
| `nba_break_right_made` | character | Shots made from the NBA three-point right wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `nba_break_right_attempt` | character | Shots attempted from the NBA three-point right wing (break) spot in the stationary spot-up shooting drill at the combine. |
| `nba_break_right_pct` | character | Shooting percentage from the NBA three-point right wing (break) spot in the stationary spot-up shooting drill, as a decimal. |
| `nba_corner_right_made` | character | Shots made from the NBA three-point right corner spot in the stationary spot-up shooting drill at the combine. |
| `nba_corner_right_attempt` | character | Shots attempted from the NBA three-point right corner spot in the stationary spot-up shooting drill at the combine. |
| `nba_corner_right_pct` | character | Shooting percentage from the NBA three-point right corner spot in the stationary spot-up shooting drill, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_draftcombinespotshooting-example}

```python
nba_stats_draftcombinespotshooting(league_id='00', season_year='2024-25')
```

_Last validated n/a._

## nba_stats_draftcombinestats

GET /stats/draftcombinestats

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinestats`

**Valid URL:** [https://stats.nba.com/stats/draftcombinestats?LeagueID=00&SeasonYear=2024-25](https://stats.nba.com/stats/draftcombinestats?LeagueID=00&SeasonYear=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_all_time` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_draftcombinestats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | character | Season year. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | numeric | Height measured without shoes, in inches. |
| `height_wo_shoes_ft_in` | character | Height without shoes formatted as feet and inches. |
| `height_w_shoes` | character | Height measured with shoes, in inches. |
| `height_w_shoes_ft_in` | character | Height with shoes formatted as feet and inches. |
| `weight` | character | Player weight in pounds. |
| `wingspan` | numeric | Wingspan measured at the combine, in inches. |
| `wingspan_ft_in` | character | Wingspan formatted as feet and inches. |
| `standing_reach` | numeric | Standing reach measured at the combine, in inches. |
| `standing_reach_ft_in` | character | Standing reach formatted as feet and inches. |
| `body_fat_pct` | character | Body fat percentage measured at the combine. |
| `hand_length` | numeric | Hand length measured at the combine, in inches. |
| `hand_width` | numeric | Hand width measured at the combine, in inches. |
| `standing_vertical_leap` | numeric | Standing (no-step) vertical leap, in inches. |
| `max_vertical_leap` | numeric | Maximum (running) vertical leap, in inches. |
| `lane_agility_time` | numeric | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | numeric | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | numeric | Three-quarter-court sprint time, in seconds. |
| `bench_press` | character | Repetitions of 185 pounds completed on the bench press. |
| `spot_fifteen_corner_left` | character | Made-attempted result (e.g. "3-5") from the 15-foot left corner spot-up shooting station at the combine. |
| `spot_fifteen_break_left` | character | Made-attempted result (e.g. "3-5") from the 15-foot left wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_top_key` | character | Made-attempted result (e.g. "3-5") from the 15-foot top of the key spot-up shooting station at the combine. |
| `spot_fifteen_break_right` | character | Made-attempted result (e.g. "3-5") from the 15-foot right wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_corner_right` | character | Made-attempted result (e.g. "3-5") from the 15-foot right corner spot-up shooting station at the combine. |
| `spot_college_corner_left` | character | Made-attempted result (e.g. "3-5") from the college three-point left corner spot-up shooting station at the combine. |
| `spot_college_break_left` | character | Made-attempted result (e.g. "3-5") from the college three-point left wing (break) spot-up shooting station at the combine. |
| `spot_college_top_key` | character | Made-attempted result (e.g. "3-5") from the college three-point top of the key spot-up shooting station at the combine. |
| `spot_college_break_right` | character | Made-attempted result (e.g. "3-5") from the college three-point right wing (break) spot-up shooting station at the combine. |
| `spot_college_corner_right` | character | Made-attempted result (e.g. "3-5") from the college three-point right corner spot-up shooting station at the combine. |
| `spot_nba_corner_left` | character | Made-attempted result (e.g. "3-5") from the NBA three-point left corner spot-up shooting station at the combine. |
| `spot_nba_break_left` | character | Made-attempted result (e.g. "3-5") from the NBA three-point left wing (break) spot-up shooting station at the combine. |
| `spot_nba_top_key` | character | Made-attempted result (e.g. "3-5") from the NBA three-point top of the key spot-up shooting station at the combine. |
| `spot_nba_break_right` | character | Made-attempted result (e.g. "3-5") from the NBA three-point right wing (break) spot-up shooting station at the combine. |
| `spot_nba_corner_right` | character | Made-attempted result (e.g. "3-5") from the NBA three-point right corner spot-up shooting station at the combine. |
| `off_drib_fifteen_break_left` | character | Made-attempted result from the 15-foot left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_top_key` | character | Made-attempted result from the 15-foot top of the key off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_break_right` | character | Made-attempted result from the 15-foot right wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_break_left` | character | Made-attempted result from the college three-point left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_top_key` | character | Made-attempted result from the college three-point top of the key off-the-dribble shooting station at the combine. |
| `off_drib_college_break_right` | character | Made-attempted result from the college three-point right wing (break) off-the-dribble shooting station at the combine. |
| `on_move_fifteen` | character | Made-attempted result from the 15-foot shooting-on-the-move station at the combine. |
| `on_move_college` | character | Made-attempted result from the college three-point shooting-on-the-move station at the combine. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_draftcombinestats-example}

```python
nba_stats_draftcombinestats(league_id='00', season_all_time='2024-25')
```

_Last validated n/a._
