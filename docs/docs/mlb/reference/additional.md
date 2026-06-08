---
title: MLB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# MLB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mlb`
not covered by the generated API-endpoint reference above.

## Statcast

### `statcast_gamefeed(game_pk: 'int', at_bat_number: 'Optional[int]' = None, **kwargs) -> 'Dict'`

GET /gf?game_pk=... — Savant per-game JSON feed (richer than the Stats API live feed).

Returns a dict with ``team_home, team_away, scoreboard, game_status, …`` plus per-play pitch tracking and shift positioning details.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `at_bat_number` | `Optional[int]` | `None` |  |

### `statcast_leaderboard_arm_strength(year: 'Union[int, str]', pos: 'Optional[str]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/arm-strength — outfielder + infielder arm-strength leaders.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `pos` | `Optional[str]` | `None` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `fielder_name` | character |  |
| `player_id` | integer |  |
| `team_name` | character |  |
| `primary_position` | integer |  |
| `primary_position_name` | character |  |
| `total_throws` | integer |  |
| `total_throws_1b` | integer |  |
| `total_throws_2b` | integer |  |
| `total_throws_3b` | integer |  |
| `total_throws_ss` | integer |  |
| `total_throws_lf` | integer |  |
| `total_throws_cf` | integer |  |
| `total_throws_rf` | integer |  |
| `total_throws_inf` | integer |  |
| `total_throws_of` | integer |  |
| `max_arm_strength` | double |  |
| `arm_1b` | double |  |
| `arm_2b` | double |  |
| `arm_3b` | character |  |
| `arm_ss` | double |  |
| `arm_lf` | double |  |
| `arm_cf` | character |  |
| `arm_rf` | double |  |
| `arm_inf` | double |  |
| `arm_of` | double |  |
| `arm_overall` | double |  |

### `statcast_leaderboard_bat_tracking(year: 'Union[int, str]', type_: 'str' = 'batter-swings', min_: 'Optional[Union[int, str]]' = 'q', attack_zone: 'Optional[str]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/bat-tracking — swing speed / attack angle (2024+).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `type_` | `str` | `'batter-swings'` |  |
| `min_` | `Optional[Union[int, str]]` | `'q'` |  |
| `attack_zone` | `Optional[str]` | `None` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `name` | character |  |
| `swings_competitive` | integer |  |
| `percent_swings_competitive` | double |  |
| `contact` | integer |  |
| `avg_bat_speed` | double |  |
| `hard_swing_rate` | double |  |
| `squared_up_per_bat_contact` | double |  |
| `squared_up_per_swing` | double |  |
| `blast_per_bat_contact` | double |  |
| `blast_per_swing` | double |  |
| `swing_length` | double |  |
| `swords` | integer |  |
| `batter_run_value` | double |  |
| `whiffs` | character |  |
| `whiff_per_swing` | double |  |
| `batted_ball_events` | integer |  |
| `batted_ball_event_per_swing` | double |  |

### `statcast_leaderboard_catch_probability(year: 'Union[int, str]', csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/catch_probability — outfielder catch-probability leaderboard.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `last_name, first_name` | character |  |
| `player_id` | integer |  |
| `oaa` | integer |  |
| `n_fieldout_5stars` | integer |  |
| `n_opp_5stars` | integer |  |
| `n_5star_percent` | double |  |
| `n_fieldout_4stars` | integer |  |
| `n_opp_4stars` | integer |  |
| `n_4star_percent` | double |  |
| `n_fieldout_3stars` | integer |  |
| `n_opp_3stars` | integer |  |
| `n_3star_percent` | double |  |
| `n_fieldout_2stars` | integer |  |
| `n_opp_2stars` | integer |  |
| `n_2star_percent` | double |  |
| `n_fieldout_1stars` | integer |  |
| `n_opp_1stars` | integer |  |
| `n_1star_percent` | double |  |

### `statcast_leaderboard_custom(year: 'Union[int, str]', type_: 'str', selections: 'str', filter_: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', sort: 'Optional[str]' = None, sort_dir: 'str' = 'desc', csv: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/custom — build-your-own metric leaderboard.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | season year. |
| `type_` | `str` |  | leaderboard type (``batter`` / ``pitcher`` / ``fielder``). |
| `selections` | `str` |  | comma-separated metric ids (e.g. ``"xba,xslg,xwoba"``). |
| `filter_` | `Optional[str]` | `None` | row filter (e.g. ``"hand_R"``). |
| `min_` | `Optional[Union[int, str]]` | `'q'` | minimum threshold; ``"q"`` for qualified. |
| `sort` | `Optional[str]` | `None` | metric to sort by; ``sort_dir`` ``"desc"`` or ``"asc"``. |
| `sort_dir` | `str` | `'desc'` |  |
| `csv` | `bool` | `False` | when True, request CSV; otherwise JSON. |
| `return_as_pandas` | `bool` | `False` |  |

### `statcast_leaderboard_expected_statistics(year: 'Union[int, str]', type_: 'str' = 'batter', position: 'Optional[str]' = None, team: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/expected_statistics — xBA / xSLG / xwOBA / xISO leaders.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `type_` | `str` | `'batter'` |  |
| `position` | `Optional[str]` | `None` |  |
| `team` | `Optional[str]` | `None` |  |
| `min_` | `Optional[Union[int, str]]` | `'q'` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `last_name, first_name` | character |  |
| `player_id` | integer |  |
| `year` | integer |  |
| `pa` | integer |  |
| `bip` | integer |  |
| `ba` | double |  |
| `est_ba` | double |  |
| `est_ba_minus_ba_diff` | double |  |
| `slg` | double |  |
| `est_slg` | double |  |
| `est_slg_minus_slg_diff` | double |  |
| `woba` | double |  |
| `est_woba` | double |  |
| `est_woba_minus_woba_diff` | double |  |

### `statcast_leaderboard_outs_above_average(year: 'Union[int, str]', pos: 'Optional[str]' = None, team: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/outs_above_average — OAA fielding leaderboard.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `pos` | `Optional[str]` | `None` |  |
| `team` | `Optional[str]` | `None` |  |
| `min_` | `Optional[Union[int, str]]` | `'q'` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `last_name, first_name` | character |  |
| `player_id` | integer |  |
| `display_team_name` | character |  |
| `year` | integer |  |
| `primary_pos_formatted` | character |  |
| `fielding_runs_prevented` | integer |  |
| `outs_above_average` | integer |  |
| `outs_above_average_infront` | integer |  |
| `outs_above_average_lateral_toward3bline` | integer |  |
| `outs_above_average_lateral_toward1bline` | integer |  |
| `outs_above_average_behind` | integer |  |
| `outs_above_average_rhh` | integer |  |
| `outs_above_average_lhh` | integer |  |
| `actual_success_rate_formatted` | character |  |
| `adj_estimated_success_rate_formatted` | character |  |
| `diff_success_rate_formatted` | character |  |

### `statcast_leaderboard_pitch_arsenal(year: 'Union[int, str]', team: 'Optional[str]' = None, min_: 'Optional[Union[int, str]]' = 'q', pitch_hand: 'Optional[str]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/pitch-arsenal-stats — per-pitch outcome stats by pitcher.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `team` | `Optional[str]` | `None` |  |
| `min_` | `Optional[Union[int, str]]` | `'q'` |  |
| `pitch_hand` | `Optional[str]` | `None` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

### `statcast_leaderboard_poptime(year: 'Union[int, str]', min2b: 'Optional[int]' = None, min3b: 'Optional[int]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/poptime — catcher pop-time leaders.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `min2b` | `Optional[int]` | `None` |  |
| `min3b` | `Optional[int]` | `None` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `entity_name` | character |  |
| `entity_id` | integer |  |
| `team_id` | integer |  |
| `age` | integer |  |
| `maxeff_arm_2b_3b_sba` | double |  |
| `exchange_2b_3b_sba` | double |  |
| `pop_2b_sba_count` | integer |  |
| `pop_2b_sba` | double |  |
| `pop_2b_cs` | double |  |
| `pop_2b_sb` | double |  |
| `pop_3b_sba_count` | integer |  |
| `pop_3b_sba` | double |  |
| `pop_3b_cs` | double |  |
| `pop_3b_sb` | double |  |

### `statcast_leaderboard_sprint_speed(year: 'Union[int, str]', position: 'Optional[str]' = None, team: 'Optional[str]' = None, min_opp: 'Optional[int]' = None, csv: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

GET /leaderboard/sprint_speed — sprint-speed (ft/sec) leaders.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `position` | `Optional[str]` | `None` |  |
| `team` | `Optional[str]` | `None` |  |
| `min_opp` | `Optional[int]` | `None` |  |
| `csv` | `bool` | `True` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `last_name, first_name` | character |  |
| `player_id` | integer |  |
| `team_id` | integer |  |
| `team` | character |  |
| `position` | character |  |
| `age` | integer |  |
| `competitive_runs` | integer |  |
| `bolts` | integer |  |
| `hp_to_1b` | double |  |
| `sprint_speed` | double |  |

### `statcast_player_page(player_id: 'int', stats: 'Optional[str]' = None, **kwargs) -> 'str'`

GET /savant-player/{playerId} — Savant player profile page (HTML with embedded JSON).

Returns the raw HTML text. The page embeds JSON blobs under ``<script id="player-data" type="application/json">…</script>`` (and a handful of others) that carry the canonical Statcast snapshots for the player. Extracting those blobs is a follow-up — for now the wrapper returns the full HTML so callers can mine it. TODO: add a sibling :func:`statcast_player_data` that does the BS4 / regex extraction and returns a typed dict.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  |  |
| `stats` | `Optional[str]` | `None` |  |

### `statcast_search(start_date: 'str', end_date: 'str', *, player_type: 'str' = 'batter', season: 'Optional[Union[str, Iterable[str]]]' = None, game_type: 'Optional[Union[str, Iterable[str]]]' = None, batters_lookup: 'Optional[Union[int, Iterable[int]]]' = None, pitchers_lookup: 'Optional[Union[int, Iterable[int]]]' = None, team: 'Optional[str]' = None, opponent: 'Optional[str]' = None, home_road: 'Optional[str]' = None, stadium: 'Optional[Union[int, str]]' = None, pitcher_throws: 'Optional[str]' = None, batter_stands: 'Optional[str]' = None, position: 'Optional[Union[str, Iterable[str]]]' = None, pitch_type: 'Optional[Union[str, Iterable[str]]]' = None, count: 'Optional[Union[str, Iterable[str]]]' = None, at_bat_result: 'Optional[Union[str, Iterable[str]]]' = None, batted_ball_type: 'Optional[Union[str, Iterable[str]]]' = None, pitch_result: 'Optional[Union[str, Iterable[str]]]' = None, zone: 'Optional[Union[str, Iterable[str]]]' = None, outs: 'Optional[Union[int, Iterable[int]]]' = None, inning: 'Optional[Union[int, Iterable[int]]]' = None, runners_on: 'Optional[Union[str, Iterable[str]]]' = None, flag: 'Optional[Union[str, Iterable[str]]]' = None, return_as_pandas: 'bool' = False, raise_on_truncation: 'bool' = True, **kwargs)`

GET /statcast_search/csv — pitch-by-pitch Statcast search.

Returns a polars DataFrame of pitches matching the filter set. The Savant endpoint caps results at **25,000 rows per response with no pagination**; if the wrapper detects exactly 25,000 rows in the response and ``raise_on_truncation=True`` (default), it raises :class:`RuntimeError` rather than silently returning a partial frame. Use :func:`statcast_search_chunked` for date ranges that may exceed 25k pitches. Most filter args accept either a scalar or an iterable; the wrapper joins iterables with Savant's trailing-pipe convention (e.g. ``["FF","SL"]`` → ``"FF|SL|"``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_date` | `str` |  |  |
| `end_date` | `str` |  |  |
| `player_type` | `str` | `'batter'` | ``"batter"`` (default) or ``"pitcher"`` — controls which side of the matchup ``batters_lookup`` / ``pitchers_lookup`` / ``team`` filters apply to. |
| `season` | `Optional[Union[str, Iterable[str]]]` | `None` |  |
| `game_type` | `Optional[Union[str, Iterable[str]]]` | `None` |  |
| `batters_lookup` | `Optional[Union[int, Iterable[int]]]` | `None` |  |
| `pitchers_lookup` | `Optional[Union[int, Iterable[int]]]` | `None` |  |
| `team` | `Optional[str]` | `None` |  |
| `opponent` | `Optional[str]` | `None` |  |
| `home_road` | `Optional[str]` | `None` | ``"home"`` / ``"road"``. |
| `stadium` | `Optional[Union[int, str]]` | `None` | venue id. |
| `pitcher_throws` | `Optional[str]` | `None` |  |
| `batter_stands` | `Optional[str]` | `None` |  |
| `position` | `Optional[Union[str, Iterable[str]]]` | `None` |  |
| `pitch_type` | `Optional[Union[str, Iterable[str]]]` | `None` | pipe-list of pitch codes (``"FF","SL","CU","CH","SI","FC"``…). |
| `count` | `Optional[Union[str, Iterable[str]]]` | `None` | pipe-list of pitcher–batter counts (e.g. ``["00","11"]``). |
| `at_bat_result` | `Optional[Union[str, Iterable[str]]]` | `None` | pipe-list of PA outcomes (``"single","home_run","walk"``…). |
| `batted_ball_type` | `Optional[Union[str, Iterable[str]]]` | `None` | ``"fly_ball","ground_ball","line_drive","popup"``. |
| `pitch_result` | `Optional[Union[str, Iterable[str]]]` | `None` | ``"called_strike","ball","swinging_strike","foul",…``. |
| `zone` | `Optional[Union[str, Iterable[str]]]` | `None` | gameday zone (``1``–``14``). |
| `outs` | `Optional[Union[int, Iterable[int]]]` | `None` |  |
| `inning` | `Optional[Union[int, Iterable[int]]]` | `None` |  |
| `runners_on` | `Optional[Union[str, Iterable[str]]]` | `None` | ``"none","on_first","on_second","on_third","RISP"``… |
| `flag` | `Optional[Union[str, Iterable[str]]]` | `None` | special flags (``"is_barrel","is_solidcontact","is_putaway"``…). |
| `return_as_pandas` | `bool` | `False` | convert the returned polars frame to pandas. |
| `raise_on_truncation` | `bool` | `True` | when True (default), raise if the response has exactly 25,000 rows. |

**Returns**

polars.DataFrame (or pandas if ``return_as_pandas=True``) with one row per pitch, ~90 columns covering pitch tracking, batted-ball metrics, Statcast outcomes, and game/play context.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character |  |
| `game_date` | character |  |
| `release_speed` | double |  |
| `release_pos_x` | double |  |
| `release_pos_z` | double |  |
| `player_name` | character |  |
| `batter` | integer |  |
| `pitcher` | integer |  |
| `events` | character |  |
| `description` | character |  |
| `spin_dir` | character |  |
| `spin_rate_deprecated` | character |  |
| `break_angle_deprecated` | character |  |
| `break_length_deprecated` | character |  |
| `zone` | integer |  |
| `des` | character |  |
| `game_type` | character |  |
| `stand` | character |  |
| `p_throws` | character |  |
| `home_team` | character |  |
| `away_team` | character |  |
| `type` | character |  |
| `hit_location` | integer |  |
| `bb_type` | character |  |
| `balls` | integer |  |
| `strikes` | integer |  |
| `game_year` | integer |  |
| `pfx_x` | double |  |
| `pfx_z` | double |  |
| `plate_x` | double |  |
| `plate_z` | double |  |
| `on_3b` | character |  |
| `on_2b` | integer |  |
| `on_1b` | integer |  |
| `outs_when_up` | integer |  |
| `inning` | integer |  |
| `inning_topbot` | character |  |
| `hc_x` | double |  |
| `hc_y` | double |  |
| `tfs_deprecated` | character |  |
| `tfs_zulu_deprecated` | character |  |
| `umpire` | character |  |
| `sv_id` | character |  |
| `vx0` | double |  |
| `vy0` | double |  |
| `vz0` | double |  |
| `ax` | double |  |
| `ay` | double |  |
| `az` | double |  |
| `sz_top` | double |  |
| `sz_bot` | double |  |
| `hit_distance_sc` | integer |  |
| `launch_speed` | double |  |
| `launch_angle` | integer |  |
| `effective_speed` | double |  |
| `release_spin_rate` | integer |  |
| `release_extension` | double |  |
| `game_pk` | integer |  |
| `fielder_2` | integer |  |
| `fielder_3` | integer |  |
| `fielder_4` | integer |  |
| `fielder_5` | integer |  |
| `fielder_6` | integer |  |
| `fielder_7` | integer |  |
| `fielder_8` | integer |  |
| `fielder_9` | integer |  |
| `release_pos_y` | double |  |
| `estimated_ba_using_speedangle` | double |  |
| `estimated_woba_using_speedangle` | double |  |
| `woba_value` | double |  |
| `woba_denom` | integer |  |
| `babip_value` | integer |  |
| `iso_value` | integer |  |
| `launch_speed_angle` | integer |  |
| `at_bat_number` | integer |  |
| `pitch_number` | integer |  |
| `pitch_name` | character |  |
| `home_score` | integer |  |
| `away_score` | integer |  |
| `bat_score` | integer |  |
| `fld_score` | integer |  |
| `post_away_score` | integer |  |
| `post_home_score` | integer |  |
| `post_bat_score` | integer |  |
| `post_fld_score` | integer |  |
| `if_fielding_alignment` | character |  |
| `of_fielding_alignment` | character |  |
| `spin_axis` | integer |  |
| `delta_home_win_exp` | double |  |
| `delta_run_exp` | double |  |
| `bat_speed` | double |  |
| `swing_length` | double |  |
| `estimated_slg_using_speedangle` | double |  |
| `delta_pitcher_run_exp` | double |  |
| `hyper_speed` | double |  |
| `home_score_diff` | integer |  |
| `bat_score_diff` | integer |  |
| `home_win_exp` | double |  |
| `bat_win_exp` | double |  |
| `age_pit_legacy` | integer |  |
| `age_bat_legacy` | integer |  |
| `age_pit` | integer |  |
| `age_bat` | integer |  |
| `n_thruorder_pitcher` | integer |  |
| `n_priorpa_thisgame_player_at_bat` | integer |  |
| `pitcher_days_since_prev_game` | integer |  |
| `batter_days_since_prev_game` | integer |  |
| `pitcher_days_until_next_game` | integer |  |
| `batter_days_until_next_game` | integer |  |
| `api_break_z_with_gravity` | double |  |
| `api_break_x_arm` | double |  |
| `api_break_x_batter_in` | double |  |
| `arm_angle` | double |  |
| `attack_angle` | double |  |
| `attack_direction` | double |  |
| `swing_path_tilt` | double |  |
| `intercept_ball_minus_batter_pos_x_inches` | double |  |
| `intercept_ball_minus_batter_pos_y_inches` | double |  |

### `statcast_search_chunked(start_date: 'str', end_date: 'str', *, chunk_days: 'int' = 5, return_as_pandas: 'bool' = False, **kwargs)`

Auto-chunk a date range into ``chunk_days``-day windows and concatenate.

Wraps :func:`statcast_search` and stitches results client-side. Useful for multi-month or full-season pulls that would exceed the 25k row cap in a single request.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_date` | `str` |  |  |
| `end_date` | `str` |  |  |
| `chunk_days` | `int` | `5` | window size in days (default 5 — typical for the regular season; smaller for postseason when there are more high-event games). |
| `return_as_pandas` | `bool` | `False` | convert the concatenated frame to pandas. |

**Returns**

polars.DataFrame (or pandas) of all pitches in the range.

## MLB Stats API

### `mlb_api_attendance(team_id: 'Optional[int]' = None, league_id: 'Optional[Union[int, str]]' = None, season: 'Optional[Union[int, str]]' = None, league_list_id: 'Optional[str]' = None, game_type: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/attendance — game attendance figures.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `league_list_id` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |

### `mlb_api_divisions(sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, division_id: 'Optional[int]' = None, **kwargs) -> 'Dict'`

GET /api/v1/divisions — list divisions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `division_id` | `Optional[int]` | `None` |  |

### `mlb_api_draft_prospects(year: 'Union[int, str]', scouting_report: 'Optional[bool]' = None, limit: 'int' = 100, **kwargs) -> 'Dict'`

GET /api/v1/draft/prospects/{year} — draft prospect list for a year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  |  |
| `scouting_report` | `Optional[bool]` | `None` |  |
| `limit` | `int` | `100` |  |

### `mlb_api_pbp_diff(game_pk: 'int', start_timecode: 'str', end_timecode: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed.

Replays of in-game state for low-bandwidth clients.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `start_timecode` | `str` |  |  |
| `end_timecode` | `Optional[str]` | `None` |  |

### `mlb_api_pbp_live(game_pk: 'int', language: 'Optional[str]' = None, timecode: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

Top-level keys: ``copyright, gamePk, link, metaData, gameData, liveData``. Includes Statcast metrics where available. The historical name ``mlb_api_pbp`` is preserved as an alias.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_pk` | `int` |  |  |
| `language` | `Optional[str]` | `None` |  |
| `timecode` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_person_stats(person_id: 'int', stats: 'str', group: 'str' = 'hitting', season: 'Optional[Union[int, str]]' = None, season_type: 'Optional[str]' = None, sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/people/{personId}/stats — player aggregate stats.

``stats``: ``season``, ``career``, ``yearByYear``, ``vsTeam``, ``vsPlayer``, ``byMonth``, ``byDayOfWeek``, ``homeAndAway``, ``gameLog``, ``lastXGames``, …

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `person_id` | `int` |  |  |
| `stats` | `str` |  |  |
| `group` | `str` | `'hitting'` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `season_type` | `Optional[str]` | `None` |  |
| `sport_ids` | `Optional[Union[int, List[int]]]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_schedule(date: 'Optional[str]' = None, start_date: 'Optional[str]' = None, end_date: 'Optional[str]' = None, team_id: 'Optional[int]' = None, opponent_id: 'Optional[int]' = None, season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, game_type: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/schedule — schedule of games for a date, range, team, or season.

Response: ``dates[].games[]``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` |  |
| `start_date` | `Optional[str]` | `None` |  |
| `end_date` | `Optional[str]` | `None` |  |
| `team_id` | `Optional[int]` | `None` |  |
| `opponent_id` | `Optional[int]` | `None` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_seasons(sport_id: 'int' = 1, season: 'Optional[Union[int, str]]' = None, all_seasons: 'bool' = False, **kwargs) -> 'Dict'`

GET /api/v1/seasons — list of seasons for a sport.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport_id` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `all_seasons` | `bool` | `False` |  |

### `mlb_api_standings(league_id: 'Union[int, str, List[int]]' = '103,104', season: 'Optional[Union[int, str]]' = None, date: 'Optional[str]' = None, standings_types: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/standings — league standings.

``league_id``: ``103`` AL, ``104`` NL (comma-separated for both, the default). ``standings_types`` e.g. ``regularSeason``, ``wildCard``, ``divisionLeaders``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `Union[int, str, List[int]]` | `'103,104'` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `date` | `Optional[str]` | `None` |  |
| `standings_types` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_stats(stats: 'str', group: 'str', season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, team_id: 'Optional[int]' = None, player_pool: 'Optional[str]' = None, game_type: 'Optional[str]' = None, limit: 'int' = 50, offset: 'int' = 0, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/stats — generic stats query.

``stats`` selects the slice (``season``, ``career``, ``yearByYear``, …) and ``group`` selects the stat group (``hitting``, ``pitching``, ``fielding``). Filters: ``season``, ``team_id``, ``league_id``, ``game_type``, ``player_pool``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stats` | `str` |  |  |
| `group` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `team_id` | `Optional[int]` | `None` |  |
| `player_pool` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `limit` | `int` | `50` |  |
| `offset` | `int` | `0` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_stats_leaders(leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, stat_group: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, limit: 'int' = 10, **kwargs) -> 'Dict'`

GET /api/v1/stats/leaders — top-N leaders for a stat category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `leader_categories` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `leader_game_types` | `Optional[str]` | `None` |  |
| `stat_group` | `Optional[str]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `limit` | `int` | `10` |  |

### `mlb_api_stats_streaks(streak_type: 'str', streak_threshold: 'int' = 1, season: 'Optional[Union[int, str]]' = None, stat_group: 'Optional[str]' = None, active_streak: 'Optional[bool]' = None, sport_id: 'int' = 1, **kwargs) -> 'Dict'`

GET /api/v1/stats/streaks — active or historical streaks.

``streak_type`` e.g. ``hittingStreakOverall``, ``onBaseOverall``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `streak_type` | `str` |  |  |
| `streak_threshold` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `stat_group` | `Optional[str]` | `None` |  |
| `active_streak` | `Optional[bool]` | `None` |  |
| `sport_id` | `int` | `1` |  |

### `mlb_api_team_leaders(team_id: 'int', leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, limit: 'int' = 10, **kwargs) -> 'Dict'`

GET /api/v1/teams/{teamId}/leaders — team leaders.

``leader_categories`` e.g. ``homeRuns``, ``battingAverage``, ``wins``, ``earnedRunAverage`` (comma-separated for multi).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `leader_categories` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `leader_game_types` | `Optional[str]` | `None` |  |
| `limit` | `int` | `10` |  |

### `mlb_api_team_stats(team_id: 'int', season: 'Union[int, str]', stats: 'str' = 'season', group: 'str' = 'hitting', sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/teams/{teamId}/stats — team-level stats.

``stats``: ``season``, ``career``, ``yearByYear``, ``byMonth``, ``byDayOfWeek``, … ``group``: ``hitting``, ``pitching``, ``fielding``.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Union[int, str]` |  |  |
| `stats` | `str` | `'season'` |  |
| `group` | `str` | `'hitting'` |  |
| `sport_ids` | `Optional[Union[int, List[int]]]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### `mlb_api_teams(season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_ids: 'Optional[Union[int, List[int], str]]' = None, active_status: 'Optional[str]' = None, all_star_statuses: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/teams — list teams. ``sport_id=1`` = MLB.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `league_ids` | `Optional[Union[int, List[int], str]]` | `None` |  |
| `active_status` | `Optional[str]` | `None` |  |
| `all_star_statuses` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

## Play-by-play, schedule & rosters

### `espn_mlb_game_rosters(game_id: 'int', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs)`

espn_mlb_game_rosters - pull the active game rosters for both teams.

Wraps the Core v2 endpoint:: https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{game_id}/competitions/{game_id}/competitors Each competitor's ``roster.$ref`` is dereferenced to the per-team athlete list, then athletes are flattened to one row per (game × team × athlete).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game id. |
| `raw` | `bool` | `False` | When True, returns the merged competitor + roster payload dict. |
| `return_as_pandas` | `bool` | `False` | When True, returns a pandas dataframe; otherwise polars. |

**Returns**

One row per (game × team × athlete) with columns ``game_id, team_id, home_away, athlete_id, athlete_full_name, athlete_jersey, athlete_position_id, athlete_position_abbreviation, athlete_starter``.

**Example**

```python
from sportsdataverse.mlb import espn_mlb_game_rosters
ros = espn_mlb_game_rosters(game_id=401569461)
print(ros.shape)
ros.group_by("home_away").len()
```

### `espn_mlb_pbp(game_id: 'int', raw: 'bool' = False, **kwargs) -> 'Dict'`

espn_mlb_pbp - pull the full ESPN game-summary payload for one MLB game.

Wraps the Site v2 endpoint:: http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={game_id}

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game id (the "event id"). Obtainable from :func:`espn_mlb_schedule`. |
| `raw` | `bool` | `False` | When True, returns the full nested payload unchanged. When False (default), the same payload is returned for now — full parsing into a tidy plays / boxscore dict is **not yet implemented**; see the TODO below. |

**Returns**

The Site v2 summary payload. Top-level keys typically include ``header``, ``boxscore``, ``plays``, ``leaders``, ``scoringPlays``, ``gameInfo``, ``winprobability``, ``pickcenter``, ``news``, ``videos``, ``standings``, ``article``, ``seasonseries``, ``broadcasts``, ``predictor``.

**Example**

```python
from sportsdataverse.mlb import espn_mlb_pbp
game = espn_mlb_pbp(game_id=401569461, raw=True)
sorted(game.keys())
print(game.get("header", {}).get("competitions", [{}])[0].get("date"))

Iterate the plays array::

plays = game.get("plays") or []
print(f"{len(plays)} plays")
for p in plays[:3]:
    print(p.get("text"))
```

### `espn_mlb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an MLB athlete's ESPN **season** stat line as one wide row.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns (for baseball: ``batting_*``, ``pitching_*``, ``fielding_*``), the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer multi-category web-v3 payload use :func:`sportsdataverse.mlb.espn_mlb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN MLB athlete identifier (e.g. ``33192`` for Aaron Judge). |
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
| `batting_games_played` | double |  |
| `batting_team_games_played` | double |  |
| `batting_hit_by_pitch` | double |  |
| `batting_ground_balls` | double |  |
| `batting_strikeouts` | double |  |
| `batting_rb_is` | double |  |
| `batting_sac_hits` | double |  |
| `batting_hits` | double |  |
| `batting_stolen_bases` | double |  |
| `batting_walks` | double |  |
| `batting_catcher_interference` | double |  |
| `batting_runs` | double |  |
| `batting_gid_ps` | double |  |
| `batting_sac_flies` | double |  |
| `batting_at_bats` | double |  |
| `batting_home_runs` | double |  |
| `batting_grand_slam_home_runs` | double |  |
| `batting_runners_left_on_base` | double |  |
| `batting_triples` | double |  |
| `batting_game_winning_rb_is` | double |  |
| `batting_intentional_walks` | double |  |
| `batting_doubles` | double |  |
| `batting_fly_balls` | double |  |
| `batting_caught_stealing` | double |  |
| `batting_pitches` | double |  |
| `batting_games_started` | double |  |
| `batting_pinch_at_bats` | double |  |
| `batting_pinch_hits` | double |  |
| `batting_player_rating` | double |  |
| `batting_is_qualified` | double |  |
| `batting_is_qualified_steals` | double |  |
| `batting_total_bases` | double |  |
| `batting_plate_appearances` | double |  |
| `batting_projected_home_runs` | double |  |
| `batting_extra_base_hits` | double |  |
| `batting_runs_created` | double |  |
| `batting_avg` | double |  |
| `batting_pinch_avg` | double |  |
| `batting_slug_avg` | double |  |
| `batting_secondary_avg` | double |  |
| `batting_on_base_pct` | double |  |
| `batting_ops` | double |  |
| `batting_ground_to_fly_ratio` | double |  |
| `batting_runs_created_per27_outs` | double |  |
| `batting_batter_rating` | double |  |
| `batting_at_bats_per_home_run` | double |  |
| `batting_stolen_base_pct` | double |  |
| `batting_pitches_per_plate_appearance` | double |  |
| `batting_isolated_power` | double |  |
| `batting_walk_to_strikeout_ratio` | double |  |
| `batting_walks_per_plate_appearance` | double |  |
| `batting_secondary_avg_minus_ba` | double |  |
| `batting_runs_produced` | double |  |
| `batting_runs_ratio` | double |  |
| `batting_patience_ratio` | double |  |
| `batting_bipa` | double |  |
| `batting_mlb_rating` | double |  |
| `batting_off_warbr` | double |  |
| `batting_warbr` | double |  |
| `fielding_games_played` | double |  |
| `fielding_team_games_played` | double |  |
| `fielding_double_plays` | double |  |
| `fielding_opportunities` | double |  |
| `fielding_errors` | double |  |
| `fielding_passed_balls` | double |  |
| `fielding_assists` | double |  |
| `fielding_outfield_assists` | double |  |
| `fielding_pickoffs` | double |  |
| `fielding_putouts` | double |  |
| `fielding_outs_on_field` | double |  |
| `fielding_triple_plays` | double |  |
| `fielding_balls_in_zone` | double |  |
| `fielding_extra_bases` | double |  |
| `fielding_outs_made` | double |  |
| `fielding_hits` | double |  |
| `fielding_total_bases` | double |  |
| `fielding_games_started` | double |  |
| `fielding_catcher_third_innings_played` | double |  |
| `fielding_catcher_caught_stealing` | double |  |
| `fielding_catcher_stolen_bases_allowed` | double |  |
| `fielding_catcher_earned_runs` | double |  |
| `fielding_is_qualified` | double |  |
| `fielding_is_qualified_catcher` | double |  |
| `fielding_is_qualified_pitcher` | double |  |
| `fielding_successful_chances` | double |  |
| `fielding_total_chances` | double |  |
| `fielding_full_innings_played` | double |  |
| `fielding_part_innings_played` | double |  |
| `fielding_fielding_pct` | double |  |
| `fielding_range_factor` | double |  |
| `fielding_zone_rating` | double |  |
| `fielding_catcher_caught_stealing_pct` | double |  |
| `fielding_catcher_era` | double |  |
| `fielding_def_warbr` | double |  |
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
from sportsdataverse.mlb import espn_mlb_player_stats
df = espn_mlb_player_stats(athlete_id=33192, season=2023)
df.select(["full_name", "team_display_name", "batting_home_runs"])
```

### `espn_mlb_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_schedule - look up the MLB schedule for a given date or season-year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Date filter. Either a calendar date as YYYYMMDD or a season-year (e.g. 2024). When a 4-digit year is passed, the call returns the full season slate (paginated by ``limit``). |
| `season_type` | `int` | `None` | Season type — 1 = spring training, 2 = regular, 3 = postseason, 4 = all-star. |
| `limit` | `int` | `500` | Number of records to return. Default 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule. Returns ``None`` if no games.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `date` | character |  |
| `season_year` | integer |  |
| `season_type` | integer |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_city` | character |  |
| `venue_state` | character |  |
| `home_id` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_score` | character |  |
| `home_winner` | logical |  |
| `away_id` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_score` | character |  |
| `away_winner` | logical |  |

**Example**

```python
from sportsdataverse.mlb import espn_mlb_schedule
sched = espn_mlb_schedule(dates=20240328)
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

Pull a regular-season slate from a season-year::

reg = espn_mlb_schedule(dates=2024, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)

Pandas round-trip for one date::

espn_mlb_schedule(dates=20240328, return_as_pandas=True).head()
```

## Dataset loaders

### `load_mlb_pbp(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_pbp - planned: load pre-built season-level MLB play-by-play.

TODO: Implement once an MLB-data release pipeline is in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_player_boxscore(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_player_boxscore - planned: load pre-built season-level MLB player boxscores.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_rosters(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_rosters - planned: load pre-built season-level MLB rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_schedule(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_schedule - planned: load pre-built season-level MLB schedule.

TODO: Implement once an MLB-data release pipeline is in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### `load_mlb_team_boxscore(seasons: 'List[int]', return_as_pandas: 'bool' = False)`

load_mlb_team_boxscore - planned: load pre-built season-level MLB team boxscores.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

## Utilities & helpers

### `most_recent_mlb_season() -> 'int'`

most_recent_mlb_season - return the most recent / current MLB season year.

MLB seasons run calendar-year. Before April we still consider the *previous* year the "most recent" season (since spring training only starts in late February).

**Returns**

The most recent MLB season year (e.g. ``2024``).

## Other

### `espn_mlb_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mlb_teams - look up MLB teams from ESPN's Site v2 API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False (default), returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for MLB. This function caches by default, so if you want to refresh the data, use ``sportsdataverse.mlb.espn_mlb_teams.cache_clear()``.

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
from sportsdataverse.mlb import espn_mlb_teams
teams = espn_mlb_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

Find Los Angeles Dodgers (team_id 19)::

import polars as pl
teams.filter(pl.col("team_id") == "19").to_dicts()

Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas::

espn_mlb_teams.cache_clear()
teams_pd = espn_mlb_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```
