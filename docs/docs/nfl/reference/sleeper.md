---
title: NFL — Sleeper fantasy API v1 (api.sleeper.app)
sidebar_label: Sleeper fantasy API v1 (api.sleeper.app)
description: "NFL — Sleeper fantasy API v1 (api.sleeper.app) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 14
toc_max_heading_level: 2
---
# NFL — Sleeper fantasy API v1 (api.sleeper.app)

`sportsdataverse.nfl` — 15 endpoints.

## sleeper_draft

A draft.

**Endpoint URL:** `GET https://api.sleeper.app/v1/draft/{draft_id}`

**Valid URL:** [https://api.sleeper.app/v1/draft/257270643320426496](https://api.sleeper.app/v1/draft/257270643320426496)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `draft_id` | `draft_id` |  | `Y` |  | draft_id path parameter. |

### Returns {#sleeper_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `created` | Int64 |  |
| `creators` | String |  |
| `draft_id` | String | Sleeper draft id, as a string. |
| `draft_order` | String |  |
| `last_message_id` | String |  |
| `last_message_time` | Int64 |  |
| `last_picked` | Int64 |  |
| `league_id` | String | Sleeper league id the draft belongs to, as a string. |
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `slot_to_roster_id` | String |  |
| `sport` | String |  |
| `start_time` | Int64 | Kickoff time in eastern time zone. |
| `status` | String | Draft status (e.g. 'pre_draft', 'drafting', 'complete'). |
| `type` | String | Draft type (e.g. 'snake', 'linear', 'auction'). |
| `metadata_description` | String |  |
| `metadata_name` | String |  |
| `metadata_scoring_type` | String |  |
| `settings_pick_timer` | Int64 |  |
| `settings_rounds` | Int64 |  |
| `settings_slots_bn` | Int64 |  |
| `settings_slots_def` | Int64 |  |
| `settings_slots_flex` | Int64 |  |
| `settings_slots_k` | Int64 |  |
| `settings_slots_qb` | Int64 |  |
| `settings_slots_rb` | Int64 |  |
| `settings_slots_te` | Int64 |  |
| `settings_slots_wr` | Int64 |  |
| `settings_teams` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_draft-example}

```python
sleeper_draft(draft_id='257270643320426496')
```

## sleeper_draft_picks

Picks of a draft.

**Endpoint URL:** `GET https://api.sleeper.app/v1/draft/{draft_id}/picks`

**Valid URL:** [https://api.sleeper.app/v1/draft/257270643320426496/picks](https://api.sleeper.app/v1/draft/257270643320426496/picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `draft_id` | `draft_id` |  | `Y` |  | draft_id path parameter. |

### Returns {#sleeper_draft_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `draft_id` | String | Sleeper draft id the pick belongs to, as a string. |
| `draft_slot` | Int64 |  |
| `is_keeper` | String |  |
| `pick_no` | Int64 |  |
| `picked_by` | String |  |
| `player_id` | String | Sleeper player id of the drafted player, as a string. |
| `reactions` | String |  |
| `roster_id` | String |  |
| `round` | Int64 | Draft round the pick was made in (1 = first round). |
| `metadata_first_name` | String |  |
| `metadata_injury_status` | String |  |
| `metadata_last_name` | String |  |
| `metadata_news_updated` | String |  |
| `metadata_number` | String |  |
| `metadata_player_id` | String |  |
| `metadata_position` | String |  |
| `metadata_sport` | String |  |
| `metadata_status` | String |  |
| `metadata_team` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_draft_picks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_draft_picks-example}

```python
sleeper_draft_picks(draft_id='257270643320426496')
```

## sleeper_drafts

Drafts of a league.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/drafts`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/drafts](https://api.sleeper.app/v1/league/289646328504385536/drafts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |

### Returns {#sleeper_drafts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `created` | Int64 |  |
| `creators` | String |  |
| `draft_id` | String | Sleeper draft id, as a string. |
| `draft_order` | String |  |
| `last_message_id` | String |  |
| `last_message_time` | Int64 |  |
| `last_picked` | Int64 |  |
| `league_id` | String | Sleeper league id the draft belongs to, as a string. |
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `sport` | String |  |
| `start_time` | Int64 | Kickoff time in eastern time zone. |
| `status` | String | Draft status (e.g. 'pre_draft', 'drafting', 'complete'). |
| `type` | String | Draft type (e.g. 'snake', 'linear', 'auction'). |
| `metadata_description` | String |  |
| `metadata_name` | String |  |
| `metadata_scoring_type` | String |  |
| `settings_cpu_autopick` | Int64 |  |
| `settings_pick_timer` | Int64 |  |
| `settings_player_type` | Int64 |  |
| `settings_rounds` | Int64 |  |
| `settings_slots_bn` | Int64 |  |
| `settings_slots_def` | Int64 |  |
| `settings_slots_flex` | Int64 |  |
| `settings_slots_qb` | Int64 |  |
| `settings_slots_rb` | Int64 |  |
| `settings_slots_te` | Int64 |  |
| `settings_slots_wr` | Int64 |  |
| `settings_teams` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_drafts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_drafts-example}

```python
sleeper_drafts(league_id='289646328504385536')
```

## sleeper_league

A league.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536](https://api.sleeper.app/v1/league/289646328504385536)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |

### Returns {#sleeper_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `status` | String | League status (e.g. 'pre_draft', 'drafting', 'in_season', 'complete'). |
| `avatar` | String |  |
| `company_id` | String |  |
| `shard` | Int64 |  |
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `sport` | String |  |
| `last_message_id` | String |  |
| `last_author_avatar` | String |  |
| `last_author_display_name` | String |  |
| `last_author_id` | String |  |
| `last_author_is_bot` | String |  |
| `last_message_attachment` | String |  |
| `last_message_text_map` | String |  |
| `last_message_time` | Int64 |  |
| `last_pinned_message_id` | String |  |
| `last_read_id` | String |  |
| `draft_id` | String | Sleeper draft id of the league's draft, as a string. |
| `league_id` | String | Sleeper league id, as a string. |
| `previous_league_id` | String |  |
| `bracket_id` | String |  |
| `roster_positions` | String |  |
| `bracket_overrides_id` | String |  |
| `group_id` | String | Sleeper league-group id when the league belongs to a group; null otherwise. |
| `loser_bracket_id` | String |  |
| `loser_bracket_overrides_id` | String |  |
| `total_rosters` | Int64 |  |
| `metadata_trophy_loser` | String |  |
| `metadata_trophy_loser_background` | String |  |
| `metadata_trophy_loser_banner_text` | String |  |
| `metadata_trophy_winner` | String |  |
| `metadata_trophy_winner_background` | String |  |
| `metadata_trophy_winner_banner_text` | String |  |
| `settings_bench_lock` | Int64 |  |
| `settings_daily_waivers` | Int64 |  |
| `settings_daily_waivers_hour` | Int64 |  |
| `settings_draft_rounds` | Int64 |  |
| `settings_last_report` | Int64 |  |
| `settings_last_scored_leg` | Int64 |  |
| `settings_league_average_match` | Int64 |  |
| `settings_leg` | Int64 |  |
| `settings_max_keepers` | Int64 |  |
| `settings_num_teams` | Int64 |  |
| `settings_offseason_adds` | Int64 |  |
| `settings_pick_trading` | Int64 |  |
| `settings_playoff_teams` | Int64 |  |
| `settings_playoff_type` | Int64 |  |
| `settings_playoff_week_start` | Int64 |  |
| `settings_reserve_allow_doubtful` | Int64 |  |
| `settings_reserve_allow_out` | Int64 |  |
| `settings_reserve_allow_sus` | Int64 |  |
| `settings_reserve_slots` | Int64 |  |
| `settings_start_week` | Int64 |  |
| `settings_taxi_allow_vets` | Int64 |  |
| `settings_taxi_deadline` | Int64 |  |
| `settings_taxi_slots` | Int64 |  |
| `settings_taxi_years` | Int64 |  |
| `settings_trade_deadline` | Int64 |  |
| `settings_trade_review_days` | Int64 |  |
| `settings_type` | Int64 |  |
| `settings_waiver_budget` | Int64 |  |
| `settings_waiver_clear_days` | Int64 |  |
| `settings_waiver_day_of_week` | Int64 |  |
| `settings_waiver_type` | Int64 |  |
| `settings_was_auto_archived` | Int64 |  |
| `scoring_settings_sack` | Float64 |  |
| `scoring_settings_qb_hit` | Float64 |  |
| `scoring_settings_fgm_40_49` | Float64 |  |
| `scoring_settings_bonus_rec_yd_100` | Float64 |  |
| `scoring_settings_bonus_rush_yd_100` | Float64 |  |
| `scoring_settings_pass_int` | Float64 |  |
| `scoring_settings_pts_allow_0` | Float64 |  |
| `scoring_settings_bonus_pass_yd_400` | Float64 |  |
| `scoring_settings_pass_2pt` | Float64 |  |
| `scoring_settings_blk_kick_ret_yd` | Float64 |  |
| `scoring_settings_st_td` | Float64 |  |
| `scoring_settings_sack_yd` | Float64 |  |
| `scoring_settings_pr_td` | Float64 |  |
| `scoring_settings_rec_td` | Float64 |  |
| `scoring_settings_tkl_ast` | Float64 |  |
| `scoring_settings_fgm_30_39` | Float64 |  |
| `scoring_settings_kr_td` | Float64 |  |
| `scoring_settings_xpmiss` | Float64 |  |
| `scoring_settings_rush_td` | Float64 |  |
| `scoring_settings_fg_ret_yd` | Float64 |  |
| `scoring_settings_idp_tkl` | Float64 |  |
| `scoring_settings_fgm` | Float64 |  |
| `scoring_settings_idp_blk` | Float64 |  |
| `scoring_settings_rec_2pt` | Float64 |  |
| `scoring_settings_int_ret_yd` | Float64 |  |
| `scoring_settings_idp_tkl_solo` | Float64 |  |
| `scoring_settings_pass_att` | Float64 |  |
| `scoring_settings_st_fum_rec` | Float64 |  |
| `scoring_settings_ff` | Float64 |  |
| `scoring_settings_idp_int` | Float64 |  |
| `scoring_settings_fgmiss_30_39` | Float64 |  |
| `scoring_settings_rec` | Float64 |  |
| `scoring_settings_idp_safe` | Float64 |  |
| `scoring_settings_pts_allow_14_20` | Float64 |  |
| `scoring_settings_def_2pt` | Float64 |  |
| `scoring_settings_fgm_0_19` | Float64 |  |
| `scoring_settings_int` | Float64 |  |
| `scoring_settings_def_st_fum_rec` | Float64 |  |
| `scoring_settings_fum_lost` | Float64 |  |
| `scoring_settings_pts_allow_1_6` | Float64 |  |
| `scoring_settings_kr_yd` | Float64 |  |
| `scoring_settings_fgmiss_20_29` | Float64 |  |
| `scoring_settings_rush_att` | Float64 |  |
| `scoring_settings_st_tkl_solo` | Float64 |  |
| `scoring_settings_idp_sack` | Float64 |  |
| `scoring_settings_fgm_20_29` | Float64 |  |
| `scoring_settings_pts_allow_21_27` | Float64 |  |
| `scoring_settings_bonus_pass_yd_300` | Float64 |  |
| `scoring_settings_xpm` | Float64 |  |
| `scoring_settings_pass_sack` | Float64 |  |
| `scoring_settings_fgmiss_0_19` | Float64 |  |
| `scoring_settings_pass_cmp` | Float64 |  |
| `scoring_settings_tkl_loss` | Float64 |  |
| `scoring_settings_rush_2pt` | Float64 |  |
| `scoring_settings_def_pass_def` | Float64 |  |
| `scoring_settings_fum_rec` | Float64 |  |
| `scoring_settings_idp_pass_def` | Float64 |  |
| `scoring_settings_bonus_rec_yd_200` | Float64 |  |
| `scoring_settings_def_st_td` | Float64 |  |
| `scoring_settings_tkl` | Float64 |  |
| `scoring_settings_fgm_50p` | Float64 |  |
| `scoring_settings_def_td` | Float64 |  |
| `scoring_settings_idp_fum_rec` | Float64 |  |
| `scoring_settings_bonus_rush_yd_200` | Float64 |  |
| `scoring_settings_safe` | Float64 |  |
| `scoring_settings_pass_yd` | Float64 |  |
| `scoring_settings_blk_kick` | Float64 |  |
| `scoring_settings_pass_td` | Float64 |  |
| `scoring_settings_tkl_solo` | Float64 |  |
| `scoring_settings_rush_yd` | Float64 |  |
| `scoring_settings_pr_yd` | Float64 |  |
| `scoring_settings_fum` | Float64 |  |
| `scoring_settings_pts_allow_28_34` | Float64 |  |
| `scoring_settings_pts_allow_35p` | Float64 |  |
| `scoring_settings_rec_yd` | Float64 |  |
| `scoring_settings_fum_ret_yd` | Float64 |  |
| `scoring_settings_def_st_ff` | Float64 |  |
| `scoring_settings_pts_allow_7_13` | Float64 |  |
| `scoring_settings_idp_ff` | Float64 |  |
| `scoring_settings_st_ff` | Float64 |  |
| `scoring_settings_idp_tkl_ast` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_league-example}

```python
sleeper_league(league_id='289646328504385536')
```

## sleeper_matchups

Matchups for a week.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/matchups/{week}`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/matchups/1](https://api.sleeper.app/v1/league/289646328504385536/matchups/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#sleeper_matchups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `points` | Float64 | Fantasy points the roster scored in the week under the league's scoring settings. |
| `players` | String | Stringified list of Sleeper player ids on the roster for the week (starters and bench). |
| `roster_id` | String |  |
| `custom_points` | String |  |
| `matchup_id` | String |  |
| `starters` | String |  |
| `starters_points` | String |  |
| `players_points` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_matchups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_matchups-example}

```python
sleeper_matchups(league_id='289646328504385536', week='1')
```

## sleeper_players

All NFL players (large; fetch at most once per day).

**Endpoint URL:** `GET https://api.sleeper.app/v1/players/nfl`

**Valid URL:** [https://api.sleeper.app/v1/players/nfl](https://api.sleeper.app/v1/players/nfl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sleeper_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Player id in the source's own namespace, as a string. |
| `weight` | String | Player's weight as the source lists it (pounds for US sources). |
| `birth_state` | String | State or region of birth, where the source lists it. |
| `birth_country` | String | Country of birth, where the source lists it. |
| `team_abbr` | String | Abbreviation of the player's current team; null for a free agent or retired player. |
| `rotoworld_id` | String | Rotoworld player id, as a string. |
| `news_updated` | String |  |
| `sportradar_id` | String | Sportradar player id (a UUID). |
| `team_changed_at` | String |  |
| `team` | String | Current team of the player as the source ships it (a nested team object, stringified, or a team code). |
| `depth_chart_position` | String | Position slot on the team depth chart, where the source lists it. |
| `swish_id` | String | Swish Analytics player id, as a string. |
| `active` | Boolean | Whether the player record is active with the source (e.g. on a current roster). |
| `last_name` | String | Player's last name as the source lists it. |
| `injury_body_part` | String |  |
| `practice_participation` | String |  |
| `injury_status` | String | Current injury designation (e.g. 'Questionable', 'Out'); null when healthy or unknown. |
| `college` | String | College the player attended, as the source lists it. |
| `search_first_name` | String |  |
| `rotowire_id` | String | RotoWire player id, as a string. |
| `injury_notes` | String |  |
| `search_full_name` | String |  |
| `gsis_id` | String | NFL GSIS player id (e.g. '00-0033873'), the nflverse play-by-play key. |
| `years_exp` | Int64 | Years of professional experience. |
| `status` | String | Player status as the source lists it (e.g. 'Active', 'Inactive'). |
| `pandascore_id` | String |  |
| `number` | Int64 | Jersey number as the source lists it. |
| `injury_start_date` | String |  |
| `metadata` | String |  |
| `height` | String | Player's height as the source encodes it (PFF uses feet and inches without a separator, 602 = 6'02"; others use inches or centimetres). |
| `search_rank` | Int64 |  |
| `fantasy_data_id` | String | FantasyData player id, as a string. |
| `espn_id` | String | ESPN athlete id, as a string. |
| `sport` | String |  |
| `search_last_name` | String |  |
| `competitions` | String |  |
| `hashtag` | String |  |
| `stats_id` | String | STATS (Stats Perform) player id, as a string. |
| `practice_description` | String |  |
| `oddsjam_id` | String |  |
| `full_name` | String | Player's full name as the source lists it. |
| `fantasy_positions` | String |  |
| `age` | Int64 | Player's age in years at capture (string), where the source lists it. |
| `first_name` | String | Player's first name as the source lists it. |
| `player_shard` | String |  |
| `position` | String | Position abbreviation as the source lists it (e.g. QB, WR). |
| `high_school` | String | High school the player attended, with its state in parentheses where the source lists it (e.g. 'Killian (FL)'). |
| `birth_city` | String | City of birth, where the source lists it. |
| `yahoo_id` | String | Yahoo player id, as a string. |
| `depth_chart_order` | String |  |
| `birth_date` | String | Player's date of birth (YYYY-MM-DD), where the source lists it. |
| `kalshi_id` | String |  |
| `opta_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_players-example}

```python
sleeper_players()
```

## sleeper_rosters

Rosters in a league.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/rosters`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/rosters](https://api.sleeper.app/v1/league/289646328504385536/rosters)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |

### Returns {#sleeper_rosters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `co_owners` | String |  |
| `keepers` | String |  |
| `league_id` | String | Sleeper league id the roster belongs to, as a string. |
| `metadata` | String |  |
| `owner_id` | String |  |
| `player_map` | String |  |
| `players` | String | Stringified list of Sleeper player ids on the roster (starters, bench and reserve). |
| `reserve` | String |  |
| `roster_id` | String |  |
| `starters` | String |  |
| `taxi` | String |  |
| `settings_fpts` | Int64 |  |
| `settings_fpts_against` | Int64 |  |
| `settings_fpts_against_decimal` | Int64 |  |
| `settings_fpts_decimal` | Int64 |  |
| `settings_losses` | Int64 |  |
| `settings_ppts` | Int64 |  |
| `settings_ppts_decimal` | Int64 |  |
| `settings_ties` | Int64 |  |
| `settings_total_moves` | Int64 |  |
| `settings_waiver_budget_used` | Int64 |  |
| `settings_waiver_position` | Int64 |  |
| `settings_wins` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_rosters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_rosters-example}

```python
sleeper_rosters(league_id='289646328504385536')
```

## sleeper_state

Current NFL week and season state.

**Endpoint URL:** `GET https://api.sleeper.app/v1/state/nfl`

**Valid URL:** [https://api.sleeper.app/v1/state/nfl](https://api.sleeper.app/v1/state/nfl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sleeper_state-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `week` | Int64 | Season week. |
| `leg` | Int64 |  |
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `league_season` | String | Season the Sleeper league year currently points at, as a string (e.g. '2026'). |
| `previous_season` | String |  |
| `season_start_date` | String | First day of the current Sleeper season (YYYY-MM-DD). |
| `display_week` | Int64 |  |
| `league_create_season` | String |  |
| `season_has_scores` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_state-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_state-example}

```python
sleeper_state()
```

## sleeper_traded_picks

Traded draft picks.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/traded_picks`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/traded_picks](https://api.sleeper.app/v1/league/289646328504385536/traded_picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |

### Returns {#sleeper_traded_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round` | Int64 | Draft round |
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `roster_id` | String |  |
| `owner_id` | String |  |
| `previous_owner_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_traded_picks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_traded_picks-example}

```python
sleeper_traded_picks(league_id='289646328504385536')
```

## sleeper_transactions

Transactions for a week.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/transactions/{week}`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/transactions/1](https://api.sleeper.app/v1/league/289646328504385536/transactions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `week` | `week` |  | `Y` |  | week path parameter. |

### Returns {#sleeper_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `status` | String | Transaction status (e.g. 'complete', 'failed'). |
| `type` | String | Transaction type (e.g. 'waiver', 'free_agent', 'trade'). |
| `created` | Int64 |  |
| `leg` | Int64 |  |
| `draft_picks` | String |  |
| `creator` | String |  |
| `transaction_id` | String | Sleeper transaction id, as a string. |
| `adds` | String |  |
| `consenter_ids` | String |  |
| `drops` | String | Throws dropped |
| `roster_ids` | String |  |
| `status_updated` | Int64 |  |
| `waiver_budget` | String |  |
| `metadata_notes` | String |  |
| `settings_waiver_bid` | Int64 |  |
| `settings_priority` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_transactions-example}

```python
sleeper_transactions(league_id='289646328504385536', week='1')
```

## sleeper_trending_adds

Trending adds (lookback_hours, limit).

**Endpoint URL:** `GET https://api.sleeper.app/v1/players/nfl/trending/add`

**Valid URL:** [https://api.sleeper.app/v1/players/nfl/trending/add](https://api.sleeper.app/v1/players/nfl/trending/add)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lookback_hours` | `lookback_hours` |  |  | `Y` | Hours to look back (default 24). |
| `limit` | `limit` |  |  | `Y` | Number of players (default 25). |

### Returns {#sleeper_trending_adds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | Int64 | Number of Sleeper leagues that added the player over the lookback window. |
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_trending_adds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_trending_adds-example}

```python
sleeper_trending_adds()
```

## sleeper_user

Look up a user by username or id.

**Endpoint URL:** `GET https://api.sleeper.app/v1/user/{username}`

**Valid URL:** [https://api.sleeper.app/v1/user/457511950237696](https://api.sleeper.app/v1/user/457511950237696)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `username` | `username` |  | `Y` |  | username path parameter. |

### Returns {#sleeper_user-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `avatar` | String |  |
| `cookies` | String |  |
| `created` | String |  |
| `currencies` | String |  |
| `data_updated` | String |  |
| `deleted` | String |  |
| `display_name` | String | Full name of player |
| `email` | String |  |
| `is_bot` | String |  |
| `metadata` | String |  |
| `notifications` | String |  |
| `pending` | String |  |
| `phone` | String |  |
| `real_name` | String |  |
| `solicitable` | String |  |
| `summoner_name` | String |  |
| `summoner_region` | String |  |
| `token` | String |  |
| `user_id` | String |  |
| `username` | String |  |
| `verification` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_user-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_user-example}

```python
sleeper_user(username='457511950237696')
```

## sleeper_user_leagues

Leagues a user is in for a season.

**Endpoint URL:** `GET https://api.sleeper.app/v1/user/{user_id}/leagues/nfl/{season}`

**Valid URL:** [https://api.sleeper.app/v1/user/457511950237696/leagues/nfl/2018](https://api.sleeper.app/v1/user/457511950237696/leagues/nfl/2018)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `user_id` | `user_id` |  | `Y` |  | user_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#sleeper_user_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `last_pinned_message_id` | String |  |
| `season` | String | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `group_id` | String | Sleeper league-group id when the league belongs to a group; null otherwise. |
| `loser_bracket_id` | String |  |
| `last_author_display_name` | String |  |
| `total_rosters` | Int64 |  |
| `last_message_id` | String |  |
| `draft_id` | String | Sleeper draft id of the league's draft, as a string. |
| `previous_league_id` | String |  |
| `sport` | String |  |
| `bracket_id` | String |  |
| `bracket_overrides_id` | String |  |
| `last_message_text_map` | String |  |
| `status` | String | League status (e.g. 'pre_draft', 'drafting', 'in_season', 'complete'). |
| `last_message_attachment` | String |  |
| `display_order` | Int64 | Position of the bracket slot within its round, controlling top-to-bottom rendering. |
| `league_id` | String | Sleeper league id, as a string. |
| `last_author_is_bot` | String |  |
| `name` | String | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `company_id` | String |  |
| `last_author_id` | String |  |
| `roster_positions` | String |  |
| `last_message_time` | Int64 |  |
| `last_read_id` | String |  |
| `loser_bracket_overrides_id` | String |  |
| `last_author_avatar` | String |  |
| `last_transaction_id` | String |  |
| `shard` | Int64 |  |
| `avatar` | String |  |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `metadata_trophy_winner` | String |  |
| `metadata_trophy_winner_background` | String |  |
| `metadata_trophy_winner_banner_text` | String |  |
| `settings_bench_lock` | Int64 |  |
| `settings_daily_waivers` | Int64 |  |
| `settings_daily_waivers_hour` | Int64 |  |
| `settings_draft_rounds` | Int64 |  |
| `settings_last_report` | Int64 |  |
| `settings_last_scored_leg` | Int64 |  |
| `settings_league_average_match` | Int64 |  |
| `settings_leg` | Int64 |  |
| `settings_max_keepers` | Int64 |  |
| `settings_num_teams` | Int64 |  |
| `settings_offseason_adds` | Int64 |  |
| `settings_pick_trading` | Int64 |  |
| `settings_playoff_teams` | Int64 |  |
| `settings_playoff_week_start` | Int64 |  |
| `settings_reserve_allow_doubtful` | Int64 |  |
| `settings_reserve_allow_out` | Int64 |  |
| `settings_reserve_allow_sus` | Int64 |  |
| `settings_reserve_slots` | Int64 |  |
| `settings_start_week` | Int64 |  |
| `settings_taxi_allow_vets` | Int64 |  |
| `settings_taxi_deadline` | Int64 |  |
| `settings_taxi_slots` | Int64 |  |
| `settings_taxi_years` | Int64 |  |
| `settings_trade_deadline` | Int64 |  |
| `settings_trade_review_days` | Int64 |  |
| `settings_type` | Int64 |  |
| `settings_waiver_budget` | Int64 |  |
| `settings_waiver_clear_days` | Int64 |  |
| `settings_waiver_day_of_week` | Int64 |  |
| `settings_waiver_type` | Int64 |  |
| `settings_was_auto_archived` | Int64 |  |
| `scoring_settings_sack` | Float64 |  |
| `scoring_settings_fgm_40_49` | Float64 |  |
| `scoring_settings_bonus_rec_te` | Float64 |  |
| `scoring_settings_fgm_yds` | Float64 |  |
| `scoring_settings_pass_int` | Float64 |  |
| `scoring_settings_pts_allow_0` | Float64 |  |
| `scoring_settings_pass_2pt` | Float64 |  |
| `scoring_settings_st_td` | Float64 |  |
| `scoring_settings_fgm_yds_over_30` | Float64 |  |
| `scoring_settings_rec_td` | Float64 |  |
| `scoring_settings_fgm_30_39` | Float64 |  |
| `scoring_settings_xpmiss` | Float64 |  |
| `scoring_settings_rush_td` | Float64 |  |
| `scoring_settings_fgm` | Float64 |  |
| `scoring_settings_rec_2pt` | Float64 |  |
| `scoring_settings_rush_fd` | Float64 |  |
| `scoring_settings_st_fum_rec` | Float64 |  |
| `scoring_settings_fgmiss` | Float64 |  |
| `scoring_settings_ff` | Float64 |  |
| `scoring_settings_fgmiss_30_39` | Float64 |  |
| `scoring_settings_rec` | Float64 |  |
| `scoring_settings_pts_allow_14_20` | Float64 |  |
| `scoring_settings_fgm_0_19` | Float64 |  |
| `scoring_settings_int` | Float64 |  |
| `scoring_settings_def_st_fum_rec` | Float64 |  |
| `scoring_settings_fum_lost` | Float64 |  |
| `scoring_settings_pts_allow_1_6` | Float64 |  |
| `scoring_settings_rec_fd` | Float64 |  |
| `scoring_settings_fgmiss_20_29` | Float64 |  |
| `scoring_settings_fgm_20_29` | Float64 |  |
| `scoring_settings_pts_allow_21_27` | Float64 |  |
| `scoring_settings_bonus_rec_wr` | Float64 |  |
| `scoring_settings_xpm` | Float64 |  |
| `scoring_settings_fgmiss_0_19` | Float64 |  |
| `scoring_settings_rush_2pt` | Float64 |  |
| `scoring_settings_fum_rec` | Float64 |  |
| `scoring_settings_def_st_td` | Float64 |  |
| `scoring_settings_fgm_50p` | Float64 |  |
| `scoring_settings_def_td` | Float64 |  |
| `scoring_settings_safe` | Float64 |  |
| `scoring_settings_pass_yd` | Float64 |  |
| `scoring_settings_fgmiss_40_49` | Float64 |  |
| `scoring_settings_blk_kick` | Float64 |  |
| `scoring_settings_pass_td` | Float64 |  |
| `scoring_settings_rush_yd` | Float64 |  |
| `scoring_settings_fum` | Float64 |  |
| `scoring_settings_pts_allow_28_34` | Float64 |  |
| `scoring_settings_pts_allow_35p` | Float64 |  |
| `scoring_settings_rec_yd` | Float64 |  |
| `scoring_settings_def_st_ff` | Float64 |  |
| `scoring_settings_pts_allow_7_13` | Float64 |  |
| `scoring_settings_st_ff` | Float64 |  |
| `metadata_trophy_loser` | String |  |
| `metadata_trophy_loser_background` | String |  |
| `metadata_trophy_loser_banner_text` | String |  |
| `settings_playoff_type` | Float64 |  |
| `scoring_settings_qb_hit` | Float64 |  |
| `scoring_settings_bonus_rec_yd_100` | Float64 |  |
| `scoring_settings_bonus_rush_yd_100` | Float64 |  |
| `scoring_settings_bonus_pass_yd_400` | Float64 |  |
| `scoring_settings_blk_kick_ret_yd` | Float64 |  |
| `scoring_settings_sack_yd` | Float64 |  |
| `scoring_settings_pr_td` | Float64 |  |
| `scoring_settings_tkl_ast` | Float64 |  |
| `scoring_settings_kr_td` | Float64 |  |
| `scoring_settings_fg_ret_yd` | Float64 |  |
| `scoring_settings_idp_tkl` | Float64 |  |
| `scoring_settings_idp_blk` | Float64 |  |
| `scoring_settings_int_ret_yd` | Float64 |  |
| `scoring_settings_idp_tkl_solo` | Float64 |  |
| `scoring_settings_pass_att` | Float64 |  |
| `scoring_settings_idp_int` | Float64 |  |
| `scoring_settings_idp_safe` | Float64 |  |
| `scoring_settings_def_2pt` | Float64 |  |
| `scoring_settings_kr_yd` | Float64 |  |
| `scoring_settings_rush_att` | Float64 |  |
| `scoring_settings_st_tkl_solo` | Float64 |  |
| `scoring_settings_idp_sack` | Float64 |  |
| `scoring_settings_bonus_pass_yd_300` | Float64 |  |
| `scoring_settings_pass_sack` | Float64 |  |
| `scoring_settings_pass_cmp` | Float64 |  |
| `scoring_settings_tkl_loss` | Float64 |  |
| `scoring_settings_def_pass_def` | Float64 |  |
| `scoring_settings_idp_pass_def` | Float64 |  |
| `scoring_settings_bonus_rec_yd_200` | Float64 |  |
| `scoring_settings_tkl` | Float64 |  |
| `scoring_settings_idp_fum_rec` | Float64 |  |
| `scoring_settings_bonus_rush_yd_200` | Float64 |  |
| `scoring_settings_tkl_solo` | Float64 |  |
| `scoring_settings_pr_yd` | Float64 |  |
| `scoring_settings_fum_ret_yd` | Float64 |  |
| `scoring_settings_idp_ff` | Float64 |  |
| `scoring_settings_idp_tkl_ast` | Float64 |  |
| `metadata` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_user_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_user_leagues-example}

```python
sleeper_user_leagues(season='2018', user_id='457511950237696')
```

## sleeper_users

Users in a league.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/users`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/users](https://api.sleeper.app/v1/league/289646328504385536/users)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |

### Returns {#sleeper_users-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `avatar` | String |  |
| `display_name` | String | Full name of player |
| `is_bot` | String |  |
| `is_owner` | Boolean |  |
| `league_id` | String | Sleeper league id the user belongs to, as a string. |
| `settings` | String |  |
| `user_id` | String |  |
| `metadata_allow_pn` | String |  |
| `metadata_mascot_item_type_id_leg_16` | String |  |
| `metadata_mascot_item_type_id_leg_17` | String |  |
| `metadata_mention_pn` | String |  |
| `metadata_player_nickname_update` | String |  |
| `metadata_team_name` | String |  |
| `metadata_team_name_update` | String |  |
| `metadata_transaction_commissioner` | String |  |
| `metadata_transaction_free_agent` | String |  |
| `metadata_transaction_trade` | String |  |
| `metadata_transaction_waiver` | String |  |
| `metadata_user_message_pn` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_users-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_users-example}

```python
sleeper_users(league_id='289646328504385536')
```

## sleeper_winners_bracket

Playoff bracket.

**Endpoint URL:** `GET https://api.sleeper.app/v1/league/{league_id}/winners_bracket`

**Valid URL:** [https://api.sleeper.app/v1/league/289646328504385536/winners_bracket](https://api.sleeper.app/v1/league/289646328504385536/winners_bracket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |

### Returns {#sleeper_winners_bracket-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `m` | Int64 | Matchup id within the bracket round. |
| `r` | Int64 | Playoff round number of the bracket matchup (1 = first round). |
| `l` | Int64 | Roster id of the matchup loser; null until played. |
| `w` | Int64 | Roster id of the matchup winner; null until played. |
| `t1` | Int64 |  |
| `t2` | Int64 |  |
| `t2_from_w` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sleeper_winners_bracket-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The Sleeper API returned 404 (unknown user, league or draft id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sleeper_winners_bracket-example}

```python
sleeper_winners_bracket(league_id='289646328504385536')
```
