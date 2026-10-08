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
| `created` | integer |  |
| `creators` | character |  |
| `draft_id` | character | Sleeper draft id, as a string. |
| `draft_order` | character |  |
| `last_message_id` | character |  |
| `last_message_time` | integer |  |
| `last_picked` | integer |  |
| `league_id` | character | Sleeper league id the draft belongs to, as a string. |
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `slot_to_roster_id` | character |  |
| `sport` | character |  |
| `start_time` | integer | Kickoff time in eastern time zone. |
| `status` | character | Draft status (e.g. 'pre_draft', 'drafting', 'complete'). |
| `type` | character | Draft type (e.g. 'snake', 'linear', 'auction'). |
| `metadata_description` | character |  |
| `metadata_name` | character |  |
| `metadata_scoring_type` | character |  |
| `settings_pick_timer` | integer |  |
| `settings_rounds` | integer |  |
| `settings_slots_bn` | integer |  |
| `settings_slots_def` | integer |  |
| `settings_slots_flex` | integer |  |
| `settings_slots_k` | integer |  |
| `settings_slots_qb` | integer |  |
| `settings_slots_rb` | integer |  |
| `settings_slots_te` | integer |  |
| `settings_slots_wr` | integer |  |
| `settings_teams` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_draft-example}

```python
sleeper_draft(draft_id='257270643320426496')
```

_Last validated n/a._

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
| `draft_id` | character | Sleeper draft id the pick belongs to, as a string. |
| `draft_slot` | integer |  |
| `is_keeper` | character |  |
| `pick_no` | integer |  |
| `picked_by` | character |  |
| `player_id` | character | Sleeper player id of the drafted player, as a string. |
| `reactions` | character |  |
| `roster_id` | character |  |
| `round` | integer | Draft round the pick was made in (1 = first round). |
| `metadata_first_name` | character |  |
| `metadata_injury_status` | character |  |
| `metadata_last_name` | character |  |
| `metadata_news_updated` | character |  |
| `metadata_number` | character |  |
| `metadata_player_id` | character |  |
| `metadata_position` | character |  |
| `metadata_sport` | character |  |
| `metadata_status` | character |  |
| `metadata_team` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_draft_picks-example}

```python
sleeper_draft_picks(draft_id='257270643320426496')
```

_Last validated n/a._

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
| `created` | integer |  |
| `creators` | character |  |
| `draft_id` | character | Sleeper draft id, as a string. |
| `draft_order` | character |  |
| `last_message_id` | character |  |
| `last_message_time` | integer |  |
| `last_picked` | integer |  |
| `league_id` | character | Sleeper league id the draft belongs to, as a string. |
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `sport` | character |  |
| `start_time` | integer | Kickoff time in eastern time zone. |
| `status` | character | Draft status (e.g. 'pre_draft', 'drafting', 'complete'). |
| `type` | character | Draft type (e.g. 'snake', 'linear', 'auction'). |
| `metadata_description` | character |  |
| `metadata_name` | character |  |
| `metadata_scoring_type` | character |  |
| `settings_cpu_autopick` | integer |  |
| `settings_pick_timer` | integer |  |
| `settings_player_type` | integer |  |
| `settings_rounds` | integer |  |
| `settings_slots_bn` | integer |  |
| `settings_slots_def` | integer |  |
| `settings_slots_flex` | integer |  |
| `settings_slots_qb` | integer |  |
| `settings_slots_rb` | integer |  |
| `settings_slots_te` | integer |  |
| `settings_slots_wr` | integer |  |
| `settings_teams` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_drafts-example}

```python
sleeper_drafts(league_id='289646328504385536')
```

_Last validated n/a._

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
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `status` | character | League status (e.g. 'pre_draft', 'drafting', 'in_season', 'complete'). |
| `avatar` | character |  |
| `company_id` | character |  |
| `shard` | integer |  |
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `sport` | character |  |
| `last_message_id` | character |  |
| `last_author_avatar` | character |  |
| `last_author_display_name` | character |  |
| `last_author_id` | character |  |
| `last_author_is_bot` | character |  |
| `last_message_attachment` | character |  |
| `last_message_text_map` | character |  |
| `last_message_time` | integer |  |
| `last_pinned_message_id` | character |  |
| `last_read_id` | character |  |
| `draft_id` | character | Sleeper draft id of the league's draft, as a string. |
| `league_id` | character | Sleeper league id, as a string. |
| `previous_league_id` | character |  |
| `bracket_id` | character |  |
| `roster_positions` | character |  |
| `bracket_overrides_id` | character |  |
| `group_id` | character | Sleeper league-group id when the league belongs to a group; null otherwise. |
| `loser_bracket_id` | character |  |
| `loser_bracket_overrides_id` | character |  |
| `total_rosters` | integer |  |
| `metadata_trophy_loser` | character |  |
| `metadata_trophy_loser_background` | character |  |
| `metadata_trophy_loser_banner_text` | character |  |
| `metadata_trophy_winner` | character |  |
| `metadata_trophy_winner_background` | character |  |
| `metadata_trophy_winner_banner_text` | character |  |
| `settings_bench_lock` | integer |  |
| `settings_daily_waivers` | integer |  |
| `settings_daily_waivers_hour` | integer |  |
| `settings_draft_rounds` | integer |  |
| `settings_last_report` | integer |  |
| `settings_last_scored_leg` | integer |  |
| `settings_league_average_match` | integer |  |
| `settings_leg` | integer |  |
| `settings_max_keepers` | integer |  |
| `settings_num_teams` | integer |  |
| `settings_offseason_adds` | integer |  |
| `settings_pick_trading` | integer |  |
| `settings_playoff_teams` | integer |  |
| `settings_playoff_type` | integer |  |
| `settings_playoff_week_start` | integer |  |
| `settings_reserve_allow_doubtful` | integer |  |
| `settings_reserve_allow_out` | integer |  |
| `settings_reserve_allow_sus` | integer |  |
| `settings_reserve_slots` | integer |  |
| `settings_start_week` | integer |  |
| `settings_taxi_allow_vets` | integer |  |
| `settings_taxi_deadline` | integer |  |
| `settings_taxi_slots` | integer |  |
| `settings_taxi_years` | integer |  |
| `settings_trade_deadline` | integer |  |
| `settings_trade_review_days` | integer |  |
| `settings_type` | integer |  |
| `settings_waiver_budget` | integer |  |
| `settings_waiver_clear_days` | integer |  |
| `settings_waiver_day_of_week` | integer |  |
| `settings_waiver_type` | integer |  |
| `settings_was_auto_archived` | integer |  |
| `scoring_settings_sack` | numeric |  |
| `scoring_settings_qb_hit` | numeric |  |
| `scoring_settings_fgm_40_49` | numeric |  |
| `scoring_settings_bonus_rec_yd_100` | numeric |  |
| `scoring_settings_bonus_rush_yd_100` | numeric |  |
| `scoring_settings_pass_int` | numeric |  |
| `scoring_settings_pts_allow_0` | numeric |  |
| `scoring_settings_bonus_pass_yd_400` | numeric |  |
| `scoring_settings_pass_2pt` | numeric |  |
| `scoring_settings_blk_kick_ret_yd` | numeric |  |
| `scoring_settings_st_td` | numeric |  |
| `scoring_settings_sack_yd` | numeric |  |
| `scoring_settings_pr_td` | numeric |  |
| `scoring_settings_rec_td` | numeric |  |
| `scoring_settings_tkl_ast` | numeric |  |
| `scoring_settings_fgm_30_39` | numeric |  |
| `scoring_settings_kr_td` | numeric |  |
| `scoring_settings_xpmiss` | numeric |  |
| `scoring_settings_rush_td` | numeric |  |
| `scoring_settings_fg_ret_yd` | numeric |  |
| `scoring_settings_idp_tkl` | numeric |  |
| `scoring_settings_fgm` | numeric |  |
| `scoring_settings_idp_blk` | numeric |  |
| `scoring_settings_rec_2pt` | numeric |  |
| `scoring_settings_int_ret_yd` | numeric |  |
| `scoring_settings_idp_tkl_solo` | numeric |  |
| `scoring_settings_pass_att` | numeric |  |
| `scoring_settings_st_fum_rec` | numeric |  |
| `scoring_settings_ff` | numeric |  |
| `scoring_settings_idp_int` | numeric |  |
| `scoring_settings_fgmiss_30_39` | numeric |  |
| `scoring_settings_rec` | numeric |  |
| `scoring_settings_idp_safe` | numeric |  |
| `scoring_settings_pts_allow_14_20` | numeric |  |
| `scoring_settings_def_2pt` | numeric |  |
| `scoring_settings_fgm_0_19` | numeric |  |
| `scoring_settings_int` | numeric |  |
| `scoring_settings_def_st_fum_rec` | numeric |  |
| `scoring_settings_fum_lost` | numeric |  |
| `scoring_settings_pts_allow_1_6` | numeric |  |
| `scoring_settings_kr_yd` | numeric |  |
| `scoring_settings_fgmiss_20_29` | numeric |  |
| `scoring_settings_rush_att` | numeric |  |
| `scoring_settings_st_tkl_solo` | numeric |  |
| `scoring_settings_idp_sack` | numeric |  |
| `scoring_settings_fgm_20_29` | numeric |  |
| `scoring_settings_pts_allow_21_27` | numeric |  |
| `scoring_settings_bonus_pass_yd_300` | numeric |  |
| `scoring_settings_xpm` | numeric |  |
| `scoring_settings_pass_sack` | numeric |  |
| `scoring_settings_fgmiss_0_19` | numeric |  |
| `scoring_settings_pass_cmp` | numeric |  |
| `scoring_settings_tkl_loss` | numeric |  |
| `scoring_settings_rush_2pt` | numeric |  |
| `scoring_settings_def_pass_def` | numeric |  |
| `scoring_settings_fum_rec` | numeric |  |
| `scoring_settings_idp_pass_def` | numeric |  |
| `scoring_settings_bonus_rec_yd_200` | numeric |  |
| `scoring_settings_def_st_td` | numeric |  |
| `scoring_settings_tkl` | numeric |  |
| `scoring_settings_fgm_50p` | numeric |  |
| `scoring_settings_def_td` | numeric |  |
| `scoring_settings_idp_fum_rec` | numeric |  |
| `scoring_settings_bonus_rush_yd_200` | numeric |  |
| `scoring_settings_safe` | numeric |  |
| `scoring_settings_pass_yd` | numeric |  |
| `scoring_settings_blk_kick` | numeric |  |
| `scoring_settings_pass_td` | numeric |  |
| `scoring_settings_tkl_solo` | numeric |  |
| `scoring_settings_rush_yd` | numeric |  |
| `scoring_settings_pr_yd` | numeric |  |
| `scoring_settings_fum` | numeric |  |
| `scoring_settings_pts_allow_28_34` | numeric |  |
| `scoring_settings_pts_allow_35p` | numeric |  |
| `scoring_settings_rec_yd` | numeric |  |
| `scoring_settings_fum_ret_yd` | numeric |  |
| `scoring_settings_def_st_ff` | numeric |  |
| `scoring_settings_pts_allow_7_13` | numeric |  |
| `scoring_settings_idp_ff` | numeric |  |
| `scoring_settings_st_ff` | numeric |  |
| `scoring_settings_idp_tkl_ast` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_league-example}

```python
sleeper_league(league_id='289646328504385536')
```

_Last validated n/a._

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
| `points` | numeric | Fantasy points the roster scored in the week under the league's scoring settings. |
| `players` | character | Stringified list of Sleeper player ids on the roster for the week (starters and bench). |
| `roster_id` | character |  |
| `custom_points` | character |  |
| `matchup_id` | character |  |
| `starters` | character |  |
| `starters_points` | character |  |
| `players_points` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_matchups-example}

```python
sleeper_matchups(league_id='289646328504385536', week='1')
```

_Last validated n/a._

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
| `player_id` | character | Player id in the source's own namespace, as a string. |
| `weight` | character | Player's weight as the source lists it (pounds for US sources). |
| `birth_state` | character | State or region of birth, where the source lists it. |
| `birth_country` | character | Country of birth, where the source lists it. |
| `team_abbr` | character | Abbreviation of the player's current team; null for a free agent or retired player. |
| `rotoworld_id` | character | Rotoworld player id, as a string. |
| `news_updated` | character |  |
| `sportradar_id` | character | Sportradar player id (a UUID). |
| `team_changed_at` | character |  |
| `team` | character | Current team of the player as the source ships it (a nested team object, stringified, or a team code). |
| `depth_chart_position` | character | Position slot on the team depth chart, where the source lists it. |
| `swish_id` | character | Swish Analytics player id, as a string. |
| `active` | logical | Whether the player record is active with the source (e.g. on a current roster). |
| `last_name` | character | Player's last name as the source lists it. |
| `injury_body_part` | character |  |
| `practice_participation` | character |  |
| `injury_status` | character | Current injury designation (e.g. 'Questionable', 'Out'); null when healthy or unknown. |
| `college` | character | College the player attended, as the source lists it. |
| `search_first_name` | character |  |
| `rotowire_id` | character | RotoWire player id, as a string. |
| `injury_notes` | character |  |
| `search_full_name` | character |  |
| `gsis_id` | character | NFL GSIS player id (e.g. '00-0033873'), the nflverse play-by-play key. |
| `years_exp` | integer | Years of professional experience. |
| `status` | character | Player status as the source lists it (e.g. 'Active', 'Inactive'). |
| `pandascore_id` | character |  |
| `number` | integer | Jersey number as the source lists it. |
| `injury_start_date` | character |  |
| `metadata` | character |  |
| `height` | character | Player's height as the source encodes it (PFF uses feet and inches without a separator, 602 = 6'02"; others use inches or centimetres). |
| `search_rank` | integer |  |
| `fantasy_data_id` | character | FantasyData player id, as a string. |
| `espn_id` | character | ESPN athlete id, as a string. |
| `sport` | character |  |
| `search_last_name` | character |  |
| `competitions` | character |  |
| `hashtag` | character |  |
| `stats_id` | character | STATS (Stats Perform) player id, as a string. |
| `practice_description` | character |  |
| `oddsjam_id` | character |  |
| `full_name` | character | Player's full name as the source lists it. |
| `fantasy_positions` | character |  |
| `age` | integer | Player's age in years at capture (string), where the source lists it. |
| `first_name` | character | Player's first name as the source lists it. |
| `player_shard` | character |  |
| `position` | character | Position abbreviation as the source lists it (e.g. QB, WR). |
| `high_school` | character | High school the player attended, with its state in parentheses where the source lists it (e.g. 'Killian (FL)'). |
| `birth_city` | character | City of birth, where the source lists it. |
| `yahoo_id` | character | Yahoo player id, as a string. |
| `depth_chart_order` | character |  |
| `birth_date` | character | Player's date of birth (YYYY-MM-DD), where the source lists it. |
| `kalshi_id` | character |  |
| `opta_id` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_players-example}

```python
sleeper_players()
```

_Last validated n/a._

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
| `co_owners` | character |  |
| `keepers` | character |  |
| `league_id` | character | Sleeper league id the roster belongs to, as a string. |
| `metadata` | character |  |
| `owner_id` | character |  |
| `player_map` | character |  |
| `players` | character | Stringified list of Sleeper player ids on the roster (starters, bench and reserve). |
| `reserve` | character |  |
| `roster_id` | character |  |
| `starters` | character |  |
| `taxi` | character |  |
| `settings_fpts` | integer |  |
| `settings_fpts_against` | integer |  |
| `settings_fpts_against_decimal` | integer |  |
| `settings_fpts_decimal` | integer |  |
| `settings_losses` | integer |  |
| `settings_ppts` | integer |  |
| `settings_ppts_decimal` | integer |  |
| `settings_ties` | integer |  |
| `settings_total_moves` | integer |  |
| `settings_waiver_budget_used` | integer |  |
| `settings_waiver_position` | integer |  |
| `settings_wins` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_rosters-example}

```python
sleeper_rosters(league_id='289646328504385536')
```

_Last validated n/a._

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
| `week` | integer | Season week. |
| `leg` | integer |  |
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `league_season` | character | Season the Sleeper league year currently points at, as a string (e.g. '2026'). |
| `previous_season` | character |  |
| `season_start_date` | character | First day of the current Sleeper season (YYYY-MM-DD). |
| `display_week` | integer |  |
| `league_create_season` | character |  |
| `season_has_scores` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_state-example}

```python
sleeper_state()
```

_Last validated n/a._

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
| `round` | integer | Draft round |
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `roster_id` | character |  |
| `owner_id` | character |  |
| `previous_owner_id` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_traded_picks-example}

```python
sleeper_traded_picks(league_id='289646328504385536')
```

_Last validated n/a._

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
| `status` | character | Transaction status (e.g. 'complete', 'failed'). |
| `type` | character | Transaction type (e.g. 'waiver', 'free_agent', 'trade'). |
| `created` | integer |  |
| `leg` | integer |  |
| `draft_picks` | character |  |
| `creator` | character |  |
| `transaction_id` | character | Sleeper transaction id, as a string. |
| `adds` | character |  |
| `consenter_ids` | character |  |
| `drops` | character | Throws dropped |
| `roster_ids` | character |  |
| `status_updated` | integer |  |
| `waiver_budget` | character |  |
| `metadata_notes` | character |  |
| `settings_waiver_bid` | integer |  |
| `settings_priority` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_transactions-example}

```python
sleeper_transactions(league_id='289646328504385536', week='1')
```

_Last validated n/a._

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
| `count` | integer | Number of Sleeper leagues that added the player over the lookback window. |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_trending_adds-example}

```python
sleeper_trending_adds()
```

_Last validated n/a._

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
| `avatar` | character |  |
| `cookies` | character |  |
| `created` | character |  |
| `currencies` | character |  |
| `data_updated` | character |  |
| `deleted` | character |  |
| `display_name` | character | Full name of player |
| `email` | character |  |
| `is_bot` | character |  |
| `metadata` | character |  |
| `notifications` | character |  |
| `pending` | character |  |
| `phone` | character |  |
| `real_name` | character |  |
| `solicitable` | character |  |
| `summoner_name` | character |  |
| `summoner_region` | character |  |
| `token` | character |  |
| `user_id` | character |  |
| `username` | character |  |
| `verification` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_user-example}

```python
sleeper_user(username='457511950237696')
```

_Last validated n/a._

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
| `last_pinned_message_id` | character |  |
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `group_id` | character | Sleeper league-group id when the league belongs to a group; null otherwise. |
| `loser_bracket_id` | character |  |
| `last_author_display_name` | character |  |
| `total_rosters` | integer |  |
| `last_message_id` | character |  |
| `draft_id` | character | Sleeper draft id of the league's draft, as a string. |
| `previous_league_id` | character |  |
| `sport` | character |  |
| `bracket_id` | character |  |
| `bracket_overrides_id` | character |  |
| `last_message_text_map` | character |  |
| `status` | character | League status (e.g. 'pre_draft', 'drafting', 'in_season', 'complete'). |
| `last_message_attachment` | character |  |
| `display_order` | integer | Position of the bracket slot within its round, controlling top-to-bottom rendering. |
| `league_id` | character | Sleeper league id, as a string. |
| `last_author_is_bot` | character |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `company_id` | character |  |
| `last_author_id` | character |  |
| `roster_positions` | character |  |
| `last_message_time` | integer |  |
| `last_read_id` | character |  |
| `loser_bracket_overrides_id` | character |  |
| `last_author_avatar` | character |  |
| `last_transaction_id` | character |  |
| `shard` | integer |  |
| `avatar` | character |  |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `metadata_trophy_winner` | character |  |
| `metadata_trophy_winner_background` | character |  |
| `metadata_trophy_winner_banner_text` | character |  |
| `settings_bench_lock` | integer |  |
| `settings_daily_waivers` | integer |  |
| `settings_daily_waivers_hour` | integer |  |
| `settings_draft_rounds` | integer |  |
| `settings_last_report` | integer |  |
| `settings_last_scored_leg` | integer |  |
| `settings_league_average_match` | integer |  |
| `settings_leg` | integer |  |
| `settings_max_keepers` | integer |  |
| `settings_num_teams` | integer |  |
| `settings_offseason_adds` | integer |  |
| `settings_pick_trading` | integer |  |
| `settings_playoff_teams` | integer |  |
| `settings_playoff_week_start` | integer |  |
| `settings_reserve_allow_doubtful` | integer |  |
| `settings_reserve_allow_out` | integer |  |
| `settings_reserve_allow_sus` | integer |  |
| `settings_reserve_slots` | integer |  |
| `settings_start_week` | integer |  |
| `settings_taxi_allow_vets` | integer |  |
| `settings_taxi_deadline` | integer |  |
| `settings_taxi_slots` | integer |  |
| `settings_taxi_years` | integer |  |
| `settings_trade_deadline` | integer |  |
| `settings_trade_review_days` | integer |  |
| `settings_type` | integer |  |
| `settings_waiver_budget` | integer |  |
| `settings_waiver_clear_days` | integer |  |
| `settings_waiver_day_of_week` | integer |  |
| `settings_waiver_type` | integer |  |
| `settings_was_auto_archived` | integer |  |
| `scoring_settings_sack` | numeric |  |
| `scoring_settings_fgm_40_49` | numeric |  |
| `scoring_settings_bonus_rec_te` | numeric |  |
| `scoring_settings_fgm_yds` | numeric |  |
| `scoring_settings_pass_int` | numeric |  |
| `scoring_settings_pts_allow_0` | numeric |  |
| `scoring_settings_pass_2pt` | numeric |  |
| `scoring_settings_st_td` | numeric |  |
| `scoring_settings_fgm_yds_over_30` | numeric |  |
| `scoring_settings_rec_td` | numeric |  |
| `scoring_settings_fgm_30_39` | numeric |  |
| `scoring_settings_xpmiss` | numeric |  |
| `scoring_settings_rush_td` | numeric |  |
| `scoring_settings_fgm` | numeric |  |
| `scoring_settings_rec_2pt` | numeric |  |
| `scoring_settings_rush_fd` | numeric |  |
| `scoring_settings_st_fum_rec` | numeric |  |
| `scoring_settings_fgmiss` | numeric |  |
| `scoring_settings_ff` | numeric |  |
| `scoring_settings_fgmiss_30_39` | numeric |  |
| `scoring_settings_rec` | numeric |  |
| `scoring_settings_pts_allow_14_20` | numeric |  |
| `scoring_settings_fgm_0_19` | numeric |  |
| `scoring_settings_int` | numeric |  |
| `scoring_settings_def_st_fum_rec` | numeric |  |
| `scoring_settings_fum_lost` | numeric |  |
| `scoring_settings_pts_allow_1_6` | numeric |  |
| `scoring_settings_rec_fd` | numeric |  |
| `scoring_settings_fgmiss_20_29` | numeric |  |
| `scoring_settings_fgm_20_29` | numeric |  |
| `scoring_settings_pts_allow_21_27` | numeric |  |
| `scoring_settings_bonus_rec_wr` | numeric |  |
| `scoring_settings_xpm` | numeric |  |
| `scoring_settings_fgmiss_0_19` | numeric |  |
| `scoring_settings_rush_2pt` | numeric |  |
| `scoring_settings_fum_rec` | numeric |  |
| `scoring_settings_def_st_td` | numeric |  |
| `scoring_settings_fgm_50p` | numeric |  |
| `scoring_settings_def_td` | numeric |  |
| `scoring_settings_safe` | numeric |  |
| `scoring_settings_pass_yd` | numeric |  |
| `scoring_settings_fgmiss_40_49` | numeric |  |
| `scoring_settings_blk_kick` | numeric |  |
| `scoring_settings_pass_td` | numeric |  |
| `scoring_settings_rush_yd` | numeric |  |
| `scoring_settings_fum` | numeric |  |
| `scoring_settings_pts_allow_28_34` | numeric |  |
| `scoring_settings_pts_allow_35p` | numeric |  |
| `scoring_settings_rec_yd` | numeric |  |
| `scoring_settings_def_st_ff` | numeric |  |
| `scoring_settings_pts_allow_7_13` | numeric |  |
| `scoring_settings_st_ff` | numeric |  |
| `metadata_trophy_loser` | character |  |
| `metadata_trophy_loser_background` | character |  |
| `metadata_trophy_loser_banner_text` | character |  |
| `settings_playoff_type` | numeric |  |
| `scoring_settings_qb_hit` | numeric |  |
| `scoring_settings_bonus_rec_yd_100` | numeric |  |
| `scoring_settings_bonus_rush_yd_100` | numeric |  |
| `scoring_settings_bonus_pass_yd_400` | numeric |  |
| `scoring_settings_blk_kick_ret_yd` | numeric |  |
| `scoring_settings_sack_yd` | numeric |  |
| `scoring_settings_pr_td` | numeric |  |
| `scoring_settings_tkl_ast` | numeric |  |
| `scoring_settings_kr_td` | numeric |  |
| `scoring_settings_fg_ret_yd` | numeric |  |
| `scoring_settings_idp_tkl` | numeric |  |
| `scoring_settings_idp_blk` | numeric |  |
| `scoring_settings_int_ret_yd` | numeric |  |
| `scoring_settings_idp_tkl_solo` | numeric |  |
| `scoring_settings_pass_att` | numeric |  |
| `scoring_settings_idp_int` | numeric |  |
| `scoring_settings_idp_safe` | numeric |  |
| `scoring_settings_def_2pt` | numeric |  |
| `scoring_settings_kr_yd` | numeric |  |
| `scoring_settings_rush_att` | numeric |  |
| `scoring_settings_st_tkl_solo` | numeric |  |
| `scoring_settings_idp_sack` | numeric |  |
| `scoring_settings_bonus_pass_yd_300` | numeric |  |
| `scoring_settings_pass_sack` | numeric |  |
| `scoring_settings_pass_cmp` | numeric |  |
| `scoring_settings_tkl_loss` | numeric |  |
| `scoring_settings_def_pass_def` | numeric |  |
| `scoring_settings_idp_pass_def` | numeric |  |
| `scoring_settings_bonus_rec_yd_200` | numeric |  |
| `scoring_settings_tkl` | numeric |  |
| `scoring_settings_idp_fum_rec` | numeric |  |
| `scoring_settings_bonus_rush_yd_200` | numeric |  |
| `scoring_settings_tkl_solo` | numeric |  |
| `scoring_settings_pr_yd` | numeric |  |
| `scoring_settings_fum_ret_yd` | numeric |  |
| `scoring_settings_idp_ff` | numeric |  |
| `scoring_settings_idp_tkl_ast` | numeric |  |
| `metadata` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_user_leagues-example}

```python
sleeper_user_leagues(season='2018', user_id='457511950237696')
```

_Last validated n/a._

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
| `avatar` | character |  |
| `display_name` | character | Full name of player |
| `is_bot` | character |  |
| `is_owner` | logical |  |
| `league_id` | character | Sleeper league id the user belongs to, as a string. |
| `settings` | character |  |
| `user_id` | character |  |
| `metadata_allow_pn` | character |  |
| `metadata_mascot_item_type_id_leg_16` | character |  |
| `metadata_mascot_item_type_id_leg_17` | character |  |
| `metadata_mention_pn` | character |  |
| `metadata_player_nickname_update` | character |  |
| `metadata_team_name` | character |  |
| `metadata_team_name_update` | character |  |
| `metadata_transaction_commissioner` | character |  |
| `metadata_transaction_free_agent` | character |  |
| `metadata_transaction_trade` | character |  |
| `metadata_transaction_waiver` | character |  |
| `metadata_user_message_pn` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_users-example}

```python
sleeper_users(league_id='289646328504385536')
```

_Last validated n/a._

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
| `m` | integer | Matchup id within the bracket round. |
| `r` | integer | Playoff round number of the bracket matchup (1 = first round). |
| `l` | integer | Roster id of the matchup loser; null until played. |
| `w` | integer | Roster id of the matchup winner; null until played. |
| `t1` | integer |  |
| `t2` | integer |  |
| `t2_from_w` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sleeper_winners_bracket-example}

```python
sleeper_winners_bracket(league_id='289646328504385536')
```

_Last validated n/a._
