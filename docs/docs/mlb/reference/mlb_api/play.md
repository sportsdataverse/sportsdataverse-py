---
title: "MLB — MLB Stats API — Play"
sidebar_label: "Play"
sidebar_position: 5
description: "MLB — MLB Stats API — Play — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Play

## mlb_play_by_play

GET /api/v1/game/{gamePk}/playByPlay — play-by-play with at-bat detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/playByPlay`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/playByPlay](https://statsapi.mlb.com/api/v1/game/716390/playByPlay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_play_by_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pitch_index` | character | A serialized list of indices identifying individual pitch events that occurred within this at-bat. |
| `action_index` | character | A serialized list of indices identifying action-type events (e.g., stolen bases, pickoffs) that occurred within the at-bat. |
| `runner_index` | character | A serialized list of indices identifying baserunner movement events that occurred during or after this play. |
| `runners` | character | A serialized representation of baserunner movement records for this play, including starting base, ending base, and relevant event types. |
| `play_events` | character | A serialized representation of the sequence of individual pitch and action events comprising this at-bat. |
| `play_end_time` | character | The ISO 8601 timestamp marking the conclusion of the entire play (as distinct from a single pitch event) within the game feed. |
| `at_bat_index` | integer | Zero-based index of the at-bat within the game. |
| `result_type` | character | The high-level category of the play result as classified by the MLB Stats API (e.g., 'atBat', 'action'). |
| `result_event` | character | The short categorical label for the play outcome as classified by the MLB Stats API (e.g., 'Strikeout', 'Home Run', 'Walk'). |
| `result_event_type` | character | The snake-cased type identifier for the play outcome used internally by the MLB Stats API (e.g., 'strikeout', 'home_run'). |
| `result_description` | character | A human-readable text description of the play result as reported by the MLB Stats API (e.g., 'Strikeout', 'Single to left field'). |
| `result_rbi` | integer | The number of runs batted in credited to the batter as a result of this play. |
| `result_away_score` | integer | The away team's cumulative run total at the conclusion of this play. |
| `result_home_score` | integer | The home team's cumulative run total at the conclusion of this play. |
| `result_is_out` | logical | Boolean flag indicating whether the play resulted in the batter being retired (i.e., an out was charged to the batter). |
| `about_at_bat_index` | integer | The sequential index of the at-bat within the game to which this play or pitch event belongs. |
| `about_half_inning` | character | Indicates whether the play occurred in the top or bottom half of the inning (e.g., 'top' or 'bottom'). |
| `about_is_top_inning` | logical | Boolean flag indicating whether this play occurred in the top half of the inning (true) or bottom half (false). |
| `about_inning` | integer | The inning number in which this play or pitch event occurred. |
| `about_start_time` | character | The ISO 8601 timestamp marking the start of the play event, used for temporal sequencing within the game feed. |
| `about_end_time` | character | The ISO 8601 timestamp marking the end of the play event, used for temporal sequencing within the game feed. |
| `about_is_complete` | logical | Boolean flag indicating whether the at-bat or play event has concluded (i.e., reached a terminal result). |
| `about_is_scoring_play` | logical | Boolean flag indicating whether this play resulted in one or more runs being scored. |
| `about_has_review` | logical | Boolean flag indicating whether this play was subject to a manager's challenge or umpire review. |
| `about_has_out` | logical | Boolean flag indicating whether this play resulted in at least one out being recorded. |
| `about_captivating_index` | integer | A numeric score assigned by the MLB Stats API reflecting how compelling or exciting a given play was, based on leverage and game context. |
| `count_balls` | integer | The ball count in the current at-bat at the time of this pitch or play event. |
| `count_strikes` | integer | The strike count in the current at-bat at the time of this pitch or play event. |
| `count_outs` | integer | The number of outs recorded in the current half-inning at the time of this pitch or play event. |
| `matchup_batter_id` | integer | The MLB Stats API (MLBAM) numeric identifier for the batter in this play's matchup. |
| `matchup_batter_full_name` | character | The full name of the batter involved in this plate appearance. |
| `matchup_batter_link` | character | The MLB Stats API relative URL linking to the batter's player resource for this matchup. |
| `matchup_bat_side_code` | character | A single-character code indicating the batter's handedness for this matchup (e.g., 'L' for left, 'R' for right, 'S' for switch). |
| `matchup_bat_side_description` | character | The human-readable description of the batter's hitting side for this matchup (e.g., 'Left', 'Right', 'Switch'). |
| `matchup_pitcher_id` | integer | The MLB Stats API (MLBAM) numeric identifier for the pitcher in this play's matchup. |
| `matchup_pitcher_full_name` | character | The full name of the pitcher involved in this plate appearance. |
| `matchup_pitcher_link` | character | The MLB Stats API relative URL linking to the pitcher's player resource for this matchup. |
| `matchup_pitch_hand_code` | character | A single-character code indicating the pitcher's throwing hand for this matchup (e.g., 'L' for left, 'R' for right). |
| `matchup_pitch_hand_description` | character | The human-readable description of the pitcher's throwing arm for this matchup (e.g., 'Left', 'Right'). |
| `matchup_post_on_first_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on first base after the play concluded. |
| `matchup_post_on_first_full_name` | character | The full name of the baserunner on first base after the play concluded, if applicable. |
| `matchup_post_on_first_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on first base after the play. |
| `matchup_batter_hot_cold_zones` | character | A serialized representation of the batter's hot and cold zone effectiveness data for this matchup context. |
| `matchup_pitcher_hot_cold_zones` | character | A serialized representation of the pitcher's hot and cold zone effectiveness data for this matchup context. |
| `matchup_splits_batter` | character | A string describing the batter's situational split relevant to this matchup (e.g., 'vs. Right' or 'vs. Left'). |
| `matchup_splits_pitcher` | character | A string describing the pitcher's situational split relevant to this matchup (e.g., 'vs. Left' or 'vs. Right'). |
| `matchup_splits_men_on_base` | character | A string describing the baserunner configuration applicable to the batter's situational split for this plate appearance. |
| `matchup_post_on_second_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on second base after the play concluded. |
| `matchup_post_on_second_full_name` | character | The full name of the baserunner on second base after the play concluded, if applicable. |
| `matchup_post_on_second_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on second base after the play. |
| `matchup_post_on_third_id` | double | The MLB Stats API (MLBAM) numeric identifier for the runner on third base after the play concluded. |
| `matchup_post_on_third_full_name` | character | The full name of the baserunner on third base after the play concluded, if applicable. |
| `matchup_post_on_third_link` | character | The MLB Stats API relative URL linking to the player resource of the runner on third base after the play. |
| `review_details_is_overturned` | logical | Boolean flag indicating whether the original on-field call was reversed as a result of the replay review. |
| `review_details_in_progress` | logical | Boolean flag indicating whether the umpire review of this play was still ongoing at the time of data capture. |
| `review_details_review_type` | character | The type of review mechanism applied to this play (e.g., 'managerChallenge', 'umpireReview'). |
| `review_details_challenge_team_id` | double | The MLB Stats API numeric identifier for the team that initiated the manager's challenge review on this play. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_play_by_play-example}

```python
mlb_play_by_play(game_pk=716390)
```

_Last validated n/a._

## mlb_play_analytics

View Statcast data for a specific play.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/{guid}/analytics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/analytics](https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/analytics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `guid` | `guid` |  | `Y` |  | guid path parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_play_analytics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_play_analytics-example}

```python
mlb_play_analytics(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

_Last validated n/a._

## mlb_play_context_metrics_averages

View Statcast contextMetrics data for a specific play.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/{guid}/contextMetricsAverages`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/contextMetricsAverages](https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/contextMetricsAverages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `guid` | `guid` |  | `Y` |  | guid path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_play_context_metrics_averages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_play_context_metrics_averages-example}

```python
mlb_play_context_metrics_averages(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

_Last validated n/a._
