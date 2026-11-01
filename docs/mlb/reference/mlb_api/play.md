# MLB — MLB Stats API — Play

> MLB — MLB Stats API — Play — function reference in sdv-py, the SportsDataverse Python package.

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
| `pitch_index` | String | A serialized list of indices identifying individual pitch events that occurred within this at-bat. |
| `action_index` | String | A serialized list of indices identifying action-type events (e.g., stolen bases, pickoffs) that occurred within the at-bat. |
| `runner_index` | String | A serialized list of indices identifying baserunner movement events that occurred during or after this play. |
| `runners` | String | A serialized representation of baserunner movement records for this play, including starting base, ending base, and relevant event types. |
| `play_events` | String | A serialized representation of the sequence of individual pitch and action events comprising this at-bat. |
| `play_end_time` | String | The ISO 8601 timestamp marking the conclusion of the entire play (as distinct from a single pitch event) within the game feed. |
| `at_bat_index` | Int64 | Zero-based index of the at-bat within the game. |
| `result_type` | String | The high-level category of the play result as classified by the MLB Stats API (e.g., 'atBat', 'action'). |
| `result_event` | String | The short categorical label for the play outcome as classified by the MLB Stats API (e.g., 'Strikeout', 'Home Run', 'Walk'). |
| `result_event_type` | String | The snake-cased type identifier for the play outcome used internally by the MLB Stats API (e.g., 'strikeout', 'home_run'). |
| `result_description` | String | A human-readable text description of the play result as reported by the MLB Stats API (e.g., 'Strikeout', 'Single to left field'). |
| `result_rbi` | Int64 | The number of runs batted in credited to the batter as a result of this play. |
| `result_away_score` | Int64 | The away team's cumulative run total at the conclusion of this play. |
| `result_home_score` | Int64 | The home team's cumulative run total at the conclusion of this play. |
| `result_is_out` | Boolean | Boolean flag indicating whether the play resulted in the batter being retired (i.e., an out was charged to the batter). |
| `about_at_bat_index` | Int64 | The sequential index of the at-bat within the game to which this play or pitch event belongs. |
| `about_half_inning` | String | Indicates whether the play occurred in the top or bottom half of the inning (e.g., 'top' or 'bottom'). |
| `about_is_top_inning` | Boolean | Boolean flag indicating whether this play occurred in the top half of the inning (true) or bottom half (false). |
| `about_inning` | Int64 | The inning number in which this play or pitch event occurred. |
| `about_start_time` | String | The ISO 8601 timestamp marking the start of the play event, used for temporal sequencing within the game feed. |
| `about_end_time` | String | The ISO 8601 timestamp marking the end of the play event, used for temporal sequencing within the game feed. |
| `about_is_complete` | Boolean | Boolean flag indicating whether the at-bat or play event has concluded (i.e., reached a terminal result). |
| `about_is_scoring_play` | Boolean | Boolean flag indicating whether this play resulted in one or more runs being scored. |
| `about_has_review` | Boolean | Boolean flag indicating whether this play was subject to a manager's challenge or umpire review. |
| `about_has_out` | Boolean | Boolean flag indicating whether this play resulted in at least one out being recorded. |
| `about_captivating_index` | Int64 | A numeric score assigned by the MLB Stats API reflecting how compelling or exciting a given play was, based on leverage and game context. |
| `count_balls` | Int64 | The ball count in the current at-bat at the time of this pitch or play event. |
| `count_strikes` | Int64 | The strike count in the current at-bat at the time of this pitch or play event. |
| `count_outs` | Int64 | The number of outs recorded in the current half-inning at the time of this pitch or play event. |
| `matchup_batter_id` | Int64 | The MLB Stats API (MLBAM) numeric identifier for the batter in this play's matchup. |
| `matchup_batter_full_name` | String | The full name of the batter involved in this plate appearance. |
| `matchup_batter_link` | String | The MLB Stats API relative URL linking to the batter's player resource for this matchup. |
| `matchup_bat_side_code` | String | A single-character code indicating the batter's handedness for this matchup (e.g., 'L' for left, 'R' for right, 'S' for switch). |
| `matchup_bat_side_description` | String | The human-readable description of the batter's hitting side for this matchup (e.g., 'Left', 'Right', 'Switch'). |
| `matchup_pitcher_id` | Int64 | The MLB Stats API (MLBAM) numeric identifier for the pitcher in this play's matchup. |
| `matchup_pitcher_full_name` | String | The full name of the pitcher involved in this plate appearance. |
| `matchup_pitcher_link` | String | The MLB Stats API relative URL linking to the pitcher's player resource for this matchup. |
| `matchup_pitch_hand_code` | String | A single-character code indicating the pitcher's throwing hand for this matchup (e.g., 'L' for left, 'R' for right). |
| `matchup_pitch_hand_description` | String | The human-readable description of the pitcher's throwing arm for this matchup (e.g., 'Left', 'Right'). |
| `matchup_post_on_first_id` | Float64 | The MLB Stats API (MLBAM) numeric identifier for the runner on first base after the play concluded. |
| `matchup_post_on_first_full_name` | String | The full name of the baserunner on first base after the play concluded, if applicable. |
| `matchup_post_on_first_link` | String | The MLB Stats API relative URL linking to the player resource of the runner on first base after the play. |
| `matchup_batter_hot_cold_zones` | String | A serialized representation of the batter's hot and cold zone effectiveness data for this matchup context. |
| `matchup_pitcher_hot_cold_zones` | String | A serialized representation of the pitcher's hot and cold zone effectiveness data for this matchup context. |
| `matchup_splits_batter` | String | A string describing the batter's situational split relevant to this matchup (e.g., 'vs. Right' or 'vs. Left'). |
| `matchup_splits_pitcher` | String | A string describing the pitcher's situational split relevant to this matchup (e.g., 'vs. Left' or 'vs. Right'). |
| `matchup_splits_men_on_base` | String | A string describing the baserunner configuration applicable to the batter's situational split for this plate appearance. |
| `matchup_post_on_second_id` | Float64 | The MLB Stats API (MLBAM) numeric identifier for the runner on second base after the play concluded. |
| `matchup_post_on_second_full_name` | String | The full name of the baserunner on second base after the play concluded, if applicable. |
| `matchup_post_on_second_link` | String | The MLB Stats API relative URL linking to the player resource of the runner on second base after the play. |
| `matchup_post_on_third_id` | Float64 | The MLB Stats API (MLBAM) numeric identifier for the runner on third base after the play concluded. |
| `matchup_post_on_third_full_name` | String | The full name of the baserunner on third base after the play concluded, if applicable. |
| `matchup_post_on_third_link` | String | The MLB Stats API relative URL linking to the player resource of the runner on third base after the play. |
| `review_details_is_overturned` | Boolean | Boolean flag indicating whether the original on-field call was reversed as a result of the replay review. |
| `review_details_in_progress` | Boolean | Boolean flag indicating whether the umpire review of this play was still ongoing at the time of data capture. |
| `review_details_review_type` | String | The type of review mechanism applied to this play (e.g., 'managerChallenge', 'umpireReview'). |
| `review_details_challenge_team_id` | Float64 | The MLB Stats API numeric identifier for the team that initiated the manager's challenge review on this play. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_play_by_play-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_play_by_play-example}

```python
mlb_play_by_play(game_pk=716390)
```

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

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 401 'Please Login' through the residential proxy; the route needs an MLB login session the package does not hold.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_play_analytics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_play_analytics-example}

```python
mlb_play_analytics(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

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

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 401 'Please Login' through the residential proxy; the route needs an MLB login session the package does not hold.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_play_context_metrics_averages-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_play_context_metrics_averages-example}

```python
mlb_play_context_metrics_averages(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```
