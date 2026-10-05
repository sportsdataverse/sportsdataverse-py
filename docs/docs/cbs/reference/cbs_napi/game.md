---
title: "CBS — CBS Sports NAPI (api.cbssports.com/napi) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "CBS — CBS Sports NAPI (api.cbssports.com/napi) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CBS — CBS Sports NAPI (api.cbssports.com/napi) — Game

## cbs_game_betting_splits

Get a BettingSplits resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/bettingSplits/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_betting_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_betting_splits-example}

```python
cbs_game_betting_splits()
```

_Last validated n/a._

## cbs_game_boxscore

Get boxscore resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/boxscore/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_boxscore-example}

```python
cbs_game_boxscore()
```

_Last validated n/a._

## cbs_game_content_preview

Get content for game preview

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/content/preview/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_content_preview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_content_preview-example}

```python
cbs_game_content_preview()
```

_Last validated n/a._

## cbs_game_content_recap

Get content for game recap

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/content/recap/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_content_recap-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_content_recap-example}

```python
cbs_game_content_recap()
```

_Last validated n/a._

## cbs_game_content_story

Get content for game story

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/content/story/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `gameIdsStoryTags` | `game_ids_story_tags` |  |  | `Y` | The tags used to retrieve stories |

### Returns {#cbs_game_content_story-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_content_story-example}

```python
cbs_game_content_story()
```

_Last validated n/a._

## cbs_game_featured

Get a FeaturedGame resource.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/featured/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_featured-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_featured-example}

```python
cbs_game_featured()
```

_Last validated n/a._

## cbs_game_lineup

Get a lineup resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/lineup/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: playerTeamAssociations, injuries, metaData, playerStats. |

### Returns {#cbs_game_lineup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_lineup-example}

```python
cbs_game_lineup()
```

_Last validated n/a._

## cbs_game_odds

Get an odds resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/odds/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `marketIds` | `market_ids` |  |  | `Y` | This value can be used to specify markets, using the marketIds. |
| `bookIds` | `book_ids` |  |  | `Y` | This value can be used to specify books, using the bookIds. |
| `state` | `state` |  |  | `Y` | This value can be used to specify a state. |
| `model` | `model` |  |  | `Y` | This value can be used set the model to be used |
| `showHiddenOdds` | `show_hidden_odds` |  |  | `Y` | If set to 1, show the odds that has been hidden within the market and/or consensus nodes |

### Returns {#cbs_game_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_odds-example}

```python
cbs_game_odds()
```

_Last validated n/a._

## cbs_game_odds_hq

Get an HQ odds resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/odds/hq/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_odds_hq-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_odds_hq-example}

```python
cbs_game_odds_hq()
```

_Last validated n/a._

## cbs_game_outcomes

Get an odds outcome for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/outcomes/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_outcomes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_outcomes-example}

```python
cbs_game_outcomes()
```

_Last validated n/a._

## cbs_game_probable_players

Get a list of players who are probably playing in a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/probablePlayers/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: player, playerTeamAssociations, injuries, transactions, depthCharts, metaData. |

### Returns {#cbs_game_probable_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_probable_players-example}

```python
cbs_game_probable_players()
```

_Last validated n/a._

## cbs_game_props

Get game props for a game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/props/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `marketIds` | `market_ids` |  |  | `Y` | This value can be used to specify markets, using the marketIds. |
| `bookIds` | `book_ids` |  |  | `Y` | This value can be used to specify books, using the bookIds. |
| `propBetTypes` | `prop_bet_types` |  |  | `Y` | This value can be used to specify prop bet types, using the propBetTypes. Allowed: player, game, team. |
| `state` | `state` |  |  | `Y` | This value can be used to specify a state. |
| `includeInactiveMarkets` | `include_inactive_markets` |  |  | `Y` | This value can be used to filter out inactive markets. |

### Returns {#cbs_game_props-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_props-example}

```python
cbs_game_props()
```

_Last validated n/a._

## cbs_game_rtwp

Get a rtwp resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/rtwp/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_rtwp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_rtwp-example}

```python
cbs_game_rtwp()
```

_Last validated n/a._

## cbs_game_ruwt_highlights

Get the RUWT highlights resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/ruwtHighlights/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_ruwt_highlights-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_ruwt_highlights-example}

```python
cbs_game_ruwt_highlights()
```

_Last validated n/a._

## cbs_game_scoring_boxscores

Get an scoring box scores resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/boxscores/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_boxscores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_boxscores-example}

```python
cbs_game_scoring_boxscores()
```

_Last validated n/a._

## cbs_game_scoring_drives

Get a drives resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/drives/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_drives-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | CBS drive number within the game; joins drive_id of the scoring-plays frame. |
| `team_id` | integer | CBS team id of the offense on the drive. |
| `quarter` | integer | Period in which the drive started. |
| `starting_time` | character | Game clock (m:ss) at the drive's first snap. |
| `ending_time` | character | Game clock (m:ss) at the drive's last play. |
| `time_of_possession` | character | Drive duration as m:ss. |
| `starting_yardline` | character | Field position where the drive started, team abbreviation plus yard line (e.g. TEXAS 22). |
| `ending_yardline` | character | Field position where the drive ended, team abbreviation plus yard line (e.g. TEXAS 18). |
| `starting_play_id` | integer | CBS play id of the drive's first play; joins id of the scoring-plays frame. |
| `ending_play_id` | integer | CBS play id of the drive's last play; joins id of the scoring-plays frame. |
| `drive_plays` | integer | Number of plays CBS counts in the drive. |
| `yards_on_drive` | integer | Net yards gained on the drive. |
| `drive_yards_total` | integer | Total drive yardage as CBS reports it, penalties included. |
| `penalty_yards` | integer | Penalty yards assessed on the drive. |
| `first_downs_on_drive` | integer | First downs gained on the drive. |
| `inside_the_20` | logical | True when the drive reached the opponent's 20-yard line (CBS "Yes"/"No" flag). |
| `score_on_drive` | logical | True when the drive produced a score (CBS "Yes"/"No" flag). |
| `result` | character | Drive outcome label from CBS (Punt, Touchdown, Field Goal, Downs, Fumble, End of Half, ...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_drives-example}

```python
cbs_game_scoring_drives()
```

_Last validated n/a._

## cbs_game_scoring_leaders

Get an scoring leaders resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/leaders/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_leaders-example}

```python
cbs_game_scoring_leaders()
```

_Last validated n/a._

## cbs_game_scoring_player_stats

Get an scoring player stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/playerStats/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_player_stats-example}

```python
cbs_game_scoring_player_stats()
```

_Last validated n/a._

## cbs_game_scoring_plays

Get an scoring plays resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/plays/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_plays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | CBS play id; the GSIS play id for NFL games, an epoch-style stamp for NCAAF games. |
| `game_id` | integer | CBS game id (null for older games, which lack it). |
| `drive_id` | integer | CBS drive number within the game; joins id of the scoring-drives frame. |
| `quarter` | integer | Period of the snap. |
| `time_remaining` | character | Game clock (m:ss) at the snap. |
| `down` | integer | Down at the snap; 0 on kickoffs and tries. |
| `distance` | character | Yards to go for a first down, or the literal "Goal" on goal-to-go downs. |
| `side` | character | Team abbreviation naming the half of the field that yardline refers to. |
| `yardline` | integer | Yard line of the ball on the side team's half of the field. |
| `team_in_possession` | integer | CBS team id with the ball at the snap. |
| `description` | character | Full CBS play text, including tacklers and spots. |
| `medium` | character | Medium-length play summary (e.g. "J. Sayin pass to M. Williams for 6 yds"). |
| `short` | character | Short play summary (e.g. "6 yd pass"). |
| `score_on_play` | logical | True when the play scored (CBS "Yes"/"No" flag). |
| `score_type` | character | Scoring type for a scoring play (Touchdown, FieldGoal, ...); null otherwise. |
| `short_score` | character | Short scoring summary; empty string when the play did not score. |
| `under_review` | logical | True when the play was under replay review (CBS "Yes"/"No" flag). |
| `home_timeouts_remaining` | integer | Home team's timeouts left after the play (null for older games, which lack it). |
| `away_timeouts_remaining` | integer | Away team's timeouts left after the play (null for older games, which lack it). |
| `real_clock` | character | UTC wall-clock timestamp of the play in ISO 8601 (null for older games, which lack it). |
| `subplays` | character | Sub-events of the play as a list of structs: type, order, and the event fields (player ids and names, yards_on_play, yards_to_endzone, team_in_possession). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_plays-example}

```python
cbs_game_scoring_plays()
```

_Last validated n/a._

## cbs_game_scoring_rosters

Get an scoring rosters resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/rosters/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_rosters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_rosters-example}

```python
cbs_game_scoring_rosters()
```

_Last validated n/a._

## cbs_game_scoring_scoreboard

Get an scoring scoreboard resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/scoreboard/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_scoreboard-example}

```python
cbs_game_scoring_scoreboard()
```

_Last validated n/a._

## cbs_game_scoring_scores

Get an scoring scores resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/scores/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_scores-example}

```python
cbs_game_scoring_scores()
```

_Last validated n/a._

## cbs_game_scoring_team_stats

Get an scoring team stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/teamStats/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_team_stats-example}

```python
cbs_game_scoring_team_stats()
```

_Last validated n/a._

## cbs_game_scoring_winprob

Get an scoring winprob resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/winprob/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_winprob-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_winprob-example}

```python
cbs_game_scoring_winprob()
```

_Last validated n/a._

## cbs_game_scoring_ytd_player_stats

Get an scoring YTD player stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/ytdPlayerStats/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_ytd_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_ytd_player_stats-example}

```python
cbs_game_scoring_ytd_player_stats()
```

_Last validated n/a._

## cbs_game_scoring_ytd_team_stats

Get an scoring YTD team stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/ytdTeamStats/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_scoring_ytd_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_scoring_ytd_team_stats-example}

```python
cbs_game_scoring_ytd_team_stats()
```

_Last validated n/a._

## cbs_game_ticket

Get a ticket resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/ticket/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_ticket-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_ticket-example}

```python
cbs_game_ticket()
```

_Last validated n/a._

## cbs_game_weather

Get a Weather resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/weather/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_game_weather-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_game_weather-example}

```python
cbs_game_weather()
```

_Last validated n/a._
