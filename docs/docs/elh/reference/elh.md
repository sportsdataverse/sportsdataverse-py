---
title: ELH — Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard)
sidebar_label: Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard)
description: "ELH — Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# ELH — Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard)

`sportsdataverse.hockey.elh` — 8 endpoints.

## elh_events

Cumulative shot attempts and shots on goal of one game, per team and game minute.

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/events/{match_id}.json`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/events/2928321.json](https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/events/2928321.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), e.g. 2928321. Not the onlajny game id. |

### Returns {#elh_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `start_date` | character | Scheduled start, local YYYY-MM-DD HH:MM. |
| `team_side` | character | The team's side: home or away. |
| `team_name` | character | Team full (official) name. |
| `team_score` | integer | The team's final goals. |
| `minute` | integer | Game minute (1-60), the end of which the totals run to. |
| `shots` | integer | Cumulative shot attempts up to the end of the minute. |
| `shots_on_goal` | integer | Cumulative shots on goal up to the end of the minute. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_events-example}

```python
elh_events(match_id=2928321)
```

_Last validated n/a._

## elh_list_objects

One page (up to 1,000 keys) of the hokej.cz S3 bucket listing; match keys carry the hokej.cz game id. The bucket is not Extraliga-only.

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/?prefix=match%2F2026%2Ffull%2F&max-keys=3](https://s3-eu-west-1.amazonaws.com/hokej.cz/?prefix=match%2F2026%2Ffull%2F&max-keys=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `prefix` | `prefix` |  |  | `Y` | Key prefix, e.g. `match/2026/full/` or `visualization/shots/`. |
| `delimiter` | `delimiter` |  |  | `Y` | `/` groups keys below the next slash into CommonPrefixes (folder view). |
| `marker` | `marker` |  |  | `Y` | ListObjects V1 paging: the last `Key` of the previous page; repeat until `IsTruncated` is false (V1 sends `NextMarker` only with a delimiter). |
| `max-keys` | `max_keys` |  |  | `Y` | Page size, at most 1000 (the default). |
| `list-type` | `list_type` |  |  | `Y` | `2` switches to ListObjectsV2 (`KeyCount`, `NextContinuationToken`); omitted = V1 as documented here. |
| `continuation-token` | `continuation_token` |  |  | `Y` | V2 only (`list-type=2`): `NextContinuationToken` of the previous page. |
| `start-after` | `start_after` |  |  | `Y` | V2 only (`list-type=2`): list keys after this key. |

### Returns {#elh_list_objects-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hokejcz_game_id` | integer | hokej.cz game id (the S3 file name; shots match.id, scoreboard hokejcz_id). Int64. |
| `key` | character | S3 object key, e.g. match/2026/full/2928255.json. |
| `last_modified` | character | Last write of the object, ISO-8601 UTC. |
| `size` | integer | Object size in bytes (a few hundred bytes = a file written ahead of the game). |

**`return_parsed=False`** — the raw ListObjects V1 XML page as text (`str`).

### Raises {#elh_list_objects-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#elh_list_objects-example}

```python
elh_list_objects(max_keys='3', prefix='match/2026/full/')
```

_Last validated n/a._

## elh_pbp

Play-by-play commentary of one game, every entry, oldest first: goals and penalties with team and players (onlajny ids).

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/match/{season}/full/{match_id}.json`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/full/2928321.json](https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/full/2928321.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026-27); the bucket files it under the START year (match/2026/). Play-by-play starts in 2014-15 (season 2015). |
| `match_id` | `match_id` |  | `Y` |  | hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), e.g. 2928321. Not the onlajny game id. |

### Returns {#elh_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comment_id` | integer | Onlajny commentary entry id (Int64); equals the mobile file's comment id. |
| `sort_order` | integer | Provider sort key (Int64); ascending = chronological, the order rows are returned in. |
| `event_type` | character | Entry kind: goal, penalty, time (period start/end), important, normal, expertComment. |
| `label2` | character | Secondary label; empty (null) in every sample. |
| `time_type` | character | For time entries: begin or end of a period; null otherwise. |
| `phase` | character | Phase of a clockless entry: 0 pre-game, 1INT / 2INT intermissions, 3PER third period, ERT after the game (meanings inferred from the entries, undocumented); null when the entry has a clock. |
| `clock` | character | Elapsed game clock mm:ss (00:01 = game start); null on a clockless entry. |
| `game_seconds` | integer | Seconds from the start of the game. |
| `period` | integer | Period number derived from the game clock (20-minute periods; 4 = overtime); null on a clockless entry. |
| `home_score` | integer | Home goals (after the entry on play-by-play rows; final on the scoreboard). |
| `away_score` | integer | Away goals (after the entry on play-by-play rows; final on the scoreboard). |
| `home_shootout_score` | integer | Home shootout goals after the entry (0 without a shootout). |
| `away_shootout_score` | integer | Away shootout goals after the entry (0 without a shootout). |
| `event_id` | integer | Onlajny event id of a goal or penalty (Int64); null on other entries. |
| `onlajny_team_id` | integer | Onlajny team id of the team credited with the goal or penalty (Sparta = 156); NOT the hokej.cz team id. The scoreboard maps it. |
| `onlajny_team_global_id` | integer | Onlajny global team id (equal to onlajny_team_id in every sample). |
| `team_code` | character | Team code of the credited team, e.g. SPA (same as the shots / scoreboard codes). |
| `onlajny_player1_id` | integer | Onlajny player id of slot 1 (goal scorer / penalised player); NOT the hokej.cz id of the shots; null for an unused slot. |
| `onlajny_player1_global_id` | integer | Onlajny GLOBAL player id of slot 1 (goal scorer / penalised player); can differ from onlajny_player1_id (Hradec 48287 / 76559). |
| `player1_name` | character | Display name of slot 1 (goal scorer / penalised player). |
| `player1_first_name` | character | Given name of slot 1 (goal scorer / penalised player); null in 2016-17. |
| `player1_last_name` | character | Family name of slot 1 (goal scorer / penalised player); null in 2016-17. |
| `onlajny_player2_id` | integer | Onlajny player id of slot 2 (first assist); NOT the hokej.cz id of the shots; null for an unused slot. |
| `onlajny_player2_global_id` | integer | Onlajny GLOBAL player id of slot 2 (first assist); can differ from onlajny_player2_id (Hradec 48287 / 76559). |
| `player2_name` | character | Display name of slot 2 (first assist). |
| `player2_first_name` | character | Given name of slot 2 (first assist); null in 2016-17. |
| `player2_last_name` | character | Family name of slot 2 (first assist); null in 2016-17. |
| `onlajny_player3_id` | integer | Onlajny player id of slot 3 (second assist); NOT the hokej.cz id of the shots; null for an unused slot. |
| `onlajny_player3_global_id` | integer | Onlajny GLOBAL player id of slot 3 (second assist); can differ from onlajny_player3_id (Hradec 48287 / 76559). |
| `player3_name` | character | Display name of slot 3 (second assist). |
| `player3_first_name` | character | Given name of slot 3 (second assist); null in 2016-17. |
| `player3_last_name` | character | Family name of slot 3 (second assist); null in 2016-17. |
| `no_assist` | integer | Unassisted flag on slot 1 (0 in every sample; 1 = unassisted is inferred, unobserved). Absent before 2026-27 samples. |
| `penalty_minutes` | integer | Penalty minutes of the penalised player (player1); two penalties are summed. Null in 2016-17. |
| `penalty_offense` | character | Czech offense code(s), e.g. VYSH (high sticking), PODR (tripping); comma-joined for two penalties. Null in 2016-17. |
| `strength` | character | Strength of a goal: EQ (read as even strength; inferred, the only value observed). Null where the on-ice block is absent (all of 2016-17). |
| `message` | character | Czech commentary text (may contain <br />). |
| `written` | character | When the entry was written, local time YYYY-MM-DD HH:MM:SS. |
| `last_change` | character | Last edit of the entry, local time YYYY-MM-DD HH:MM:SS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_pbp-example}

```python
elh_pbp(match_id=2928321, season=2027)
```

_Last validated n/a._

## elh_pbp_goals

Players on the ice at each goal of one game (onlajny ids), one row per player, with the strength.

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/match/{season}/full/{match_id}.json`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/full/2928321.json](https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/full/2928321.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026-27); the bucket files it under the START year (match/2026/). Play-by-play starts in 2014-15 (season 2015). |
| `match_id` | `match_id` |  | `Y` |  | hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), e.g. 2928321. Not the onlajny game id. |

### Returns {#elh_pbp_goals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comment_id` | integer | Onlajny commentary entry id (Int64); equals the mobile file's comment id. |
| `event_id` | integer | Onlajny event id of a goal or penalty (Int64); null on other entries. |
| `clock` | character | Elapsed game clock mm:ss (00:01 = game start); null on a clockless entry. |
| `game_seconds` | integer | Seconds from the start of the game. |
| `period` | integer | Period number derived from the game clock (20-minute periods; 4 = overtime); null on a clockless entry. |
| `strength` | character | Strength of a goal: EQ (read as even strength; inferred, the only value observed). Null where the on-ice block is absent (all of 2016-17). |
| `home_score` | integer | Home goals (after the entry on play-by-play rows; final on the scoreboard). |
| `away_score` | integer | Away goals (after the entry on play-by-play rows; final on the scoreboard). |
| `scoring_onlajny_team_id` | integer | Onlajny team id of the scoring team. |
| `scoring_team_code` | character | Team code of the scoring team, e.g. SPA. |
| `on_ice_onlajny_team_id` | integer | Onlajny team id of the on-ice player's team; null for a goal without the on-ice block (2016-17). |
| `on_ice_team_code` | character | Team code of the on-ice player's team. |
| `is_scoring_team` | logical | True when the on-ice player is on the scoring team. |
| `slot` | integer | Player slot within the team's on-ice block (1-6). |
| `onlajny_player_id` | integer | Onlajny player id of the on-ice player (per competition, inferred); NOT the hokej.cz player id of the shots. |
| `onlajny_player_global_id` | integer | Onlajny GLOBAL player id of the on-ice player; can differ from onlajny_player_id for the same player. |
| `player_name` | character | On-ice player's display name (surname). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_pbp_goals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_pbp_goals-example}

```python
elh_pbp_goals(match_id=2928321, season=2027)
```

_Last validated n/a._

## elh_pbp_latest

The newest 20 play-by-play entries of one game (same columns as elh_pbp).

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/match/{season}/short/{match_id}.json`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/short/2928321.json](https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/short/2928321.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026-27); the bucket files it under the START year (match/2026/). Play-by-play starts in 2014-15 (season 2015). |
| `match_id` | `match_id` |  | `Y` |  | hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), e.g. 2928321. Not the onlajny game id. |

### Returns {#elh_pbp_latest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comment_id` | integer | Onlajny commentary entry id (Int64); equals the mobile file's comment id. |
| `sort_order` | integer | Provider sort key (Int64); ascending = chronological, the order rows are returned in. |
| `event_type` | character | Entry kind: goal, penalty, time (period start/end), important, normal, expertComment. |
| `label2` | character | Secondary label; empty (null) in every sample. |
| `time_type` | character | For time entries: begin or end of a period; null otherwise. |
| `phase` | character | Phase of a clockless entry: 0 pre-game, 1INT / 2INT intermissions, 3PER third period, ERT after the game (meanings inferred from the entries, undocumented); null when the entry has a clock. |
| `clock` | character | Elapsed game clock mm:ss (00:01 = game start); null on a clockless entry. |
| `game_seconds` | integer | Seconds from the start of the game. |
| `period` | integer | Period number derived from the game clock (20-minute periods; 4 = overtime); null on a clockless entry. |
| `home_score` | integer | Home goals (after the entry on play-by-play rows; final on the scoreboard). |
| `away_score` | integer | Away goals (after the entry on play-by-play rows; final on the scoreboard). |
| `home_shootout_score` | integer | Home shootout goals after the entry (0 without a shootout). |
| `away_shootout_score` | integer | Away shootout goals after the entry (0 without a shootout). |
| `event_id` | integer | Onlajny event id of a goal or penalty (Int64); null on other entries. |
| `onlajny_team_id` | integer | Onlajny team id of the team credited with the goal or penalty (Sparta = 156); NOT the hokej.cz team id. The scoreboard maps it. |
| `onlajny_team_global_id` | integer | Onlajny global team id (equal to onlajny_team_id in every sample). |
| `team_code` | character | Team code of the credited team, e.g. SPA (same as the shots / scoreboard codes). |
| `onlajny_player1_id` | integer | Onlajny player id of slot 1 (goal scorer / penalised player); NOT the hokej.cz id of the shots; null for an unused slot. |
| `onlajny_player1_global_id` | integer | Onlajny GLOBAL player id of slot 1 (goal scorer / penalised player); can differ from onlajny_player1_id (Hradec 48287 / 76559). |
| `player1_name` | character | Display name of slot 1 (goal scorer / penalised player). |
| `player1_first_name` | character | Given name of slot 1 (goal scorer / penalised player); null in 2016-17. |
| `player1_last_name` | character | Family name of slot 1 (goal scorer / penalised player); null in 2016-17. |
| `onlajny_player2_id` | integer | Onlajny player id of slot 2 (first assist); NOT the hokej.cz id of the shots; null for an unused slot. |
| `onlajny_player2_global_id` | integer | Onlajny GLOBAL player id of slot 2 (first assist); can differ from onlajny_player2_id (Hradec 48287 / 76559). |
| `player2_name` | character | Display name of slot 2 (first assist). |
| `player2_first_name` | character | Given name of slot 2 (first assist); null in 2016-17. |
| `player2_last_name` | character | Family name of slot 2 (first assist); null in 2016-17. |
| `onlajny_player3_id` | integer | Onlajny player id of slot 3 (second assist); NOT the hokej.cz id of the shots; null for an unused slot. |
| `onlajny_player3_global_id` | integer | Onlajny GLOBAL player id of slot 3 (second assist); can differ from onlajny_player3_id (Hradec 48287 / 76559). |
| `player3_name` | character | Display name of slot 3 (second assist). |
| `player3_first_name` | character | Given name of slot 3 (second assist); null in 2016-17. |
| `player3_last_name` | character | Family name of slot 3 (second assist); null in 2016-17. |
| `no_assist` | integer | Unassisted flag on slot 1 (0 in every sample; 1 = unassisted is inferred, unobserved). Absent before 2026-27 samples. |
| `penalty_minutes` | integer | Penalty minutes of the penalised player (player1); two penalties are summed. Null in 2016-17. |
| `penalty_offense` | character | Czech offense code(s), e.g. VYSH (high sticking), PODR (tripping); comma-joined for two penalties. Null in 2016-17. |
| `strength` | character | Strength of a goal: EQ (read as even strength; inferred, the only value observed). Null where the on-ice block is absent (all of 2016-17). |
| `message` | character | Czech commentary text (may contain <br />). |
| `written` | character | When the entry was written, local time YYYY-MM-DD HH:MM:SS. |
| `last_change` | character | Last edit of the entry, local time YYYY-MM-DD HH:MM:SS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_pbp_latest-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_pbp_latest-example}

```python
elh_pbp_latest(match_id=2928321, season=2027)
```

_Last validated n/a._

## elh_pbp_mobile

Play-by-play of one game as plain text (no team or player ids), oldest first.

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/match/{season}/mobile/{match_id}.json`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/mobile/2928321.json](https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/mobile/2928321.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026-27); the bucket files it under the START year (match/2026/). Play-by-play starts in 2014-15 (season 2015). |
| `match_id` | `match_id` |  | `Y` |  | hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), e.g. 2928321. Not the onlajny game id. |

### Returns {#elh_pbp_mobile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comment_id` | integer | Onlajny commentary entry id (Int64); equals the mobile file's comment id. |
| `event_type` | character | Entry kind: goal, penalty, time (period start/end), important, normal, expertComment. |
| `game_seconds` | integer | Seconds from the start of the game. |
| `content` | character | Czech commentary text (plain, newline line breaks). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_pbp_mobile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_pbp_mobile-example}

```python
elh_pbp_mobile(match_id=2928321, season=2027)
```

_Last validated n/a._

## elh_scoreboard

Extraliga games of one recent day with both id spaces (hokej.cz and onlajny) for games and teams.

**Endpoint URL:** `GET https://json.esports.cz/hokejcz/scoreboard/{date}.json`

**Valid URL:** [https://json.esports.cz/hokejcz/scoreboard/2026-10-11.json](https://json.esports.cz/hokejcz/scoreboard/2026-10-11.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  | `Y` |  | Day, YYYY-MM-DD. The scoreboard is a ROLLING window of recent days: an older day (and today) answers 404, raised as NoDataError; a day without an Extraliga game raises NoDataError too. |

### Returns {#elh_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | integer | Scoreboard league key: 101 Tipsport extraliga (ELH), 82 Maxa liga, 130 para hockey. |
| `league_name` | character | League name, e.g. Tipsport extraliga. |
| `hokejcz_game_id` | integer | hokej.cz game id (the S3 file name; shots match.id, scoreboard hokejcz_id). Int64. |
| `onlajny_game_id` | integer | Onlajny game id; the scoreboard is the only place it meets the hokej.cz game id. |
| `game_date` | character | Game date (shots: YYYY-MM-DD string; scoreboard: Date). |
| `start_time` | character | Start time, local HH:MM. |
| `status_code` | character | Game state code: K = finished (others unobserved). |
| `status_name` | character | Game state in Czech, e.g. konec zapasu (end of game). |
| `match_status` | character | Status in Czech, e.g. po zapase (after the game). |
| `home_score` | integer | Home goals (after the entry on play-by-play rows; final on the scoreboard). |
| `away_score` | integer | Away goals (after the entry on play-by-play rows; final on the scoreboard). |
| `score_periods` | character | Per-period scores home:away, comma-joined, e.g. 2:0,0:0,1:0. |
| `hokejcz_home_team_id` | integer | hokej.cz team id of the home team (Sparta = 8); NOT the onlajny id of the play-by-play. |
| `onlajny_home_team_id` | integer | Onlajny team id of the home team (Sparta = 156); joins the play-by-play onlajny_team_id. |
| `home_team_name` | character | Home team full name. |
| `home_team_short_name` | character | Home team short name, e.g. Sparta. |
| `home_team_code` | character | Home team code, e.g. SPA. |
| `hokejcz_away_team_id` | integer | hokej.cz team id of the away team (Vitkovice = 7); NOT the onlajny id of the play-by-play. |
| `onlajny_away_team_id` | integer | Onlajny team id of the away team (Vitkovice = 168); joins the play-by-play onlajny_team_id. |
| `away_team_name` | character | Away team full name. |
| `away_team_short_name` | character | Away team short name. |
| `away_team_code` | character | Away team code, e.g. VIT. |
| `stream_url` | character | Stream code; md in every sample. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_scoreboard-example}

```python
elh_scoreboard(date='2026-10-11')
```

_Last validated n/a._

## elh_shots

Shot attempts of one game with rink coordinates and result code (hokej.cz ids).

**Endpoint URL:** `GET https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/shots/{match_id}.json`

**Valid URL:** [https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/shots/2928321.json](https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/shots/2928321.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), e.g. 2928321. Not the onlajny game id. |

### Returns {#elh_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hokejcz_game_id` | integer | hokej.cz game id (the S3 file name; shots match.id, scoreboard hokejcz_id). Int64. |
| `game_date` | character | Game date (shots: YYYY-MM-DD string; scoreboard: Date). |
| `hokejcz_home_team_id` | integer | hokej.cz team id of the home team (Sparta = 8); NOT the onlajny id of the play-by-play. |
| `home_team_name` | character | Home team full name. |
| `home_team_code` | character | Home team code, e.g. SPA. |
| `hokejcz_away_team_id` | integer | hokej.cz team id of the away team (Vitkovice = 7); NOT the onlajny id of the play-by-play. |
| `away_team_name` | character | Away team full name. |
| `away_team_code` | character | Away team code, e.g. VIT. |
| `shot_id` | integer | Feed id of the shot event. |
| `hokejcz_player_id` | integer | hokej.cz player id of the shooter (Hradec = 10093378); NOT an onlajny id. No player crosswalk is published. |
| `jersey` | integer | Shooter's jersey number (the join key for a per-game crosswalk with the play-by-play names). |
| `first_name` | character | Shooter's given name. |
| `last_name` | character | Shooter's family name. |
| `hokejcz_team_id` | integer | hokej.cz team id of the shooting team (equals the home or away team id). |
| `team_side` | character | The team's side: home or away. |
| `match_shot_resutl_id` | integer | Shot result code, provider's typo kept: 4 = goal (verified against the play-by-play); 1/2/3/5 unknown. |
| `shot_result_id` | integer | Alias of match_shot_resutl_id: 4 = goal; 1/2/3/5 unknown. |
| `is_goal` | logical | True when the result code is 4 (goal). |
| `time` | integer | Seconds from the start of the game; in 2016-17 only the period's start offset (0 / 1200 / 2400), use real_date. |
| `period` | integer | Period number derived from the game clock (20-minute periods; 4 = overtime); null on a clockless entry. |
| `coordinate_x` | numeric | Shot position along the rink LENGTH (goal to goal), centred at 0, normalised so the home team shoots at +x and the away team at -x; about -100..100. The scaling (units) and the orientation are inferred from the data, not documented by the provider. |
| `coordinate_y` | numeric | Shot position across the rink WIDTH (board to board), centred at 0; about -100..100. The scaling (each axis looks independently scaled to -100..100) and which board is positive are inferred from the data, not documented by the provider. |
| `real_date` | character | Wall-clock time of the shot, local YYYY-MM-DD HH:MM:SS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#elh_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A missing file (404 NoSuchKey), a scoreboard day outside the rolling window, or a stub body (no comments / shots / minutes, no Extraliga game that day). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope, a body for another game or day, or a broken listing page. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#elh_shots-example}

```python
elh_shots(match_id=2928321)
```

_Last validated n/a._
