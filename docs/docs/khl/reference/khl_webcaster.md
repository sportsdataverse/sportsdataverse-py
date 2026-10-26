---
title: KHL — KHL webcaster mobile API (khl.api.webcaster.pro)
sidebar_label: KHL webcaster mobile API (khl.api.webcaster.pro)
description: "KHL — KHL webcaster mobile API (khl.api.webcaster.pro) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# KHL — KHL webcaster mobile API (khl.api.webcaster.pro)

`sportsdataverse.hockey.khl` — 9 endpoints.

> **Note:** Owner flag: the KHL and its webcaster platform are run by Russian operators, some clubs by sanctioned state companies. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first.

## khl_webcaster_stages

Season table: every stage (season END year, regular / playoff) with its khl.ru tournament id and webcaster stage id.

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/data.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/data.json?locale=en](https://khl.api.webcaster.pro/api/khl_mobile/data.json?locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_stages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26) |
| `type` | String | regular or playoff |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `stage_id` | Int64 | Webcaster stage id (its own id space; not a khl.ru tournament id) |
| `season_label` | String | Season label as sent (26/27, 2025/2026) |
| `title` | String | Stage title (Regular / Playoff) |
| `display_rule` | String | Front-end display rule key |
| `percent_points_scored` | Boolean | Front-end flag: rank the stage by points percentage (False in every stage captured; unverified) |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_stages-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_stages-example}

```python
khl_webcaster_stages()
```

## khl_webcaster_event

One game: officials, team totals, per-player match stats, goals, penalties (one row; lists as JSON text).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/event_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/event_v2.json?id=2786714&locale=en](https://khl.api.webcaster.pro/api/khl_mobile/event_v2.json?id=2786714&locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `event_id` |  | `Y` |  | Required. Webcaster event id (khl_webcaster_events id), not the khl.ru game id. |
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_state_key` | String | Game state key (finished, ...) |
| `period` | Int64 | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `hd` | Boolean | True when the broadcast is in HD |
| `id` | Int64 | Webcaster id of the `arena` object (webcaster key space); no khl.ru counterpart in the body |
| `season` | String | Season END year (2026 = 2025-26) |
| `stage_id` | Int64 | Webcaster stage id (its own id space; not a khl.ru tournament id) |
| `type_id` | Int64 | Webcaster event type code |
| `commentator` | Boolean | True when the broadcast has a commentator |
| `likes_enabled` | Boolean | True when likes are enabled on the broadcast page |
| `is_vertical_video` | Boolean | True when the broadcast video is vertical (portrait) |
| `khl_id` | Int64 | khl.ru game id of the `event` object (site key space): the join key to khl-rest |
| `arena_id` | Int64 | khl.ru arena id |
| `arena_capacity` | Int64 | Arena seating capacity |
| `arena_phone` | String | Arena phone number as published |
| `arena_website` | String | Arena website URL |
| `arena_name` | String | Arena (city) name, from data.ARENAS |
| `arena_address` | String | Arena street address (English with locale=en) |
| `arena_city` | String | Arena city (English with locale=en) |
| `arena_image` | String | Arena photo URL on khl.ru |
| `arena_geo_lat` | Float64 | Arena latitude, decimal degrees |
| `arena_geo_long` | Float64 | Arena longitude, decimal degrees |
| `team_a_id` | Int64 | Webcaster id of the `team_a` object (webcaster key space); join to khl.ru through its khl_id |
| `team_a_khl_id` | Int64 | khl.ru club id of the `team_a` object (site key space): the join key to khl-rest |
| `team_a_image` | String | Home team logo URL (webcaster thumbnail) |
| `team_a_name` | String | Home team name (English with locale=en) |
| `team_a_location` | String | Home team city / region (English with locale=en) |
| `team_a_shots` | Int64 | Home shots on goal |
| `team_a_gf` | Int64 | Home team goals (final) |
| `team_a_ppg` | Int64 | Power-play goals |
| `team_a_shg` | Int64 | Short-handed goals |
| `team_a_ppc` | Int64 | Count; equals the team's own penalties in the sample (5 for 10 PIM), so probably penalties taken. UNVERIFIED |
| `team_a_vbr` | Int64 | Faceoffs won ('vbr' from Russian вбрасывания; = game/text fow) |
| `team_a_pim` | Int64 | Penalty minutes |
| `team_a_total_puck_control_time` | Float64 | Team puck-control time, decimal MINUTES (21.81 = 21:48) |
| `team_a_total_distance_travelled` | Float64 | Distance skated by the team, km (= game/text dtA) |
| `team_a_offensive_blue_line_crossings_count` | Int64 | Offensive blue-line crossings |
| `team_a_top_players` | String | Home game leaders (hardest shot, ...), JSON list with the webcaster and khl.ru player ids |
| `team_a_start_fives` | String | Home starting lineup, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_a_players` | String | Home game roster, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_a_active` | Boolean | True when the home team is active in the webcaster app |
| `team_b_id` | Int64 | Webcaster id of the `team_b` object (webcaster key space); join to khl.ru through its khl_id |
| `team_b_khl_id` | Int64 | khl.ru club id of the `team_b` object (site key space): the join key to khl-rest |
| `team_b_image` | String | Visitor team logo URL (webcaster thumbnail) |
| `team_b_name` | String | Visitor team name (English with locale=en) |
| `team_b_location` | String | Visitor team city / region (English with locale=en) |
| `team_b_shots` | Int64 | Visitor shots on goal |
| `team_b_gf` | Int64 | Visitor team goals (final) |
| `team_b_ppg` | Int64 | Power-play goals |
| `team_b_shg` | Int64 | Short-handed goals |
| `team_b_ppc` | Int64 | Count; equals the team's own penalties in the sample (3 for 6 PIM), so probably penalties taken. UNVERIFIED |
| `team_b_vbr` | Int64 | Faceoffs won ('vbr' from Russian вбрасывания; = game/text fow) |
| `team_b_pim` | Int64 | Penalty minutes |
| `team_b_total_puck_control_time` | Float64 | Team puck-control time, decimal MINUTES (21.81 = 21:48) |
| `team_b_total_distance_travelled` | Float64 | Distance skated by the team, km (= game/text dtA) |
| `team_b_offensive_blue_line_crossings_count` | Int64 | Offensive blue-line crossings |
| `team_b_top_players` | String | Visitor game leaders (hardest shot, ...), JSON list with the webcaster and khl.ru player ids |
| `team_b_start_fives` | String | Visitor starting lineup, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_b_players` | String | Visitor game roster, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_b_active` | Boolean | True when the visitor team is active in the webcaster app |
| `quotes` | String | Video clips (webcaster quotes) |
| `goals` | String | Goals of the game, JSON list (time in game seconds, score, status, author, assistants) |
| `violations` | String | Penalties in order |
| `other_events_with_both_teams` | String | Other games between the two teams, JSON list of webcaster events |
| `social_tags` | String | Social-media hashtags of the game, JSON list |
| `free` | Boolean | True when the broadcast is free to watch (no subscription) |
| `yandex_rights` | Boolean | True when Yandex holds rights to the broadcast (unverified) |
| `yandex_id` | String | Yandex video id of the broadcast (m-prefixed) |
| `match_id` | Int64 | khl.ru game id as a string (= khl_id) |
| `balancer_type` | String | Video delivery type (record = archived broadcast) |
| `event_or_quote_type_name` | String | Video category label in Russian (Архив = archive) |
| `outer_stage_id` | Int64 | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `stage_name` | String | Stage label (Regular 2025/2026) |
| `parent_id` | Int64 |  |
| `name` | String | Name (English with locale=en) |
| `mref1` | String | Referee 1, name in Russian |
| `mref2` | String | Referee 2, name in Russian |
| `lref1` | String | Linesman 1, name in Russian |
| `lref2` | String | Linesman 2, name in Russian |
| `announce` | String |  |
| `views` | String | Broadcast view count as display text ('7,579') |
| `stage_type` | String | Stage type: regular or playoff |
| `audio_tracks` | String | Broadcast audio tracks, JSON list of id / title (main, intersound) |
| `image_big` | String | Large broadcast thumbnail image URL |
| `location` | String | Home team city / region (English with locale=en) |
| `start_at_day` | Int64 | Game day start (midnight Moscow time), unix seconds |
| `start_at` | Int64 | Start time, unix milliseconds |
| `event_start_at` | Int64 | Puck-drop time, unix milliseconds |
| `end_at` | Int64 | End time, unix milliseconds |
| `tickets` | String |  |
| `text_events` | String | Summary text events only; no shot-level play-by-play |
| `not_regular` | Boolean | True for a game outside the regular schedule (False in the captured bodies; unverified) |
| `infographics` | String | Scoreboard overlay label of the broadcast (With scoreboard) |
| `infographics_enabled` | Boolean | True when the broadcast carries the scoreboard overlay |
| `score` | String | Score after the goal 'a:b' |
| `condensed_game_id` | Int64 | Webcaster id of the `condensed_game` object (webcaster key space); no khl.ru counterpart in the body |
| `condensed_game_name` | String | Condensed-game video title |
| `condensed_game_start_ts` | Int64 | Condensed-game video start time, unix milliseconds |
| `condensed_game_m3u8_url` | String | Condensed-game video HLS (m3u8) stream URL; signed and expiring |
| `condensed_game_feed_url` | String | Condensed-game video webcaster player feed URL; signed and expiring |
| `condensed_game_iframe_url` | String | Condensed-game video embeddable player (iframe) URL |
| `condensed_game_iframe_code` | String | Condensed-game video embed HTML (an iframe tag) |
| `condensed_game_finish_ts` | Int64 | Condensed-game video end time, unix milliseconds |
| `condensed_game_image_url` | String | Condensed-game video thumbnail image URL |
| `condensed_game_description` | String | Condensed-game video description text (HTML-escaped; Russian goal list on highlights) |
| `condensed_game_audio_tracks` | String | Condensed-game video audio tracks, JSON list of id / title (main, intersound) |
| `condensed_game_is_vertical_video` | Boolean | Condensed-game video: True when the video is vertical (portrait) |
| `condensed_game_stage_id` | Int64 | Condensed-game video: webcaster stage id (its own id space; not a khl.ru tournament id) |
| `condensed_game_free` | Boolean | Condensed-game video: True when free to watch (no subscription) |
| `condensed_game_yandex_rights` | Boolean | Condensed-game video: True when Yandex holds rights to the video (unverified) |
| `condensed_game_yandex_id` | String | Condensed-game video: Yandex video id of the broadcast (m-prefixed) |
| `condensed_game_match_id` | Int64 | khl.ru game id as a string (= khl_id) |
| `condensed_game_balancer_type` | String | Condensed-game video: video delivery type (record = archived broadcast, file = clip) |
| `condensed_game_event_or_quote_type_name` | String | Condensed-game video: video category label (Архив = archive, Хайлайты = highlights) |
| `condensed_game_outer_stage_id` | Int64 | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `condensed_game_stage_name` | String | Condensed-game video: stage label (Regular 2025/2026) |
| `highlight_id` | Int64 | Webcaster id of the `highlight` object (webcaster key space); no khl.ru counterpart in the body |
| `highlight_name` | String | Highlights video title |
| `highlight_start_ts` | Int64 | Highlights video start time, unix milliseconds |
| `highlight_m3u8_url` | String | Highlights video HLS (m3u8) stream URL; signed and expiring |
| `highlight_feed_url` | String | Highlights video webcaster player feed URL; signed and expiring |
| `highlight_iframe_url` | String | Highlights video embeddable player (iframe) URL |
| `highlight_iframe_code` | String | Highlights video embed HTML (an iframe tag) |
| `highlight_finish_ts` | Int64 | Highlights video end time, unix milliseconds |
| `highlight_image_url` | String | Highlights video thumbnail image URL |
| `highlight_description` | String | Highlights video description text (HTML-escaped; Russian goal list on highlights) |
| `highlight_audio_tracks` | String | Highlights video audio tracks, JSON list of id / title (main, intersound) |
| `highlight_is_vertical_video` | Boolean | Highlights video: True when the video is vertical (portrait) |
| `highlight_stage_id` | Int64 | Highlights video: webcaster stage id (its own id space; not a khl.ru tournament id) |
| `highlight_free` | Boolean | Highlights video: True when free to watch (no subscription) |
| `highlight_yandex_rights` | Boolean | Highlights video: True when Yandex holds rights to the video (unverified) |
| `highlight_yandex_id` | String | Highlights video: Yandex video id of the broadcast (m-prefixed) |
| `highlight_match_id` | Int64 | khl.ru game id as a string (= khl_id) |
| `highlight_balancer_type` | String | Highlights video: video delivery type (record = archived broadcast, file = clip) |
| `highlight_event_or_quote_type_name` | String | Highlights video: video category label (Архив = archive, Хайлайты = highlights) |
| `highlight_outer_stage_id` | Int64 | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `highlight_stage_name` | String | Highlights video: stage label (Regular 2025/2026) |
| `outer_url` | String | khl.ru page of the game (the game_id and tournament_id are in its path) |
| `m3u8_url` | String | Broadcast HLS (m3u8) stream URL; empty without rights |
| `feed_url` | String | Broadcast webcaster player feed URL; empty without rights |
| `iframe_url` | String | Broadcast embeddable player (iframe) URL |
| `iframe_code` | String | Broadcast embed HTML (an iframe tag) |
| `scores_first_period` | String | 1st-period score 'a:b' |
| `scores_second_period` | String | 2nd-period score 'a:b' |
| `scores_third_period` | String | 3rd-period score 'a:b' |
| `scores_overtime` | String | Overtime score 'a:b' (null if none) |
| `scores_bullitt` | String | Shootout score 'a:b' (null if none); 'bullitt' is Russian for shootout |
| `this_pair_stat_events_count` | Int64 | Head-to-head: games between the two teams on record (scope unverified) |
| `this_pair_stat_team_a_wins_count` | Int64 | Head-to-head: home team wins in those games |
| `this_pair_stat_team_a_goals_count` | Int64 | Head-to-head: home team goals in those games |
| `this_pair_stat_team_a_points_count` | Int64 | Head-to-head: home team standings points in those games |
| `this_pair_stat_team_b_wins_count` | Int64 | Head-to-head: visitor team wins in those games |
| `this_pair_stat_team_b_goals_count` | Int64 | Head-to-head: visitor team goals in those games |
| `this_pair_stat_team_b_points_count` | Int64 | Head-to-head: visitor team standings points in those games |
| `transaction_types` | String | Paid video products (app store) |
| `sscore` | String |  |
| `commentators_names` | String | Broadcast commentators, comma-separated |
| `transactions` | String | JSON-encoded list of the team's roster transactions over the requested window. |
| `has_video` | Boolean | True when the broadcast has a video |
| `geo_error` | String |  |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_event-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_event-example}

```python
khl_webcaster_event(event_id=2786714)
```

## khl_webcaster_events

Games of a stage, 16 per page (one row per game; khl_id is the khl.ru game id).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/events_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/events_v2.json?season=2026&stage=regular&page=1&locale=en](https://khl.api.webcaster.pro/api/khl_mobile/events_v2.json?season=2026&stage=regular&page=1&locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL webcaster stage id through the webcaster season table (khl_webcaster_stages). None = the current stage. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `stage_id` | `stage_id` |  |  | `Y` | Webcaster stage id, sent as is instead of season / stage (370 = 2025-26 regular season; khl_webcaster_stages lists them). Not a khl.ru tournament id. |
| `page` | `page` |  |  | `Y` | 1-based page of 16 games; an empty page ends the stage. |
| `q[team_a_or_team_b_in][]` | `team_ids` |  |  | `Y` | Webcaster team id(s) (khl_webcaster_teams id, not a khl.ru club id): games of these teams. |
| `q[start_at_gt_time_from_unixtime]` | `start_after` |  |  | `Y` | Games starting after this unix time (seconds). Needs a stage. |
| `q[start_at_lt_time_from_unixtime]` | `start_before` |  |  | `Y` | Games starting before this unix time (seconds). Needs a stage. |
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_state_key` | String | Game state key (only finished observed) |
| `period` | Int64 | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `hd` | Boolean | HD video available |
| `id` | Int64 | Webcaster id of the `event` object (webcaster key space); join to khl.ru through its khl_id |
| `stage_id` | Int64 | Webcaster stage id (its own id space; not a khl.ru tournament id) |
| `type_id` | Int64 | Webcaster event type code |
| `commentator` | Boolean | Commentary track available |
| `likes_enabled` | Boolean | True when likes are enabled on the broadcast page |
| `is_vertical_video` | Boolean | True when the broadcast video is vertical (portrait) |
| `khl_id` | Int64 | khl.ru game id of the `event` object (site key space): the join key to khl-rest |
| `team_a_id` | Int64 | Webcaster id of the `team_a` object (webcaster key space); join to khl.ru through its khl_id |
| `team_a_khl_id` | Int64 | khl.ru club id of the `team_a` object (site key space): the join key to khl-rest |
| `team_a_image` | String | Home team logo URL (webcaster thumbnail) |
| `team_a_name` | String | Home team name (English with locale=en) |
| `team_a_location` | String | Home team city / region (English with locale=en) |
| `team_b_id` | Int64 | Webcaster id of the `team_b` object (webcaster key space); join to khl.ru through its khl_id |
| `team_b_khl_id` | Int64 | khl.ru club id of the `team_b` object (site key space): the join key to khl-rest |
| `team_b_image` | String | Visitor team logo URL (webcaster thumbnail) |
| `team_b_name` | String | Visitor team name (English with locale=en) |
| `team_b_location` | String | Visitor team city / region (English with locale=en) |
| `name` | String | Matchup label in English |
| `tickets` | String |  |
| `location` | String | Home team city / region (English with locale=en) |
| `start_at_day` | Int64 | Game day start (midnight Moscow time), unix seconds |
| `start_at` | Int64 | Start time, unix milliseconds |
| `event_start_at` | Int64 | Puck-drop time, unix milliseconds |
| `end_at` | Int64 | End time, unix milliseconds |
| `not_regular` | Boolean | True for a game outside the regular schedule (False in the captured bodies; unverified) |
| `score` | String | Final score 'team_a:team_b' |
| `scores_first_period` | String | 1st-period score 'a:b' |
| `scores_second_period` | String | 2nd-period score 'a:b' |
| `scores_third_period` | String | 3rd-period score 'a:b' |
| `scores_overtime` | String | Overtime score 'a:b' (null if none) |
| `scores_bullitt` | String | Shootout score 'a:b' (null if none); 'bullitt' is Russian for shootout |
| `sscore` | String |  |
| `image` | String | Broadcast thumbnail image URL |
| `condensed_game_id` | Int64 | Webcaster event id of the condensed-game video |
| `highlight_id` | Int64 | Webcaster event id of the highlights video |
| `infographics` | String | Scoreboard overlay label of the broadcast (With scoreboard) |
| `infographics_enabled` | Boolean | True when the broadcast carries the scoreboard overlay |
| `has_video` | Boolean | Video available |
| `m3u8_url` | String | Broadcast HLS (m3u8) stream URL; empty without rights |
| `feed_url` | String | Broadcast webcaster player feed URL; empty without rights |
| `iframe_url` | String | Broadcast embeddable player (iframe) URL |
| `iframe_code` | String | Broadcast embed HTML (an iframe tag) |
| `free` | Boolean | Video free to watch |
| `yandex_rights` | Boolean | Yandex broadcast rights flag |
| `yandex_id` | String | Yandex video id of the broadcast (m-prefixed) |
| `match_id` | Int64 | khl.ru game id as a string (= khl_id) |
| `balancer_type` | String | Video delivery type (record = archived broadcast) |
| `event_or_quote_type_name` | String | Video category label in Russian (Архив = archive) |
| `outer_stage_id` | Int64 | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `stage_name` | String | Stage label (Regular 2025/2026) |
| `previews` | String |  |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_events-example}

```python
khl_webcaster_events(season=2026)
```

## khl_webcaster_leaders

Stat leaders of a stage: one row per player per category.

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/leaders_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/leaders_v2.json?season=2026&stage=regular&locale=en](https://khl.api.webcaster.pro/api/khl_mobile/leaders_v2.json?season=2026&stage=regular&locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL webcaster stage id through the webcaster season table (khl_webcaster_stages). None = the current stage. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `stage_id` | `stage_id` |  |  | `Y` | Webcaster stage id, sent as is instead of season / stage (370 = 2025-26 regular season; khl_webcaster_stages lists them). Not a khl.ru tournament id. |
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String | Leader category |
| `rank` | Int64 | 1-based position in its table / list |
| `id` | Int64 | Webcaster id of the `players` object (webcaster key space); join to khl.ru through its khl_id |
| `khl_id` | Int64 | khl.ru player id of the `players` object (site key space): the join key to khl-rest |
| `name` | String | Name (English with locale=en) |
| `image` | String | Player photo URL on khl.ru |
| `shirt_number` | Int64 | Jersey number (not an id) |
| `role_key` | String | Position key: goaltender, defensemen, forward |
| `team_id` | Int64 | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `team_khl_id` | Int64 | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `team_name` | String | Team name as the site prints it |
| `team_location` | String | Team city / region (English with locale=en) |
| `pos` | String | Leader position in the category, as text ('1' = leader) |
| `stats` | String | Category value(s) |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_leaders-example}

```python
khl_webcaster_leaders(season=2026)
```

## khl_webcaster_players

Players with stage-scoped stats (one row per player; khl_id is the khl.ru player id).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/players_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/players_v2.json?q%5Bid_in%5D%5B%5D=33405&q%5Bid_in%5D%5B%5D=1144&season=2026&stage=regular&locale=en](https://khl.api.webcaster.pro/api/khl_mobile/players_v2.json?q%5Bid_in%5D%5B%5D=33405&q%5Bid_in%5D%5B%5D=1144&season=2026&stage=regular&locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `q[id_in][]` | `player_ids` |  |  | `Y` | Webcaster player id(s), not khl.ru ids. None = every player of the stage (a large body). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL webcaster stage id through the webcaster season table (khl_webcaster_stages). None = the current stage. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `stage_id` | `stage_id` |  |  | `Y` | Webcaster stage id, sent as is instead of season / stage (370 = 2025-26 regular season; khl_webcaster_stages lists them). Not a khl.ru tournament id. |
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Webcaster id of the `player` object (webcaster key space); join to khl.ru through its khl_id |
| `height` | Int64 | Player height in cm |
| `weight` | Int64 | Player weight in kg |
| `age` | Int64 | Player age in years |
| `shirt_number` | Int64 | Jersey number (not an id) |
| `khl_id` | Int64 | khl.ru player id of the `player` object (site key space): the join key to khl-rest |
| `role` | String | Position: goaltender, defensemen or forward |
| `role_key` | String | Position key: goaltender, defensemen, forward |
| `country` | String | Country (English with locale=en) |
| `image` | String | Player photo URL on khl.ru |
| `flag_image_url` | String | Country flag image URL |
| `name` | String | Player name (English with locale=en) |
| `birthday` | Int64 | Birth date, unix seconds |
| `stick` | String | Shooting hand: l or r |
| `team_id` | Int64 | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `team_khl_id` | Int64 | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `team_name` | String | Team name as the site prints it |
| `team_location` | String | Team city / region (English with locale=en) |
| `team_image` | String | Team logo URL (webcaster thumbnail) |
| `team_division` | String | Team division name (English with locale=en) |
| `team_division_key` | String | Team division key (bobrov, tarasov, kharlamov, chernyshev) |
| `team_conference` | String | Team conference name (West / East) |
| `team_conference_key` | String | Team conference key (west / east) |
| `stats` | String | Stage-scoped stat lines |
| `teams` | String | Teams played for |
| `quotes` | String | Video clips (webcaster quotes) |
| `seasons_count_khl` | Int64 | KHL seasons played |
| `seasons_count_team` | Int64 | Seasons played for the current team |
| `positions` | String | Positions played |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_players-example}

```python
khl_webcaster_players(player_ids=[33405, 1144], season=2026)
```

## khl_webcaster_players_light

Every current player: webcaster id, khl.ru id, jersey and team (one row per player).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/players_v2_light.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/players_v2_light.json?locale=en](https://khl.api.webcaster.pro/api/khl_mobile/players_v2_light.json?locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_players_light-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Webcaster id of the `row` object (webcaster key space); join to khl.ru through its khl_id |
| `shirt_number` | Int64 | Jersey number (not an id) |
| `khl_id` | Int64 | khl.ru id of the `row` object (site key space): the join key to khl-rest |
| `image` | String | Player photo URL on khl.ru |
| `name` | String | Name (English with locale=en) |
| `team_id` | Int64 | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `team_khl_id` | Int64 | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `team_name` | String | Team name as the site prints it |
| `team_location` | String | Team city / region (English with locale=en) |
| `team` | String | Team (webcaster) |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_players_light-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_players_light-example}

```python
khl_webcaster_players_light()
```

## khl_webcaster_tables

League table of every season (one row per season per team, from the championship table).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/tables_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/tables_v2.json?locale=en](https://khl.api.webcaster.pro/api/khl_mobile/tables_v2.json?locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_tables-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26) |
| `season_label` | String | Season label as sent (26/27, 2025/2026) |
| `rank` | Int64 | 1-based position in its table / list |
| `gp` | Int64 | Games played (gp = w + otw + sow + sol + otl + l) |
| `w` | Int64 | Regulation wins only: overtime (otw) and shootout (sow) wins are separate columns |
| `otw` | Int64 | Overtime wins (not counted in w) |
| `sow` | Int64 | Shootout wins (not counted in w) |
| `sol` | Int64 | Shootout losses |
| `otl` | Int64 | Overtime losses |
| `otl_en` | Int64 | Overtime losses with the net empty |
| `l` | Int64 | Regulation losses only: overtime (otl) and shootout (sol) losses are separate columns |
| `pts` | Int64 | Points: 2 per win of any kind, 1 per overtime or shootout loss (3-2-1 in early seasons such as 2013-14) |
| `pts_pct` | Float64 | Points percentage |
| `soa` | Int64 | Shutouts against, 1-7 a season (the refs say shots on goal against; the values do not fit that; unverified) |
| `so` | Int64 | Shutouts (refs: khl-returns.md tables_v2) |
| `gf` | Int64 | Goals scored by the team |
| `ga` | Int64 | Goals allowed by the team |
| `pim` | Int64 | Penalty minutes |
| `pima` | Int64 | Opponents' penalty minutes |
| `id` | Int64 | Webcaster id of the `stages` object (webcaster key space); no khl.ru counterpart in the body |
| `khl_id` | Int64 | khl.ru club id of the `championship` object (site key space): the join key to khl-rest |
| `image` | String | Team logo URL (webcaster thumbnail) |
| `tv_image` | String | Team logo path on the KHL TV front end |
| `name` | String | Name (English with locale=en) |
| `location` | String | Team city / region (English with locale=en) |
| `division` | String | Division name (English with locale=en) |
| `division_key` | String | Division key (bobrov, tarasov, kharlamov, chernyshev) |
| `conference` | String | Conference (west / east) |
| `conference_key` | String | Conference key (east / west) |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_tables-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_tables-example}

```python
khl_webcaster_tables()
```

## khl_webcaster_team

One team with roster, stats and news feed (one row; lists as JSON text).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/team_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/team_v2.json?id=18&season=2026&stage=regular&locale=en](https://khl.api.webcaster.pro/api/khl_mobile/team_v2.json?id=18&season=2026&stage=regular&locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `team_id` |  | `Y` |  | Required. Webcaster team id (khl_webcaster_teams id), not the khl.ru club id. |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL webcaster stage id through the webcaster season table (khl_webcaster_stages). None = the current stage. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `stage_id` | `stage_id` |  |  | `Y` | Webcaster stage id, sent as is instead of season / stage (370 = 2025-26 regular season; khl_webcaster_stages lists them). Not a khl.ru tournament id. |
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `stage` | String | Stage type the team page answers for: regular or playoff |
| `khl_id` | Int64 | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `name` | String | Name (English with locale=en) |
| `location` | String | Team city / region (English with locale=en) |
| `feed_items` | String | Team news feed, JSON list (type, date, id, title, image) |
| `arena_id` | Int64 | Webcaster id of the team's home arena |
| `arena_capacity` | Int64 | Arena seating capacity |
| `arena_phone` | String | Arena phone number as published |
| `arena_website` | String | Arena website URL |
| `arena_name` | String | Home arena name (English with locale=en) |
| `arena_address` | String | Arena street address (English with locale=en) |
| `arena_city` | String | Arena city (English with locale=en) |
| `arena_image` | String | Arena photo URL on khl.ru |
| `arena_geo_lat` | Float64 | Arena latitude, decimal degrees |
| `arena_geo_long` | Float64 | Arena longitude, decimal degrees |
| `arenas` | String | Home arenas, JSON list (id, capacity, phone, website, name, address) |
| `calendar_events` | String | Team schedule, JSON list of webcaster events |
| `recent_events` | String | Recent team games, JSON list of webcaster events |
| `website` | String | Club website URL |
| `mail` | String | Club e-mail address as published |
| `foundation_year` | String | Year the club was founded, as text |
| `photo` | String | Team photo URL on khl.ru |
| `phone` | String | Club phone number as published |
| `head_coach_photo` | String | Head coach photo URL on khl.ru |
| `head_coach_name` | String | Head coach name (English with locale=en) |
| `social_networks_tw` | String | Club Twitter / X page URL (empty when none) |
| `social_networks_vk` | String | Club VKontakte page URL |
| `social_networks_ok` | String | Club Odnoklassniki page URL |
| `social_networks_fb` | String | Club Facebook page URL (empty when none) |
| `social_networks_instagram` | String | Club Instagram page URL |
| `social_networks_youtube` | String | Club YouTube channel URL (empty when none) |
| `social_networks_telegram` | String | Club Telegram channel URL |
| `apps_ios` | String |  |
| `apps_android` | String |  |
| `apps_windows` | String |  |
| `address` | String | Club office address (English with locale=en) |
| `image` | String | Team logo URL (webcaster thumbnail) |
| `division` | String | Division name (English with locale=en) |
| `division_key` | String | Division key (bobrov, tarasov, kharlamov, chernyshev) |
| `conference` | String | Conference (west / east) |
| `conference_key` | String | Conference key (east / west) |
| `stats` | String | Team stat lines |
| `players` | String | Roster (webcaster players) |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_team-example}

```python
khl_webcaster_team(season=2026, team_id=18)
```

## khl_webcaster_teams

Teams of a stage (one row per team; khl_id is the khl.ru club id).

**Endpoint URL:** `GET https://khl.api.webcaster.pro/api/khl_mobile/teams_v2.json`

**Valid URL:** [https://khl.api.webcaster.pro/api/khl_mobile/teams_v2.json?season=2026&stage=regular&locale=en](https://khl.api.webcaster.pro/api/khl_mobile/teams_v2.json?season=2026&stage=regular&locale=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL webcaster stage id through the webcaster season table (khl_webcaster_stages). None = the current stage. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `stage_id` | `stage_id` |  |  | `Y` | Webcaster stage id, sent as is instead of season / stage (370 = 2025-26 regular season; khl_webcaster_stages lists them). Not a khl.ru tournament id. |
| `locale` | `locale` |  |  | `Y` | Response language: en or ru. |

### Returns {#khl_webcaster_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `khl_id` | Int64 | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `name` | String | Team name (English with locale=en) |
| `location` | String | Team city / region (English with locale=en) |
| `image` | String | Team logo URL (webcaster thumbnail) |
| `division` | String | Division name (English with locale=en) |
| `division_key` | String | Division key (bobrov, tarasov, kharlamov, chernyshev) |
| `conference` | String | Conference (west / east) |
| `conference_key` | String | Conference key (east/west) |

**`return_parsed=False`** — the raw JSON body (betting fields and the caller's IP removed).

### Raises {#khl_webcaster_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An error envelope with code 404 (unknown id), an empty object, or an unknown season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A body for another id / stage / player than the one requested, or any other error envelope. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_webcaster_teams-example}

```python
khl_webcaster_teams(season=2026)
```
