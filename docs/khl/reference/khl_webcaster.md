# KHL — KHL webcaster mobile API (khl.api.webcaster.pro)

> KHL — KHL webcaster mobile API (khl.api.webcaster.pro) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.khl` — 9 endpoints.

> **Note:** Owner flag (sdv-internal-refs/khl/README.md): the KHL and its webcaster platform are run by Russian operators, some clubs by sanctioned state companies. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first.

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
| `season` | integer | Season END year (2026 = 2025-26) |
| `type` | character | regular or playoff |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `stage_id` | integer | Webcaster stage id (its own id space; not a khl.ru tournament id) |
| `season_label` | character | Season label as sent (26/27, 2025/2026) |
| `title` | character | Stage title (Regular / Playoff) |
| `display_rule` | character | Front-end display rule key |
| `percent_points_scored` | logical | Front-end flag: rank the stage by points percentage (False in every stage captured; unverified) |

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

_Last validated n/a._

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
| `game_state_key` | character | Game state key (finished, ...) |
| `period` | integer | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `hd` | logical | True when the broadcast is in HD |
| `id` | integer | Webcaster id of the `arena` object (webcaster key space); no khl.ru counterpart in the body |
| `season` | character | Season END year (2026 = 2025-26) |
| `stage_id` | integer | Webcaster stage id (its own id space; not a khl.ru tournament id) |
| `type_id` | integer | Webcaster event type code |
| `commentator` | logical | True when the broadcast has a commentator |
| `likes_enabled` | logical | True when likes are enabled on the broadcast page |
| `is_vertical_video` | logical | True when the broadcast video is vertical (portrait) |
| `khl_id` | integer | khl.ru game id of the `event` object (site key space): the join key to khl-rest |
| `arena_id` | integer | khl.ru arena id |
| `arena_capacity` | integer | Arena seating capacity |
| `arena_phone` | character | Arena phone number as published |
| `arena_website` | character | Arena website URL |
| `arena_name` | character | Arena (city) name, from data.ARENAS |
| `arena_address` | character | Arena street address (English with locale=en) |
| `arena_city` | character | Arena city (English with locale=en) |
| `arena_image` | character | Arena photo URL on khl.ru |
| `arena_geo_lat` | numeric | Arena latitude, decimal degrees |
| `arena_geo_long` | numeric | Arena longitude, decimal degrees |
| `team_a_id` | integer | Webcaster id of the `team_a` object (webcaster key space); join to khl.ru through its khl_id |
| `team_a_khl_id` | integer | khl.ru club id of the `team_a` object (site key space): the join key to khl-rest |
| `team_a_image` | character | Home team logo URL (webcaster thumbnail) |
| `team_a_name` | character | Home team name (English with locale=en) |
| `team_a_location` | character | Home team city / region (English with locale=en) |
| `team_a_shots` | integer | Home shots on goal |
| `team_a_gf` | integer | Home team goals (final) |
| `team_a_ppg` | integer | Power-play goals |
| `team_a_shg` | integer | Short-handed goals |
| `team_a_ppc` | integer | Count; equals the team's own penalties in the sample (5 for 10 PIM), so probably penalties taken. UNVERIFIED |
| `team_a_vbr` | integer | Faceoffs won ('vbr' from Russian вбрасывания; = game/text fow) |
| `team_a_pim` | integer | Penalty minutes |
| `team_a_total_puck_control_time` | numeric | Team puck-control time, decimal MINUTES (21.81 = 21:48) |
| `team_a_total_distance_travelled` | numeric | Distance skated by the team, km (= game/text dtA) |
| `team_a_offensive_blue_line_crossings_count` | integer | Offensive blue-line crossings |
| `team_a_top_players` | character | Home game leaders (hardest shot, ...), JSON list with the webcaster and khl.ru player ids |
| `team_a_start_fives` | character | Home starting lineup, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_a_players` | character | Home game roster, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_a_active` | logical | True when the home team is active in the webcaster app |
| `team_b_id` | integer | Webcaster id of the `team_b` object (webcaster key space); join to khl.ru through its khl_id |
| `team_b_khl_id` | integer | khl.ru club id of the `team_b` object (site key space): the join key to khl-rest |
| `team_b_image` | character | Visitor team logo URL (webcaster thumbnail) |
| `team_b_name` | character | Visitor team name (English with locale=en) |
| `team_b_location` | character | Visitor team city / region (English with locale=en) |
| `team_b_shots` | integer | Visitor shots on goal |
| `team_b_gf` | integer | Visitor team goals (final) |
| `team_b_ppg` | integer | Power-play goals |
| `team_b_shg` | integer | Short-handed goals |
| `team_b_ppc` | integer | Count; equals the team's own penalties in the sample (3 for 6 PIM), so probably penalties taken. UNVERIFIED |
| `team_b_vbr` | integer | Faceoffs won ('vbr' from Russian вбрасывания; = game/text fow) |
| `team_b_pim` | integer | Penalty minutes |
| `team_b_total_puck_control_time` | numeric | Team puck-control time, decimal MINUTES (21.81 = 21:48) |
| `team_b_total_distance_travelled` | numeric | Distance skated by the team, km (= game/text dtA) |
| `team_b_offensive_blue_line_crossings_count` | integer | Offensive blue-line crossings |
| `team_b_top_players` | character | Visitor game leaders (hardest shot, ...), JSON list with the webcaster and khl.ru player ids |
| `team_b_start_fives` | character | Visitor starting lineup, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_b_players` | character | Visitor game roster, JSON list of players (webcaster id, khl_id, jersey, role) |
| `team_b_active` | logical | True when the visitor team is active in the webcaster app |
| `quotes` | character | Video clips (webcaster quotes) |
| `goals` | character | Goals of the game, JSON list (time in game seconds, score, status, author, assistants) |
| `violations` | character | Penalties in order |
| `other_events_with_both_teams` | character | Other games between the two teams, JSON list of webcaster events |
| `social_tags` | character | Social-media hashtags of the game, JSON list |
| `free` | logical | True when the broadcast is free to watch (no subscription) |
| `yandex_rights` | logical | True when Yandex holds rights to the broadcast (unverified) |
| `yandex_id` | character | Yandex video id of the broadcast (m-prefixed) |
| `match_id` | integer | khl.ru game id as a string (= khl_id) |
| `balancer_type` | character | Video delivery type (record = archived broadcast) |
| `event_or_quote_type_name` | character | Video category label in Russian (Архив = archive) |
| `outer_stage_id` | integer | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `stage_name` | character | Stage label (Regular 2025/2026) |
| `parent_id` | integer |  |
| `name` | character | Name (English with locale=en) |
| `mref1` | character | Referee 1, name in Russian |
| `mref2` | character | Referee 2, name in Russian |
| `lref1` | character | Linesman 1, name in Russian |
| `lref2` | character | Linesman 2, name in Russian |
| `announce` | character |  |
| `views` | character | Broadcast view count as display text ('7,579') |
| `stage_type` | character | Stage type: regular or playoff |
| `audio_tracks` | character | Broadcast audio tracks, JSON list of id / title (main, intersound) |
| `image_big` | character | Large broadcast thumbnail image URL |
| `location` | character | Home team city / region (English with locale=en) |
| `start_at_day` | integer | Game day start (midnight Moscow time), unix seconds |
| `start_at` | integer | Start time, unix milliseconds |
| `event_start_at` | integer | Puck-drop time, unix milliseconds |
| `end_at` | integer | End time, unix milliseconds |
| `tickets` | character |  |
| `text_events` | character | Summary text events only; no shot-level play-by-play |
| `not_regular` | logical | True for a game outside the regular schedule (False in the captured bodies; unverified) |
| `infographics` | character | Scoreboard overlay label of the broadcast (With scoreboard) |
| `infographics_enabled` | logical | True when the broadcast carries the scoreboard overlay |
| `score` | character | Score after the goal 'a:b' |
| `condensed_game_id` | integer | Webcaster id of the `condensed_game` object (webcaster key space); no khl.ru counterpart in the body |
| `condensed_game_name` | character | Condensed-game video title |
| `condensed_game_start_ts` | integer | Condensed-game video start time, unix milliseconds |
| `condensed_game_m3u8_url` | character | Condensed-game video HLS (m3u8) stream URL; signed and expiring |
| `condensed_game_feed_url` | character | Condensed-game video webcaster player feed URL; signed and expiring |
| `condensed_game_iframe_url` | character | Condensed-game video embeddable player (iframe) URL |
| `condensed_game_iframe_code` | character | Condensed-game video embed HTML (an iframe tag) |
| `condensed_game_finish_ts` | integer | Condensed-game video end time, unix milliseconds |
| `condensed_game_image_url` | character | Condensed-game video thumbnail image URL |
| `condensed_game_description` | character | Condensed-game video description text (HTML-escaped; Russian goal list on highlights) |
| `condensed_game_audio_tracks` | character | Condensed-game video audio tracks, JSON list of id / title (main, intersound) |
| `condensed_game_is_vertical_video` | logical | Condensed-game video: True when the video is vertical (portrait) |
| `condensed_game_stage_id` | integer | Condensed-game video: webcaster stage id (its own id space; not a khl.ru tournament id) |
| `condensed_game_free` | logical | Condensed-game video: True when free to watch (no subscription) |
| `condensed_game_yandex_rights` | logical | Condensed-game video: True when Yandex holds rights to the video (unverified) |
| `condensed_game_yandex_id` | character | Condensed-game video: Yandex video id of the broadcast (m-prefixed) |
| `condensed_game_match_id` | integer | khl.ru game id as a string (= khl_id) |
| `condensed_game_balancer_type` | character | Condensed-game video: video delivery type (record = archived broadcast, file = clip) |
| `condensed_game_event_or_quote_type_name` | character | Condensed-game video: video category label (Архив = archive, Хайлайты = highlights) |
| `condensed_game_outer_stage_id` | integer | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `condensed_game_stage_name` | character | Condensed-game video: stage label (Regular 2025/2026) |
| `highlight_id` | integer | Webcaster id of the `highlight` object (webcaster key space); no khl.ru counterpart in the body |
| `highlight_name` | character | Highlights video title |
| `highlight_start_ts` | integer | Highlights video start time, unix milliseconds |
| `highlight_m3u8_url` | character | Highlights video HLS (m3u8) stream URL; signed and expiring |
| `highlight_feed_url` | character | Highlights video webcaster player feed URL; signed and expiring |
| `highlight_iframe_url` | character | Highlights video embeddable player (iframe) URL |
| `highlight_iframe_code` | character | Highlights video embed HTML (an iframe tag) |
| `highlight_finish_ts` | integer | Highlights video end time, unix milliseconds |
| `highlight_image_url` | character | Highlights video thumbnail image URL |
| `highlight_description` | character | Highlights video description text (HTML-escaped; Russian goal list on highlights) |
| `highlight_audio_tracks` | character | Highlights video audio tracks, JSON list of id / title (main, intersound) |
| `highlight_is_vertical_video` | logical | Highlights video: True when the video is vertical (portrait) |
| `highlight_stage_id` | integer | Highlights video: webcaster stage id (its own id space; not a khl.ru tournament id) |
| `highlight_free` | logical | Highlights video: True when free to watch (no subscription) |
| `highlight_yandex_rights` | logical | Highlights video: True when Yandex holds rights to the video (unverified) |
| `highlight_yandex_id` | character | Highlights video: Yandex video id of the broadcast (m-prefixed) |
| `highlight_match_id` | integer | khl.ru game id as a string (= khl_id) |
| `highlight_balancer_type` | character | Highlights video: video delivery type (record = archived broadcast, file = clip) |
| `highlight_event_or_quote_type_name` | character | Highlights video: video category label (Архив = archive, Хайлайты = highlights) |
| `highlight_outer_stage_id` | integer | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `highlight_stage_name` | character | Highlights video: stage label (Regular 2025/2026) |
| `outer_url` | character | khl.ru page of the game (the game_id and tournament_id are in its path) |
| `m3u8_url` | character | Broadcast HLS (m3u8) stream URL; empty without rights |
| `feed_url` | character | Broadcast webcaster player feed URL; empty without rights |
| `iframe_url` | character | Broadcast embeddable player (iframe) URL |
| `iframe_code` | character | Broadcast embed HTML (an iframe tag) |
| `scores_first_period` | character | 1st-period score 'a:b' |
| `scores_second_period` | character | 2nd-period score 'a:b' |
| `scores_third_period` | character | 3rd-period score 'a:b' |
| `scores_overtime` | character | Overtime score 'a:b' (null if none) |
| `scores_bullitt` | character | Shootout score 'a:b' (null if none); 'bullitt' is Russian for shootout |
| `this_pair_stat_events_count` | integer | Head-to-head: games between the two teams on record (scope unverified) |
| `this_pair_stat_team_a_wins_count` | integer | Head-to-head: home team wins in those games |
| `this_pair_stat_team_a_goals_count` | integer | Head-to-head: home team goals in those games |
| `this_pair_stat_team_a_points_count` | integer | Head-to-head: home team standings points in those games |
| `this_pair_stat_team_b_wins_count` | integer | Head-to-head: visitor team wins in those games |
| `this_pair_stat_team_b_goals_count` | integer | Head-to-head: visitor team goals in those games |
| `this_pair_stat_team_b_points_count` | integer | Head-to-head: visitor team standings points in those games |
| `transaction_types` | character | Paid video products (app store) |
| `sscore` | character |  |
| `commentators_names` | character | Broadcast commentators, comma-separated |
| `transactions` | character | JSON-encoded list of the team's roster transactions over the requested window. |
| `has_video` | logical | True when the broadcast has a video |
| `geo_error` | character |  |

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

_Last validated n/a._

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
| `game_state_key` | character | Game state key (only finished observed) |
| `period` | integer | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `hd` | logical | HD video available |
| `id` | integer | Webcaster id of the `event` object (webcaster key space); join to khl.ru through its khl_id |
| `stage_id` | integer | Webcaster stage id (its own id space; not a khl.ru tournament id) |
| `type_id` | integer | Webcaster event type code |
| `commentator` | logical | Commentary track available |
| `likes_enabled` | logical | True when likes are enabled on the broadcast page |
| `is_vertical_video` | logical | True when the broadcast video is vertical (portrait) |
| `khl_id` | integer | khl.ru game id of the `event` object (site key space): the join key to khl-rest |
| `team_a_id` | integer | Webcaster id of the `team_a` object (webcaster key space); join to khl.ru through its khl_id |
| `team_a_khl_id` | integer | khl.ru club id of the `team_a` object (site key space): the join key to khl-rest |
| `team_a_image` | character | Home team logo URL (webcaster thumbnail) |
| `team_a_name` | character | Home team name (English with locale=en) |
| `team_a_location` | character | Home team city / region (English with locale=en) |
| `team_b_id` | integer | Webcaster id of the `team_b` object (webcaster key space); join to khl.ru through its khl_id |
| `team_b_khl_id` | integer | khl.ru club id of the `team_b` object (site key space): the join key to khl-rest |
| `team_b_image` | character | Visitor team logo URL (webcaster thumbnail) |
| `team_b_name` | character | Visitor team name (English with locale=en) |
| `team_b_location` | character | Visitor team city / region (English with locale=en) |
| `name` | character | Matchup label in English |
| `tickets` | character |  |
| `location` | character | Home team city / region (English with locale=en) |
| `start_at_day` | integer | Game day start (midnight Moscow time), unix seconds |
| `start_at` | integer | Start time, unix milliseconds |
| `event_start_at` | integer | Puck-drop time, unix milliseconds |
| `end_at` | integer | End time, unix milliseconds |
| `not_regular` | logical | True for a game outside the regular schedule (False in the captured bodies; unverified) |
| `score` | character | Final score 'team_a:team_b' |
| `scores_first_period` | character | 1st-period score 'a:b' |
| `scores_second_period` | character | 2nd-period score 'a:b' |
| `scores_third_period` | character | 3rd-period score 'a:b' |
| `scores_overtime` | character | Overtime score 'a:b' (null if none) |
| `scores_bullitt` | character | Shootout score 'a:b' (null if none); 'bullitt' is Russian for shootout |
| `sscore` | character |  |
| `image` | character | Broadcast thumbnail image URL |
| `condensed_game_id` | integer | Webcaster event id of the condensed-game video |
| `highlight_id` | integer | Webcaster event id of the highlights video |
| `infographics` | character | Scoreboard overlay label of the broadcast (With scoreboard) |
| `infographics_enabled` | logical | True when the broadcast carries the scoreboard overlay |
| `has_video` | logical | Video available |
| `m3u8_url` | character | Broadcast HLS (m3u8) stream URL; empty without rights |
| `feed_url` | character | Broadcast webcaster player feed URL; empty without rights |
| `iframe_url` | character | Broadcast embeddable player (iframe) URL |
| `iframe_code` | character | Broadcast embed HTML (an iframe tag) |
| `free` | logical | Video free to watch |
| `yandex_rights` | logical | Yandex broadcast rights flag |
| `yandex_id` | character | Yandex video id of the broadcast (m-prefixed) |
| `match_id` | integer | khl.ru game id as a string (= khl_id) |
| `balancer_type` | character | Video delivery type (record = archived broadcast) |
| `event_or_quote_type_name` | character | Video category label in Russian (Архив = archive) |
| `outer_stage_id` | integer | khl.ru tournament id of the event's stage (= stage.khl_id) |
| `stage_name` | character | Stage label (Regular 2025/2026) |
| `previews` | character |  |

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

_Last validated n/a._

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
| `category` | character | Leader category |
| `rank` | integer | 1-based position in its table / list |
| `id` | integer | Webcaster id of the `players` object (webcaster key space); join to khl.ru through its khl_id |
| `khl_id` | integer | khl.ru player id of the `players` object (site key space): the join key to khl-rest |
| `name` | character | Name (English with locale=en) |
| `image` | character | Player photo URL on khl.ru |
| `shirt_number` | integer | Jersey number (not an id) |
| `role_key` | character | Position key: goaltender, defensemen, forward |
| `team_id` | integer | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `team_khl_id` | integer | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `team_name` | character | Team name as the site prints it |
| `team_location` | character | Team city / region (English with locale=en) |
| `pos` | character | Leader position in the category, as text ('1' = leader) |
| `stats` | character | Category value(s) |

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

_Last validated n/a._

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
| `id` | integer | Webcaster id of the `player` object (webcaster key space); join to khl.ru through its khl_id |
| `height` | integer | Player height in cm |
| `weight` | integer | Player weight in kg |
| `age` | integer | Player age in years |
| `shirt_number` | integer | Jersey number (not an id) |
| `khl_id` | integer | khl.ru player id of the `player` object (site key space): the join key to khl-rest |
| `role` | character | Position: goaltender, defensemen or forward |
| `role_key` | character | Position key: goaltender, defensemen, forward |
| `country` | character | Country (English with locale=en) |
| `image` | character | Player photo URL on khl.ru |
| `flag_image_url` | character | Country flag image URL |
| `name` | character | Player name (English with locale=en) |
| `birthday` | integer | Birth date, unix seconds |
| `stick` | character | Shooting hand: l or r |
| `team_id` | integer | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `team_khl_id` | integer | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `team_name` | character | Team name as the site prints it |
| `team_location` | character | Team city / region (English with locale=en) |
| `team_image` | character | Team logo URL (webcaster thumbnail) |
| `team_division` | character | Team division name (English with locale=en) |
| `team_division_key` | character | Team division key (bobrov, tarasov, kharlamov, chernyshev) |
| `team_conference` | character | Team conference name (West / East) |
| `team_conference_key` | character | Team conference key (west / east) |
| `stats` | character | Stage-scoped stat lines |
| `teams` | character | Teams played for |
| `quotes` | character | Video clips (webcaster quotes) |
| `seasons_count_khl` | integer | KHL seasons played |
| `seasons_count_team` | integer | Seasons played for the current team |
| `positions` | character | Positions played |

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

_Last validated n/a._

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
| `id` | integer | Webcaster id of the `row` object (webcaster key space); join to khl.ru through its khl_id |
| `shirt_number` | integer | Jersey number (not an id) |
| `khl_id` | integer | khl.ru id of the `row` object (site key space): the join key to khl-rest |
| `image` | character | Player photo URL on khl.ru |
| `name` | character | Name (English with locale=en) |
| `team_id` | integer | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `team_khl_id` | integer | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `team_name` | character | Team name as the site prints it |
| `team_location` | character | Team city / region (English with locale=en) |
| `team` | character | Team (webcaster) |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26) |
| `season_label` | character | Season label as sent (26/27, 2025/2026) |
| `rank` | integer | 1-based position in its table / list |
| `gp` | integer | Games played (gp = w + otw + sow + sol + otl + l) |
| `w` | integer | Regulation wins only: overtime (otw) and shootout (sow) wins are separate columns |
| `otw` | integer | Overtime wins (not counted in w) |
| `sow` | integer | Shootout wins (not counted in w) |
| `sol` | integer | Shootout losses |
| `otl` | integer | Overtime losses |
| `otl_en` | integer | Overtime losses with the net empty |
| `l` | integer | Regulation losses only: overtime (otl) and shootout (sol) losses are separate columns |
| `pts` | integer | Points: 2 per win of any kind, 1 per overtime or shootout loss (3-2-1 in early seasons such as 2013-14) |
| `pts_pct` | numeric | Points percentage |
| `soa` | integer | Shutouts against, 1-7 a season (the refs say shots on goal against; the values do not fit that; unverified) |
| `so` | integer | Shutouts (refs: khl-returns.md tables_v2) |
| `gf` | integer | Goals scored by the team |
| `ga` | integer | Goals allowed by the team |
| `pim` | integer | Penalty minutes |
| `pima` | integer | Opponents' penalty minutes |
| `id` | integer | Webcaster id of the `stages` object (webcaster key space); no khl.ru counterpart in the body |
| `khl_id` | integer | khl.ru club id of the `championship` object (site key space): the join key to khl-rest |
| `image` | character | Team logo URL (webcaster thumbnail) |
| `tv_image` | character | Team logo path on the KHL TV front end |
| `name` | character | Name (English with locale=en) |
| `location` | character | Team city / region (English with locale=en) |
| `division` | character | Division name (English with locale=en) |
| `division_key` | character | Division key (bobrov, tarasov, kharlamov, chernyshev) |
| `conference` | character | Conference (west / east) |
| `conference_key` | character | Conference key (east / west) |

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

_Last validated n/a._

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
| `id` | integer | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `stage` | character | Stage type the team page answers for: regular or playoff |
| `khl_id` | integer | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `name` | character | Name (English with locale=en) |
| `location` | character | Team city / region (English with locale=en) |
| `feed_items` | character | Team news feed, JSON list (type, date, id, title, image) |
| `arena_id` | integer | Webcaster id of the team's home arena |
| `arena_capacity` | integer | Arena seating capacity |
| `arena_phone` | character | Arena phone number as published |
| `arena_website` | character | Arena website URL |
| `arena_name` | character | Home arena name (English with locale=en) |
| `arena_address` | character | Arena street address (English with locale=en) |
| `arena_city` | character | Arena city (English with locale=en) |
| `arena_image` | character | Arena photo URL on khl.ru |
| `arena_geo_lat` | numeric | Arena latitude, decimal degrees |
| `arena_geo_long` | numeric | Arena longitude, decimal degrees |
| `arenas` | character | Home arenas, JSON list (id, capacity, phone, website, name, address) |
| `calendar_events` | character | Team schedule, JSON list of webcaster events |
| `recent_events` | character | Recent team games, JSON list of webcaster events |
| `website` | character | Club website URL |
| `mail` | character | Club e-mail address as published |
| `foundation_year` | character | Year the club was founded, as text |
| `photo` | character | Team photo URL on khl.ru |
| `phone` | character | Club phone number as published |
| `head_coach_photo` | character | Head coach photo URL on khl.ru |
| `head_coach_name` | character | Head coach name (English with locale=en) |
| `social_networks_tw` | character | Club Twitter / X page URL (empty when none) |
| `social_networks_vk` | character | Club VKontakte page URL |
| `social_networks_ok` | character | Club Odnoklassniki page URL |
| `social_networks_fb` | character | Club Facebook page URL (empty when none) |
| `social_networks_instagram` | character | Club Instagram page URL |
| `social_networks_youtube` | character | Club YouTube channel URL (empty when none) |
| `social_networks_telegram` | character | Club Telegram channel URL |
| `apps_ios` | character |  |
| `apps_android` | character |  |
| `apps_windows` | character |  |
| `address` | character | Club office address (English with locale=en) |
| `image` | character | Team logo URL (webcaster thumbnail) |
| `division` | character | Division name (English with locale=en) |
| `division_key` | character | Division key (bobrov, tarasov, kharlamov, chernyshev) |
| `conference` | character | Conference (west / east) |
| `conference_key` | character | Conference key (east / west) |
| `stats` | character | Team stat lines |
| `players` | character | Roster (webcaster players) |

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

_Last validated n/a._

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
| `id` | integer | Webcaster id of the `team` object (webcaster key space); join to khl.ru through its khl_id |
| `khl_id` | integer | khl.ru club id of the `team` object (site key space): the join key to khl-rest |
| `name` | character | Team name (English with locale=en) |
| `location` | character | Team city / region (English with locale=en) |
| `image` | character | Team logo URL (webcaster thumbnail) |
| `division` | character | Division name (English with locale=en) |
| `division_key` | character | Division key (bobrov, tarasov, kharlamov, chernyshev) |
| `conference` | character | Conference (west / east) |
| `conference_key` | character | Conference key (east/west) |

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

_Last validated n/a._
