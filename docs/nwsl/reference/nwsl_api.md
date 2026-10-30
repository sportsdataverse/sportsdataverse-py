# NWSL — NWSL official web API (StatsPerform SDP)

> NWSL — NWSL official web API (StatsPerform SDP) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.soccer.nwsl` — 9 endpoints.

## nwsl_competitions

All competitions StatsPerform tracks for NWSL (league + friendlies/cups).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns {#nwsl_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | String | Composite Competition id (Utf8 join key). |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `name` | String | Stage display name. |
| `official_name` | String | Official team name. |
| `short_name` | String | Short team name. |
| `acronym_name` | String | 3-letter team code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_competitions-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_competitions-example}

```python
nwsl_competitions()
```

## nwsl_match_lineups

Team lineups (starting XI + bench + staff) for a match.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/matches/{match_id}/lineups`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matches/nwsl::Football_Match::0b6761e4701749f593690c0f338da74c/lineups?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matches/nwsl::Football_Match::0b6761e4701749f593690c0f338da74c/lineups?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns {#nwsl_match_lineups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | String | Composite Match id (Utf8 join key). |
| `side` | String |  |
| `team_id` | String | Composite Team id (Utf8 join key). |
| `selection` | String |  |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `player_id` | String | Composite Player id (Utf8 join key). |
| `bib_number` | String | Shirt/bib number. |
| `role_label` | String | Human position label (e.g. `Forward`). |
| `role` | Int64 | Numeric position/role code. |
| `media_first_name` | String | Media-style first name. |
| `media_last_name` | String | Media-style last name. |
| `shirt_name` | String | Name printed on the shirt. |
| `short_name` | String | Short team name. |
| `display_name` | String |  |
| `nationality` | String | Nationality name. |
| `nationality_iso_code` | String | ISO country code. |
| `is_captain` | Boolean | Whether the player wears the captain's armband. |
| `is_goalkeeper` | Boolean | Whether the player is the goalkeeper. |
| `events` | String | In-match events attributed to the player (goals/cards/subs). |
| `tactical_x_position` | String | Formation-grid X (0-1 normalized canvas). |
| `tactical_y_position` | String | Formation-grid Y (0-1 normalized canvas). |
| `average_x_position` | String | Average pitch x-coordinate of the player over the match. |
| `average_y_position` | String | Average pitch y-coordinate of the player over the match. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_match_lineups-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_match_lineups-example}

```python
nwsl_match_lineups(match_id='nwsl::Football_Match::0b6761e4701749f593690c0f338da74c', season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

## nwsl_matchdays

Match days (rounds) for a season.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/matchdays`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matchdays?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matchdays?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns {#nwsl_matchdays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_set_id` | String | Composite match-day (match set) id. |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `name` | String | Stage display name. |
| `season_id` | String | Composite Season id (Utf8 join key). |
| `competition_id` | String | Composite Competition id (Utf8 join key). |
| `round_id` | String | Composite id of the round. |
| `stage_id` | String | Composite Stage id (`nwsl::Football_Stage::{hex}`). |
| `index` | String | Ordinal position of the match day within the season. |
| `short_name` | String | Short team name. |
| `match_set_format_id` | String | Identifier of the match-day format. |
| `type` | String | Type discriminator for the record. |
| `start_date_utc` | String | Season window start (ISO-8601 UTC). |
| `end_date_utc` | String | Season window end (ISO-8601 UTC). |
| `matchday_status` | String | Status of the match day (scheduled, in progress, completed). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_matchdays-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_matchdays-example}

```python
nwsl_matchdays(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

## nwsl_player_stats

Player-stats leaderboard for a season (paginated).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/stats/players`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/players?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/players?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `category` | `category` |  |  | `Y` | Stat family: `general` (default), `attack`, `defence`, etc. |
| `role` | `role` |  |  | `Y` | Position filter, e.g. `all`. |
| `direction` | `direction` |  |  | `Y` | `asc` or `desc`. |
| `page` | `page` |  |  | `Y` | 1-based page number. |
| `pageNumElement` | `page_num_element` |  |  | `Y` | Page size (e.g. 400). |

### Returns {#nwsl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank_label` | String | Leaderboard rank label (null unless ranked view). |
| `player_id` | String | Composite Player id (Utf8 join key). |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `bib_number` | String | Shirt/bib number. |
| `role_label` | String | Human position label (e.g. `Forward`). |
| `role` | Int64 | Numeric position/role code. |
| `media_first_name` | String | Media-style first name. |
| `media_last_name` | String | Media-style last name. |
| `shirt_name` | String | Name printed on the shirt. |
| `short_name` | String | Short team name. |
| `display_name` | String |  |
| `nationality` | String | Nationality name. |
| `nationality_iso_code` | String | ISO country code. |
| `api_call_request_time` | String | Server timestamp the payload was assembled (ISO-8601). |
| `stats_id` | String | Stable stat key (e.g. `goals`, `points`, `Xg`). |
| `stats_label` | String | Human stat name. |
| `stats_label_abbreviation` | String | Short label (e.g. `PTS`, `GD`). |
| `stats_value` | String | Stat value - integer, string, or array (e.g. `form`). |
| `stats_unit` | String | Unit name (usually null). |
| `stats_unit_abbreviation` | String | Unit abbreviation (usually null). |
| `team_team_id` | String | Club: Composite Team id (Utf8 join key). |
| `team_provider_id` | String | Club: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `team_short_name` | String | Club: short team name. |
| `team_official_name` | String | Club: official team name. |
| `team_acronym_name` | String | Club: 3-letter team code. |
| `team_acronym_name_localized` | String | Club: localized 3-letter code. |
| `team_is_team_fake` | Boolean | Club: true for placeholder/TBD teams. |
| `team_media_name` | String | Club: media-style display name. |
| `team_media_short_name` | String | Club: short media-style display name. |
| `team_country_code` | String | Club: ISO country code. |
| `team_team_type` | String | Club: team type (e.g. `club`). |
| `team_overall_summary` | String | Club: Season summary blurb. |
| `team_stadium` | String | Club: home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `team_all_season_imagery` | String | Club: per-season crest variants. |
| `team_editorial_social_facebook` | String | Club editorial: facebook handle or URL. |
| `team_editorial_social_instagram` | String | Club editorial: instagram handle or URL. |
| `team_editorial_social_x` | String | Club editorial: x (Twitter) handle or URL. |
| `team_editorial_social_tik_tok` | String | Club editorial: tikTok handle or URL. |
| `team_editorial_social_you_tube` | String | Club editorial: youTube handle or URL. |
| `team_editorial_social_linked_in` | String | Club editorial: linkedIn handle or URL. |
| `team_editorial_website_url` | String | Club editorial: official website URL. |
| `team_editorial_shop_url` | String | Club editorial: club shop URL. |
| `team_editorial_tickets_url` | String | Club editorial: ticketing URL. |
| `team_editorial_club_primary_colour` | String | Club editorial: club primary colour (hex). |
| `team_editorial_club_secondary_colour` | String | Club editorial: club secondary colour (hex). |
| `team_editorial_club_text_colour` | String | Club editorial: club text colour (hex). |
| `editorial_player_role_within_team` | String | Editorial metadata: editorial description of the player's role in the side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_player_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_player_stats-example}

```python
nwsl_player_stats(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

## nwsl_season_matches

Matches across one or more seasons within a US-format date window.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/multipleSeasonMatches`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/multipleSeasonMatches?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/multipleSeasonMatches?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `seasonIds` | `season_ids` |  |  | `Y` | Comma-separated composite Season ids. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `startDate` | `start_date` |  |  | `Y` | Window start, `MM/DD/YYYY` (US format). |
| `endDate` | `end_date` |  |  | `Y` | Window end, `MM/DD/YYYY` (US format). |

### Returns {#nwsl_season_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `season_id` | String | Composite Season id (Utf8 join key). |
| `match_id` | String | Composite Match id (Utf8 join key). |
| `status` | String | Normalized match status (e.g. `PostMatch`, `PreMatch`, `Live`). |
| `provider_status` | String | Raw provider status string. |
| `phase` | String | Season phase (e.g. `RegularSeason`, `Final Series`). |
| `match_date_utc` | String | Kickoff time in UTC (ISO-8601). |
| `match_date_local` | String | Kickoff time in venue-local time. |
| `local_time_utc_offset` | String | Venue UTC offset. |
| `is_unknown_kick_off_time` | Boolean | True if kickoff time is TBD. |
| `home_score_push` | Int64 | Home club: live score pushed by the feed for this side. |
| `away_score_push` | Int64 | Away club: live score pushed by the feed for this side. |
| `provider_penalty_score_home` | String | Provider-reported: home side's penalty-shootout score. |
| `provider_penalty_score_away` | String | Provider-reported: away side's penalty-shootout score. |
| `aggregate` | String | Two-leg aggregate label. |
| `win_reason` | String | How the result was decided. |
| `win_team_id` | String | Composite Team id of the winner (null if draw/unplayed). |
| `previous_legs_result` | String | Aggregate result of previous legs in a two-legged tie. |
| `stadium_id` | String | Composite Stadium id for the venue. |
| `stadium_name` | String | Stadium: stage display name. |
| `city_name` | String | City the stadium is in. |
| `group` | String |  |
| `group_name` | String |  |
| `round_id` | String | Composite id of the round. |
| `round_name` | String | Name of the round or series this match belongs to. |
| `schedule_status` | String | Scheduling status. |
| `provider_home_score` | Int64 | Provider-reported: home side's score. |
| `provider_away_score` | Int64 | Provider-reported: away side's score. |
| `group_id` | String |  |
| `sub_league` | String | Sub-league label. |
| `time` | String |  |
| `additional_time` | String | Stoppage time added, in minutes. |
| `previous_leg_id` | String | Composite Match id of the previous leg (two-legged ties). |
| `editorial_broadcasters_broadcaster_national1` | String | Broadcast listing: first national broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_national2` | String | Broadcast listing: second national broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_national3` | String | Broadcast listing: third national broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_international1` | String | Broadcast listing: first international broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_international2` | String | Broadcast listing: second international broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_international3` | String | Broadcast listing: third international broadcaster carrying the match. |
| `editorial_highlights_url` | String | Editorial metadata: Match highlights URL. |
| `editorial_highlights_national_url` | String | Editorial metadata: nationally-geofenced match highlights URL. |
| `editorial_highlights_international_url` | String | Editorial metadata: internationally-geofenced match highlights URL. |
| `editorial_tickets_url` | String | Editorial metadata: ticketing URL. |
| `editorial_sponsor_image` | String | Editorial metadata: sponsor image asset, JSON-encoded. |
| `editorial_theme_night` | String | Editorial metadata: theme-night promotion attached to the match. |
| `editorial_editorials` | String | Editorial metadata: editorial blurbs attached to the record, JSON-encoded. |
| `home_team_id` | String | Home club: Composite Team id (Utf8 join key). |
| `home_provider_id` | String | Home club: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `home_short_name` | String | Home club: short team name. |
| `home_official_name` | String | Home club: official team name. |
| `home_acronym_name` | String | Home club: 3-letter team code. |
| `home_acronym_name_localized` | String | Home club: localized 3-letter code. |
| `home_is_team_fake` | Boolean | Home club: true for placeholder/TBD teams. |
| `home_media_name` | String | Home club: media-style display name. |
| `home_media_short_name` | String | Home club: short media-style display name. |
| `home_country_code` | String | Home club: ISO country code. |
| `home_team_type` | String | Home club: team type (e.g. `club`). |
| `home_overall_summary` | String | Home club: Season summary blurb. |
| `home_stadium` | String | Home club: home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `home_all_season_imagery` | String | Home club: per-season crest variants. |
| `home_editorial_social_facebook` | String | Home club editorial: facebook handle or URL. |
| `home_editorial_social_instagram` | String | Home club editorial: instagram handle or URL. |
| `home_editorial_social_x` | String | Home club editorial: x (Twitter) handle or URL. |
| `home_editorial_social_tik_tok` | String | Home club editorial: tikTok handle or URL. |
| `home_editorial_social_you_tube` | String | Home club editorial: youTube handle or URL. |
| `home_editorial_social_linked_in` | String | Home club editorial: linkedIn handle or URL. |
| `home_editorial_website_url` | String | Home club editorial: official website URL. |
| `home_editorial_shop_url` | String | Home club editorial: club shop URL. |
| `home_editorial_tickets_url` | String | Home club editorial: ticketing URL. |
| `home_editorial_club_primary_colour` | String | Home club editorial: club primary colour (hex). |
| `home_editorial_club_secondary_colour` | String | Home club editorial: club secondary colour (hex). |
| `home_editorial_club_text_colour` | String | Home club editorial: club text colour (hex). |
| `away_team_id` | String | Away club: Composite Team id (Utf8 join key). |
| `away_provider_id` | String | Away club: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `away_short_name` | String | Away club: short team name. |
| `away_official_name` | String | Away club: official team name. |
| `away_acronym_name` | String | Away club: 3-letter team code. |
| `away_acronym_name_localized` | String | Away club: localized 3-letter code. |
| `away_is_team_fake` | Boolean | Away club: true for placeholder/TBD teams. |
| `away_media_name` | String | Away club: media-style display name. |
| `away_media_short_name` | String | Away club: short media-style display name. |
| `away_country_code` | String | Away club: ISO country code. |
| `away_team_type` | String | Away club: team type (e.g. `club`). |
| `away_overall_summary` | String | Away club: Season summary blurb. |
| `away_stadium` | String | Away club: home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `away_all_season_imagery` | String | Away club: per-season crest variants. |
| `away_editorial_social_facebook` | String | Away club editorial: facebook handle or URL. |
| `away_editorial_social_instagram` | String | Away club editorial: instagram handle or URL. |
| `away_editorial_social_x` | String | Away club editorial: x (Twitter) handle or URL. |
| `away_editorial_social_tik_tok` | String | Away club editorial: tikTok handle or URL. |
| `away_editorial_social_you_tube` | String | Away club editorial: youTube handle or URL. |
| `away_editorial_social_linked_in` | String | Away club editorial: linkedIn handle or URL. |
| `away_editorial_website_url` | String | Away club editorial: official website URL. |
| `away_editorial_shop_url` | String | Away club editorial: club shop URL. |
| `away_editorial_tickets_url` | String | Away club editorial: ticketing URL. |
| `away_editorial_club_primary_colour` | String | Away club editorial: club primary colour (hex). |
| `away_editorial_club_secondary_colour` | String | Away club editorial: club secondary colour (hex). |
| `away_editorial_club_text_colour` | String | Away club editorial: club text colour (hex). |
| `match_set_match_set_id` | String | Match day (round): Composite match-day (match set) id. |
| `match_set_provider_id` | String | Match day (round): underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `match_set_name` | String | Match day (round): stage display name. |
| `match_set_season_id` | String | Match day (round): Composite Season id (Utf8 join key). |
| `match_set_competition_id` | String | Match day (round): Composite Competition id (Utf8 join key). |
| `match_set_round_id` | String | Match day (round): Composite id of the round. |
| `match_set_stage_id` | String | Match day (round): Composite Stage id (`nwsl::Football_Stage::{hex}`). |
| `match_set_index` | String | Match day (round): ordinal position of the match day within the season. |
| `match_set_short_name` | String | Match day (round): short team name. |
| `match_set_match_set_format_id` | String | Match day (round): identifier of the match-day format. |
| `match_set_type` | String | Match day (round): type discriminator for the record. |
| `match_set_start_date_utc` | String | Match day (round): Season window start (ISO-8601 UTC). |
| `match_set_end_date_utc` | String | Match day (round): Season window end (ISO-8601 UTC). |
| `match_set_matchday_status` | String | Match day (round): status of the match day (scheduled, in progress, completed). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_season_matches-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_season_matches-example}

```python
nwsl_season_matches()
```

## nwsl_stages

Competition stages for a season (may be empty for league play).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/stages`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stages?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stages?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns {#nwsl_stages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `stage_id` | String | Composite Stage id (`nwsl::Football_Stage::{hex}`). |
| `name` | String | Stage display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_stages-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_stages-example}

```python
nwsl_stages(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

## nwsl_standings

Overall standings table for a season (table/home/away splits).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/standings/overall`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/standings/overall?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/standings/overall?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `orderBy` | `order_by` |  |  | `Y` | Sort field, e.g. `rank`. |
| `direction` | `direction` |  |  | `Y` | `asc` or `desc`. |

### Returns {#nwsl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `split_type` | String |  |
| `achievement_statuses` | String | Achievement flags (champion, clinched, etc.). |
| `note` | String | Free-text standings note (null when none). |
| `team_id` | String | Composite Team id (Utf8 join key). |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `short_name` | String | Short team name. |
| `official_name` | String | Official team name. |
| `acronym_name` | String | 3-letter team code. |
| `acronym_name_localized` | String | Localized 3-letter code. |
| `is_team_fake` | Boolean | True for placeholder/TBD teams. |
| `media_name` | String | Media-style display name. |
| `media_short_name` | String | Short media-style display name. |
| `country_code` | String | ISO country code. |
| `team_type` | String | Team type (e.g. `club`). |
| `overall_summary` | String | Season summary blurb. |
| `stadium` | String | Home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `all_season_imagery` | String | Per-season crest variants. |
| `rank` | Int64 | Position within the group/table. |
| `team` | String | Team identity block the player belongs to. |
| `points` | Int64 | Competition points. |
| `qualification_qualification_id` | String | Qualification band: identifier of the qualification band. |
| `qualification_qualification_label` | String | Qualification band: display label of the qualification band. |
| `qualification` | String | Qualification band (e.g. playoff/Final Series). |
| `matches_played` | Int64 | Matches played in this split. |
| `win` | Int64 | Matches won. |
| `draw` | Int64 | Matches drawn. |
| `lose` | Int64 | Matches lost. |
| `goals_for` | Int64 | Goals scored. |
| `goals_against` | Int64 | Goals conceded. |
| `goal_difference` | Int64 | Goals scored minus goals conceded. |
| `movement` | String | Position movement versus the previous matchday. |
| `form` | String | Recent results sequence, JSON-encoded (the API sends an array). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_standings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_standings-example}

```python
nwsl_standings(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

## nwsl_team_stats

Team-stats leaderboard for a season.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/stats/teams`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/teams?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/teams?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `category` | `category` |  |  | `Y` | Stat family: `general` (default), `attack`, `defence`, etc. |

### Returns {#nwsl_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank_label` | String | Leaderboard rank label (null unless ranked view). |
| `team_id` | String | Composite Team id (Utf8 join key). |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `short_name` | String | Short team name. |
| `official_name` | String | Official team name. |
| `acronym_name` | String | 3-letter team code. |
| `acronym_name_localized` | String | Localized 3-letter code. |
| `is_team_fake` | Boolean | True for placeholder/TBD teams. |
| `media_name` | String | Media-style display name. |
| `media_short_name` | String | Short media-style display name. |
| `country_code` | String | ISO country code. |
| `team_type` | String | Team type (e.g. `club`). |
| `overall_summary` | String | Season summary blurb. |
| `stadium` | String | Home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `all_season_imagery` | String | Per-season crest variants. |
| `stats_id` | String | Stable stat key (e.g. `goals`, `points`, `Xg`). |
| `stats_label` | String | Human stat name. |
| `stats_label_abbreviation` | String | Short label (e.g. `PTS`, `GD`). |
| `stats_value` | String | Stat value - integer, string, or array (e.g. `form`). |
| `stats_unit` | String | Unit name (usually null). |
| `stats_unit_abbreviation` | String | Unit abbreviation (usually null). |
| `editorial_social_facebook` | String | Editorial metadata: facebook handle or URL. |
| `editorial_social_instagram` | String | Editorial metadata: instagram handle or URL. |
| `editorial_social_x` | String | Editorial metadata: x (Twitter) handle or URL. |
| `editorial_social_tik_tok` | String | Editorial metadata: tikTok handle or URL. |
| `editorial_social_you_tube` | String | Editorial metadata: youTube handle or URL. |
| `editorial_social_linked_in` | String | Editorial metadata: linkedIn handle or URL. |
| `editorial_website_url` | String | Editorial metadata: official website URL. |
| `editorial_shop_url` | String | Editorial metadata: club shop URL. |
| `editorial_tickets_url` | String | Editorial metadata: ticketing URL. |
| `editorial_club_primary_colour` | String | Editorial metadata: club primary colour (hex). |
| `editorial_club_secondary_colour` | String | Editorial metadata: club secondary colour (hex). |
| `editorial_club_text_colour` | String | Editorial metadata: club text colour (hex). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_team_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_team_stats-example}

```python
nwsl_team_stats(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

## nwsl_teams

Teams participating in a season.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/teams`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/teams?locale=en-US](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/teams?locale=en-US)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns {#nwsl_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | Composite Team id (Utf8 join key). |
| `provider_id` | String | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `short_name` | String | Short team name. |
| `official_name` | String | Official team name. |
| `acronym_name` | String | 3-letter team code. |
| `acronym_name_localized` | String | Localized 3-letter code. |
| `is_team_fake` | Boolean | True for placeholder/TBD teams. |
| `media_name` | String | Media-style display name. |
| `media_short_name` | String | Short media-style display name. |
| `country_code` | String | ISO country code. |
| `team_type` | String | Team type (e.g. `club`). |
| `overall_summary` | String | Season summary blurb. |
| `all_season_imagery` | String | Per-season crest variants. |
| `stadium_id` | String | Composite Stadium id for the venue. |
| `stadium_provider_id` | String | Stadium: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `stadium_name` | String | Stadium: stage display name. |
| `stadium_city_name` | String | Stadium: city the stadium is in. |
| `stadium_country` | String | Stadium: country the stadium is in. |
| `stadium_address` | String | Stadium: street address of the stadium. |
| `stadium_capacity` | Int64 | Stadium: seating capacity of the stadium. |
| `stadium_year_of_construction` | Int64 | Stadium: year the stadium was built. |
| `stadium_maps_geo_code_latitude` | String | Stadium: latitude in decimal degrees. |
| `stadium_maps_geo_code_longitude` | String | Stadium: longitude in decimal degrees. |
| `editorial_social_facebook` | String | Editorial metadata: facebook handle or URL. |
| `editorial_social_instagram` | String | Editorial metadata: instagram handle or URL. |
| `editorial_social_x` | String | Editorial metadata: x (Twitter) handle or URL. |
| `editorial_social_tik_tok` | String | Editorial metadata: tikTok handle or URL. |
| `editorial_social_you_tube` | String | Editorial metadata: youTube handle or URL. |
| `editorial_social_linked_in` | String | Editorial metadata: linkedIn handle or URL. |
| `editorial_website_url` | String | Editorial metadata: official website URL. |
| `editorial_shop_url` | String | Editorial metadata: club shop URL. |
| `editorial_tickets_url` | String | Editorial metadata: ticketing URL. |
| `editorial_club_primary_colour` | String | Editorial metadata: club primary colour (hex). |
| `editorial_club_secondary_colour` | String | Editorial metadata: club secondary colour (hex). |
| `editorial_club_text_colour` | String | Editorial metadata: club text colour (hex). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nwsl_teams-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The SDP host returned 404 (unknown composite id, or an unmapped match sub-resource). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nwsl_teams-example}

```python
nwsl_teams(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```
