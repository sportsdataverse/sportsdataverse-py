---
title: NWSL — NWSL official web API (StatsPerform SDP)
sidebar_label: NWSL official web API (StatsPerform SDP)
description: "NWSL — NWSL official web API (StatsPerform SDP) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# NWSL — NWSL official web API (StatsPerform SDP)

`sportsdataverse.nwsl` — 9 endpoints.

## `nwsl_competitions`

All competitions StatsPerform tracks for NWSL (league + friendlies/cups).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions](https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `competition_id` | character | Composite Competition id (Utf8 join key). |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `name` | character | Stage display name. |
| `official_name` | character | Official team name. |
| `short_name` | character | Short team name. |
| `acronym_name` | character | 3-letter team code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_competitions()
```

_Last validated n/a._

## `nwsl_match_lineups`

Team lineups (starting XI + bench + staff) for a match.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/matches/{match_id}/lineups`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matches/nwsl::Football_Match::0b6761e4701749f593690c0f338da74c/lineups](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matches/nwsl::Football_Match::0b6761e4701749f593690c0f338da74c/lineups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `match_id` | character | Composite Match id (Utf8 join key). |
| `side` | character | Side label (e.g. 'home', 'away', or 'overUnder'). |
| `team_id` | character | Composite Team id (Utf8 join key). |
| `selection` | character | Selection. |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `player_id` | character | Composite Player id (Utf8 join key). |
| `bib_number` | character | Shirt/bib number. |
| `role_label` | character | Human position label (e.g. `Forward`). |
| `role` | integer | Numeric position/role code. |
| `media_first_name` | character | Media-style first name. |
| `media_last_name` | character | Media-style last name. |
| `shirt_name` | character | Name printed on the shirt. |
| `short_name` | character | Short team name. |
| `display_name` | character | Display name. |
| `nationality` | character | Nationality name. |
| `nationality_iso_code` | character | ISO country code. |
| `is_captain` | logical | Whether the player wears the captain's armband. |
| `is_goalkeeper` | logical | Whether the player is the goalkeeper. |
| `events` | character | In-match events attributed to the player (goals/cards/subs). |
| `tactical_x_position` | character | Formation-grid X (0-1 normalized canvas). |
| `tactical_y_position` | character | Formation-grid Y (0-1 normalized canvas). |
| `average_x_position` | character | Average pitch x-coordinate of the player over the match. |
| `average_y_position` | character | Average pitch y-coordinate of the player over the match. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_match_lineups(match_id='nwsl::Football_Match::0b6761e4701749f593690c0f338da74c', season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._

## `nwsl_matchdays`

Match days (rounds) for a season.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/matchdays`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matchdays](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/matchdays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `match_set_id` | character | Composite match-day (match set) id. |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `name` | character | Stage display name. |
| `season_id` | character | Composite Season id (Utf8 join key). |
| `competition_id` | character | Composite Competition id (Utf8 join key). |
| `round_id` | character | Composite id of the round. |
| `stage_id` | character | Composite Stage id (`nwsl::Football_Stage::{hex}`). |
| `index` | character | Ordinal position of the match day within the season. |
| `short_name` | character | Short team name. |
| `match_set_format_id` | character | Identifier of the match-day format. |
| `type` | character | Type discriminator for the record. |
| `start_date_utc` | character | Season window start (ISO-8601 UTC). |
| `end_date_utc` | character | Season window end (ISO-8601 UTC). |
| `matchday_status` | character | Status of the match day (scheduled, in progress, completed). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_matchdays(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._

## `nwsl_player_stats`

Player-stats leaderboard for a season (paginated).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/stats/players`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/players](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `category` | `category` |  |  | `Y` | Stat family: `general` (default), `attack`, `defence`, etc. |
| `role` | `role` |  |  | `Y` | Position filter, e.g. `all`. |
| `direction` | `direction` |  |  | `Y` | `asc` or `desc`. |
| `page` | `page` |  |  | `Y` | 1-based page number. |
| `pageNumElement` | `page_num_element` |  |  | `Y` | Page size (e.g. 400). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank_label` | character | Leaderboard rank label (null unless ranked view). |
| `player_id` | character | Composite Player id (Utf8 join key). |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `bib_number` | character | Shirt/bib number. |
| `role_label` | character | Human position label (e.g. `Forward`). |
| `role` | integer | Numeric position/role code. |
| `media_first_name` | character | Media-style first name. |
| `media_last_name` | character | Media-style last name. |
| `shirt_name` | character | Name printed on the shirt. |
| `short_name` | character | Short team name. |
| `display_name` | character | Display name. |
| `nationality` | character | Nationality name. |
| `nationality_iso_code` | character | ISO country code. |
| `api_call_request_time` | character | Server timestamp the payload was assembled (ISO-8601). |
| `stats_id` | character | Stable stat key (e.g. `goals`, `points`, `Xg`). |
| `stats_label` | character | Human stat name. |
| `stats_label_abbreviation` | character | Short label (e.g. `PTS`, `GD`). |
| `stats_value` | character | Stat value - integer, string, or array (e.g. `form`). |
| `stats_unit` | character | Unit name (usually null). |
| `stats_unit_abbreviation` | character | Unit abbreviation (usually null). |
| `team_team_id` | character | Club: Composite Team id (Utf8 join key). |
| `team_provider_id` | character | Club: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `team_short_name` | character | Club: short team name. |
| `team_official_name` | character | Club: official team name. |
| `team_acronym_name` | character | Club: 3-letter team code. |
| `team_acronym_name_localized` | character | Club: localized 3-letter code. |
| `team_is_team_fake` | logical | Club: true for placeholder/TBD teams. |
| `team_media_name` | character | Club: media-style display name. |
| `team_media_short_name` | character | Club: short media-style display name. |
| `team_country_code` | character | Club: ISO country code. |
| `team_team_type` | character | Club: team type (e.g. `club`). |
| `team_overall_summary` | character | Club: Season summary blurb. |
| `team_stadium` | character | Club: home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `team_all_season_imagery` | character | Club: per-season crest variants. |
| `team_editorial_social_facebook` | character | Club editorial: facebook handle or URL. |
| `team_editorial_social_instagram` | character | Club editorial: instagram handle or URL. |
| `team_editorial_social_x` | character | Club editorial: x (Twitter) handle or URL. |
| `team_editorial_social_tik_tok` | character | Club editorial: tikTok handle or URL. |
| `team_editorial_social_you_tube` | character | Club editorial: youTube handle or URL. |
| `team_editorial_social_linked_in` | character | Club editorial: linkedIn handle or URL. |
| `team_editorial_website_url` | character | Club editorial: official website URL. |
| `team_editorial_shop_url` | character | Club editorial: club shop URL. |
| `team_editorial_tickets_url` | character | Club editorial: ticketing URL. |
| `team_editorial_club_primary_colour` | character | Club editorial: club primary colour (hex). |
| `team_editorial_club_secondary_colour` | character | Club editorial: club secondary colour (hex). |
| `team_editorial_club_text_colour` | character | Club editorial: club text colour (hex). |
| `editorial_player_role_within_team` | character | Editorial metadata: editorial description of the player's role in the side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_player_stats(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._

## `nwsl_season_matches`

Matches across one or more seasons within a US-format date window.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/multipleSeasonMatches`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/multipleSeasonMatches](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/multipleSeasonMatches)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `seasonIds` | `season_ids` |  |  | `Y` | Comma-separated composite Season ids. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `startDate` | `start_date` |  |  | `Y` | Window start, `MM/DD/YYYY` (US format). |
| `endDate` | `end_date` |  |  | `Y` | Window end, `MM/DD/YYYY` (US format). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `season_id` | character | Composite Season id (Utf8 join key). |
| `match_id` | character | Composite Match id (Utf8 join key). |
| `status` | character | Normalized match status (e.g. `PostMatch`, `PreMatch`, `Live`). |
| `provider_status` | character | Raw provider status string. |
| `phase` | character | Season phase (e.g. `RegularSeason`, `Final Series`). |
| `match_date_utc` | character | Kickoff time in UTC (ISO-8601). |
| `match_date_local` | character | Kickoff time in venue-local time. |
| `local_time_utc_offset` | character | Venue UTC offset. |
| `is_unknown_kick_off_time` | logical | True if kickoff time is TBD. |
| `home_score_push` | integer | Home club: live score pushed by the feed for this side. |
| `away_score_push` | integer | Away club: live score pushed by the feed for this side. |
| `provider_penalty_score_home` | character | Provider-reported: home side's penalty-shootout score. |
| `provider_penalty_score_away` | character | Provider-reported: away side's penalty-shootout score. |
| `aggregate` | character | Two-leg aggregate label. |
| `win_reason` | character | How the result was decided. |
| `win_team_id` | character | Composite Team id of the winner (null if draw/unplayed). |
| `previous_legs_result` | character | Aggregate result of previous legs in a two-legged tie. |
| `stadium_id` | character | Composite Stadium id for the venue. |
| `stadium_name` | character | Stadium: stage display name. |
| `city_name` | character | City the stadium is in. |
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `group_name` | character | Group name (conference / division). |
| `round_id` | character | Composite id of the round. |
| `round_name` | character | Name of the round or series this match belongs to. |
| `schedule_status` | character | Scheduling status. |
| `provider_home_score` | integer | Provider-reported: home side's score. |
| `provider_away_score` | integer | Provider-reported: away side's score. |
| `group_id` | character | ESPN group id. |
| `sub_league` | character | Sub-league label. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `additional_time` | character | Stoppage time added, in minutes. |
| `previous_leg_id` | character | Composite Match id of the previous leg (two-legged ties). |
| `editorial_broadcasters_broadcaster_national1` | character | Broadcast listing: first national broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_national2` | character | Broadcast listing: second national broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_national3` | character | Broadcast listing: third national broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_international1` | character | Broadcast listing: first international broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_international2` | character | Broadcast listing: second international broadcaster carrying the match. |
| `editorial_broadcasters_broadcaster_international3` | character | Broadcast listing: third international broadcaster carrying the match. |
| `editorial_highlights_url` | character | Editorial metadata: Match highlights URL. |
| `editorial_highlights_national_url` | character | Editorial metadata: nationally-geofenced match highlights URL. |
| `editorial_highlights_international_url` | character | Editorial metadata: internationally-geofenced match highlights URL. |
| `editorial_tickets_url` | character | Editorial metadata: ticketing URL. |
| `editorial_sponsor_image` | character | Editorial metadata: sponsor image asset, JSON-encoded. |
| `editorial_theme_night` | character | Editorial metadata: theme-night promotion attached to the match. |
| `editorial_editorials` | character | Editorial metadata: editorial blurbs attached to the record, JSON-encoded. |
| `home_team_id` | character | Home club: Composite Team id (Utf8 join key). |
| `home_provider_id` | character | Home club: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `home_short_name` | character | Home club: short team name. |
| `home_official_name` | character | Home club: official team name. |
| `home_acronym_name` | character | Home club: 3-letter team code. |
| `home_acronym_name_localized` | character | Home club: localized 3-letter code. |
| `home_is_team_fake` | logical | Home club: true for placeholder/TBD teams. |
| `home_media_name` | character | Home club: media-style display name. |
| `home_media_short_name` | character | Home club: short media-style display name. |
| `home_country_code` | character | Home club: ISO country code. |
| `home_team_type` | character | Home club: team type (e.g. `club`). |
| `home_overall_summary` | character | Home club: Season summary blurb. |
| `home_stadium` | character | Home club: home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `home_all_season_imagery` | character | Home club: per-season crest variants. |
| `home_editorial_social_facebook` | character | Home club editorial: facebook handle or URL. |
| `home_editorial_social_instagram` | character | Home club editorial: instagram handle or URL. |
| `home_editorial_social_x` | character | Home club editorial: x (Twitter) handle or URL. |
| `home_editorial_social_tik_tok` | character | Home club editorial: tikTok handle or URL. |
| `home_editorial_social_you_tube` | character | Home club editorial: youTube handle or URL. |
| `home_editorial_social_linked_in` | character | Home club editorial: linkedIn handle or URL. |
| `home_editorial_website_url` | character | Home club editorial: official website URL. |
| `home_editorial_shop_url` | character | Home club editorial: club shop URL. |
| `home_editorial_tickets_url` | character | Home club editorial: ticketing URL. |
| `home_editorial_club_primary_colour` | character | Home club editorial: club primary colour (hex). |
| `home_editorial_club_secondary_colour` | character | Home club editorial: club secondary colour (hex). |
| `home_editorial_club_text_colour` | character | Home club editorial: club text colour (hex). |
| `away_team_id` | character | Away club: Composite Team id (Utf8 join key). |
| `away_provider_id` | character | Away club: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `away_short_name` | character | Away club: short team name. |
| `away_official_name` | character | Away club: official team name. |
| `away_acronym_name` | character | Away club: 3-letter team code. |
| `away_acronym_name_localized` | character | Away club: localized 3-letter code. |
| `away_is_team_fake` | logical | Away club: true for placeholder/TBD teams. |
| `away_media_name` | character | Away club: media-style display name. |
| `away_media_short_name` | character | Away club: short media-style display name. |
| `away_country_code` | character | Away club: ISO country code. |
| `away_team_type` | character | Away club: team type (e.g. `club`). |
| `away_overall_summary` | character | Away club: Season summary blurb. |
| `away_stadium` | character | Away club: home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `away_all_season_imagery` | character | Away club: per-season crest variants. |
| `away_editorial_social_facebook` | character | Away club editorial: facebook handle or URL. |
| `away_editorial_social_instagram` | character | Away club editorial: instagram handle or URL. |
| `away_editorial_social_x` | character | Away club editorial: x (Twitter) handle or URL. |
| `away_editorial_social_tik_tok` | character | Away club editorial: tikTok handle or URL. |
| `away_editorial_social_you_tube` | character | Away club editorial: youTube handle or URL. |
| `away_editorial_social_linked_in` | character | Away club editorial: linkedIn handle or URL. |
| `away_editorial_website_url` | character | Away club editorial: official website URL. |
| `away_editorial_shop_url` | character | Away club editorial: club shop URL. |
| `away_editorial_tickets_url` | character | Away club editorial: ticketing URL. |
| `away_editorial_club_primary_colour` | character | Away club editorial: club primary colour (hex). |
| `away_editorial_club_secondary_colour` | character | Away club editorial: club secondary colour (hex). |
| `away_editorial_club_text_colour` | character | Away club editorial: club text colour (hex). |
| `match_set_match_set_id` | character | Match day (round): Composite match-day (match set) id. |
| `match_set_provider_id` | character | Match day (round): underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `match_set_name` | character | Match day (round): stage display name. |
| `match_set_season_id` | character | Match day (round): Composite Season id (Utf8 join key). |
| `match_set_competition_id` | character | Match day (round): Composite Competition id (Utf8 join key). |
| `match_set_round_id` | character | Match day (round): Composite id of the round. |
| `match_set_stage_id` | character | Match day (round): Composite Stage id (`nwsl::Football_Stage::{hex}`). |
| `match_set_index` | character | Match day (round): ordinal position of the match day within the season. |
| `match_set_short_name` | character | Match day (round): short team name. |
| `match_set_match_set_format_id` | character | Match day (round): identifier of the match-day format. |
| `match_set_type` | character | Match day (round): type discriminator for the record. |
| `match_set_start_date_utc` | character | Match day (round): Season window start (ISO-8601 UTC). |
| `match_set_end_date_utc` | character | Match day (round): Season window end (ISO-8601 UTC). |
| `match_set_matchday_status` | character | Match day (round): status of the match day (scheduled, in progress, completed). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_season_matches()
```

_Last validated n/a._

## `nwsl_stages`

Competition stages for a season (may be empty for league play).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/stages`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stages](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `stage_id` | character | Composite Stage id (`nwsl::Football_Stage::{hex}`). |
| `name` | character | Stage display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_stages(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._

## `nwsl_standings`

Overall standings table for a season (table/home/away splits).

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/standings/overall`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/standings/overall](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/standings/overall)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `orderBy` | `order_by` |  |  | `Y` | Sort field, e.g. `rank`. |
| `direction` | `direction` |  |  | `Y` | `asc` or `desc`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `split_type` | character | Split type code. |
| `achievement_statuses` | character | Achievement flags (champion, clinched, etc.). |
| `note` | character | Free-text standings note (null when none). |
| `team_id` | character | Composite Team id (Utf8 join key). |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `short_name` | character | Short team name. |
| `official_name` | character | Official team name. |
| `acronym_name` | character | 3-letter team code. |
| `acronym_name_localized` | character | Localized 3-letter code. |
| `is_team_fake` | logical | True for placeholder/TBD teams. |
| `media_name` | character | Media-style display name. |
| `media_short_name` | character | Short media-style display name. |
| `country_code` | character | ISO country code. |
| `team_type` | character | Team type (e.g. `club`). |
| `overall_summary` | character | Season summary blurb. |
| `stadium` | character | Home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `all_season_imagery` | character | Per-season crest variants. |
| `rank` | integer | Position within the group/table. |
| `team` | character | Team identity block the player belongs to. |
| `points` | integer | Competition points. |
| `qualification_qualification_id` | character | Qualification band: identifier of the qualification band. |
| `qualification_qualification_label` | character | Qualification band: display label of the qualification band. |
| `qualification` | numeric | Qualification band (e.g. playoff/Final Series). |
| `matches_played` | integer | Matches played in this split. |
| `win` | integer | Matches won. |
| `draw` | integer | Matches drawn. |
| `lose` | integer | Matches lost. |
| `goals_for` | integer | Goals scored. |
| `goals_against` | integer | Goals conceded. |
| `goal_difference` | integer | Goals scored minus goals conceded. |
| `movement` | character | Position movement versus the previous matchday. |
| `form` | character | Recent results sequence, JSON-encoded (the API sends an array). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_standings(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._

## `nwsl_team_stats`

Team-stats leaderboard for a season.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/stats/teams`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/teams](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/stats/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |
| `category` | `category` |  |  | `Y` | Stat family: `general` (default), `attack`, `defence`, etc. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank_label` | character | Leaderboard rank label (null unless ranked view). |
| `team_id` | character | Composite Team id (Utf8 join key). |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `short_name` | character | Short team name. |
| `official_name` | character | Official team name. |
| `acronym_name` | character | 3-letter team code. |
| `acronym_name_localized` | character | Localized 3-letter code. |
| `is_team_fake` | logical | True for placeholder/TBD teams. |
| `media_name` | character | Media-style display name. |
| `media_short_name` | character | Short media-style display name. |
| `country_code` | character | ISO country code. |
| `team_type` | character | Team type (e.g. `club`). |
| `overall_summary` | character | Season summary blurb. |
| `stadium` | character | Home venue: `{id, providerId, name, cityName, country, address, capacity, yearOfConstruction, mapsGeoCodeLatitude, mapsGeoCodeLongitude, imagery}`. |
| `all_season_imagery` | character | Per-season crest variants. |
| `stats_id` | character | Stable stat key (e.g. `goals`, `points`, `Xg`). |
| `stats_label` | character | Human stat name. |
| `stats_label_abbreviation` | character | Short label (e.g. `PTS`, `GD`). |
| `stats_value` | character | Stat value - integer, string, or array (e.g. `form`). |
| `stats_unit` | character | Unit name (usually null). |
| `stats_unit_abbreviation` | character | Unit abbreviation (usually null). |
| `editorial_social_facebook` | character | Editorial metadata: facebook handle or URL. |
| `editorial_social_instagram` | character | Editorial metadata: instagram handle or URL. |
| `editorial_social_x` | character | Editorial metadata: x (Twitter) handle or URL. |
| `editorial_social_tik_tok` | character | Editorial metadata: tikTok handle or URL. |
| `editorial_social_you_tube` | character | Editorial metadata: youTube handle or URL. |
| `editorial_social_linked_in` | character | Editorial metadata: linkedIn handle or URL. |
| `editorial_website_url` | character | Editorial metadata: official website URL. |
| `editorial_shop_url` | character | Editorial metadata: club shop URL. |
| `editorial_tickets_url` | character | Editorial metadata: ticketing URL. |
| `editorial_club_primary_colour` | character | Editorial metadata: club primary colour (hex). |
| `editorial_club_secondary_colour` | character | Editorial metadata: club secondary colour (hex). |
| `editorial_club_text_colour` | character | Editorial metadata: club text colour (hex). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_team_stats(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._

## `nwsl_teams`

Teams participating in a season.

**Endpoint URL:** `GET https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{season_id}/teams`

**Valid URL:** [https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/teams](https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/nwsl::Football_Season::0b6761e4701749f593690c0f338da74c/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `locale` | `locale` |  |  | `Y` | UI locale, always `en-US`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Composite Team id (Utf8 join key). |
| `provider_id` | character | Underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `short_name` | character | Short team name. |
| `official_name` | character | Official team name. |
| `acronym_name` | character | 3-letter team code. |
| `acronym_name_localized` | character | Localized 3-letter code. |
| `is_team_fake` | logical | True for placeholder/TBD teams. |
| `media_name` | character | Media-style display name. |
| `media_short_name` | character | Short media-style display name. |
| `country_code` | character | ISO country code. |
| `team_type` | character | Team type (e.g. `club`). |
| `overall_summary` | character | Season summary blurb. |
| `all_season_imagery` | character | Per-season crest variants. |
| `stadium_id` | character | Composite Stadium id for the venue. |
| `stadium_provider_id` | character | Stadium: underlying StatsPerform/Opta provider id (e.g. `opta:...`). |
| `stadium_name` | character | Stadium: stage display name. |
| `stadium_city_name` | character | Stadium: city the stadium is in. |
| `stadium_country` | character | Stadium: country the stadium is in. |
| `stadium_address` | character | Stadium: street address of the stadium. |
| `stadium_capacity` | integer | Stadium: seating capacity of the stadium. |
| `stadium_year_of_construction` | integer | Stadium: year the stadium was built. |
| `stadium_maps_geo_code_latitude` | character | Stadium: latitude in decimal degrees. |
| `stadium_maps_geo_code_longitude` | character | Stadium: longitude in decimal degrees. |
| `editorial_social_facebook` | character | Editorial metadata: facebook handle or URL. |
| `editorial_social_instagram` | character | Editorial metadata: instagram handle or URL. |
| `editorial_social_x` | character | Editorial metadata: x (Twitter) handle or URL. |
| `editorial_social_tik_tok` | character | Editorial metadata: tikTok handle or URL. |
| `editorial_social_you_tube` | character | Editorial metadata: youTube handle or URL. |
| `editorial_social_linked_in` | character | Editorial metadata: linkedIn handle or URL. |
| `editorial_website_url` | character | Editorial metadata: official website URL. |
| `editorial_shop_url` | character | Editorial metadata: club shop URL. |
| `editorial_tickets_url` | character | Editorial metadata: ticketing URL. |
| `editorial_club_primary_colour` | character | Editorial metadata: club primary colour (hex). |
| `editorial_club_secondary_colour` | character | Editorial metadata: club secondary colour (hex). |
| `editorial_club_text_colour` | character | Editorial metadata: club text colour (hex). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nwsl_teams(season_id='nwsl::Football_Season::0b6761e4701749f593690c0f338da74c')
```

_Last validated n/a._
