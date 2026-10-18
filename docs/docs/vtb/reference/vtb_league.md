---
title: VTB — VTB United League API (api.vtb-league.com)
sidebar_label: VTB United League API (api.vtb-league.com)
description: "VTB — VTB United League API (api.vtb-league.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# VTB — VTB United League API (api.vtb-league.com)

`sportsdataverse.basketball.vtb` — 3 endpoints.

> **Note:** Owner flag (sdv-internal-refs/vtb/README.md): the VTB United League is run from Russia and several clubs are owned or sponsored by sanctioned state companies (the KHL precedent); Infobasket is the Russian basketball federation's stats platform. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first. No accounts or payments are involved.

> **Note:** Unofficial and unsupported: both hosts are keyless public JSON backends of vtb-league.com. No terms of use are published; robots.txt is a 404 on api.vtb-league.com and org.infobasket.su (no rules; vtb-league.com disallows only /wp/wp-admin/). The runtime waits at least 1 s before every request, retries included, and honours Retry-After on 429 / 503. No key, cookie or proxy is used (live-verified direct from the droplet 2026-10-12).

> **Note:** Seasons are END years (2026 = 2025-26) on both backends. Infobasket keys everything by competition id: vtb_infobasket_seasons maps the END year to the season-level CompID, and the season-keyed wrappers do that mapping for you. CompID means three things: a season (50714), a stage below it (50720 regular season, 54281 playoff; stage-level wrappers take the stage id from vtb_infobasket_comp_issue) and, in the player season stats, an END year.

> **Note:** Ids: the v2 matchId is the Infobasket GameID, the v2 teamId the Infobasket TeamID and the v2 venueId the Infobasket ArenaId. team_id is the key on every team route; comp_team_id (a registration in one stage) and the club-record id of the team page are other key spaces. person_id is one person across clubs and seasons.

> **Note:** v2 pages: meta.count is the rows returned (not a total), page is ignored and the default limit is 20, so the matches wrapper asks for 1000 and refuses a body whose count reaches its limit. The API lists new seasons only (from 2024-25; vtb: 2025-2027, wbc: 2026-2027): earlier years answer no rows (NoDataError).

## vtb_league_leagues

The leagues served by the API: vtb, vtbyouth, wbc.

**Endpoint URL:** `GET https://api.vtb-league.com/v2/leagues`

**Valid URL:** [https://api.vtb-league.com/v2/leagues](https://api.vtb-league.com/v2/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#vtb_league_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | integer | v2 league id (1 vtb, 2 vtbyouth, 3 wbc). Not an Infobasket id. |
| `league` | character | League slug used in the paths: 'vtb' (VTB United League), 'vtbyouth' (youth league), 'wbc' (WINLINE Basket Cup). |
| `league_name_en` | character | The league name in English. |
| `league_name_ru` | character | The league name in Russian. |
| `league_abbrev_en` | character | The league abbreviation in English. |
| `league_abbrev_ru` | character | The league abbreviation in Russian. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_league_leagues-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | a season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_league_leagues-example}

```python
vtb_league_leagues()
```

_Last validated n/a._

## vtb_league_seasons

The seasons the API serves for one league (new seasons only, from 2024-25).

**Endpoint URL:** `GET https://api.vtb-league.com/v2/leagues/{league_slug}/seasons`

**Valid URL:** [https://api.vtb-league.com/v2/leagues/vtb/seasons](https://api.vtb-league.com/v2/leagues/vtb/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League slug (league in vtb_league_leagues): vtb (VTB United League), vtbyouth (youth league) or wbc (WINLINE Basket Cup). |

### Returns {#vtb_league_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26); the sdv season key and the `{season}` path param. |
| `league` | character | League slug, echoed. |
| `is_current` | logical | True for the current season. |
| `season_name` | character | Season name ('Season 2025/2026'). |
| `season_abbrev` | character | Season abbreviation ('25/26'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_league_seasons-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | a season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_league_seasons-example}

```python
vtb_league_seasons(league_slug='vtb')
```

_Last validated n/a._

## vtb_league_matches

Every match of one league-season (schedule, scores, quarter points, venue), in one request.

**Endpoint URL:** `GET https://api.vtb-league.com/v2/leagues/{league_slug}/seasons/{season}/matches`

**Valid URL:** [https://api.vtb-league.com/v2/leagues/vtb/seasons/2026/matches?limit=1000](https://api.vtb-league.com/v2/leagues/vtb/seasons/2026/matches?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League slug (league in vtb_league_leagues): vtb (VTB United League), vtbyouth (youth league) or wbc (WINLINE Basket Cup). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The v2 API holds 2025 onwards (vtb: 2025-2027; wbc: 2026-2027); earlier years answer no rows. |

### Returns {#vtb_league_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Match id; equals the Infobasket GameID (/Widget/GetOnline/{gameId}). |
| `league` | character | League slug, echoed. |
| `season` | integer | Season END year, echoed. |
| `competition` | character | Competition slug: 'vtb' (league games), 'vtb-supercup', 'vtb-allstars', 'wbc'. |
| `phase_id` | integer | v2 phase id of the stage (not an Infobasket CompID). |
| `phase_name_en` | character | The stage name in English. |
| `phase_name_ru` | character | The stage name in Russian. |
| `match_number` | character | Game number ('1/2-2' = semi-final 2; regular-season numbers otherwise). |
| `match_type` | character | Game type: REGULAR, FINALS (any playoff or cup knockout game), ALLSTARS. |
| `match_status` | character | Status: SCHEDULED, IN_PROGRESS, COMPLETE, POSTPONED, CANCELLED, UNKNOWN (the site filters on the first four). |
| `match_time` | character | Tip-off in venue local time, ISO 8601 with offset. |
| `match_time_msk` | character | Tip-off in Moscow time (UTC+3), ISO 8601. |
| `match_time_utc` | character | Tip-off in UTC, ISO 8601. |
| `is_time_set` | logical | True when the tip-off time is fixed. |
| `time_actual` | character | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `time_end_actual` | character | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `duration` | integer | Game duration; 0 in every sample (unverified). |
| `periods` | integer | Periods played (4 in regulation). |
| `extra_periods_used` | logical | True when the game went to overtime. |
| `attendance` | integer | Spectators (null when not recorded). |
| `hosting_team_id` | integer | Home team id (Infobasket TeamID). |
| `venue_id` | integer | Venue id; equals the Infobasket arena id (ArenaId). |
| `venue_name_en` | character | The venue name in English. |
| `venue_name_ru` | character | The venue name in Russian. |
| `venue_region_name_en` | character | The venue city in English. |
| `venue_region_name_ru` | character | The venue city in Russian. |
| `home_team_id` | integer | Infobasket TeamID of the home team. |
| `home_score` | integer | Final points of the home team (an integer; 0 for a cancelled match). |
| `home_team_name_en` | character | The home team's name in English. |
| `home_team_name_ru` | character | The home team's name in Russian. |
| `home_team_code_en` | character | The home team's short code in English. |
| `home_team_code_ru` | character | The home team's short code in Russian. |
| `home_region_name_en` | character | Region or city of the home team in English. |
| `home_region_name_ru` | character | Region or city of the home team in Russian. |
| `away_team_id` | integer | Infobasket TeamID of the away team. |
| `away_score` | integer | Final points of the away team (an integer; 0 for a cancelled match). |
| `away_team_name_en` | character | The away team's name in English. |
| `away_team_name_ru` | character | The away team's name in Russian. |
| `away_team_code_en` | character | The away team's short code in English. |
| `away_team_code_ru` | character | The away team's short code in Russian. |
| `away_region_name_en` | character | Region or city of the away team in English. |
| `away_region_name_ru` | character | Region or city of the away team in Russian. |
| `subtitle_en` | character | The site subtitle of the match in English. |
| `subtitle_ru` | character | The site subtitle of the match in Russian. |
| `necessary_label_en` | character | The site label of the match in English. |
| `necessary_label_ru` | character | The site label of the match in Russian. |
| `points_by_quarters` | character | Points per period as [home, away] pairs; null before the game. The home team (isHomeCompetitor true) is listed first in `competitors` on every probed row (vtb seasons 2025 and 2026, wbc 2026: 620 of 620), and the first value of each pair summed to that team's score on every finished one. |
| `home_q1` | integer | Points of the home team in period 1. |
| `away_q1` | integer | Points of the away team in period 1. |
| `home_q2` | integer | Points of the home team in period 2. |
| `away_q2` | integer | Points of the away team in period 2. |
| `home_q3` | integer | Points of the home team in period 3. |
| `away_q3` | integer | Points of the away team in period 3. |
| `home_q4` | integer | Points of the home team in period 4. |
| `away_q4` | integer | Points of the away team in period 4. |
| `home_ot` | integer | Points of the home team in all overtimes together (null without overtime). |
| `away_ot` | integer | Points of the away team in all overtimes together (null without overtime). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_league_matches-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | a season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_league_matches-example}

```python
vtb_league_matches(league_slug='vtb', season=2026)
```

_Last validated n/a._
