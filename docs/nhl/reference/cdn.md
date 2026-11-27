# NHL — ESPN CDN API (cdn.espn.com)

> NHL — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.nhl` — 1 endpoint.

## espn_nhl_cdn_schedule

espn.com schedule page data, one row per game: up to 7 days starting at `date` (mbb and wbb: that day only; cfb and nfl: one week).

**Endpoint URL:** `GET https://cdn.espn.com/core/nhl/schedule`

**Valid URL:** [https://cdn.espn.com/core/nhl/schedule?xhr=1&date=20250115](https://cdn.espn.com/core/nhl/schedule?xhr=1&date=20250115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_nhl_cdn_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN event id. |
| `uid` | String | Competitor uid string. |
| `date` | String | Match start timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | Int64 | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | Int64 | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | String | ESPN slug of the event's season type (e.g. 'regular-season', 'post-season'; soccer competitions use their own, such as 'final' or 'torneo-apertura'). |
| `status_type_id` | Int64 | Status type identifier. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status state (pre/in/post). |
| `status_type_completed` | Boolean | Whether the game is complete. |
| `status_type_description` | String | Status description. |
| `status_type_detail` | String | Status detail text. |
| `status_type_short_detail` | String | Short status detail. |
| `status_clock` | Float64 | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | String | Display clock string. |
| `status_period` | Int64 | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | Boolean | Whether the match is played at a neutral venue. |
| `conference_competition` | Boolean | Whether it is a conference competition. |
| `attendance` | Int64 | Game attendance. |
| `venue_id` | Int64 | Venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_city` | String | Venue city. |
| `venue_state` | String | Venue state. |
| `venue_indoor` | Boolean | Whether the venue is indoors. |
| `broadcast` | String | Every network name of the competition in feed order, deduped and ", "-joined (an entry with no names gives its market); "" when the game lists no broadcast. |
| `note` | String | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | Int64 | Home team ESPN identifier. |
| `home_name` | String | Home team display name. |
| `home_abbreviation` | String | Home team abbreviation. |
| `home_display_name` | String | Home team display name. |
| `home_location` | String | Home team city. |
| `home_color` | String | Home team primary color hex. |
| `home_alternate_color` | String | Home team alternate color hex. |
| `home_logo` | String | Home team logo URL. |
| `home_score` | String | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | Boolean | Whether the home team won. |
| `home_rank` | String | Home team rank (if ranked). |
| `away_id` | Int64 | Away team ESPN identifier. |
| `away_name` | String | Away team display name. |
| `away_abbreviation` | String | Away team abbreviation. |
| `away_display_name` | String | Away team display name. |
| `away_location` | String | Away team city. |
| `away_color` | String | Away team primary color hex. |
| `away_alternate_color` | String | Away team alternate color hex. |
| `away_logo` | String | Away team logo URL. |
| `away_score` | String | Away team's score. For cricket, the innings string. |
| `away_winner` | Boolean | Whether the away team won. |
| `away_rank` | String | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_cdn_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_cdn_schedule-example}

```python
espn_nhl_cdn_schedule(date='20250115')
```
