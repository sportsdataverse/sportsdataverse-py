---
title: NHL — ESPN CDN API (cdn.espn.com)
sidebar_label: ESPN CDN API (cdn.espn.com)
description: "NHL — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 24
toc_max_heading_level: 2
---
# NHL — ESPN CDN API (cdn.espn.com)

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
| `game_id` | character | ESPN event id. |
| `uid` | character | Competitor uid string. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season end year. |
| `season_type` | integer | Season type code (echoed from arg). |
| `season_slug` | character | Season type slug. |
| `status_type_id` | character | Status type identifier. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status description. |
| `status_type_detail` | character | Status detail text. |
| `status_type_short_detail` | character | Short status detail. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Display clock string. |
| `status_period` | integer | Current period. |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical | Whether it is a conference competition. |
| `attendance` | integer | Game attendance. |
| `venue_id` | character | Venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state. |
| `venue_indoor` | logical | Whether the venue is indoors. |
| `broadcast` | character | Broadcast network(s). |
| `note` | character | Game note or headline. |
| `home_id` | character | Home team ESPN identifier. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team city. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Home team alternate color hex. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | character | Home team rank (if ranked). |
| `away_id` | character | Away team ESPN identifier. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team city. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Away team alternate color hex. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | character | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nhl_cdn_schedule-example}

```python
espn_nhl_cdn_schedule(date='20250115')
```

_Last validated n/a._
