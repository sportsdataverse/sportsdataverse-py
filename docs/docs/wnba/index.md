# sportsdataverse.wnba package

## Submodules

## sportsdataverse.wnba.wnba_draft module

ESPN WNBA draft picks scraper.

Single ESPN endpoint:
: site.web.api.espn.com/apis/site/v2/sports/basketball/wnba/draft?season=\{year\}

ESPN ships the modern draft response with each pick inlined under
`picks[]`, carrying the rich athlete metadata (display name, height,
position id, college team, headshot, ESPN profile link) the older
`sports.core.api.espn.com` `/draft/rounds` endpoint required a separate
`$ref` resolution to fetch. This wrapper flattens that `picks[]` array
to a single polars DataFrame, one row per pick.

Fields ESPN does not inline on the draft response (e.g. `firstName` /
`lastName`, `weight`, `age`, birth city / state, full position name,
school id) come back as `None`; resolve them via
`espn_wnba_athlete_info` (or the matching wehoop R wrapper) using the
returned `athlete_id`.

### sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull ESPN WNBA draft picks for a season.

* **Parameters:**
  * **season** – Season year (e.g. `2024` for the 2024 WNBA Draft).
    Forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise
    polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with one row per draft pick.
  Documented columns: `season`, `round_number`, `pick_number`,
  `overall_pick`, `team_id`, `team_abbreviation`,
  `team_display_name`, `athlete_id`, `athlete_first_name`,
  `athlete_last_name`, `athlete_full_name`,
  `athlete_display_name`, `athlete_position_id`,
  `athlete_position_name`, `athlete_position_abbreviation`,
  `athlete_height`, `athlete_weight`, `athlete_age`,
  `athlete_birth_city`, `athlete_birth_state`, `headshot_href`,
  `school_id`, `school_name`, `school_abbreviation`,
  `link_web`.

  Fields ESPN does not inline on the draft response (e.g.
  first / last name, weight, age, birth location, school id) come
  back as `None`; resolve them via the athlete-info endpoint
  using the returned `athlete_id`.

  If `raw=True`, returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Pull a single draft year — one row per pick:

```default
from sportsdataverse.wnba import espn_wnba_draft
draft = espn_wnba_draft(season=2024)
print(draft.shape)
draft.select(
    ["overall_pick", "round_number", "team_abbreviation", "athlete_display_name", "school_name"]
).head(12)
```

First-round picks only:

```default
import polars as pl
draft.filter(pl.col("round_number") == 1).head()
```

Pandas round-trip — convenient for joining against your own roster table:

```default
draft_pd = espn_wnba_draft(season=2024, return_as_pandas=True)
draft_pd[["overall_pick", "athlete_display_name", "school_name"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_espn_ext module

sportsdataverse.wnba.wnba_espn_ext — ESPN endpoint wrappers ported from wehoop.

Registers `espn_wnba_*` wrappers via `sportsdataverse._common_espn.make_league_module()`.
~105 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
and Core v2 (league, seasons, athletes, events, catalog).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/awards — awards won by the athlete.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_bio(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/bio — athlete bio.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_bio()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_career_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statistics[/\{type\}]. `type` ∈ \{0=reg, 1=post, 2=career\}.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_contracts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/contracts — contract info.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_contracts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — enriched athlete profile (core v2).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_eventlog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/eventlog — event participation log.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_eventlog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_gamelog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/gamelog?season=\{y\}. **404 for NHL.**

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_gamelog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_gamelog()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — athlete profile (site v2 lite shape).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/injuries — per-athlete injuries.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/news — athlete-scoped news.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/notes — analyst notes.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_overview(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/\{sport\}/\{league\}/athletes/\{id\}/overview — rich snapshot.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_overview()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_overview()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_records(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/records — career records.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_records()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/seasons — seasons played.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_splits(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/splits?season=\{y\} — situational splits.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_splits()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_splits()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_statisticslog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statisticslog — game-by-game log (NHL gamelog replacement).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statisticslog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/stats?season=\{y\} — parallel-array stats.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_stats()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_stats()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athlete_vs_athlete(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/vsathlete/\{oid\} — head-to-head.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_vsathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_athletes_index(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes?active=\{bool\}&limit=\{n\}&page=\{p\} — paginated athletes index.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athletes_index()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_award(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards/\{id\} — single award detail.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_award()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards — league award catalog.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_calendar(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar — full season calendar.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_calendar_offseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/offseason.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_offseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_calendar_ondays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/ondays — dates with games.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_ondays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_calendar_postseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/postseason.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_postseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_calendar_regular_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/regular-season — week-by-week regular season ranges.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_regular_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_coach(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\} — single coach.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_coach_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/record/\{type\} — coaching record.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_coach_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/seasons/\{y\} — coach’s per-season record.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches — coaches index. **Often 404s — prefer /seasons/\{y\}/coaches.**

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_conferences(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /groups — conferences and divisions.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_groups()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /draft — draft board (varies per sport).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\} — event root.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_broadcasts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/broadcasts — TV/streaming broadcasters.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_broadcasts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competition(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\} — competition (cid defaults to event_id).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competition()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors/\{tid\} — single competitor.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitor_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/leaders — per-team game leaders.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitor_linescores(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/linescores — per-period scores.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_linescores()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_linescores()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitor_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/record — competitor record at game-time.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitor_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/roster — competitor roster for one game.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitor_statistics(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/statistics — team game statistics.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_statistics()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_competitors(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors — both teams’ refs.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitors()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/leaders — per-game leaders.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_odds(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/odds — game odds.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_odds()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_official_detail(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials/\{oid\} — single official detail.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_official_detail()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_officials(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials — referees/umpires.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_officials()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_play(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays/\{pid\} — single play detail.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_play_personnel(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/plays/\{pid\}/personnel — personnel on the play.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play_personnel()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_plays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays — raw plays for one game.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_plays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_plays()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/powerindex — power index for the game.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_predictor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/predictor — ESPN game predictor.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_predictor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_probabilities(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/probabilities — per-play WP timeline.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_probabilities()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_propbets(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/propbets — prop bet markets.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_propbets()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_scoringplays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/scoringplays — scoring summary.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_scoringplays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_situation(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/situation — current in-game state.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_situation()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_event_status(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/status — current event status.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_status()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events?dates=\{d\} — paginated events index.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_franchise(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises/\{id\} — single franchise.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchise()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_franchises(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises — franchise list.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchises()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /injuries — league-wide injury report.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/statistics/byathlete — ranked leaderboard with glossary.

`category` is optional: when omitted the URL is built without
`?category=...` and ESPN returns the league-default leader set,
which is the shape the cross-league `espn_<league>_leaders()`
callers (and `parse_leaders`) expect.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._espn_statistics_byathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_leaders()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_leaders_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leaders — league-wide statistical leaders (core v2).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_league_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /notes — league-level editorial notes (sparse; NFL crawler discovery).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_league_root(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\} — league root.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_root()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /news — league-wide news.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_position(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions/\{id\} — single position.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_position()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_positions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions — position definitions.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_positions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_scoreboard(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /scoreboard. `dates`: YYYYMMDD or YYYYMMDD-YYYYMMDD or season year.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_scoreboard()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_scoreboard()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_athletes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/athletes — athletes active in a season.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_athletes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/awards — awards given in a season.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/coaches — coaches active in a season.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft — draft board for a year.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_draft()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_draft_round_picks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft/rounds/\{r\}/picks — per-round picks.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft_round_picks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_freeagents(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/freeagents — UFA/RFA list (where applicable).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_freeagents()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_futures(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/futures — futures odds.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_futures()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_group(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\} — single group within season-type.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_group_children(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/children — sub-groups (divisions inside conf).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_children()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_group_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/teams — teams in a group.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_groups(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups — conferences/divisions within season-type.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\} — single-season root.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_pointer(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/season — current-season pointer.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_pointer()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex[/\{teamId\}] — BPI/FPI/SP+. Per-team when `team_id`.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_powerindex_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex/leaders — power-index leaderboard.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams/\{id\} — team-in-a-season profile.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams — teams active in a season.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_type(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\} — season-type root.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_type_corrections(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/corrections — stat-correction audit trail.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_corrections()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_type_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/leaders — per-season-type leaders.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_types(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types — season-type list (1=pre, 2=reg, 3=post, 4=off/all-star).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_types()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_week(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\} — single-week root.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_week_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/events — week-scoped events.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_season_weeks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks — weeks within a season-type (NFL/CFB).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_weeks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/seasons — paginated season list.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_standings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /apis/v2/sports/\{sport\}/\{league\}/standings — full standings (not the stub).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_alt_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_standings_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /standings — league standings (core v2 form).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_statistics_league(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /statistics — league statistical leaders (site-v2 variant).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_summary(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /summary?event=\{id\} — comprehensive game summary (boxscore + plays + leaders).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_summary()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_summary()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_talentpicks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /talentpicks — ESPN editorial talent picks (sparse; NFL crawler discovery).

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_talentpicks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — single team detail.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — enriched team.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_depthcharts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/depthcharts — depth chart by position.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_depthcharts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_history(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/history — franchise historical record.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_history()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/injuries — team injury report.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/leaders — team statistical leaders.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/news — team-scoped news.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/record — team win/loss record.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/roster — team roster.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_schedule(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/schedule — team schedule for a season.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_schedule()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_schedule()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_team_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/transactions — recent team transactions.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_teams_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — paginated teams catalog.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_teams_site(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — all teams.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_tournaments(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /tournaments — tournament list.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_tournaments()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /transactions — league-wide transactions.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._site_v2_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_venue(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues/\{id\} — single venue detail.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_venue()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.wnba.wnba_espn_ext.espn_wnba_venues(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues — stadiums/arenas.

Bound to `sport='basketball'`, `league='wnba'`. Core implementation: `sportsdataverse._common_espn._core_v2_venues()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

## sportsdataverse.wnba.wnba_event_officials module

ESPN WNBA game officials scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_event_officials()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_event_officials._espn_basketball_event_officials`
to keep the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull the officials assigned to a WNBA game.

See `sportsdataverse.wbb.espn_wbb_event_officials()` for full
documentation of the column set, the empty-frame fallback when ESPN
ships no officials, and the `raw` / `return_as_pandas` flag
semantics.

* **Parameters:**
  * **game_id** – ESPN WNBA event identifier (e.g. `401620238` for Game 1
    of the 2024 WNBA Finals).
  * **season** – Season year (recorded as output column only).
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with the same columns documented in
  `sportsdataverse.wbb.espn_wbb_event_officials()`. If
  `raw=True`, returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Pull officials for the 2024 WNBA Finals Game 1:

```default
from sportsdataverse.wnba import espn_wnba_event_officials
refs = espn_wnba_event_officials(game_id=401620238, season=2024)
print(refs.shape)
refs.select(["full_name", "position_name", "order"]).head()
```

Pandas round-trip:

```default
refs_pd = espn_wnba_event_officials(
    game_id=401620238, season=2024, return_as_pandas=True
)
refs_pd[["full_name", "position_name"]].head()
```

Inspect the raw ESPN payload (e.g. for fields not flattened):

```default
payload = espn_wnba_event_officials(game_id=401620238, season=2024, raw=True)
list(payload.keys())[:8]
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_game_rosters module

### sportsdataverse.wnba.wnba_game_rosters.espn_wnba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_wnba_schedule().
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars Data frame of game roster data with columns:
  ‘athlete_id’, ‘athlete_uid’, ‘athlete_guid’, ‘athlete_type’,
  ‘first_name’, ‘last_name’, ‘full_name’, ‘athlete_display_name’,
  ‘short_name’, ‘weight’, ‘display_weight’, ‘height’, ‘display_height’,
  ‘age’, ‘date_of_birth’, ‘slug’, ‘jersey’, ‘linked’, ‘active’,
  ‘alternate_ids_sdr’, ‘birth_place_city’, ‘birth_place_state’,
  ‘birth_place_country’, ‘headshot_href’, ‘headshot_alt’,
  ‘experience_years’, ‘experience_display_value’,
  ‘experience_abbreviation’, ‘status_id’, ‘status_name’, ‘status_type’,
  ‘status_abbreviation’, ‘hand_type’, ‘hand_abbreviation’,
  ‘hand_display_value’, ‘draft_display_text’, ‘draft_round’, ‘draft_year’,
  ‘draft_selection’, ‘player_id’, ‘starter’, ‘valid’, ‘did_not_play’,
  ‘display_name’, ‘ejected’, ‘athlete_href’, ‘position_href’,
  ‘statistics_href’, ‘team_id’, ‘team_guid’, ‘team_uid’, ‘team_slug’,
  ‘team_location’, ‘team_name’, ‘team_abbreviation’,
  ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
  ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
  ‘logo_href’, ‘logo_dark_href’, ‘game_id’
* **Return type:**
  pl.DataFrame

### Example

Pull both teams’ rosters for a single game:

```default
from sportsdataverse.wnba import espn_wnba_game_rosters
rosters = espn_wnba_game_rosters(game_id=401620238)  # 2024 WNBA Finals Game 1
print(rosters.shape)
rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)
```

Just the starters:

```default
import polars as pl
rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])
```

Pandas round-trip:

```default
rosters_pd = espn_wnba_game_rosters(game_id=401620238, return_as_pandas=True)
rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_game_items(summary)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_team_items(items, \*\*kwargs)

## sportsdataverse.wnba.wnba_loaders module

### sportsdataverse.wnba.wnba_loaders.load_wnba_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA play by play data going back to 2002

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Pull a single season’s play-by-play parquet:

```default
from sportsdataverse.wnba import load_wnba_pbp
pbp = load_wnba_pbp(seasons=2024)
print(pbp.shape)
```

Pull a range of seasons (closed-open like Python `range`):

```default
pbp = load_wnba_pbp(seasons=range(2020, 2025))
pbp.group_by("season").len().sort("season")
```

Pandas round-trip and a quick filter on play type:

```default
pbp_pd = load_wnba_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd[pbp_pd["type_text"] == "JumpShot"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_loaders.load_wnba_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA player boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  player boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Pull player box scores for a single season:

```default
from sportsdataverse.wnba import load_wnba_player_boxscore
pb = load_wnba_player_boxscore(seasons=2024)
print(pb.shape)
```

A’ja Wilson (athlete_id 3149391) game-by-game scoring:

```default
import polars as pl
wilson = pb.filter(pl.col("athlete_id") == 3149391)
wilson.select(["game_id", "minutes", "points", "rebounds", "assists"]).head()
```

Pandas round-trip across multiple seasons:

```default
pb_pd = load_wnba_player_boxscore(seasons=range(2022, 2025), return_as_pandas=True)
pb_pd.groupby("season")["points"].mean()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_loaders.load_wnba_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA schedule data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  schedule for  the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Pull a single season’s schedule:

```default
from sportsdataverse.wnba import load_wnba_schedule
sched = load_wnba_schedule(seasons=2024)
print(sched.shape)
```

Pull a range of seasons and count by status:

```default
sched = load_wnba_schedule(seasons=range(2020, 2025))
sched.group_by(["season", "status_type_description"]).len().sort(["season", "len"])
```

Pandas round-trip with a single season:

```default
sched_pd = load_wnba_schedule(seasons=[2024], return_as_pandas=True)
sched_pd[["game_id", "home_name", "away_name", "game_date"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_loaders.load_wnba_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA team boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  team boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Pull team box scores for a single season:

```default
from sportsdataverse.wnba import load_wnba_team_boxscore
tb = load_wnba_team_boxscore(seasons=2024)
print(tb.shape)
```

Pull a range of seasons:

```default
tb = load_wnba_team_boxscore(seasons=range(2020, 2025))
tb.group_by("season").len().sort("season")
```

Aces (team_id 17) game-by-game scoring:

```default
import polars as pl
tb.filter(pl.col("team_id") == 17).select(["game_id", "team_score", "opponent_team_score"]).head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_pbp module

### sportsdataverse.wnba.wnba_pbp.espn_wnba_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_wnba_pbp() - Pull the game by id. Data from API endpoints - wnba/playbyplay, wnba/summary

* **Parameters:**
  **game_id** (*int*) – Unique game_id, can be obtained from wnba_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”,
  : ”broadcasts”, “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”,
    “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Pull a single game’s play-by-play feed:

```default
from sportsdataverse.wnba import espn_wnba_pbp
game = espn_wnba_pbp(game_id=401620238)  # 2024 WNBA Finals Game 1
list(game.keys())  # ['gameId', 'plays', 'winprobability', ...]
```

Inspect the parsed plays and a header summary:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
print(plays.select(["period", "time", "type.text", "text"]).head(5))
```

Fetch the unparsed payload for custom downstream parsing:

```default
raw = espn_wnba_pbp(game_id=401620238, raw=True)
sorted(raw.keys())[:5]  # raw ESPN summary keys, no flattening
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_pbp.helper_wnba_game_data(pbp_txt, init)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp(game_id, pbp_txt)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pickcenter(pbp_txt)

### sportsdataverse.wnba.wnba_pbp.wnba_pbp_disk(game_id, path_to_json)

## sportsdataverse.wnba.wnba_player_stats module

ESPN WNBA athlete season stats scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_player_stats()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_player_stats._espn_basketball_player_stats` to
keep the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]

### sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]

Pull ESPN season stats for a WNBA athlete.

See `sportsdataverse.wbb.espn_wbb_player_stats()` for full
documentation of the return shape, the canonical three category keys
(`"Averages"`, `"Totals"`, `"Misc"`), the per-category column
set, and the `"Other"` fallback bucket.

* **Parameters:**
  * **athlete_id** – ESPN WNBA athlete identifier (e.g. `3149391` for A’ja
    Wilson).
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a dict of pandas DataFrames;
    otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Dict with one DataFrame per stat category — see
  `sportsdataverse.wbb.espn_wbb_player_stats()` for the full
  column / key documentation. If `raw=True`, returns the raw
  response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Pull A’ja Wilson’s 2024 season stats and inspect the canonical category keys:

```default
from sportsdataverse.wnba import espn_wnba_player_stats
frames = espn_wnba_player_stats(athlete_id=3149391, season=2024)
sorted(frames.keys())  # at minimum: 'Averages', 'Totals', 'Misc'
frames["Averages"].head()
```

Combine the per-game `Averages` and full-season `Totals`:

```default
avgs = frames["Averages"]
totals = frames["Totals"]
print(avgs.shape, totals.shape)
avgs.select(["points_per_game", "rebounds_per_game", "assists_per_game"]).head()
```

Pandas round-trip — returns a dict of DataFrames keyed by category:

```default
frames_pd = espn_wnba_player_stats(
    athlete_id=3149391, season=2024, return_as_pandas=True
)
frames_pd["Misc"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_schedule module

### sportsdataverse.wnba.wnba_schedule.espn_wnba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_calendar - look up the WNBA calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **ondays** (*boolean*) – Used to return dates for calendar ondays
* **Returns:**
  Polars dataframe containing calendar dates for the requested season.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Calendar entries for a season:

```default
from sportsdataverse.wnba import espn_wnba_calendar
cal = espn_wnba_calendar(season=2024)
print(cal.shape)
cal.head()
```

Just the on-days (game-played dates), useful for batch loops:

```default
ondays = espn_wnba_calendar(season=2024, ondays=True)
for url in ondays["url"].head(3).to_list():
    print(url)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_schedule.espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_schedule - look up the WNBA schedule for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Pull a single date’s slate (YYYYMMDD):

```default
from sportsdataverse.wnba import espn_wnba_schedule
sched = espn_wnba_schedule(dates=20241011)  # 2024 WNBA Finals Game 1
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()
```

Pull a full regular season’s worth of games:

```default
reg = espn_wnba_schedule(dates=2024, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)
```

Pandas round-trip for a single date:

```default
espn_wnba_schedule(dates=20241011, return_as_pandas=True).head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_schedule.most_recent_wnba_season()

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it’s May or later (the WNBA regular
season has tipped off), otherwise the previous calendar year.

* **Returns:**
  Year (e.g. `2024`) suitable for passing as a `season` argument
  to schedule / loader functions.
* **Return type:**
  int

### Example

Use as a default for season-aware loaders:

```default
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_schedule.scoreboard_event_parsing(event)

## sportsdataverse.wnba.wnba_standings module

ESPN WNBA standings scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_standings()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_standings._espn_basketball_standings` to keep
the wbb / wnba pair DRY.

Unlike the WBB endpoint, the WNBA standings call doesn’t take a `group`
filter — the league has a single division, so the helper is invoked with
`group=None`.

### sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull ESPN WNBA standings for a season.

See `sportsdataverse.wbb.espn_wbb_standings()` for full
documentation of the column set. The WNBA endpoint does not take a
`group` filter.

* **Parameters:**
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise
    polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with one row per team — see
  `sportsdataverse.wbb.espn_wbb_standings()` for the full
  column list. If `raw=True`, returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Pull WNBA standings for a season:

```default
from sportsdataverse.wnba import espn_wnba_standings
standings = espn_wnba_standings(season=2024)
print(standings.shape)
standings.head()
```

Sort by win percentage:

```default
import polars as pl
standings.sort("win_percent", descending=True).select(
    ["team_display_name", "wins", "losses", "win_percent"]
).head(8)
```

Pandas round-trip:

```default
standings_pd = espn_wnba_standings(season=2024, return_as_pandas=True)
standings_pd[["team_display_name", "wins", "losses"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_team_roster module

ESPN WNBA team-level season roster scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_team_roster()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_team_roster._espn_basketball_team_roster` to keep
the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_team_roster.espn_wnba_team_roster(team_id: int, season: int | None = None, *, raw: bool = False, return_as_pandas: bool = False, \*\*kwargs: Any) → DataFrame | DataFrame | dict[str, Any]

Pull the current ESPN team roster for a WNBA team.

See `sportsdataverse.wbb.espn_wbb_team_roster()` for full documentation
of the column set. ESPN’s `/teams/{id}/roster` endpoint ignores
`?season=YYYY`; the `season` argument is recorded as an output column
only and does not alter the request URL.

* **Parameters:**
  * **team_id** – ESPN WNBA team identifier (e.g. `3` for Dallas Wings).
  * **season** – Season year (recorded as output column only).
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with the same columns documented in
  `sportsdataverse.wbb.espn_wbb_team_roster()`. If `raw=True`,
  returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Las Vegas Aces (team_id 17) current roster:

```default
from sportsdataverse.wnba import espn_wnba_team_roster
roster = espn_wnba_team_roster(team_id=17, season=2024)
print(roster.shape)
roster.select(["athlete_id", "full_name", "jersey", "position_abbreviation"]).head()
```

Pandas round-trip — useful for one-off notebook work:

```default
roster_pd = espn_wnba_team_roster(team_id=17, season=2024, return_as_pandas=True)
roster_pd[["full_name", "jersey", "position_abbreviation", "height"]].head()
```

Inspect the raw ESPN payload:

```default
payload = espn_wnba_team_roster(team_id=17, season=2024, raw=True)
list(payload.keys())[:8]
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_team_stats module

ESPN WNBA team season-stats scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_team_stats()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_team_stats._espn_basketball_team_stats` to keep
the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]

### sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]

Pull ESPN team season stats for a WNBA team.

See `sportsdataverse.wbb.espn_wbb_team_stats()` for full
documentation of the return shape, the canonical three category keys
(`"Averages"`, `"Totals"`, `"Misc"`), the per-category column
set, and the `"Other"` fallback bucket.

* **Parameters:**
  * **team_id** – ESPN WNBA team identifier (e.g. `17` for the Las Vegas
    Aces).
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a dict of pandas DataFrames;
    otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Dict with one DataFrame per stat category — see
  `sportsdataverse.wbb.espn_wbb_team_stats()` for the full
  column / key documentation. If `raw=True`, returns the raw
  response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Las Vegas Aces’ 2024 team stats — keyed by category:

```default
from sportsdataverse.wnba import espn_wnba_team_stats
frames = espn_wnba_team_stats(team_id=17, season=2024)
sorted(frames.keys())  # 'Averages', 'Totals', 'Misc' (plus optional 'Other')
frames["Averages"].head()
```

Compare per-game and totals at a glance:

```default
avgs = frames["Averages"]
totals = frames["Totals"]
print(avgs.shape, totals.shape)
avgs.select(["games_played", "points_per_game", "rebounds_per_game"])
```

Pandas round-trip:

```default
frames_pd = espn_wnba_team_stats(team_id=17, season=2024, return_as_pandas=True)
frames_pd["Misc"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_teams module

### sportsdataverse.wnba.wnba_teams.espn_wnba_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_teams - look up WNBA teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.wnba.espn_wnba_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Pull the full WNBA team directory:

```default
from sportsdataverse.wnba import espn_wnba_teams
teams = espn_wnba_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()
```

Find Las Vegas Aces (team_id 17):

```default
teams.filter(__import__("polars").col("team_id") == "17").to_dicts()
```

Refresh the cache (the call is `lru_cache`’d):

```default
espn_wnba_teams.cache_clear()  # cached at function-level
teams_pd = espn_wnba_teams(return_as_pandas=True)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## Module contents
