# sportsdataverse.mbb package

## Submodules

## sportsdataverse.mbb.mbb_espn_ext module

sportsdataverse.mbb.mbb_espn_ext — ESPN endpoint wrappers ported from hoopR.

Registers `espn_mbb_*` wrappers via `sportsdataverse._common_espn.make_league_module()`.
~108 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
Core v2 (league, seasons, athletes, events, catalog), plus NCAA-only
extensions (rankings, recruits, weekly rankings).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/awards — awards won by the athlete.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_bio(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/bio — athlete bio.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_bio()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_career_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statistics[/\{type\}]. `type` ∈ \{0=reg, 1=post, 2=career\}.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_contracts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/contracts — contract info.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_contracts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — enriched athlete profile (core v2).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_eventlog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/eventlog — event participation log.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_eventlog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_gamelog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/gamelog?season=\{y\}. **404 for NHL.**

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_gamelog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_gamelog()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — athlete profile (site v2 lite shape).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/injuries — per-athlete injuries.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/news — athlete-scoped news.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/notes — analyst notes.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_overview(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/\{sport\}/\{league\}/athletes/\{id\}/overview — rich snapshot.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_overview()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_overview()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_records(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/records — career records.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_records()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/seasons — seasons played.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_splits(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/splits?season=\{y\} — situational splits.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_splits()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_splits()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_statisticslog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statisticslog — game-by-game log (NHL gamelog replacement).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statisticslog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/stats?season=\{y\} — parallel-array stats.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_stats()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_stats()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athlete_vs_athlete(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/vsathlete/\{oid\} — head-to-head.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_vsathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_athletes_index(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes?active=\{bool\}&limit=\{n\}&page=\{p\} — paginated athletes index.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_athletes_index()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_award(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards/\{id\} — single award detail.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_award()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards — league award catalog.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_calendar(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar — full season calendar.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_calendar_offseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/offseason.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_offseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_calendar_ondays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/ondays — dates with games.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_ondays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_calendar_postseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/postseason.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_postseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_calendar_regular_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/regular-season — week-by-week regular season ranges.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_regular_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_coach(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\} — single coach.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_coach_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/record/\{type\} — coaching record.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_coach_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/seasons/\{y\} — coach’s per-season record.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches — coaches index. **Often 404s — prefer /seasons/\{y\}/coaches.**

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_conferences(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /groups — conferences and divisions.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_groups()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /draft — draft board (varies per sport).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\} — event root.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_broadcasts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/broadcasts — TV/streaming broadcasters.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_broadcasts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competition(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\} — competition (cid defaults to event_id).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competition()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors/\{tid\} — single competitor.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitor_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/leaders — per-team game leaders.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitor_linescores(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/linescores — per-period scores.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_linescores()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_linescores()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitor_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/record — competitor record at game-time.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitor_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/roster — competitor roster for one game.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitor_statistics(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/statistics — team game statistics.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_statistics()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_competitors(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors — both teams’ refs.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitors()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/leaders — per-game leaders.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_odds(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/odds — game odds.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_odds()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_official_detail(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials/\{oid\} — single official detail.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_official_detail()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_officials(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials — referees/umpires.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_officials()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_play(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays/\{pid\} — single play detail.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_play_personnel(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/plays/\{pid\}/personnel — personnel on the play.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play_personnel()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_plays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays — raw plays for one game.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_plays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_plays()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/powerindex — power index for the game.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_predictor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/predictor — ESPN game predictor.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_predictor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_probabilities(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/probabilities — per-play WP timeline.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_probabilities()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_propbets(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/propbets — prop bet markets.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_propbets()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_scoringplays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/scoringplays — scoring summary.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_scoringplays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_situation(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/situation — current in-game state.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_situation()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_event_status(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/status — current event status.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_status()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events?dates=\{d\} — paginated events index.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_franchise(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises/\{id\} — single franchise.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchise()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_franchises(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises — franchise list.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchises()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /injuries — league-wide injury report.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/statistics/byathlete — ranked leaderboard with glossary.

`category` is optional: when omitted the URL is built without
`?category=...` and ESPN returns the league-default leader set,
which is the shape the cross-league `espn_<league>_leaders()`
callers (and `parse_leaders`) expect.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._espn_statistics_byathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_leaders()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_leaders_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leaders — league-wide statistical leaders (core v2).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_league_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /notes — league-level editorial notes (sparse; NFL crawler discovery).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_league_root(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\} — league root.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_root()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /news — league-wide news.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_position(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions/\{id\} — single position.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_position()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_positions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions — position definitions.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_positions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_rankings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /rankings — poll rankings (NCAA leagues only).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_rankings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_scoreboard(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /scoreboard. `dates`: YYYYMMDD or YYYYMMDD-YYYYMMDD or season year.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_scoreboard()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_scoreboard()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_athletes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/athletes — athletes active in a season.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_athletes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/awards — awards given in a season.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/coaches — coaches active in a season.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft — draft board for a year.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_draft()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_draft_round_picks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft/rounds/\{r\}/picks — per-round picks.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft_round_picks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_freeagents(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/freeagents — UFA/RFA list (where applicable).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_freeagents()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_futures(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/futures — futures odds.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_futures()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_group(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\} — single group within season-type.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_group_children(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/children — sub-groups (divisions inside conf).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_children()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_group_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/teams — teams in a group.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_groups(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups — conferences/divisions within season-type.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\} — single-season root.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_pointer(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/season — current-season pointer.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_pointer()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex[/\{teamId\}] — BPI/FPI/SP+. Per-team when `team_id`.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_powerindex_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex/leaders — power-index leaderboard.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_recruits(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/recruits — NCAA recruiting (CFB / MBB / WBB).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_recruits()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams/\{id\} — team-in-a-season profile.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams — teams active in a season.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_type(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\} — season-type root.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_type_corrections(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/corrections — stat-correction audit trail.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_corrections()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_type_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/leaders — per-season-type leaders.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_types(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types — season-type list (1=pre, 2=reg, 3=post, 4=off/all-star).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_types()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_week(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\} — single-week root.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_week_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/events — week-scoped events.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_week_rankings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/rankings — weekly polls (NCAA/CFB).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week_rankings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_season_weeks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks — weeks within a season-type (NFL/CFB).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_weeks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/seasons — paginated season list.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_standings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /apis/v2/sports/\{sport\}/\{league\}/standings — full standings (not the stub).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_alt_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_standings_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /standings — league standings (core v2 form).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_statistics_league(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /statistics — league statistical leaders (site-v2 variant).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_summary(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /summary?event=\{id\} — comprehensive game summary (boxscore + plays + leaders).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_summary()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_summary()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_talentpicks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /talentpicks — ESPN editorial talent picks (sparse; NFL crawler discovery).

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_talentpicks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — single team detail.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — enriched team.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_depthcharts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/depthcharts — depth chart by position.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_depthcharts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_history(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/history — franchise historical record.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_history()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/injuries — team injury report.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/leaders — team statistical leaders.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/news — team-scoped news.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/record — team win/loss record.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/roster — team roster.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_schedule(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/schedule — team schedule for a season.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_schedule()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_schedule()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_team_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/transactions — recent team transactions.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_teams_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — paginated teams catalog.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_teams_site(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — all teams.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_tournaments(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /tournaments — tournament list.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_tournaments()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /transactions — league-wide transactions.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._site_v2_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_venue(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues/\{id\} — single venue detail.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_venue()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.mbb.mbb_espn_ext.espn_mbb_venues(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues — stadiums/arenas.

Bound to `sport='basketball'`, `league='mens-college-basketball'`. Core implementation: `sportsdataverse._common_espn._core_v2_venues()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

## sportsdataverse.mbb.mbb_game_rosters module

### sportsdataverse.mbb.mbb_game_rosters.espn_mbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from mbb_schedule().
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe of game roster data with columns:
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
  ‘team_location’, ‘team_name’, ‘team_nickname’, ‘team_abbreviation’,
  ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
  ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
  ‘team_alternate_ids_sdr’, ‘logo_href’, ‘logo_dark_href’, ‘game_id’
* **Return type:**
  pl.DataFrame

### Example

Quick start (2024 NCAA M championship game):

```default
from sportsdataverse.mbb import espn_mbb_game_rosters
roster = espn_mbb_game_rosters(game_id=401638637)
print(roster.shape)
```

Identify starters:

```default
import polars as pl
starters = roster.filter(pl.col("starter") == True).select(
    ["full_name", "jersey", "team_display_name"]
)
```

Pandas round-trip:

```default
roster_pd = espn_mbb_game_rosters(game_id=401638637, return_as_pandas=True)
roster_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_game_items(summary)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_team_items(items, \*\*kwargs)

## sportsdataverse.mbb.mbb_loaders module

### sportsdataverse.mbb.mbb_loaders.load_mbb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball play by play data going back to 2002

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

Single season:

```default
from sportsdataverse.mbb import load_mbb_pbp
pbp = load_mbb_pbp(seasons=[2024])
print(pbp.shape)
```

Range of seasons:

```default
pbp_multi = load_mbb_pbp(seasons=range(2022, 2025))
print(pbp_multi["season"].unique().sort())
```

Pandas round-trip:

```default
pbp_pd = load_mbb_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_loaders.load_mbb_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball player boxscore data

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

Single season:

```default
from sportsdataverse.mbb import load_mbb_player_boxscore
pb = load_mbb_player_boxscore(seasons=[2024])
print(pb.shape)
```

Range of seasons + top scorers:

```default
import polars as pl
pb_multi = load_mbb_player_boxscore(seasons=range(2022, 2025))
top = (
    pb_multi
    .group_by("athlete_display_name")
    .agg(pl.col("points").sum().alias("total_points"))
    .sort("total_points", descending=True)
    .head(10)
)
```

Pandas round-trip:

```default
pb_pd = load_mbb_player_boxscore(seasons=[2024], return_as_pandas=True)
pb_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_loaders.load_mbb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball schedule data

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

Single season:

```default
from sportsdataverse.mbb import load_mbb_schedule
sched = load_mbb_schedule(seasons=[2024])
print(sched.shape)
```

Range of seasons:

```default
sched_multi = load_mbb_schedule(seasons=range(2022, 2025))
print(sched_multi["season"].unique().sort())
```

Pandas round-trip:

```default
sched_pd = load_mbb_schedule(seasons=[2024], return_as_pandas=True)
sched_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_loaders.load_mbb_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball team boxscore data

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

Single season:

```default
from sportsdataverse.mbb import load_mbb_team_boxscore
tb = load_mbb_team_boxscore(seasons=[2024])
print(tb.shape)
```

Range of seasons + filter to a specific team (Duke `team_id=150`):

```default
import polars as pl
tb_multi = load_mbb_team_boxscore(seasons=range(2022, 2025))
duke = tb_multi.filter(pl.col("team_id") == 150)
```

Pandas round-trip:

```default
tb_pd = load_mbb_team_boxscore(seasons=[2024], return_as_pandas=True)
tb_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.mbb.mbb_pbp module

### sportsdataverse.mbb.mbb_pbp.espn_mbb_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_mbb_pbp() - Pull the game by id. Data from API endpoints: mens-college-basketball/playbyplay, mens-college-basketball/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from mbb_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.
* **Returns:**
  Dictionary of game data with keys: “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
  “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”, “againstTheSpread”, “odds”, “predictor”,
  “espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start (2024 NCAA Division I men’s championship game):

```default
from sportsdataverse.mbb import espn_mbb_pbp
game = espn_mbb_pbp(game_id=401638637)
print(game["gameId"])
print(len(game["plays"]))
```

Filter shooting plays for a basic shot chart:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
shots = plays.filter(pl.col("shooting_play") == True)
shots.select(
    [
        "period_number",
        "clock_display_value",
        "team_id",
        "coordinate_x",
        "coordinate_y",
        "score_value",
        "text",
    ]
).head()
```

Convert to pandas:

```default
import pandas as pd
plays_pd = pd.DataFrame(game["plays"])
plays_pd[plays_pd["shooting_play"] == True].head()
```

Raw payload (skip the cleaning pipeline) for debugging:

```default
raw = espn_mbb_pbp(game_id=401638637, raw=True)
sorted(raw.keys())
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package; mirrors this surface for men’s basketball
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_pbp.helper_mbb_game_data(pbp_txt, init)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp(game_id, pbp_txt)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pickcenter(pbp_txt)

### sportsdataverse.mbb.mbb_pbp.mbb_pbp_disk(game_id, path_to_json)

## sportsdataverse.mbb.mbb_schedule module

### sportsdataverse.mbb.mbb_schedule.espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_calendar - look up the men’s college basketball calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **ondays** (*boolean*) – Used to return dates for calendar ondays
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing calendar dates for the requested season.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Calendar dates for a single season:

```default
from sportsdataverse.mbb import espn_mbb_calendar
cal = espn_mbb_calendar(season=2024)
cal.head()
```

On-days only (dates with games on the schedule):

```default
ondays = espn_mbb_calendar(season=2024, ondays=True)
ondays.head()
```

Pandas round-trip:

```default
cal_pd = espn_mbb_calendar(season=2024, return_as_pandas=True)
cal_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_schedule.espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_schedule - look up the men’s college basketball scheduler for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **groups** (*int*) – Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Single date (April 8, 2024 - 2024 NCAA M championship day):

```default
from sportsdataverse.mbb import espn_mbb_schedule
day = espn_mbb_schedule(dates=20240408)
print(day.shape)
```

Season-level pull (2024 season):

```default
season = espn_mbb_schedule(dates=2024, limit=1500)
print(season.shape)
```

Filter to a specific team (Duke `team_id=150`):

```default
import polars as pl
duke = season.filter(
    (pl.col("home_id") == "150") | (pl.col("away_id") == "150")
)
```

Pandas round-trip:

```default
season_pd = espn_mbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_schedule.most_recent_mbb_season()

Return the most recent men’s college basketball season year.

The men’s college basketball season spans early November through early
April; for any month October-December the “current season” is the
following calendar year (e.g. October 2025 returns `2026`).

* **Returns:**
  The most recent / current season year.
* **Return type:**
  int

### Example

Use as a default season argument:

```default
from sportsdataverse.mbb import most_recent_mbb_season, espn_mbb_schedule
season = most_recent_mbb_season()
sched = espn_mbb_schedule(dates=season)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football

### sportsdataverse.mbb.mbb_schedule.scoreboard_event_parsing(event)

## sportsdataverse.mbb.mbb_teams module

### sportsdataverse.mbb.mbb_teams.espn_mbb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_teams - look up the men’s college basketball teams

* **Parameters:**
  * **groups** (*int*) – Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.mbb.espn_mbb_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Default groups (D1):

```default
from sportsdataverse.mbb import espn_mbb_teams
teams = espn_mbb_teams()
print(teams.shape)
print(teams.columns[:8])
```

Walk every team-id (handy for batched scrapes):

```default
team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")
```

Pandas round-trip + Division II/III:

```default
d2_d3 = espn_mbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## Module contents
