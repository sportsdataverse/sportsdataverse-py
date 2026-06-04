# sportsdataverse.nba package

## Submodules

## sportsdataverse.nba.nba_espn_ext module

sportsdataverse.nba.nba_espn_ext — ESPN endpoint wrappers ported from hoopR.

Registers `espn_nba_*` wrappers via `sportsdataverse._common_espn.make_league_module()`.
~105 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
and Core v2 (league, seasons, athletes, events, catalog).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/awards — awards won by the athlete.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_bio(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/bio — athlete bio.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_bio()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_career_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statistics[/\{type\}]. `type` ∈ \{0=reg, 1=post, 2=career\}.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_contracts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/contracts — contract info.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_contracts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — enriched athlete profile (core v2).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_eventlog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/eventlog — event participation log.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_eventlog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_gamelog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/gamelog?season=\{y\}. **404 for NHL.**

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_gamelog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_gamelog()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — athlete profile (site v2 lite shape).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/injuries — per-athlete injuries.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/news — athlete-scoped news.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/notes — analyst notes.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_overview(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/\{sport\}/\{league\}/athletes/\{id\}/overview — rich snapshot.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_overview()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_overview()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_records(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/records — career records.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_records()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/seasons — seasons played.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_splits(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/splits?season=\{y\} — situational splits.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_splits()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_splits()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_statisticslog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statisticslog — game-by-game log (NHL gamelog replacement).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statisticslog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/stats?season=\{y\} — parallel-array stats.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_stats()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_stats()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athlete_vs_athlete(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/vsathlete/\{oid\} — head-to-head.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_vsathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_athletes_index(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes?active=\{bool\}&limit=\{n\}&page=\{p\} — paginated athletes index.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_athletes_index()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_award(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards/\{id\} — single award detail.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_award()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards — league award catalog.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_calendar(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar — full season calendar.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_calendar_offseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/offseason.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_offseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_calendar_ondays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/ondays — dates with games.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_ondays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_calendar_postseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/postseason.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_postseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_calendar_regular_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/regular-season — week-by-week regular season ranges.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_regular_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_coach(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\} — single coach.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_coach_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/record/\{type\} — coaching record.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_coach_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/seasons/\{y\} — coach’s per-season record.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches — coaches index. **Often 404s — prefer /seasons/\{y\}/coaches.**

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_conferences(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /groups — conferences and divisions.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_groups()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /draft — draft board (varies per sport).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\} — event root.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_broadcasts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/broadcasts — TV/streaming broadcasters.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_broadcasts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competition(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\} — competition (cid defaults to event_id).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competition()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors/\{tid\} — single competitor.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitor_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/leaders — per-team game leaders.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitor_linescores(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/linescores — per-period scores.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_linescores()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_linescores()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitor_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/record — competitor record at game-time.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitor_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/roster — competitor roster for one game.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitor_statistics(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/statistics — team game statistics.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_statistics()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_competitors(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors — both teams’ refs.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitors()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/leaders — per-game leaders.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_odds(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/odds — game odds.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_odds()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_official_detail(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials/\{oid\} — single official detail.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_official_detail()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_officials(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials — referees/umpires.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_officials()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_play(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays/\{pid\} — single play detail.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_play_personnel(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/plays/\{pid\}/personnel — personnel on the play.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play_personnel()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_plays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays — raw plays for one game.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_plays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_plays()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/powerindex — power index for the game.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_predictor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/predictor — ESPN game predictor.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_predictor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_probabilities(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/probabilities — per-play WP timeline.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_probabilities()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_propbets(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/propbets — prop bet markets.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_propbets()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_scoringplays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/scoringplays — scoring summary.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_scoringplays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_situation(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/situation — current in-game state.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_situation()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_event_status(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/status — current event status.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_status()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events?dates=\{d\} — paginated events index.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_franchise(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises/\{id\} — single franchise.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchise()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_franchises(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises — franchise list.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchises()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /injuries — league-wide injury report.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/statistics/byathlete — ranked leaderboard with glossary.

`category` is optional: when omitted the URL is built without
`?category=...` and ESPN returns the league-default leader set,
which is the shape the cross-league `espn_<league>_leaders()`
callers (and `parse_leaders`) expect.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._espn_statistics_byathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_leaders()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_leaders_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leaders — league-wide statistical leaders (core v2).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_league_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /notes — league-level editorial notes (sparse; NFL crawler discovery).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_league_root(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\} — league root.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_root()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /news — league-wide news.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_position(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions/\{id\} — single position.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_position()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_positions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions — position definitions.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_positions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_scoreboard(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /scoreboard. `dates`: YYYYMMDD or YYYYMMDD-YYYYMMDD or season year.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_scoreboard()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_scoreboard()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_athletes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/athletes — athletes active in a season.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_athletes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/awards — awards given in a season.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/coaches — coaches active in a season.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft — draft board for a year.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_draft()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_draft_round_picks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft/rounds/\{r\}/picks — per-round picks.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft_round_picks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_freeagents(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/freeagents — UFA/RFA list (where applicable).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_freeagents()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_futures(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/futures — futures odds.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_futures()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_group(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\} — single group within season-type.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_group_children(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/children — sub-groups (divisions inside conf).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_children()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_group_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/teams — teams in a group.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_groups(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups — conferences/divisions within season-type.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\} — single-season root.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_pointer(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/season — current-season pointer.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_pointer()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex[/\{teamId\}] — BPI/FPI/SP+. Per-team when `team_id`.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_powerindex_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex/leaders — power-index leaderboard.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams/\{id\} — team-in-a-season profile.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams — teams active in a season.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_type(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\} — season-type root.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_type_corrections(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/corrections — stat-correction audit trail.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_corrections()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_type_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/leaders — per-season-type leaders.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_types(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types — season-type list (1=pre, 2=reg, 3=post, 4=off/all-star).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_types()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_week(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\} — single-week root.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_week_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/events — week-scoped events.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_season_weeks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks — weeks within a season-type (NFL/CFB).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_weeks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/seasons — paginated season list.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_standings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /apis/v2/sports/\{sport\}/\{league\}/standings — full standings (not the stub).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_alt_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_standings_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /standings — league standings (core v2 form).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_statistics_league(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /statistics — league statistical leaders (site-v2 variant).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_summary(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /summary?event=\{id\} — comprehensive game summary (boxscore + plays + leaders).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_summary()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_summary()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_talentpicks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /talentpicks — ESPN editorial talent picks (sparse; NFL crawler discovery).

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_talentpicks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — single team detail.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — enriched team.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_depthcharts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/depthcharts — depth chart by position.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_depthcharts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_history(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/history — franchise historical record.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_history()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/injuries — team injury report.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/leaders — team statistical leaders.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/news — team-scoped news.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/record — team win/loss record.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/roster — team roster.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_schedule(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/schedule — team schedule for a season.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_schedule()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_schedule()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_team_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/transactions — recent team transactions.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_teams_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — paginated teams catalog.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_teams_site(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — all teams.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_tournaments(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /tournaments — tournament list.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_tournaments()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /transactions — league-wide transactions.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._site_v2_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_venue(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues/\{id\} — single venue detail.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_venue()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.nba.nba_espn_ext.espn_nba_venues(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues — stadiums/arenas.

Bound to `sport='basketball'`, `league='nba'`. Core implementation: `sportsdataverse._common_espn._core_v2_venues()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

## sportsdataverse.nba.nba_game_rosters module

### sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_nba_schedule().
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
  ‘team_location’, ‘team_name’, ‘team_abbreviation’,
  ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
  ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
  ‘logo_href’, ‘logo_dark_href’, ‘game_id’
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
print(rosters.shape)
```

Pandas round-trip:

```default
rosters_pd = espn_nba_game_rosters(game_id=401585183, return_as_pandas=True)
rosters_pd.head()
```

Pipeline next step (filter to game starters):

```default
import polars as pl
starters = espn_nba_game_rosters(game_id=401585183).filter(
    pl.col("starter") == True
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA rosters
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_game_rosters.helper_nba_athlete_items(teams_rosters, \*\*kwargs)

Internal helper that resolves each athlete `$ref` in a team-rosters frame
to the canonical athlete detail row.

* **Parameters:**
  * **teams_rosters** (*pl.DataFrame*) – Output of [`helper_nba_roster_items()`](#sportsdataverse.nba.nba_game_rosters.helper_nba_roster_items)
    (must contain an `athlete_href` column).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per resolved athlete.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

### sportsdataverse.nba.nba_game_rosters.helper_nba_game_items(summary)

Internal helper that flattens the ESPN `competitions/competitors`
summary payload into a polars DataFrame keyed by `team_id`.

* **Parameters:**
  **summary** (*dict*) – Parsed JSON from the ESPN competitors summary endpoint.
* **Returns:**
  Polars dataframe with one row per competitor team in the game.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

### sportsdataverse.nba.nba_game_rosters.helper_nba_roster_items(items, summary_url, \*\*kwargs)

Internal helper that fetches the roster entries for every team in a game.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_nba_game_items()`](#sportsdataverse.nba.nba_game_rosters.helper_nba_game_items).
  * **summary_url** (*str*) – Base ESPN summary URL used to derive each team’s
    roster endpoint.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per game-roster entry across both teams.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

### sportsdataverse.nba.nba_game_rosters.helper_nba_team_items(items, \*\*kwargs)

Internal helper that fetches team detail rows for every team referenced in
the competitors summary and returns them as a flat polars DataFrame.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_nba_game_items()`](#sportsdataverse.nba.nba_game_rosters.helper_nba_game_items).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  Team detail rows with logo URLs flattened out.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

## sportsdataverse.nba.nba_loaders module

### sportsdataverse.nba.nba_loaders.load_nba_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA play by play data going back to 2002

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

Quick start:

```default
from sportsdataverse.nba import load_nba_pbp
pbp = load_nba_pbp(seasons=[2023])
print(pbp.shape)
```

Multi-season pull as pandas:

```default
pbp_pd = load_nba_pbp(seasons=range(2020, 2024), return_as_pandas=True)
pbp_pd.head()
```

Pipeline next step (filter to made 3-pointers):

```default
import polars as pl
threes = load_nba_pbp(seasons=[2023]).filter(
    pl.col("type_text") == "3PT Field Goal"
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_loaders.load_nba_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA player boxscore data

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

Quick start:

```default
from sportsdataverse.nba import load_nba_player_boxscore
box = load_nba_player_boxscore(seasons=[2023])
print(box.shape)
```

Pandas round-trip:

```default
box_pd = load_nba_player_boxscore(seasons=[2023], return_as_pandas=True)
box_pd.head()
```

Pipeline next step (top season scorers):

```default
import polars as pl
top = (
    load_nba_player_boxscore(seasons=[2023])
    .group_by("athlete_display_name")
    .agg(pl.col("points").sum())
    .sort("points", descending=True)
    .head(10)
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_loaders.load_nba_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA schedule data

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

Quick start:

```default
from sportsdataverse.nba import load_nba_schedule
sched = load_nba_schedule(seasons=[2023])
print(sched.shape)
```

Pandas round-trip:

```default
sched_pd = load_nba_schedule(seasons=range(2020, 2024), return_as_pandas=True)
sched_pd.head()
```

Pipeline next step (filter to playoff games):

```default
import polars as pl
playoffs = load_nba_schedule(seasons=[2023]).filter(pl.col("season_type") == 3)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_loaders.load_nba_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA team boxscore data

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

Quick start:

```default
from sportsdataverse.nba import load_nba_team_boxscore
box = load_nba_team_boxscore(seasons=[2023])
print(box.shape)
```

Pandas round-trip:

```default
box_pd = load_nba_team_boxscore(seasons=[2023], return_as_pandas=True)
box_pd.head()
```

Pipeline next step (compute average team OFF rating):

```default
import polars as pl
avg = (
    load_nba_team_boxscore(seasons=[2023])
    .group_by("team_display_name")
    .agg(pl.col("offensive_rating").mean())
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

## sportsdataverse.nba.nba_pbp module

### sportsdataverse.nba.nba_pbp.espn_nba_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_nba_pbp() - Pull the game by id - Data from API endpoints - nba/playbyplay, nba/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from nba_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”, “pickcenter”, “againstTheSpread”,
    “odds”, “predictor”, “espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start:

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
print(list(pbp.keys()))
```

Pull only the raw ESPN summary payload (skip cleaning):

```default
raw_pbp = espn_nba_pbp(game_id=401585183, raw=True)
```

Pipeline next step (load plays into a polars DataFrame):

```default
import polars as pl
pbp = espn_nba_pbp(game_id=401585183)
plays_df = pl.from_dicts(pbp["plays"])
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA PBP
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_pbp.helper_nba_game_data(pbp_txt, init)

Internal helper that lifts home/away team identification fields from the
ESPN summary payload onto the cleaned `pbp_txt` and `init` dictionaries.

* **Parameters:**
  * **pbp_txt** (*dict*) – ESPN summary payload.
  * **init** (*dict*) – Pickcenter-derived spread / favorite / over-under metadata.
* **Returns:**
  `(pbp_txt, init)` with team-id, mascot, location,
  abbreviation, and alt-name fields populated for both sides.
* **Return type:**
  tuple[dict, dict]

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.helper_nba_pbp(game_id, pbp_txt)

Internal helper that runs the ESPN summary payload through pickcenter,
game-data, and feature pipelines and returns the cleaned dictionary
consumed by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp).

* **Parameters:**
  * **game_id** (*int*) – ESPN game / event identifier.
  * **pbp_txt** (*dict*) – Trimmed ESPN summary payload (already filtered to
    the keys [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp) keeps).
* **Returns:**
  Cleaned game payload with cleaned plays, boxscore, broadcasts,
  odds, etc.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.helper_nba_pbp_features(game_id, pbp_txt, init)

Internal helper that builds the polars play-by-play frame and timeout
metadata from the ESPN summary payload.

Adds clock decomposition (minutes/seconds), per-quarter and per-half
seconds-remaining columns, half/quarter lag-lead helpers, and a per-game
timeout map keyed by team id and half.

* **Parameters:**
  * **game_id** (*int*) – ESPN game / event identifier.
  * **pbp_txt** (*dict*) – ESPN summary payload (with `plays` already lifted).
  * **init** (*dict*) – Output of [`helper_nba_pickcenter()`](#sportsdataverse.nba.nba_pbp.helper_nba_pickcenter) plus team-id
    metadata from [`helper_nba_game_data()`](#sportsdataverse.nba.nba_pbp.helper_nba_game_data).
* **Returns:**
  `pbp_txt` mutated with `plays` (a polars DataFrame) and
  `timeouts` populated.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.helper_nba_pickcenter(pbp_txt)

Internal helper that extracts spread / over-under / home-favorite info
from the ESPN `pickcenter` array.

Falls back to sensible defaults (spread 2.5, OU 215.5, home favorite True,
spread unavailable) when no pickcenter data is present.

* **Parameters:**
  **pbp_txt** (*dict*) – ESPN summary payload.
* **Returns:**
  `{"gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable"}`.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.nba_pbp_disk(game_id, path_to_json)

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads `{path_to_json}/{game_id}.json`.

* **Parameters:**
  * **game_id** (*int*) – ESPN game / event identifier.
  * **path_to_json** (*str*) – Directory containing the cached JSON file.
* **Returns:**
  Parsed JSON contents.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

## sportsdataverse.nba.nba_schedule module

### sportsdataverse.nba.nba_schedule.espn_nba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_calendar - look up the NBA calendar for a given season from ESPN

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

Quick start:

```default
from sportsdataverse.nba import espn_nba_calendar
cal = espn_nba_calendar(season=2023)
print(cal.shape)
```

Use ondays to get every scheduled date for the season:

```default
ondays = espn_nba_calendar(season=2023, ondays=True)
```

Pipeline next step (loop the URLs to scrape day-by-day):

```default
cal = espn_nba_calendar(season=2023, ondays=True)
urls = cal["url"].to_list()  # feed each into espn_nba_schedule
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data

### sportsdataverse.nba.nba_schedule.espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_schedule - look up the NBA schedule for a given date from ESPN

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **season_type** (*int*) – season type, 1 for pre-season, 2 for regular season, 3 for post-season,
  * **all-star** (*4 for*)
  * **off-season** (*5 for*)
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Quick start (today’s slate):

```default
from sportsdataverse.nba import espn_nba_schedule
slate = espn_nba_schedule()
print(slate.shape)
```

Pull a specific date:

```default
jan2 = espn_nba_schedule(dates=20230102, season_type=2)
```

Pipeline next step (extract finals only):

```default
import polars as pl
finals = espn_nba_schedule(dates=20230102).filter(
    pl.col("status_type_completed") == True
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_schedule.most_recent_nba_season()

Return the most recent NBA season year based on today’s date.

The NBA season crosses calendar years – a season started in October of
year Y is reported as season Y+1. If today is in October or later, this
returns next calendar year; otherwise it returns the current calendar year.

* **Returns:**
  The most recent NBA season year (e.g. 2024 for the 2023-24 season).
* **Return type:**
  int

### Example

Quick start:

```default
from sportsdataverse.nba import most_recent_nba_season
year = most_recent_nba_season()
print(year)
```

Combine with the loaders for a “current season” pull:

```default
from sportsdataverse.nba import load_nba_schedule, most_recent_nba_season
sched = load_nba_schedule(seasons=[most_recent_nba_season()])
```

### sportsdataverse.nba.nba_schedule.scoreboard_event_parsing(event)

Internal helper that flattens an ESPN NBA scoreboard event dict into a
shape suitable for `pd.json_normalize`.

* **Parameters:**
  **event** (*dict*) – A single scoreboard `events[*]` entry from the ESPN
  NBA scoreboard API.
* **Returns:**
  The same event dict, mutated in place with `home`/`away`
  copies of the competitors and trimmed of unused link/odds keys.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_schedule()`](#sportsdataverse.nba.nba_schedule.espn_nba_schedule):

```default
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
```

### sportsdataverse.nba.nba_schedule.year_to_season(year)

Convert a season-end year (e.g. 2024) to the NBA’s hyphenated label
(e.g. `"2023-24"`).

Handles century rollover (1999 -> `"1999-00"`) and zero-pads the
second half of the label.

* **Parameters:**
  **year** (*int*) – The starting calendar year of the season (e.g. 2023 for
  the 2023-24 season).
* **Returns:**
  NBA-style season label.
* **Return type:**
  str

### Example

Quick start:

```default
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"
```

Century rollover:

```default
print(year_to_season(1999))  # "1999-00"
```

## sportsdataverse.nba.nba_teams module

### sportsdataverse.nba.nba_teams.espn_nba_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_teams - look up NBA teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.nba.espn_nba_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nba import espn_nba_teams
teams = espn_nba_teams()
print(teams.shape)
```

Pandas round-trip:

```default
teams_pd = espn_nba_teams(return_as_pandas=True)
teams_pd.head()
```

Pipeline next step (build a team_id to abbreviation map):

```default
teams = espn_nba_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA team data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

## Module contents
