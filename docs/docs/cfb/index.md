# sportsdataverse.cfb package

## Submodules

## sportsdataverse.cfb.cfb_espn_ext module

sportsdataverse.cfb.cfb_espn_ext — ESPN endpoint wrappers ported from cfbfastR.

Registers `espn_cfb_*` wrappers via `sportsdataverse._common_espn.make_league_module()`.
~110 functions cover Site v2, Site v2 alt standings, Web v3 athlete + leaders,
Core v2 (league, seasons, athletes, events, catalog), plus NCAA extensions
(rankings, recruits, weekly rankings) and football extensions (QBR by season
and by week).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/awards — awards won by the athlete.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_bio(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/bio — athlete bio.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_bio()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_career_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statistics[/\{type\}]. `type` ∈ \{0=reg, 1=post, 2=career\}.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_contracts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/contracts — contract info.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_contracts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — enriched athlete profile (core v2).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_eventlog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/eventlog — event participation log.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_eventlog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_gamelog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/gamelog?season=\{y\}. **404 for NHL.**

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_gamelog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_gamelog()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\} — athlete profile (site v2 lite shape).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/injuries — per-athlete injuries.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/news — athlete-scoped news.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_athlete_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/notes — analyst notes.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_overview(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/\{sport\}/\{league\}/athletes/\{id\}/overview — rich snapshot.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_overview()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_overview()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_records(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/records — career records.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_records()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/seasons — seasons played.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_splits(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/splits?season=\{y\} — situational splits.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_splits()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_splits()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_statisticslog(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/statisticslog — game-by-game log (NHL gamelog replacement).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_statisticslog()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_stats(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/athletes/\{id\}/stats?season=\{y\} — parallel-array stats.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._espn_athlete_stats()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_athlete_stats()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athlete_vs_athlete(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes/\{id\}/vsathlete/\{oid\} — head-to-head.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athlete_vsathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_athletes_index(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /athletes?active=\{bool\}&limit=\{n\}&page=\{p\} — paginated athletes index.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_athletes_index()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_award(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards/\{id\} — single award detail.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_award()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /awards — league award catalog.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_calendar(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar — full season calendar.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_calendar_offseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/offseason.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_offseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_calendar_ondays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/ondays — dates with games.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_ondays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_calendar_postseason(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/postseason.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_postseason()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_calendar_regular_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /calendar/regular-season — week-by-week regular season ranges.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_calendar_regular_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_coach(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\} — single coach.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_coach_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/record/\{type\} — coaching record.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_coach_season(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches/\{id\}/seasons/\{y\} — coach’s per-season record.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_coach_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /coaches — coaches index. **Often 404s — prefer /seasons/\{y\}/coaches.**

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_conferences(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /groups — conferences and divisions.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_groups()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /draft — draft board (varies per sport).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\} — event root.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_broadcasts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/broadcasts — TV/streaming broadcasters.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_broadcasts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competition(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\} — competition (cid defaults to event_id).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competition()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors/\{tid\} — single competitor.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitor_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/leaders — per-team game leaders.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitor_linescores(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/linescores — per-period scores.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_linescores()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_linescores()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitor_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/record — competitor record at game-time.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitor_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/roster — competitor roster for one game.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitor_statistics(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/competitors/\{tid\}/statistics — team game statistics.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitor_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_competitor_statistics()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_competitors(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/competitors — both teams’ refs.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_competitors()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/leaders — per-game leaders.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_odds(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/odds — game odds.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_odds()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_official_detail(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials/\{oid\} — single official detail.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_official_detail()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_officials(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/officials — referees/umpires.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_officials()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_play(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays/\{pid\} — single play detail.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_play_personnel(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/plays/\{pid\}/personnel — personnel on the play.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_play_personnel()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_plays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/plays — raw plays for one game.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_plays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_event_plays()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/powerindex — power index for the game.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_predictor(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/predictor — ESPN game predictor.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_predictor()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_probabilities(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/competitions/\{cid\}/probabilities — per-play WP timeline.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_probabilities()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_propbets(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/propbets — prop bet markets.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_propbets()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_scoringplays(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/scoringplays — scoring summary.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_scoringplays()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_situation(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/situation — current in-game state.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_situation()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_event_status(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events/\{id\}/…/status — current event status.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_event_status()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /events?dates=\{d\} — paginated events index.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_franchise(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises/\{id\} — single franchise.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchise()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_franchises(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /franchises — franchise list.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_franchises()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /injuries — league-wide injury report.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET \{WEB_V3\}/…/statistics/byathlete — ranked leaderboard with glossary.

`category` is optional: when omitted the URL is built without
`?category=...` and ESPN returns the league-default leader set,
which is the shape the cross-league `espn_<league>_leaders()`
callers (and `parse_leaders`) expect.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._espn_statistics_byathlete()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_leaders()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_leaders_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leaders — league-wide statistical leaders (core v2).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_league_notes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /notes — league-level editorial notes (sparse; NFL crawler discovery).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_notes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_league_root(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\} — league root.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_league_root()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /news — league-wide news.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_position(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions/\{id\} — single position.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_position()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_positions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /positions — position definitions.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_positions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_rankings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /rankings — poll rankings (NCAA leagues only).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_rankings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_scoreboard(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /scoreboard. `dates`: YYYYMMDD or YYYYMMDD-YYYYMMDD or season year.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_scoreboard()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_scoreboard()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_athletes(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/athletes — athletes active in a season.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_athletes()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_awards(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/awards — awards given in a season.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_awards()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_coaches(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/coaches — coaches active in a season.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_coaches()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_coaches()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_draft(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft — draft board for a year.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_draft()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_draft_round_picks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/draft/rounds/\{r\}/picks — per-round picks.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_draft_round_picks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_freeagents(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/freeagents — UFA/RFA list (where applicable).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_freeagents()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_futures(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/futures — futures odds.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_futures()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_group(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\} — single group within season-type.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_group_children(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/children — sub-groups (divisions inside conf).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_children()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_group_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/teams — teams in a group.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_group_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_groups(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups — conferences/divisions within season-type.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_groups()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_info(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\} — single-season root.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_pointer(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/season — current-season pointer.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_pointer()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_powerindex(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex[/\{teamId\}] — BPI/FPI/SP+. Per-team when `team_id`.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_powerindex_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/powerindex/leaders — power-index leaderboard.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_powerindex_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_qbr(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/groups/\{g\}/qbr/\{split\} — Total QBR (NFL/CFB).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_qbr()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_qbr_week(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/qbr/\{split\} — per-week QBR.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_qbr_week()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_recruits(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/recruits — NCAA recruiting (CFB / MBB / WBB).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_recruits()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams/\{id\} — team-in-a-season profile.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_teams(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/teams — teams active in a season.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_type(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\} — season-type root.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_type_corrections(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/corrections — stat-correction audit trail.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_corrections()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_type_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/leaders — per-season-type leaders.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_types(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types — season-type list (1=pre, 2=reg, 3=post, 4=off/all-star).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_types()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_week(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\} — single-week root.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_week_events(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/events — week-scoped events.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week_events()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_week_rankings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks/\{w\}/rankings — weekly polls (NCAA/CFB).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_week_rankings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_season_weeks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /seasons/\{y\}/types/\{t\}/weeks — weeks within a season-type (NFL/CFB).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_season_type_weeks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_seasons(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /leagues/\{league\}/seasons — paginated season list.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_seasons()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_standings(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /apis/v2/sports/\{sport\}/\{league\}/standings — full standings (not the stub).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_alt_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_standings_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /standings — league standings (core v2 form).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_standings()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_standings()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_statistics_league(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /statistics — league statistical leaders (site-v2 variant).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_statistics()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_summary(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /summary?event=\{id\} — comprehensive game summary (boxscore + plays + leaders).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_summary()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_summary()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_talentpicks(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /talentpicks — ESPN editorial talent picks (sparse; NFL crawler discovery).

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_talentpicks()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — single team detail.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\} — enriched team.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_team()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_depthcharts(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/depthcharts — depth chart by position.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_depthcharts()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_history(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/history — franchise historical record.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_history()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_injuries(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/injuries — team injury report.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_injuries()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_injuries()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_leaders(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/leaders — team statistical leaders.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_leaders()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_news(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/news — team-scoped news.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_news()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_news()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_record(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/record — team win/loss record.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_record()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_roster(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/roster — team roster.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_roster()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_roster()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_schedule(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/schedule — team schedule for a season.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_schedule()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_team_schedule()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_team_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams/\{id\}/transactions — recent team transactions.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_team_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_teams_core(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — paginated teams catalog.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_teams_site(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /teams — all teams.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_teams()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_teams()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_tournaments(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /tournaments — tournament list.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_tournaments()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_transactions(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /transactions — league-wide transactions.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._site_v2_transactions()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_venue(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues/\{id\} — single venue detail.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_venue()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_single_entity()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

### sportsdataverse.cfb.cfb_espn_ext.espn_cfb_venues(\*args, return_parsed: bool = False, return_as_pandas: bool = False, \*\*kwargs)

GET /venues — stadiums/arenas.

Bound to `sport='football'`, `league='college-football'`. Core implementation: `sportsdataverse._common_espn._core_v2_venues()`.

Pass `return_parsed=True` to dispatch the raw response through `sportsdataverse._common_espn_parsers.parse_items()` and return a polars DataFrame (or pandas via `return_as_pandas=True`).

## sportsdataverse.cfb.cfb_game_rosters module

### sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_cfb_schedule().
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

Quick start:

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)
```

Pandas round-trip:

```default
rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()
```

Pipeline next step (filter to game starters):

```default
import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB rosters
  * [recruitR](https://github.com/sportsdataverse/recruitR) – recruiting data companion

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_athlete_items(teams_rosters, \*\*kwargs)

Internal helper that resolves each athlete `$ref` in a team-rosters frame
to the canonical athlete detail row.

* **Parameters:**
  * **teams_rosters** (*pl.DataFrame*) – Output of [`helper_cfb_roster_items()`](#sportsdataverse.cfb.cfb_game_rosters.helper_cfb_roster_items)
    (must contain an `athlete_href` column).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per resolved athlete.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items(summary)

Internal helper that flattens the ESPN `competitions/competitors` summary
payload into a polars DataFrame keyed by `team_id`.

* **Parameters:**
  **summary** (*dict*) – Parsed JSON from the ESPN competitors summary endpoint.
* **Returns:**
  Polars dataframe with one row per competitor team in the game.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_roster_items(items, summary_url, \*\*kwargs)

Internal helper that fetches the roster entries for every team in a game.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_cfb_game_items()`](#sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items).
  * **summary_url** (*str*) – Base ESPN summary URL used to derive each team’s
    roster endpoint.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per game-roster entry across both teams.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_team_items(items, \*\*kwargs)

Internal helper that fetches team detail rows for every team referenced in
the competitors summary and returns them as a flat polars DataFrame.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_cfb_game_items()`](#sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  Team detail rows with logo URLs flattened out.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

## sportsdataverse.cfb.cfb_loaders module

### sportsdataverse.cfb.cfb_loaders.get_cfb_teams(return_as_pandas=False) → DataFrame

Load college football team ID information and logos

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.cfb import get_cfb_teams
teams = get_cfb_teams()
print(teams.shape)
```

Pandas round-trip:

```default
teams_pd = get_cfb_teams(return_as_pandas=True)
teams_pd.head()
```

Pipeline next step (build a team_id to logo URL map):

```default
teams = get_cfb_teams()
logo_map = dict(zip(teams["team_id"], teams["logo"]))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB team metadata

### sportsdataverse.cfb.cfb_loaders.load_cfb_betting_lines(return_as_pandas=False) → DataFrame

Load college football betting lines information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing betting lines available for the available seasons.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)
```

Pandas round-trip:

```default
lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()
```

Pipeline next step (filter to one provider in 2023):

```default
import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB betting lines
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_loaders.load_cfb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load college football play by play data going back to 2003

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2003 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2003.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_pbp
pbp = load_cfb_pbp(seasons=[2023])
print(pbp.shape)
```

Multi-season pull as pandas:

```default
pbp_pd = load_cfb_pbp(seasons=range(2020, 2024), return_as_pandas=True)
pbp_pd.head()
```

Pipeline next step (filter to rushing plays):

```default
import polars as pl
rushes = load_cfb_pbp(seasons=[2023]).filter(pl.col("rush") == True)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_loaders.load_cfb_rosters(seasons: List[int], return_as_pandas=False) → DataFrame

Load roster data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2014 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing rosters available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2014.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_rosters
rosters = load_cfb_rosters(seasons=[2023])
print(rosters.shape)
```

Pandas round-trip:

```default
rosters_pd = load_cfb_rosters(seasons=[2023], return_as_pandas=True)
rosters_pd.head()
```

Pipeline next step (count quarterbacks per team):

```default
import polars as pl
qbs = (
    load_cfb_rosters(seasons=[2023])
    .filter(pl.col("position").eq("QB"))
    .group_by("team")
    .len()
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB rosters
  * [recruitR](https://github.com/sportsdataverse/recruitR) – recruiting data companion

### sportsdataverse.cfb.cfb_loaders.load_cfb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load college football schedule data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the schedule for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_schedule
sched = load_cfb_schedule(seasons=[2023])
print(sched.shape)
```

Multi-season pull as pandas:

```default
sched_pd = load_cfb_schedule(seasons=range(2020, 2024), return_as_pandas=True)
sched_pd.head()
```

Pipeline next step (extract bowl games):

```default
import polars as pl
bowls = load_cfb_schedule(seasons=[2023]).filter(pl.col("season_type") == 3)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB schedules
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_loaders.load_cfb_team_info(seasons: List[int], return_as_pandas=False) → DataFrame

Load college football team info

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the team info available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_team_info
teams = load_cfb_team_info(seasons=[2023])
print(teams.shape)
```

Pandas round-trip:

```default
teams_pd = load_cfb_team_info(seasons=[2023], return_as_pandas=True)
teams_pd.head()
```

Pipeline next step (join team info onto schedule):

```default
from sportsdataverse.cfb import load_cfb_schedule
sched = load_cfb_schedule(seasons=[2023])
teams = load_cfb_team_info(seasons=[2023])
enriched = sched.join(teams, left_on="home_id", right_on="team_id", how="left")
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB team data
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

## sportsdataverse.cfb.cfb_pbp module

### *class* sportsdataverse.cfb.cfb_pbp.CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, odds_override=None, \*\*kwargs)

Bases: `object`

#### \_\_helper_\_espn_cfb_odds_information_\_()

Fetch pre-game spread/total from ESPN’s modern core odds endpoint.

Returns `(gameSpread, overUnder, homeFavorite, gameSpreadAvailable)`.
ESPN emptied the legacy `pickcenter` array on the summary endpoint
for 2024+ college games; this helper restores the data path for those
games via the `sports.core.api.espn.com` v2 odds collection. Falls
back to defaults `(2.5, 55.5, True, False)` when the endpoint
returns no items, errors out, or the JSON cannot be decoded —
preserving the legacy caller-visible behavior on those failure
paths.

#### \_\_init_\_(gameId=0, raw=False, path_to_json='/', return_keys=None, odds_override=None, \*\*kwargs)

CFBPlayProcess.

* **Parameters:**
  * **gameId** – ESPN game id.
  * **raw** – if True, espn_cfb_pbp() returns the (allowlisted) summary verbatim.
  * **path_to_json** – directory for cfb_pbp_disk() offline loads.
  * **return_keys** – optional subset of result keys to return.
  * **odds_override** – optional dict \{gameSpread, overUnder, homeFavorite,
    gameSpreadAvailable\} that short-circuits odds resolution (sets
    odds_source=”injected”) so offline rebuilds never hit the live
    core-odds endpoint or fall back to defaults. Validated + coerced here.

#### odds_source

provenance of the resolved spread —
“summary_pickcenter” | “core_odds_api” | “default” | “injected”.

#### cfb_pbp_disk()

Load a previously cached ESPN summary JSON for this game from disk.

Reads `{path_to_json}/{gameId}.json` where `path_to_json` was passed
to the [`CFBPlayProcess`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess) constructor.

* **Returns:**
  Parsed JSON contents, also stored on `self.json`.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334, path_to_json="./cache")
pbp = game.cfb_pbp_disk()
print(list(pbp.keys()))
```

#### cfb_pbp_json(\*\*kwargs)

Return the JSON payload currently attached to this [`CFBPlayProcess`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess)
instance.

* **Returns:**
  The cached JSON payload (`self.json`).
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cached = game.cfb_pbp_json()
```

#### corrupt_pbp_check()

Heuristic check for corrupt or incomplete play-by-play.

Flags games with zero plays, fewer than 50 plays for a completed game,
or more than 500 plays for a completed game – all of which historically
indicate ESPN delivered a malformed PBP payload that should not be
processed downstream.

* **Returns:**
  True if PBP looks corrupt and the processing pipeline should
  be skipped, False otherwise.
* **Return type:**
  bool

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
if not game.corrupt_pbp_check():
    game.run_processing_pipeline()
```

#### create_box_score(play_df)

Build a per-team and per-player advanced box score from a processed
plays frame.

Triggers [`run_processing_pipeline()`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline) first if it hasn’t already run,
so the input `play_df` is expected to be the post-pipeline plays frame.

* **Parameters:**
  **play_df** (*pl.DataFrame*) – The plays frame produced by
  [`run_processing_pipeline()`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline) (with EPA, WPA and play-type
  flags already populated).
* **Returns:**
  Box-score sections keyed by `"passing"`, `"rushing"`,
  `"receiving"`, `"defensive"`, `"turnover"`, and `"drives"`.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
box = game.create_box_score(game.plays_json)
print(list(box.keys()))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package

#### espn_cfb_pbp(\*\*kwargs)

espn_cfb_pbp() - Pull the game by id. Data from API endpoints: college-football/playbyplay,
college-football/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from cfb_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a
  * **datasets.** (*cleaned dictionary of*)
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
    “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
    “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
pbp = game.espn_cfb_pbp()
print(list(pbp.keys()))
```

Pull only the raw ESPN summary payload (skip cleaning):

```default
raw_pbp = CFBPlayProcess(gameId=401628334, raw=True).espn_cfb_pbp()
```

Pipeline next step (run the full processing pipeline for advanced features):

```default
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()  # adds EPA, WPA, box score
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

#### gameId *= 0*

#### path_to_json *= '/'*

#### ran_cleaning_pipeline *= False*

#### ran_pipeline *= False*

#### raw *= False*

#### return_keys *= None*

#### run_cleaning_pipeline()

Run the lighter cleaning pipeline (no EPA/WPA/QBR/box-score).

Same per-play feature engineering as [`run_processing_pipeline()`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline)
through `__add_spread_time`, but stops short of the modeling steps.
Use this when you only need cleaned plays and don’t need expected
points or win probability columns.

* **Returns:**
  Cleaned game payload (no `advBoxScore` key).
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cleaned = game.run_cleaning_pipeline()
print(len(cleaned["plays"]))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP

#### run_processing_pipeline()

Run the full play-by-play processing pipeline.

Applies every scoring/feature step in order: down detection, play type
flags, rush/pass flags, team score variables, new play types, penalty
setup, play category flags, yardage cols, player cols, after cols,
spread time, EPA, WPA, drive data, and QBR. Also produces an advanced
box score and stores it under `advBoxScore` on the returned dict.

Idempotent – subsequent calls return the cached `self.json`.

* **Returns:**
  The fully-processed game payload. If the constructor was
  given `return_keys`, only those keys are returned.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
print(processed["advBoxScore"].keys())
```

Pipeline next step (return only selected keys):

```default
game = CFBPlayProcess(gameId=401628334, return_keys=["plays", "advBoxScore"])
game.espn_cfb_pbp()
trimmed = game.run_processing_pipeline()
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP

## sportsdataverse.cfb.cfb_play_participants module

ESPN college-football play-participants scraper.

Single ESPN endpoint:
: sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/\{game_id\}/competitions/\{game_id\}/plays?limit=1000

ESPN’s per-play `participants[]` array is the authoritative source for which
athletes were involved in each play (passer, rusher, receiver, tackler, etc.).
This wrapper pulls the full play-list for a game, extracts the participants,
resolves each `$ref` URL into an `athlete_id` / `position_id`, attaches
the per-athlete display name from a sibling roster lookup, and pivots the
result so each play has one row keyed by `play_id` with the participant
display name and id materialized as `{type}_player_name` /
`{type}_player_id` columns (e.g. `passer_player_name`).

Designed to replace the regex-based player-name extraction the
`cfb_pbp.CFBPlayProcess.__add_player_cols` method previously did against
the freeform `text` column. Coverage was probed back to season 2014 (the
earliest season with reliable ESPN CFB PBP coverage) and is solid for every
sampled era — see the project diff doc for the probe table.

Caveats:

* `$ref` URLs are parsed for the athlete/position id (the trailing numeric
  segment). The full `$ref` URL is also retained so the optional
  `resolve_missing` pass can fetch any athlete the sidecar omitted.
* Display names come primarily from the `cdn.espn.com/.../playbyplay`
  sidecar (the same one the legacy class uses). The sidecar is one round
  trip for the whole roster, but it is built from the box-score side and
  occasionally omits athletes who appear only in the participants payload
  (split sacks where the second sacker isn’t on the leaders list, returners
  on lateral plays, etc.). When `resolve_missing=True` (the default),
  athletes still missing a name after the sidecar pass are fetched
  one-by-one from their canonical `$ref` URL and the names backfilled
  before the pivot. The fan-out is capped per game (default 50) so a
  pathological game can’t run away.
* Pagination: the endpoint historically caps at one page of 1000 plays per
  game. We follow the `pageCount` cursor defensively in case ESPN ever
  changes that.

### sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[True], return_as_pandas: bool = False, resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → DataFrame

### sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → DataFrame

Pull ESPN per-play participants for a college-football game.

* **Parameters:**
  * **game_id** – ESPN game / event identifier.
  * **raw** – If True, returns the raw list of play-items dicts (after
    following pagination) before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **resolve_missing** – If True (default), athletes that the
    `cdn.espn.com` sidecar omits are fetched one-by-one from
    their canonical ESPN `$ref` URL so the resulting frame has
    populated `*_player_name` / `*_player_names` columns
    wherever an `*_player_id` is non-null. Setting this to
    False skips the extra HTTP fan-out and reproduces the
    pre-enhancement behavior — rows may then ship with
    `*_player_id` populated but `*_player_name` null on the
    handful of athletes the sidecar misses (most visible on
    split sacks, multi-lateral returns, and older games).
  * **resolve_missing_max** – Hard cap on the number of per-athlete
    `$ref` requests issued for a single game. Defaults to 50,
    which comfortably covers every probed game (typical max is
    ≤8 unique missing athletes). If breached, a warning is
    logged and the remaining missing athletes are left with
    null names. Ignored when `resolve_missing=False`.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame, one row per play. Columns include
  `game_id`, `play_id`, and TWO column families for every
  participant `type` ESPN ships for the game (typical types:
  `passer`, `rusher`, `receiver`, `tackler`, `sacked_by`,
  `forced_by`, `pass_defender`, `kicker`, `punter`,
  `returner`, `recoverer`, `scorer`, `pat_scorer`,
  `penalized`, `assisted_by`):
  * **Scalar** — `{type}_player_id` / `{type}_player_name`: the
    first occurrence of that participant type on the play. Backwards
    compatible with the legacy regex-extractor shape.
  * **List** — `{type}_player_ids` / `{type}_player_names`:
    `List(Utf8)` columns containing **every** occurrence of that
    participant type on the play, in the order ESPN shipped them.
    Plays with no participant of a given type carry an empty list
    `[]` (not null) for downstream consumption simplicity. This
    family preserves multi-entry participant types (split sacks
    where ESPN ships two `sackedBy` entries, multi-tacklers,
    etc.) that the scalar family collapses to first-only.

  If `raw=True`, returns the parsed JSON list of play dicts.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Quick start:

```default
from sportsdataverse.cfb import espn_cfb_play_participants
participants = espn_cfb_play_participants(game_id=401628334)
print(participants.shape)
```

Skip the per-athlete fan-out for speed:

```default
participants_fast = espn_cfb_play_participants(
    game_id=401628334,
    resolve_missing=False,
)
```

Pipeline next step (join onto play-by-play frame):

```default
from sportsdataverse.cfb import CFBPlayProcess
pbp = CFBPlayProcess(gameId=401628334).espn_cfb_pbp()
plays = pbp["plays"]
joined = plays.join(participants, how="left", left_on="id", right_on="play_id")
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

## sportsdataverse.cfb.cfb_schedule module

### sportsdataverse.cfb.cfb_schedule.espn_cfb_calendar(season=None, groups=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_calendar - look up the men’s college football calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **groups** (*int*) – Used to define different divisions. 80 is FBS, 81 is FCS.
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
from sportsdataverse.cfb import espn_cfb_calendar
cal = espn_cfb_calendar(season=2023)
print(cal.shape)
```

Use ondays to get every scheduled date for the season:

```default
ondays = espn_cfb_calendar(season=2023, ondays=True)
```

Pipeline next step (loop the URLs to scrape day-by-day):

```default
cal = espn_cfb_calendar(season=2023, ondays=True)
urls = cal["url"].to_list()  # feed each into espn_cfb_schedule
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB schedules

### sportsdataverse.cfb.cfb_schedule.espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_schedule - look up the college football schedule for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **week** (*int*) – Week of the schedule.
  * **groups** (*int*) – Used to define different divisions. 80 is FBS, 81 is FCS.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Quick start (today’s slate):

```default
from sportsdataverse.cfb import espn_cfb_schedule
slate = espn_cfb_schedule()
print(slate.shape if slate is not None else "no games")
```

Pull a specific week of FBS games:

```default
week5 = espn_cfb_schedule(dates=2023, week=5, season_type=2)
```

Pipeline next step (extract finals only):

```default
import polars as pl
finals = espn_cfb_schedule(dates=2023, week=5).filter(
    pl.col("status_type_completed") == True
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB schedules
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_schedule.most_recent_cfb_season()

Return the most recent college football season year based on today’s date.

The college football season starts in mid-August. If today is on or after
August 15 (or any day in September or later), this returns the current
calendar year. Otherwise, it returns the previous calendar year.

* **Returns:**
  The most recent CFB season year.
* **Return type:**
  int

### Example

Quick start:

```default
from sportsdataverse.cfb import most_recent_cfb_season
year = most_recent_cfb_season()
print(year)
```

Combine with the loaders for a “current season” pull:

```default
from sportsdataverse.cfb import load_cfb_schedule, most_recent_cfb_season
sched = load_cfb_schedule(seasons=[most_recent_cfb_season()])
```

### sportsdataverse.cfb.cfb_schedule.scoreboard_event_parsing(event)

Internal helper that flattens an ESPN scoreboard event dict into a shape
suitable for `pd.json_normalize`.

* **Parameters:**
  **event** (*dict*) – A single scoreboard `events[*]` entry from the ESPN
  college-football scoreboard API.
* **Returns:**
  The same event dict, mutated in place with `home`/`away`
  copies of the competitors and trimmed of unused link/odds keys.
* **Return type:**
  dict

### Example

Used internally by [`espn_cfb_schedule()`](#sportsdataverse.cfb.cfb_schedule.espn_cfb_schedule):

```default
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```

## sportsdataverse.cfb.cfb_teams module

### sportsdataverse.cfb.cfb_teams.espn_cfb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_teams - look up the college football teams

* **Parameters:**
  * **groups** (*int*) – Used to define different divisions. 80 is FBS, 81 is FCS.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.cfb.espn_cfb_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Quick start (FBS only by default):

```default
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)
```

Pull FCS teams (group 81):

```default
fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()
```

Pipeline next step (build an abbreviation lookup):

```default
teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB team data
  * [recruitR](https://github.com/sportsdataverse/recruitR) – recruiting data companion

## sportsdataverse.cfb.model_vars module

## Module contents
