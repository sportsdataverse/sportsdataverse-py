---
title: Unreleased changes
---

# Unreleased changes

Merged to `main` since 0.1.4 and not yet released. Released versions are on the [Changelog](/CHANGELOG).

## Unreleased

### Fixed — a failed flat-API fetch raises instead of returning the error body (BREAKING)

`_codegen_runtime._get`, the getter behind every generated wrapper of the 30 `espn_*`
league families, `fox_api_*`, `cbs_*` (NAPI), `mlb_api_*`, the four NHL families
(`nhl_web_*`, `nhl_edge_*`, `nhl_stats_*`, `nhl_records_*`), `asa_*`, and the hand-written
`fox_cfb_*`, `yahoo_cfb_*` and CFB crosswalk helpers, returned whatever came back. A
401/403/429/5xx with a JSON body was handed over as if it were the payload (Fox's
`{"fault": {"faultstring": "Invalid ApiKey"}}`, ESPN's `{"code": 400, ...}`); a non-JSON
or empty answer became `{}`. Parsed, both were a zero-row frame: a failed fetch read as
"no data". It now follows the package error vocabulary:

| Answer | Before | Now |
|---|---|---|
| 2xx with a JSON body | the body | the body (unchanged) |
| 204 / 205 | `{}` | `{}` (no content by definition) |
| 200 with an empty body | `{}` | `AssetFetchError` (barttorvik's block, pro.nfl.com's rejected params and stats-host throttling all answer this way) |
| 404, or ESPN 200 with `{"code": 404}` | `NoDataError` | `NoDataError` (unchanged) |
| 400 / 422 | the error body as data | `ValueError`: the request is wrong, retrying cannot help |
| 401 / 403 / 429 / 5xx after retries, any other non-2xx | the error body as data | `AssetFetchError` |
| 2xx with a non-JSON body | `{}` | `AssetFetchError` |
| connection failure (timeout, reset, DNS) after retries | a raw `requests` exception | `AssetFetchError`, chained to it |

Every message names host, path and status plus a bounded excerpt of the body, never the
query string (API keys travel there, including in ESPN `$ref` links) and with credentials
redacted; the non-JSON case is raised outside the decode handler, so the `JSONDecodeError`
(whose `.doc` is the whole body) is not chained. The same rule now applies to the other
runtime getters: `mlb_statcast_*` (and the player page behind `mlb_statcast_player`),
`torvik_*` / `bart_wbb` and `kenpom_*` (an error page was returned as CSV/HTML text), the MLS
and NWSL stats-API wrappers (`mls_*`, `nwsl_*`), the Yahoo shangrila wrappers (`yahoo_*`,
whose HTTP 400 `{"errors": [...]}` for a bad persisted query is now a `ValueError`),
`on3_*`, `sports247_*` (RDB and site pages), the LEGACY `pff_*` premium wrappers,
`pff_api_*` (connection failures), `nhl_scoreboard`, the hand-written `nhl_records_*`
helpers and the `mlb_api_extra` helpers. Specifically:

- `on3_*` and `yahoo_*` answered a 404 with `{}`; it is now `NoDataError`. On3's Next.js
  data route still treats its FIRST 404 as "the buildId rotated" and refreshes once; a
  second 404, or an unchanged buildId, raises `NoDataError`. A 2xx On3 page with no
  buildId (a bot-challenge interstitial) raises `AssetFetchError` instead of returning `{}`.
- `nfl_api_*` raised a bare `requests.HTTPError` (or `JSONDecodeError`); it now raises
  `AssetFetchError` / `ValueError` / `NoDataError`. Code that caught `HTTPError` or read
  `exc.response` breaks: read the status from the message, or catch the new types.
- The CFB crosswalks (`cfb_schedule_crosswalk`, through its ESPN calendar/schedule, Fox and
  Yahoo legs) swallowed every exception into an empty leg, recording a failed fetch as "no
  games". They now use the basketball crosswalk's `FetchTally`: a 404 is an answered empty
  item, an isolated failed week is skipped and logged once, and a leg where nothing
  answered raises `CrosswalkSourceError`. A failed ESPN calendar still falls back to the
  default week slots. `fox_cfb_schedule` keeps raising on one bad segment: a partial
  season must not look complete.

The generated docstrings name `NoDataError`, `ValueError` and `AssetFetchError` under
`Raises:` (the stale `requests.exceptions.RequestException` lines are gone).
`nba_stats_*` / `wnba_stats_*` are unchanged here (a separate change); `nflpro_*` keeps
its own errors.

Migration: code that relied on an empty frame to keep a loop going should catch the error,
e.g. `except AssetFetchError: log_and_retry_later()`; `except NoDataError` keeps skipping
genuinely absent resources. `except SportsDataverseError` catches both, timeouts included.
A `ValueError` means the call itself needs fixing.

### Fixed — PFF time to throw, aimed passes and receiving positive-EPA descriptions

The return tables of the legacy `pff_*` passing and receiving reports, and the `pff_api` position
and team reports, described three PFF stats wrongly. These are the same texts that #689 corrected
for the `pff_api` player summaries. Each of the 210 descriptions was checked against real nfl and
ncaa rows:

- `avg_time_to_throw` and every `*_avg_time_to_throw` is per dropback (`ttt_total_time / dropbacks`
  on all 74 rows that carry both, 41 of them with dropbacks different from attempts), not "on the
  passer's attempts".
- `aimed_passes` and every `*_aimed_passes` also excludes batted passes and throws made while hit:
  `attempts − throwaways − spikes − bats − hit_as_threw` on 478 of 478 rows. The old formula
  matched 331.
- Receiving `positive_epa_percent` and its depth, concept and scheme splits are a share of the
  receiver's plays with an EPA value, in practice their routes run, not of their targets. The
  published percentage is a whole number of plays out of routes run on 83 of 85 rows, but out of
  targets on only 21 of 76.

Text only; no column or value changes.

### Security — a credential in a query string no longer reaches a log or an error message

`dl_utils.download` wrote the request URL and its `params` dict into its retry and failure
log lines, and the `NoDataError` a 404 raises quoted the URL. Any key sent in the query
string went with them: The Odds API's private, paid `apiKey`, the HockeyTech `key`, and the
Fox `apikey`. A connection failure was re-raised as requests' own exception, which quotes
the request path and chains urllib3's `MaxRetryError`, which quotes it again.

- The value of a credential pair now reads `REDACTED` in every log line `download` writes,
  in every sportsdataverse error message, in urllib3's request, retry, redirect and
  header-parse log lines (including a caller's own `Retry` adapter), and in the re-raised transport
  exception and every exception chained to it. That covers the exception's attributes as
  well as its text: `err.url`, `err.request.url` and urllib3's pickled message are
  redacted, and the request's `Authorization` / `Proxy-Authorization` / `Cookie` headers
  are dropped. Covered names are
  `apiKey` / `api_key` / `apikey`, `token` / `access_token`, `password`, `secret` /
  `client_secret` and the rest of the sportsdataverse-js list, case-insensitively and in
  URL-encoded and `'name': 'value'` form; a quoted value is redacted up to its closing
  quote, spaces included. A bare `key` is redacted when its value has 16 or
  more characters, as every HockeyTech key does. The host, path, status and other params
  are kept, so the line still says which request failed.
- The Odds API wrappers raise `AssetFetchError` on a non-2xx answer: a rejected key (401),
  spent quota (429), or a 5xx that outlived the retries. Before, the error body came back
  as if it were odds. A 2xx with a non-JSON body raises `AssetFetchError` too, instead of
  a bare `JSONDecodeError`. A 404 still raises `NoDataError`. `cfb_odds_events_crosswalk`
  reads The Odds API, so it now raises these too, where it used to parse the error body as
  an event list.

No signature changes. One behaviour change: the `args[0]` of a re-raised requests
`ConnectionError` is now its redacted message string, not urllib3's `MaxRetryError`
object; that object is still chained as `__context__`. `tests/test_credential_redaction.py` sends a synthetic key through
each of the three providers on a 404, a 503 and a connection failure, and checks the
message, `str`, `repr`, the formatted traceback with its chained causes, and the captured
logs.

### Fixed — nba_stats / wnba_stats defaults: a season where the API needs one, each league's own ids

The `nba_stats_*` / `wnba_stats_*` defaults are mined from hoopR / wehoop through the
sdv-internal-refs catalog, and three things were lost on the way. Called with their defaults, 67
of 128 NBA and 65 of 111 WNBA wrappers returned data before this change; all 128 and all 111 do
now (live sweep through the proxy pool, 2026-10-05; no wrapper went from working to broken).

- **Season.** hoopR's default is a call (`year_to_season(...)`), and the catalog dropped it, so
  `season` defaulted to `None` and the request went out without a `Season`. stats.nba.com answers
  that with an empty HTTP 500, which these wrappers returned as an empty frame with no error.
  51 NBA and 42 WNBA wrappers failed this way, among them `playergamelogs`, `playergamelog`,
  `teamgamelogs`, `commonteamroster`, `commonallplayers`, `leaguedashplayerstats` and
  `leaguestandingsv3`; `synergyplaytypes`, the draft-combine family, `cumestats*`,
  `videodetailsasset`, `commonplayoffseries` and WNBA `playercompare` need theirs too.
  These arguments now default to the **previous season**, resolved at call time: `"2025-26"`
  for NBA, G League and Summer League from October 2026, and `"2025"` for WNBA during 2026
  (wehoop's own `most_recent_wnba_season() - 1`). The previous season always has data; a
  current-season default returned empty frames in the preseason. An explicit value, including
  `""`, is sent as given.
- **Endpoints that work without a season keep the API's default.** For `drafthistory`,
  `leaguegamefinder`, `playergamestreakfinder`, `playercareerbycollegerollup` and
  `shotchartdetail` that default is every season, which a season default would silently narrow.
  The 30 NBA and 25 WNBA endpoints the sweep measured this way are listed in
  `tools/codegen/gen_nba_stats.py`.
- **Each league's own ids.** The catalog kept one example per argument and let wehoop's overwrite
  hoopR's, so NBA wrappers defaulted to WNBA games, teams and players. `nba_stats_teaminfocommon()`
  asked for a WNBA team and got HTTP 500, and every NBA box-score wrapper defaulted to a WNBA
  game. NBA wrappers now take hoopR's examples and WNBA wrappers wehoop's, and an id that only the
  other league's package sets is left out rather than borrowed: WNBA `boxscorehustlev2` /
  `hustlestatsboxscore` used to fetch an NBA game, and `playerdashptshotdefend` defaulted to LeBron
  James (without a player it now returns the league-wide table). `playercompare` gains its
  player-id lists, and WNBA `playbyplayv2` sends wehoop's `StartPeriod` / `EndPeriod` (it was
  HTTP 500 without).
- **Empty results now warn.** When stats.nba.com / stats.wnba.com answer a non-200 status, a
  blank body or an empty object, the wrappers still return `{}` / an empty frame (pipelines rely
  on that for routine misses), but they now warn `sportsdataverse.errors.EmptyResponseWarning`
  with the URL and status. Silence it with
  `warnings.filterwarnings("ignore", category=EmptyResponseWarning)`.
- **The vendored catalog is a plain copy again.** `tools/codegen/inputs/nba_canonical_catalog.json`
  had drifted from sdv-internal-refs through edits made only here (#391's video endpoints, older
  statuses). sdv-internal-refs now classifies the video envelope itself, so the file is copied
  verbatim; the generated wrappers are unchanged by the copy.
- stats.wnba.com answers `draftcombinestats` with the NBA draft combine; wehoop has deprecated its
  draft-combine wrappers.

### Fixed — returns tables no longer cite R-only arguments

Column descriptions mined from hoopR / wehoop said "`team_detail = TRUE` only" (also
`athlete_detail`, `position_detail`) for columns that the R wrappers add behind an argument. The
Python parsers always return those columns and have no such argument, so the condition is dropped
from 131 descriptions.

### Fixed — a failed HockeyTech fetch raises instead of returning an empty frame (BREAKING)

`hockeytech_api`, the one HTTP entry point behind the PWHL surface and the 19 other HockeyTech
league families, caught every exception and returned `None`. A 403, a 5xx, a timeout or an
unparseable body therefore parsed to a zero-row frame, so a failed fetch read as "no games".
It now follows the package error vocabulary:

- **HTTP 404** raises `NoDataError`.
- **A failed fetch** raises `AssetFetchError`: a transport error, a non-2xx status that outlived
  the retries, an empty or unparseable body, or one of the HTTP-200 error sentinels
  (`Undefined Tab <view>`, or any top-level `{"error": "..."}` such as
  `InvalidView error: <view>`). The sentinels used to warn and then parse to an empty frame.
  The error text never carries the feed key: it is masked, and the URL-bearing transport
  exception is not chained.
- **`Feed type access denied.`**, the plain-text reply MJHL's public key gets on `gc`, is still
  a graceful empty: `mjhl_game_summary` returns empty frames and `mjhl_pbp` returns plays without
  game metadata.

Every `<lg>_*` family function, the `pwhl_*` functions and the analytics fetches pass these errors
to the caller. `resolve_season_id` keeps PWHL's fallback table for a failed seasons fetch and for
a season the list lacks; other leagues re-raise. The table now runs through 2026-27 (ids 1-11,
preseasons and the 2026 playoffs included). `pwhl_streaks` (deprecated, no such upstream view) no
longer sends a request; it still warns and returns an empty frame.

Season resolution also skips the one-off events HockeyTech lists as seasons (all-star games,
showcases, prospect games, combines, special events, exhibitions, play-ins):
`resolve_season_id("ahl", season=2026)` returned 91, the "2026 All-Star Challenge", instead of 90,
the "2025-26 Regular Season". An explicit `season_id=` never asks for the seasons list, so a dead
seasons feed cannot break `pwhl_stats(season_id=11)`. `pwhl_playoff_bracket()` with no arguments
now uses the newest season that has playoffs, not the newest season (which usually has none yet).

`most_recent_<lg>_season` / `most_recent_pwhl_season` no longer return a hard-coded 2026, which was
already stale (the live PWHL seasons feed lists 2026-27, end-year 2027). A seasons list the feed
answered with no season raises `NoDataError`; a failed fetch raises `AssetFetchError`. This
matches sportsdataverse-js.

**Breaking:** code that checked for `None` or an empty frame to detect a HockeyTech failure now
gets `AssetFetchError` / `NoDataError`. Wrappers that default the season (`<lg>_standings`,
`_teams`, `_team_roster`, `_leaders`, `pwhl_stats`, `pwhl_playoff_bracket`) raise the same way when
they need the seasons lookup and it fails; pass `season_id=` to skip the lookup. Catch them as:

```python
import sportsdataverse as sdv
from sportsdataverse.errors import AssetFetchError, NoDataError

try:
    df = sdv.ahl_schedule()
except NoDataError:
    df = None  # the fetch worked and there is nothing there: skip it
except AssetFetchError:
    raise  # the fetch failed and the answer is unknown: retry later, never record it as empty
```

Both subclass `SportsDataverseError`. A season that the list does not carry is still `ValueError`
("No ahl season for season=..."), as before.

34 PWHL return-column descriptions were also wrong and are corrected from real values. In
`pwhl_scorebar`: `id` is the game id, `home_id` is the HockeyTech team id, `game_status` is the
numeric code, `quick_score` is always `'0'`, `game_summary_url` is a game id or a site path,
`game_letter` is the playoff-series letter, `game_date_iso8601` carries the start time and UTC
offset, `date` is a date only, `home_city` / `timezone` / `home_goals` / `league_id` say what they
hold, and the eight W/L columns are the team's season record as fetched, not as of the game. In
`pwhl_player_search`: `score` is a search relevance score, `profile_image` is a file name, and
`role_id` / `role_name` are the person's role, not a position. In `pwhl_stats`: `height` is
feet-and-inches text, `rank` is the table rank, `namelink` is plain text, `division` is a name,
`veteran` is a code, and `name` (also in `pwhl_leaders` and `pwhl_team_roster`) is the player's
name, not a team mascot.

### Fixed — ESPN basketball pbp: one-provider spreads, paired spread signs, team timeouts, MBB double-overtime seconds

Four fixes to `espn_nba_pbp`, `espn_wnba_pbp`, `espn_mbb_pbp` and `espn_wbb_pbp` (and their
`helper_<lg>_pbp` reprocess path). The shared logic now lives in one private module,
`sportsdataverse/_espn_basketball_pbp.py`.

- **The spread from a one-provider pickcenter.** The pickcenter helper read the odds only when
  ESPN listed more than one provider. Modern summaries list one (DraftKings), so every such game
  got the default spread (2.5, home favored, `gameSpreadAvailable=False`). The 2026 men's title
  game (401856600) shipped 2.5 when DraftKings had MICH -6.5. One provider is now enough. A
  pickcenter with no spread (a lone teamrankings record entry) still gets the defaults, and an
  all-null over/under column no longer raises.
- **The spread and the home favorite come from the same provider.** They used to be taken
  independently, each as the first non-null value across providers. A record-only teamrankings
  row has no spread and sorts first, and its favorite flag (False for both teams) was paired
  with consensus' spread. UNC Asheville, a 17.5-point home favorite (330582427), got a home line
  of -17.5; it is now +17.5. MIA in NBA 401430219 goes from -4.5 to +4.5. Both now come from the
  first provider with a spread, and the favorite is the spread's sign (ESPN's spread is the home
  line). The provider order is now explicit and unchanged: `str(provider.id)`, so teamrankings
  ("1002") reads ahead of consensus ("1004") and Caesars ("45"). Where teamrankings and consensus
  disagree, teamrankings matches the winner more often, and an integer sort would move MBB
  2021-22 to Caesars' line. A spread of exactly 0 takes that row's favorite flag (home if unset).
  `helper_<lg>_pickcenter` now returns plain floats/bools for `gameSpread`, `overUnder` and
  `homeFavorite`; a found line used to come back as a 1-element numpy array.
- **Every team timeout.** The timeout flags matched only ESPN's NCAA `ShortTimeOut` type, so the
  NBA/WNBA `timeouts` map was always empty and NCAA full timeouts (`RegularTimeOut`) were
  dropped. The flags now cover `RegularTimeOut`, `ShortTimeOut`, `Full Timeout`, `Short Timeout`,
  `No Timeout` and `Reset Timeout`. Official and TV timeouts belong to no team and stay out. The
  calling team comes from the play's own `team.id`. The team-name match that used to decide it is
  a fallback for plays without one, and now matches whole words: as a substring test it credited
  "Memphis" to PHI and "timeout" to ME. The map holds timeouts *called* as ESPN logs them, not
  timeouts *charged*. A coach's challenge outcome is not applied because ESPN logs the
  challenge's own timeout too inconsistently: a team timeout precedes 90% of charged and 56% of
  retained NBA challenges, and about 5% of NCAA ones.
- **MBB end-of-period seconds in the second and later overtimes.** On the first play of 2OT and
  later, `end.period_seconds_remaining` took the next play's start while
  `end.game_seconds_remaining` was set to 300. Both are now 300, matching the first overtime and
  the other leagues. A bare-seconds MBB clock ("23.4") now parses as 0:23 instead of raising.

`tests/test_basketball_pbp_offline.py` checks each fix against real summaries in
`tests/fixtures/espn/basketball_pbp/`. The spread does not feed the shipped basketball
win-probability models, which are ratings-based, so a reprocess changes only the published
spread columns (`game_spread`, `home_team_spread`, `game_spread_available`, `home_favorite`)
and the timeout flags. Published data changes only after a release reprocess, and its scope is
the owner's call:

- **One-provider games only.** About 9,150 games in the raw stores have a one-provider pickcenter
  with a spread: MBB about 5,300 (4,766 of them in 2025-26), NBA 1,078 (2025-26), WBB 1,855
  (mostly 2022-23 and 2025-26) and WNBA 911 (2020-22 and 2026). Add the 128 mixed-row sign
  games (127 MBB, 107 of them in 2012-13, and NBA 401430219).
- **Full history.** Older MBB and NBA `final.json` files were built by the pickcenter helper as
  it stood before August 2023. A full reprocess also changes about 27% of MBB 2013-22
  multi-provider games (about 13,000; 2.3% change sign, median change 0.5 point) and about 32% of
  NBA 2013-19 (about 2,900; about 1% change sign). Most changes are improvements: where the
  signs disagree, the current code matches the winner in MBB 22 of 35 and NBA 12 of 15.

### Fixed — pff_api return tables for the per-player and coverage-matrix routes

19 `pff_api_*` routes had no columns in their return tables. The capture they were generated
from had two gaps. It requested one QB for every per-player report, so the kicker, punter,
returner and defense reports came back with no week rows. It also recorded the nested bodies
(per-game `weeks`, the coverage matrix, the snaps and rushing-direction objects) only as object
keys. A new live capture uses a player who played each role in 2025 (NFL and NCAA, 38 reads). The
return table of each of these routes now lists what its parser returns on that capture:

- **13 per-player summaries** (`pff_api_player_*_summary`, `_offense_blocking`,
  `_offense_pass_blocking`, `_offense_run_blocking`): the per-game rows, including the `game_*`
  columns exploded from each row's nested game object.
- `pff_api_player_seasons`, `_snaps_summary`, `_position_pivot` and `_rushing_direction`.
- `pff_api_facet_receiving_coverage` and `pff_api_facet_defense_coverage_matchup`: one shared
  schema with three frames, `defenders`, `receivers` and `versus`.

710 column descriptions were added. Stat columns reuse the text already written for the same
column of the same PFF report. Context and new columns were written from PFF's spec, and each
derived column's formula was checked against the captured rows.

**Parser fix.** For the player rushing-direction and player snaps-summary bodies,
`parse_pff_report` returned a zero-row frame even when PFF sent data, because each body is one
object rather than a list of rows. It now returns one row per direction, and one row with
`snap_counts_<type>` columns, respectively. `pff_api_player_rushing_direction()` and
`pff_api_player_snaps_summary()` now return those rows by default. Passing the envelope key
explicitly (`report="rushing_direction_stats"` or `report="snaps"`) returns the same rows.

### Fixed — reference-docs Valid URLs are the URLs the example calls request; summary documents its dict

The **Valid URL** on each generated reference page, and the `Example URL:` line in the
wrapper's docstring, now replay the wrapper body on its example arguments. Each one is the URL
that the documented example call requests.

- **ESPN Core v2 child resources.** `espn_<lg>_game_competition(event_id='401584793')` requests
  `/events/401584793/competitions/401584793`, but the page showed `/events/401584793/competitions`.
  Every game, competitor, play and official child resource showed that same collection URL. Path
  tokens the wrapper fills from a default (`cid` falls back to `event_id`, `record_type=0`), an
  optional segment, or a `/now` variant are now substituted.
- **Default query params.** Params the wrapper always sends, such as `limit=1000` or the
  nba_stats `PerMode` / `SeasonType` defaults, now appear in the URL.
- **No runnable example.** 50 flat-API wrappers (41 `cbs_*`, 9 `sports247_site_pages_*`) have no
  example value for a required argument. Their pages no longer show a truncated URL.
- **Soccer and cricket catch-all wrappers** now pass the required `league=` argument in their
  examples.

1,426 of 3,446 ESPN URLs and 467 of 1,033 flat-API URLs changed.
`tests/codegen/test_valid_url_matches_call.py` calls every generated wrapper offline against a
recording `_get` and asserts that the documented URL is the requested one.

`espn_<lg>_summary` (30 leagues) said it returns "a tidy `polars.DataFrame` with the columns
below". With `section=None`, `parse_summary` returns a dict of frames keyed by section. The
endpoint now declares `parsed_doc`, like the `espn_cdn` game pages, so its docs and docstring
say it returns a dict.

### Added — ESPN NBA G League wrappers (`espn_nbagl_*`)

ESPN's G League (`basketball/nba-development`) is registered in `leagues.yaml` like every other
ESPN league, so codegen now emits the full universal family (112 wrappers) as
`sportsdataverse.nbagl.nbagl_espn_ext`: `espn_nbagl_standings`, `espn_nbagl_scoreboard`,
`espn_nbagl_teams_site`, `espn_nbagl_summary`, `espn_nbagl_team_roster`, and the rest. They are
exported from `sportsdataverse.nbagl` and the top-level package, and `return_parsed=True` (the
default) routes through the shared ESPN parsers. Before this, G League standings needed the
private `sportsdataverse._common_espn_parsers` and a hand-built URL. Offline tests drive the
standings, teams, and scoreboard wrappers through real captured 2025-26 G League payloads;
a gated live smoke test checks the teams and standings endpoints.

### Fixed — nba_stats, wnba_stats and on3 return tables now match what the parsers return

The reference-docs return tables for these families named columns from the stats-API catalog
and the On3 OpenAPI spec, not from the parser output. Many of those names never appear in a
parsed frame. The tables are now generated from the parser's output on a committed real
capture of each endpoint.

- **Renamed columns.** Examples: `fg3m` → `fg3_m`, `leagueid` → `league_id`,
  `5-9_ft_fgm` → `5_9_ft_fgm`.
- **Result sets.** 67 NBA and 63 WNBA endpoints return a dict of result sets. Their docs now
  show every set, and their docstrings say they return a dict.
- **On3 tables.** These show the flattened nested-object columns that `parse_on3_rdb` returns.
- **On3 without a capture.** 48 On3 endpoints have no capture with rows. Each is marked
  `unverified`, and its docs carry a one-line caveat in place of a table. The OpenAPI response
  types were not used as a fallback: on the 9 endpoints where they could be checked against a
  capture, their field names matched the parser on only 7.
- **`nba_stats_playbyplayv3` / `wnba_stats_playbyplayv3`** are marked `unverified`. The generic
  parser returns no columns for their `{meta, game}` payload.
- **`on3_people_measurements`** shows a single `player_measurements` column. `parse_on3_rdb` does
  not unwrap the `{playerMeasurements: [...]}` envelope, and the table documents what the parser
  returns.

Wrapper behaviour is unchanged.

### Fixed — CFB scores ESPN marks but no text rule named, textless copies, untyped admin rows

Three ESPN feed defects, sized on the 20,080 processed games of 2004-26:

- **ESPN scored the row; sdv-py did not (~650 rows).** The touchdown or field goal is in ESPN's
  `scoringPlay` and the score, but no text rule names it. Examples: a pick-six on a frozen
  scoreboard (169; 282640084 "... returned for 40 yards for a TOUCHDOWN." at 28-20 before and
  after), a 2004-07 "Vernon Gholston 21 yd fumble return." (177), a 2014+ fumble return closed
  by "(Aaron Boumerhi KICK)" (168), and field goals typed as the snap before them. Each realised
  the play's model end state instead of the points. A last pass, `_type_espn_scored_rows`, types
  these from the row itself:
  - **Who scored** is the start team's margin change: ±6-8 a touchdown, +3 a field goal. On a
    frozen board, the play family decides, provided the text or ESPN's `scoringType` says
    touchdown.
  - **Rows left alone:** a row whose margin credits the other side than its family (ESPN's start
    team is the returner), and a frozen-board fumble on a rush or pass, whose side the text
    cannot settle.
  - **Kickoff return touchdowns keep their score at the end of a game.** Miami's eight-lateral
    return at Duke (2015) closes the game and is a touchdown, not a dead possession.
- **Textless copies (~180 rows).** A row with no text whose drive has a texted row of the same
  type, period and start state is a copy and is dropped. ESPN files it before the play at the
  previous play's clock (401403886, 2022: eleven punts and sacks, each booked twice) or after it
  (2007-15).
- **Untyped admin rows (1,387 in 483 games).** These are dropped before the plays are ordered: the 2004
  quarter and game markers, "Begin Drive", "PURDUE drive start at 15:00 (OT ).", empty rows, and a
  try alone in parentheses ("(Sean O'Haire Kick)" ahead of the touchdown row that carries it).
  As "Unknown" (or "End Period") rows they carried model EPA up to 4. In the drive of
  401752914's touchdown, the kick fragment also moved that touchdown to the end of the game.

On a 200-game random sample (32,146 rows), nothing else changed: 7 rows were retyped to the score,
8 dropped (6 admin, 2 textless copies), and 3 next to them moved EPA.

### Fixed — CFB plays that end a half leave a possession worth nothing

A play ends the half when it is the first half's last play, regulation's last play in a game that
goes to overtime, or a finished game's last play. Without a score nothing follows it, so its EP_end
is now 0 and its EPA -EP_start, the cfbfastR / nflfastR convention. EPA already booked -EP_start on
the first half's last play, but EP_end was the model at 0:00 on the 1 (about -0.4). Three cases were
wrong outright:

- **A finished game's last play was never flagged.** The flag compared against `lead_half` in the
  same `with_columns` as the fill of its null, so the last row read the null. The final kneel of
  400547699 booked EPA -1.51 where its possession was worth 1.93.
- **Regulation's last play in an overtime game was not flagged**, because overtime is half 2. It
  read the 0:00 own-1 state: 400787460's last regulation rush booked -0.55 instead of -0.10.
- **A play whose NEXT snap is at 0:00 was treated as the end.** The dead-possession end state keyed
  on an end clock of 0, which since the end clock became the next play's clock also catches the play
  before an untimed down or a half's last snap. 400547699's rush to the 5 with 0:30 left was moved to
  the 1 and booked EPA -3.34; it ends at the 0:00 snap's EP (1.93), EPA -1.64.

A live game's latest play still ends nothing, since nothing follows it yet.

### Fixed — CFB returned kickoffs end at the receiving team's first down

ESPN ends a returned kickoff ("Kickoff Return (Offense)", 2014 on) at `end.down` -1, its no-down
sentinel, and the EP model (all four `down_*_end` flags False) and the WP model (`end.down`) scored
that end as a state with no down: 401282817's second-half return to the 17 read EP 0.79 where the
1st & 10 snapped there reads 1.83. A non-scoring kickoff whose end down is outside 1-4 now ends at
down 1, so its EP_end is the next snap's EP_start. On a 26-game 2014-26 sample that is 47 kickoffs
(1.8 a game), whose EPA moves by a median 0.95. Found chasing cfbfastR's last second-half kickoff gap
to sdv-py: cfbfastR already read the next snap.

### Fixed — CFB plays end at the next play's clock

`end.TimeSecsRem` (and `end.adj_TimeSecsRem`) was `start.TimeSecsRem.shift(1)`: the clock at the
PREVIOUS play's start. Every EP_end and every after-state that reads the end clock was scored at an
earlier clock than the play ended at. On most plays the difference is a few seconds, but at the end
of a half or a game it is the whole question: 400547730's final kneel at 0:14 "ended" at 0:54, and
its EPA was -0.97, as if the offence kept a possession worth 3.6 points. The end clock is now the
next play's start clock, and the last play of a half or game ends at 0:00 (game time 1800 / 0):
that kneel is EPA -4.8. Over a 7-game probe the median EPA change is 0.009; 17% of plays move by more
than 0.1 and 2.3% by more than 0.5, almost all of them at period ends. Found aligning cfbfastR's
end-of-half rules with sdv-py: with this fix and those, the two engines' EPA agree to a median of
0.000 on a 92-game 2004-26 sample (was 0.015).

### Fixed — CFB 2007-13 touchdowns filed as their own kick get the snap's down

ESPN's 2005-13 feed writes down and distance -1 on plays with no down (kickoffs, tries,
penalties on tries: 119,040 rows). In 2007-13 it also filed some touchdowns as ONE row with their
extra point, typed as the kick, so the row carries the try's start state. `__helper_cfb_pbp_features`
already retypes those 248 rows to the pass or rush touchdown and takes the snap's spot from the
text ("for 36 yards"), but kept down -1: the EP model's down one-hots were all zero and scored the
snap as no down at all (302602440's 59-yard rushing touchdown: EP_start 0.11). The down and distance
are now the end state of the play before, when that play ended at the snap's spot (219 of the 248,
a change of possession included: ESPN's end state is already the new offence's), and otherwise 1st
and 10 (goal to go inside the 10); ESPN's "& Goal" distance 0 is the distance to the goal line. All
247 such rows in their 86 games now carry a down 1-4 (was -1); EP_start median 3.8 -> 4.5.

### Fixed — CFB blocked field goals keep ESPN's type (#641); null keys never twin a play copy

Four string relabels in `__helper_cfb_pbp_features` turned any type containing "field goal" or
"extra point" plus "blocked" or "no good" into "Extra Point Missed". On ESPN's types they only ever
matched "Blocked Field Goal" and "Blocked Field Goal Touchdown" (1,061 rows 2004–26), which then
went through the kick rules after them, and the one later rule that restores the type reads the
text: 12 blocked field goals finished as "Penalty" (400547866, EPA +1.69), 4 as an "Extra Point
Missed" try (400548023), and 55 blocked-field-goal return touchdowns as a plain "Blocked Field
Goal" (400547865: the defence's touchdown lost, EPA -0.93 -> -7.7). The four rules are removed;
ESPN's types stand. Found porting the relabel block to cfbfastR (sportsdataverse/cfbfastR#175).

`_drop_espn_play_copies` twins a play with a later copy through a self-join on the drive and start
state. Polars (1.40–1.44) matches rows whose join key has four or more null columns despite
`nulls_equal=False`, so plays with no drive, team, down or distance could be dropped as a stale
batch. Null keys are now dropped before the join; no game in the raw corpus was affected.

### Added — the metric registry (`sportsdataverse.registry`)

`sportsdataverse/registry/metrics.yaml` is the one source for how a published football metric is
displayed: label, short label, axis label, format (`num2` / `num1` / `pct1` / `int`), polarity
(`higher` / `lower`, from the offense or player perspective), family, qualifier, glossary slug and
per-basis variants, one entry per base metric. `resolve(column)` maps any published column
(`EPAplay_off_pass_rank`, `adj_def_epa`, `havoc_margin`) onto its entry plus the column's side,
phase, suffix and effective polarity: `_def` flips the base's, a `_margin` is always higher-is-
better (every producer margin is good-minus-bad). `python -m sportsdataverse.registry --ts
--target gop|web` renders a deterministic TypeScript module (`METRICS` + `resolveMetric`) headed
by the sdv-py version and a sha256 of its body, which Game on Paper and the web platform generate
their copies from instead of keeping four drifting tables. The yaml ships in the wheel and is
read without PyYAML, like `validation/thresholds.yaml`. Documented under *Architecture → Metric
registry*.

### Fixed — CFB win probability in overtime and the final seconds, and made field goals' WPA

The regulation WP boosters were trained on a frame that drops every game that reached overtime
(cfbfastR-cfb-data `clean_plays`), so they estimate P(win | state, settled in regulation). A tied
game late in the fourth was learned only from games somebody won in regulation, and overtime was
never seen: its clock reads 0, so it scored as the last snap of such a game. On the 2022–25
holdout, tied with two minutes or less left, the team with the ball was given 0.77 against 0.65
won, and a tied overtime snap 0.94 against 0.50. New `cfb_wp_overtime` (fitted by
`tools/fit_cfb_wp_overtime.py` on 2004–21, read from `models/wp_ot_reach.card.json`) mixes each
regulation prediction with the overtime it may reach, `(1 - q) * wp + q * tie_value` (q from the new
`wp_ot_reach` booster, the tie value a logistic in the pregame spread), and values overtime by its
rules: the possession ends in a touchdown, a field goal or nothing, and the first team is answered
by the second from the 25 (who had the ball first is read off the period's first snap that is not
a timeout or a flag). Both the spread and the spread-free surfaces use it. The card pins the
sha256 of `wp_spread.ubj` / `wp_naive.ubj`; retraining either (above all to keep overtime games)
needs a refit of this correction, and a test fails until then.

The fourth-down and two-point surfaces score the state a decision leads to. A state with no
regulation time left after the play is now decided (win, loss, or overtime if level), and in
overtime a punt, a kick or a failed try ends the possession instead of handing the opponent the
ball at the spot. Georgia Tech's walk-off field goal (401754623) goes from "punt 91.7%" to FG
68.9% vs punt 47.3%; a tied punt with 0:05 left (401762856) is overtime (56.4%, not 91.2%); Cal's
overtime 4th and 3 at the 3, down 3 (401754585), is go 38.7% vs FG 32.8% (was FG 85.2%).
`CFBPlayProcess` passes the overtime possession order as `ot_second_possession`; other callers
may, and without it a non-zero margin implies the second possession.

A made field goal's `wp_after` now hands over to the kickoff that follows, as a try's does. It
was the kicker's snap at the spot with the points counted, a team with the ball: 11% of 2025's
made field goals missed the next row by more than 5 points, up to 50 late in the fourth
(Louisville's tying kick in 401754554 published WPA +30.9%). In overtime a touchdown row that
carries its own try (2014 on) ends the possession at the realised margin.

In a two-point shootout (2019-20 from the fifth overtime, 2021 on from the third) an attempt is
valued by the shootout rule: a make leaves the other team one attempt of its own, not a drive from
the 25. Alabama's first attempt at Auburn (401282146) reads 0.59 -> 0.85; scored as a possession it
read -0.40. ESPN often files the second attempt under the first team, so the row order decides
which attempt a row is. A shootout attempt is not a two-point decision (there is no kick to weigh),
and its `two_pt_*` columns are null.

Every play's `wp_*` / `wpa` and every fourth-down and two-point column move a little (most in
close fourth quarters), so every CFB season needs a reprocess. EPA is unchanged. Per game,
`fg_wp` / `make_fg_wp` / `miss_fg_wp` / `xp_wp` are Float64 like `go_wp` and `punt_wp` (they were
Float32; the published parquet was already Float64).

### Fixed — CFB completions whose text states no "complete to ... for N" gain keep their yards

`yds_receiving` was read only from "complete to ... for N" text (or the touchdown form "N Yd pass
from"), so a completion written any other way had no receiving yards and its passer's box line
lost them. ESPN's 2025 feed writes "Preston Stone pass to Cam Porter for 4 yds" (no "complete"):
Stone's line in 401752817 read 21 completions for 15 yards. ESPN's 2024 feed writes "pass complete
to X for a 1ST down" with no yardage at all (874 rows in 38 games). Such a completion now takes
ESPN's `statYardage`, which equals the stated gain on every 2025 "pass to" row and the change in
field position on every 2024 row. The completion flag keeps incompletions and sacks out, and a
play with a penalty or a fumble is left alone. Every season has a few such rows (2004: 1,342
touchdown rows with no text; 2019: 47; 2021–2023: 25–32 each; 2025: 83 in the release), so
passing / receiving yards, yards per attempt and the box scores move wherever they occur. Two
related gaps remain (see the PR): a reversed-order text ("to X for 20 yds ..., Stone pass") files
the passer as TEAM, and a completion lost on a fumble is typed as the recovery with `pass=False`.

### Changed — "situation-neutral" reads the score-and-clock win probability (CFB and NFL)

The neutral split in `football.tendencies` (win probability 20–80%, regulation, outside the last
two minutes of a half) read `wp_before`, which carries the pregame line, so a heavy favorite's
tied first quarter was not neutral: in 2025 only 64.1% of tied first-quarter FBS snaps counted,
433 of 1,739 team-games had no neutral snap at all, and 25 teams' neutral pass rate moved 3+
points between the two WPs. Per the owner's decision (2026-09-30), CFB now reads
`wp_before_naive` (score, clock and field position only), the same model adjusted EPA's
garbage-time rule uses; the band and the clock rules are unchanged. This moves every `*_neutral`
column (`plays_` / `passes_` / `epa_` / `successes_neutral`, their rates and `def_` twins) and
`sec_per_play_neutral` in team / coach tendencies and coach careers. NFL follows (owner, same
day) so both leagues' "neutral pass rate" mean the same thing: it reads its `wp_before_naive`
(nflfastR's spread-free `wp`) under the same band and clock rules. In 2025's Raiders–Texans game
(401772805) the pregame line left 19 of 102 snaps neutral; the score-and-clock WP leaves 93.

### Fixed — pace counts regulation drives once, for the drive's own offense

`sec_per_play` in `football.tendencies` summed ESPN's drive clock over every drive with a
parseable `drive.timeElapsed`, overtime included. Overtime has no game clock: ESPN files its
drives as 0:00 (86 of 89 in 2025), so they added plays and no seconds, and one North Texas OT drive
(401762461) carried 15:00 over 3 plays. Separately, an ESPN drive id holding standing snaps by both
offenses (51 ids in 28 games in 2025) handed each offense the whole drive clock and play count.
A drive now carries a clock only in regulation and only for its owner: the offense ESPN names as
the drive team, else the one with the most standing snaps (the rule cfb-data's `team_summaries`
uses). In 2025, 75 of 136 FBS teams' `sec_per_play` move, by at most 0.71 s (North Texas 27th →
16th), and 10 teams' `sec_per_play_neutral` move by at most 0.58 s. `drives_with_clock`,
`drive_seconds`, `drive_plays` and `pace_coverage` change with them; drive counts, finishing and
scripting are unchanged.

### Fixed — a season usage table keeps one row per player

`aggregate_usage_box` summed per-game rows on `(team, player_id, player_name, position_group)`, so
a player whose position group was missing in some games (a roster gap) or whose name changed
split into several season rows: 177 CFB player ids in 2025 (763 in 2014), and 121 FBS players'
main row undercounted targets (Danny Scudero, San Jose State: 160 targets published as 106 + 54).
Season rows now key on `(team, player_id)` (the name when a row has no id) and carry the most
frequent non-null name and position group. Every `usage_*` player table (players, tackles and the
special-teams tables) needs a rebuild; the per-game `adv_*` tables are unchanged.

### Changed — CFB xQBR retrained on the served box score, without the spread, behind a publish gate

The bundled `cfb/models/qbr_model.ubj` is replaced. The old model was fitted on features
that serving never computes: plays were grouped by passer name, so QB runs never reached
`rush_epa`, and the booster had no split on it. Overtime games were dropped, and penalty
plays were handled differently. The new model is trained on the published `adv_passing`
rows, which are exactly what `create_box_score` scores. Its labels are ESPN game QBR,
committed with provenance in cfbfastR-cfb-data. It also drops `spread`: `qbr_vars` is now
the five EPA aggregates plus `era0..era3`. On identical plays, the old model gave a
14-point favourite's QB about 10 points more than a 14-point underdog's.

On the frozen, never-trained-on holdout (2026 weeks 1–4, 541 QB-games) the RMSE against
ESPN raw QBR fell from 14.19 to 11.74. The correlation rose from 0.864 to 0.914 (0.660 to
0.759 against Total QBR). The paired squared-error change is −63.5, with a 95%
game-clustered CI of [−84.2, −43.6]. Keeping the spread would have scored 11.49; the
pre-registered tolerance for dropping it was 0.30. The bundle now carries
`qbr_model.gate.json`, the trainer's gate record, and `tests/cfb/test_qbr_model_gate.py`
fails if `qbr_model.ubj` is not the candidate that passed it. Every `exp_qbr` changes;
published `adv_passing` / pbp box scores keep the old values until they are reprocessed.
The box score still emits a `spread` column.

### Changed — tackle share counts only the defense's own scrimmage snaps

Tackle share divided a player's tackle points by his team's across every play, special teams
included: kickoff and punt coverage made up 6,590 of 113,407 CFB credits in 2025 (5.8%), with
22 more on plays a penalty wiped out. Per the owner's decision (2026-09-30) the share now counts
only the defense's own standing scrimmage snaps: two new columns,
`scrimmage_tackle_points` and `team_scrimmage_tackle_points`, carry its numerator and
denominator, and `tackle_share` is their ratio (per game, per position group and per season in
`aggregate_usage_box`). `tackles`, `assists`, `tackle_points` and `team_tackle_points` still count
every credit, special teams and the offense's tackles after a turnover included. Rows built before
this change still share on every credit. The two new columns are declared in the loader schemas
after the tackle tables are republished.

### Fixed — a tackle is credited to the tackler's own team

The usage box credited every tackler on a play to the play's defense, so a punting team's
coverage tackles and an offense's tackles after an interception or fumble landed in the
opponent's tackle table: 2,945 of 113,407 CFB tackle credits in 2025 (punts, punt returns,
interception returns, fumble recoveries), and every FBS team's season table listed opposing
players (Indiana's Jeff Utzinger under Miami in the 2025 title game). `create_usage_box` now reads
each tackler's team from the game roster (`rosters`, which the CFB processor and the cfb-data build
already pass) and files the credit under that team; a tackler the roster doesn't list stays with the
play's defense, as before (NFL, which passes no roster, is unchanged). Raw tackle and assist counts
are unchanged; `def_pos_team` in the `tackles` / `position_group_tackles` sections now means the
tackler's team. `adv_tackles`, `adv_position_group_tackles` and their `usage_*` season tables need a
rebuild.

### Fixed — a pick-six or fumble-return touchdown is not the offense's conversion or touchdown

The football tendencies and usage box read "touchdown" as any touchdown on the play, so a third
down that ended in an interception or fumble returned for a score counted as the offense's
conversion: 816 of 115,222 CFB third-down conversions in 2014–2025 (640 interception-return, 129
fumble-return and 47 fumble-recovery touchdowns; 58 in 2025 across 40 FBS offenses). The same flag
fed fourth-down conversions (13 of 2,081 in 2025), a ball carrier's `touchdowns` / `fd_or_td` when
his fumble was returned for a score, red-zone and scoring-opportunity touchdowns, and the drive
touchdown behind `rz_tds` / `so_tds`. A touchdown now counts only when it is not a
`defense_score_play`, in `football.tendencies`, `football.usage_box` and `fit_third_down_curve`
(both leagues). Team / coach tendencies, coach careers and the usage tables need a rebuild; the
bundled third-down curves should be refit on the reprocessed play-by-play (today's release no
longer reproduces them exactly, so a refit now would mix in unrelated data changes).

### Fixed — CFB plays ESPN files twice under new ids are dropped

ESPN sometimes files a play again under a fresh play id, in three shapes the adjacent-copy
dedupe could not see: a stub echo on the next row (same text and down/distance, no spot), a batch
of a drive's plays filed at the drive's start clock ahead of the same plays at their real clocks,
and the same play on both sides of a timeout or end-of-period row. `CFBPlayProcess` now drops
them before that dedupe, keeping the echo's play type (401752854, Oregon @ Penn State 2025, files
its punts, kickoffs and a missed field goal first as "Pass Completion"). That game goes from 281
rows to 173, and its scrimmage plays now match the box score. Across the raw store the pass removes
809 rows in 105 games in 2014–2026 and 727 in 156 games in 2007–2013. Feeds with no start spot
(2004–2006) are left to the adjacent rule. Play counts, EPA/play and success rate move in the
affected games, so every season with drops needs a reprocess.

### Fixed — CFB losses written "for N yards loss" read as gains

ESPN's 2025 text states a loss after the number: "rush middle for 4 yards loss", "caught at
SAC18, for 1 yard loss". The "rush for N" and "for N" readers matched first and stored the loss
as a gain, so `yds_rushed` was +N on 2,224 rushes and `yds_receiving` +N on 581 receptions in the
published 2025 season (1,508 and 364 so far in 2026, 44 rushes in 2023). The existing "yds loss"
branch sat behind them, and it missed the singular "1 yard loss" (968 of the 2,224). Both
readers now take the stated loss first. EPA is unaffected (it comes from field position); rushing
and receiving yards, yards per carry, line / highlight yards, stuff and opportunity rates, and the
penalty residual `statYardage - yds_rushed` all move. A run filed twice in one 2023 text
("run for 7 yds ... fumbled ... rush middle for 7 yards loss") reads the loss from the second copy
(43 rows). The 2023 and 2025 seasons and 2026 to date need a reprocess (2022 has one rush and one
reception).

### Fixed — CFB fumbles in ESPN's 2025 text format keep their rush / pass flag

ESPN's 2025 feed writes a run as "rush right for 6 yards gain" (the rush flag on a
fumble-typed row only read "run for") and files a fumble that goes out of bounds under a new
`Fumble` type that neither flag listed. 466 of 1,401 FBS-vs-FBS scrimmage fumbles in 2025 came
out with `rush` and `pass` both False (33 in 2024), 435 of them through these two gaps, so every
pass/rush aggregate (havoc, EPA/play, success rate) dropped them. Fumble-typed rows now read the
2025 rush phrasing, and a `Fumble`-typed pass counts as a pass (and a completion when complete). Safeties on "rush for a loss" rows in the
2005–2013 feeds pick up the rush flag through the same pattern (~25 per season). A `Fumble`-typed pick whose returner
fumbles out of bounds is typed "Interception Return" (3 rows in 2025–26), so the strip-sack rule no
longer retypes it as a lost fumble.

### Added — MLB park dimensions by season (`load_mlb_park_dimensions`)

`load_mlb_park_dimensions()` reads the season-less `mlb_parks` release built by
`sportsdataverse/sdv-reference-data`: one row per MLB venue per season, 2001 on
(regular-season, spring-training, neutral and international sites), with fence
distances in feet at MLB's seven markers, capacity, turf, roof, azimuth, elevation and
coordinates as of that season, from the MLB Stats API. `venue_id` stays a string (the
API's `venue.id`). Cited corrections for fence moves the API lags or misses are applied
and described in `notes`. Every column is described in the returns table.

### Added — conference and division reference tables for nine leagues (`{league}_groups`)

Thirty-six dataset loaders over the `{league}_groups` release tags built by
`sportsdataverse/sdv-reference-data`, four per league for `cfb`, `mbb`, `wbb`, `nfl`,
`nba`, `wnba`, `mlb`, `nhl` and NCAA baseball (`load_ncaa_baseball_*`, under `mlb`):
the season-less `load_<league>_groups()` (one row per group lineage, with SDV's own
`<league>:<slug>` `group_id`), `load_<league>_group_seasons()` (each group's name,
abbreviation, parent and member count as of that season, never today's label applied
to the past) and `load_<league>_group_aliases()` (every label and id a source uses for
a group, with its valid seasons), plus `load_<league>_team_group_seasons(seasons)` (each
team's subdivision / conference / division per season, one asset per season). Seasons
keep each league's own key -- the ENDING year for `mbb`, `wbb`, `nba` and `nhl` -- so no
asset offset is applied. `team_id` stays a string, as the tables publish it. The NFL
loaders are hand-written in `nfl_loaders.py` (a missing season raises `NoDataError`);
the rest are generated. Every column is described in the returns tables.

### Added — the official PFF Developer API (`api.pff.com`), with the premium wrappers kept as LEGACY

PFF now publishes an official, API-key-authenticated Developer API. `pff_api_*` (68 generated
wrappers in `sportsdataverse.nfl.pff_api`, also at top level) cover every read operation in PFF's
own spec, named after PFF's `restish pff <command>` operations: the 60 `/v1` routes (the Premium
Stats reports, byte-identical to `premium.pff.com`, plus 13 per-player reports, `teams/summary`
per-game team grades and `whoami` that the legacy surface never had) and the 8 `/v2/{league}`
tables (team stats with ranks, rosters with depth order and snap share, schedules with PFF Elo and
SOS, qualified leaders with percentiles, 19 team and league-wide reports with ~110 new player
metrics). Auth is one key — `api_key=`, or `PFF_API_KEY` / `SDV_PY_PFF_API_KEY` — sent as a bearer
header by the new `pff_api_runtime`; a refused or failed fetch -- or a 200 whose body is not a
JSON object -- raises `AssetFetchError`, never an empty frame. PFF responses bypass the package
response cache (its key ignores the `Authorization` header, so one key could be served another's
body). A view-only entitlement answers 200 with columns removed and a `restricted` list: the
wrappers return that partial body with a `UserWarning` naming the columns, and raise instead with
`strict=True` on any wrapper or `SDV_PY_PFF_STRICT=1` -- the setting for pipelines.
`parse_pff_v2_table` types each `/v2` table from its own declared columns; id columns are `Int64`
by name even where PFF declares them `string` (it types an all-null column, and every column of an
empty answer, as `string`), and such typeless columns are `Null`, so a union across weeks keeps
the real dtypes.

The wire detail that matters: the new host **silently ignores** camelCase `franchiseId`/`gameId`
(it returns the whole leaderboard). The new wrappers send `franchise_id`/`game_id`; the legacy
`pff_*` / `pff_<league>_*` wrappers keep camelCase because `premium.pff.com` expects it.

The cookie-auth `premium.pff.com` wrappers are unchanged and still work, but every one now
documents itself as LEGACY, as do their runtime module and reference page. `parse_pff_report` and
`parse_pff_player_detail` now look past the `restricted` block the Developer API may place beside
a report envelope. Previously such a body came back as a dict or an empty frame.

Every `pff_api` return table now documents its columns (2,739 descriptions). Report metrics that
`/v1` already shipped reuse the legacy `native/pff` text; the `/v2`-only metrics (over-expected
rates, positive/negative graded-play rates, pass-rush side splits, true-pass-set rates) and the
team tables are described from PFF's own column labels and captured bodies. Each team-stats rank
states which end ranks first, read from PFF's captured rows (1 = highest EPA, 1 = fewest
turnovers). `native/pff_api` is off the deferred list, so the residual-description gate now covers
it. The new tables' returns-schemas are named `pff_api_<table>` (`pff_api_team_roster`, ...):
descriptions are looked up by that name, and the bare `team_roster`, `team_schedule`, `team_stats`
and `team_report` already belong to ESPN, MLB, CBS, NWSL and NHL tables, so one source's text
would have rendered on another's page (PFF packs height as feet x 100 + inches; ESPN gives inches).

Fixed scoreboard cache TTL selection when dates are supplied in query parameters:
current/future days and ranges containing them bypass both cache reads and writes,
while wholly historical dates retain the 30-day TTL. Explicit TTL overrides still
take precedence.

The legacy PFF return tables (`pff_*`, and the `pff_api` `/v1` routes that reuse them) no longer
borrow another source's column text: 277 columns that showed nflreadr/ESPN wording ("as reported
by NFL.com", "ESPN franchise id", "Player ID (aka GSIS ID)") now describe PFF's own values —
PFF team abbreviations and ids, per-target EPA, gross punt yards, one row per team line on the
pass-blocking-efficiency table. The description check now resolves a flat family's fallback text
with the league its page renders it with.

### Added — NBA officiating data: Last Two Minute reports, referee assignments, and cdn liveData

New module `sportsdataverse.nba.nba_officiating` reads official.nba.com:

- `nba_l2m(game_id)` returns one game's Last Two Minute report as three frames:
  - `calls`: one row per graded action, with the decision normalized to
    CC / CNC / IC / INC. A blank or "Undetectable" grade stays `null`, never INC.
  - `game`: one row of game metadata.
  - `stats`: the report's calls / errors-in-favor / possessions-in-favor block.
- `nba_l2m_games(season)` lists every game that has a report, from the season index
  page. JSON reports exist only from 2019-01-01.
- `nba_referee_assignments(date, league="nba"|"gl"|"wnba")` returns:
  - `officials`: one row per game and filled crew slot (an empty slot has no row).
    `crew_position` is the feed's order; slot 1 as crew chief is inferred, not labelled.
  - `replay_center`: one row per replay-center official for the date and league.
    The feed ties these to a date, not a game, so there is no game key.

  `wnba_referee_assignments()` is the WNBA shim.

official.nba.com needs a browser User-Agent: the default libcurl/curl UA gets an
Akamai 403. Its 403s mean two different things, and the module keeps them apart:

- An S3 `AccessDenied` body means the game has no report. It raises `NoDataError`.
- An Akamai HTML page means the fetch was blocked. It raises `AssetFetchError`.

New `sportsdataverse.nba.nba_live` / `sportsdataverse.wnba.wnba_live` wrap the
cdn.nba.com / cdn.wnba.com liveData feeds: `nba_live_pbp()` / `nba_live_boxscore()`,
plus the WNBA twins.

- The play-by-play carries `official_id` on every foul (2019-20 on) and wall-clock
  `time_actual`; it joins to `nba_referee_assignments()` on `official_id`. Only
  2pt/3pt shots carry coordinates (`x_legacy` / `y_legacy`); fouls and blocks carry
  a court zone (`area` / `area_detail`).
- The cdn refuses requests carrying a plain client's default headers (a 403 page),
  so these use the same curl_cffi Chrome impersonation as stats.nba.com, which
  sends a browser-consistent request.
- Every frame carries a typed core column set, even for a game with no actions.
- Late-first-seen fields are kept, because schema inference scans every row.

Every function here follows the same error rules:

- A bad argument raises `ValueError` before any request: a `game_id` that is not
  one non-negative integer id of at most 10 digits (a bool, a negative or fractional
  number, a string that is not all digits, or a longer id), a `season` that is not a 4-digit year, a `date` that is
  not one valid `YYYY-MM-DD`, or an unknown `league`.
- "No data" raises `NoDataError`: a 404, or S3's `AccessDenied` 403. That covers a
  game without an L2M report or a liveData object, and a season without a listing page.
- A failed fetch raises `AssetFetchError`. That means a transport error, an Akamai
  or WAF block, any other non-200 status, or a 200 without the expected shape:
  - a body that is not a JSON object;
  - an L2M report whose `game` is not exactly one row (the parser reads one row,
    so a second would vanish silently) or whose `l2m` / `stats` table is not a list
    of records (an empty record would become a made-up all-null row), or a liveData
    body without its `game` object;
  - a referee block whose `Table` / `Table1` rows are missing or malformed,
    including a `Table` row without `game_id` or a `Table1` (replay-center) row
    without a `replaycenter_official` name;
  - a listing page without its "Last Two Minute" marker, or whose report links
    the parser cannot read.

  `raw=True` runs the same checks. On the referee feed it checks all three
  leagues, since it returns the whole payload.
- The liveData fetch retries throttles, 5xx and transport errors on the
  `SDV_PY_NBA_STATS_RETRIES` / `SDV_PY_NBA_STATS_BACKOFF` budget that `nba_stats_*`
  uses (default: no retry). A missing curl_cffi raises `ImportError` and is never
  retried.
- The parsers never raise. A malformed envelope gives zero-row frames with the
  documented schema, and a cell of the wrong type becomes null (an object or list in
  a text column is kept as JSON text).

Port of atlhawksfanatic/L2M's scraping logic (MIT).

### Changed — the CFB vendor special-teams name patterns moved into the shared football grammar

Three CFB-local regexes (`_VENDOR_FG_KICKER_RE`, `_VENDOR_KICKOFF_RETURNER_RE`,
`_VENDOR_PUNT_RETURNER_RE`) read a kicker or returner whose name is not the abbreviated
"X.Surname" shape -- stats.ncaa.org's surname-first "Arreola,Carlos" and 2005-2014's
spelled-out "Bryan Hahnfeldt". They are now one name expression in
`sportsdataverse.football.espn_text` (`CLAUSE_NAME`) plus the two anchors built from it
(`CLAUSE_RETURNER_RE`, one expression for punts and kickoffs since the call site already
knows which kick it has, and `CLAUSE_FG_KICKER_RE`), and `cfb_pbp` keeps no name regex of
its own for them. Output is unchanged: replayed over every local ESPN summary (3,227,541
plays / 20,718 games, 2004-2026), `punt_return_player_name`, `kickoff_return_player_name`
and `fg_kicker_player_name` are identical -- 0 lost, 0 changed, 0 gained.

### Added — CFB kick distances and bare-punt returns derived from field position, with provenance

ESPN's 2004 play text states no kick distance at all ("Punt by Vinnie Burns (VT)
returned 15 yards by Reggie Bush (USC) to the Trojans 21.", "Trojans kickoff,
touchback by Hokies."), so `yds_punted` was 2% filled and `yds_kickoff` empty for the
season; and 29% of 2023 punts read only "Alex Weir punt for 44 yds", leaving
`yds_punt_return` null. `CFBPlayProcess` now fills those nulls from ESPN's own field
position at the end of the yardage step, and three new columns say where every value
came from: `yds_punted_source`, `yds_kickoff_source`, `yds_punt_return_source` --
`"text"` (present before, parsed or a flag convention), `"derived"`, or null. A
parsed value is never changed.

- **Punt distance**: `start.yardsToEndzone - landing`, `landing = (100 -
  end.yardsToEndzone) - yds_punt_return`; a touchback is the distance to the goal line
  (the text convention: 98.9-100% of stated touchback punts per sampled season).
- **Kickoff distance**: the kick spot minus the landing, touchback = the spot. The spot
  is ESPN's `start.yardsToEndzone` from 2005 (65 from the 35, and 70 in 2007-2011 when
  kickoffs moved to the 30 -- a fixed 65 is exact on 0.06% of 2009 non-touchback
  kickoffs). 2004
  stores the catch spot there instead, so 2004 assumes the 35, requires the computed
  landing to equal ESPN's catch spot, and skips kicks after a flag or safety.
- **Bare-punt return**: `(100 - end) - (start - yds_punted)` when positive, the next snap
  starts at that spot with the receiving team, and the text either describes no outcome or
  states only that the returner stepped out of bounds -- "Jared Ballman punt for 48 yards,
  returned by Ryan Broyles out-of-bounds.", a return whose length ESPN never gives (41 such
  rows in a 1,329-game 2004-2026 sample, 25 of them derivable under the guards above). Returner names are not recoverable.
- **Never derived**: penalties, fumbles, muffs, blocks, laterals, safeties,
  touchdowns, onside kicks, out-of-bounds *kickoffs*, "for a 1ST down", unchanged
  possession, out-of-range values (punt 0-80, kickoff 0-75, landing 10+ yards deep), a
  no-return punt ending exactly at the 20 (a 2004 touchback reads the same), an
  end-zone punt with the receiver at the 20, and a derived return of exactly 5 or 15
  (what an unrecorded flag looks like).
- A punt out of bounds **is** derived, unlike a kickoff out of bounds: a kickoff out of
  bounds is spotted by rule (the receiving team's 35, stored as a 40-yard return), so
  ESPN's end spot is a placement, while a punt out of bounds is dead where it crossed
  the sideline -- the landing spot. On the 22 `punt_oob` rows of a 2005-2025 sample the
  field position reproduces the stated distance exactly 22 times.

Validated offline on 1,749 stored games (250 per season, 2004/2005/2009/2015/2023-2025;
one 2009 game has no play-by-play).
With the parsed value hidden, derived punt distances match the text on 98.7-99.3% of
derived punts in 2005/2009/2015 (87.7-91.5% in 2023-2025, where the stated distance
already disagrees with the field position on 10-24% of punts) and kickoff distances on 99.3-99.7%
(95.8-99.2% in 2023-2025). Simulated bare punts return the parsed yardage exactly on
95.2-97.9% of derived returns in 2005-2015 and 82.2-87.4% in 2023-2025, with 0.9-3.1%
of unreturned punts given a return. Against ESPN's box score, which neither the text
nor the field position feeds: 2004 team-game punting yards match exactly as often
with derived distances (23.6%) as 2005's parsed distances do (22.9%), and on the 105
2023 team-games that gained a derived return, 102 moved closer to the box's punt-return
yards and 3 farther. Sample fill: 2004 `yds_punted` 1.9% -> 86.2% and `yds_kickoff`
0% -> 90.6%; 2023 `yds_punt_return` 68.9% -> 76.4%.

### Changed — `cfb_returning_production` measures defense from play participants and weights it into `overall_returning`

`def_returning` was built from ESPN's per-game defensive player box, which covers 0% of
teams in 2014-2015 and 17-65% through 2023, so the column was null for most of the league
before 2024 and the fitted FBS weights were offense 1.0 / defense 0.0: every published
`overall_returning` equalled `off_returning`. Defense now comes from play participants
when the production season is 2014+, 92-100% of teams every season (all of FBS), and
from play-by-play splash ids (sacks, interceptions, pass breakups, forced fumbles) for
2004-2013. Participants are scored the way the box counts the same plays (every
tackler, assister or sacker a tackle; a sack 2.0 split across sackers; one tackle for
loss shared on a play that lost yardage or had a sack; a pass defended 1.0), and each
defender is credited to the team their game roster lists them on, because on punts
and turnover returns the tacklers play for the team in possession. Against the box
where both are near-complete they track it at player r = 0.944 (2024) and 0.980 (2025).
A season whose participants join fewer than 95% of pbp plays warns; a missing source
release keeps the box defense.

New columns `def_basis` (`participants` / `pbp_splash` / `box`) and `overall_basis`
(`offense+defense`, or `offense` for a team with no defensive value, whose overall
then equals `off_returning`) say which measure each row used. The splash measure has
no tackle volume and is not on the participants' scale.
They are returned by `cfb_returning_production()` now and reach the
`load_cfb_returning_production` release asset when it is next rebuilt.

**`overall_returning` values change.** FBS weights are refitted on the corrected
metric: offense 0.49 / defense 0.51 (FBS 2018-2025, n = 1,017; standardized
coefficients off +1.15, def +1.21). Spearman against the next season's scoring-margin
change rises from 0.173 (offense only) to 0.205, and on the splash era the fit never
saw (2005-2014, n = 1,213) from 0.239 to 0.299 (gain 95% team-cluster interval
[+0.022, +0.097]). Fitted on 2018-2023 and scored on 2024-2025 the gain is +0.010,
an interval spanning zero. The retention gate is re-baselined on a recaptured
2005-2025 fixture (floors 0.18 over 2018-2025 and 0.24 over 2018-2023; the retired
gate's fixture scores 0.156 under the new weights, its defense column being the
retired measure), with held-out gates for 2024-2025 and the splash era, a
fixture coverage/level gate, and the shipped weights tested against the committed
fit. The new results fixture keeps completed games only: canceled and postponed
games carry 0-0 scores (164 in 2016-2024) that the earlier capture admitted.

### Fixed — fantasy-football ids are strings, pinned instead of inferred from each DynastyProcess release

`load_nfl_ff_playerids` / `load_ff_playerids` and the CSV-backed kinds of
`load_nfl_ff_rankings` / `load_ff_rankings` read DynastyProcess CSVs with polars' type
inference, so an id column's dtype depended on whatever the current upstream release
happened to contain. `fantasypros_id`, `pff_id` and `nfl_id` used to come back as strings
and now infer as `Int64`; ids that are null in the first 100 rows (`yahoo_id`,
`fleaflicker_id`, `rotoworld_id`, `swish_id`) flip whenever upstream reorders. Integer
inference also dropped zero-padding: 225 `mfl_id` values such as `"0156"`, and `nfl_id`
`"038666"`. Every id column is now pinned to `Utf8` at read time. That matches upstream's
own `db_playerids.rds` (all 20 ids are character), keeps the padding, and lines the ids up
with the `Utf8` ids of `build_nfl_rosters` / `build_nfl_players` and with each other, so
cross-loader joins no longer depend on the release.

Returned dtype changes from `Int64` to `Utf8`:

- `load_nfl_ff_playerids`: `mfl_id`, `fantasypros_id`, `pff_id`, `sleeper_id`, `nfl_id`,
  `espn_id`, `cbs_id`, `rotowire_id`, `ktc_id`, `stats_id`, `stats_global_id`,
  `fantasy_data_id`. The other eight id columns were already strings and are now pinned
  so they cannot flip.
- `load_nfl_ff_rankings(kind="draft")`: `id`, the FantasyPros id that joins to
  `load_nfl_ff_playerids`' `fantasypros_id`. `sportsdata_id`, `yahoo_id` and `cbs_id` are
  pinned `Utf8` (already strings).
- `load_nfl_ff_rankings(kind="week")`: `fantasypros_id`. `player_opponent_id` is pinned
  `Utf8` (already a string).

`kind="all"` reads upstream's parquet, which already stores these ids as strings. Code
that joined or compared these ids as integers needs to cast its own side to `pl.Utf8`.

Cached frames keep their old dtypes until the cache entry expires, so call
`sportsdataverse.nfl.clear_cache()` after upgrading (it matters most with
`cache_mode="filesystem"`, which persists across processes).

### Fixed — the usage box glued shared tackles into one phantom player and read positions only from participants

`cfbfastR-cfb-raw` stores each game's play participants with every list cell written
as a numpy array's `str()` -- `"['5152441' '5220449']"`, no commas. The usage box
decoded those cells with `ast.literal_eval`, which reads two adjacent string literals
as ONE concatenated string without raising, so every two-player assist became a single
phantom tackler (`51524415220449`, "Jon JohnsonBrett Karhu") and the real players lost
the credit. In a sample of stored games every multi-player cell had this shape (1,528
of 1,528); game 401760404 produced 72 `tackles` rows instead of 43. List cells now
decode by tokenizing, so the JSON, Python-repr and numpy-repr shapes (line-wrapped,
double-quoted names such as "D'Andre Swift", bare numbers) all yield the same items.

`create_usage_box` also only knew a player's position from the participants'
`{type}_position_id` columns, which the stored CFB participants predate, so
`position_group_usage` and `position_group_tackles` were empty for every historical
CFB game. It takes an optional `rosters` (a frame, a list of athlete records or the
stored `{"data": [...]}` envelope) and fills the group of any athlete the participants
did not classify from `position_id`, or the id inside `position_href`; a participant's
own position still wins. `CFBPlayProcess` passes its supplied `game_roster`. On stored
2014-2025 games the roster resolves 97-100% of participant ids; 2004-2013 have no play
participants at all.

### Fixed — CFB special teams read ESPN's 2025 jersey-style text; the usage box keys a kicker once

ESPN's 2025 college feed writes kicks the way the NFL feed does -- "(04:07) #43
M.Chiumento punt 43 yards to the OSU36 #0 B.Inniss return 16 yards to the TEX48
(#81 N.Townsend), out of bounds", "#49 M.Diomede kickoff 65 yards to the TEX00,
Touchback", "#96 C.Hawkins field goal attempt from 26 yards GOOD" -- and the CFB
processor only read "punt for N yards", "kickoff for N yards", "N Yd Field Goal"
and "returned by X for N yards". On those games (401856682, Ohio State @ Texas,
and many more) `yds_punted`, `yds_kickoff`, `yds_fg`, `yds_punt_return` and
`yds_kickoff_return` were null on every kick, as were the returner and
fair-catcher names, and a returner stepping out of bounds was counted as a punt
out of bounds. The jersey-style clauses now fill the distances, the return
yards, the touchback and fair catch, and the punter / kicker / returner /
fair-catcher names from the text; ESPN's participants still overwrite the
names wherever they exist (the abbreviated text name is the fallback, as in the
NFL processor), and the older phrasings are unchanged. The abbreviated-name
pattern the NFL grammar was built on moved to
`sportsdataverse.football.espn_text`, which both processors share. Verified on
the committed 401856682 summary and participants fixtures.

`create_usage_box` keyed each special-teams source on `coalesce(player_id,
player_name)` separately, so a kicker whose kickoffs carried his id (ESPN's
participants) and whose field goals carried only his name (the play text)
appeared twice in `st_kickers` -- "Eli Ozick" with id 5157006 and six kickoffs,
and again with a null id and the field-goal line. Every (id, name) pair seen on
any source now resolves one key per team for kickers, punters, returners and
blockers, and the merged row carries the resolved id and name.

### Added — loaders for the ESPN football usage leaderboards and team / coach tendencies

Twenty-eight dataset loaders over the release tags `cfbfastR-cfb-data` and `nfl-data`
publish from `sportsdataverse.football.usage_box` and `tendencies`: the eleven usage
sections (`load_{cfb,nfl}_usage_players`, `_usage_position_groups`, `_usage_tackles`,
`_usage_position_group_tackles`, `_usage_teams`, `_usage_drive_scripting`,
`_usage_st_kickers`, `_usage_st_punters`, `_usage_st_returners`, `_usage_st_blocks`,
`_usage_st_team`), `load_{cfb,nfl}_team_tendencies`, `load_{cfb,nfl}_coach_tendencies`,
and the season-less `load_{cfb,nfl}_coach_careers()`. One parquet per season
(`{stem}_{season}.parquet`, CFB from 2004, NFL from 2002), unioned with
`diagonal_relaxed`; the CFB loaders are generated from `releases.yaml` (a missing
season is skipped with a warning), the NFL ones are hand-written in `nfl_loaders.py`
(a missing season raises `NoDataError`, matching its siblings). Returns tables are
derived from the published parquets and every column is described from the
producers' semantics; published coverage caveats (NFL 2005 has no play text upstream,
the participant-based sections start in 2014, the kicker / punter / returner tags
have no 2005-2007 assets) live in each loader's `notes:` / docstring.

The loader codegen learned a season-less form: a `releases.yaml` url with no
`{season}` token now renders a `fn(return_as_pandas=False)` loader that reads one
asset (an absent asset is an empty frame plus a warning), and the loaders page
renders its example as `fn()`.

### Added — team and coach tendencies (`sportsdataverse.football.tendencies`)

`tendencies(plays, league=)` folds a season of processed plays (either
processor's output) into one row per group -- `(season, pos_team)` by default,
or `(season, coach)` when the caller attaches a coach column -- with pace
(seconds per play from the drive clock, plays per game and per drive, with a
coverage share so pre-clock seasons read as missing rather than wrong),
run/pass splits by down, by score state (leading / tied / trailing) and in
situation-neutral snaps (win probability 20-80%, regulation, outside the last
two minutes of a half), early-down and neutral pass rates, explosive and
success rates, EPA per play, third downs over expected, red-zone and
scoring-opportunity trips with TD rate, points per trip and success, scripted
vs non-scripted drive efficiency, and fourth-down decision making (go rate,
agreement with the bundled fourth-down model, go rate when the model says go,
go rate when it says kick, conversion rate when going, win probability left on
the field by deciding against the model). Every rate carries its numerator and
denominator (`RATES`), so `aggregate_tendencies(frames, keys=)` sums seasons
into careers and recomputes the rates exactly. A defense twin (`def_*`) is
computed by the defending key so a coach's defense is judged on what it
allowed. Expected third downs stay null, never zero, when no curve is
available.

### Added — usage and situational box (`sportsdataverse.football.usage_box`)

Six new `advBoxScore` sections on BOTH football processors, computed once in the
shared football layer from the processed plays and the per-play participants:
`player_usage` (explosive plays, first downs, touchdowns, first-down +
touchdown rate, target share, first-down share, red-zone and
scoring-opportunity touches / targets / touchdowns, third downs converted
over expected), `position_group_usage`, `tackles` (tackle share:
tackles + 0.5 assists over the team total), `position_group_tackles`,
`team_usage` (third downs over expected, red-zone and scoring-opportunity
efficiencies: trips, TD rate, points per trip, success, EPA per play) and
`drive_scripting` (scripted = a team's first two drives of each half vs the
rest). `aggregate_usage_box` sums per-game rows into season leaderboards and
recomputes every rate. The participants pivot now also emits
`{type}_position_id`; `sportsdataverse.football.positions` maps ESPN position
ids to abbreviations and groups. Bundled third-down conversion curves
(`{cfb,nfl}/models/{league}_third_down_conversion.parquet`, isotonic in yards to
go; NFL 2002-2025, CFB 2022-2025) feed the "over expected" columns and refit
with `fit_third_down_curve`.

### Added — NFL field-position EP curve (`nfl_field_position`)

`load_nfl_fp_curve()` loads the bundled `nfl/models/nfl_field_position_ep.parquet`
(EP of a drive start by own yard line, 1..99), the NFL twin of the college curve
and fit with the same recipe -- weighted isotonic regression of realized drive
points on the starting yard line -- so the two leagues' field-position margins
are comparable. `fit_nfl_field_position_ep(pbp)` refits it from released
`espn_nfl_pbp` plays; the bundled artifact is the 2016-2025 fit (59,026 drives).

### Added — offline processor inputs (#491)

`espn_nfl_pbp(summary=)` / `espn_cfb_pbp(summary=)` run the processor over a
stored ESPN summary with no network (participants, roster and odds fetches all
gated); `play_participants_from_items` + `athlete_lookup_from_summary` build the
participants frame from stored core play items; `NFLPlayProcess(odds_override=)`
mirrors the CFB contract and `odds_source` records which branch resolved the line.

### Added — CFB drive summary and situational team stats, graduated from Game on Paper (#470)

`cfb_drive_summary.create_drive_summary(drives, frame, home_id, away_id,
periods=None)` builds the StatBroadcast-style drive summary — per-team drive
lines (both named drive-success metrics, points off turnovers, forced
three-and-outs, TOP by quarter, first-down sources), the OBTAINED/HOW-LOST
drive chart, how-scores-happened, and per-team long-play lists — windowable
by start-quarter set or `"ot"`. `cfb_situational_stats.create_situational_stats(frame,
home_id, away_id, window_expr=None)` builds the per-team situational block
(down-by-down with distance buckets and conversion attribution, red zone,
finishing drives, rushing tiers, passing profile, 4th-down decision report,
score state, penalties, havoc, turnovers, field zones, pace, big plays),
windowable via a polars filter with window-inherent sections omitted on
windowed builds. Thin `CFBPlayProcess.create_drive_summary` /
`.create_situational_stats` delegates mirror `create_box_score`. Both consume
the post-pipeline `plays_frame`; drive-level attribution reads the drives
grouping (`drive.team`), never plays grouped by `drive.id`.

### Fixed — Statcast search runner ids are Int64, not Float64

`mlb_statcast_search`, `mlb_statcast_search_minors` and `mlb_statcast_search_wbc`
returned `on_1b` / `on_2b` / `on_3b` as Float64 (`660271.0`): pandas reads any
integer CSV column holding a blank as float, and a base is blank whenever it is
empty. Float ids break joins against `batter` / `pitcher` and stringify as
`"660271.0"`. The 14 MLBAM id columns (`batter`, `pitcher`, `on_1b`..`on_3b`,
`fielder_2`..`fielder_9`, `game_pk`) are now pinned to nullable Int64 (blank ->
null), in polars and in `return_as_pandas=True` output, and the Returns docs say
`integer`. An id column holding a non-integral value is left as read and warned
about once per call rather than truncated.

**Returned dtypes change:** `on_1b` / `on_2b` / `on_3b` go from Float64 to Int64,
and pandas output from `parse_mlb_statcast_search` gives nullable `Int64` instead
of numpy `int64` for the other ids.

### Fixed — MLB expected stats counted raw pitches as plate appearances

`mlb_expected_stats` counted every non-batted-ball *pitch* row toward `pa`
and `ab` — published batter-seasons carried `pa` up to ~3,400, deflating
`xba`/`xslg` to ~.05 — and trusted each cache vintage's
`woba_value`/`woba_denom` semantics, which corrupted the xwOBA scale per
season (qualified league means of .34–.72 shipped past the rank-based
gates, which are scale-blind by construction). `pa`/`ab` and the wOBA
denominator now count only plate-appearance-ending rows (`events`
non-null), the denominator is derived from events (PA enders minus
intentional walks, sac bunts and catcher interference) rather than read
from `woba_denom`, PA-ending events with a null `woba_value` get fixed
fallback weights (walk .69, HBP .72), and `intent_walk` no longer counts
as an at-bat. Downstream, `baseballr-data` now enforces an absolute
league-mean scale gate at publish; the full-history republish of
`mlb_hitting_models` is the tracked follow-up.
