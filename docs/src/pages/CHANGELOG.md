---
title: Changelog
---

# Changelog

The newest releases. Changes merged since 0.1.5 are under [Unreleased](/changelog-unreleased); older releases are in the [archive](/changelog-archive).

## 0.1.5 Release: October 8, 2026

**Highlights**

- Failed fetches raise instead of returning empty frames: `AssetFetchError` (the fetch failed),
  `NoDataError` (nothing there) or `ValueError` (bad request), across ESPN, stats.nba.com, the
  generated flat APIs and HockeyTech.
- polars 2.x is supported (`polars>=1.0,<3`); the code runs on both polars 1.x and 2.0.
- New sources: the official PFF Developer API (`pff_api_*`), Formula 1 (`f1_*`), EuroLeague live
  data, kloppy soccer event data with SPADL and Expected Threat, Polymarket, Kalshi and more.
- Football: one play-by-play shape from Shield, Yahoo, CBS, NCAA and Fox feeds; team and coach
  tendencies and the usage box; many CFB EPA / WP fixes that need a reprocess.
- `sdv-docs`: an MCP server that answers exact questions about the package from a docs index.

### Breaking changes

- **Errors:** a failed fetch in generated flat-API wrappers (`espn_*`, `fox_api_*`, `cbs_*`,
  `mlb_api_*`, NHL, `asa_*`) and other getters (`mlb_statcast_*`, `torvik_*`, `kenpom_*`, `on3_*`,
  `yahoo_*`, `nfl_api_*`, ...) raises `AssetFetchError` (400 / 422: `ValueError`), never "no data".
- **ESPN (cross-league):** the hand-written scrapers (`espn_<league>_schedule` / `_teams` / `_pbp`,
  `espn_*_player_stats`, `bref_*`, `build_nfl_players`, ...) raise `AssetFetchError` on a failed
  fetch and `ValueError` on 400 / 422 instead of parsing the error body as data.
- **HockeyTech:** season names read as their end year, so `season=` can pick another season (WHL /
  KIJHL `season=2026` is now 2025-26); one-year preseasons answer to the next year, exhibitions are
  `game_type="exhibition"`, the default is the newest regular season, and `season_yr` is `Int64`.
- **HockeyTech:** `hockeytech_api` (PWHL and the 19 other leagues) raises `AssetFetchError` on a
  failed fetch or HTTP-200 error sentinel and `NoDataError` on a 404 instead of returning an empty
  frame, as do season-defaulting wrappers whose seasons lookup fails.
- **MLB:** `mlb_statcast_search` / `_search_minors` / `_search_wbc` pin the 14 MLBAM id columns to
  nullable `Int64`: `on_1b` / `on_2b` / `on_3b` were Float64 (`660271.0`), and pandas output from
  `parse_mlb_statcast_search` gives nullable `Int64` instead of numpy `int64`.
- **NBA / WNBA:** `nba_stats_*` / `wnba_stats_*` raise `AssetFetchError` instead of returning `{}`
  (404: `NoDataError`, 400 / 422: `ValueError`, also for `nba_live_*`, `nba_l2m*`,
  `nba_referee_assignments`); `wnba_on_court` / `wnba_possessions` raise on a failed rotation fetch.
- **NBA / WNBA:** `nba_stats_*` / `wnba_stats_*` season arguments default to the latest season with
  rows and each league's own default ids, so endpoints that summed every season (`drafthistory`,
  `leaguegamefinder`, `playergamestreakfinder`, ...) return one season. (#391)
- **NFL:** the 11 per-type loaders deprecated since 0.0.68 (`load_nfl_ngs_passing` / `_rushing` /
  `_receiving` and eight `load_nfl_pfr_*` per-type and weekly variants) are removed, with their
  `sportsdataverse.parsed.nfl` aliases.
- **NFL:** fantasy-football id columns are `Utf8` (were `Int64`): `load_nfl_ff_playerids` (`mfl_id`,
  `fantasypros_id`, `pff_id`, `nfl_id`, ...), `load_nfl_ff_rankings(kind="draft")` `id` and
  `kind="week"` `fantasypros_id`; zero-padded ids such as `"0156"` keep their padding.
- **Soccer / EuroLeague:** the ASA, FIFA, FotMob, UEFA, MLS, NWSL and EuroLeague frames keep
  nullable types: a nullable boolean is `Boolean` (was a string with `"nan"`), a nullable integer is
  `Int64` (was `Float64`), and an all-null column is `Utf8`.

**Upgrade notes** —

- **Errors:** catch `AssetFetchError` where a loop must keep going; `except NoDataError` keeps
  skipping absent resources; `except SportsDataverseError` catches both, timeouts included; a
  `ValueError` means the call itself needs fixing. `EmptyResponseWarning` now fires only for a 2xx
  with an empty object. Code that caught `requests.HTTPError` from `nfl_api_*` or read
  `exc.response` must catch the new types (read the status from the message). HockeyTech wrappers
  that default the season raise when the seasons lookup fails; pass `season_id=` to skip it:

  ```python
  import sportsdataverse as sdv
  from sportsdataverse.errors import AssetFetchError, NoDataError

  try:
      df = sdv.ahl_schedule()
  except NoDataError:
      df = None  # the fetch worked and there is nothing there: skip it
  except AssetFetchError:
      raise  # the fetch failed: retry later, never record it as empty
  ```

- **HockeyTech seasons:** a caller who passed the start year to get a season should pass the end
  year; preseasons answer to the next year (`resolve_season_id("ohl", season=2027,
  game_type="preseason")` is the "2026 Pre-season"); pass `season_id=` where divisions listed side
  by side for one year are not told apart (e.g. BCHL 2024).
- **nba_stats / wnba_stats:** pass `season` explicitly for a lockout or pandemic calendar; for every
  season pass an empty one (`season_year_nullable=""` for `drafthistory`, `season_nullable=""` for
  `leaguegamefinder` / `playergamestreakfinder`).
- **NFL loaders:** replace `load_nfl_ngs_passing`, `load_nfl_ngs_rushing` and
  `load_nfl_ngs_receiving` with `load_nfl_nextgen_stats(seasons, stat_type=...)`, and
  `load_nfl_pfr_pass`, `load_nfl_pfr_weekly_pass`, `load_nfl_pfr_rush`, `load_nfl_pfr_weekly_rush`,
  `load_nfl_pfr_rec`, `load_nfl_pfr_weekly_rec`, `load_nfl_pfr_def` and `load_nfl_pfr_weekly_def`
  with `load_nfl_pfr_advstats(seasons, stat_type=..., summary_level=...)`.
- **NFL fantasy ids:** code that joined or compared these ids as integers should cast its own side
  to `pl.Utf8`; call `sportsdataverse.nfl.clear_cache()` after upgrading, since cached frames keep
  their old dtypes (above all with `cache_mode="filesystem"`).

### Added

- **Analytics:** `rolling_windows` computes event-count form windows with prev-season, season and
  career baselines, extended to stats.nba / stats.wnba shot events. (#590, #657)
- **Analytics:** `metric_curves` fits rate curves along a continuous axis (shot distance, field
  position), binned on the coordinate distance rather than the nominal label. (#652, #658)
- **Analytics:** `defense_vs_position` reports what each defense allowed to QB, RB, WR and TE,
  filtered by the season-type column. (#659, #660)
- **Basketball:** KenPom, Her Hoop Stats, Basketball-Reference, RealGM and the salary, draft and
  injury surfaces (#451), then Bart Torvik's `torvik_game_stats`, `torvik_player_stats` and the
  public `torvik_game_schedule` (#678).
- **CFB:** `paper_index` ports Game on Paper's Paper Index and season deserved wins. (#661)
- **CFB:** `CFBPlayProcess` fills missing `yds_punted`, `yds_kickoff` and bare-punt
  `yds_punt_return` from ESPN field position, with new `yds_punted_source`, `yds_kickoff_source` and
  `yds_punt_return_source` columns (`"text"`, `"derived"` or null); parsed values never change.
- **CFB:** `cfb_drive_summary.create_drive_summary()` (StatBroadcast-style drive summary) and
  `cfb_situational_stats.create_situational_stats()` (per-team situational block), with
  `CFBPlayProcess` delegates; graduated from Game on Paper. (#470)
- **ESPN (cross-league):** the ESPN CDN family (`cdn.espn.com/core`) as generated
  `espn_<league>_cdn_*` wrappers, with a per-endpoint league allowlist from a live probe. (#681)
- **EuroLeague:** 8 new wrappers: `euroleague_game_points()` (shot chart), `euroleague_game_pbp()`,
  `euroleague_game_boxscore()`, `euroleague_game_header()`, `euroleague_standings(kind=)`,
  `euroleague_player_stats()`, `euroleague_team_stats()` and `euroleague_game_report()`.
- **F1:** `sportsdataverse.f1` wraps the keyless Jolpica (Ergast-compatible) API: `f1_schedule`,
  `f1_race`, `f1_results`, `f1_qualifying`, `f1_sprint`, `f1_pitstops`, standings, drivers,
  constructors, circuits and the paged `f1_laps(season, round)`; data is CC BY-NC-SA 4.0.
- **Fox:** `fox_api_*` wraps `api.foxsports.com` directly (33 endpoints), beside the per-league
  `fox_<league>_*` Bifrost wrappers. (#680)
- **G League:** ESPN's G League is a documented league: the universal family (112 wrappers:
  `espn_nbagl_standings`, `espn_nbagl_scoreboard`, `espn_nbagl_summary`, ...) is exported from
  `sportsdataverse.nbagl` and the top-level package. (#684)
- **MLB:** `load_mlb_park_dimensions()` loads one row per MLB venue per season from 2001: fence
  distances, capacity, turf, roof, elevation and coordinates, with `venue_id` as a string.
- **NBA:** `sportsdataverse.nba.nba_officiating` reads official.nba.com: `nba_l2m(game_id)`,
  `nba_l2m_games(season)`, `nba_referee_assignments(date, league=)`, `wnba_referee_assignments()`;
  `nba_live_pbp()` / `nba_live_boxscore()` and WNBA twins read the cdn liveData feeds. (#592)
- **NFL:** NFL Pro Next Gen Stats: the `pro.nfl.com` family (`nfl_pro_*`, 16 endpoints) and
  `load_nfl_ngs(seasons, dataset=)` over the SDV-native `nfl_ngs_*` releases (12 datasets).
  (#454, #481, #489)
- **NFL:** `sportsdataverse.nfl.shield_pbp` graduates the `native_pbp` parser and adds its live
  layer (phase, provisional rows, current situation) and the `shield_nfl_pbp` entry point.
  (#528, #536)
- **NFL:** `load_nfl_fp_curve()` loads the bundled NFL field-position EP curve (2016-2025 fit), and
  `fit_nfl_field_position_ep(pbp)` refits it.
- **NFL / CFB:** `NFLPlayProcess` / `CFBPlayProcess` accept `source=` and build the same frame from
  Shield (#540), Yahoo (#541, #543), CBS (#542, #545), NCAA (#544) and Fox (#548) feeds through
  `sportsdataverse.football.sources`; the dispatch table landed in #525.
- **NFL / CFB:** `football.tendencies` (team and coach splits) and `football.usage_box` (usage,
  situational and special-teams box), with EPA and game-context cuts, read back by 28 loaders such
  as `load_{cfb,nfl}_team_tendencies` and `load_{cfb,nfl}_usage_players`. (#496, #497, #498, #614)
- **NFL / CFB:** `espn_nfl_pbp(summary=)` / `espn_cfb_pbp(summary=)` process a stored ESPN summary
  with no network, and `NFLPlayProcess(odds_override=)` mirrors the CFB contract. (#491)
- **PFF:** the official Developer API: 68 `pff_api_*` wrappers (`sportsdataverse.nfl.pff_api`),
  keyed by `api_key=`, `PFF_API_KEY` or `SDV_PY_PFF_API_KEY`; `strict=True` / `SDV_PY_PFF_STRICT=1`
  raises on withheld columns. The cookie-auth `pff_*` premium wrappers still work, marked LEGACY.
- **Providers:** 46 wrappers in six new families: `espn_content_*`, `thesportsdb_*`
  (`$THESPORTSDB_API_KEY`), `football_data_*` (Football-Data.co.uk), `openligadb_*`, and the
  read-only prediction markets `polymarket_*` and `kalshi_*` under `sportsdataverse.odds`.
- **Providers:** six documented provider APIs, each generated from its own endpoint YAML: CBS NAPI
  (`cbs_napi_*`), Yahoo Shangrila (`yahoo_shangrila_*`), Fox, ASA (`asa_*`), MLS (`mls_api_*`) and
  NWSL (`nwsl_api_*`). (#452)
- **Reference data:** 36 loaders over the `{league}_groups` tags for nine leagues:
  `load_<league>_groups()`, `load_<league>_group_seasons()`, `load_<league>_group_aliases()` and
  `load_<league>_team_group_seasons(seasons)`; NCAA baseball is `load_ncaa_baseball_*`.
- **Registry:** `sportsdataverse.registry` (`metrics.yaml`) defines each published football metric
  once; `resolve(column)` maps a column to its entry, and `python -m sportsdataverse.registry --ts
  --target gop|web` renders a TypeScript module. (#645)
- **sdv-docs:** `sdv-docs`, a stdio MCP server over a published docs index with six read-only tools
  (`search`, `get_function`, `find_columns`, `find_endpoints`, `list_datasets`, `index_info`);
  install `sportsdataverse[mcp]` (Python 3.10+); `SDV_DOCS_DB=<file>` uses a local index.
- **Soccer:** `XThreat` fits an Expected Threat grid and `soccer_xthreat_rate(actions)` adds
  `xt_value`; `load_xthreat_model()` loads a bundled 12 x 16 grid fit on StatsBomb open data
  (research / non-commercial use only).
- **Soccer:** `soccer_spadl(dataset)` converts any kloppy event dataset into SPADL actions (a
  socceraction port); `soccer_open_dataset()` returns the dataset behind `soccer_open_events()`.
- **Soccer:** `asa_players_xpass(league_slug, season_name=...)` and the `nasl` and `usls` ASA
  leagues; USL Super League takes split-year labels (`season_name="2024-25"`, not `2024`).
- **Soccer:** the optional `soccer` extra (`pip install "sportsdataverse[soccer]"`, kloppy) adds
  `soccer_open_events("statsbomb", 8658)` for open event data and `soccer_events_to_frame(dataset)`
  for any kloppy dataset.
- **Validation:** `sportsdataverse.validation` gains `validate_game` + `GameReport`, the advBoxScore
  reconciliation rules, NCAA source column aliases and the football processor invariant sweeps.
  (#553, #554, #555, #556, #558, #510)

### Changed

- **CFB:** the bundled xQBR model (`cfb/models/qbr_model.ubj`) is retrained on the served box score
  without the spread (`qbr_vars` is the five EPA aggregates plus `era0..era3`), behind a publish
  gate; every `exp_qbr` changes.
- **CFB:** the vendor special-teams name regexes moved into `sportsdataverse.football.espn_text`
  (`CLAUSE_NAME`, `CLAUSE_RETURNER_RE`, `CLAUSE_FG_KICKER_RE`); output is unchanged.
- **CFB:** `cfb_returning_production` measures defense from play participants (2014+) or pbp splash
  plays (2004-13) and weights it into `overall_returning` (offense 0.49 / defense 0.51), so
  `overall_returning` values change; new `def_basis` and `overall_basis` columns.
- **Docs:** `generate.py --check` fails when a public callable has no `Returns:` / `Yields:`
  section; reference pages show a returns table captured from real data, or say why there is none.
- **Docs:** blank returns-table descriptions are filled only from the league's own sport's R
  packages, never with R argument text; NFL Pro, On3 and 13 Fox Sports tables gain authored
  descriptions.
- **Docs:** league index pages can list companion packages in a "See also" block
  (`tools/codegen/companions.yaml`); the SOCCER page links kloppy, sdvplot, sdvplotR,
  itscalledsoccer, soccerdata and mplsoccer.
- **Docs:** the docs site is generated from one league registry: table rows leave the search index,
  plain function headings, large reference pages split by family, a `main (latest)` label and a
  codegen-rendered changelog. (#664, #671, #672, #673, #674, #676)
- **Models:** native thread pools default to one: `SDV_XGB_THREADS` (default 1) stops xgboost's
  predict from fanning across every core per request; raise it deliberately. (#563)
- **NFL / CFB:** the situation-neutral split in `football.tendencies` reads `wp_before_naive`
  (score, clock and field position) instead of `wp_before`, so every `*_neutral` column and
  `sec_per_play_neutral` move.
- **NFL / CFB:** `tackle_share` counts only the defense's own scrimmage snaps, with new
  `scrimmage_tackle_points` and `team_scrimmage_tackle_points` columns; `tackles`, `assists` and
  `tackle_points` still count every credit.
- **Packaging:** polars 2.x is allowed (`polars>=1.0,<3`, mirrored in `recipe/meta.yaml`); the lock
  resolves polars 2.0.0 on Python 3.10+ and 1.36.1 on Python 3.9.
- **Validation:** `tools/validation/source_parity`, a nightly harness, compiles the same games from
  two providers and reports column-level disagreement. (#547)

### Fixed

- **CFB:** `load_cfb_passing`, `load_cfb_receiving` and `load_cfb_rushing` declare the columns the
  assets gained (`dispersion_games`, `EPAplay_sd`, `boom_rate`, `stuff_rate`, ...). Under polars
  2.0, `pl.read_parquet(url)` gets HTTP 501 from release URLs; loaders (`use_pyarrow=True`) work.
- **CFB:** the NCAA mapper fixes quarter markers, the score walk, overtime interception flags,
  overturned yardage and same-row penalty enforcement; no-play rows carry no yardage, and
  block-printed tries go to the kicking team. (#557, #550, #560)
- **CFB:** about 650 plays ESPN scores but no text rule named (frozen-board pick-sixes, fumble
  returns, field goals) now score; textless copies and untyped admin rows ("Begin Drive", quarter
  markers, lone try fragments) are dropped.
- **CFB:** a play that ends a half or a finished game leaves a possession worth nothing (EP_end 0,
  EPA -EP_start), now also for a finished game's last play and regulation's last play before
  overtime.
- **CFB:** a returned kickoff whose end down is outside 1-4 (ESPN's -1 sentinel) ends at down 1, so
  its EP_end is the next snap's EP_start.
- **CFB:** `end.TimeSecsRem` / `end.adj_TimeSecsRem` are the next play's start clock (they were the
  previous play's), and the last play of a half or game ends at 0:00; EPA moves most at period ends.
- **CFB:** 2007-13 touchdowns ESPN filed as their own extra-point kick carry the snap's down and
  distance (was -1), so the EP model no longer scores them as no down.
- **CFB:** blocked field goals keep ESPN's type instead of becoming "Penalty", "Extra Point
  Missed" or a plain "Blocked Field Goal" (sportsdataverse/cfbfastR#175); plays with null join keys
  are never dropped as copies. (#641)
- **CFB:** win probability in overtime and the final seconds uses the new `cfb_wp_overtime`
  correction, and made field goals, decision surfaces and two-point shootouts are valued correctly;
  every `wp_*` / `wpa` moves, and `fg_wp` / `make_fg_wp` / `miss_fg_wp` / `xp_wp` are Float64.
- **CFB:** completions written without "complete to ... for N" (ESPN 2024-25) keep their receiving
  yards from ESPN's `statYardage`, so passing / receiving yards and box scores move.
- **CFB:** a tackle is credited to the tackler's own team from the game roster, so coverage and
  post-turnover tackles leave the opponent's tackle table; `def_pos_team` in `tackles` /
  `position_group_tackles` is now the tackler's team.
- **CFB:** plays ESPN files twice under new ids (stub echoes, drive batches at the start clock,
  copies around a timeout) are dropped before the adjacent-copy dedupe; play counts, EPA/play and
  success rate move in affected games.
- **CFB:** losses written "for N yards loss" or "for 1 yard loss" (ESPN 2025) no longer read as
  gains in `yds_rushed` and `yds_receiving`.
- **CFB:** fumbles in ESPN's 2025 text format and the new `Fumble` type keep their `rush` / `pass`
  flags, so pass / rush aggregates (havoc, EPA/play, success rate) no longer drop them.
- **CFB:** the usage box no longer glues a two-player assist into one phantom tackler, and
  `create_usage_box(rosters=)` fills `position_group_usage` / `position_group_tackles` for
  historical games.
- **CFB:** ESPN's 2025 jersey-style kick text fills kick distances, return yards, touchbacks, fair
  catches and kicker / returner names, and `st_kickers` lists a kicker once per team.
- **Cache:** a scoreboard request whose `dates=` include today or a future day now bypasses the response
  cache (no read, no write); a wholly historical date keeps the 30-day TTL, and an explicit `cache_ttl=`
  still wins.
- **Docs:** descriptions mined from hoopR / wehoop no longer say "`team_detail = TRUE` only" (or
  `athlete_detail` / `position_detail`) for columns the Python parsers always return.
- **Docs:** each reference page's **Valid URL** and docstring `Example URL:` is the URL its example
  call requests (ESPN Core v2 child resources, default params); `espn_<lg>_summary` documents
  that it returns a dict.
- **Docs:** `nba_stats`, `wnba_stats` and `on3` return tables are generated from parser output on
  real captures (e.g. `fg3m` -> `fg3_m`), and multi-result-set endpoints document their dict;
  wrapper behaviour is unchanged.
- **MLB:** `mlb_expected_stats` counts only plate-appearance-ending rows for `pa`, `ab` and the wOBA
  denominator (raw pitch rows had pushed `pa` to ~3,400, deflating `xba` / `xslg`), and
  `intent_walk` is no longer an at-bat.
- **NBA / WNBA / MBB / WBB:** `espn_nba_pbp`, `espn_wnba_pbp`, `espn_mbb_pbp`, `espn_wbb_pbp` read
  the spread from a one-provider pickcenter, pair it with the same provider's favorite, flag every
  team timeout and fix MBB 2OT+ end seconds; `helper_<lg>_pickcenter` returns plain floats / bools.
- **NFL:** a strip-sack (type 80) credits the recovery to the defence, not the offence. (#546)
- **NFL / CFB:** processor bug sweeps: timeouts, roof, spread sign, re-run idempotence, dedupe, yard
  line and end clock (NFL), and C3-C40 plus the NCAA rounds (CFB). (#503, #504, #506, #514, #517,
  #519, #526, #530, #532, #533)
- **NFL / CFB:** `sec_per_play` in `football.tendencies` counts regulation drives only, each for its
  own offense, so overtime 0:00 drives and drive ids shared by two offenses no longer distort pace.
- **NFL / CFB:** `aggregate_usage_box` keeps one season row per player, keyed on
  `(team, player_id)`, instead of splitting a player whose name or position group changed.
- **NFL / CFB:** a pick-six or fumble-return touchdown no longer counts as the offense's conversion
  or touchdown in `football.tendencies`, `football.usage_box` and `fit_third_down_curve`.
- **NHL, ESPN, NFL, KenPom:** the twelve `nhl_edge_*_top_10` boards return data (always empty
  before); `espn_<league>_transactions` uses `parse_transactions`; `load_nfl_ff_rankings` no longer
  reads `rank_delta` as String early in a season; KenPom examples use ids KenPom recognises.
- **PFF:** the legacy `pff_*` passing / receiving and the `pff_api` position / team return tables
  now describe `avg_time_to_throw` (per dropback), `aimed_passes` and receiving
  `positive_epa_percent` correctly, as already done for the player summaries. (#689)
- **PFF:** 19 `pff_api_*` per-player and coverage-matrix routes gain their return-table columns, and
  `pff_api_player_rushing_direction()` / `pff_api_player_snaps_summary()` return rows instead of a
  zero-row frame.
- **Polars:** code polars 2.0 rejects now runs on 1.x and 2.0 with unchanged 1.x output: the
  `*_pbp` clock split, `start.down` / `start.distance` / `end.down` / `end.distance` cast to
  `Int64`, String-to-Date casts and `explode()` on empty lists.
- **Polars:** on polars 2.0, a column R's arrow wrote with the `arrow.r.vctrs` extension type (such as
  `game_json_url` in the NHL and PWHL schedules) reads as its storage type, not an `Extension` column no
  `.str` op, comparison or join accepts; importing sportsdataverse registers it. (#723)
- **Soccer:** `soccer_open_events()` and `soccer_open_dataset()` no longer raise `AttributeError` in
  a fresh interpreter.
- **stats.ncaa.org:** the fetch layer passes the new `/stats_terms` Terms gate without returning or
  caching it as content (#568), fits its three-view cap and backs off on refusals (#569), and
  rotates the proxy when a network error interrupts acceptance (#570).

### Security

- **HTTP:** a credential in a query string (The Odds API `apiKey`, HockeyTech `key`, Fox `apikey`,
  ...) reads `REDACTED` in every `dl_utils.download` log line, error message and chained exception;
  The Odds API wrappers raise `AssetFetchError` on a non-2xx answer instead of returning it as odds.
