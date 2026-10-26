---
title: Changelog
---

# Changelog

The newest releases. Changes merged since 0.2.0b1 are under [Unreleased](/changelog-unreleased); older releases are in the [archive](/changelog-archive).

## 0.2.0b1 Release: October 26, 2026

**Highlights**

0.2.0b1 is the beta of 0.2.0, the first release since 0.1.4 (0.1.5 was never published; its changes are folded
in here). Install it with `pip install --pre sportsdataverse`; the [0.2.0 migration guide](https://py.sportsdataverse.org/docs/guides/migration-0-2-0)
covers the sixteen breaking changes.

- **Failed fetches raise.** `AssetFetchError` (the fetch failed), `NoDataError` (nothing there) and `ValueError` (a bad
  request) replace empty frames across ESPN, stats.nba.com, the generated flat APIs and HockeyTech.
- **polars 2 and Int64 ids.** polars `>=2.0,<3` is required, and parser id columns are Int64 at the boundary.
- **A reorganised package.** Side leagues and EuroLeague live under `sportsdataverse.basketball`, cross-sport providers
  under `sportsdataverse.providers`, and the analytics helpers under `sportsdataverse.analytics`; the old import paths
  warn and keep working until 0.4.0. The G League gains its own `nbagl_stats_*` wrappers.
- **New sources.** The PFF Developer API, Formula 1, EuroLeague, 24 international basketball and 12 European hockey
  families, kloppy soccer event data with SPADL and Expected Threat, Polymarket and Kalshi.
- **A rebuilt docs site.** Pages titled by league name, returns tables in polars types, grouped leagues on the home
  page, new sdvplot charts in the tutorials, and an MCP docs server (`sportsdataverse[mcp]`).

### Breaking changes

- **247Sports site pages:** `primary_color`, `jersey` and `postal_code` are String (were Int64 when every value was
  digits, so event `"000000"` was `0`); a (0, 0) `latitude` / `longitude` pair, 247's no-location sentinel, is null.
  (#928)
- **`build_nfl_season` cache:** `PIPELINE_VERSION` is 2, so per-game frames cached earlier (String `*_player_id`, Int32
  `pos_team`) are rebuilt instead of concatenated with the new Int64 ones, where `diagonal_relaxed` would turn those
  columns back into String. (#981)
- **`fox_nfl_pbp` / `fox_cfb_pbp`:** `play_team` is now the club code of the drive group the play is
  filed under (e.g. `"NYG"`); it held the name of the player Fox credits the play to, which moves to
  the new `play_player` column. (#922)
- **`ncaa_{mbb,wbb}_play_by_play` / `ncaa_{mbb,wbb}_box_scores`** raise `AssetFetchError` when a fetch fails (a ban, an
  exhausted pool, an uncleared challenge) instead of dropping that game, which made a ban read as a smaller season;
  `errors="skip"` keeps going and adds a `scrape_status` column with one row per dropped game saying why. (#924)
- **`nfl.update_config(cache_mode=...)`** raises `ValueError` for a mode other than `"memory"`,
  `"filesystem"` or `"off"` (an unknown mode used to switch caching off silently). (#899)
- **`nfl_players_crosswalk()`** raises `AssetFetchError` when the nflverse players master cannot be
  fetched; it used to return a zero-row frame ("never a raise"), which nulled every id in a join. An
  absent asset still gives zero rows, and a failed DynastyProcess load now warns (yahoo/cbs ids null). (#899)
- **`nhl_player_props` / `pwhl_player_props`:** `game_id` and `player_id` are `Int64` (were Utf8), so the props join
  the NHL parser / EDGE and HockeyTech frames on `player_id`; compare them to ints (`player_id == 8478402`). (#986)
- **Bart Torvik:** `torvik_player_stats()` / `parse_torvik_player_stats()` return `number` as String (was Int64);
  the 8 `"00"` jerseys of 2025 no longer merge into the 291 `0` rows. (#928)
- **Basketball-Reference:** `bref_team_roster()` returns `number` as String (was Float64), so Robert Parish's
  `"00"` is no longer `0.0`; any other zero-padded column also stays String. (#928)
- **ESPN game rosters:** `espn_<lg>_game_rosters(raw=True)` (mbb, wbb, nba, wnba, nfl, nhl, cfb) returns the Core v2
  competitors payload (a dict); `raw=` was accepted and ignored, so it returned the parsed frame. (#878)
- **ESPN hand-written families:** `espn_<lg>_schedule` (`id`, `game_id`, `*_id`, `home_/away_id`, `*_venue_id`,
  `*_conference_id`, `groups_id`; `game_id` was Int32), `espn_<lg>_teams` (`team_id`) and `espn_<lg>_game_rosters`
  (every `*_id`; `team_id` / `game_id` / `position_id` were Int32) are `Int64`. (#981)
- **ESPN parsers:** every `id` / `*_id` column from `_common_espn_parsers` (the `espn_<lg>_<short>` wrappers) is
  `Int64`, where it was String, or Float64 when the pandas hop met a missing value, so `parse_fpi` joins
  `parse_weekly_powerindex`; `now_id` (`"1-44686270"`) is not an integer and stays String. (#981)
- **ESPN parsers:** not yet in this release are the soccer leagues' `scoreboard`, `standings`, `summary`, `teams_site`
  and `team_roster` and the cricket leagues' `scoreboard`, `standings` and `summary` wrappers, whose parsers do not
  cast: their ids stay String; those leagues' other wrappers (`news`, `injuries`, ...) are `Int64`. (#981)
- **ESPN schedules:** the six copies that sent `seasonType` now send `seasontype`. ESPN ignored the camelCase key, so
  `season_type=` with `dates=YYYY&week=N` (NFL, NHL, WNBA, NBA, MBB, WBB) never filtered; it does now, and those calls
  return different rows. (#855)
- **ESPN snapshots / content / crosswalks:** `espn_injuries_snapshot` / `espn_depthcharts_snapshot` ids, the
  `parse_espn_content` `id` and `ncaa_espn_team_crosswalk` `espn_team_id` / `espn_conference_id` are `Int64` (were
  Utf8); `load_{nba,wnba,mbb,wbb}_{team,player,schedule}_crosswalk` cast their `espn_*` ids to `Int64`. (#981)
- **ESPN snapshots:** the injuries / depth-chart snapshots read an id that is not an integer (text, `"007"`) as null
  with a `SchemaDriftWarning` naming the field. (#981)
- **ESPN, pandas output:** with `return_as_pandas=True` the same ids are pandas' nullable `Int64` (was `object`, or
  `float64` such as `150.0` where a value was missing); the Arrow-backed `parse_fpi`, `parse_weekly_powerindex` and the
  generated `load_*_crosswalk` loaders carry them as `int64[pyarrow]`. (#981)
- **ESPN:** `espn_<league>_leaders()` drops its one-row envelope columns (`pagination_*`, `league_*`,
  `current_season_*`, the `athletes` / `glossary` / `categories` cells) for one row per athlete; leagues without a
  leaderboard give zero rows. Page counts and ESPN's per-stat ranks stay in `return_parsed=False`. (#1018)
- **HockeyTech:** game-summary `home_team_id` / `away_team_id` / `game_season_id` are null, not `""`, when unavailable;
  pandas output carries nullable `Int64` (was `float64` such as `1.0`, or `str`); a non-integer id stays String and
  warns `SchemaDriftWarning`. (#988)
- **HockeyTech:** the PWHL and 19-league parsers, `<lg>_pbp`, schedules, rosters, leaders and summaries emit every `id`
  / `*_id` as `Int64` (`player_id` was Float64 in pbp, `team_id` / `game_id` String or the caller's dtype). (#988)
- **KenPom:** the jersey column is String on every page and renamed to `jersey`: `team` / `team_players_expanded`
  `player_table*` `column_0` -> `jersey` (was Float64) and `box` `table_*` `column_0_rk` -> `jersey` (was Int64). A
  season of pages stacks without a String / Float64 clash, and no jersey reads `"2.0"`. (#928)
- **NBA / WNBA / MBB / WBB pbp:** `espn_<lg>_pbp` keeps `coordinate.x` / `coordinate.y` but changes their meaning:
  they were ESPN's raw feet (hoop at (25, 0)) and are now the hoopR / wehoop full-court frame (`x = y - 41.75`,
  `y = x - 25`, both negated on home-team plays), always Float64. (#844)
- **NBA / WNBA / MBB / WBB pbp:** ESPN's missing-location sentinel reads about +-2.1e8 after the transform; filter on
  `coordinate_x_raw > nba_game_page.COORD_SENTINEL`. The release converters (`helper_<lg>_play_by_play`) rebuild from
  the raw pair, so a processed payload converts once. (#844)
- **NBA / WNBA / MBB / WBB pbp:** ESPN's values move to the new `coordinate.x.raw` / `coordinate.y.raw`
  (`coordinate_x_raw` / `coordinate_y_raw` snake-cased); on the NBA / WNBA free throws hoopR / wehoop's "Free Throw"
  match finds, the raw pair holds the pinned (25, 13.75), as in the published files. (#844)
- **NFL processor:** `NFLPlayProcess`'s `*_player_id` columns (String) and `pos_team`, `lag_pos_team`, `lead_pos_team`,
  `lead_pos_team2`, `start.pos_team.id` (Int32) are `Int64`, as in the CFB processor; a source whose ids are not
  integers (NFL.com gsis `"00-0034796"`) keeps them String. (#981)
- **NFL, CFB:** processed plays keep their documented Float64 for the EP/WP family, `first_down_prob`, `prob_2pt` and
  `drive_start` (they were Float32 when a model scored), with no value change, and NFL `teamParticipants` has one struct
  field order; `create_box_score(plays_frame)` still reproduces `advBoxScore` exactly. (#797)
- **NFL:** `sportsdataverse.nfl.model_vars.qbr_vars` is in the QBR trainer's order (`qbr_epa`, `pass_epa`, `rush_epa`,
  `sack_epa`, `pen_epa`, `spread`), and `nfl/models/qbr_model.ubj` is re-saved with those feature names (same trees), so
  a frame in the old column order raises instead of scoring permuted inputs. (#795)
- **NFL:** the season flips on the Wednesday after Labor Day and week 1 starts the Thursday after (nflreadr's rules;
  week read one high in 2015-16, 2020-22, 2026-27). `most_recent_nfl_season` and `get_current_week` share that rule, so
  from September 1 to that Wednesday they give the previous season (week 22), not the new one. (#791)
- **NHL API parsers:** `parse_nhl_web_*`, `parse_nhl_records`, `parse_nhl_stats_rest`, `parse_nhl_edge_*` and
  `nhl_edge_skating_value` emit every `id` / `*_id` as `Int64` (was Float64 / String by payload, `player_id` a Utf8
  pin); stats-rest `formatted_season_id` and the country `id` are text and stay String. (#986)
- **NHL API parsers:** pandas output carries the ids as nullable `Int64` (was `float64` / `object`); any other id that
  is not an integer keeps its column String and warns `SchemaDriftWarning` naming it, as the ESPN parsers do. (#986)
- **polars 2 required.** The runtime pin is now `polars>=2.0,<3` (conda recipe mirrored); polars 1.x is no
  longer supported, so code may use 2.x-only API such as the `bin_*` replacements for `cut`/`qcut`. The lock
  already resolved polars 2.0 on every supported Python, so nothing tested 1.x. (#776)
- **Producers, RDS:** `pyreadr.write_rds` cannot write pandas' nullable `Int64` with a missing value. For that write
  only, cast id columns to nullable `Int32` when every id fits in 32 bits (`df[id_cols].astype("Int32")`: R integer,
  `NA` kept), else to `float64` (R double); the parquet and the frame stay Int64. (#981)
- **Producers, RDS:** `sportsdataverse._rds.write_rds` writes nullable Int64 as R integer, but only for a flat frame:
  it raises `ValueError` on a nested column, which every ESPN schedule frame has (`List(Struct)` linescores, records,
  highlights). (#981)
- **Producers:** before the first post-upgrade run, cast stored ids once (`pl.col(c).cast(pl.Int64)`) or rebuild every
  season; otherwise `pd.concat` / `to_parquet` fails on a mixed column, and `pl.concat(how="diagonal_relaxed")` silently
  turns the ids back into String. (#981)
- **Producers:** move stored and republished ids to `Int64` now, not back to String, and rewrite the stored history in
  one pass so every season of a release carries the same integer ids. Update (or pin to a pre-release commit) every
  producer that writes these frames first, then upgrade to this release. (#981)
- **Producers:** that covers the snapshot histories, the ESPN schedule season masters, and the PWHL schedule master and
  per-game final JSON, whose seasons mix text (`"1"`) and integer (`1`) ids until the JSON is reprocessed. (#981)
- **PWHL:** `pwhl_player_box(game_id)` returns `{"skaters": DataFrame, "goalies": DataFrame}`, as
  fastRhockey's `pwhl_player_box` does and as the `fastRhockey-pwhl-raw` producer reads it. It used
  to return a single (always empty) DataFrame. (#851)
- **R packages:** releases rebuilt from these parsers (the ESPN schedule seasons of the hoopR / wehoop raw repos, the
  PWHL schedule and pbp releases) carry integer ids that hoopR, wehoop and fastRhockey document as character in places;
  republish every season in the same pass and update the R documentation to match. (#981)
- **Release loaders:** Int32 ids (`team_id` in those rosters, `load_pwhl_rosters`, `load_pwhl_xg_pbp`) join Int64 ones
  as they are. Code that relied on String ids can cast back with `pl.col("team_id").cast(pl.String)` (pandas:
  `.astype("string")`); an `Int64` id stringifies as `"123"`, never `"123.0"`. (#981)
- **Release loaders:** String ids also remain in `load_{mbb,nba,wbb,wnba}_rosters` (`athlete_id`, `position_id`,
  `status_id`) and `load_{mbb,nba,wnba}_game_rosters` (`game_id`); joining one to live parser output raises
  `SchemaError`, so cast first: `release.with_columns(pl.col("game_id").cast(pl.Int64))`. (#981)
- **Release loaders:** they keep their published dtypes, so String ids remain in `load_pwhl_schedules` (`game_id`,
  `home_team_id`, `away_team_id`), `load_pwhl_pbp` (`team_id`, `player_team_id`, `home_team_id`, `away_team_id`,
  `game_season_id`) and the PWHL box scores' `player_id`. (#981)
- **Spring football EP/WP:** `build_spring_football_pbp(..., league="cfl")` and `enrich_spring_football_pbp(...,
  league="cfl")` raise `ValueError`; they scored CFL plays (110-yard field, three downs) with the NFL boosters, whose
  support is `yardline_100` 1-99. `get_sf_constants("cfl")` still returns the CFL rule constants. (#1010)
- **stats.ncaa.org fetch layer:** a real 404 raises `NoDataError` after one request; a banned or exhausted proxy pool,
  an uncleared bm-verify challenge or the Terms gate raises `AssetFetchError` naming which. Both were `RuntimeError`;
  catch `AssetFetchError` instead (a missing proxy configuration is still `RuntimeError`). (#920)
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
  (404: `NoDataError`, 400 / 422: `ValueError`); `nba_live_*` / `nba_l2m*` / `nba_referee_assignments`
  now raise `ValueError` on 400 / 422; `wnba_on_court` / `wnba_possessions` raise on a failed rotation fetch.
- **NBA / WNBA:** `nba_stats_*` / `wnba_stats_*` season arguments default to the latest season with
  rows and each league's own default ids, so endpoints that summed every season (`drafthistory`,
  `leaguegamefinder`, `playergamestreakfinder`, ...) return one season.
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

- **`fox_nfl_pbp` / `fox_cfb_pbp`:** new `play_player` column, the player Fox credits the play to. (#922)
- **Basketball crosswalks:** `_crosswalk_alt.with_alt_game_legs()` / `fill_game_legs()` fill a schedule crosswalk's
  null Fox game and team ids (and a null Yahoo game id) from the parsed `*-alt-data` history; on the NBA crosswalks
  that is 1,260 of 1,379 games for 2002. The rest are preseason and All-Star games, which Fox does not list. (#825)
- **Basketball crosswalks:** `genius_game_crosswalk`, `genius_team_crosswalk`, `ibba_segev_game_crosswalk` and
  `pleague_team_crosswalk` (exported from the genius, ibba and plg homes) map provider ids onto the Genius,
  Segev and Preciser ids by shared keys, never by name. (#862)
- **Basketball crosswalks:** the Fox and Yahoo legs are opt-in and fail-soft: any Fox or Yahoo failure logs a warning
  and leaves the columns null. (#793)
- **Basketball:** each minted function takes an END-year `season`; `comp=` (FIBA), `comp_id=` (Genius) or
  `season_id=` / `competition_id=` (Atrium) override it. (#862)
- **Basketball:** nine more FIBA competition families, verified against the live GDAP API: `fiba_eurobasket_*`,
  `fiba_americup_*`, `fiba_asiacup_*`, `fiba_icup_*` (Intercontinental Cup) and `fiba_bcla_*` (BCL Americas). (#864)
- **Basketball:** per-competition functions minted from `tools/codegen/basketball_competitions.yaml`: `fiba_wc_*`,
  `fiba_oly_*`, `fiba_bcl_*` and more (FIBA), `wbbl_*`, `bal_*`, `lfb_*` and more (Genius), `lnb_*`, `nbl1_*`, `lpb_*`
  and more (Atrium). (#862)
- **Basketball:** the women's FIBA families `fiba_ecw_*` (EuroCup Women), `fiba_wamericup_*`, `fiba_wasiacup_*` and
  `fiba_wafrobasket_*`. (#864)
- **CFB / NFL:** on a slice of `plays_frame` (Game on Paper's quarter and half boxes), a drive is valued at its first
  snap of the whole game, so a carried-over drive keeps its real start; a drive with no scrimmage snap is not averaged.
  The `espn_{cfb,nfl}_adv_drives` releases pick it up on their next rebuild. (#836)
- **CFB / NFL:** the `espn_{cfb,nfl}_adv_team` (and CFB `adv_team_gamelog`) releases pick up `yards_per_dropback` on
  their next rebuild. (#740)
- **CFB / NFL:** the advanced box's `drives` rows carry `avg_start_ep`, the team's average drive start in expected
  points: each drive's first scrimmage snap valued on the league's bundled field-position curve
  (`cfb_field_position_ep` / `nfl_field_position_ep`). It is Game on Paper's Drives-table EP row, moved here. (#836)
- **CFB / NFL:** the advanced box's team rows carry `yards_per_dropback`, sack-inclusive yards per dropback
  (`(pass_yards + sack_yards) / passes`, over the same dropbacks as `yards_per_pass`, which still counts a sack as 0
  yards). It is the basis of SDV's `yardsdropback` percentiles. (#740)
- **CFB / NFL:** the advanced box's turnover rows carry `passes_defended`, the opponent's ESPN box
  passes defended against the team's throws (CFB adds `Int`; ESPN's college PD excludes it), null
  where the box has none. (#737)
- **CFB CBS ids:** the CBS scoreboard resolver reads FCS and postseason pages for the id map, a new
  offline `_match_cbs_card` joins a season's stored CBS cards to ESPN games (either home/away
  order, within three days), and the ESPN -> CBS team map gains five FCS schools (325 teams). (#815)
- **CFB crosswalks:** `_crosswalk_alt.match_season(known_teams=)` takes fixed ESPN -> provider team pairs for teams
  that share every game date (an FCS team with one FBS opponent, any team early in a season); `known_teams("cbs",
  "cfb")` serves the CBS resolver's map, so CBS CFB games match 99% of ESPN's schedule in most seasons. (#835)
- **CFB models:** `fg_model` / `two_pt_model` / `xpass_model` cards record the rows actually fitted (42,617 / 1,628 /
  1,902,481) instead of the training frame's height, and `cfb_cp_model_air_yards` gains the card it shipped without.
  (#847)
- **CFB models:** each booster ships `<model>.importance.json` (gain / cover / split counts read off the booster by
  `tools/cfb_model_importance.py`). (#847)
- **CFB models:** every bundled model card states its holdout status: `qbr_model` names its disjoint holdout (2026
  regular season, weeks 1-4), `wp_ot_reach` already did, and the other nine say `in_sample: true` with the reason (fit
  on every season they score). No model bytes changed. (#847)
- **CFB play-by-play:** `errors.PlayOrderWarning`, warned when the processed plays step back a period (the row
  order or ESPN's period label is wrong there; 401628439 files an overtime touchdown as period 2). (#837)
- **CFB:** an `overUnderAvailable` play column and payload key say whether `overUnder` is a market total or the 55.5
  default (a line with a spread and no total). `odds_override` accepts it (a missing key is unknown: a null flag), so a
  rebuild keeps it and warns `DefaultOddsWarning` again instead of relabelling the default as injected. (#895)
- **Cricket:** `cricket_win_probability` selects the ODI fielding-restriction regime from the match date
  (`cricket_model_constants.ODI_REGIMES` / `cricket_regime`: pre-2005, 2005 powerplays, 2011 window, 2012 two new balls,
  2015+); `cricket_match_state` carries `match_date` and the output gains `regime`. (#743)
- **Cricket:** a regime applies whole (its own resource grid and `REGIME_CONSTANTS` par/sigma), or falls back to the
  pooled 2002-2026 fit when its grid has fewer than 100 cells of 30+ states or it has no fitted constants (the 2011
  window). (#743)
- **Crosswalk history harvest:** private `sportsdataverse._crosswalk_history` captures Fox, Yahoo and CBS raw game
  listings and per-game box payloads into the `*-alt-raw` repos: resumable, atomic, season-checked, and paced by
  `XWALK_RPS` / `XWALK_<PROVIDER>_RPS` only; a 403 or 429 stops the run. (#805)
- **Crosswalks:** the private `sportsdataverse._crosswalk_alt` parses stored Fox / Yahoo / CBS payloads into per-season
  `games` / `game_rosters` parquet and matches them to ESPN: teams by (date, side), games by (US-Eastern date, team
  pair), players within each game and team, voted across the season. (#817)
- **Czech Extraliga:** 9 `elh_*` wrappers in `sportsdataverse.hockey.elh` (play-by-play, shots,
  events, scoreboard), plus `elh_game_ids`. (#826)
- **Dependencies:** `pyyaml` joins the `all` extra. (#752)
- **Docs:** the CFB, NFL, NBA, WNBA, NHL and soccer tutorials each draw a chart with sdvplot: team-coloured win
  probability (CFB, NFL), a season shot chart on the court (NBA, WNBA), a rink shot map (NHL) and a pitch shot map
  (soccer). Without the plotting extras the cell prints a one-line install hint instead of a traceback. (#1021)
- **Errors:** `sportsdataverse.errors.SchemaDriftWarning`. The NHL api-web / EDGE / Stats REST / Records, MLB Stats
  API, stats.nba.com and ESPN parsers still return zero rows for a payload shape they do not recognise, but warn naming
  the parser and payload path, so an upstream schema change no longer reads as "no data". (#907)
- **Errors:** `warnings.filterwarnings("error", category=SchemaDriftWarning)` turns schema drift into a failure. (#907)
- **ESPN Core v2 lists:** every wrapper `parse_items` parses (`athletes_index`, `venues`, `season_athletes`, ...)
  returns two Int64 page-envelope columns, `count` (items across every page) and `page_count`, polars and pandas alike,
  so one page of several shows in the frame; null where the payload has no envelope (`athlete_statisticslog`). (#906)
- **ESPN international basketball wrappers:** ESPN's `nbl` has box scores but no play-by-play for 2025-26 (the NBL
  changed stats provider); the NBL Rosetta family fills that gap. (#849)
- **ESPN international basketball wrappers:** ten more ESPN basketball leagues get the universal `espn_<prefix>_*`
  family: `nbl` (Australian NBL), `fiba` (FIBA events), `olympics_mbk` / `olympics_wbk`, and the NBA Summer League
  sites `nbasl_lv`, `nbasl_ut`, `nbasl_ca`, `nbasl_sac`, `nbasl_orl`, `nbasl_gs`. Every slug was checked live. (#849)
- **ESPN soccer:** `parse_soccer_scoreboard` (every `espn_<league>_scoreboard`) carries the event's `season_type` and
  `season_slug`, and it and the summary `header` frame carry a derived `tournament` (`"apertura"` / `"clausura"`, null
  for single-tournament leagues), so Liga MX's two competitions per ESPN season id can be split. (#742)
- **eSports hokejovyzapis:** 41 wrappers over the DEL (12), ICE Hockey League (12), EIHL (9) and
  Metal Ligaen (8) feeds, in `sportsdataverse.hockey.{del_,icehl,eihl,metal}`. (#826)
- **G League:** `sportsdataverse.nbagl.nbagl_stats` wraps the 94 stats.nba.com endpoints live for the G League
  (`LeagueID=20`, sent by default), with returns tables built from real G League captures. Its default ids come
  from the capture sweep and have not been called live; `shotchartlineupdetail` requires `group_id`. (#1020)
- **Game pages:** `sportsdataverse.nba.nba_game_page` scores WNBA, men's and women's college basketball game pages as
  well as the NBA's, chosen by a `league` argument ("nba", "wnba", "mbb", "wbb"; `league_id="00"/"10"` still work),
  with the college in-game models and priors and the men's college period clock. (#848)
- **Game pages:** new warehouse-path and fetch-pair helpers for the sdv-db stage and the Game on Paper processor.
  (#848)
- **Game pages:** regulation periods are read per game (the WNBA before 2006 and women's college before 2015-16 played
  halves, men's college always does); a neutral site gets no home-court advantage (prior source "neutral"); college
  free throws stay off the shot chart. (#848)
- **Game pages:** women's college turnovers follow their eras: 2009-12 from team turnovers, null before 2009 and for
  any game whose two teams together show fewer than 5 turnovers. (#848)
- **Hockey:** `khl_club_crosswalk` (KHL club id <-> per-tournament team-season id, read from a covering set of game
  headers), `khl_club_crosswalk_from_frames`, and `ha_player_crosswalk` (HockeyAllsvenskan player id == SHL isa id, plus
  the SHL athlete uuid). (#865)
- **Hockey:** `sihf_standings` counts every game by default (`venue="All"`; unasked, SIHF answers the away-games table),
  and a table body with fewer rows than it announces raises `AssetFetchError`. (#779)
- **Hockey:** `sportsdataverse.hockey.sihf` wraps the keyless Swiss Ice Hockey Federation statistics API
  (`data.sihf.ch`) for every SIHF league (National League, Swiss League, MyHockey League, amateur, junior and women's
  leagues), with eleven `sihf_*` wrappers. (#779)
- **Hockey:** per-game `sihf_pbp` (goals, penalties, goaltender changes, revoked goals, shootout attempts),
  `sihf_game_info`, `sihf_game_player_stats`, `sihf_game_goalie_stats`, `sihf_game_team_stats` and `sihf_game_lineups`.
  (#779)
- **Hockey:** season tables `sihf_results`, `sihf_standings`, `sihf_player_stats` and `sihf_goalie_stats` take a
  `phase_id`; `sihf_phases` lists a league and season's phases (ids differ by league, season and table) with names and
  game counts. (#779)
- **Hockey:** SIHF answers an unknown season, league or phase with another one (`Season=1890` gives the current season);
  the runtime compares the body's filter echo with the request and raises `NoDataError` when they differ. (#779)
- **HockeyAllsvenskan:** 11 `ha_*` wrappers in `sportsdataverse.hockey.ha` (schedule, play-by-play,
  standings, skater/goalie/team stats, leaders, players). (#826)
- **International basketball:** 530 wrappers over 24 providers (28 flat-API stems), each family in its own
  `sportsdataverse.basketball.<home>` package with parsers and a throttled runtime. Payloads are fetched from public
  sources, not redistributed; each family's docstring notes carry its terms-of-use flag. (#859)
- **International basketball:** providers are FIBA (`fiba_gdap`, `fiba_gamepage`), Genius Sports, Sportradar Atrium,
  NBL Australia, FEB, BBL/DBBL (`scb`), Lega Basket, ACB, IBBA, KZS, LKL, ABA, BNXT (`sportpress`), Basketligaen
  (`sportality`), VTB, W League, KBL, B.LEAGUE, CBA, TBF, CEBL, TPBL, P. LEAGUE+ and EASL. (#859)
- **KHL:** 23 wrappers in `sportsdataverse.hockey.khl`: 14 `khl_*` over the khl.ru REST and 9
  `khl_webcaster_*` over the webcaster API. (#826)
- **Ligue Magnus:** 11 `magnus_*` wrappers in `sportsdataverse.hockey.magnus` (competitions, schedule,
  standings, game sheet, play-by-play, lineups, shootout). (#826)
- **Liiga:** 23 `liiga_*` wrappers in `sportsdataverse.hockey.liiga` (schedule, box scores, shot map,
  standings, team and player stats). (#826)
- **MBB:** `sportsdataverse.mbb` now exports (and the docs list) the NCAA league-RAPM helpers of `mbb_ncaa_rapm_input` /
  `mbb_ncaa_rapm_league` (`resolve_possessions`, `aggregate_stints`, `solve_rapm_league`, ...) plus
  `current_ncaa_team_alts` and `sides_from_box`; they were public module functions no page documented. (#987)
- **MLB:** `load_mlb_schedule(seasons)` reads the `mlb_schedules` release (statsapi schedule, every game type,
  1988+). (#794)
- **MLB:** `load_mlb_statcast(seasons)` reads the `mlb_statcast` release (Baseball Savant pitch-level data, one
  parquet per season, 2015+; spring training and exhibition pitches included). The `on_1b` / `on_2b` / `on_3b`
  runner ids are published as floats and cast to Int64 at the loader boundary. (#794)
- **Model lineage:** a new *Model lineage* docs page records the fitting script of every bundled booster (26 `.ubj`
  files) and fitted constant, which boosters are byte-identical to their release asset, and which cited fitting
  scripts are lost or untracked. (#984)
- **Model lineage:** the NCAA RAPM ridge-lambda and NBA shot-talent shrinkage fitters move into `tools/`
  (`fit_ncaa_rapm_lambda.py`, `fit_nba_shot_shrinkage_k.py`); `TALENT_SHRINKAGE_K["10"]` (WNBA) is documented as a
  seeded placeholder, and the NFL prediction constants say their fitting scripts are lost. (#984)
- **National League:** 17 `nl_*` wrappers in `sportsdataverse.hockey.nl` (games, box scores, shots
  with xG, standings, player and team stats). (#826)
- **NBA / WNBA / MBB / WBB crosswalks:** `{nba,wnba,mbb,wbb}_schedule_crosswalk(fox=True)` fills `fox_game_id` (and the
  NBA / WNBA `fox_home_team_id` / `fox_away_team_id`) from Fox's per-date scoreboard, matched on US-Eastern date and
  teams in either home/away order; a late tip ESPN files at 23:59 ET matches Fox's 00:00 ET a day apart. (#793)
- **NBA / WNBA crosswalks:** `nba_crosswalks_from_archive()` / `wnba_crosswalks_from_archive()` take `alt_root=`, an
  `*-alt-data` checkout whose parsed Fox / Yahoo history fills the player crosswalk's null `fox_athlete_id` /
  `fox_player` / `yahoo_player_id` / `yahoo_player_name` offline. (#817)
- **NBA / WNBA crosswalks:** `nba_crosswalks_from_archive` / `wnba_crosswalks_from_archive(fox_games=True)` fill the
  Fox game and team ids for any season Fox serves. (#793)
- **NBA / WNBA crosswalks:** `nba_crosswalks_from_archive` / `wnba_crosswalks_from_archive` take `fox=True`:
  `fox_team_id` by team name for any season, and the Fox player columns for the current season
  only (Fox rosters are today's). A failed Fox fetch warns and leaves them null. (#786)
- **NBA / WNBA stats:** `SDV_PY_NBA_STATS_PROXY` routes every `nba_stats_*` / `wnba_stats_*` call through a proxy
  (`http://user:pass@host:port`) when the call passes no `proxy_url`; `proxy_url=""` bypasses it. stats.nba.com and
  stats.wnba.com hang on cloud IPs, so the `tutorials-refresh` workflow sets it from a proxy pool. (#1017)
- **NBA / WNBA:** `league_id` picks NBA 48-minute or WNBA 40-minute regulation; only `"00"` and `"10"` are scored, and
  anything else raises `ValueError`. (#749)
- **NBA / WNBA:** `nba_crosswalks_from_archive()` / `wnba_crosswalks_from_archive()` build a past season's team,
  schedule and player crosswalks offline from the ESPN schedule and game-roster parquet plus the local stats box-score
  archive; the live builders only reach the current season. (#773)
- **NBA / WNBA:** `sportsdataverse.nba.nba_game_page` scores a game page: per-play in-game win probability with deltas
  and an overtime regime, the pregame prior from the spread, court-frame shot coordinates, team four factors / ratings
  / pace and player TS% / usage. (#749)
- **NBA / WNBA:** a null play score is carried forward before scoring, and `game_page_frames` returns the rate frames
  with their documented columns (zero rows) before both team box rows exist. (#749)
- **NBA / WNBA:** team turnovers repair ESPN's duplicated pre-2013 team turnover and its null 2018 `total_turnovers`,
  and the win probability settles to 1.0 / 0.0 on the "End Game" / "End Period" row of a decided game from Q4 on. (#749)
- **NBA:** `load_nba_stats_draft(seasons)` reads the `nba_stats_draft` release; `seasons` is the draft year. (#780)
- **NBA:** loaders for the multi-file `nba_stats` releases: `load_nba_stats_synergy`, `_hustle_player` / `_team`,
  `_matchups` / `_matchups_rollup` and five `_draft_combine_*` tables. Each unions a season's files, stamps
  `season_type` / `per_mode` from the file name where rows lack them, and takes START-year `seasons`. (#780)
- **NBA:** the team and schedule crosswalks now compute `yahoo_team_id` (`nba.t.{espn id}`) and `yahoo_game_id`
  (`nba.g.{YYYYMMDD ET}{home id}`) from 2014 on, where they were null. (#773)
- **NCAA:** `scrape.ncaa.espn_player_xwalk` maps NCAA roster player ids to ESPN athlete ids per
  (season, ESPN team) for MBB, WBB and MFB, in four tiers that leave ties unmatched; the `-raw`
  repos run it as stage 07. (#813)
- **NFL Fox ids:** `sportsdataverse.nfl.fox_pbp.game_id._resolve_fox_game_id` resolves Fox event ids, the Fox twin of
  the CBS week-scoreboard resolver (same arguments, light import), so a nightly id map can fill `fox_event_id`; the
  football id map's team table now carries `fox_team_id`. (#804)
- **Packaging:** new `sdvplot` and `sportypy` extras (both also in `[all]`): sdvplot (>= 0.1.1, its first release that
  allows polars 2) for team logos, colors and chart themes, sportypy for drawing play-by-play and shot coordinates on
  rulebook fields, rinks and courts. sportypy is GPL-3.0 and optional. (#1017)
- **SHL:** 27 `shl_*` wrappers in `sportsdataverse.hockey.shl`, plus `shl_ssgt_uuid` and
  `shl_player_crosswalk`. (#826)
- **SIHF:** 22 more `sihf_*` table wrappers (team, skater and goalie goals, shots, special teams,
  penalties, shootouts, faceoffs, time on ice, attendance), 33 in the family. (#826)
- **Validation:** `pbp_invariants` checks basketball clock seconds: `end <= start` on every
  `*_seconds_remaining` pair, and `end` equals the next play's `start` within a period. (#841)
- **Validation:** `pbp_invariants` holds a period's last basketball play to the period's 0:00
  (`seconds.<scope>_period_last_row_ends_at_zero`), so a producer that clamps `end` to
  `min(end, start)` no longer passes. (#844)
- **VHL:** 13 `vhl_*` wrappers in `sportsdataverse.hockey.vhl` (schedule, game report, play-by-play,
  standings, skater/goalie stats, players, rosters), plus `vhl_tn`. (#826)
- **WBB NCAA:** `ncaa_wbb_shot_locations` takes `season=`, resolved through `wbb_period_model` (halves through 2014-15,
  quarters from 2015-16); the default stays quarters, since stats.ncaa.org shot locations only exist from 2019. (#742)
- **WNBA / MBB / WBB crosswalks:** `yahoo=True` on the team and schedule crosswalks fills `yahoo_team_id` (Yahoo's
  listing joined to ESPN by full name, short name, then nickname where school names agree; unmatched teams are logged).
  (#793)
- **WNBA / MBB / WBB crosswalks:** `yahoo_game_id` = `{lg}.g.{YYYYMMDD ET}{home Yahoo id}`, checked against the ids
  Yahoo lists so a neutral-site game takes Yahoo's home side. (#793)
- **WNBA crosswalks:** `wnba_crosswalks_from_archive(..., yahoo=True)` fills `yahoo_team_*` and
  `yahoo_game_id` for a past season through the live builders' Yahoo code; a failed Yahoo fetch
  leaves the columns null with a warning. (#803)
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

- **`download()`:** a 403 from api.nfl.com, premium.pff.com, comp.uefa.com, kenpom.com or herhoopstats.com is not
  retried; a malformed URL (`MissingSchema`, `InvalidURL`, `InvalidHeader`) raises at once instead of retrying ~50 s.
  (#880)
- **`download()`:** retry waits share a 180 s budget per call and a wait over 10 s logs a WARNING; a 2xx error envelope
  or JSON scalar is never written to the response cache. (#880)
- **Basketball / hockey runtimes:** in the 34 international basketball and European hockey getters a route's own data
  request's 204 / 205 returns `{}` (was `NoDataError` / `AssetFetchError`), and a 204 on a lookup (token, session, key
  page, season list, game or scope check) or mid-way through a multi-request answer is `AssetFetchError`. (#929)
- **Basketball runtimes:** a 400 is `ValueError` in EASL, KBL, KZS and VTB Infobasket, and IBBA's blank 200 is
  `AssetFetchError`. (#929)
- **CFB crosswalks:** `load_cfb_teams_crosswalk` / `load_cfb_schedule_crosswalk` accept 2013. The `cfb_crosswalk`
  release now carries 2013 (Yahoo and Fox legs), which the old 2014 floor hid. (#790)
- **CFB play-by-play:** the kickoff touchback and out-of-bounds spots (2012 kickoff rule) come from one
  catalogued table, `sportsdataverse.cfb.model_vars.TOUCHBACK_YARDLINE_BY_SEASON` (+ `cfb_touchback_yardline_expr`).
  A refactor: no output value changes. (#829)
- **CFB validation:** the box-score parity harness grades penalties (accepted fouls, committing team),
  fumbles lost (offense and defense) and net passing (total minus rushing) the way ESPN counts them, and
  adds a player-level check and a per-game integrity screen. (#764)
- **CFB:** the team summaries, percentiles, passing, receiving and advanced-box column descriptions
  match the republished definitions: run-stuff and opportunity rates over rushes, per-drive drive
  metrics, passer success over dropbacks, per-target receiving rates null for 2021-2024. (#741)
- **Codegen:** an endpoint YAML can name a `body_builder:` (a runtime callable) that turns the flat body into the nested
  JSON an API wants; only top-level `None` fields are dropped, so a builder handles `None` inside what it nests. (#864)
- **Codegen:** endpoint YAML can declare path/query transforms (`transform: {offset: -1}`, `{fn: ..., with: [...]}`), a
  per-route `getter:`, typed `choices:` (int / date, checked typed by the wrapper) and a family `no_r_fallback:` flag.
  (#865)
- **Codegen:** flat-API endpoints take `body: query|form|json` and per-param `in: query|body`; CBA, KHL REST and
  HockeyAllsvenskan use it, with a curl example in the docs. A caller `params=` entry that names a body field overrides
  the body; any other `params=` entry goes to the query string. (#862)
- **Dependencies:** `urllib3 >= 2.8.0` is a direct runtime floor (three open advisories below it) and the `tests`/`all`
  extras need `curl_cffi >= 0.15.0` (redirect SSRF). `recipe/meta.yaml` is re-mirrored from `pyproject.toml`
  (`scikit-learn`, `urllib3`, `tomli` were missing); `tools/check_recipe_mirror.py` diffs the two. (#751)
- **Docs:** a parser's "empty payload gives a zero-row frame" note moves to its Returns, and the parameter table of a
  POST body endpoint (`body: form|json`: CBA, KHL, HockeyAllsvenskan) gains an **In** column (`path` / `query` /
  `body`). (#864)
- **Docs:** admonitions render again, every hand-written page has a search description, and version
  stamps, stale counts, dead parser links and unused template images are gone. (#1021)
- **Docs:** league pages are titled with the registry label ("NBA Summer League (Las Vegas)", "Men's college
  basketball") instead of the raw prefix, and the Getting Started sources table runs sport by sport with a Sport
  column and each ESPN-only league's API breakdown. (#1021)
- **Docs:** pages that mostly list per-competition functions minted from the competition registry no longer call them
  "hand-written wrappers", and a split family page never ends on a lone function. (#864)
- **Docs:** reference and "Additional functions" pages render a **Raises** table from the function's docstring, one
  row per exception and never a `None` row; generated wrappers (including `nba_stats` / `wnba_stats`) list their
  runtime's error vocabulary plus any endpoint YAML `raises:` qualifier. (#864)
- **Docs:** returns tables show polars dtypes (`String`, `Int64`, `Float64`, `Boolean`) instead of R type
  names; reference rows whose summary was only `GET /path` read as the endpoint's name ("Box score advanced
  v3."), in the wrapper docstrings too. (#1021)
- **Docs:** the home page leads with the `pip install --pre` line, a code example, leagues grouped by
  sport and registry group, and a data-sources strip; a small bar announces the 0.2.0 beta. (#1021)
- **Docs:** the MBB, WBB, G League, AHL, ECHL, OHL, QMJHL, USHL, WHL, college hockey, college baseball and softball,
  UFL, XFL, FIBA and cricket pages list sdvplot (and sportypy where it draws the surface) as companions, and every
  sdvplot / sdvplotR link points at its docs site. (#1021)
- **Empty results keep their columns:** the ESPN league wrappers of 45 dedicated parsers and the soccer / cricket
  scoreboards, generated and multi-file NBA stats loaders, 3 HockeyTech parsers and `enrich_pbp` return their table,
  not (0, 0); `parse_items`, `parse_single_entity` and cdn rankings still return (0, 0). (#978)
- **ESPN player stats / Fox:** a stat with no `value` takes a plain-number `displayValue` ("0") and is otherwise null
  (NHL "0:00"); the column is always Float64. Fox `frame()` warns when a column mixes text and numbers. (#979)
- **ESPN schedules:** `espn_cfb_schedule` and `espn_mlb_schedule` return an empty frame with the documented columns on a
  day with no games; they returned `None`. (#855)
- **ESPN schedules:** the seven scoreboard-backed `espn_<lg>_schedule` emit their documented columns in documented order
  on every day (a column the day left empty is a typed null, not absent); columns ESPN adds later follow. A documented
  integer column no longer arrives as Float64 when one event lacks it (`tournament_id`). (#855)
- **ESPN wrappers:** a collection as a path value raises `ValueError`, and a float id is sent as its integer (`"13"`,
  not `"13.0"`). (#896)
- **ESPN wrappers:** a keyword the wrapper does not take raises `TypeError` naming the wrapper, before any request.
  (#896)
- **ESPN:** `parse_team_roster` on a position-grouped roster returns zero rows with a `SchemaDriftWarning` when any
  group or player is not an object; it returned the other groups' rows. (#941)
- **ESPN:** the 12 generated site-v2 wrappers whose name a hand-written function held are now public as
  `espn_<lg>_calendar_site` (cfb, nfl, mlb, nba, wnba, mbb, wbb) and `espn_{wbb,wnba}_{standings,team_roster}_site` /
  `espn_wnba_draft_site`. The package names still run the hand-written functions; other leagues keep theirs. (#903)
- **ESPN:** the scoreboard `broadcast` column keeps every network name, ", "-joined in feed order and deduplicated
  ("ESPNU, ESPN+"); it kept only the first name of each broadcast entry. (#865)
- **Game pages:** `nba_game_page.game_page_frames` reads the game's neutral site, so a lineless neutral-site NBA game
  (the All-Star Game, Rising Stars) starts from a 0.5 prior (prior source "neutral") instead of home-court advantage;
  its win probabilities move by up to 0.08 for existing NBA callers. (#848)
- **Generated wrappers:** a `bool_str` flag (ESPN `active=`, Fox `include_answers=`, MLS `is_live=`) raises `ValueError`
  for any string other than `true`/`false`/`1`/`0`; `"yes"` was sent as `"true"` and `""` as `"false"`. (#896)
- **HTTP chokepoints:** a 2xx `{"code": 400-599, "message"}` envelope is classified by its code (404 `NoDataError`,
  400/422 `ValueError`, else `AssetFetchError`); barttorvik and the Savant CSV leaderboards refuse an HTML page as
  `AssetFetchError`; Yahoo and On3 data routes refuse a JSON array where an object belongs. (#880)
- **HTTP chokepoints:** a 2xx JSON `null`, number, string or boolean body raises `AssetFetchError` instead of reaching a
  parser as data (or `{}`), except OpenLigaDB's `/getlastchangedate`, which answers a bare ISO string by design. (#880)
- **HTTP:** `dl_utils.download` POSTs when given `data=` / `json_body=` and never caches a body request, so
  `_codegen_runtime._get` sends form and JSON bodies and a body endpoint needs no runtime getter of its own. (#864)
- **HTTP:** a POST through `download` is retried like a GET, so it is for idempotent read APIs only. (#864)
- **KHL:** `khl_player_stats` documents goalie `sv_pct` as a 0-100 percentage (92.2 = .922), `gaa` as goals against per
  60 minutes and `toi` as total "minutes:seconds" text. (#868)
- **MBB/WBB:** `ncaa_espn_team_crosswalk(league=)` reads the `{mbb,wbb}_crosswalk` sportsdataverse-data release
  (built weekly by sdv-reference-data), so a new season no longer waits for an sdv-py release; it warns and falls back
  to the bundled CSV when the release cannot be read. (#770)
- **MBB:** `load_mbb_player_crosswalk()` documents `kp_player_id`, the KenPom player id hoopR-mbb-data now
  publishes after `yahoo_player_name` (18 columns), and `kp_game_id` on `load_mbb_schedule_crosswalk()` is
  described as filled. The live `mbb_player_crosswalk()` builder stays at 17 columns. (#787)
- **MLB:** `fit_zone_model` excludes 2026+ pitches with a `UserWarning` (Savant reports them against the ABS zone and
  the calls are challengeable), and `mlb_umpire_called_strike_prob` / `mlb_umpire_bias` / `mlb_catcher_framing` carry
  an `abs_era` flag, with per-umpire and per-catcher rows split by era. (#743)
- **NFL fourth-down getters:** `get_go_wp`, `get_fg_wp`, `get_punt_wp`, `get_2pt_wp`, `get_2pt_probs` and
  `get_4th_down_probs` treat only `FileNotFoundError` as a missing model; a corrupt cached booster (`XGBoostError`) or a
  disagreeing model card (`ValueError`) propagates instead of yielding NaN. (#899)
- **NFL loaders:** `[]` returned every season from `load_nfl_schedule`, `load_nfl_nextgen_stats`, season-level
  `load_nfl_pfr_advstats` and `load_nfl_espn_qbr`, and raised from `load_nfl_ratings_weekly` / `load_nfl_ngs`; a
  mistyped season (`2024.5`, `True` in a list, `"2024x"`) now raises `ValueError` naming `seasons`. (#898)
- **NFL loaders:** `load_nfl_player_stats(True)`, an old positional call for the removed kicking flag, loads every
  season of the non-kicking stats where it raised `TypeError`; pass `kicking=True` for kicking stats.
  `load_nfl_player_stats(False)` still raises `TypeError`. (#898)
- **NFL loaders:** `load_nfl_rosters` and `load_nfl_depth_charts` take the current season on the roster calendar (the
  year turns on March 15), as nflreadpy does; `load_nfl_nextgen_stats` raises `SeasonNotFoundError` before 2016. (#898)
- **NFL loaders:** `seasons=None` raised `TypeError` in the helper-backed loaders, and in `load_nfl_player_stats` (its
  default) loaded every season since 1999. (#898)
- **NFL loaders:** `seasons` follows nflreadpy: `None` loads the current season, `True` every season from the loader's
  first to the current one (skipping a current season not yet published), and `[]` an empty frame without a request.
  (#898)
- **NFL models:** `enrich_nfl_pbp(strict=True)` or `SDV_PY_MODELS_STRICT=1` raises `FileNotFoundError` naming a missing
  model instead of nulling xYAC, xpass or fourth-down columns; the ESPN `NFLPlayProcess` path honours the variable.
  (#899)
- **NFL models:** `enrich_nfl_pbp` and `NFLPlayProcess` still catch such an error and skip the step with a
  `RuntimeWarning` unless strict mode is on. (#899)
- **NFL models:** when the download-on-demand nfl4th models (`fd_model.ubj`, `wp_model.ubj`) cannot be obtained, the
  fourth-down step warns once per artifact; it returned all-null `go_wp` / `fg_wp` / `punt_wp` silently. (#899)
- **NFL Pro:** `NFLProAuthError` is an `AssetFetchError` (still a `RuntimeError`); pro.nfl.com's empty-200 rejection,
  a GraphQL `errors` envelope or a body without `data.viewer.gameDetail` raises instead of giving an empty pbp. (#918)
- **NFL:** `nfl_pro_*`, the `api.nfl.com` token mint, `nfl_game_schedule` / `nfl_game_details` / `nfl_game_pbp` and
  `nfl_ngs_*` raise `NoDataError` (404), `ValueError` (400/422) or `AssetFetchError` (429/5xx after retries, a dead
  connection, an empty/HTML/scalar 2xx) instead of `HTTPError` / `KeyError` / `JSONDecodeError` / `ValueError`. (#918)
- **NFL:** NGS retries 408/429/5xx but not its 403 deny, the schedule / details GETs retry, the getters honour
  `SDV_PY_HTTP_TIMEOUT`, a string pro.nfl.com `limit` pages correctly, and an undecodable token is logged. (#918)
- **NHL Records:** `nhl_records_comeback_wins(scope=)` raises `ValueError` for anything but `"league"` or
  `"franchise"`; `"Franchise"` or `"team"` returned the league-wide table. (#928)
- **On3:** the deprecated `on3_*_rankings` shims raise `ValueError` on a malformed `year` / `sport_slug` (was `{}`).
  (#921)
- **Packaging:** the `sdvplot` extra now installs `sdvplot[mpl]`, so `pip install --pre "sportsdataverse[sdvplot]"`
  brings matplotlib and draws charts without a separate install. Courts, rinks and pitches still need the
  GPL-3.0 `sportypy` extra. (#1021)
- **Possession cache:** `nba_season_compile.PIPELINE_VERSION` is 5, so per-game caches keyed by it
  (`compile_nba_season`, and WNBA builders that import `_game_cache_key`) recompile once. (#818)
- **Provider flags and numbers take their documented types:** CBS / Yahoo flags are Boolean (never "nan" / "True"),
  Odds API `outcome_point` is Float64 where markets exist, EuroLeague header `round` is Int64, college baseball
  pandas integers stay `Int64`, and the On3 rankings tables document their integer columns as integer. (#979)
- **PWHL:** `pwhl_team_ratings` forwards to `pwhl_ratings_from_proxy` (its `league` argument is ignored);
  `nhl_player_props` takes `load_skater_boxscores=` and `team_ratings=`, and the NHL impact engines take
  `xg_scored=`, so another league binds its own data and xG model. (#846)
- **Runtimes:** The Odds API raises `AssetFetchError` on a dead connection (was `ConnectionError`) or a scalar 2xx body;
  LEGACY `pff_*` raises `AssetFetchError` on a JSON array (was `{}`); `pff_*` and the 247Sports getters honour
  `SDV_PY_HTTP_TIMEOUT`. (#921)
- **Season loaders (generated, and `load_nfl_team_group_seasons`):** a fractional season (`[2024.5]`, which
  loaded 2024) raises `ValueError`, as does a bool season; `seasons=None` raises `ValueError` where it raised
  `TypeError`. (#896)
- **Spring football:** `enrich_spring_football_pbp` threads the league-season `touchback_yardline` and
  `ep_point_values` from `get_sf_constants` into `enrich_nfl_pbp` (new keyword-only `league=`, `touchback_yardline=`,
  `ep_point_values=`; NFL defaults unchanged), so XFL / UFL kickoffs are scored from their own touchback spot. (#744)
- **Statcast search:** every chunk takes its returns table's dtypes; `zone`, `release_spin_rate`, `spin_axis` and the
  `*_days_*` columns are Int64 on all three routes, and an empty window has those dtypes instead of `Null`. (#979)
- **Tests:** warnings are errors. A test asserts an expected warning with `pytest.warns(..., match=...)` or
  filters an incidental one by exact message; live tests skip on a timeout or upstream 429/5xx, the
  live job runs on Ubuntu, and the `tests` extra needs pytest >= 8.0. (#726)
- **WNBA / NBA loaders:** `load_wnba_stats_shots` and `load_wnba_stats_player_game_logs` accept 1997 onward, and
  `load_nba_player_crosswalk` and `load_wnba_player_crosswalk` 2002 onward, the seasons their releases publish; the old
  2026 / 2025 floors hid them. (#848)
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

### Deprecated

- **Config:** 29 unread `sportsdataverse.config` release URLs warn `DeprecationWarning` and are removed in 0.4.0; use
  the generated `load_<league>_*` loaders. They are `CFB_{ROSTER,TEAM_SCHEDULE,TEAM_INFO}_URL`, `NFL_PLAYER_STATS_URL`,
  `NFL_PLAYER_KICKING_STATS_URL` and `{NHL,PHF,MBB,NBA,WBB,WNBA}_{BASE,PLAYER_BOX,TEAM_BOX,TEAM_SCHEDULE}_URL`. (#987)
- **Config:** the warning fires on `from sportsdataverse.config import NBA_BASE_URL` and on
  `importlib.import_module("sportsdataverse.config")`. `sdv.config` is `sportsdataverse.nfl.config` (bound by
  `sportsdataverse.nfl`'s star import), so `sdv.config.NBA_BASE_URL` raises `AttributeError` with no warning. (#987)
- **ESPN teams:** `espn_<lg>_teams.cache_clear()` (the eight leagues) is a no-op that warns `DeprecationWarning` and is
  removed in 0.4.0. The functions no longer cache, so there is nothing to clear. (#888)
- **ESPN:** the old names of those 12 wrappers in `*_espn_ext` (`from sportsdataverse.cfb.cfb_espn_ext import
  espn_cfb_calendar`, ...) still return the wrapper but warn `DeprecationWarning` naming the `*_site` name; they are
  removed in 0.4.0. (#903)
- **MBB / WBB:** `sportsdataverse.scrape.ncaa.league_config` (`NcaaLeagueConfig`, `MBB`, `WBB`, `by_league`)
  warns `DeprecationWarning` on access and is removed in 0.4.0. Nothing read it, and `WBB.periods = 4` is wrong
  before 2015-16; pass the league slug and use `scrape.ncaa.parse.wbb_period_model(season)`. (#853)
- **NBA / WNBA:** `load_nba_stats_{pbp,possessions,lineups}_v3` and the four `load_wnba_stats_*` shims
  (`lineups`, `player_season_stats`, `standings`, `team_season_stats`) now say they are removed in 0.4.0;
  they named 0.1.0, which has shipped. A test fails if a removal version falls behind again. (#726)
- **Package layout:** the ESPN side leagues (`nbl`, `fiba`, `olympics_*`, `nbasl_*`) and `euroleague` moved under
  `sportsdataverse.basketball`, the cross-sport sources under `sportsdataverse.providers`, and four helpers under
  `sportsdataverse.analytics`. Old paths, deep ones included, alias the new modules until 0.4.0, warning. (#1020)
- **PWHL:** `pwhl_team_ratings(league=...)` is deprecated and ignored (the ratings are always PWHL); passing it
  warns `DeprecationWarning`; removed in 0.4.0. (#846)
- **Utilities:** `sportsdataverse.dl_utils.ESPNHTTP` / `ESPNResponse`, the polars `df.janitor.to_pascal_case()` /
  `to_camel_case()` / `to_kebab_case()` methods and `sportsdataverse.decorators.record_mem_usage` /
  `record_time_usage` had no caller; they still work, warn `DeprecationWarning`, and are removed in 0.4.0. (#987)

### Removed

- **Dependencies:** `matplotlib`, `pyjanitor`, `inflection` and `attrs` are no longer runtime dependencies (nothing
  imported them; `tests/test_import_sweep.py` imports every module with them blocked). `black`, `flake8`, `isort`,
  `pycln` and `pydocstyle` leave the `tests`/`all` extras; the `models` extra is empty (already base deps). (#752)
- **Internals:** the private `sportsdataverse._common_espn` module (host constants plus a `_get` / `_csv` re-export
  nothing read) leaves the package and the wheel; import `_get` from `sportsdataverse._codegen_runtime`. The committed
  `tools/codegen/_generated/` staging copies and the retired `extract*.py` bootstraps are deleted. (#985)
- **MBB / WBB:** the NCAA <-> ESPN team-crosswalk builder `tools/crosswalk/` is gone; sdv-reference-data
  (`sdv_reference/crosswalks/`) builds and publishes the `{mbb,wbb}_crosswalk` asset that
  `ncaa_espn_team_crosswalk()` reads first (#770). The bundled fallback copies stay. (#788)
- **Python 3.9 dropped.** `requires-python` is now `>=3.10` (3.9 is end-of-life and HEAD no longer
  imported on it: `cfb/cfb_drive_summary.py` evaluates `set[int] | str | None` at def time). The lock
  now resolves polars 2.0 on every supported Python; `scrape/ncaa/canary.py` imports on 3.10. (#748)

### Fixed

- **`*_pbp_disk` / `CFBPlayProcess.cfb_pbp_disk` / `NFLPlayProcess.nfl_pbp_disk`:** saved ESPN payloads are read as
  UTF-8. On Windows they were decoded with the platform default (cp1252), which garbled every non-ASCII player and team
  name in an offline rebuild. (#996)
- **`download()`:** a call made without a `logger` no longer adds another `NullHandler` to the
  `sdv.dl_utils` logger each time; the module adds its one handler at import. (#992)
- **`espn_injuries_snapshot` / `espn_depthcharts_snapshot`** warn and keep the other leagues / teams when one fetch
  fails; one failure discarded the whole day, which cannot be re-taken. Every fetch failing still raises. (#924)
- **`fox_nfl_pbp` / `fox_cfb_pbp`:** rows come back oldest-first on a live game too (Fox serves
  newest-first until FINAL). (#922)
- **`ncaa_mbb_date_games` / `ncaa_wbb_date_games`:** they close the browser they open; a lingering one made the next
  browser fetch in the process fail. (#920)
- **`scrape.espn.persist.write_payload`** writes through a temp file and `os.replace`, so a crash mid-write no longer
  leaves a truncated `.json` that counts as captured and is never re-fetched; ESPN's `{code, message}` error body is
  refused like the other error shapes. (#924)
- **Archive crosswalks:** the team pairing (date and home/away co-occurrence) leaves a team unpaired when two
  candidates tie, instead of taking the lower id. A full season never ties; a slice of a few dates could pair the wrong
  franchises. (#817)
- **Atrium game wrappers:** a game the site does not serve (an unknown fixture id, or another site's game) raises
  `NoDataError` instead of an echo `AssetFetchError`, and the minted Atrium game functions no longer refuse a game
  whose season the site's (incomplete) season list omits. (#864)
- **Basketball courts:** `get_court(league_id, season=)` returns the three-point geometry in force that season (the NBA
  22-ft arc of 1994-97, the WNBA 19'9" / 20'6.25" / FIBA eras). (#738)
- **Basketball game pages:** `game_page_frames` no longer raises when ESPN's `format` is not an object (a string, or a
  `regulation` that is not one); the game's regulation falls back to the league's season era. (#900)
- **Basketball:** the minted per-competition game functions refuse a game of another competition with
  `AssetFetchError`, and need `season` or `comp` (their docs say so). The parameterised `genius_*` /
  `fiba_gamepage_*` / `atrium_*` wrappers are otherwise unchanged. (#864)
- **Basketball:** the per-process listing cache keeps at most 1,024 entries (least recently used evicted), so a
  backfill does not grow it without bound. (#864)
- **Basketligaen (`sportality`):** five returns-table descriptions (`ppg`, `3_p_perc`, `blkpg`, `ec`, `ft_perc`) are no
  longer cut mid-parenthesis, and the `standings_standings` test samples keep their whole `dataColumns` header. (#892)
- **BBL / DBBL (`scb`):** `tv_channel` on `scb_games_recent` and `scb_game` joins the channel names
  (`"DYN,Sport Bild Stream"`) instead of writing Python dict text; null entries and null names are
  skipped, never written as `"None"`. (#892)
- **Caches:** `clear_cache()` (both) deletes only the entries the cache wrote, never other files in the cache
  directory, and a corrupt NFL cache entry that cannot be unlinked no longer escapes as `PermissionError`. (#899)
- **Caches:** a memory-cache hit (NFL loaders and the HTTP response cache) returns a copy, so editing a returned frame
  or payload in place no longer changes what the next call gets. (#899)
- **CBA numbers:** about 100 display-string columns (`'21.6'` averages, `'39.4%'` percentages) across the season lists,
  team tables, advanced box and shot zones are Float64. A percent string is a fraction 0-1, so
  `cba_player_offensive.turnovers_rate` ('13.5%') is 0.135, while `cba_team_four`'s numeric 13.5 stays 0-100. (#864)
- **CBA numbers:** thousands commas are dropped (`'1,234'`), `'12.0'` in an integer column is 12, `'nan'` / `'inf'` are
  null; `season` and the schedule win/loss counts are Int64, jersey numbers and code-table values stay text, and
  columns null in every capture take the family's dtype. (#864)
- **CBA:** a POST route called without `json=` raises `TypeError`; the generated wrappers always send it, so their
  behaviour is unchanged. (#864)
- **CBA:** without `CBA_PROXY` a request from a datacenter fails in seconds (5 s connect timeout, no retry of a refused
  or timed-out connect) instead of after about three minutes; read timeouts, mid-response resets and proxied failures
  are still retried. Set `CBA_DIRECT=1` where direct access works for the full timeout and retries. (#864)
- **CBS football adapter:** a failed (non-404) optional NAPI resource raises instead of reading as absent, and the NFL
  adapter does not fetch CBS's odds when the caller already supplies the line. (#922)
- **CBS football adapter:** a failed scoreboard page (a dead connection, a 403/5xx after retries) is read again after
  5 minutes instead of being remembered as "no games" for the life of the process. (#922)
- **CBS NAPI:** an unknown id (HTTP 200 with a `warnings` entry of code 404) raises `NoDataError`; it
  parsed to a zero-row frame. (#919)
- **CFB / NFL fumble attribution:** a recovery-less fumble behind ESPN's degenerate end block that kept the ball
  (non-scoring, `change_of_pos_team` False) is no longer lost: CFB 401856674165 (EPA unchanged); NFL is unchanged except
  2810260184707, a 2008 fumble out of the end zone for a safety, correctly no longer a lost fumble. (#911)
- **CFB / NFL:** the advanced box's `expected_turnovers` takes its pass term from ESPN's box passes
  defended, not from "broken up by" text whose coverage swings by season (2025 CFB weeks 1-8 vs
  9-15: 0.68 vs 1.45 before, 1.26 vs 1.46 now); text stays the fallback where the box has no PD. (#737)
- **CFB `advBoxScore` row order:** `team` / `situational` / `defensive` / `drives` list the home team first, and the
  usage and special-teams sections (CFB and NFL) sort by their keys, so a game serialises to the same box in every
  process; before, these rows reshuffled from run to run. Values are unchanged. (#910)
- **CFB crosswalks:** `cfb_schedule_crosswalk` builds the same rows from the same upstream data. A Yahoo week that
  answered HTTP 200 with a cut-off JSON body was skipped, so its Yahoo ids and Yahoo-only games came and went between
  nightly builds. (#812)
- **CFB crosswalks:** a full-season build raises `CrosswalkSourceError` on any failed ESPN calendar, ESPN week or Yahoo
  week instead of skipping it. (#812)
- **CFB pbp:** `fumble_lost` / `fumble_recovered` read the recovering team from the text: a punt returner recovering
  his own fumble is no loss, and a muffed punt the kicking team recovers is one. (#778)
- **CFB pbp:** `td_play` follows a touchdown type that 2014+ scoring-summary text never spells out ("12 Yd
  Interception Return (X Kick)"). (#777)
- **CFB pbp:** kickoff touchbacks come out to the 25 from 2012 (they switched only after 2013, in `yds_kickoff_return`
  and the EP touchback yardline), and `yds_fumble_return` reads the 2004-06 "returned for a N yard touchdown". (#778)
- **CFB pbp:** pre-2014 onside recoveries (`Kickoff Return (Defense)`) are typed `Kickoff` before the kickoff
  possession flip, so they carry `kickoff_play` / `kickoff_onside` and the right kicking / receiving team. (#777)
- **CFB play-by-play:** `lag_half` and `game_play_number` follow the final row order (15 captures moved); a play
  filed under an id outside the feed's numbering is placed by its period and clock (401425418's third-quarter
  kickoff was the game's first row). (#837)
- **CFB play-by-play:** a `drive.*` column no drive of a game carries (no `end` block in 2005-09 games, no
  `start.clock` or `team` in a few others) is an all-null column of its documented String / Int64 type, not
  `Null`; every `drive.*` field is typed from the returns table. (#980)
- **CFB play-by-play:** a feed whose plays all lack the `end` block or the scores processes instead of raising
  `ColumnNotFoundError` (NFL too, for `start`, `end`, `clock` and the scores); a pickcenter with no spread on any
  provider takes the no-pickcenter path instead of raising `IndexError`. (#837)
- **CFB play-by-play:** a fumble recovered by a team written mixed-case ("recovered by AlbSt") or in
  parentheses ("recovered by Omar Jacobs (BGSU)") is charged to the right team; 2014-20 team
  fumbles lost now match ESPN's box in 97-98% of team-games, from 86-90%. (#767)
- **CFB play-by-play:** a fumble's recovering team (and so `is_turnover`, `fumble_lost`, EPA/WPA) comes from the
  recoverer's box-score team; ESPN's `recovered by <TEAM>` token can name the wrong side and is now the
  fallback. 2024-26 recoverer team matches the box in 100% of sampled recoveries, from 90-95%. (#1015)
- **CFB play-by-play:** a missing pregame total is 55.5 on every odds path (the pickcenter path used 55.0) and warns
  `errors.DefaultOddsWarning`; ESPN's literal play text "null" is no longer a player name. (#837)
- **CFB play-by-play:** kneel-downs ("Kneel down by TEAM ... (team loss of 2)", "takes a knee") and rushes no yardage
  reader matched (2004-13 "rush up the middle for 21 yards", yardless vendor rows) carry rushing yards; 2025 team
  rushing yards match ESPN's box in 64% of team-games, from 46%. (#768)
- **CFB play-by-play:** pre-2014 extra-point rows ("Extra point by Dave Rayner (MSU) is good.", "Felipe
  Macias extra point GOOD.") set `xp_attempt`, `xp_made` and `xp_kicker_player_name`; all 46,762 had
  none. (#771)
- **CFB play-by-play:** pre-2014 penalties ("HAWAII penalty 15 yard personal foul accepted") are charged
  to the team named in the text; 2006-13 team penalties match ESPN's box in 72-83% of team-games,
  from 21-25%. (#769)
- **CFB play-by-play:** regex-fallback player names no longer carry the clause they ran into
  ("Tommylee Lewistackled b", "Logan Hall, clock:32", a spot read as a fumbler); garbled names fall
  below 0.3% in every role and era, from up to 21%. (#772)
- **CFB:** `cfb_rosters_crosswalk(providers=(..., "yahoo"))` fills `yahoo_athlete_id` from Yahoo's team roster,
  matched by name + jersey, then by a unique name; it read the ~200-player stat-leaders feed and found 0-2 per team.
  New optional `yahoo_team_id` skips the Yahoo team lookup. (#762)
- **CFB:** `CFBPlayProcess(odds_override=...)` reads `homeFavorite` and `gameSpreadAvailable` strictly, as
  `overUnderAvailable` already was: `"False"`/`"0"` are False (`bool("False")` read them as True), and a value that is
  not True/False, 1/0 or `"true"`/`"false"`/`"1"`/`"0"`, None included, raises `ValueError`. (#946)
- **CFB:** `CFBPlayProcess.cfb_pbp_disk()` documents that an offline rebuild needs `join_participants=False` and
  `odds_override`. (#852)
- **CFB:** `CFBPlayProcess` logs through its own `sdv.cfb_pbp` logger; its root-module `logging.debug` calls ran
  `logging.basicConfig()` and installed a stderr handler on the host application's root logger. (#895)
- **CFB:** `firstHalfKickoffTeamId` is the opening kicker and `pos_team_receives_2H_kickoff` is true only on its
  first-half possessions; it was inverted on every first-half row of home-kicked games, and set after halftime. (#781)
- **CFB:** `fox_cfb_play_process` reads Fox's absolute `yardStart` through `fox_common`'s geometry, so
  away-offense snaps are no longer mirrored (EP/EPA/WPA); an uncovered game raises `NoDataError`, the
  backup makes no ESPN request, and the source contract rejects an adapted summary with a mirrored field. (#783)
- **CFB:** `get_go_wp` returns NaN for a 4th-down row with no `yards_to_goal` or `distance`, instead of
  a garbage go-for-it value from casting NaN to an integer; other plays are unchanged. (#726)
- **CFB:** `parse_cfb_ncaa_scoring_summary` reads `team` from the "Has Ball" logo's alt text; it was null on
  every scoring row of every stats.ncaa.org football box score. (#789)
- **CFB:** `yds_rushed` (and the line-yards split) is read from rush texts that left it null: "for 1 yard", "rush for
  loss of N", "Banyard rush" (matched "Yard Rush"), "1 Yd Run (... Run for Two-Point Conversion)", and directional
  gains on fumbled or flagged rushes. (#854)
- **CFB:** a defensive Timeout row's `pos_team` is the offense, as nflfastR's `posteam`, so its `wp_before` adds the
  offense's margin to the offense's EP; its EPA stays 0. (#905)
- **CFB:** a fumble play the offense kept (`change_of_pos_team` False) whose ESPN end block sits at yardLine 0 /
  yardsToEndzone 0 no longer scores as a goal-line turnover: its end state is the next snap's start (one 2025 Q4 own
  recovery moves from EPA -5.91 to -0.41; 20 rows in 18 games of 2024-2026 move). (#834)
- **CFB:** a game with no pregame spread (`gameSpreadAvailable` False, rebuilds included) warns the new
  `errors.DefaultOddsWarning`. (#828)
- **CFB:** a half-ending play keys `wp_after` on the next real play's team (cfbfastR's rule), not its own possession
  change: a play that kept the ball when the other team snaps next now takes 1 - that play's `wp_before`. (#966)
- **CFB:** a play ESPN files twice under two ids is booked once when the id sort strands one copy outside its drive
  (401420836's 11:58 rush and 10:44 punt now match ESPN's box score); a clock half its period shares is never compared.
  Across the 23,024 cfb-raw games: 489 fewer rows in 235 games, each removed row's twin kept (#925)
- **CFB:** a play ESPN files under an out-of-scheme id (a 10-digit `<game_id>0` among 18-digit ids) goes right after
  the last play that precedes it by period, clock and, on a clock tie, ESPN's feed order, instead of after a count of
  such plays; a post-touchdown personal foul now follows its touchdown. (#908)
- **CFB:** a rush, pass, sack or penalty whose ESPN end block is `{down 0, yardLine 0, yardsToEndzone 0}` takes the
  next snap's down, distance and spot when the same team snaps next in the period (a 4th-down play short of the line
  to gain stays a turnover on downs); EPA moves on those rows, e.g. -1.85 -> -0.95 on a 2nd & 5 incompletion. (#905)
- **CFB:** a shootout try ESPN files after a touchdown still counts inside the shootout: shootout win probability, no
  kick recommendation. (#792)
- **CFB:** a Timeout row (filed under the team that called it) is no longer a change of possession: the next play reads
  the last real play's possession and score, and a play's `wp_after` the next real board. (#834)
- **CFB:** an overtime try is a two-point shootout attempt only under its season's rule set
  (`model_vars.OT_SHOOTOUT_FROM_PERIOD`: none before 2019, from the fifth overtime in 2019-20, the third from 2021).
  (#792)
- **CFB:** an untyped overtime shootout attempt ("Sam Howell Two-Point Rush Attempt Failed") is a missed attempt, not a
  scrimmage snap. (#890)
- **CFB:** every attempt of a shootout period that does not open tied on ESPN's scores has null `wp_before` /
  `wp_after` instead of a value read off them; the game-ending attempt keeps the result. (#890)
- **CFB:** mostly first-half endings move (max |change| 0.161), in `wp_after`, `wpa`, `def/home/away_wp_after` and
  their `_naive` twins. (#966)
- **CFB:** only nulls change and EPA/WP do not move; a rush whose text names no carry stays null. (#854)
- **CFB:** overtime win probability values the forced two-point try after a touchdown (the third
  overtime on before 2021, the second from 2021; `cfb_wp_overtime.OT_TWO_POINT_FORCED_FROM`) instead of a
  kicked extra point in every period; `get_2pt_probs` recommends the try there with `xp_wp` null. (#743)
- **CFB:** penalty rows after a defensive timeout lose a phantom `EP_between`, so their EPA moves by up to 6.2 points;
  `firstD_by_*` flags and WP columns move with it, and `lead_wp_before2` skips timeouts to the second real row. (#834)
- **CFB:** scoring zero rows (QBR on a live game's opening drive, `predict_from_card` on an empty frame)
  no longer logs XGBoost's "Empty dataset" warning. (#726)
- **CFB:** the fourth-down (go, punt, field goal) and two-point decision surfaces put a post-score
  kickoff at the season's touchback spot, 80 to go through 2011 and 75 from 2012; they used 75 for
  every season. 2012+ values are unchanged. (#842)
- **CFB:** the mirrored-end repair reads across a Timeout or End Period row to the next real snap when that snap starts
  where the play's `statYardage` ends it (a Q3-closing rush to the 1: EPA -4.62 -> 0.46). (#905)
- **CFB:** the payload's `pickcenter` odds are plain numbers (they were one-element arrays) and include `homeFavorite`,
  so they feed straight back into `CFBPlayProcess(odds_override=...)`; values and play columns are unchanged. (#828)
- **CFB:** the WP boosters learned that flag as `1 - is_home` and are still fed that (the `_as_trained_2h_flag` shim),
  so published WP is unchanged (each game's first row aside) until the boosters are retrained; no model bytes changed.
  (#781)
- **CLI:** `sdv` prints `sdv: <msg>` and exits 2 on a `NoDataError` / `AssetFetchError` instead of a traceback. (#899)
- **Codegen:** each `generate.py --check` gate renders into its own temp dir, so two concurrent checks no
  longer delete each other's files. (#761)
- **Cricket:** `cricket_match_state` reads a DLS-reduced innings' revised over allocation from the score string instead
  of the format's full ball budget. (#738)
- **Crosswalks:** `{mbb,wbb,nba,wnba}_player_crosswalk(season=)` for a past season reads that season's
  ESPN player box scores; ESPN's team-roster endpoint ignores `?season=`, so past seasons shipped
  the current roster under their own label (MBB 2025, WBB 2014-2025). (#782)
- **Crosswalks:** the `_crosswalk_alt` CBS parse skips a box score whose `playerStats` is a list: CBS answers a few
  college football game ids with a basketball box (CFB 2017 and 2021), which used to stop the whole season's parse.
  (#833)
- **discover:** `function_count()` and `list_functions()` cover all 105 documented leagues (they skipped 60),
  from a table codegen writes from the league registry, and count the `nba_stats` / `wnba_stats` /
  `nbagl_stats` wrappers with their league. (#1021)
- **Docs build:** reference-page front matter quotes a `title` or `sidebar_label` only when plain YAML would misread
  it, so the ABA and TBF pages (labels with a colon and a space) build again; tests now check every page's front
  matter and every `_category_.json`. The scb `x-api-secret` note no longer claims to mirror the site's code. (#861)
- **Docs:** `espn_<lg>_calendar` in seven leagues, and the wbb/wnba `standings` / `team_roster` / `draft`, are
  documented as the hand-written functions those names run, not as the generated wrappers they shadow. (#901)
- **Docs:** `nba_live_boxscore` / `wnba_live_boxscore` document their six returned frames, one table each, and
  the `nba_stats` / `wnba_stats` wrappers list the `NoDataError` / `ValueError` / `AssetFetchError` they raise. (#901)
- **Docs:** a function counts as documented only by its own reference block, not by a mention in a table or note, so 19
  public functions (`load_pbp`, `atrium_end_year`, `shot_value`, ...) and two classes now have entries. (#901)
- **Docs:** a reST literal that starts with an underscore (``` ``_request`` ```, ``` ``_PBP_COLS`` ```) no longer loses
  its backtick on the generated pages. (#862)
- **Docs:** Examples and page titles name each league's home module (`sportsdataverse.soccer.mls`), not its
  deprecated alias; the NFL Pro page has a title, and `parse_summary` gives the right section count. (#901)
- **Docs:** reference pages no longer cite private repositories, working notes, plan task ids or Scala
  line numbers; `generate.py --check` now fails on such text in any `docs/docs` page. (#1021)
- **Docs:** tutorial install lines use `pip install --pre` for the 0.2.0 beta and name the `sportypy` extra where a
  court, rink or pitch is drawn; the international basketball tutorial no longer says the `sdvplot` extra brings
  sportypy, and the NFL tutorial skips its NFL Pro cell without credentials instead of printing the auth error. (#1021)
- **Docs:** tutorial pages print a skipped call as one short line (no repeated `NoDataError: NoDataError:`, no
  HTML error page) and carry a real meta description, the opening paragraph of their notebook. (#1021)
- **Errors:** a 404 message reads `No data found for <url>`, not `NoDataError: No data found ...`, so a printed
  `NoDataError` no longer shows its class name twice. (#1021)
- **ESPN calendars:** `espn_mbb_calendar` / `espn_wbb_calendar` accept `season=None` (current season) and a float
  `2024.0`. (#855)
- **ESPN game rosters:** `espn_mlb_game_rosters` returns a zero-row frame with its columns when ESPN lists no
  competitor roster; it returned `None`. (#878)
- **ESPN game rosters:** an athlete `$ref` answering 404 drops that athlete with a `UserWarning` instead of failing the
  game; a single team logo is kept as `logo_href`, and an unreadable logo shape warns instead of writing `""`. (#878)
- **ESPN game rosters:** the seven Core v2 `espn_<lg>_game_rosters` (mbb, wbb, nba, wnba, nfl, nhl, cfb) keep
  `position_id` (integer), `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf`
  (football and hockey also `position_parent_href`), as hoopR and wehoop do. (#878)
- **ESPN parsers:** `parse_athlete_stats` reads the Web v3 per-season rows (it returned an empty frame for every
  athlete); `parse_event_competitor_statistics` reads Core v2's `splits` object and `parse_draft` the season-draft index
  (both raised `AttributeError` on every real call). Their returns tables are documented from real captures. (#889)
- **ESPN parsers:** `parse_items` warns when ESPN answered one page of several (`espn_nba_athletes_index()` returns 100
  of 610 athletes by default). (#880)
- **ESPN parsers:** the 30 `_common_espn_parsers` parsers that raised `AttributeError` on a non-dict body, or turned
  ESPN's 200 error envelope into a one-row frame, return zero rows. (#880)
- **ESPN rankings:** `espn_{cfb,mbb,wbb,mch,wch}_rankings()` returned zero rows (the Site v2 polls sit in a top-level
  `rankings` list, not Core v2 `items`). A dedicated `parse_rankings` gives one row per ranked or vote-receiving team
  per poll. (#732)
- **ESPN rankings:** columns are `poll_id`, `season`, `season_type`, `week` (within the season type), `rank` (null on
  vote-receiving rows), `previous_rank`, `points`, `first_place_votes` and the team fields; `team_id` is a string.
  (#732)
- **ESPN rankings:** the college baseball and softball wrappers use it too, but ESPN answers their rankings URL with a
  404. (#732)
- **ESPN schedules:** `espn_mbb_schedule(dates=2024)` returns the whole 2023-24 season, November through the title
  game, sorted by date (one request per game day); it returned Jan 1-13 2024 only. A failed day, or one that fills its
  `limit`, raises `AssetFetchError`. (#855)
- **ESPN schedules:** all eight `espn_<lg>_schedule` return a zero-row frame with the documented columns on an empty
  slate; one malformed event no longer discards the day; `Canceled` games get null linescores like `Postponed`; NFL
  `*_current_rank` is a null integer (was the CFB default 99); parsing the same payload twice no longer raises. (#855)
- **ESPN soccer/cricket:** the 68 wrappers that parse through `parse_soccer_*` / `parse_cricket_*` now document
  those parsers and their returns tables (the 12 headline soccer leagues showed the generic tables), and every
  ESPN wrapper is annotated `Union[pl.DataFrame, pd.DataFrame, Dict]` instead of `Dict`. (#858)
- **ESPN soccer:** `parse_soccer_summary` (every `espn_<league>_summary`) warns `UserWarning`, naming the event id, when
  a payload has a match header but empty `key_events`, `commentary` and `lineups`, instead of returning zero-row frames
  silently; ESPN lists usa.nwsl 2014-2018 but ships no detail for those seasons. (#742)
- **ESPN soccer:** summaries null the `(0.0, 0.0)` "no location" coordinate pairs and strip trailing spaces from names.
  (#738)
- **ESPN teams:** a payload without `sports[0].leagues[0].teams` returns a zero-row frame with the documented columns
  instead of raising `TypeError` / `IndexError`, and the docstrings no longer name a `clear_cache()` that never existed.
  (#888)
- **ESPN teams:** the eight `espn_<lg>_teams` (mbb, wbb, nba, wnba, nfl, nhl, cfb, mlb) drop their process-lifetime
  `functools.lru_cache`, which raised `TypeError` on `headers={...}`, shared one frame object across callers and never
  expired. Each call returns its own frame; `sportsdataverse.cache` is the opt-in TTL cache. (#888)
- **ESPN:** `espn_<league>_leaders()` returns one row per athlete with every stat as `{category}_{stat}` (Float64)
  and a `sort_order`; it flattened ESPN's envelope into one row. Football `category=` takes `offense:passing` (a bare
  `passing` is an HTTP 400), and a league with no leaderboard gives zero rows. (#1018)
- **ESPN:** `espn_<lg>_player_stats` raises `AssetFetchError` when the athlete or team dereference fails (403, 429, 5xx,
  a dead connection) instead of returning a row with null identity columns; only a 404 leaves those columns null. (#907)
- **FIBA game wrappers:** a FIBA game page asked under a `comp` that does not list the game raises `AssetFetchError`
  (wrong scope) when GDAP knows the game, and `NoDataError` when it does not. (#864)
- **FIBA:** a `comp=` override must be an edition of the bound family; a competition GDAP gives no family is refused
  with that reason. (#864)
- **Fox / CBS / Yahoo / Shield / NCAA adapters:** an id-map row with float ids adapts like one with text ids
  (`"401671775"`, not `"401671775.0"`); a None / NaN / `pd.NA` id counts as missing. (#928)
- **Fox CFB / college basketball:** a failed Fox CFP group fetch in `fox_cfb_schedule(season)` is retried once, then
  warns (a 404 means "no CFP group"); `fox_{mbb,wbb}_teams_all` warn when `max_calls` runs out while the last fetch
  was still finding teams. (#922)
- **Fox football adapter:** a punt, kickoff or missed field goal with no next snap (end of a half or of regulation, or
  the newest row of a live poll) ends with the receiving team at 1st & 10 (the kicking team if it recovers a muff).
  (#922)
- **Fox football adapter:** an event-less "No Play" penalty (a club-charged Delay of Game) is typed Penalty (8) and
  starts at the flag's spot, not End Period with the next snap's; a sack for a safety is a Safety scored by the defence.
  (#922)
- **Fox football adapter:** the last regulation snap of an overtime game no longer takes the OT kickoff's spot as its
  end state, and a live poll no longer marks both clubs `winner`. (#922)
- **Fox:** an empty standings (or polls, events, nav, header, search, trending, roster) table parses to a
  zero-row frame instead of rows from another list in the body. (#901)
- **Generated loaders and wrappers:** `seasons=np.int64(2024)` and `seasons=2024.0` load the 2024 season (they raised
  `TypeError`), and a fractional year raises `ValueError` instead of loading the truncated one. `bool_str("false")`
  is `"false"`; ESPN path params are URL-quoted; a misspelled ESPN wrapper kwarg names the wrapper. (#896)
- **Generated wrappers:** `typing.get_type_hints()` resolves every generated ESPN and flat-API wrapper (it raised
  `NameError` on `pl`), and wrappers that return a dict of frames (`espn_*_summary`, the CDN game payloads,
  multi-result-set `nba_stats` endpoints) are annotated `Dict[str, pl.DataFrame]`, as their docstrings say. (#891)
- **Genius hosted pages:** the HTTP 500 "A website error has occurred" page is reported as an intermittent Genius
  host-side failure marked `transient` (retry later), not as "the org has no plain hosted site"; the same WBBL, SLB,
  BAL, NZNBL and ESAKE schedule URLs answered it and then a full schedule. (#870)
- **Genius:** `esake_pbp` and the rest read the game's LiveStats `/u/` shell (one cached request per game) and check its
  competition name; when that request fails twice or the shell names none, the call raises `AssetFetchError` saying
  the scope check could not run, rather than returning the game unchecked. (#864)
- **Genius:** `lfb_schedule(season=2019)` (and `genius_schedule` of any page with an unfilled play-off slot) passes its
  scope check: a `To be determined` block takes the competition the page's other blocks prove, and a page of only such
  blocks raises `AssetFetchError`. `lfb_*` covers the two seasons Genius hosts, 2018 and 2019. (#864)
- **Genius:** HTML pages served without a declared charset are read as UTF-8, so accented competition and team names
  (French, Greek) no longer come out garbled (`requests` assumed ISO-8859-1). (#864)
- **Helpers:** `find_athlete` raises `AssetFetchError` when a roster fetch fails (403, rate limit, 5xx) instead of
  reporting "no such player", skipping only a 404 roster; `find_athlete` / `find_event` raise `ValueError` (not
  `KeyError`) for an unknown league. (#899)
- **Her Hoop Stats:** `herhoopstats_team_roster` answers a zero-row frame with the roster columns when the
  page has no roster; it returned the page's tallest table. (#919)
- **Hockey:** `sihf_phases` gives a null `phase_name` when a phase's name is missing, blank or not
  a string, like the other SIHF parsers; it gave `""` for a missing or blank name and the
  stringified value (`"['x']"`) for a list or object. (#827)
- **Hockey:** a `{"data": null}` body parses to a zero-row frame (it was one all-null row) in the generic KHL webcaster,
  Liiga and National League parsers; `khl_playoff_bracket.game_ids` is `List(Int64)` (it was a comma-joined string).
  (#865)
- **Hockey:** the European hockey throttle now leaves at least 1 s from the END of one response to the start of the next
  request to the same host, with one request in flight per host (it spaced request starts, so a 0.8 s answer let the
  next request go 0.2 s later). Every transport path records the finish, error answers and exceptions included. (#865)
- **HockeyTech (PWHL and the 19 league families):** a call with no `season` reads `modulekit/seasons`
  once to pick and resolve the newest regular season; it fetched the feed twice under the 0.4 s rate
  limit. (#992)
- **HockeyTech:** `<league>_schedule(season=...)` (the 19 league families and `pwhl_schedule`) returns that season's
  games; it read `modulekit/scorebar`, which ignores `season_id` and stops at 10,000 rows oldest-first, so
  `ahl_schedule(season=2026)` returned AHL games from 1995-2012 and PWHL every season's. (#727)
- **HockeyTech:** `<lg>_game_corsi` / `pwhl_game_corsi` without a game summary (MJHL, whose `gc/gamesummary` is
  denied) return zero rows instead of crediting every shot to the away skaters; a blank `""` `home_team_id` is skipped
  too. (#988, #989)
- **HockeyTech:** `add_coord_transforms` (every `<league>_pbp` and `pwhl_pbp`) put home-team events off the rink: it
  mirrored centre-origin feet with a 0-200 x 0-85 flip, and `y_coord_fixed` applied the y transform twice. The derived
  columns are now rotations of the feet frame; the feed's home team attacks toward x = 0 in every period. (#735)
- **HockeyTech:** `game_status` reads the schedule view's strings: `Final OT` where scorebar said `Final 1st OT`, and
  the start time (`7:00 pm EST`) for an unplayed game. (#727)
- **HockeyTech:** `ot_period_length` carries the regular-season OT length for PWHL (300), ECHL and USHL (420) and BCHL
  (600). (#738)
- **HockeyTech:** `parse_game_summary` adds its `game` stub row only with a `game_id` or data. (#923)
- **HockeyTech:** `shot_distance`, `shot_angle`, `scoring_chance` and the xG features of those empty-net goals change.
  (#735)
- **HockeyTech:** `x_coord_fixed` / `y_coord_fixed` = (-x, -y), home team shooting right; `x_coord_right` /
  `y_coord_right` = home (-x, -y), visitor (x, y); `x_coord_vertical` / `y_coord_vertical` = (-y_right, x_right). (#735)
- **HockeyTech:** a season's playoffs and preseason are separate HockeyTech season ids, so pass `season_id` for them;
  an empty reply, or a season filter that keeps nothing, is zero rows with the 12 columns. (#727)
- **HockeyTech:** an empty-net goal (`empty_net` "1") is measured to the net its team attacks, so a goal from the
  team's own half is the long shot it is; every other event still measures to the nearer net (`89 - |x|`), which is
  right for own-half events with a goalie in net, whose coordinates the feed mirrors. (#735)
- **HockeyTech:** the 0.4 s request spacing holds when several threads call `hockeytech_api` at once; they could all
  read one stale timestamp and fire together. (#924)
- **HockeyTech:** the client unwraps a JSONP body that ends in `);`. (#851)
- **HockeyTech:** the right and vertical columns are null when an event's side is unknown (no team, as on faceoffs, or
  a null or empty home team id) instead of defaulting to the visitor's. `shot_distance`, `shot_angle` and xG are
  unchanged; the published PWHL play-by-play keeps the old values until it is rebuilt. (#735)
- **HockeyTech:** the schedules read `modulekit/schedule&season_id=<id>`, which the feed scopes to one season, and
  `parse_schedule(..., season_id=)` drops any row from another season. (#727)
- **HockeyTech:** with neither `season` nor `season_id`, a schedule is the newest regular season, as in the other
  season-aware functions; it used to be the feed's whole window. (#727)
- **HTTP:** `_download_json` (and the Yahoo Shangrila getter) re-fetches a truncated JSON body up to twice. (#812)
- **International basketball dtypes:** a column name has one dtype across a family's tables:
  `fiba_gamepage_box_teams.blocks_received` is Int64 (was String), and `utc_offset` is Float64 on every GDAP table (a
  half-hour zone such as India +5.5 is a fraction). (#864)
- **International basketball dtypes:** VTB Infobasket `plus_minus` / `player_position` / `height` / `weight`, the KZS
  match `season` and the TBF team-box `efficiency` are Int64; SCB `shirt_number` is always String (a `00` jersey
  survives); Sportality per-game averages (`blkpg`, `orpg`, `plus_minus_pg`) are Float64. (#864)
- **KBL:** the docs say an earlier (pre-2006-07) game's shot chart is a zero-row frame, as the code
  returns, not `NoDataError`. (#892)
- **KenPom / Her Hoop Stats:** a failed login request raises the package's error vocabulary
  (`AssetFetchError` / `NoDataError` / `ValueError`) instead of a bare `requests` error. (#919)
- **KenPom / Her Hoop Stats:** a member page answered with the login form or Her Hoop Stats' free-tier
  teaser (expired, replaced or unsubscribed session) raises `AssetFetchError` and drops the cached session. (#919)
- **KenPom:** `parse_kenpom_page()` keeps a jersey `"00"` (team and box pages): `read_html` typed the jersey column,
  so `"00"` became 0. Any other zero-padded text column also stays text. (#928)
- **KHL:** `khl_player_stats(position="goalies")` declares the goalie columns (`w`, `l`, `sop`, `ga`, `sv`, `sv_pct`,
  `gaa`, `so`, `toi`), fetches every page and sorts by games played, wins, then name (page 1 used to be the goalies
  without a game). A partial page set raises `AssetFetchError`. (#865)
- **KHL:** a khl.ru answer that is ServicePipe's anti-bot JS-challenge page now raises
  `AssetFetchError` naming the block (retry later or from another network) instead of
  "answered HTTP 200 without a sessid". (#866)
- **Lega Basket:** the play-by-play `side` is documented as what it is, the basket the court zone is measured from
  (0 = x 0, 1 = x 100), not the home/visitor team (that is `team_side`). (#864)
- **MBB / WBB:** `ncaa_mbb_box_scores(..., errors="skip")` (and the WBB twin) records a
  `parse_failed` status row for a game whose page parses to zero player rows (a page of another
  type); the game used to vanish from the season with no row. (#1014)
- **MBB / WBB:** `ncaa_mbb_play_by_play` / `ncaa_wbb_play_by_play` fetch a page the format guard rejects once and
  warn once; they re-fetched it as an empty scrape, a second stats.ncaa.org request and a second warning. (#887)
- **MBB / WBB:** `ncaa_mbb_play_by_play` / `ncaa_wbb_play_by_play` raise an `UnsupportedPageFormatWarning` the
  caller escalated to an error; they swallowed it, re-fetched the page and returned 0 rows. (#853)
- **MBB / WBB:** `parse_ncaa_bb_game_pbp` on a 2018-format page and `parse_ncaa_bb_team_schedule` on the
  older team page (2016) return the zero-row frame with an `errors.UnsupportedPageFormatWarning` naming the
  format; they returned 0 rows silently, or crashed / mis-paired half the games. (#853)
- **MBB / WBB:** the bigballR-port parsers (`parse_ncaa_bb_box`, `_team_schedule`, `_team_roster`,
  `_game_pbp`, `_shots`) return a zero-row frame for a non-HTML payload, and `parse_ncaa_bb_box` for another
  stats.ncaa.org page type, instead of raising. (#789)
- **MBB / WBB:** the stats.ncaa.org pbp / shots / box drivers turn a float contest id into `"6470186"`, never
  `contests/6470186.0/`; box-score failures are logged, not printed; one owner for the WBB period model. (#853)
- **MLB docs:** `pitcher` in the `load_mlb_xera`, `load_mlb_stuff_plus`, `load_mlb_command_plus`, `mlb_statcast_search`
  (+ `_minors` / `_wbc`), `as_of_split` and `bip_trajectory_features` returns tables reads "MLBAM player id of the
  pitcher", not "Whether the position is a pitcher". (#799)
- **MLB docs:** on `espn_mlb_schedule`, `home_score` / `away_score` are described as the ESPN scoreboard score rather
  than a per-play run total. (#799)
- **MLB docs:** on the Statcast-search, `as_of_split` and `bip_trajectory_features` pages, `home_score` / `away_score`
  are the score before the pitch and `player_name` follows `player_type` (the batter by default). (#799)
- **MLB Stats API:** `mlb_api_extra` sends a set, numpy array or float `league_id` / `sport_ids` through the shared
  `_csv` (`"103,104"`, `"103"`); a set went out as `"{103, 104}"`. (#928)
- **MLB umpire zone / catcher framing:** a 2026-only pitch frame is fit on its own pitches; the 2026 guard
  dropped them all, so every pitch scored 0.5 and framing became a called-strike rate. A degenerate zone
  fit now warns. (#909)
- **MLB:** `espn_mlb_game_rosters(raw=True)` keeps its per-entry rows, now with `game_position_id`, and `athlete_id`
  there is the entry's `playerId` (it was null). (#1002, #1010)
- **MLB:** `espn_mlb_game_rosters` reads each rostered athlete from its Core v2 `$ref`, so `athlete_id`,
  `athlete_full_name`, `athlete_jersey` and the three `athlete_position_*` columns (the listed season position) are
  filled; all six were null on every real game. (#1002, #1010)
- **MLB:** `mlb_statcast_search` clamps a window before 2008-03-25 to the tracking floor with a warning, and
  `get_baselines` warns when it falls back to the nearest committed season. (#738)
- **MLB:** `pitch_features` standardizes velocity, spin, movement and release within pitcher AND season
  (`game_year`, else the year of `game_date`), so PITCHf/x speeds (2008-16) and Statcast out-of-hand speeds
  (2017+, about +0.5-1 mph) never share one mean. (#743)
- **MLB:** an entry with no readable athlete is left out with a warning; when entries exist but no athlete can be read,
  the game raises `NoDataError`. No entries at all is the zero-row frame; a failed fetch raises `AssetFetchError`.
  (#1002, #1010)
- **MLB:** the new `game_position_id` (Int64) is the position the player was entered at in this game. The athlete id
  is the entry's `playerId`, else the id in its athlete `$ref`; an inline athlete record is used as is. (#1002, #1010)
- **NBA / WNBA / MBB / WBB pbp:** `home_score` / `away_score` are rebuilt from the scoring plays when that reproduces
  the official final (late-keyed plays, 2013-19 free throws, neutral-site side swaps); new `score_source` (`"rebuilt"` /
  `"espn"`) says which (#830). (#830)
- **NBA / WNBA / MBB / WBB pbp:** a period's first play ends where the next play starts, not at the
  full period length after it started (WNBA Q2 at 9:41: 1781 -> 1800); a period's last play ends at
  0:00. Clock minutes are Int32 (WBB seconds Int32, NBA / WNBA Float64), not Float32. (#841)
- **NBA / WNBA crosswalks:** the archive team and player crosswalks no longer carry All-Star and
  other exhibition sides (e.g. WNBA 2026 TEAM SPOON / TEAM COOP); their games stay in the schedule
  crosswalk. (#786)
- **NBA / WNBA player props:** `nba_player_props()` raises its documented `ValueError` when the
  player box logs carry no `game_date`; it used to fail with polars' `ColumnNotFoundError` before the
  check ran. (#943)
- **NBA/WNBA:** `nba_availability` imputes missing GP% with the train-season median stored in the
  bundled artifact, not a median over the seasons being scored. (#850)
- **NBA/WNBA:** `nba_player_props(as_of_date=)` uses only box lines and ratings games dated strictly
  before the cut; the default is the target game's date (box logs, else schedule; else a new
  `errors.AsOfCutWarning`), so a played game never sees its own or later lines. (#850)
- **NBA:** `logistic_fit_irls` no longer overflows on separable data, and `nba_rapm` fits a single
  possession without a divide-by-zero warning. (#726)
- **NCAA discover / roster sweeps:** a team whose page 404s is skipped without counting toward the "ban-suspect"
  abort after 5 failures in a row, as the capture stage already did. (#924)
- **NCAA ESPN game crosswalk:** a crosswalk written in this process is read back instead of the cached empty
  answer from before it existed. (#924)
- **NCAA parse stage:** a family whose parser crashed is recorded in a `parse_errors` field of the game's JSON (it was
  an empty list, like a game with no shots), and the stage exits 1 when any game failed or carries one. (#924)
- **NCAA proxy pool:** `load_proxybonanza_pool` raises `AssetFetchError` on a 200 that is not the package JSON, not
  `KeyError`. (#920)
- **NCAA:** `ncaa_espn_team_crosswalk` falls back to the bundled copy only on a failed or absent release fetch or a
  drifted asset; any other error raises. `ncaa_mbb_box_scores` reports a dropped game through the module logger, not
  `print`. (#907)
- **NFL / CFB play-by-play:** `line_yards` follows the standard adjusted-line-yards band (losses 120%,
  yards 0-4 100%, 5-10 50%, nothing past 10; cap 7), and line + second-level + open-field yards now sum
  to the carry (NFL was half a yard short on every 4+ carry). `adj_rush_yardage` caps at 10. (#843)
- **NFL `advBoxScore` row order:** `team` / `situational` / `defensive` / `drives` list the home team first, so these
  four tables serialise in the same order in every process; before, their rows reshuffled from run to run. Values
  are unchanged. (#917)
- **NFL CSV loaders:** `load_nfl_teams`, `load_nfl_ff_playerids` and `load_nfl_ff_rankings` fetch through the HTTP
  gateway: 404 `NoDataError`, 400 `ValueError`, a rate limit retried then `AssetFetchError`, a challenge page refused
  (on polars 2.0 every failure was a bare `OSError`). (#898)
- **NFL fourth down:** the PAT make probability (`get_2pt_wp` / `get_2pt_probs`, `conv_1pt`) scores the FG model at
  nflfastR's pre-2015 spot (`yardline_100 = 3`) before 2015, not the modern 15 for every season; new constants
  `PAT_YARDLINE_PRE_2015` / `PAT_YARDLINE_POST_2015` / `PAT_FIRST_SEASON_FROM_15` in `nfl.model_vars`. (#742)
- **NFL pbp:** `opportunity_run` marks a carry that gained 4 or more yards; it was inverted (`<= 4`), so the box's
  `rushing_opportunity_rate` ran about 0.62 instead of about 0.46. (#739)
- **NFL pbp:** `opportunity_run`, `highlight_run` and `adj_rush_yardage` gate on `rush`, as the CFB processor does, so
  rushing touchdowns and fumbled rushes are no longer dropped. (#739)
- **NFL, CFB:** a half or game that ends on a timeout row ends on the play before it, which now takes
  `end_of_half` and `EPA == -EP_start`; the timeout row took it (EPA 0.0). (#832)
- **NFL.com Shield / 247Sports / MLS parsers:** `parse_nfl_standings` no longer raises on a non-object week or a
  non-list `standings`; `parse_sports247_teams`, `_institution_rankings` and `parse_mls_entity` read a scalar array
  (or a non-list `list`) as drift, not invented rows; 247Sports keeps a pandas-2 gap null, never `"nan"`. (#945)
- **NFL.com Shield and Statcast parsers:** records that are not a list, or a record that is not an object
  (`parse_nfl_team(["a"])`, `{"rosters": 5}`), are zero rows with a `SchemaDriftWarning` on pandas 2 and 3; it was
  an N x 0 frame on pandas 2, a raise on pandas 3. A null row (`None` / `NaN` / `pd.NA`) is no row. (#945)
- **NFL:** `clean_nfl_pbp` matches nflfastR's 15 `fix_weird_pass_plays` keys on nflverse pbp, whose `play_id` is
  Float64: the key cast goes through Int64 (`..._1611`, not `..._1611.0`), so those rows read `pass` 0, as in nflverse.
  (#852)
- **NFL:** `end_of_half` is true on a finished game's last play and on the play that ends regulation, so a
  non-scoring game-ending play scores `EPA == -EP_start` and the last drive's `fixed_drive_result` fills; the
  2002-05 feeds' untyped "End of the game." row is no longer a play. (#832)
- **NFL:** `exp_qbr` feeds the bundled QBR booster in its trainer's order; it scored sack EPA as pass EPA,
  pass as rush and rush as sack. (#795)
- **NFL:** `get_go_wp` on a full nflverse frame no longer raises pandas `PerformanceWarning`s; output is
  unchanged. (#726)
- **NFL:** `home_`/`away_wp_after_naive` were flipped on every change of possession (about 14 rows a game), and
  `wp_after_naive` / `wpa_naive` read the next row (a Timeout, or a half's end), not the next real play (up to 0.38).
  Naive home WP now runs continuously into the next real play, as in nflfastR. (#973)
- **NFL:** `NFLPlayProcess` (ESPN and the CBS/Fox/Yahoo/Shield adapters) takes a play's end team/down/distance from
  the next play in the game and half, and keeps a half-ending plain snap's possession, not ESPN's `end` (corrupt
  2014-2021); the published 2014-2021 ESPN NFL pbp and everything built from it need the reprocess. (#785)
- **NFL:** `NFLPlayProcess` reads a first down by penalty from the 2009+ NFL foul text (2002-2008 text names no accepted
  foul, so those seasons stay unrecovered), and `series` / `fixed_drive` follow nflfastR on muffed punts, timeout rows
  and the kickoff after a defensive touchdown; the ESPN NFL pbp and its series tables need the reprocess. (#806)
- **NFL:** `td_play` keeps a scored "TOUCHDOWN NULLIFIED" row the try follows (foul enforced on the kickoff),
  and clears a touchdown reversed into a safety and, from 2009, an unscored "Kickoff/Punt Return Touchdown" with
  no touchdown in its text (2024 onside/short kicks): 34 rows 2006-2024 move, all to nflverse. (#840)
- **NFL:** `td_play`, `pass_td` and `rush_td` are False on a touchdown that did not stand: "TOUCHDOWN
  NULLIFIED by Penalty" and a replay-REVERSED score ESPN leaves unscored but still types "Passing/Rushing
  Touchdown". Team box TD counts now equal ESPN's box (291018018 NO passing 4, 401671740 TB rushing 2). (#808)
- **NFL:** a half-ending play's `wp_after`, `def_wp_after`, `home_`/`away_wp_after`, `wpa` and `prog_drive_WPA` follow
  the next real play's WP (nflfastR `lead(home_wp)`): 20 half-end rows in 18 of 37 captures, plus `prog_drive_WPA` on
  400791590's Timeout after its half end; 140 rows in 139 of 268 held-out 2003-24 games, max 0.325. (#969)
- **NFL:** a kickoff the kicking team recovers (a muff, an onside kick) ends at the kicking team's next snap and is
  scored from the receiver's side, so its EPA is the loss nflverse gives it (2010 muff: +0.63 -> -3.82, nflverse
  -3.89). (#831)
- **NFL:** a kickoff's net spot is read from the receiving team's next snap only; ESPN sometimes files a kickoff
  inside the kicking team's drive, whose next snap was read as the receiver's. (#831)
- **NFL:** a pre-2009 punt (ESPN's bare "Punt" feed) ends at the receiver's next snap, like a kickoff, not at ESPN's end
  yard line: summed |EPA - nflverse| on the 93 pre-2009 punts #856 moved (644 sample games) goes 86.2 -> 11.3 (74.2
  before #856), and on all 3,315 pre-2009 punts there 1,520 -> 525. (#963)
- **NFL:** a pre-2009 return ESPN types "Punt/Kickoff Return Touchdown" with no try after it is no longer a touchdown:
  17 rows over all 6,749 nfl-raw games, all 2005-08, summed |EPA - nflverse| 103.4 -> 2.1. (#975)
- **NFL:** a punt from 2009 on also ends at the receiver's next snap, and every punt takes that snap's clocks, so
  published 2009-2026 punt EPA moves closer to nflverse, as do the rows after a punt (`EP_between`, `lag_EP_end`,
  `prog_drive_EPA`) and 4 pre-2009 punts. (#971)
- **NFL:** a quarterback with no sack, rush or penalty play gets 0 for that `exp_qbr` input, as the QBR trainer
  did, instead of 0/0 = NaN, which the booster read as missing. Spearman of `exp_qbr` vs ESPN Total QBR over
  the 573 rated 2023 quarterback-games rises from 0.807 to 0.834. (#802)
- **NFL:** a safety typed otherwise, left unscored or with no "safety" in its text takes the -2 overlay and keeps its
  2 points: all nfl-raw, 60 rows 211.2 -> 13.3 (48 before 2009, 10 in 2009-15, 2 in 2025), every nflverse-matched
  row closer, 148 score rows in 8 games now nflverse's. Seasons 2002-15 and 2025 change. (#975)
- **NFL:** a touchback kickoff (its text says touchback, or the receiver next snaps at the touchback spot) scores
  `EP_end` at its start clock, so ESPN's end clock no longer moves its EPA (2007 KC kickoff at 0:23 with a 1:08 end
  clock: +0.52 -> 0, nflverse 0), at the touchback spot when ESPN's end clock is 0; WPA is unchanged. (#856)
- **NFL:** a touchback kickoff ends at the season's touchback spot, not a fixed 75, so its EPA is 0 in every season (it
  read +0.35 before 2016, -0.25 / -0.48 in 2024 / 2025); a 2024+ landing-zone touchback keeps its next snap at the 20.
  nflverse still scores 2024+ kickoffs from 75 (touchback `epa` +0.26 / +0.35 in 2024 / 2025). (#831)
- **NFL:** before 2014, a play ESPN files under a later period than the plays on both sides of it takes their period
  (727 rows 2002-13, every one that joins agreeing with nflverse `qtr`), so a half no longer "ends" on a misfiled
  kickoff and its drives, clock and EPA follow; 261022011 now has one first-half end. (#856)
- **NFL:** kickoff and missed field goal EPA score the next possession's 1st & 10: an unscored kickoff
  at the receiver's next snap (1st & goal inside the 10), a missed field goal at the spot of the kick
  (the 20 when that is inside it), not ESPN's down -1 or stale end. (#810)
- **NFL:** overtime `wp` / `vegas_wp` follow the season's overtime rule (`model_vars.OVERTIME_FORMATS`): sudden death,
  nflfastR's One_FG form with a per-game `Drive_Diff`, or both-teams-possess (postseason 2022+, regular season 2025+),
  where an opening-drive touchdown no longer ends the game; spring-football overtime gets null. (#744)
- **NFL:** the EPA scoring overlay reads the corrected touchdown flags: a kicking-team recovery returned for a score
  takes the kicker's touchdown (2010: +6.30 -> -7.54, nflverse -7.67), and a touchdown-typed row whose score did not
  stand takes no touchdown value (2024 short and onside kicks: 5.13 / 5.75 -> 0.67 / 1.02, nflverse 0.93 / 1.42). (#856)
- **NFL:** the kickoff-touchback spot is now the rule spot per season (`model_vars.touchback_yardline`: 80 / 75 / 70
  for 2024 / 65 from 2025) in the EP feature substitution, the overtime re-score and the ESPN `yds_kickoff_return`;
  nflfastR still scores 2024+ kickoffs at 75, so kickoff-row `ep` diverges from nflverse there by design. (#744)
- **NFL:** the processor's summary-pickcenter odds are Python scalars (they were shape-(1,) arrays), so they round-trip
  through `odds_override`, and an all-null pickcenter spread takes the default path instead of raising `IndexError`.
  (#852)
- **NHL / PWHL player impact:** `build_stints` credits a shot or goal at a line-change second to the line on the ice
  before the change, not the one after it (scorer on ice: 11 -> 22 of 25 PWHL and 1 -> 13 of 14 NHL fixture goals;
  PWHL xG off the shooter's stint 6.73 -> 3.30 of 25.0). RAPM, WAR, unit and special-teams values move. (#960)
- **NHL:** `espn_nhl_pbp` reads ESPN's NHL clock as elapsed time, so `start/end.*_seconds_remaining`
  count down within each period (playoff OT 20 min, regular-season 5, shootout 0; in OT the game clock is
  the OT period clock, as in fastRhockey) and no row ends after it starts. (#857)
- **NHL:** `load_nhl_game_rosters()` accepts published roster seasons from 2010 instead
  of rejecting seasons before 2024. (#765)
- **NHL:** `load_nhl_pbp_lite()` reads the current `play_by_play_lite_<season>` files (2010-2027, every legacy column
  plus `game_date`); the legacy `play_by_play_<season>_lite` files stopped at 2026, so the current season was missing.
  (#775)
- **NHL:** `nhl_skater_war` no longer emits a `player_id` 0 row when a faceoff or penalty names
  no player on one side. (#846)
- **NHL:** the `nhl_scoreboard(team=...)` route is never cached, like `/scoreboard/now`. (#880)
- **NHL:** twelve release loaders accept 2010 (the 2009-10 season), where every NHL release starts: `game_info`,
  `goalie_boxscores`, `linescore`, `penalties`, `scoring`, `scratches`, `skater_boxscores` and `three_stars` started at
  2024, `officials`, `shifts`, `shootout` and `shots_by_period` at 2025 (all `load_nhl_*`). (#775)
- **P. LEAGUE+ (`pleague`):** `action_en` in the play-by-play is read from each row's own phrase, so free throws say
  which throw of how many (`"free throw 2, made"`) instead of every one reading `"free throw 1 of 3 awarded, ..."`.
  (#892)
- **Parsers:** 153 provider parsers (HockeyTech, NFL.com Shield, Fox, CBS, 247Sports, EuroLeague, Sleeper, NFL Pro,
  Kalshi, Polymarket, soccer, international basketball, European hockey) return zero rows for an error envelope, an
  empty `{}` record or a wrong-typed body. (#923)
- **Parsers:** a `null` row in a row list is dropped wherever it sits, instead of becoming an all-null row, raising,
  or (as the first row) changing the frame's shape; a list holding only nulls reads as an empty one. Any other row
  that is not an object is still schema drift. Covers the ESPN, NHL and MLB Stats API parsers. (#953)
- **Parsers:** the NHL api-web / EDGE, MLB Stats API and stats.nba.com parsers return zero rows for a `None`, `""`, list
  or `{code, message}` error-envelope body instead of raising or flattening it into a row. (#907)
- **Parsers:** with pandas 2 installed, a row list that stops holding objects now warns `SchemaDriftWarning` and
  returns zero rows, as with pandas 3; it came back as an N x 0 frame with no warning. Covers the ESPN, NHL api-web
  / EDGE / Stats REST / Records and MLB Stats API parsers, and `parse_draft`'s inline rounds no longer raise. (#941)
- **PWHL:** `pwhl_game_info` returns the published `load_pwhl_game_info` columns, `pwhl_player_box` the fastRhockey
  skater and goalie frames (ids Int64), and `pwhl_player_info` one row of player bio. (#851)
- **PWHL:** `pwhl_game_info`, `pwhl_player_box` and `pwhl_player_info` return data again; they read the bare-object
  `statviewfeed` `gameSummary` / `player` replies as `SiteKit` envelopes and so returned an empty frame every time.
  (#851)
- **PWHL:** `pwhl_skater_rapm`, `pwhl_unit_ratings`, `pwhl_special_teams_value` and `pwhl_skater_war`
  run on `load_pwhl_pbp` + `load_pwhl_shifts` output; they raised `ColumnNotFoundError: "secondary_type"`.
  `pwhl_team_ratings` / `pwhl_player_props` read PWHL data; they returned NHL ratings and props. (#846)
- **Python 3.9:** `sportsdataverse.cfb` imports on 3.9 again (`cfb_drive_summary` and `cfb_situational_stats` used
  runtime `X | Y` annotations); a test walks every module for runtime PEP 604/585 annotations. (#865)
- **RealGM:** every `realgm_*` function raises `AssetFetchError` when the page is still Cloudflare's
  "Just a moment..." challenge; it returned a zero-row frame. (#919)
- **Release loaders:** a body that arrived whole but does not parse is re-raised at once, without a second fetch, and
  `download()` no longer decodes an `application/octet-stream` body as JSON. (#898)
- **Release loaders:** a refused or reset connection raises `AssetFetchError` (it leaked `requests.ConnectionError`); a
  failed read is checked with a one-byte ranged GET instead of a full re-download, and a reachable asset whose read
  failed in transit is read again, a second failure classified too (404 `NoDataError`, else `AssetFetchError`). (#898)
- **Release utilities:** `sportsdataverse_upload` stops retrying a permanent `gh` failure (HTTP 400/401/404, a 403
  that is not a rate limit, a 422 other than `already_exists`, a missing release), judged on gh's stderr, instead of
  sleeping ~645 s. (#899)
- **Runtimes:** the Data API id-map lookup no longer returns an error body as the row (dispatch falls back);
  `pff_api_*` returns `{}` on a 204/205; with the opt-in cache on, statcast / torvik return the same JSON on a hit as on
  the first call; the On3, The Odds API and statcast player parsers return zero rows for a hostile body. (#921)
- **sdv-docs index:** stdlib and third-party objects a module re-exports (`typing.Any`, `xgboost.Booster`,
  `tqdm`, `functools.lru_cache`, 14 in all) are no longer indexed as sdv-py functions. SDV partials and
  type aliases whose `__module__` is `functools` or `typing` stay indexed. (#746)
- **sdv-docs:** the server opens an index whose path contains `?`, `#` or `%`; such a path used to end
  the SQLite URI early and silently open a different, empty file. A corrupt or foreign `SDV_DOCS_DB`
  now gets an `index unreadable: …` answer instead of a raised `DatabaseError`. (#746)
- **sdv-docs:** when the `mcp` import fails, `sdv-docs` prints the import error after the install hint,
  so an installed but incompatible `mcp` is visible. (#746)
- **Soccer:** `soccer_open_dataset` / `soccer_open_events` retry a truncated StatsBomb events file once,
  after evicting the partial download kloppy cached (only that file; kloppy's cache setting is untouched),
  then raise `AssetFetchError` naming the URL instead of a raw `JSONDecodeError`. (#1014)
- **Spring football:** `get_sf_constants(league, season=)` resolves the UFL 2024 / 2025 / 2026, XFL and CFL rulesets.
  (#738)
- **Statcast search:** a 200 chunk that is not a Statcast CSV (an HTML or error page) raises
  `AssetFetchError` instead of adding a junk row or reading as "no games"; a dropped connection does too. (#919)
- **stats.nba.com / stats.wnba.com:** a float or numpy `GameID` is sent as `"0022400001"`; it was sent as
  `"22400001.0"`, which the host rejects with HTTP 400. A malformed `SDV_PY_NBA_STATS_*` value raises naming the
  variable. (#928)
- **stats.ncaa.org cache:** a cached bm-verify shell, sub-1 KB stub or Access Denied page is re-fetched; it was served
  forever, so every parser read zero rows for that game until `force=True`. (#920)
- **stats.ncaa.org:** an HTTP 400 or 422 (without a WAF ban or challenge page) raises `ValueError` after one request; it
  rotated the whole proxy pool and ended in `AssetFetchError`. (#920)
- **TheSportsDB:** the default free key is `123` (key `3` now answers HTTP 403), and a free-key list that
  reaches the key's row cap warns that it is probably truncated. (#919)
- **Validation:** `validate_game` no longer crashes under polars 2.0 when the score columns are Float64, and a failure
  building the standing-scrimmage frame is reported as `box.standing_scrimmage_built` (warning) instead of silently
  lowering coverage. (#899)
- **VTB Infobasket:** an empty string in an id, rank or number column is null; the roster `rank` (blank in every
  sample) is documented as unobserved and typed Int64. (#864)
- **WBB / WNBA release helpers:** `helper_wbb_officials` / `helper_wnba_officials` skip a null official, return a
  zero-row frame with their columns when there are none, and no longer merge two id-less officials with equal fields.
  (#878)
- **WBB / WNBA:** `find_athlete(..., league="wnba" | "wbb")` no longer raises `TypeError` (`download()` got
  `return_parsed`): `espn_wbb_team_roster` / `espn_wnba_team_roster` now accept `return_parsed=False`, the generated
  wrappers' spelling of `raw=True`. (#1017)
- **WBB NCAA:** `ncaa_wbb_game_pbp` / `ncaa_wbb_play_by_play` use the period model of the game's era (halves through
  2014-15, quarters from 2015-16), not quarters always, so a pre-2016 game no longer parses to zero rows. A new
  `season=` pins the era; without it `infer_wbb_period_model` reads it from the first-period clock. (#738)
- **WBB release helpers:** `helper_wbb_game_rosters` reads `"false"` / `"F"` as false (R `as.logical`), and the ESPN
  stat pivot's float coercion maps NaN to null. (#878)
- **WBB:** `espn_wbb_calendar` accepts season 2001 (ESPN serves a women's college basketball calendar from 2000-01); it
  refused every season before 2002. (#860)
- **WBB:** `ncaa_wbb_game_pbp` / `ncaa_wbb_play_by_play` infer the era from the page for a NaN or blank `season`,
  as for `None`; they bound the quarters model and a halves-era game returned 0 rows. (#887)
- **WBB:** `ncaa_wbb_game_pbp` / `ncaa_wbb_shot_locations` read `season=2015.0` as 2015 (halves); it became
  `"2015.0"`, bound the quarters model and a halves-era game returned 0 rows. (#853)
- **WBB:** `period_and_sec_left` (the NCAA shots' `period` / `sec_left`) reads `season=2015.0` as 2015
  (halves); it became `"2015.0"` and labelled a halves-era shot with the quarters model. (#887)
- **WNBA / Summer League lineups:** rotation lineups are timed on the league's own periods (WNBA
  10-minute quarters, halves before 2006; Summer League 10-minute quarters), not NBA 12-minute ones; a
  malformed rotation returns an empty frame and every engine falls back to pbp lineups. (#818)
- **CFB:** `load_cfb_passing`, `load_cfb_receiving` and `load_cfb_rushing` declare the columns the
  assets gained (`dispersion_games`, `EPAplay_sd`, `boom_rate`, `stuff_rate`, ...).
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
  `game_json_url` in the NHL and PWHL schedules) reads as its storage type, not an `Extension` column,
  which no `.str` op, comparison or join accepts; importing sportsdataverse registers it. (#723)
- **Polars:** on polars 2.0, `pl.read_parquet(url)` gets HTTP 501 from GitHub release URLs; the release
  loaders read with `use_pyarrow=True` and work.
- **Soccer:** `soccer_open_events()` and `soccer_open_dataset()` no longer raise `AttributeError` in
  a fresh interpreter.
- **stats.ncaa.org:** the fetch layer passes the new `/stats_terms` Terms gate without returning or
  caching it as content (#568), fits its three-view cap and backs off on refusals (#569), and
  rotates the proxy when a network error interrupts acceptance (#570).

### Security

- **Cache:** the opt-in response cache stores bodies after credential scrubbing, not before, so a token or API key
  echoed in a body or URL never lands on disk. (#865)
- **Cached bodies:** only the request's own credential values are scrubbed from a stored body (now including
  `api_token=` / `user_key=`), so a signed media URL in the body (`X-Amz-Signature=`) is served as the live call gave
  it. (#897)
- **Redaction:** a `Bearer` / `Basic` credential and a whole `Cookie:` header line are hidden, and a failed request's
  `X-Api-Key` / `x-api-secret` / `X-Authorization` headers are dropped with `Authorization` and `Cookie`. (#897)
- **Redaction:** error messages, retry log lines and cached URLs also hide a URL's `user:password@`, `appkey=`,
  `access_key=`, `subscription-key=`, `sig=`, `signature=`, `auth=`, `pass=` and `Authorization` values. (#897)
- **Response cache:** `cache_ttl=0` given as an `int` no longer raises `AttributeError` when a cache mode is on, and a
  body that cannot be cached, or a binary one (`application/octet-stream`, images, parquet), is not JSON-decoded. (#897)
- **Response cache:** a request that authenticates outside its URL (an `Authorization`, `Cookie`, `X-Api-Key` or other
  credential header, or a `session=` holding cookies) is never cached, so a second credential in the same process or
  cache dir is not served the first one's body (PFF Premium, `api.nfl.com`, NFL Pro, KenPom, Her Hoop Stats). (#897)
- **Security:** re-lock certifi (2026.10.23, compromised emSign intermediates), soupsieve, cryptography, nbconvert,
  mistune, tornado, virtualenv and opentelemetry-api past their advisories. The Python 3.10 resolution keeps
  Pillow 12.3 and virtualenv 21.14.5, whose fixes need Python 3.11+. (#1021)
- **Subscription logins:** the KenPom / Her Hoop Stats session cache tells passwords apart (a changed password logs in
  again), threads that miss it together log in once, and the 247Sports token and On3 buildId are minted under a lock.
  (#897)
- **HTTP:** a credential in a query string (The Odds API `apiKey`, HockeyTech `key`, Fox `apikey`,
  ...) reads `REDACTED` in every `dl_utils.download` log line, error message and chained exception;
  The Odds API wrappers raise `AssetFetchError` on a non-2xx answer instead of returning it as odds.

### Data

- **Cricket:** pooled constants move to T20 149.5/42/33 and ODI 249.3/76/59, and both holdout fixtures carry
  `match_date`. Holdout Brier 0.1898 -> 0.1882; max per-decile gap 0.0326 -> 0.0369 (gate 0.05). (#743)
- **Cricket:** the win-probability artifacts are refit on Cricsheet male internationals 2002-2026 (3,581 T20I, 2,589
  ODI): a regime-keyed surface with per-cell support and a calibrator fitted on the regime-aware probabilities. (#743)
- **MBB / WBB pbp:** 2014-2026 free throws (ESPN types made and missed alike "MadeFreeThrow") now sit on the line in
  `coordinate_x/_y`, transformed from (25, 13.75) like NBA / WNBA's; `coordinate_x_raw/_y_raw` keep ESPN's (25, 0).
  (#844)
- **MBB / WBB pbp:** hoopR / wehoop's case-sensitive "Free Throw" match never caught those rows, so the published MBB /
  WBB files keep ESPN's coordinates on them until the next republish corrects them. (#844)
- **NCAA team ids:** 2026-27 MBB (365) and WBB (363) ids join the bundled `ncaa_teamids_{mbb,wbb}.csv` and the NCAA ↔
  ESPN team crosswalks, every team matched (West Florida joins the ASUN, New Orleans is now LSU New Orleans, Saint
  Francis left Division I). (#809)
