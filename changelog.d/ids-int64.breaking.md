- **ESPN parsers (`_common_espn_parsers`, the `espn_<lg>_<short>` wrappers it parses for):** every `id` /
  `*_id` column is `Int64` (it was String, or Float64 where the pandas hop met a missing value), so `parse_fpi`
  joins `parse_weekly_powerindex`. `now_id` (`"1-44686270"`) is not an integer and stays String. Not yet in
  this release: the soccer leagues' `scoreboard`, `standings`, `summary`, `teams_site` and `team_roster`
  wrappers and the cricket leagues' `scoreboard`, `standings` and `summary` wrappers. Codegen sends those to
  `soccer_espn_parsers` / `cricket_espn_parsers`, which do not cast, so their ids stay String for now; the same
  leagues' other wrappers (`news`, `injuries`, ...) go through the common parsers and are `Int64`. (#981)
- **ESPN hand-written families:** `espn_<lg>_schedule` (`id`, `game_id`, `*_id`, `home_/away_id`,
  `*_venue_id`, `*_conference_id`, `groups_id`; `game_id` was Int32), `espn_<lg>_teams` (`team_id`),
  `espn_<lg>_game_rosters` (every `*_id`; `team_id` / `game_id` / `position_id` were Int32) are `Int64`. (#981)
- **ESPN snapshots / content / crosswalks:** `espn_injuries_snapshot` and `espn_depthcharts_snapshot` ids,
  `parse_espn_content` `id`, and `ncaa_espn_team_crosswalk` `espn_team_id` / `espn_conference_id` are `Int64`
  (were Utf8). `load_{nba,wnba,mbb,wbb}_{team,player,schedule}_crosswalk` cast their `espn_*` ids to `Int64`. (#981)
- **NFL processor (`NFLPlayProcess`):** the `*_player_id` columns (String) and `pos_team`, `lag_pos_team`,
  `lead_pos_team`, `lead_pos_team2`, `start.pos_team.id` (Int32) are `Int64`, as in the CFB processor.
  A source whose ids are not integers (NFL.com gsis `"00-0034796"`) keeps them String. (#981)
- **NHL API parsers (`parse_nhl_web_*`, `parse_nhl_records`, `parse_nhl_stats_rest`, `parse_nhl_edge_*`) and
  `nhl_edge_skating_value`:** every `id` / `*_id` is `Int64` (was Float64 / String by payload, `player_id` a
  Utf8 pin); stats-rest `formatted_season_id` and the country `id` are text and stay String. The pandas output
  (`return_as_pandas=True`) carries them as pandas' nullable `Int64` (was `float64` / `object`). Any other id that
  is not an integer keeps its column String and warns `SchemaDriftWarning` naming it, as the ESPN parsers do. (#986)
- **`nhl_player_props` and `pwhl_player_props`:** `game_id` and `player_id` are `Int64` (were Utf8), so the props
  join the NHL parser / EDGE frames and the HockeyTech frames on `player_id`. Code that compares them to strings
  (`player_id == "8478402"`) must use ints. (#986)
- **ESPN, pandas output (`return_as_pandas=True`):** the same id columns are pandas' nullable `Int64` (was `object`,
  or `float64` such as `150.0` where a value was missing). The frames whose pandas output is Arrow-backed carry them
  as `int64[pyarrow]` instead: `parse_fpi`, `parse_weekly_powerindex` and the generated `load_*_crosswalk` loaders.
  The injuries / depth-chart snapshots read an id that is
  not an integer (text, `"007"`) as null with a `SchemaDriftWarning` naming the field. (#981)
- **HockeyTech (PWHL + 19 leagues: parsers, `<lg>_pbp`, schedules, rosters, leaders, summaries):** every `id` /
  `*_id` is `Int64` (`player_id` was Float64 in pbp, `team_id` / `game_id` String or the caller's dtype); the
  game-summary `home_team_id` / `away_team_id` / `game_season_id` are null, not `""`, when it is unavailable. The
  pandas output (`return_as_pandas=True`) carries the ids as pandas' nullable `Int64` (was `float64` such as `1.0`,
  or `str`). An id that is not an integer keeps its column String and warns `SchemaDriftWarning`. (#988)
- **`build_nfl_season` cache:** `PIPELINE_VERSION` is 2, so per-game frames cached by an earlier version (String
  `*_player_id`, Int32 `pos_team`) are rebuilt instead of being concatenated into a season with the new Int64
  ones, where `diagonal_relaxed` would turn those columns back into String.
- **Release loaders keep their published dtypes.** Some release files still carry String ids, while the live
  parsers above now emit Int64: `load_pwhl_schedules` (`game_id`, `home_team_id`, `away_team_id`),
  `load_pwhl_pbp` (`team_id`, `player_team_id`, `home_team_id`, `away_team_id`, `game_season_id`), the PWHL
  box scores' `player_id`, `load_{mbb,nba,wbb,wnba}_rosters` (`athlete_id`, `position_id`, `status_id`) and
  `load_{mbb,nba,wnba}_game_rosters` (`game_id`). Joining one of those String ids to live parser output raises
  `SchemaError`, so cast it first, e.g. `release.with_columns(pl.col("game_id").cast(pl.Int64))`. The Int32 ids
  (`team_id` in those rosters, `load_pwhl_rosters`, `load_pwhl_xg_pbp`) join Int64 ones as they are. Code that
  relied on String ids from the live parsers can cast back with `pl.col("team_id").cast(pl.String)` (pandas:
  `.astype("string")`); an `Int64` id stringifies as `"123"`, never `"123.0"`.
- **Producers that store or republish these frames: move the published ids to Int64 now.** Cast them to
  `Int64`, not back to String, and rewrite the stored history in one pass, so every season of a release
  carries the same integer ids.
  - *Deploy order:* update (or pin to a pre-release commit) every producer that writes these frames first,
    then upgrade to this release.
  - *Stored history:* a season written before this release holds String / Int32 ids, a new one Int64. Before
    the first post-upgrade run, cast the stored ids once (`pl.col(c).cast(pl.Int64)`) or rebuild every season;
    otherwise `pd.concat` / `to_parquet` of the master fails on a mixed column, and `pl.concat(how=
    "diagonal_relaxed")` silently turns the ids back into String. This covers the snapshot histories, the
    ESPN schedule season masters, and the PWHL schedule master and per-game final JSON, whose seasons mix text
    (`"1"`) and integer (`1`) ids until the JSON is reprocessed.
  - *RDS:* `pyreadr.write_rds` cannot write pandas' nullable `Int64` with a missing value (`TypeError: must be
    real number, not NAType`). Cast the Int64 id columns to `float64` for the `pyreadr` write only
    (`df[id_cols] = df[id_cols].astype("float64")`): R reads them as double, with `NA` kept, and the parquet
    and the frame itself stay Int64. `sportsdataverse._rds.write_rds` writes nullable Int64 as R integer, but
    only for a flat frame: it raises `ValueError` on a nested column, which every ESPN schedule frame has
    (`List(Struct)` linescores, records, highlights).
  - *R packages:* releases rebuilt from these parsers (the ESPN schedule seasons of the hoopR / wehoop raw
    repos, the PWHL schedule and play-by-play releases) carry integer ids, where hoopR, wehoop and
    fastRhockey document some of them as character. Republish every season in the same pass and update the R
    documentation to match.
