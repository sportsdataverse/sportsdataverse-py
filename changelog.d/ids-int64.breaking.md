- **ESPN parsers:** every `id` / `*_id` column from `_common_espn_parsers` (the `espn_<lg>_<short>` wrappers) is
  `Int64`, where it was String, or Float64 when the pandas hop met a missing value, so `parse_fpi` joins
  `parse_weekly_powerindex`; `now_id` (`"1-44686270"`) is not an integer and stays String. (#981)
- **ESPN parsers:** not yet in this release are the soccer leagues' `scoreboard`, `standings`, `summary`, `teams_site`
  and `team_roster` and the cricket leagues' `scoreboard`, `standings` and `summary` wrappers, whose parsers do not
  cast: their ids stay String; those leagues' other wrappers (`news`, `injuries`, ...) are `Int64`. (#981)
- **ESPN hand-written families:** `espn_<lg>_schedule` (`id`, `game_id`, `*_id`, `home_/away_id`, `*_venue_id`,
  `*_conference_id`, `groups_id`; `game_id` was Int32), `espn_<lg>_teams` (`team_id`) and `espn_<lg>_game_rosters`
  (every `*_id`; `team_id` / `game_id` / `position_id` were Int32) are `Int64`. (#981)
- **ESPN snapshots / content / crosswalks:** `espn_injuries_snapshot` / `espn_depthcharts_snapshot` ids, the
  `parse_espn_content` `id` and `ncaa_espn_team_crosswalk` `espn_team_id` / `espn_conference_id` are `Int64` (were
  Utf8); `load_{nba,wnba,mbb,wbb}_{team,player,schedule}_crosswalk` cast their `espn_*` ids to `Int64`. (#981)
- **NFL processor:** `NFLPlayProcess`'s `*_player_id` columns (String) and `pos_team`, `lag_pos_team`, `lead_pos_team`,
  `lead_pos_team2`, `start.pos_team.id` (Int32) are `Int64`, as in the CFB processor; a source whose ids are not
  integers (NFL.com gsis `"00-0034796"`) keeps them String. (#981)
- **ESPN, pandas output:** with `return_as_pandas=True` the same ids are pandas' nullable `Int64` (was `object`, or
  `float64` such as `150.0` where a value was missing); the Arrow-backed `parse_fpi`, `parse_weekly_powerindex` and the
  generated `load_*_crosswalk` loaders carry them as `int64[pyarrow]`. (#981)
- **ESPN snapshots:** the injuries / depth-chart snapshots read an id that is not an integer (text, `"007"`) as null
  with a `SchemaDriftWarning` naming the field. (#981)
- **NHL API parsers:** `parse_nhl_web_*`, `parse_nhl_records`, `parse_nhl_stats_rest`, `parse_nhl_edge_*` and
  `nhl_edge_skating_value` emit every `id` / `*_id` as `Int64` (was Float64 / String by payload, `player_id` a Utf8
  pin); stats-rest `formatted_season_id` and the country `id` are text and stay String. (#986)
- **NHL API parsers:** pandas output carries the ids as nullable `Int64` (was `float64` / `object`); any other id that
  is not an integer keeps its column String and warns `SchemaDriftWarning` naming it, as the ESPN parsers do. (#986)
- **`nhl_player_props` / `pwhl_player_props`:** `game_id` and `player_id` are `Int64` (were Utf8), so the props join
  the NHL parser / EDGE and HockeyTech frames on `player_id`; compare them to ints (`player_id == 8478402`). (#986)
- **HockeyTech:** the PWHL and 19-league parsers, `<lg>_pbp`, schedules, rosters, leaders and summaries emit every `id`
  / `*_id` as `Int64` (`player_id` was Float64 in pbp, `team_id` / `game_id` String or the caller's dtype). (#988)
- **HockeyTech:** game-summary `home_team_id` / `away_team_id` / `game_season_id` are null, not `""`, when unavailable;
  pandas output carries nullable `Int64` (was `float64` such as `1.0`, or `str`); a non-integer id stays String and
  warns `SchemaDriftWarning`. (#988)
- **`build_nfl_season` cache:** `PIPELINE_VERSION` is 2, so per-game frames cached earlier (String `*_player_id`, Int32
  `pos_team`) are rebuilt instead of concatenated with the new Int64 ones, where `diagonal_relaxed` would turn those
  columns back into String. (#981)
- **Release loaders:** they keep their published dtypes, so String ids remain in `load_pwhl_schedules` (`game_id`,
  `home_team_id`, `away_team_id`), `load_pwhl_pbp` (`team_id`, `player_team_id`, `home_team_id`, `away_team_id`,
  `game_season_id`) and the PWHL box scores' `player_id`. (#981)
- **Release loaders:** String ids also remain in `load_{mbb,nba,wbb,wnba}_rosters` (`athlete_id`, `position_id`,
  `status_id`) and `load_{mbb,nba,wnba}_game_rosters` (`game_id`); joining one to live parser output raises
  `SchemaError`, so cast first: `release.with_columns(pl.col("game_id").cast(pl.Int64))`. (#981)
- **Release loaders:** Int32 ids (`team_id` in those rosters, `load_pwhl_rosters`, `load_pwhl_xg_pbp`) join Int64 ones
  as they are. Code that relied on String ids can cast back with `pl.col("team_id").cast(pl.String)` (pandas:
  `.astype("string")`); an `Int64` id stringifies as `"123"`, never `"123.0"`. (#981)
- **Producers:** move stored and republished ids to `Int64` now, not back to String, and rewrite the stored history in
  one pass so every season of a release carries the same integer ids. Update (or pin to a pre-release commit) every
  producer that writes these frames first, then upgrade to this release. (#981)
- **Producers:** before the first post-upgrade run, cast stored ids once (`pl.col(c).cast(pl.Int64)`) or rebuild every
  season; otherwise `pd.concat` / `to_parquet` fails on a mixed column, and `pl.concat(how="diagonal_relaxed")` silently
  turns the ids back into String. (#981)
- **Producers:** that covers the snapshot histories, the ESPN schedule season masters, and the PWHL schedule master and
  per-game final JSON, whose seasons mix text (`"1"`) and integer (`1`) ids until the JSON is reprocessed. (#981)
- **Producers, RDS:** `pyreadr.write_rds` cannot write pandas' nullable `Int64` with a missing value. For that write
  only, cast id columns to nullable `Int32` when every id fits in 32 bits (`df[id_cols].astype("Int32")`: R integer,
  `NA` kept), else to `float64` (R double); the parquet and the frame stay Int64. (#981)
- **Producers, RDS:** `sportsdataverse._rds.write_rds` writes nullable Int64 as R integer, but only for a flat frame:
  it raises `ValueError` on a nested column, which every ESPN schedule frame has (`List(Struct)` linescores, records,
  highlights). (#981)
- **R packages:** releases rebuilt from these parsers (the ESPN schedule seasons of the hoopR / wehoop raw repos, the
  PWHL schedule and pbp releases) carry integer ids that hoopR, wehoop and fastRhockey document as character in places;
  republish every season in the same pass and update the R documentation to match. (#981)
