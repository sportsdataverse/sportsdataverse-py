<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [metric_curves fixtures](#metric_curves-fixtures)
  - [`nfl_model_pbp_BUF_2024.parquet`](#nfl_model_pbp_buf_2024parquet)
  - [`nba_stats_shots_2026_slice.parquet` and `wnba_stats_shots_2025_slice.parquet`](#nba_stats_shots_2026_sliceparquet-and-wnba_stats_shots_2025_sliceparquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# metric_curves fixtures

Real released data for `sportsdataverse/metric_curves.py`. The CFB fixture and
the Stephen Curry shots fixture live in `../rolling_windows/` (shared with the
rolling-windows tests; provenance in that folder's README).

## `nfl_model_pbp_BUF_2024.parquet`

The 2024 Buffalo Bills offensive plays (`posteam == "BUF"`, 20 games: 17
regular season + 3 postseason) from the SDV-native `nfl_model_pbp` release
(the nflfastR shape `load_nfl_model_pbp` serves), projected to the columns
`nflfastr_attempts` reads.

- Source: `nfl-data` HEAD `b78df69402c26a256def4b6c0f876223180e6c8c`,
  `nfl/model_pbp/model_pbp_2024.parquet` (the file published as
  `nfl_model_pbp/model_pbp_2024.parquet`).
- Selection (polars):

  ```python
  from sportsdataverse.metric_curves import NFLFASTR_ATTEMPT_COLUMNS

  pbp = pl.scan_parquet("nfl/model_pbp/model_pbp_2024.parquet")
  pbp.filter(pl.col("posteam") == "BUF").select(NFLFASTR_ATTEMPT_COLUMNS).collect()
  ```

- Expected counts: 1632 rows, 20 games; 602 pass attempts with air yards
  (`pass_attempt == 1 & sack == 0 & air_yards not null`, three passers); 35
  field-goal attempts (all T.Bass, 30 made); 31 fourth-down plays from
  scrimmage that stood (`play_type in (pass, run)`), 23 converted.

## `nba_stats_shots_2026_slice.parquet` and `wnba_stats_shots_2025_slice.parquet`

Real published shots for the coordinate-distance binning in `shot_attempts`.
stats.nba `playbyplayv3` reports `shotDistance` 0 for every three released
under 23.5 ft (the NBA corner); the rows below pin that defect.

- Source: sportsdataverse-data releases `nba_stats_shots/shots_2026.parquet`
  (234,876 rows) and `wnba_stats_shots/shots_2025.parquet` (41,856 rows), read
  2026-10-01.
- Selection (polars), in file order, regular season + playoffs only
  (`season_type_id` "2"/"4"; the WNBA file has no such column, so the game id's
  type digit `game_id.str.slice(2, 1)` stands in, as the producer derives it):

  ```python
  at00 = (pl.col("x_legacy") == 0) & (pl.col("y_legacy") == 0)
  sv, sd = pl.col("shot_value"), pl.col("shot_distance")
  nba_groups = [  # (filter, first n rows)
      ((sv == 3) & (sd == 0), 40),          # zeroed corner threes
      ((sv == 2) & (sd == 0) & at00, 20),   # tips / putbacks the feed places at the rim
      ((sv == 2) & (sd == 0) & ~at00, 20),  # other rim twos
      ((sv == 3) & sd.is_between(24, 26), 20),
      ((sv == 2) & sd.is_in([22, 23]), 10), # long twos
  ]
  wnba_groups = [((sv == 3) & (sd == 0), 30), ((sv == 2) & (sd == 0), 10), ((sv == 3) & (sd == 24), 10)]
  ```

  Columns kept: `game_id, season, season_type_id (NBA only), team_id,
  team_tricode, person_id, player_name, shot_result, shot_value, shot_distance,
  x_legacy, y_legacy, sub_type`.
- Expected counts: NBA 110 rows, 51 made; by `floor(sqrt(x^2 + y^2) / 10)` ft
  {0: 40, 21: 5, 22: 22, 23: 25, 24: 4, 25: 9, 26: 5} against `shot_distance`
  {0: 80, 22: 6, 23: 4, 24: 2, 25: 9, 26: 9}. WNBA 50 rows, 18 made;
  {0: 10, 22: 12, 23: 25, 24: 3} against `shot_distance` {0: 40, 24: 10}.
- No sampled published season (NBA 1997/2001/2005/2010/2016/2020/2026, WNBA
  1997/2005/2015/2025) has a null coordinate, so the fallback test nulls
  coordinates on these real rows.
