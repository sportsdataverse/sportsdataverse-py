<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [metric_curves fixtures](#metric_curves-fixtures)
  - [`nfl_model_pbp_BUF_2024.parquet`](#nfl_model_pbp_buf_2024parquet)

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
