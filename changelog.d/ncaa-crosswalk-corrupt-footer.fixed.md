- **MBB / WBB:** `ncaa_espn_team_crosswalk()` falls back to the bundled copy, with a warning, when the release asset's
  parquet footer cannot be decoded (pyarrow raises a bare `OSError` there); it used to raise. (#1364)
