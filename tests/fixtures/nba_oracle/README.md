<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA oracle-CSV fixture samples](#nba-oracle-csv-fixture-samples)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA oracle-CSV fixture samples

Three-row samples used by `tests/nba/test_nba_oracle_data.py` to exercise
`sportsdataverse/nba/nba_oracle_data.py`'s six `load_*` parsers offline. Each
row below is byte-quoted from the real published CSVs checked into
`ClaudeCowork/nba_data/data_metrics/` (captured 2026-07-01/02; see that
directory for the full files, which are not committed to this repo). The
gated real-CSV smoke tests (`SDV_PY_NBA_ORACLE_DIR`, Task 9) validate the
loaders against the full files directly.

**Honesty note:** an earlier revision of these fixtures had several
round-number rows (e.g. RAPM `5.1`/`0.4`, EPM `5.0`/`2.0`/`7.0`, ewins
`18.5`/`15.2`) that were plan-dictated placeholders, not verified captures.
They have since been replaced with byte-quoted real rows below; the two
affected test assertions in `test_nba_oracle_data.py`
(`test_load_rapm_ryan_davis_schema_and_values`) were updated to match the
real values.

| File | Source file | Rows | Notes |
|---|---|---|---|
| `rapm_ryan_davis_sample.csv` | `rapm_ryan_davis.csv`, season `2009-10` | Andrew Bogut, Kevin Durant, Stephen Curry | All 3 rows are real captured values (Bogut was already real; Durant/Curry were replaced -- Curry's real rookie-year `RAPM` is `-0.01`, not the placeholder `5.9`). |
| `epm_sample.csv` | `2025_EPM_data.csv`, season `2025` | Nikola Jokic, Shai Gilgeous-Alexander, Victor Wembanyama | Jokic row was already real. SGA was a placeholder (`5.0/2.0/7.0`) -- replaced with the real 2025 row (`5.41631/1.97887/7.3952`). Wembanyama was real data but mislabeled: the fixture's numbers (`3.80691/3.99316/7.8001`) are actually his **2026** row, not 2025 -- replaced with his real 2025 row (`1.20167/2.77651/3.9782`). |
| `lebron_season_sample.csv` | `lebron-data-2026.csv`, season `2026` | LeBron James, Chris Paul, Kevin Durant | All 3 rows are byte-exact real captures. |
| `lebron_daily_sample.csv` | `lebron_daily_2026-07-02.csv` | Kawhi Leonard (2025-10-22), Victor Wembanyama (2026-04-12) | Both rows are real captures; `Mins`/`LEBRON WAR` etc. are truncated to fewer decimal places than the source's float64 repr, which is just formatting, not a value change. |
| `darko_dpm_sample.csv` | `2026-darko-dpm-leaderboard.csv` (UTF-8 BOM + `#` header) | Nikola Jokic, Kawhi Leonard, Victor Wembanyama, Shai Gilgeous-Alexander | Byte-exact real captures (first 4 ranked rows), including the sign-prefixed integer `DPM`/`ODPM`/`DDPM` columns and the leading BOM. |
| `dunks_threes_sample.csv` | `2025_Dunks_&_Threes_Stats.csv`, season `2025` | Shai Gilgeous-Alexander, Nikola Jokić, Victor Wembanyama | SGA row was already real. Jokic (`18.5`) and Wembanyama (`15.2`) `ewins` were placeholders -- replaced with real 2025 values (`19.9574` / `8.92318`); Jokić's real capture also carries the diacritic ("Jokić") that `normalize_player_name` folds to ASCII. |

To refresh a fixture, re-pull the corresponding season file from the
published source (Ryan Davis RAPM / Dunks & Threes / LEBRON / DARKO sites)
and re-quote 2-3 representative rows, keeping the exact header + dtype
quirks (BOM, sign-prefixed ints, etc.) each loader is built to handle.
