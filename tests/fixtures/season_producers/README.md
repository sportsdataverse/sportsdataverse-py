<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Season producer fixtures (supp-013)](#season-producer-fixtures-supp-013)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Season producer fixtures (supp-013)

Raw ESPN standings payloads from the raw repos and the R-released rows built from them: the oracle for
`helper_nba_standings` (`sportsdataverse/nba/nba_season_producers.py`) and `helper_wbb_standings`
(`sportsdataverse/wbb/wbb_season_producers.py`). Read by `tests/nba/test_nba_season_producers.py` and
`tests/wbb/test_wbb_season_producers.py`. Built 2026-10-11.

| file | source |
|---|---|
| `nba_standings_2024.json.gz` | `hoopR-nba-raw` `nba/standings/json/2024.json` @ `4ce184e3366` (2026-05-30), whole payload, gzip |
| `nba_standings_2024_release.parquet` | the R producer's release asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_standings/standings_2024.parquet` (690 rows, `load_nba_standings([2024])`) |
| `wbb_standings_2026_trimmed.json.gz` | `wehoop-wbb-raw` `wbb/standings/json/2026.json` @ `bca4b5bbf53` (2026-05-12), trimmed to the top-level `uid/id/name/abbreviation/shortName/season` plus the first two `children` (America East `1`, American `62`); the full file is 6.8 MB |
| `wbb_standings_2026_release.parquet` | `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_standings/standings_2026.parquet` (`load_wbb_standings([2026])`) filtered to `group_id` in `{1, 62}` (1,848 rows) |
