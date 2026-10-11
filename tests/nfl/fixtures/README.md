<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL test fixtures](#nfl-test-fixtures)
  - [ESPN game summaries (the 31 whole-game captures)](#espn-game-summaries-the-31-whole-game-captures)
  - [nflverse slices (`nflverse/`)](#nflverse-slices-nflverse)
  - [Other files](#other-files)
  - [Overtime rule sets (C-M6)](#overtime-rule-sets-c-m6)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL test fixtures

## ESPN game summaries (the 31 whole-game captures)

Each `summary_<game_id>*` file is the ESPN Site v2 summary for one NFL game,
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`. The
`*_trimmed.json.gz` files come from the nfl-raw library (`sportsdataverse/nfl-raw`,
`nfl/espn/raw/<season>/<game_id>.json.gz`) trimmed to the keys `NFLPlayProcess` reads (header,
drives, boxscore, gameInfo, pickcenter, scoringPlays where present); `summary_401872922.json` is a
full summary fetched from the URL above. "Committed" is the commit and date that added the file to this
repository; the ESPN fetch date is not recorded with the fixtures.
`tests/nfl/test_nfl_whole_game_oracle.py` and `test_nfl_schema_allowlist.py` run every one offline.

| game_id | season | week (REG) | away @ home | file | committed |
|---|---|---|---|---|---|
| 221013007 | 2002 | 6 | MIA @ DEN | `summary_221013007_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 221013028 | 2002 | 6 | NO @ WSH | `summary_221013028_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 231221029 | 2003 | 16 | DET @ CAR | `summary_231221029_trimmed.json.gz` | 40b448e588, 2026-09-26 |
| 240919004 | 2004 | 2 | MIA @ CIN | `summary_240919004_trimmed.json.gz` | ac5b330676, 2026-09-18 |
| 241010028 | 2004 | 5 | BAL @ WSH | `summary_241010028_trimmed.json.gz` | 40b448e588, 2026-09-26 |
| 251113015 | 2005 | 10 | NE @ MIA | `summary_251113015_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 260917021 | 2006 | 2 | NYG @ PHI | `summary_260917021_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 261022011 | 2006 | 7 | WSH @ IND | `summary_261022011_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 261126002 | 2006 | 12 | JAX @ BUF | `summary_261126002_trimmed.json.gz` | d92519bcc3, 2026-09-26 |
| 271209010 | 2007 | 14 | SD @ TEN | `summary_271209010_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 290927014 | 2009 | 3 | GB @ STL | `summary_290927014_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 291018018 | 2009 | 6 | NYG @ NO | `summary_291018018_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 291115014 | 2009 | 10 | NO @ STL | `summary_291115014_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 301003002 | 2010 | 4 | NYJ @ BUF | `summary_301003002_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 331103020 | 2013 | 9 | NO @ NYJ | `summary_331103020_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 400791508 | 2015 | 16 | WSH @ PHI | `summary_400791508_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 400791567 | 2015 | 7 | PHI @ CAR | `summary_400791567_trimmed.json.gz` | 9310e773f9, 2026-09-17 |
| 400791590 | 2015 | 1 | NYG @ DAL | `summary_400791590_trimmed.json.gz` | e9c3ca7c48, 2026-09-18 |
| 400874485 | 2016 | 16 | NYG @ PHI | `summary_400874485_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 400874512 | 2016 | 2 | BAL @ CLE | `summary_400874512_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 400951676 | 2017 | 2 | SF @ SEA | `summary_400951676_trimmed.json.gz` | d28cc51212, 2026-09-18 |
| 401030824 | 2018 | 17 | LAC @ DEN | `summary_401030824_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 401128075 | 2019 | 11 | JAX @ IND | `summary_401128075_trimmed.json.gz` | 45567d8bf9, 2026-09-25 |
| 401220341 | 2020 | 10 | BUF @ ARI | `summary_401220341_trimmed.json.gz` | 9310e773f9, 2026-09-17 |
| 401547449 | 2023 | 4 | LAR @ IND | `summary_401547449_trimmed.json.gz` | c921519970, 2026-09-19 |
| 401671740 | 2024 | 4 | PHI @ TB | `summary_401671740_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 401671835 | 2024 | 17 | DEN @ CIN | `summary_401671835_trimmed.json.gz` | 9310e773f9, 2026-09-17 |
| 401772636 | 2025 | 10 | ATL @ IND | `summary_401772636_trimmed.json.gz` | 9dc97e9872, 2026-09-18 |
| 401772726 | 2025 | 2 | CLE @ BAL | `summary_401772726_trimmed.json.gz` | 9dc97e9872, 2026-09-18 |
| 401772748 | 2025 | 6 | CLE @ PIT | `summary_401772748_trimmed.json.gz` | 9dc97e9872, 2026-09-18 |
| 401872922 | 2026 | 1 | CLE @ JAX | `summary_401872922.json` | 741c871a31, 2026-09-14 |

## nflverse slices (`nflverse/`)

Two games of nflverse's play-by-play, the oracle for the series, fixed-drive, down / distance /
yard line, yardage, score, touchdown, EP / EPA / WP and CP columns of the same games' ESPN
captures. ESPN's play id is the game id followed by nflverse's GSIS `play_id`, so the two join
exactly.

| file | nflverse game_id | ESPN capture | rows x cols |
|---|---|---|---|
| `nflverse/2023_04_LA_IND.parquet` | 2023_04_LA_IND (old_game_id 2023100106) | 401547449 | 190 x 41 |
| `nflverse/2024_04_PHI_TB.parquet` | 2024_04_PHI_TB (old_game_id 2024092907) | 401671740 | 173 x 41 |

- **Source.** nflverse-data release `pbp`, assets
  `https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet` and
  `.../play_by_play_2024.parquet`, as read by `sportsdataverse.nfl.load_nfl_pbp([season])` and
  written by nfl-data's `python/model_training/play_level/ingest.py:download_pbp` to
  `nflverse-dev/nfl-data/python/data/pbp_2023.parquet` (sha256
  `dd42da597b12a47b00c4ced90888e7b7b16152f0019c8f5a059e9902a571f8a1`) and
  `pbp_2024.parquet` (sha256 `418ceb1b794c61d6f9d23aecc76bb6c8cc2f3a63941a8b5aed0c5ee928591844`),
  downloaded 2026-06-18.
- **Trimmed** 2026-10-11, from the nfl-data checkout root, to one game and the 41 columns the tests read:

```sh
uv run python -c "import sys, polars as pl; src, gid, out = sys.argv[1:]; pl.read_parquet(src).filter(pl.col('game_id') == gid).select(['game_id', 'old_game_id', 'play_id', 'season', 'week', 'home_team', 'away_team', 'posteam', 'defteam', 'qtr', 'down', 'ydstogo', 'yardline_100', 'play_type', 'desc', 'touchdown', 'pass_touchdown', 'rush_touchdown', 'rushing_yards', 'receiving_yards', 'total_home_score', 'total_away_score', 'home_score', 'away_score', 'fixed_drive', 'fixed_drive_result', 'series', 'series_success', 'series_result', 'first_down_penalty', 'ep', 'epa', 'wp', 'vegas_wp', 'home_wp', 'cp', 'air_yards', 'roof', 'pass_location', 'qb_hit', 'complete_pass']).write_parquet(out)" python/data/pbp_2023.parquet 2023_04_LA_IND <sdv-py>/tests/nfl/fixtures/nflverse/2023_04_LA_IND.parquet
# and the same with python/data/pbp_2024.parquet 2024_04_PHI_TB .../2024_04_PHI_TB.parquet
```

## Other files

- `plays_401872922.json.gz`, `box_401872925_espn_trimmed.json.gz`: see the tests that read them.
- `fox/`, `nfl_cbs/`, `yahoo_nfl/`: provider captures with their own READMEs where present.

## Overtime rule sets (C-M6)

One committed regular-season overtime capture inside each NFL overtime rule set, cut where the code's
behaviour changes (`sportsdataverse/nfl/model_vars.py` `OVERTIME_FORMATS`; ids from sdv-internal-refs
`rules/nfl.yaml`); `tools/check_ot_era_captures.py` checks them. All are ESPN summaries from
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`, trimmed
(`summary_<id>_trimmed.json.gz`).

| rule set | rule id | capture | game |
|---|---|---|---|
| <2012, sudden death | before `nfl-2012-overtime-modified-sudden-death-regular-season` | 260917021 | NYG @ PHI, 2006 week 2, 30-24 in overtime |
| 2012-24, modified sudden death | `nfl-2012-overtime-modified-sudden-death-regular-season` | 401547449 | LAR @ IND, 2023 week 4, 29-23 in overtime |
| 2025+, both teams possess | `nfl-2025-overtime-both-possess-regular-season` | 401772636 | ATL @ IND, 2025 week 10, 25-31 in overtime |

Other overtime captures: 271209010 (SD @ TEN, 2007 week 14), 401671835 (DEN @ CIN, 2024 week 17).
`nfl-2017-overtime-ten-minutes` changes only the period length, which no branch reads. The
quality-program ledger wrote the sets as <2017 / 2017-23 / 2024+; the catalogue dates the
regular-season both-possess rule 2025 (2024 still played the 2012 rule). The 2012 cut is guarded only at
season granularity: the nearest captures are 2007 and 2023, so moving the boundary anywhere in 2008-2022
changes no capture's set until a 2012-16 overtime capture is committed.
