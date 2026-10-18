<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL test fixtures](#nfl-test-fixtures)
  - [ESPN game summaries (the 37 whole-game captures)](#espn-game-summaries-the-37-whole-game-captures)
  - [nflverse slices (`nflverse/`)](#nflverse-slices-nflverse)
  - [Fourth-down and offline-model fixtures (task 1.5)](#fourth-down-and-offline-model-fixtures-task-15)
    - [Booster replay (`nfl4th_booster_replay.parquet`)](#booster-replay-nfl4th_booster_replayparquet)
  - [ESPN Total QBR slice (`espn_qbr_2023_slice.parquet`, task 2.7)](#espn-total-qbr-slice-espn_qbr_2023_sliceparquet-task-27)
  - [Other files](#other-files)
  - [Overtime rule sets (C-M6)](#overtime-rule-sets-c-m6)
  - [Games that end on a try (`try_endings/`, task 2.2)](#games-that-end-on-a-try-try_endings-task-22)
  - [Touchdown-flag residuals (`td_flag_residuals/`, task 2.20)](#touchdown-flag-residuals-td_flag_residuals-task-220)
  - [Era play order (`era_play_order/`, task 2.19)](#era-play-order-era_play_order-task-219)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL test fixtures

## ESPN game summaries (the 37 whole-game captures)

Each `summary_<game_id>*` file is the ESPN Site v2 summary for one NFL game,
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`. The
`*_trimmed.json.gz` files come from the nfl-raw library (`sportsdataverse/nfl-raw`,
`nfl/espn/raw/<season>/<game_id>.json.gz`) trimmed to the keys `NFLPlayProcess` reads (header,
drives, boxscore, gameInfo, pickcenter, scoringPlays where present); `summary_401872922.json` is a
full summary fetched from the URL above. "Committed" is the commit and date that added the file to this
repository; the ESPN fetch date is not recorded with the fixtures, except for the six EX-1.5-d
captures (400554340, 401030962, 401326467 at 2026-10-11 16:53 UTC; 401326132 at 20:00 and 401326383 at
21:24 UTC, the review R785-2 half-end rows; 401326581 at 23:17 UTC, the R785-9 lateral fumble): fetched from the URL above, one request each (`curl -s -o summary_<id>_live.json "<url>"`), and trimmed to the same keys with
`json.dump({k: d[k] for k in ("header", "drives", "boxscore", "gameInfo", "pickcenter", "scoringPlays") if k in d}, gzip_fh, separators=(",", ":"))`.
Every play's `end` object in those fetches is identical to the nfl-raw copy, so ESPN still serves
the 2014-2021 end-state corruption they pin.
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
| 400554340 | 2014 | 11 | MIN @ CHI | `summary_400554340_trimmed.json.gz` | task 2.18, 2026-10-11 |
| 400791508 | 2015 | 16 | WSH @ PHI | `summary_400791508_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 400791567 | 2015 | 7 | PHI @ CAR | `summary_400791567_trimmed.json.gz` | 9310e773f9, 2026-09-17 |
| 400791590 | 2015 | 1 | NYG @ DAL | `summary_400791590_trimmed.json.gz` | e9c3ca7c48, 2026-09-18 |
| 400874485 | 2016 | 16 | NYG @ PHI | `summary_400874485_trimmed.json.gz` | 7591ec61f5, 2026-09-17 |
| 400874512 | 2016 | 2 | BAL @ CLE | `summary_400874512_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 400951676 | 2017 | 2 | SF @ SEA | `summary_400951676_trimmed.json.gz` | d28cc51212, 2026-09-18 |
| 401030824 | 2018 | 17 | LAC @ DEN | `summary_401030824_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 401030962 | 2018 | 13 | IND @ JAX | `summary_401030962_trimmed.json.gz` | task 2.18, 2026-10-11 |
| 401128075 | 2019 | 11 | JAX @ IND | `summary_401128075_trimmed.json.gz` | 45567d8bf9, 2026-09-25 |
| 401220341 | 2020 | 10 | BUF @ ARI | `summary_401220341_trimmed.json.gz` | 9310e773f9, 2026-09-17 |
| 401326132 | 2021 | 2 | DEN @ JAX | `summary_401326132_trimmed.json.gz` | task 2.18, 2026-10-11 |
| 401326383 | 2021 | 4 | CAR @ DAL | `summary_401326383_trimmed.json.gz` | task 2.18, 2026-10-11 |
| 401326467 | 2021 | 10 | JAX @ IND | `summary_401326467_trimmed.json.gz` | task 2.18, 2026-10-11 |
| 401326581 | 2021 | 17 | LAR @ BAL | `summary_401326581_trimmed.json.gz` | task 2.18, 2026-10-11 |
| 401547449 | 2023 | 4 | LAR @ IND | `summary_401547449_trimmed.json.gz` | c921519970, 2026-09-19 |
| 401671740 | 2024 | 4 | PHI @ TB | `summary_401671740_trimmed.json.gz` | a43428ecf9, 2026-09-25 |
| 401671835 | 2024 | 17 | DEN @ CIN | `summary_401671835_trimmed.json.gz` | 9310e773f9, 2026-09-17 |
| 401772636 | 2025 | 10 | ATL @ IND | `summary_401772636_trimmed.json.gz` | 9dc97e9872, 2026-09-18 |
| 401772726 | 2025 | 2 | CLE @ BAL | `summary_401772726_trimmed.json.gz` | 9dc97e9872, 2026-09-18 |
| 401772748 | 2025 | 6 | CLE @ PIT | `summary_401772748_trimmed.json.gz` | 9dc97e9872, 2026-09-18 |
| 401872922 | 2026 | 1 | CLE @ JAX | `summary_401872922.json` | 741c871a31, 2026-09-14 |

## nflverse slices (`nflverse/`)

Five games of nflverse's play-by-play, the oracle for the series, fixed-drive, down / distance /
yard line, yardage, score, touchdown, EP / EPA / WP and CP columns of the same games' ESPN
captures. ESPN's play id is the game id followed by nflverse's GSIS `play_id`, so the two join
exactly.

| file | nflverse game_id | ESPN capture | rows x cols |
|---|---|---|---|
| `nflverse/2023_04_LA_IND.parquet` | 2023_04_LA_IND (old_game_id 2023100106) | 401547449 | 190 x 41 |
| `nflverse/2024_04_PHI_TB.parquet` | 2024_04_PHI_TB (old_game_id 2024092907) | 401671740 | 173 x 41 |
| `nflverse/2014_11_MIN_CHI.parquet` | 2014_11_MIN_CHI | 400554340 | 161 x 42 |
| `nflverse/2018_13_IND_JAX.parquet` | 2018_13_IND_JAX | 401030962 | 170 x 42 |
| `nflverse/2021_10_JAX_IND.parquet` | 2021_10_JAX_IND | 401326467 | 180 x 42 |

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

- **The three EX-1.5-d slices** (task 2.18, read by the `test_EX_1_5_d_*` tests) come the same way from
  the same release's 2014, 2018 and 2021 assets as downloaded to `nfl-data/python/data/` on 2026-06-18:
  `pbp_2014.parquet` (sha256 `db5f70b59bc6e3cd358b4ff3691738c4fab2f3023a2b792ee8f817f9a326c4a3`),
  `pbp_2018.parquet` (`c9477ef3065e906f5de0e8489f82f4673fd317c8cf84ab3064388027c78f95be`) and
  `pbp_2021.parquet` (`aa8dec05bb188140f08ad56345395a1708e9012c102f90988c76e149e9b9d074`), trimmed
  2026-10-11 with the command above plus a 42nd column, `first_down`, appended to the select list.
- **`nflverse/end_down_minus_one_epa.csv`** (task 2.3, read by `test_end_down_minus_one_epa_agrees_with_nflverse`):
  nflverse's `epa` on 45 plays of the committed captures, columns `espn_game_id, play_id,
  nflverse_game_id, play_type, epa`: the 21 kickoff returns ESPN ends at down -1 (2015-2024) and every
  unblocked missed field goal (8 from 2005-2013, 16 from 2014-2025; the test reads the 2014+ ones).
  Downloaded 2026-10-12 from the same release's `play_by_play_<season>.parquet` assets (2005-2025;
  the 2014+ field goals from the nfl-data cache of those assets,
  `nflverse-dev/nfl-data/python/.cache/nflverse_pbp/pbp_<season>.parquet`); the ESPN game id is mapped to nflverse's
  `game_id` by the `espn` column of `https://github.com/nflverse/nfldata/raw/master/data/games.csv`, and
  the play by ESPN's play id (the game id followed by the GSIS `play_id`).
- **`nflverse/kickoff_touchback_epa.csv`** (task 2.21, EX-2.3-a, read by
  `test_kickoff_touchback_epa_agrees_with_nflverse`): nflverse's `epa` on 124 plays of the committed
  captures, columns `espn_game_id, play_id, nflverse_game_id, season, epa`: every play the processor
  flags `kickoff_tb` (unscored) that nflverse also has as `play_type == "kickoff"` with `touchback == 1`
  (2006-2025). The other 32 unscored 2006-2007 `kickoff_tb` rows are left out: ESPN describes those kicks only
  as "KickOff", the touchback pattern flags them, and nflverse has every one as a returned kick
  (`touchback == 0`), not a touchback (review R831-2, routed to task 2.19).
  Read 2026-10-12 from the nfl-data cache of the same release's `play_by_play_<season>.parquet`
  assets (`nflverse-dev/nfl-data/python/.cache/nflverse_pbp/pbp_<season>.parquet`), mapped and joined as
  the file above.

## Fourth-down and offline-model fixtures (task 1.5)

| file | rows x cols | read by |
|---|---|---|
| `nfl4th_2022_slice.parquet` | 4,239 x 25 | `tests/nfl/test_nfl_fourth_down.py::test_nfl4th_oracle_2022` (gap-C-125) |
| `nflverse/2022_01_BUF_LA_full.parquet` | 160 x 372 | `tests/nfl/test_models_offline.py` (C-M15) |

- **`nfl4th_2022_slice.parquet`: the oracle.** nfl4th's own published decision values for every 2022
  fourth down: release `nfl4th_infrastructure`, asset
  `https://github.com/nflverse/nfl4th/releases/download/nfl4th_infrastructure/pre_computed_go_boost.rds`
  (all seasons, 50,384 rows; cols `game_id, play_id, go_boost, go_wp, punt_wp, fg_wp`), downloaded
  2026-10-11 00:45 EDT by the live-local check (R 4.6.1, nfl4th 1.0.7; converted to parquet, sha256 prefix
  `8ed65f364ad744c6`), filtered to `game_id` starting `2022_` (4,239 rows) and renamed `nfl4th_*`.
- **Inputs.** The same rows of the nflverse-data release `pbp` asset
  `https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2022.parquet` (read with
  `sportsdataverse.nfl.load_nfl_pbp([2022])` on 2026-10-11), the 21 columns `get_4th_down_probs` reads:
  `game_id, play_id, season, week, season_type, posteam, defteam, home_team, away_team, roof, qtr,
  quarter_seconds_remaining, ydstogo, yardline_100, score_differential, posteam_timeouts_remaining,
  defteam_timeouts_remaining, home_opening_kickoff, spread_line, total_line, down`; inner join on
  (`game_id`, `play_id`) keeps all 4,239 rows.
- **`nflverse/2022_01_BUF_LA_full.parquet`.** Every column of that same release for one game
  (`game_id == "2022_01_BUF_LA"`, 160 plays, 8 fourth downs), so `enrich_nfl_pbp` runs end to end offline.
- Rebuilt with (sdv-py root, `LIVE` = the live-local check's directory holding the oracle files):

```python
import polars as pl
from sportsdataverse.nfl import load_nfl_pbp
cols = ["game_id","play_id","season","week","season_type","posteam","defteam","home_team","away_team","roof",
        "qtr","quarter_seconds_remaining","ydstogo","yardline_100","score_differential",
        "posteam_timeouts_remaining","defteam_timeouts_remaining","home_opening_kickoff","spread_line",
        "total_line","down"]
pbp = load_nfl_pbp([2022])
o = pl.read_parquet(f"{LIVE}/pre_computed_go_boost.parquet").filter(pl.col("game_id").str.starts_with("2022_"))
o = o.rename({c: f"nfl4th_{c}" for c in ("go_boost", "go_wp", "punt_wp", "fg_wp")})
(pbp.select(cols).with_columns(pl.col("play_id").cast(o.schema["play_id"]))
    .join(o, on=["game_id", "play_id"], how="inner").sort("game_id", "play_id")
    .write_parquet("tests/nfl/fixtures/nfl4th_2022_slice.parquet", compression="zstd"))
pbp.filter(pl.col("game_id") == "2022_01_BUF_LA").write_parquet(
    "tests/nfl/fixtures/nflverse/2022_01_BUF_LA_full.parquet", compression="zstd")
```

### Booster replay (`nfl4th_booster_replay.parquet`)

Read by `tests/nfl/test_nfl_fourth_down.py::test_nfl4th_oracle_replay_2022` through
`tests/nfl/_booster_replay.py`, so the nfl4th oracle runs in CI without the two download-on-demand boosters
(review PR-774 R774-1). It holds the raw outputs of `fd_model.ubj` (605 distinct feature rows, 76 class
probabilities each) and `wp_model.ubj` (60,291 rows, one probability each) for every feature row
`get_4th_down_probs` handed them on every 7th play of `nfl4th_2022_slice.parquet` (606 plays), keyed by the
row's exact float32 bytes. Columns: `model, key, x (list[f32]), pred (list[f32])`.

- **Boosters.** sportsdataverse-data release `nfl_4th_down_models` (sdv-py's xgboost conversions of
  nfl4th's models), as cached 2026-06-24: `fd_model.ubj` sha256
  `74020522809537e67a01e058215631c00fd4379d0c4e7db51386ccc5eec05f9d`, `wp_model.ubj` sha256
  `56444fc03114dccd9f01fae791408b8eac6636503614b5310359c1a3ae74da1f`. The local-only test
  `test_nfl4th_replay_was_recorded_from_the_cached_boosters_local_only` fails when the cached boosters
  change; re-record then.
- **Recorded** 2026-10-11 (xgboost 3.3.0, polars 2.0, Python 3.13) from the sdv-py root with
  `PYTHONPATH=. uv run --frozen --all-extras python tests/nfl/fixtures/record_nfl4th_replay.py`. That
  needs the boosters in the local model cache and never downloads them. The replayed scores equal the
  live scores on all 606 plays.

## ESPN Total QBR slice (`espn_qbr_2023_slice.parquet`, task 2.7)

The oracle for the NFL QBR feature order (`tests/nfl/test_qbr_oracle.py`, gap-C-083): 573 rows x 11
columns, one per quarterback-game ESPN rated in 2023 (regular season and playoffs).

- **ESPN Total QBR** (`qbr_total`, `season_type`, `player_id`, `passer`): nflverse's `espn_data` release
  asset `https://github.com/nflverse/nflverse-data/releases/download/espn_data/qbr_week_level.parquet`, as
  read by `load_nfl_espn_qbr([2023], summary_type="week")` on 2026-10-11.
- **Model inputs** (`qbr_epa`, `pass_epa`, `rush_epa`, `sack_epa`, `pen_epa`, `spread`): `NFLPlayProcess` run
  offline (devel-nfl a345c60771 plus the R795-1 empty-group fix, #802) over nfl-raw's 286 committed 2023
  ESPN summaries (`sportsdataverse/nfl-raw` @ 4f85a779, `nfl/espn/raw/2023/<game_id>.json.gz`, added in
  3ecf9147), read back from `advBoxScore["pass"]`. The inputs do not depend on the feature order; 401616889
  (the Pro Bowl Games) has no passing box and is skipped.
- **Join**: ESPN game id plus first initial + last name, case and punctuation folded ("B.Purdy" ==
  Brock Purdy, "C.Stroud" == C.J. Stroud); all 573 ESPN rows matched, one row each.
- **Recorded** 2026-10-11 with `PYTHONPATH=. uv run --frozen --all-extras python
  tests/nfl/fixtures/record_espn_qbr_2023_slice.py <nfl-raw>/nfl/espn/raw/2023` (about 5 minutes on 6
  processes; never loads a download-on-demand model). The first recording (#795, at ece03537a6) is
  superseded: merged PRs had since moved every EPA input.
- **Re-record** whenever a merged PR moves an `exp_qbr` input (the EPA, the weights, the QBR aggregate), and
  re-pin the floor in `test_qbr_oracle.py` to the measured value.

## Other files

- `plays_401872922.json.gz`, `box_401872925_espn_trimmed.json.gz`: see the tests that read them.
- `athlete_refs_401872922.json.gz`: the Core v2 `$ref` payloads (`sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2026/athletes/<id>`) of the 7 athletes in `plays_401872922.json.gz` that the summary's athlete lookup misses, keyed by `$ref` URL; one request each, 2026-10-11, whole payloads. Read by `tests/football/test_play_participants_resolve.py` (supp-007).
- `fox/`, `nfl_cbs/`, `yahoo_nfl/`: provider captures with their own READMEs where present.
- `kickoff_misfiled_401326517_trimmed.json.gz` (task 2.21, read by `test_kickoff_net_spot_is_the_receivers_next_snap`):
  the ESPN summary of 401326517 (2021 week 13, PHI @ NYJ) from the nfl-raw library
  (`nfl/espn/raw/2021/401326517.json.gz`, nfl-raw commit 5ca32021, 2026-09-14), trimmed to the same keys as the
  whole-game captures. It is NOT one of them (the name keeps it out of the `summary_*` glob): ESPN files the
  touchback kickoff 1442 between two PHI snaps of one drive, the case the kickoff net-spot possession check covers.
- `kickoff_onside_400554238_trimmed.json.gz` (task 2.21 / R810-3, read by
  `test_kickoff_kicking_team_recovery_ends_at_the_kickers_snap[onside]`): the ESPN summary of 400554238
  (2014 week 4, TEN @ IND; nflverse 2014_04_TEN_IND) from the nfl-raw library
  (`nfl/espn/raw/2014/400554238.json.gz`, nfl-raw commit f2d99262, 2026-09-14), trimmed the same way and
  likewise outside the `summary_*` glob. Play 568 is an onside kick IND recovers (nflverse epa -3.798).
- `kickoff_reversed_recovery_301114003_trimmed.json.gz` (task 2.21 / R810-3, read by
  `test_kickoff_reversed_recovery_stays_the_receivers_ball`): the ESPN summary of 301114003 (2010 week 10,
  MIN @ CHI; nflverse 2010_10_MIN_CHI) from the nfl-raw library (`nfl/espn/raw/2010/301114003.json.gz`,
  nfl-raw commit 56da67ca, 2026-09-14), trimmed the same way. Play 3734 is a kickoff whose fumble recovery by
  the kicking team was REVERSED (nflverse epa 0.399).

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

## Games that end on a try (`try_endings/`, task 2.2)

Two untrimmed ESPN Site v2 summaries whose last real play is a scored try, copied from
`nflverse-dev/nfl-raw` `nfl/espn/raw/<season>/<game_id>.json.gz` (nfl-raw commit 4f540c18, 2026-09-14;
source `https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`). They sit
in a subdirectory so the whole-game captures above (`summary_*` in this directory) do not pick them up.

| game | last play | why |
|---|---|---|
| 291122008 | `Extra Point Good` ("J.Hanson extra point is GOOD") | R832-3: the try that ends the game keeps its own EPA (nflfastR `sp == 1`) |
| 271021004 | `Two Point Pass` (scoringPlay True) | the same on a two-point try |

## Touchdown-flag residuals (`td_flag_residuals/`, task 2.20)

Five untrimmed ESPN Site v2 summaries, copied from `nflverse-dev/nfl-raw`
`nfl/espn/raw/<season>/<game_id>.json.gz` (nfl-raw commits 044caca9 and, for 271209034 / 291129016, 4f540c18, 2026-09-14; source
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`), each holding a row
the task 2.5 touchdown rule got wrong (review PR-808, R808-2/3/4) or one task 2.20 must keep (review PR-840, R840-1/2). The oracle is nflverse
(`nflverse-dev/nfl-data` `python/data/pbp_<season>.parquet`, joined on `game_id` + `play_id`).

| game (nflverse) | play | ESPN type / scoringPlay | nflverse `touchdown` / `rush_touchdown` / `epa` | finding |
|---|---|---|---|---|
| 311218002 (`2011_15_MIA_BUF`) | 3112180024175, R.Bush 76-yard run, "TOUCHDOWN NULLIFIED by Penalty" (excessive celebration, enforced on the kickoff) | `Rushing Touchdown` / True; next row the try, a scored `Extra Point Good` | 1 / 1 / 6.450 | R808-2 |
| 291126008 (`2009_12_GB_DET`) | 2911260083341, fumble-return TD reversed on challenge into a safety | `Safety` / True | 0 / 0 / -0.438 (`safety` 1) | R808-3 |
| 401671715 (`2024_12_DAL_WAS`) | 4016717152262 (short of landing zone), 4016717154728 (onside kick) | `Kickoff Return Touchdown` / False | 0 / 0 / 0.926, 1.419 | R808-4 |
| 291129016 (`2009_12_CHI_MIN`) | 2911290162862, C.Taylor 20-yard run, "TOUCHDOWN NULLIFIED by Penalty" (offensive holding); ESPN's score reads 27 the row before and 24 on it | `Rushing Touchdown` / True; next row a `Penalty`, no try | 0 / 0 / 0.474 | R840-2 |
| 271209034 (`2007_14_TB_HOU`) | 2712090341856, A.Davis 97-yard kickoff return TD; ESPN's 2007 text is the bare "Kickoff" | `Kickoff Return Touchdown` / False | 1 / 0 / 6.057 (`return_touchdown` 1) | R840-1 (the 2009 floor) |

## Era play order (`era_play_order/`, task 2.19)

Eight ESPN Site v2 summaries from `nflverse-dev/nfl-raw` `nfl/espn/raw/<season>/<game_id>.json.gz` (source
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`), trimmed to the same keys as
the whole-game captures, read by `tests/nfl/test_nfl_era_play_order.py`. The oracle is nflverse
(`nflverse-dev/nfl-data` `python/.cache/nflverse_pbp/pbp_<season>.parquet`, joined on `game_id` + `play_id`).

| game (nflverse) | nfl-raw commit | play | ESPN | nflverse | finding |
|---|---|---|---|---|---|
| 260910005 (`2006_01_NO_CLE`) | 704bd9ad | 2797 / 2819 / 2843 (Q3, 2:00-1:11) | period 4 / 3 / 4 | `qtr` 3 / 3 / 3 | R832-10 (lower only) |
| 270930001 (`2007_04_HOU_ATL`) | a62f3dfb | 1000, punt at 14:37, next row Q1's late "End Period" | period 2 | `qtr` 2 | R832-10 (End-row guard) |
| 271111013 (`2007_10_CHI_OAK`) | 4f540c18 | 1856, Q3 opening kickoff after "End of Half", before a Q2 timeout | period 3 | `qtr` 3 | R832-10 |
| 281005009 (`2008_05_ATL_GB`) | 4b1d90b6 | 1872, Q3 opening kickoff between two Q2 timeouts | period 3 | `qtr` 3 | R832-10 (clock check) |
| 401030749 (`2018_08_IND_OAK`) | d0480d17 | 906, a Q2 snap whose id sorts among Q1 rows | period 2 | `qtr` 2 | R832-10 (season floor) |
| 301107009 (`2010_09_DAL_GB`) | 56da67ca | 1557, GB recovers DAL's kickoff fumble and scores | `Kickoff Return Touchdown` | `epa` -7.672 | R831-6 |
| 281109021 (`2008_10_NYG_PHI`) | 4b1d90b6 | 3307, D.Hixon's 73-yard punt return "TOUCHDOWN NULLIFIED by Penalty" (holding) | `Punt Return Touchdown`, `td_play` false | `touchdown` 0, `epa` -1.690 | R856-1 (`def_score` needs the TD to stand) |
| 271223008 (`2007_16_KC_DET`) | 4f540c18 | 1912, KC's bare "KickOff" at 0:23 of Q2, a touchback; ESPN's end clock (1:08) is a timeout row filed after it | `Kickoff`, start 0:23 / end 1:08 | `touchback` 1, `epa` 0.000 | R856-2 (owner ruling: EP_end at the start clock) |
