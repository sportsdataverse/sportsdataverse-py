<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Basketball crosswalk golden fixtures](#basketball-crosswalk-golden-fixtures)
  - [Known defect in the MBB team golden: all-null bart columns](#known-defect-in-the-mbb-team-golden-all-null-bart-columns)
  - [Re-capturing](#re-capturing)
  - [Fox game and Yahoo id captures](#fox-game-and-yahoo-id-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Basketball crosswalk golden fixtures

These are the **R producers' committed outputs**, copied unmodified. They are
the oracle for `tests/test_basketball_crosswalk_parity.py`, which asserts the
ported Python assemblers reproduce them row-for-row (design section 9.3: "a
crosswalk that is 97% right is a silent data defect, not a passing port").

| Fixture | Source repo | Path in that repo | Rows |
|---|---|---|---:|
| `mbb_team_crosswalk_2026.parquet` | `hoopR-mbb-data` | `mbb/crosswalk/parquet/` | 362 |
| `mbb_player_crosswalk_2026.parquet` | `hoopR-mbb-data` | `mbb/crosswalk/parquet/` | 5,442 |
| `nba_team_crosswalk_2026.parquet` | `hoopR-nba-data` | `nba/crosswalk/parquet/` | 30 |
| `nba_player_crosswalk_2026.parquet` | `hoopR-nba-data` | `nba/crosswalk/parquet/` | 544 |
| `wbb_team_crosswalk_2026.parquet` | `wehoop-wbb-data` | `wbb/crosswalk/parquet/` | 361 |
| `wbb_schedule_crosswalk_2026.parquet` | `wehoop-wbb-data` | `wbb/crosswalk/parquet/` | 6,521 |
| `wbb_player_crosswalk_2026.parquet` | `wehoop-wbb-data` | `wbb/crosswalk/parquet/` | 5,018 |
| `wnba_team_crosswalk_2026.parquet` | `wehoop-wnba-data` | `wnba/crosswalk/parquet/` | 15 |
| `wnba_schedule_crosswalk_2026.parquet` | `wehoop-wnba-data` | `wnba/crosswalk/parquet/` | 355 |
| `wnba_player_crosswalk_2026.parquet` | `wehoop-wnba-data` | `wnba/crosswalk/parquet/` | 203 |

Produced by `hoopR::{mbb,nba}_*_crosswalk()` / `wehoop::{wbb,wnba}_*_crosswalk()`
via each repo's `<lg>_1{1,2,3}_*_crosswalk_creation.R` stage, 2026 season.

`hoopR-nba-data` also carries an **untracked, in-progress**
`nba_schedule_crosswalk` CSV/parquet; it is deliberately excluded — only
committed R output is used as an oracle.

Note the `<lg>/crosswalk/*.csv` files in those repos are per-season **manifests**
(`season,row_count,generated_at_utc,source_endpoint`), not the crosswalk data.
The parquets above are the actual outputs.

## Known defect in the MBB team golden: all-null bart columns

Every one of its 362 rows has `bart_team` / `bart_conf` /
`bart_match_confidence` **null**, and `match_method` reads `fox+kp` (359) /
`fox_only` (2) / `espn_only` (1). That is **not** intended behaviour and not a
statement about Torvik coverage — the R builder wraps its Torvik fetch in
`tryCatch(torvik_ratings(year = season), error = function(e) NULL)`
(`hoopR/R/mbb_crosswalk.R:346-347`), and that fetch failed on the day the R
producer ran. The golden froze a transient upstream outage.

sdv-py deliberately **diverges**: `mbb_team_crosswalk()` has no such swallow (a
failed Torvik fetch raises `CrosswalkSourceError`), so it joins Torvik and
reports `fox+bart+kp` for the same 359 rows. Do not "fix" Python to reproduce
the nulls. The `bart_*` columns and `match_method` must be **positively
asserted** — populated counts and `match_method` composition — before the
column-by-column exactness diff excludes them; excluding them from that diff is
correct, since the golden is known-wrong there, but excluding them *and
asserting nothing about them* silently permits a regression back to all-null.

`mbb_torvik_teams_2026.parquet` exists to make that assertable offline:

| Fixture | Source | Captured | Rows |
|---|---|---|---:|
| `mbb_torvik_teams_2026.parquet` | `sportsdataverse.mbb.torvik.torvik_ratings(year=2026)`, `team` + `conf` only | 2026-08-12 (UTC) | 365 |

`test_mbb_team_crosswalk_joins_torvik_where_the_golden_froze_an_outage` feeds it
to the assembler alongside the bundled KenPom directory and pins both sides of
the divergence: the golden's 0 populated `bart_team`, and Python's 359. The
three ESPN teams Torvik's directory does not carry (LSU New Orleans,
St. Thomas, West Florida) are named in that test. Note the sibling test
`test_mbb_team_crosswalk_bundled_kenpom_reproduces_the_golden` passes Torvik in
**empty on purpose** — it reproduces the golden as frozen, defect included, and
is not the authority on `bart_*`.

This is the general hazard with a committed golden: it measures calendar time,
not correctness. Date every capture.

## Re-capturing

Copy the file again from the sibling checkout; do not edit in place. If the R
producer's schema changes, update the parity test in the same commit and say
which columns moved.

`mbb_torvik_teams_2026.parquet` is not an R output — re-capture it by running
this **from the repository root**, so it overwrites the committed fixture
instead of dropping a stray file wherever the shell happened to be:

```python
import polars as pl
from sportsdataverse.mbb.torvik import torvik_ratings

torvik_ratings(year=2026).select(
    pl.col("team").cast(pl.Utf8), pl.col("conf").cast(pl.Utf8)
).write_parquet("tests/fixtures/crosswalk_basketball/mbb_torvik_teams_2026.parquet")
```

Update the capture date in the table above and re-measure `BART_JOINED` in the
parity test; never lower it to make a failing run pass.

## Fox game and Yahoo id captures

Real responses for `tests/test_crosswalk_basketball_fox_yahoo.py`, captured
2026-10-11 (UTC). Thanksgiving 2025 is used because 19 of its 20 Division I
games were at neutral sites (Fox and Yahoo both call the other team home for
nine of them) and three late West-coast tips sit on 2025-11-26 in ESPN's
schedule (23:59 ET) but on 2025-11-27 in Fox's (00:00 ET).

| Fixture | Source | Trim |
|---|---|---|
| `fox_cbk_scores_segment_20251127.json` | `bifrost/v1/cbk/league/scores-segment/20251127?groupId=2` | image, sort and favorite keys dropped (23 games kept) |
| `fox_nba_scores_segment_20260302.json` | `bifrost/v1/nba/league/scores-segment/20260302` | same keys dropped (4 games) |
| `yahoo_ncaab_league_teams_division1.json` | Yahoo shangrila `leagueTeams?league=ncaab&divisionIds=1&getTeamsByDivision=true` | 51 of 365 teams (the slate's, eight name-tier cases, and Queens, which no ESPN team takes); logo, color, alias and standings keys dropped |
| `yahoo_ncaab_game_ids_20251126_20251127.json` | Yahoo shangrila `leagueGameIdsByDate?leagues=ncaab&startRange=2025-11-26&endRange=2025-11-27&season=2025` | unmodified (92 games) |
| `espn_mbb_schedule_20251127.parquet` | `hoopR-mbb-data` `mbb_schedule_2026.parquet` (via `load_mbb_schedule`), the 2025-11-27 games plus the 2025-11-26 games tipping after 04:00 UTC | 8 columns |
| `espn_mbb_teams_20251127.parquet` | `espn_team_directory("mbb")` | the slate's 42 Division I teams, the ESPN side of the eight name-tier cases, and Memphis (absent from the trimmed Yahoo listing, so two ESPN "Tigers" share a nickname); 4 columns |
| `espn_nba_schedule_20260302.parquet` | `hoopR-nba-data` `nba_schedule_2026.parquet`, 2026-03-02 | 8 columns |
