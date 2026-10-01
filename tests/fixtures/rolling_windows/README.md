<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [rolling_windows fixtures](#rolling_windows-fixtures)
  - [Source](#source)
  - [Selection rule](#selection-rule)
  - [Expected counts](#expected-counts)
  - [Known data quirk: ESPN "TEAM" sentinel plays](#known-data-quirk-espn-team-sentinel-plays)
  - [Stephen Curry shots (`nba_stats_shots_201939_2024_2025.parquet`)](#stephen-curry-shots-nba_stats_shots_201939_2024_2025parquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# rolling_windows fixtures

Real released CFB play-by-play, sliced to the 36 games (2021-2024) in which
Kyle McCord (ESPN athlete id `4433971`; Ohio State 2021-23, Syracuse 2024)
threw a pass.

## Source

- `cfbfastR-cfb-data` (published CFB pbp + schedules), HEAD
  `6e0391dc30192acb8c60de8da6c8247a42d9ad49` at capture time.
- `cfb/pbp/parquet/play_by_play_{2021,2022,2023,2024}.parquet`
- `cfb/cfb_schedules/parquet/cfb_schedules_{2021,2022,2023,2024}.parquet`

## Selection rule

1. Read the four pbp season files, projected to `FOOTBALL_PBP_COLUMNS`
   (see `sportsdataverse/rolling_windows.py`) plus the columns
   `metric_curves.football_attempts` reads (`FOOTBALL_ATTEMPT_COLUMNS` in
   `sportsdataverse/metric_curves.py`: `yds_fg, fg_attempt, fg_made,
   fg_kicker_player_id, fg_kicker_player_name, air_yards, completion,
   pass_attempt, distance, first_down_created, touchdown, penalty_no_play,
   scrimmage_play, defense_score_play`) and ESPN's raw `start.down, start.distance`
   (so `football_attempts` is exercised on a frame that carries both the raw and
   the repaired down/distance, as `load_cfb_pbp` does), concatenated
   `how="diagonal_relaxed"`.
   The blobs are read at the pinned commit (`git show <sha>:<path>`), so the
   2026-10-01 regeneration that added the metric_curves columns changed no
   row: the rolling_windows hand counts below still hold.
2. `games` = the unique `game_id`s where `passer_player_id == 4433971`.
3. `cfb_pbp_4433971_2021_2024.parquet` = every play (any player) from those
   games — not just McCord's own plays — so team-level (`play` unit) and
   receiver/rusher-side events reflect the real game population.
4. `cfb_schedule_4433971_2021_2024.parquet` = the matching `game_id` +
   `start_date` rows, deduped on `game_id`.

## Expected counts

- `pbp.height` = 6048 (35 columns)
- `pbp["game_id"].n_unique()` = 36
- `sched.height` = 36
- metric_curves populations: 102 field-goal attempts (74 made; one at 19
  yards, four at exactly 45), 4,704 standing scrimmage plays on downs 1-4 with
  a distance, 126 fourth-down rushes/passes that stood (70 converted), and no
  `air_yards` at all (CFB air yards are 2025+ only). `start.down` /
  `start.distance` equal `down` / `distance` on every standing play (measured on
  all five released seasons at the pinned commit and at HEAD `38a878fd`).

Reproduced exactly against the source above.

## Known data quirk: ESPN "TEAM" sentinel plays

Within the population `football_events` selects (`EPA_scrimmage` not null, `down`
1-4, regular season/postseason), 20 distinct plays carry a synthetic negative
`passer_player_id`/`rusher_player_id` with `passer_player_name`/`rusher_player_name
== "TEAM"` (16 dropback-flagged, 4 carry-flagged) -- ESPN's play text couldn't be
attributed to an individual (mostly kneel-type snaps), and `cfb_pbp.py` stamps
that participant `"TEAM"` (see `sportsdataverse/cfb/cfb_pbp.py:5330`).
`football_events` excludes these from player units (`dropback`/`target`/`carry`)
since `"TEAM"` isn't a real player and its sentinel id would poison a player's
rolling window; the team-level `play` unit is unaffected.

## Stephen Curry shots (`nba_stats_shots_201939_2024_2025.parquet`)

Every stats.nba shot row for Stephen Curry (`person_id` 201939) in END-year
seasons 2024 and 2025, verbatim (all 19 columns, every season type).

- Source: `hoopR-nba-stats-data` HEAD `3bce10e581bb523ba8454a7b096c1e6d163a7588`,
  `nba_stats/shots/parquet/shots_{2024,2025}.parquet` -- byte-identical in
  row count and season-type split to the published
  `nba_stats_shots/shots_{2024,2025}.parquet` assets (checked 2026-10-01).
- Selection: `pl.concat([pl.read_parquet(f"shots_{y}.parquet").filter(pl.col("person_id") == 201939) for y in (2024, 2025)])`.
- Expected counts: 2871 rows (1461 + 1410); 2833 in the regular season
  (`season_type_id == "2"`, 1445 + 1258) plus playoffs (`"4"`, 0 + 130) --
  the population `metric_curves.shot_attempts` keeps; the other 38 are
  play-in games (`"5"`). 1760 three-point attempts.
