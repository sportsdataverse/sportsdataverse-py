<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL microstat oracle corpus](#nhl-microstat-oracle-corpus)
  - [`pbp_2024_slice.parquet`](#pbp_2024_sliceparquet)
  - [`edge_skater_detail_sample.parquet`](#edge_skater_detail_sampleparquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NHL microstat oracle corpus

Committed offline validation corpus for the `sportsdataverse.nhl` microstat
value spine (T5.2). Consumed by `tests/nhl/test_nhl_microstat_oracle.py` and
the per-model unit tests. No network access required to run the oracle
suite once these files exist.

## `pbp_2024_slice.parquet`

- **Source:** `sportsdataverse.nhl.nhl_web_pbp` (`api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`),
  parsed via `parse_nhl_web_pbp`, plus `nhl_boxscore` for `home_team_id`.
- **Capture date:** 2026-07-08.
- **Season:** 2023-24 regular season (`season=2023`).
- **Games:** 40 consecutive regular-season games, `game_id` 2023020001-2023020040.
- **Row count:** 13,180 plays.
- **IDs:** `game_id`, `event_owner_team_id`, `home_team_id`, and every
  `*_player_id` column are `Utf8` (cast from the raw integer via
  `cast(Float64, strict=False).cast(Int64, strict=False).cast(Utf8)` --
  never a float->Utf8 cast).
- **Column contract:** see `dev/nhl_microstat/capture_corpus.py::PBP_CONTRACT`.
  Real api-web field names (`details.zoneCode`, `details.winningPlayerId`,
  etc.) are renamed to the contract names via `PBP_RENAME` in that script.
- **Regenerate:** `SDV_PY_LIVE_TESTS=1 uv run python dev/nhl_microstat/capture_corpus.py`.

## `edge_skater_detail_sample.parquet`

- **Source:** `nhl_edge_skater_skating_speed_detail` / `_skating_distance_detail`
  / `_zone_time` (`api-web.nhle.com/v1/edge/*`), raw payload walked directly
  (the generic `parse_edge_detail`/`parse_edge_zone_time` parsers return
  nested/stringified blobs for these endpoints, not flat columns -- the
  capture script extracts `skatingSpeedDetails.maxSkatingSpeed.metric`,
  `.bursts20To22.value`, `skatingDistanceDetails[strengthCode="all"].distanceTotal.metric`,
  and the `zone_time` all-situations row's `offensiveZonePctg`/`defensiveZonePctg`/`neutralZonePctg`).
- **Capture date:** 2026-07-08.
- **Season:** 2023-24 (`season=2024` in the EDGE endpoint's end-year convention).
- **Skaters:** 108, seeded from the forward/defense rosters of 3 games
  (`game_id` 2023020001, 2023020010, 2023020020), deduped.
- **IDs:** `player_id` is `Utf8`.

Both files carry `Utf8` ids per the ID/join-key discipline in `CLAUDE.md` --
never re-derive a differently-typed id column from these fixtures.
