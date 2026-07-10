<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL player-impact validation corpus](#nhl-player-impact-validation-corpus)
  - [Contents](#contents)
  - [Games captured](#games-captured)
  - [Team full-name <-> abbreviation crosswalk](#team-full-name---abbreviation-crosswalk)
  - [Data-blocked oracle fixtures (EvolvingHockey RAPM/WAR, MoneyPuck GSAx)](#data-blocked-oracle-fixtures-evolvinghockey-rapmwar-moneypuck-gsax)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NHL player-impact validation corpus

Captured 2026-07-08 via `dev/nhl_player_impact/capture_corpus.py` (gitignored scratch
script; run with `SDV_PY_LIVE_TESTS=1`).

## Contents

| File | Source wrapper | Rows | Notes |
|---|---|---|---|
| `pbp_sample.parquet` | `load_nhl_pbp_full(seasons=2025)` | 1052 | 3 games, 2024-25 season (`season=2025` key). `game_id`/`event_id`/`event_idx`/`season` cast to `Int64`. |
| `shifts_sample.parquet` | `load_nhl_shifts(seasons=2025)` | 1469 | Same 3 games. `game_id`/`season` cast to `Int64`. `ids_on`/`ids_off` are comma-**space**-joined player-id strings (e.g. `"8477979, 8478043"`) -- split on `", "`, not `","`. |
| `goalie_box_sample.parquet` | `load_nhl_goalie_box(seasons=2025)` | 12 | Same 3 games. `game_id`/`player_id`/`season` cast to `Int64`. |
| `xg_models/xg_model_5v5.json`, `xg_model_st.json`, `xg_model_meta.json` | `nhl_xg_models` GitHub release (`sportsdataverse/sportsdataverse-data`) | -- | Published, already-trained fastRhockey boosters (Apache-2.0 lineage; see `THIRD_PARTY_NOTICES`). Copied local so the offline test suite never downloads. |
| `eh_skaters.parquet` | *(data-blocked, see below)* | 0 | Documented zero-row stub: `player_id:Int64, player:Utf8, xg_rapm:Float64, war:Float64`. |
| `mp_gsax.parquet` | *(data-blocked, see below)* | 0 | Documented zero-row stub: `player_id:Int64, goalie:Utf8, gsax:Float64`. |

## Games captured

`2024020001` (BUF @ NJD), `2024020002` (SEA @ STL), `2024020003` (NJD @ BUF) -- 2024-25
regular season. 274 unblocked shot events (SHOT/MISSED_SHOT/GOAL), 14 goals, across
5v5/5v4/4v5/5v6/6v5 strength states.

## Team full-name <-> abbreviation crosswalk

`load_nhl_pbp_full` keys team identity by abbreviation (`event_team_abbr`, `home_abbr`,
`away_abbr`); `load_nhl_shifts` keys team identity by full display name (`event_team`,
e.g. `"Buffalo Sabres"`). The stint builder bridges this via the static
`NHL_TEAM_FULLNAME_TO_ABBR` table in `nhl_player_impact_constants.py` (same pattern as
`nfl/datasets.py::team_abbr_mapping` -- a static, non-network-fetched data table, not a
hardcoded algorithm constant).

## Data-blocked oracle fixtures (EvolvingHockey RAPM/WAR, MoneyPuck GSAx)

Per the design spec's §8 open item ("if a source is paywalled/blocked, fall back to
internal calibration + a smaller manually-entered sample and document the provenance"):

- **MoneyPuck** (`moneypuck.com/moneypuck/playerData/seasonSummary/...`) actively blocks
  programmatic scraping without a paid data-license agreement -- a direct `curl` attempt
  on 2026-07-08 returned HTTP 200 with a Cloudflare-challenge "Data License" notice page
  instead of CSV data (bandwidth-cost gate, confirmed non-bypassable from this box
  without purchasing a license).
- **EvolvingHockey** (`evolving-hockey.com`) RAPM/GAR/WAR leaderboards are behind a paid
  subscription; no free/public per-player season export is capturable.

Per the "never fake data" rule (data-blocked models ship as documented zero-row frames,
not fabricated numbers), `eh_skaters.parquet` and `mp_gsax.parquet` are committed as
**zero-row, schema-only stubs**. The skater-RAPM (Phase 3), GAR/WAR (Phase 6), and GSAx
(Phase 2) oracle tests therefore run their **internal** construction-invariant gates
(league Σ`gsax`≈0, off/def coefficient centering, monotone calibration, synthetic
ordering-recovery) unconditionally, and skip the **external** concurrent-validity
assertion when the oracle fixture is empty (documented in each test with a
`pytest.skip` + a comment pointing back here) rather than asserting against invented
numbers. Capture contract for a future refresh: obtain either (a) a MoneyPuck data
license and re-run `capture_corpus.py`'s (not-yet-written) MoneyPuck section, or (b) an
EvolvingHockey subscription export, matching the schema documented in the table above.
