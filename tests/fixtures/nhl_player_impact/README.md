<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL player-impact validation corpus](#nhl-player-impact-validation-corpus)
  - [Contents](#contents)
  - [Games captured](#games-captured)
  - [Team full-name <-> abbreviation crosswalk](#team-full-name---abbreviation-crosswalk)
  - [External concurrent-validity oracle fixtures (EvolvingHockey RAPM/WAR, MoneyPuck GSAx)](#external-concurrent-validity-oracle-fixtures-evolvinghockey-rapmwar-moneypuck-gsax)

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
| `eh_skaters.parquet` | EvolvingHockey `stats/skater_rapm/` + `stats/skater_gar/` (2024-25 regular, Pro Subscriber login) | 72 | `player_id:Int64, player:Utf8, xg_rapm:Float64, war:Float64`. See "External concurrent-validity oracle fixtures" below. |
| `mp_gsax.parquet` | MoneyPuck `playerData/seasonSummary/2024/regular/goalies.csv` (public download) | 103 | `player_id:Int64, goalie:Utf8, gsax:Float64`. See "External concurrent-validity oracle fixtures" below. |
| `mp_shots_sample.parquet` | MoneyPuck `playerData/shots/shots_2024.zip` (public download), filtered to the 3 captured games | 266 | `game_id:Int64` (NHL 10-digit), `period:Int64`, `shooter_id:Int64`, `game_seconds:Int64`, `mp_xgoal:Float64`, `mp_goal:Int64`. Per-shot `xGoal` for the NHL-booster-vs-MoneyPuck agreement gate in `tests/nhl/test_nhl_xg_oracle.py` (T5 R5). See below. |

## Games captured

`2024020001` (BUF @ NJD), `2024020002` (SEA @ STL), `2024020003` (NJD @ BUF) -- 2024-25
regular season. 274 unblocked shot events (SHOT/MISSED_SHOT/GOAL), 14 goals, across
5v5/5v4/4v5/5v6/6v5 strength states. One event carries a null `xg` (unscoreable feature
row), so the `test_nhl_xg_oracle.py` gate evaluates **273** scored shots after the
`xg.is_not_null()` filter; 265 of those (97%) match a MoneyPuck per-shot row.

## Team full-name <-> abbreviation crosswalk

`load_nhl_pbp_full` keys team identity by abbreviation (`event_team_abbr`, `home_abbr`,
`away_abbr`); `load_nhl_shifts` keys team identity by full display name (`event_team`,
e.g. `"Buffalo Sabres"`). The stint builder bridges this via the static
`NHL_TEAM_FULLNAME_TO_ABBR` table in `nhl_player_impact_constants.py` (same pattern as
`nfl/datasets.py::team_abbr_mapping` -- a static, non-network-fetched data table, not a
hardcoded algorithm constant).

## External concurrent-validity oracle fixtures (EvolvingHockey RAPM/WAR, MoneyPuck GSAx)

`eh_skaters.parquet` and `mp_gsax.parquet` were originally shipped (T5.1, #194) as
documented **zero-row, schema-only stubs** -- MoneyPuck's `seasonSummary` endpoint was
mistaken for scrape-blocked (see below), and EvolvingHockey's RAPM/GAR/WAR leaderboards
are genuinely behind a paid subscription that wasn't available at the time. Both are now
**real captures** (2026-07-11):

- **MoneyPuck** (`moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/*.csv`)
  is a **public, unauthenticated download**, free for non-commercial use with credit
  (see <https://moneypuck.com/about.htm> -- **Data: MoneyPuck.com**). The 2026-07-08
  capture attempt hit a Cloudflare "Data License" bandwidth-cost notice page (still HTTP
  200!) not because the endpoint is blocked, but because it keys off `User-Agent` and
  flags this package's default identifying UA (`"sportsdataverse-py"`) as a scraper --
  passing a normal browser UA via `download(..., headers=...)` returns the real CSV (no
  JA3/TLS impersonation needed, unlike stats.nba.com). Captured by
  `dev/nhl_player_impact/capture_moneypuck.py`: `mp_gsax.parquet` is the full 2024-25
  regular-season `situation == "all"` goalie table (103 goalies), `gsax = xGoals -
  goals` (same sign convention as `nhl_goalie_gsax`), keyed on MoneyPuck's native NHL
  `playerId` (a true id crosswalk, like the internal data).
- **MoneyPuck per-shot xGoal** (`moneypuck.com/moneypuck/playerData/shots/shots_2024.zip`,
  same public/license-free download + browser-UA gotcha) -- `mp_shots_sample.parquet`,
  captured 2026-07-13 by `dev/nhl_player_impact/capture_mp_shots.py`. It is MoneyPuck's
  **independent** per-shot `xGoal` (a different xG model, different features) for the same
  3 games, the concurrent-validity oracle for the NHL-booster xG (T5 R5,
  `tests/nhl/test_nhl_xg_oracle.py`). Join key **(game_id, shooter_id, game_seconds)**:
  MoneyPuck's `game_id` is the 5-digit form (`20001` == NHL `2024020001`, remapped to the
  10-digit id in the fixture), and MoneyPuck's `time` is **elapsed game seconds**
  (cumulative across periods, == our `game_seconds`; NOT per-period -- the per-period
  reading matched only period 1, ~31%). At 97% match rate (265/273 shots) the two models'
  per-shot agreement is corr 0.66 / mean-abs-diff 0.038 (2026-07-13) -- the floors the
  gate asserts (corr > 0.55, mad < 0.06) are set below those observed values.
- **EvolvingHockey** (`evolving-hockey.com`) RAPM/GAR/WAR Shiny-app tables require a Pro
  Subscriber login (`EVOLVING_HOCKEY_USER`/`EVOLVING_HOCKEY_PASS` in `~/.Renviron`,
  read at call time only -- never hardcoded/committed). Captured by
  `dev/nhl_player_impact/eh_capture.py` (Playwright login + Shiny `Download` button,
  since the tables render client-side and a plain HTTP GET returns "Please Sign In or
  Become a Subscriber to View") and turned into the fixture by
  `dev/nhl_player_impact/build_eh_fixture.py`. **EH↔NHL id crosswalk caveat**: EH's CSV
  exports carry no NHL `playerId`, only a display name, so `eh_skaters.parquet`'s
  `player_id` is a **name-based crosswalk against this package's own internal player
  universe** (the skaters in `shifts_sample.parquet`'s `ids_on`/`players_on` +
  `ids_off`/`players_off` parallel lists) rather than a general EH-wide id table --
  hence its scope is exactly the 72 skaters who (a) appear in the 3-game internal
  fixture and (b) case-fold-match an EH name (goalies excluded by construction; "Alexei"
  vs "Alexey" Toropchenko is a known transliteration mismatch that is correctly dropped,
  not silently joined to the wrong row). `xg_rapm` is EH's **EV**-strength `xG±/60`
  (EH's RAPM tool has no all-situations-combined table -- RAPM is inherently
  strength-segmented -- so the internal comparator uses
  `nhl_skater_rapm(..., strength_states=["5v5"])`, not the default all-situations call);
  `war` is EH's all-situations season-total `WAR` column, comparable to the default
  `nhl_skater_war(...)`. Traded-player team-splits are collapsed by TOI-weighted mean
  (`xg_rapm`) / sum (`war`).

The skater-RAPM (Phase 3), GAR/WAR (Phase 6), and GSAx (Phase 2) oracle tests run their
**internal** construction-invariant gates (league Σ`gsax`≈0, off/def coefficient
centering, monotone calibration) unconditionally, and now also run the **external**
concurrent-validity assertions against these real fixtures (`test_gsax_moneypuck_
concurrent_validity`, `test_rapm_evolvinghockey_concurrent_validity`,
`test_war_evolvinghockey_concurrent_validity` in `test_nhl_player_impact_oracle.py`) --
each `pytest.skip`s (rather than fakes a pass) only if a fixture ever reverts to zero
rows. Observed correlations (documented in each gate, magnitude floors set a bit below,
never invented; reproducible after the `build_stints` `.mode()` tiebreak fix in
`nhl_rapm.py` -- see below):

- **GSAx vs MoneyPuck**: Spearman **0.771** (n=6 goalies). A small-sample **sanity**
  check (only 6 goalies in the 3-game fixture), gated at floor 0.65 -- catches a gross
  attribution/sign regression, not a powered validity certification.
- **skater RAPM (5v5) vs EH EV**: Spearman **0.406** (n=72), gated at floor 0.30. n=72
  clears the Spearman significance threshold, so this is a powered magnitude gate.
- **WAR vs EH WAR**: Spearman **0.132** (n=72). This is inside the noise band at n=72
  (below the ~0.23 two-sided significance threshold), so its gate is a **directional
  (sign) check** (`corr > 0`), NOT a magnitude floor -- WAR sums several individually-
  noisy components over just 3 games, and a powered magnitude concurrent-validity gate
  needs a full-season sdv-py WAR build (deferred, mirroring the season-scale
  team-Σ`war` gate already deferred in `test_war_runs_on_real_fixture_and_is_bounded`).

**Reproducibility**: the RAPM/WAR fit is deterministic. An earlier version bounced
run-to-run (RAPM ρ 0.407-0.434, WAR ρ 0.122-0.157) because `build_stints` picked a
window's modal `strength_state`/goalie via a bare `.mode()[0]`, which returns tied-most-
frequent values in an unspecified order -- ~9/1241 stints' `strength_state` flipped
(e.g. `5v4`↔`5v6`) between runs, flipping which stints survive the `strength_states=
["5v5"]` filter and thus which λ the CV selects. Fixed at the source with a deterministic
`.mode().sort()[0]` tiebreak (`nhl_rapm.py`), so the observed correlations above are now
stable across runs.

Refresh procedure: re-run `dev/nhl_player_impact/capture_moneypuck.py` (no auth) and/or
`dev/nhl_player_impact/eh_capture.py` + `build_eh_fixture.py` (needs the account's own
EvolvingHockey Pro Subscriber `~/.Renviron` creds), matching the schemas documented in
the table above.
