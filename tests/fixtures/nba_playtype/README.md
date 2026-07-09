<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA play-type/impact oracle corpus (T3.5)](#nba-play-typeimpact-oracle-corpus-t35)
  - [Model (2) deferred external oracle](#model-2-deferred-external-oracle)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA play-type/impact oracle corpus (T3.5)

Committed offline validation corpus for the play-type/impact spine
(`nba_playtype`, `nba_matchup_drapm`, `nba_foul_drawing`, `nba_expected_turnovers`).
League: NBA (`league_id="00"`). Season: **2023-24** (`season="2023-24"`).
Captured live 2026-07-08 via `dev/nba_playtype/capture_oracle.py` +
`dev/nba_playtype/normalize_oracle.py` (both scratch, not committed) against
`stats.nba.com` through the `nba_stats` runtime (curl_cffi `impersonate="chrome"`,
`SDV_PY_NBA_STATS_LIVE=1`).

All ids (`team_id`, `player_id`, `off_player_id`, `def_player_id`) are `Int64`.

| File | Source wrapper | Params | Rows | Notes |
|---|---|---|---|---|
| `synergy_off_team_2024.parquet` | `nba_stats_synergyplaytypes` | `type_grouping_nullable="Offensive"`, `player_or_team_abbreviation="T"`, looped over the 11 `SYNERGY_PLAY_TYPES` | 330 (30 teams x 11 types) | Renamed `poss_pct`->`freq`, `tov_poss_pct`->`turnover_freq`, `ft_poss_pct`->`ft_freq` |
| `synergy_def_team_2024.parquet` | `nba_stats_synergyplaytypes` | same, `type_grouping_nullable="Defensive"` | 330 | same renames |
| `synergy_off_player_2024.parquet` | `nba_stats_synergyplaytypes` | same, `player_or_team_abbreviation="P"`, `type_grouping_nullable="Offensive"` | 3093 | player-level mix feeding models (3)/(4) |
| `matchups_2024.parquet` | `nba_stats_leagueseasonmatchups` | `per_mode_simple="Totals"` | 137763 | `matchup_min` parsed from raw `"MM:SS"` string to float minutes |
| `leaguedash_base_2024.parquet` | `nba_stats_leaguedashplayerstats` | `measure_type_detailed_defense="Base"` | 572 | `poss` column joined in from the `Advanced` measure (Base doesn't ship `poss`); `pfd` IS present on Base |
| `leaguedash_adv_2024.parquet` | `nba_stats_leaguedashplayerstats` | `measure_type_detailed_defense="Advanced"` | 572 | `pfd` joined in from Base (Advanced doesn't ship it) so both `pfd`-consuming call sites can read from either fixture |
| `gamelog_2024.parquet` | `nba_stats_leaguegamelog` | `player_or_team_abbreviation="T"` | 2460 (1230 games x 2 teams) | `opp_team_id` derived by self-joining on `game_id` and keeping the non-self row |
| `rapm_2024.parquet` | `nba_rapm` (shipped model, not a live wrapper) | fit over possessions from **126 games sampled across the whole season** (via `dev/nba_playtype/capture_more_rapm.py` direct + `capture_rapm_proxy.py` through the ProxyBonanza pool, both resumable per-game checkpoints) | 493 players, 24,869 possessions | **Sampled, NOT full-season** -- see the "Model (2) deferred external oracle" note below |

## Model (2) deferred external oracle

`rapm_2024.parquet` exists for the **deferred** stint-RAPM cross-validation of
`nba_matchup_drapm` (`test_matchup_drapm_vs_shipped_rapm_DEFERRED`, marked
`@pytest.mark.skip`). It is a 126-game / ~25k-possession sample (mean ~250
def-poss/player). At that volume stint `d_rapm` is still ridge-shrunk noise at
the player level, and the Spearman-vs-`matchup_drapm` trajectory is **flat**
(~-0.03 at both 107 and 126 games -- adding games did not move it). A valid
stint-RAPM oracle needs a **full-season (~1230-game / ~90k-possession)**
snapshot; capturing that game-by-game was infeasible here (stats.nba.com
throttles per-game pbp on the direct residential IP, and even through the
ProxyBonanza pool a subset of exit IPs hang). Separately, matchup DRAPM
(on-ball) and stint DRAPM (team/help defense) are only weakly correlated in
principle.

**Capture contract to close the deferral:** fetch a full-season possessions
snapshot (all ~1230 `0022300xxx` game ids from `gamelog_2024.parquet`) via the
resumable proxy checkpoint, refit `nba_rapm`, overwrite `rapm_2024.parquet`,
set `DRAPM_SPEARMAN_FLOOR` from the **observed** value (spec target >=0.3),
and remove the `@pytest.mark.skip`.

Model (2) is **not ungated** in the meantime: it ships gated on its INTERNAL
concurrent-validity (`test_matchup_drapm_internal_concurrent_validity`) --
`matchup_drapm` rank-agrees with each defender's raw points-allowed-per-100 at
Spearman **-0.733** (floor -0.5, n=251), plus ridge-centering (mean~0),
magnitude sanity (max ~11.6, guards the fixed double-scale bug), non-trivial
offense-FE adjustment (|rho|<1), and the synthetic dominant-defender recovery
unit test.

Endpoint param names pinned by live capture (Task 0.1 Step 1):
`nba_stats_synergyplaytypes(league_id, season, play_type_nullable,
player_or_team_abbreviation, type_grouping_nullable)` -- the grouping param
**is** present (unlike the plan's fallback assumption), so offense/defense are
two separate calls, not a split of one combined payload. One correction versus
the plan's assumed play-type spelling: the API's canonical `play_type` value
for the pick-and-roll-screener type is **`PRRollMan`** (capital M), not
`PRRollman` -- `SYNERGY_PLAY_TYPES` in `nba_playtype_constants.py` uses the
capture-confirmed spelling.
