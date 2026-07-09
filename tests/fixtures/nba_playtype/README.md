<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA play-type/impact oracle corpus (T3.5)](#nba-play-typeimpact-oracle-corpus-t35)

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
| `rapm_2024.parquet` | `nba_rapm` (shipped model, not a live wrapper) | fit over possessions from the **first 25 games of the season** (`game_id` `0022300001`..`0022300025`, sorted ascending) via `nba_possessions` per game | 341 players, 4975 possessions | **Sampled, not full-season** -- a full 1230-game RAPM refit is expensive on this host; the sample is large enough for the Phase-2 concurrent-validity gate but individual `d_rapm` values are shrunk harder toward zero than a full-season fit would be (ridge penalty dominates on out-of-scope-sized samples for pure RAPM) |

Endpoint param names pinned by live capture (Task 0.1 Step 1):
`nba_stats_synergyplaytypes(league_id, season, play_type_nullable,
player_or_team_abbreviation, type_grouping_nullable)` -- the grouping param
**is** present (unlike the plan's fallback assumption), so offense/defense are
two separate calls, not a split of one combined payload. One correction versus
the plan's assumed play-type spelling: the API's canonical `play_type` value
for the pick-and-roll-screener type is **`PRRollMan`** (capital M), not
`PRRollman` -- `SYNERGY_PLAY_TYPES` in `nba_playtype_constants.py` uses the
capture-confirmed spelling.
