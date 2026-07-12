<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA play-type/impact oracle corpus (T3.5)](#nba-play-typeimpact-oracle-corpus-t35)
  - [Model (2) construct-gap finding (resolved, not deferred)](#model-2-construct-gap-finding-resolved-not-deferred)

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
| `rapm_2024.parquet` | `nba_rapm` (shipped model, not a live wrapper) | fit over possessions from the **FULL 2023-24 season, all 1230 games** (via `dev/nba_playtype/close_drapm_oracle.py` through the ProxyBonanza pool, `lineup_source="pbp"`, resumable per-game checkpoint) | 572 players, 242,619 possessions | **Full-season, maximum possible sample** -- see the "Model (2) construct-gap finding" note below |

## Model (2) construct-gap finding (resolved, not deferred)

`rapm_2024.parquet` was originally a 126-game / ~25k-possession SAMPLE built
to external-cross-validate `nba_matchup_drapm` against shipped stint RAPM
(the test was marked `@pytest.mark.skip`, deferred pending a bigger capture).
That deferral is now **resolved, permanently**: the fixture was rebuilt from
the full 1230-game / 242,619-possession season (the maximum sample this
comparison can ever have), captured via the ProxyBonanza pool with
`lineup_source="pbp"` (~1.3s/game -- the shipped default `"auto"` measured
~50-55s/game due to its rotation -> quarter_box -> pbp fallback chain, which
would have made a full-season capture a multi-hour job; `"pbp"` needs only
the two unconditional network calls and has ~96.7% on-court agreement with
`"auto"` per the `nba_possessions` docstring).

The Spearman(`matchup_drapm`, stint `d_rapm`) trajectory as the sample grew:
-0.018 (100g) / -0.002 (150g) / +0.041 (200g) / +0.039 (300g) / +0.063 (400g)
/ +0.089 (500g) / +0.091 (650g) / +0.111 (800g) / +0.111 (1000g) / **+0.115
(1230g, full season)**. It clearly **plateaus from ~800 games on** (the
800->1000 step, +200 games, moved rho by only -0.0003) -- this is not
ridge-shrinkage noise still settling, it is the actual full-season
relationship, measured at the maximum achievable power.

**Conclusion: matchup-DRAPM (on-ball defense) and stint-DRAPM (team/help
defense) are related but distinct constructs.** The relationship is
weak-but-real and positive (on-ball defense contributes to, but does not
dominate, team defensive value) but falls well short of a shared-construct
correlation. Strict external cross-validation against stint RAPM does not
apply to model (2) as a validation route -- no larger capture will change
this conclusion, since this snapshot already is the full season.

Model (2) ships gated on its INTERNAL concurrent-validity
(`test_matchup_drapm_internal_concurrent_validity`) -- `matchup_drapm`
rank-agrees with each defender's raw points-allowed-per-100 at Spearman
**-0.733** (floor -0.5, n=251), plus ridge-centering (mean~0), magnitude
sanity (max ~11.6, guards the fixed double-scale bug), non-trivial offense-FE
adjustment (|rho|<1), and the synthetic dominant-defender recovery unit test.
The construct-gap finding itself is locked in by
`test_matchup_drapm_vs_stint_rapm_construct_gap` (a real, passing assertion
that the correlation stays in the observed weak-positive band -- a sign-flip
or a sudden jump to a strong correlation would fail it, which is the point:
either would contradict this finding and deserve a fresh look).

Endpoint param names pinned by live capture (Task 0.1 Step 1):
`nba_stats_synergyplaytypes(league_id, season, play_type_nullable,
player_or_team_abbreviation, type_grouping_nullable)` -- the grouping param
**is** present (unlike the plan's fallback assumption), so offense/defense are
two separate calls, not a split of one combined payload. One correction versus
the plan's assumed play-type spelling: the API's canonical `play_type` value
for the pick-and-roll-screener type is **`PRRollMan`** (capital M), not
`PRRollman` -- `SYNERGY_PLAY_TYPES` in `nba_playtype_constants.py` uses the
capture-confirmed spelling.
