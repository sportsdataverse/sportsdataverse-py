<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB Recruiting & Roster Projection oracle fixtures](#cfb-recruiting--roster-projection-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB Recruiting & Roster Projection oracle fixtures

Committed validation corpus for the T2.2 CFB projection spine
(`sportsdataverse/cfb/cfb_roster_talent.py`, `cfb_returning_production.py`,
`cfb_recruiting_projection.py`, `cfb_transfer_impact.py`, `cfb_draft_projection.py`).
Captured **2026-07-08** by `dev/cfb_projection/capture_oracle.py`. All id columns are
`Utf8`. Read offline in the `tests/cfb/` projection tests — no network needed once committed.

| File | Rows | Source | Key columns |
|---|---:|---|---|
| `results_2016_2023.parquet` | ~30k | `load_cfb_schedule(range(2016, 2024))`, completed games, normalized (`home_id`→`home_team_id`, `home_points`→`home_score`, ids→Utf8) | `game_id, season, week, home_team_id, away_team_id, home_score, away_score, neutral_site` |
| `talent_247_2023.parquet` | ~140 | **247Sports composite team ranking** — `sports247_composite_team_ranking_feed(2023, sport_key=1)` (`ipa.247sports.com` RDB; needs `curl_cffi` at capture time). `talent_247` = `composite_rating`, `talent_rank` = `composite_overall_rank`. | `season, team_id, team, talent_247, talent_rank` |
| `recruits_2020_2023.parquet` | ~15.2k | **247Sports RDB recruit feed** — `sports247_recruits(sport_key=1, year=Y)` paginated (page_size=200 + per-page retry; the RDB times out under rapid paging). Team = **signed** institution (falls back to committed) — the RDB's `committed_institution` drifts with decommits/transfers. | `season, team_id, team, recruit_id, stars, grade, position` |
| `returning_2017_2023.parquet` | ~1.5k | **Computed** — `cfb_returning_production(2017..2023)` over the hosted `cfbfastR-data` per-play player-stats parquet + `load_cfb_rosters`, ESPN id + `classification` attached via `load_cfb_team_info` (`_norm_team(school)`; the teams-crosswalk `norm_key` carries the mascot and does not match play-stats school names). | `season, team, off_returning, def_returning, overall_returning, n_returning, team_id, classification` |

**Validation seasons:** results span 2016–2023; the 247 talent snapshot is the **2023**
validation season.

**Join-key note:** `talent_247_2023.team_id` is the **247 team key** (`key`), *not* an ESPN
id — the two id spaces don't align. Downstream gates (Phase 1) join `cfb_roster_talent`
(ESPN-id-keyed) to this oracle on the **normalized `team` name**, the same pattern the T2.1
spine used for its SP+/FEI oracles.

**Draft fixture — deferred to Phase 5.** `draft_2017_2024.parquet` is *not* captured here:
the ESPN season-draft endpoint (`espn_cfb_season_draft`) 404s for recent years and needs
its own source investigation. Nothing before Phase 5 (NFL-draft projection) consumes it, so
it is captured when Phase 5 lands rather than blocking the Phase-0 substrate.
