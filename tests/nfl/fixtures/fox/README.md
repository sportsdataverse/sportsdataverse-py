<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fox (Bifrost) NFL fixtures](#fox-bifrost-nfl-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Fox (Bifrost) NFL fixtures

Real captures from `api.foxsports.com/bifrost/v1/nfl/event/{id}/{data,odds}`, trimmed to the
fields the adapter reads (header teams + status + colours; per play the id, title, description,
period, clock and the `modalPlay` event geometry). Nothing here is synthetic. The trimmer is
`background-research/2026-09-17-football-sources-program/s2-fox/make_fixtures.py`.

| file | provenance |
|---|---|
| `fox_nfl_401671775.json` | Fox event **10625**, ARI @ SEA (away ESPN team 22, home 26 in the meta), 2024 week 12 (ESPN 401671775). Captured 2026-09-18. Chosen because a **penalty row sits between a touchdown and its try** — the case the #540 PAT-fold defect needed and an all-adjacent game cannot prove. |
| `fox_nfl_401671775_meta.json` | the id-map fields for that game + the id the adapter's own resolver returned (`scores_segment:2024-12-1`). |
| `fox_nfl_401671775_odds.json` | the same event's `/odds` six-pack, trimmed to `sixPack.odds.rows`. |
| `fox_nfl_live_{early,mid,late}.json` | three **unmodified live polls** of Fox event **11052**, DET @ BUF, 2026 week 2 (ESPN 401872932), from the 291-poll capture in `background-research/2026-09-16-nfl-alt-sources/fox_scratch/live_11052/`. Fox serves the whole `pbp` tree **newest-first** until FINAL; these fixtures are that order, which is what makes the re-sort testable. |
| `fox_nfl_401772834.json` | Fox event **10777**, NYG @ DAL (home ESPN team 6, away 19), 2025 week 2 (ESPN 401772834), **FINAL / OT**. Captured 2026-10-19, trimmed by the same field list. Carries the two **event-less "No Play" penalties** (NYG-NYG and DAL-DAL Delay of Game) that ESPN types 8. Its regulation ends on a made field goal, so it does not exercise the end-of-regulation boundary. |
| `fox_nfl_401671702.json` | Fox event **10432**, SEA @ NE (home 17, away 26), 2024 week 2 (ESPN 401671702), **FINAL / OT**. Captured 2026-10-19. Regulation ends on a **kneel** and Fox emits no end-of-Q4 row, so the kneel's end state must not be read from the OT kickoff (23 of the 32 OT games of 2024-25 have this shape). |
| `fox_nfl_401671635.json` | Fox event **10431**, CLE @ JAX (home 30, away 5), 2024 week 2 (ESPN 401671635). Captured 2026-10-19. Carries a **sack for a safety** ("Sacked at JAC End Zone ... SAFETY.", event code `SACK` only). |
| `fox_nfl_401671688.json` | Fox event **10518**, TB @ KC (home 12, away 27), 2024 week 9 (ESPN 401671688), **FINAL / OT**. Captured 2026-10-19. Regulation ends on a **punt** downed at the TB 21 (ESPN: TB ball, 79 to go). |
| `fox_nfl_401772939.json` | Fox event **10726**, SF @ LA (home 14, away 25), 2025 week 5 (ESPN 401772939), **FINAL / OT**. Captured 2026-10-19. Regulation ends on a returned **kickoff** (ESPN: SF ball, 68 to go). |
| `fox_nfl_401671842.json` | Fox event **10616**, ATL @ WAS (home 28, away 1), 2024 week 17 (ESPN 401671842), **FINAL / OT**. Captured 2026-10-19. Regulation ends on a **missed 56-yard field goal** (ESPN: WAS ball at the spot, 62 to go). |
| `fox_nfl_401772882.json` | Fox event **10766**, CAR @ ATL (home 1, away 29), 2025 week 11 (ESPN 401772882), **FINAL / OT**. Captured 2026-10-19. Play 100 is a CAR kickoff that ATL muffs and **CAR recovers** (ESPN: CAR ball); the test cuts the final after it to give it no next snap. |
| `fox_nfl_no_coverage.json` | Fox event **11192**, WAS @ DAL, 2026 week 2 (ESPN 401872944, kickoff 2026-09-20T20:25Z), captured **before kickoff** (`n_plays` 0 in the meta): Fox answers **HTTP 200 with a real header and no plays** (`"pbp": {"sections": []}`). It is a not-yet-played game, not a game below the 2024 play floor (B-GB-12); a pre-2024 capture for the floor case is still owed. |
