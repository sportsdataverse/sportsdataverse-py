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
| `fox_nfl_no_coverage.json` | Fox event **11192**, WAS @ DAL, 2026 week 2 (ESPN 401872944, kickoff 2026-09-20T20:25Z), captured **before kickoff** (`n_plays` 0 in the meta): Fox answers **HTTP 200 with a real header and no `pbp` key**. It is a not-yet-played game, not a game below the 2024 play floor (B-GB-12); a pre-2024 capture for the floor case is still owed. |
