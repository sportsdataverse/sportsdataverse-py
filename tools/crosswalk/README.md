<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NCAA <-> ESPN basketball team-id crosswalk — build inputs](#ncaa---espn-basketball-team-id-crosswalk--build-inputs)
  - [Rebuild](#rebuild)
  - [Files](#files)
    - [Why the ESPN pool is a union of three sources](#why-the-espn-pool-is-a-union-of-three-sources)
  - [Match rate](#match-rate)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NCAA <-> ESPN basketball team-id crosswalk — build inputs

Everything here is a **build input** for
`build_ncaa_espn_crosswalk.py`. It is not shipped in the wheel; the two
generated outputs are:

- `sportsdataverse/mbb/data/ncaa_espn_team_crosswalk_mbb.csv`
- `sportsdataverse/wbb/data/ncaa_espn_team_crosswalk_wbb.csv`

read at runtime by `ncaa_espn_team_crosswalk(league=...)`.

## Rebuild

```sh
uv run python tools/crosswalk/build_ncaa_espn_crosswalk.py            # offline
uv run python tools/crosswalk/build_ncaa_espn_crosswalk.py --capture  # refresh inputs (network)
```

The default mode is fully offline and deterministic. Run `--capture` only when
ESPN gains/renames a program or a conference realigns, then re-run the default
mode and review the diff.

## Files

| File | Provenance |
|---|---|
| `espn_mbb_teams.csv` | Captured 2026-08-01. Union of ESPN Site-API `mens-college-basketball/teams?groups=50`, the same women's list, hoopR's committed `data-raw/espn_mbb_teams.csv` (2023 snapshot), and per-id lookups for alias targets. Conference columns from hoopR. |
| `espn_wbb_teams.csv` | Same union; conference columns from ESPN Core v2 `seasons/2025/types/2/groups/50/children` -> `groups/{id}/teams`. |
| `dict_hoopR_ncaa_espn.csv` | `NCAA`/`ESPN`/`ESPN_PBP` columns of hoopR's hand-curated `data-raw/dict_hoopR.csv` (367 rows, `year` 2023). The primary bridge. |
| `alias_ncaa_espn.csv` | Hand-curated, league-independent. One row per NCAA name the normalizer + dictionary cannot resolve; every row carries a written justification and was verified individually against ESPN's per-team endpoint. |

### Why the ESPN pool is a union of three sources

- ESPN team ids are **school-level** — one id shared by the men's and women's
  programs — so either league's list backfills the other.
- The two Site-API D-I lists **disagree**: the men's list is missing
  Lindenwood, Queens University and Southern Indiana; the women's list has them.
- Both live lists **forget programs that left D-I** (Hartford, both St.
  Francises); hoopR's 2023 snapshot still carries them.
- Programs that left before 2023 (Savannah State, Centenary, Winston-Salem
  State) are in no bulk list at all, but their per-team endpoint still
  resolves — `--capture` rebuilds those rows from the alias table's ids.

## Match rate

100% of every season in both leagues (2009-10 .. 2025-26) as of the last
build. `match_method` records how each row was resolved: `exact` (~92%),
`dict` (~6%), `alias` (~1%). There is no fuzzy matching — a fuzzy candidate
must be verified by hand and written into `alias_ncaa_espn.csv`.

Contract tests: `tests/mbb/test_ncaa_espn_team_crosswalk.py`.
