<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`mlb_chadwick_register` fixtures](#mlb_chadwick_register-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `mlb_chadwick_register` fixtures

Verbatim slices of two Chadwick Baseball Bureau register shards, used by
`tests/mlb/test_mlb_people_register.py` so `load_mlb_chadwick_register()` runs offline.
The register is published under ODC-By 1.0 (Chadwick Baseball Bureau,
<https://github.com/chadwickbureau/register>).

| File | Provenance | Rows | sha256 |
|---|---|---|---|
| `people-e.csv` | header + 2 verbatim lines of `data/people-e.csv` at register commit `ba48ce1` (2026-11-01): Ichiro Suzuki (`key_mlbam` 400085, `key_fangraphs` 1101) and the shard's first row (no `key_mlbam`) | 2 | `bf41a1276c1186567341f8a2fa033e8bb1a3ba2cb3366ee15c46c31dc86e6ed4` |
| `people-f.csv` | header + 3 verbatim lines of `data/people-f.csv` at the same commit: Mike Trout (`key_mlbam` 545361), the shard's first row (no `key_mlbam`) and file line 1442, Motohiro Shima (zero-padded `key_npb` `01905114`) | 3 | `cecf2a3ceebd1f015e1556377bbaa93f2d1e01eea1842ca37b232e0f99d7b999` |

The test serves the other 14 shards as the real header line with no rows. Captured 2026-11-26.

Id dtypes as loaded: `key_mlbam`, `key_fangraphs` Int64; every other `key_*` Utf8 (`key_npb` is
zero-padded upstream: 331 of its values start with `0`).
Full register at capture: 527,990 people, 137,320 with `key_mlbam` (no duplicate `key_mlbam`),
`key_person` unique.
