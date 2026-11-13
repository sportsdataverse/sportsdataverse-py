<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ncaa_espn_schools](#ncaa_espn_schools)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ncaa_espn_schools

Real rows from the static school tables sdv-reference-data publishes to the `mbb_crosswalk` (and, identically,
`wbb_crosswalk`) sportsdataverse-data releases, written back unchanged: same columns, same dtypes (`espn_team_id`
is String in both, as published). `tests/mbb/test_ncaa_espn_schools.py` feeds them to `ncaa_espn_schools` /
`ncaa_espn_school_seasons` offline. Captured 2026-11-13.

| file | source | rows kept |
|---|---|---|
| `ncaa_espn_schools.parquet` | release `mbb_crosswalk`, asset `ncaa_espn_schools.parquet` | the five lowest `ncaa_org_id`s |
| `ncaa_espn_school_seasons.parquet` | release `mbb_crosswalk`, asset `ncaa_espn_school_seasons.parquet` | those schools, seasons 2023-24 and 2024-25, both `sport`s |
