<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Preciser (P. LEAGUE+ vendor) fixtures: provenance](#preciser-p-league-vendor-fixtures-provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Preciser (P. LEAGUE+ vendor) fixtures: provenance

Real bodies from the THIRD-PARTY analytics vendor `api.preciser.io` (keyless, CORS-open), captured live on
**2026-10-12** by the maintainers' capture tool. The tests read only these committed files.
All bodies are for gid 197 (uuid `e6f98675-3e5b-11ed-949e-5d17350689cf`, 2022-23 opener).

| File | Source URL | What |
|---|---|---|
| `full/chart_data_197.json` | `https://api.preciser.io/v1/chart-data/id/e6f98675-3e5b-11ed-949e-5d17350689cf` | The FULL, untruncated shot list: all 168 shots (the made 2- and 3-pointers equal the box's). The numeric dtypes of the shots frame come from it. Clip urls carry `key=<redacted>`: the vendor access key was scrubbed by the capture and is never kept |
| `summary_197.json` | `/post/game/summary/o/plg/gameId/<uuid>` | Whole body |
| `rotation_197.json` | `/game/rotation/chart/o/plg/gameId/<uuid>` | Whole body |
| `injury_197.json` | `/game/injury/list/o/plg/gameId/<uuid>` | Whole body (`injury_report.home/away` are null: shape unverified) |
| `captures/*.json` | the maintainers' vendor capture records | The same routes for six games (`@206` double overtime, `@319` playoffs, `@662` and `@716` 2025-26), every array cut to 3 rows |
| `spec_paths.json` | `pleague-preciser.openapi.yaml` | The spec's four routes, written by `tools/codegen/gen_pleague.py` |

The site's box for gid 197 (`tests/fixtures/pleague/box_197.json`) is the roster the verified-mode tests compare the
shots and the rotation with.
