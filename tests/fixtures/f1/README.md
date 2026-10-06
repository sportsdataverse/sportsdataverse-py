<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Jolpica F1 fixtures](#jolpica-f1-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Jolpica F1 fixtures

Real, trimmed responses from the Jolpica F1 API (Ergast-compatible,
`https://api.jolpi.ca/ergast/f1`, keyless), captured 2026-10-06 and copied
byte-for-byte from `sdv-internal-refs/f1/captures/` (source commit `4fe3515`, copied
2026-10-06). Season 2024, round 1 (Bahrain); round 5 (China) for the sprint, the first
2024 sprint weekend; driver `max_verstappen`. Every array is cut to its first 3
elements at every depth (`MRData.total` still counts the full result).

Data license: [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)
(non-commercial, attribution, share-alike). These are documentation excerpts for
offline parser tests; sdv-py never republishes Jolpica payloads as release assets.

| File | Route |
|---|---|
| `seasons.json` | `/seasons.json` |
| `2024.json` | `/2024.json` |
| `current.json` | `/current.json` (a second schedule sample whose later rounds carry the sprint columns) |
| `2024__1.json` | `/2024/1.json` |
| `2024__1__results.json` | `/2024/1/results.json` |
| `2024__1__qualifying.json` | `/2024/1/qualifying.json` |
| `2024__5__sprint.json` | `/2024/5/sprint.json` |
| `2024__1__laps.json` | `/2024/1/laps.json?limit=100` |
| `2024__1__pitstops.json` | `/2024/1/pitstops.json` |
| `2024__driverStandings.json` | `/2024/driverStandings.json` |
| `2024__constructorStandings.json` | `/2024/constructorStandings.json` |
| `2024__drivers.json` | `/2024/drivers.json` |
| `2024__constructors.json` | `/2024/constructors.json` |
| `circuits.json` | `/circuits.json` |
| `status.json` | `/status.json` |
| `drivers__max_verstappen.json` | `/drivers/max_verstappen.json` |

Every body is `{"MRData": {..., "<Table>": {...}}}`; every value is a string on the
wire. Regenerate by re-copying from the reference repo; do not hand-edit.
