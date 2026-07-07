<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NCAA HTML parser oracle fixtures](#ncaa-html-parser-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NCAA HTML parser oracle fixtures

Vendored from `Alex-At-Home/cbb-explorer` (Apache License 2.0), local clone
`GitHub-Data/cbb-explorer`, `src/test/resources/ncaa/` -- byte-exact copies,
used as oracle inputs for the Phase 5e HTML-parser port (`RosterParser`,
`BoxscoreParser`, `PlayByPlayParser`, `TeamIdParser`/`TeamScheduleParser`,
`ShotEventParser`). Read as UTF-8 (`sample_roster.html`'s upstream test
suite exercises a diacritic-mutation case).

| File | Bytes | Provenance | Used by |
|---|---:|---|---|
| `sample_roster.html`      | 8,669   | Raw HTTrack mirror capture of `stats.ncaa.org/team/391/roster/15480`, captured 17 Apr 2021 (trailing `<!-- Mirrored from ... -->` comment preserved). v0-format (`table#stat_grid`) roster page, 16 rows (1 initials-only row + 15 real players). | `mbb_ncaa_roster_parser.py` (Task 5e.1) |
| `test_play_by_play.html`  | 117,546 | HTML capture (no trailing HTTrack comment -- lightly prepared as a test fixture rather than a raw mirror), 2018-format play-by-play page including an inline "2018 format: specific end of game marker" comment. | `mbb_ncaa_pbp_parser.py` (Task 5e.3) |
| `test_lineup.html`        | 42,337  | HTML capture (no trailing HTTrack comment), box-score/lineup page (`dataTable`-shaped). | `mbb_ncaa_boxscore_parser.py` (Task 5e.2) |
| `test_schedule.html`      | 15,041  | Redacted/synthetic test fixture -- contains placeholder tokens (`TEAM_NAME`, `OTHER TEAM NAME`) in place of real team names/logos, not a raw site capture. | `mbb_ncaa_team_parsers.py` (Task 5e.4) |
| `test_attendance_list.html` | 3,784 | HTML capture (no trailing HTTrack comment), a `dataTable`-shaped attendance report fragment. | `mbb_ncaa_team_parsers.py` (Task 5e.4, `get_neutral_games`) |

Regenerate by re-copying the same 5 files from the upstream clone's
`src/test/resources/ncaa/` directory (byte-exact -- do not re-format or
re-encode). The parser tests are fixture-specific (unlike the ESPN JSON
fixtures, these HTML pages aren't payload-agnostic across sports/leagues),
so a re-capture should only be done if the upstream fixture itself changes.
