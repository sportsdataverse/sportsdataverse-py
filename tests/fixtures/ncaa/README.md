<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NCAA HTML parser oracle fixtures](#ncaa-html-parser-oracle-fixtures)
  - [Live v1 captures (current stats.ncaa.org markup, 2026)](#live-v1-captures-current-statsncaaorg-markup-2026)

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

## Live v1 captures (current stats.ncaa.org markup, 2026)

These two are **not** from cbb-explorer -- they are live raw-server captures
of a real game, taken 07 Jul 2026 to give the `format_version=1` parser path
its first oracle coverage. The cbb-explorer `:2018+` selectors were ported
faithfully but had drifted against *current* NCAA markup (the team header
moved from `div.card-header img[alt]` / `table[align=center]` to
`a.skipMask img[alt]`, and the per-player box split out of `box_score` into a
separate `individual_stats` tab). See `dev/phase5f-live-proof.md` for how the
Akamai `bm-verify` wall was cleared (a headful browser mints the challenge
cookie; a cookie-carrying fetch then returns the raw server HTML the parsers
target).

| File | Bytes | Provenance | Used by |
|---|---:|---|---|
| `test_v1_play_by_play.html`    | 133,183 | Raw-server capture of `stats.ncaa.org/contests/1613299/play_by_play` (Illinois @ Maryland, 2019-01-26; legacy game 4690813). Current 2018+ (`format_version=1`) markup. | `mbb_ncaa_pbp_parser.py` v1 path (`test_mbb_ncaa_ncaa_v1_live.py`) |
| `test_v1_individual_stats.html`| 177,033 | Raw-server capture of `stats.ncaa.org/contests/1613299/individual_stats` (same game) -- the split-out per-player box (`table.dataTable.small_font#competitor_*`). | `mbb_ncaa_boxscore_parser.py` v1 path (same test) |

Re-capture (only if the current NCAA markup drifts again) needs the
`bm-verify` cookie-mint path documented in `dev/phase5f-live-proof.md`.
