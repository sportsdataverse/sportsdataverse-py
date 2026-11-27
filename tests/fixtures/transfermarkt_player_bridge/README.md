<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN athlete -> Transfermarkt player bridge fixtures](#espn-athlete---transfermarkt-player-bridge-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN athlete -> Transfermarkt player bridge fixtures

Real rows, values unchanged, for `tests/soccer/test_soccer_espn_transfermarkt_player_crosswalk.py`:

- `espn_rows.csv`: 50 athletes from the ESPN Site v2 team rosters fetched 2026-11-27 (the build's input, in its
  `espn_frame` columns): every AFC Bournemouth (team 349) athlete, the two ESPN López born 2006-01-01 (397289,
  259983), up to four athletes from each of the nationality and 1 January resolution paths, and seven unmatched
  athletes that exercise the other branches (several same-date same-name players with no or two citizenship matches,
  and a lone 1 January candidate without club or nationality evidence).
- `tm_players.csv`: every Transfermarkt `players` row (snapshot 2026-07-06) whose birth date equals one of those
  athletes' birth dates, with the columns the matcher reads. Since every candidate shares the birth date, this is the
  whole candidate set for those athletes.
- `tm_transfers.csv`: those players' `transfers` rows dated 2025-07-01 or later (`player_id`, `transfer_date`,
  `to_club_id`).
- `team_club.csv`: the ESPN team -> Transfermarkt `club_id` rows of `soccer_espn_transfermarkt_team_crosswalk` for
  the teams above.
- `reep_people.csv`: the reep (CC0, release 2026.25) `people` rows carrying a `key_transfermarkt` whose `key_espn` is
  an athlete of the bundled table (`reep_id`, `key_espn`, `key_transfermarkt`).
