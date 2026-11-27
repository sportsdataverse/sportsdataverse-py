<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Transfermarkt dataset fixtures](#transfermarkt-dataset-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Transfermarkt dataset fixtures

Real rows of the CC0 1.0 Transfermarkt dataset (snapshot current to 2026-07-06), downloaded
2026-11-26 from `https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/<table>.csv.gz`
(the files the loaders read). Values are copied unchanged; the CSVs were re-written
uncompressed and unquoted, and the tests gzip them before serving.

- `clubs.csv`: every club whose `club_id` the ESPN <-> Transfermarkt team crosswalk carries.
- `players.csv`: the three lowest `player_id` of each of those clubs, plus two players with no
  `date_of_birth` and two with no `height_in_cm`.
- `player_valuations.csv`, `transfers.csv`: every row of the first 40 of those players, plus three
  transfers with no fee.
- `competitions.csv`: the whole table (65 rows).

Used by `tests/soccer/test_soccer_transfermarkt.py`.
