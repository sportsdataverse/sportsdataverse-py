<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WBB test fixtures](#wbb-test-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WBB test fixtures

- `final_320940239_archival_flag_false.json` — trimmed real payload
  (header + boxscore only) of the 2012 national championship game
  (Baylor–Notre Dame), captured 2026-07-17 from
  `wehoop-wbb-raw/wbb/json/final/320940239.json`. The archival-bug case:
  `header.competitions[0].boxscoreAvailable` is **false** while
  `boxscore.teams[].statistics` and `boxscore.players` are fully populated.
  ESPN's flag is unreliable for pre-2014 WBB games (~30% of 2012 games carry
  stats under a false flag); the extraction gates must derive availability
  from the payload, never from this flag.
