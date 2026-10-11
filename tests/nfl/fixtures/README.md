<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL fixtures](#nfl-fixtures)
  - [Overtime rule sets (C-M6)](#overtime-rule-sets-c-m6)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL fixtures

## Overtime rule sets (C-M6)

One committed regular-season overtime capture inside each NFL overtime rule set (sdv-internal-refs
`rules/nfl.yaml`); `tools/check_ot_era_captures.py` checks them. All are ESPN summaries from
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=<game_id>`, trimmed
(`summary_<id>_trimmed.json.gz`).

| rule set | rule id | capture | game |
|---|---|---|---|
| <2017, 15-minute sudden death (modified from 2012) | `nfl-2012-overtime-modified-sudden-death-regular-season` | 260917021 | NYG @ PHI, 2006 week 2, 30-24 in overtime |
| 2017-24, 10-minute modified sudden death | `nfl-2017-overtime-ten-minutes` | 401547449 | LAR @ IND, 2023 week 4, 29-23 in overtime |
| 2025+, both teams possess | `nfl-2025-overtime-both-possess-regular-season` | 401772636 | ATL @ IND, 2025 week 10, 25-31 in overtime |

Other overtime captures: 271209010 (SD @ TEN, 2007 week 14), 401671835 (DEN @ CIN, 2024 week 17).
The quality-program ledger wrote the third set as 2024+; the catalogue dates the regular-season
both-possess rule 2025 (2024 still played the 2017 rule), so 401671835 sits in the second set.
