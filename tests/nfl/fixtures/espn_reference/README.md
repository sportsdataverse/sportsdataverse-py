<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN reference summaries (supp-012)](#espn-reference-summaries-supp-012)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN reference summaries (supp-012)

ESPN's own play-by-play of games whose alternate-source adapters had no ESPN-reference gate. Read by
`tests/football/sources/_espn_reference.py` (the Fox CFB, Fox NFL and NCAA `*_espn_reference_parity`
tests). Kept in this subdirectory so the whole-game oracle suites, which glob `fixtures/summary_*`,
do not pick them up.

Captured 2026-10-11 with one request each (gzip -9, otherwise untouched):

```sh
curl -sS -A "Mozilla/5.0" "https://site.api.espn.com/apis/site/v2/sports/football/<league>/summary?event=<id>" -o summary_<id>.json
```

| file | game | `<league>` | adapter fixture it references |
|---|---|---|---|
| `summary_401671775.json.gz` | Arizona at Seattle, 2024 week 12 | `nfl` | `tests/nfl/fixtures/fox/fox_nfl_401671775.json` |
