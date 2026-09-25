# ncaa_reference fixtures

Real stats.ncaa.org reference pages for the sport-generic parsers in
`sportsdataverse/scrape/ncaa/reference.py`. Captured 2026-08-26 via the
patchright browser transport (Decodo US residential).

| file | page | why this capture | source |
| --- | --- | --- | --- |
| `mba_team_list_2026_d1.html` | `team/inst_team_list?academic_year=2026&division=1&sport_code=MBA` | 308 D-I baseball teams; proves the parser is sport-generic (graduated from MFB) | stats.ncaa.org |
| `mba_team_page_614839.html` | `/teams/614839` (A&M-Corpus Christi 2026) | 51-game schedule incl. doubleheaders (`02/14/2026(1)`/`(2)` -> game_number) | stats.ncaa.org |
| `mba_team_page_2016_85309.html` | `/teams/85309` (Coastal Carolina 2016, 55-18) | older layout: a "Schedule/Results" title row before the `<th>` header row, results printed `W 17 - 2`; 73 games, 72 contest links. Captured 2026-09-25 by the baseballr-data 2016 backfill (stage 01), copied byte-for-byte | stats.ncaa.org |
| `mba_roster_614839.html` | `/teams/614839/roster` | 40-player roster, header-keyed columns | stats.ncaa.org |
