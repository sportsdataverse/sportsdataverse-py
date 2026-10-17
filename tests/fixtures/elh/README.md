# `elh` fixtures — Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard)

Keyless sources, captured 2026-10-12:

- `https://s3-eu-west-1.amazonaws.com/hokej.cz` (public, listable S3 bucket)
- `https://json.esports.cz/hokejcz` (scoreboard, rolling window of recent days)

| Dir | What | Source |
|---|---|---|
| `2026/` | 2026-27 (S3 folder `2026`), game 2928321 (HC Sparta Praha 3-0 HC Vítkovice Ridera, 2026-10-11); scoreboard day 2026-10-11; `list.xml` = `?prefix=match/2026/full/&max-keys=3` | byte copies of `sdv-internal-refs/hokejcz/captures/2026/` (refs `eb57f5d`), truncated to 3 items per array |
| `2016/` | 2016-17 (S3 folder `2016`), game 2508598 (Bílí Tygři Liberec v HC Škoda Plzeň, 2016-09-20): the shots floor; no events file (starts 2017-09) | byte copies of `sdv-internal-refs/hokejcz/captures/2016/`, truncated |
| `full/` | FULL, untruncated bodies of game 2928321 and day 2026-10-11, for the dtype / schema tests | fetched live 2026-10-12 13:53 UTC with curl, 1.5 s apart |

`full/` files and their URLs:

| File | URL | Size |
|---|---|---|
| `match__season__full__match_id.json` | `https://s3-eu-west-1.amazonaws.com/hokej.cz/match/2026/full/2928321.json` | 63 KB, 119 comments |
| `visualization__shots__match_id.json` | `https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/shots/2928321.json` | 26 KB, 121 shots |
| `visualization__events__match_id.json` | `https://s3-eu-west-1.amazonaws.com/hokej.cz/visualization/events/2928321.json` | 2.5 KB |
| `scoreboard__date.json` | `https://json.esports.cz/hokejcz/scoreboard/2026-10-11.json` | 9 KB, 7 ELH games |

File names follow the refs capture slug rule (`capture.py` `slug`: the path template, `/` -> `__`, braces and
`.json` dropped; `/` -> `list`). Everything is public data that hokej.cz's pages read anonymously; nothing is
edited out.
