<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [TheSportsDB API v1 fixtures](#thesportsdb-api-v1-fixtures)
  - [Free key `123` (2026-10-20)](#free-key-123-2026-10-20)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# TheSportsDB API v1 fixtures

Copied verbatim from `sdv-internal-refs/thesportsdb/captures/` (refs main `22b148b`).

Server: `https://www.thesportsdb.com/api/v1/json/3`, the old free key `3` these files were captured with (retired since: it now answers HTTP 403). The current free key is `123`; its captures are in `free_key_123/` (below).
Captured **2026-10-06** live at 2 s pacing; every array is cut to its first 3 elements at
every depth. League `4328` (English Premier League), season `2025-2026`, team `133604`
(Arsenal), player `34145937`, event `2267073`, player search `Danny Welbeck`.

| File | Route | Example params | Envelope key |
|---|---|---|---|
| `all_sports.json` | `/all_sports.php` | — | `sports` |
| `all_leagues.json` | `/all_leagues.php` | — | `leagues` |
| `lookupleague.json` | `/lookupleague.php` | `id=4328` | `leagues` |
| `search_all_teams.json` | `/search_all_teams.php` | `l=English Premier League` | `teams` |
| `lookupteam.json` | `/lookupteam.php` | `id=133604` | `teams` |
| `lookup_all_players.json` | `/lookup_all_players.php` | `id=133604` | `player` |
| `searchplayers.json` | `/searchplayers.php` | `p=Danny Welbeck` | `player` |
| `lookupplayer.json` | `/lookupplayer.php` | `id=34145937` | `players` |
| `eventsseason.json` | `/eventsseason.php` | `id=4328&s=2025-2026` | `events` |
| `lookupevent.json` | `/lookupevent.php` | `id=2267073` | `events` |
| `eventsnextleague.json` | `/eventsnextleague.php` | `id=4328` | `events` |
| `lookuptable.json` | `/lookuptable.php` | `l=4328&s=2025-2026` | `table` |

The envelope key names the resource and varies per route — note `player` for
`/lookup_all_players.php` and `/searchplayers.php` but `players` for
`/lookupplayer.php`. That inconsistency is upstream, not a capture artifact.

## Free key `123` (2026-10-20)

The old free key `3` above is retired: on 2026-10-20 every route answered HTTP 403 with
`The old '3' Free Sports API key is permanently disabled, Please use the new test key "123"`.
`free_key_123/` holds two answers to the new key, byte-for-byte (not trimmed), from
`https://www.thesportsdb.com/api/v1/json/123/` at 2.5 s pacing from a residential IP:

| File | Route | Example params | Rows |
|---|---|---|---|
| `free_key_123/lookuptable.json` | `/lookuptable.php` | `l=4328&s=2025-2026` | 5 (the EPL table has 20) |
| `free_key_123/all_sports.json` | `/all_sports.php` | -- | 2 |

The same session measured the key's caps on the other list routes: `all_leagues` 10,
`search_all_teams` 10 (EPL has 20), `lookup_all_players` 10, `searchplayers` 1,
`eventsseason` 15 (of 380), `eventsnextleague` 1 (gap-B-038). Those caps are the
`_FREE_KEY_CAPS` table in `sportsdataverse/thesportsdb/thesportsdb_runtime.py`, which warns
when a free-key answer reaches one. Used by `tests/thesportsdb/test_runtime.py`.
