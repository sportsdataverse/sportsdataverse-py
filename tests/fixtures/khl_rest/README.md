<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [KHL site REST fixtures (`khl_rest`)](#khl-site-rest-fixtures-khl_rest)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# KHL site REST fixtures (`khl_rest`)

Real responses from the khl.ru site REST, `POST https://www.khl.ru/rest/<route>/` with a primed
cookie jar and the Bitrix `sessid` (form-encoded; see `sportsdataverse/hockey/khl/khl_runtime.py`).

- `*.json` (top level): copies of `sdv-internal-refs/khl/captures/rest/` at refs commit `1104660`,
  captured 2026-10-12 by `khl/tools/capture.py`. Tournament `1369` (2025-26 regular season; the
  playoff sample uses `1370`), game `898236`, club `7`, player `41928`, `stat/players` page 1 at
  `nav[count]=20`. TRUNCATED: every array and every id/date-keyed map keeps its first 3 entries,
  and strings over 1,000 characters keep 200 (see the refs `captures/README.md`).
- `full/game__text.json`: ONE complete `/game/text/` body (game `898236`, tournament `1369`),
  fetched 2026-10-12 with `values[tournament]=1369&values[gameid]=898236`, untruncated, written
  compact. The game-page returns schemas (`game_info`, `player_box`, `pbp`, `shots`) and their dtype
  tests come from it, never from the truncated sample.
- `stat__players_goalies_starters.json`: the merged `khl_player_stats(tournament_id=1369,
  position="goalies", return_parsed=False)` envelope fetched 2026-10-18, trimmed to its first 4
  goalies (the most games played), so the goalie columns carry real non-zero values.
- `servicepipe_challenge.html`: the ServicePipe anti-bot JS-challenge page `GET https://www.khl.ru/`
  answered with HTTP 200 on 2026-10-18 instead of the sessid page. The `spsn` / `spid` cookie values
  and the `get_options()` payload are replaced with placeholders; the markup is otherwise as served.

Redactions: `fonbet` (the betting-widget flag) and `bets` are removed at any depth, as the runtime
does. Nothing else is edited. Owner flag (refs `khl/README.md`): fine as test fixtures; no published
data release without sanctions sign-off.
