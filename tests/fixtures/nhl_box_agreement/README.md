<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL boxscore source-agreement fixtures](#nhl-boxscore-source-agreement-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NHL boxscore source-agreement fixtures

Captured 2026-11-25 with the package's own wrappers, for
`tests/nhl/test_nhl_box_source_agreement.py`: the same three 2025-26 regular-season games from
the NHL api-web boxscore (`nhl_boxscore(game_id, return_parsed=False)`) and the ESPN Site v2
summary (`espn_nhl_summary(event_id, return_parsed=False)`).

| NHL game id | ESPN event id | Date (ET) | Game | Why |
|---|---|---|---|---|
| 2025020009 | 401802360 | 2025-10-09 | NYR @ BUF | BUF's `J.Kulich` ships with no space after the initial |
| 2025020026 | 401802377 | 2025-10-11 | NJD @ TBL | both abbreviations differ between the sources (`NJ`, `TB`) |
| 2025020020 | 401802371 | 2025-10-09 | VGK @ SJS | `SJ` vs `SJS`; ESPN's UTC start is already 2025-10-10 |

Endpoints:

- `nhl_boxscore_<game_id>.json`: `https://api-web.nhle.com/v1/gamecenter/<game_id>/boxscore`
- `espn_summary_<event_id>.json`: `https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=<event_id>`

Both are trimmed to what the test reads; values are unchanged:

- NHL: the game header (`id`, `season`, `gameType`, `gameDate`, `gameState`, each team's `id` /
  `abbrev` / `score`) and the `forwards` / `defense` skaters with `playerId`, `sweaterNumber`,
  `name`, `position`, `goals`, `assists`, `plusMinus`, `sog`. Goalies are dropped.
- ESPN: `header.competitions[0]` (id, date, competitors' side and abbreviation) and the `forwards`
  / `defenses` blocks of `boxscore.players`, each team's `id` / `abbreviation` / `displayName` /
  `location`, the `goals`, `assists`, `shotsTotal`, `plusMinus` keys and each athlete's id, names,
  jersey and position abbreviation, with its `stats` cut to those four keys in the same order.
