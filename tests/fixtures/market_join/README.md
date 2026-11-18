<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Market-join fixtures](#market-join-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Market-join fixtures

One recorded week of real payloads per league for `tests/odds/test_market_join.py`, written by
`tools/record_market_join_fixtures.py --start 20261118 --days 7` on **2026-11-18**.

| File | Route |
|---|---|
| `<league>/espn_scoreboard.json` | `site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard?dates=<day>&limit=500`, one request per day 2026-11-18..24, events concatenated |
| `<league>/kalshi_markets.json` | `api.elections.kalshi.com/trade-api/v2/markets?series_ticker=<KX..GAME>&status=open&limit=1000`, every cursor page |
| `<league>/gamma_events.json` | `gamma-api.polymarket.com/events?series_id=<id>&active=true&closed=false&limit=100`, every offset page |

| League | ESPN games | Kalshi markets | Gamma events |
|---|---|---|---|
| nba | 51 | 70 | 118 |
| nfl | 13 | 58 | 42 |
| nhl | 42 | 88 | 154 |

Trimmed to the keys the join reads. ESPN events keep `id`, `uid`, `date`, `name`, `shortName`, `status`
and, in the first competition, `id`, `date`, `neutralSite` and each competitor's `id`, `homeAway` and team
`id`, `abbreviation`, `displayName`, `shortDisplayName`, `name` and `location`. Kalshi markets keep the
ticker, title, status, time and the five `*_dollars` prices. Gamma events keep `id`, `slug`, `title`,
`eventDate`, `startTime`, `usId`, `active` and `closed`, plus each team's `abbreviation`, `alias`, `name`
and `ordering`.

Kalshi lists a game a few days ahead: in this recording NBA tickers run through 2026-11-22, so
the NBA games on 11-23 and 11-24 have no Kalshi market. Gamma lists the whole week.
