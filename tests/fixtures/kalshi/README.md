<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kalshi fixtures](#kalshi-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kalshi fixtures

Real, trimmed responses from the Kalshi Trade API v2
(`https://api.elections.kalshi.com/trade-api/v2`), captured 2026-10-06 and copied
byte-for-byte from `sdv-internal-refs/kalshi/captures/` at refs main `22b148b`.

| File | Route |
|---|---|
| `exchange__status.json` | `/exchange/status` |
| `series.json` | `/series` (`category=Sports`, first 3 rows) |
| `series__series_ticker.json` | `/series/{series_ticker}` |
| `events.json` | `/events` (`limit=3&status=open&series_ticker=KXNFLGAME`) |
| `events__event_ticker.json` | `/events/{event_ticker}` (`with_nested_markets=true`) |
| `markets.json` | `/markets` (`limit=3&event_ticker=KXNFLGAME-26OCT08TBDAL`) |
| `markets__ticker.json` | `/markets/{ticker}` |
| `markets__ticker__orderbook.json` | `/markets/{ticker}/orderbook` (`depth=3`) |
| `markets__trades.json` | `/markets/trades` (`limit=3&ticker=KXNFLGAME-26OCT08TBDAL-DAL`) |

Example tickers: series `KXNFLGAME`, event `KXNFLGAME-26OCT08TBDAL`, market
`KXNFLGAME-26OCT08TBDAL-DAL`. Arrays are cut to their first 3 elements at every
depth.

Market data is **keyless**; `/portfolio/*`, order placement and
`/exchange/announcements` need a `KALSHI-ACCESS-KEY` plus an RSA signature and are
out of scope for this family. Regenerate by re-copying from the reference repo; do
not hand-edit.
