<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Polymarket fixtures](#polymarket-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Polymarket fixtures

Real, trimmed responses from Polymarket's three keyless read hosts
(`https://gamma-api.polymarket.com` and `https://clob.polymarket.com`, captured
**2026-10-06**, and the Polymarket US exchange's `https://gateway.polymarket.us`,
captured **2026-11-18**), copied byte-for-byte from the maintainers' captures.

| File | Host | Route |
|---|---|---|
| `gamma-api__markets.json` | `gamma-api.polymarket.com` | `/markets` (bare list) |
| `gamma-api__markets__id.json` | `gamma-api.polymarket.com` | `/markets/{id}` (object, carries `$schema`) |
| `gamma-api__events.json` | `gamma-api.polymarket.com` | `/events` (bare list) |
| `gamma-api__tags.json` | `gamma-api.polymarket.com` | `/tags` (bare list) |
| `clob__markets.json` | `clob.polymarket.com` | `/markets` (`{data, next_cursor, limit, count}` envelope) |
| `clob__book.json` | `clob.polymarket.com` | `/book` (`bids` + `asks` plus scalar siblings) |
| `clob__midpoint.json` | `clob.polymarket.com` | `/midpoint` (`{"mid": "..."}`) |
| `clob__price.json` | `clob.polymarket.com` | `/price` (`{"price": "..."}`) |
| `gateway__v1__markets.json` | `gateway.polymarket.us` | `/v1/markets?closed=false&active=true` (`{"markets": [...]}` envelope; open MLS Cup futures) |
| `gateway__v1__markets__closed.json` | `gateway.polymarket.us` | `/v1/markets` unfiltered first page (closed 2025 game markets; no `title` / `bestBidQuote` / ...) |
| `gateway__v1__markets__slug__bbo.json` | `gateway.polymarket.us` | `/v1/markets/{slug}/bbo` (`{"marketData": {...}}`; open market) |
| `gateway__v1__markets__slug__bbo__expired.json` | `gateway.polymarket.us` | `/v1/markets/aec-nba-sa-sac-2026-11-06/bbo` (expired: `bestBid` / `bestAsk` null, no quotes) |

Pinned examples: Gamma market `608565` ("Will Lionel Messi win the 2026 Ballon
d'Or?", event `ballon-dor-winner-2026`) and its first `clobTokenIds` entry, the
77-digit token id
`14940935985254400645996902276818598247231942791302218826999289776538227075176`.
That market is **a long-dated contract** (it ends 2027-01-01), deliberately
chosen so `/book`, `/midpoint` and `/price` keep answering: a daily market's
order book disappears at resolution and would make these fixtures meaningless on
the next re-capture. Re-pin after 2027-01-01.

The US bbo is pinned to `tec-mls-winner-2026-11-07-sea`, an MLS Cup future that
ends 2027-01-01, so `bestBid` / `bestAsk` / `longQuote` / `shortQuote` are populated
(an expired game market answers the bests null and drops the quotes). A game
market's slug is `aec-` + a Gamma event's `usId`, the form `join_espn_polymarket` (#1227)
emits as `polymarket_us_slug`. Re-pin after 2027-01-01.

List routes are the **first page only** — Gamma and US page by `limit`/`offset`, CLOB
`/markets` by `next_cursor`; the wrapper sends the caller's paging parameter and
never follows a continuation token. Arrays are cut to their first 3 elements at
every depth. Regenerate by re-copying from the reference repo; do not hand-edit.
