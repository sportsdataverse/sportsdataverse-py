<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [official.nba.com fixtures](#officialnbacom-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# official.nba.com fixtures

Real captures from `official.nba.com`, taken 2026-09-26 from a residential IP with a Chrome
desktop User-Agent (`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like
Gecko) Chrome/128.0.0.0 Safari/537.36`) unless noted otherwise. Copied byte-for-byte from
`ClaudeCowork/plans/2026-09-26-f5-l2m-referees/research/fixtures/`.

| File | URL | HTTP | Notes |
|---|---|---|---|
| `l2m_json_0042500405.json` | `https://official.nba.com/l2m/json/0042500405.json` | 200 `application/json` (S3 behind Akamai) | 2026 Finals G5, NYK @ SAS, 21 graded rows. Top-level keys: `game`, `l2m`, `stats`. |
| `l2m_json_0022500002_no_report_s3_403.xml` | `https://official.nba.com/l2m/json/0022500002.json` | **403** `application/xml` S3 `AccessDenied` | What "no L2M for this game" looks like. This is a 403, not a 404. |
| `akamai_403_blocked_ua.html` | `https://official.nba.com/l2m/json/0042500405.json` (same URL as the first row, sent with a libcurl User-Agent: `libcurl/7.68.0 r-curl/4.3.2 httr/1.4.2`) | **403** `text/html` Akamai "Access Denied" | The WAF block on a non-browser User-Agent. |
| `l2m_listing_2025-26.html` | `https://official.nba.com/2025-26-nba-officiating-last-two-minute-reports/` | 200 `text/html` | Season index page: `L2MReport.html?gameId=` links for the season. |
| `referee_assignments_2026-06-13.json` | `https://official.nba.com/wp-json/api/v1/get-game-officials?&date=2026-06-13` | 200 `application/json` (envoy, `Cache-Control: max-age=300`) | Keys `nba`, `gl`, `wnba`. Each holds `Table` (games) and `Table1` (replay-center officials) with `columns` and `rows`. |
