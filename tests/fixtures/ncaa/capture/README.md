<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [A stats.ncaa.org page that did not clear](#a-statsncaaorg-page-that-did-not-clear)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# A stats.ncaa.org page that did not clear

| file | source | notes |
|---|---|---|
| `access_denied_box_score_385627.html` | `GET https://stats.ncaa.org/contests/385627/box_score` with plain `requests` and a desktop Chrome user agent, captured 2026-10-11 | HTTP 403, `text/html`, 405 bytes as served (335 after redaction): Akamai's "Access Denied" page, what a client that has not solved the bot-manager challenge gets instead of the box score. Byte-for-byte except the two Akamai reference ids, replaced with `REDACTED` (they encode request/edge addresses). `capture._is_clean` must reject it (B-M36). |
| `server_error_play_by_play_780704.html` | `https://stats.ncaa.org/contests/780704/play_by_play` (baseball 2012) via the patchright browser transport after the Terms gate, captured 2026-11-26 | HTTP 500, 643 bytes, byte-for-byte: the Rails `public/500.html` page, identical on 6 Decodo exit IPs (contests 780721 and 780074 answered the same). Under the 1 KB stub rule it read as "bm-verify not passed"; the transport must return it and the fetcher must stop after `_SERVER_ERRORS_MAX` proxies. |
