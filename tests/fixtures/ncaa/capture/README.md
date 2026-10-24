<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [A stats.ncaa.org page that did not clear](#a-statsncaaorg-page-that-did-not-clear)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# A stats.ncaa.org page that did not clear

| file | source | notes |
|---|---|---|
| `access_denied_box_score_385627.html` | `GET https://stats.ncaa.org/contests/385627/box_score` with plain `requests` and a desktop Chrome user agent, captured 2026-10-11 | HTTP 403, `text/html`, 405 bytes as served (335 after redaction): Akamai's "Access Denied" page, what a client that has not solved the bot-manager challenge gets instead of the box score. Byte-for-byte except the two Akamai reference ids, replaced with `REDACTED` (they encode request/edge addresses). `capture._is_clean` must reject it (B-M36). |
