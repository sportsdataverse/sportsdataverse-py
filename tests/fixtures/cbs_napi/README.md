<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CBS Sports NAPI error bodies](#cbs-sports-napi-error-bodies)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CBS Sports NAPI error bodies

| file | source | notes |
|---|---|---|
| `warnings_404.json` | `GET https://api.cbssports.com/napi/resource/game/scoring/plays/1` through `sportsdataverse.cbs.cbs_napi.cbs_game_scoring_plays(1, return_parsed=False)`, captured 2026-10-11 | HTTP 200, byte-for-byte: `{"warnings": [{"code": 404, "type": "NotFoundException", ...}]}`, NAPI's answer for an unknown game id (the key is `warnings`, not `errors`). It parses to a zero-row frame today instead of `NoDataError` (B-L60, strict xfail in `tests/cbs/test_cbs_napi.py` until task 4.4). |

The other NAPI captures live in `tests/fixtures/cbs/`.
