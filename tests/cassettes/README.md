<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [HTTP cassettes (record/replay fixtures)](#http-cassettes-recordreplay-fixtures)
  - [Format](#format)
  - [Recording a new cassette](#recording-a-new-cassette)
  - [Secret hygiene (non-negotiable)](#secret-hygiene-non-negotiable)
  - [Demo cassettes](#demo-cassettes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# HTTP cassettes (record/replay fixtures)

Each `*.json` file here is a **cassette**: one or more recorded HTTP
interactions (request match-key → response body/status/url) that the
`tests/_vcr.py` harness replays offline. They let live-path tests exercise the
*real* call chain (URL build → `download()` retry loop → `no_espn_data()` →
parser) with **zero network**, so CI never flakes on upstream downtime.

## Format

```jsonc
{
  "version": 1,
  "interactions": [
    {
      "request": {            // the match key (see tests/_vcr.py::_key)
        "method": "GET",
        "url": "https://.../teams/194",   // base URL, query folded into params
        "params": "{}"                    // scrubbed, sorted params as a JSON string
      },
      "response": {
        "status_code": 200,
        "url": "https://.../teams/194",   // secrets in the echoed URL are redacted
        "body": "{...}",                  // raw response text
        "content_type": "application/json"
      }
    }
  ]
}
```

## Recording a new cassette

Wrap the live call in `use_cassette(...)` inside a **live-gated** test, then run
it once in record mode:

```sh
SDV_PY_RECORD=1 SDV_PY_LIVE_TESTS=1 uv run pytest tests/test_vcr.py::test_name
```

The harness writes the cassette on context exit. Commit it; subsequent runs
(no env vars) replay it offline. Re-record when the upstream schema drifts.

## Secret hygiene (non-negotiable)

Cassettes are committed. The harness **scrubs known-secret query params**
(`apiKey`, `token`, `key`, ...) from both the request key and the echoed
`response.url` before writing — so a recorded Odds API interaction stores
`apiKey=REDACTED`, never the real credential. `_SECRET_KEYS` in
`tests/_vcr.py` is the single source of truth; extend it (don't hand-redact) if
a provider uses a new credential param name. Before committing a freshly
recorded cassette, grep it for any leaked secret.

## Demo cassettes

| Cassette | Exercises |
|---|---|
| `espn_cfb_team_basic.json` | Happy-path GET → `download().json()` |
| `espn_cfb_team_missing.json` | ESPN 404 → `no_espn_data()` raises `NoESPNDataError` |
