---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook: tennis"
sidebar_label: "Playbook: tennis"
sidebar_position: 6
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook: tennis — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook: tennis

## yahoo_playbook_tennis_match

Yahoo shangrila persisted query `playbookTennisMatch` -> one row per `events` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_playbook_tennis_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_tennis_match-example}

```python
yahoo_playbook_tennis_match()
```

_Last validated n/a._
