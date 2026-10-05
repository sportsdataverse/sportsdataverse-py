---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook: team–tennis"
sidebar_label: "Playbook: team–tennis"
sidebar_position: 6
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook: team–tennis — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook: team–tennis

## yahoo_playbook_team_social_share

Yahoo shangrila persisted query `playbookTeamSocialShare` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns {#yahoo_playbook_team_social_share-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `team_id` | character | Unique team identifier. |
| `primary_color` | character | Primary team color (hex). |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_team_social_share-example}

```python
yahoo_playbook_team_social_share()
```

_Last validated n/a._

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
