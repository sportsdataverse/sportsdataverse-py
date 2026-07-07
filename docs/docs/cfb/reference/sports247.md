---
title: CFB — 247Sports Recruit Database (ipa.247sports.com)
sidebar_label: 247Sports Recruit Database (ipa.247sports.com)
sidebar_position: 11
---
# CFB — 247Sports Recruit Database (ipa.247sports.com)

`sportsdataverse.cfb` — 2 endpoints.

## `sports247_teams`

247Sports RDB college team directory (teamId / institutionKey / conference) for a sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/teams/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/teams/](https://ipa.247sports.com/rdb/v1/teams/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `institutionType` | `institution_type` |  |  | `Y` | institutionType query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Team display name (school + nickname). |
| `team_id` | integer | 247Sports RDB team id (per-sport). |
| `institution_key` | integer | 247Sports institution key (school-level, sport-agnostic). |
| `conference` | character | Full name of the athletic conference the team competes in (e.g. ACC, Big Ten). |
| `conference_abbreviation` | character | Conference abbreviation. |
| `sport` | character | Sport name (Football, Basketball, ...). |
| `type` | character | Institution type (College, ...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_teams()
```

_Last validated n/a._

## `sports247_institution_rankings`

247Sports team recruiting-class rankings (247 rank/rating + industry composite) for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/institutionrankings/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/institutionrankings/](https://ipa.247sports.com/rdb/v1/rankings/1/2026/institutionrankings/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pagesize` | `pagesize` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `useComposite` | `use_composite` |  |  | `Y` | useComposite query parameter. |
| `conferenceAbbreviation` | `conference_abbreviation` |  |  | `Y` | conferenceAbbreviation query parameter. |
| `institutionKey` | `institution_key` |  |  | `Y` | institutionKey query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Short display name of the institution as shown on the 247Sports class-ranking page (e.g. USC, Notre Dame). |
| `full_name` | character | Institution full name (school + nickname). |
| `conference_rank` | integer | 247Sports class rank within the conference. |
| `conference_composite_rank` | integer | Composite class rank within the conference. |
| `rank` | integer | 247Sports national team-class rank. |
| `composite_rank` | integer | Industry-composite national team-class rank. |
| `institution_key` | integer | 247Sports institution key (school-level). |
| `team_key` | integer | 247Sports RDB team key (per-sport). |
| `average_rating` | double | Average 247Sports rating of counted commits. |
| `rating` | double | Total 247Sports class rating (team-ranking formula points). |
| `composite_rating` | double | Total composite class rating (formula points). |
| `average_composite_rating` | double | Average composite rating of counted commits. |
| `default_asset` | character | CDN URL of the institution's default logo. |
| `alternate_asset` | character | CDN URL of the institution's alternate logo. |
| `light_asset` | character | CDN URL of the institution's light-background logo. |
| `high_school_ranking_position` | character | Position of the class in the high-school-only ranking. |
| `transfer_points` | character | Transfer-portal points contributed to the class rating. |
| `transfer_number` | integer | Number of incoming transfers counted in the class. |
| `five_stars` | integer | Count of 247Sports five-star commits. |
| `composite_five_stars` | integer | Count of composite five-star commits. |
| `four_stars` | integer | Count of 247Sports four-star commits. |
| `composite_four_stars` | integer | Count of composite four-star commits. |
| `three_stars` | integer | Count of 247Sports three-star commits. |
| `composite_three_stars` | integer | Count of composite three-star commits. |
| `commits` | integer | Number of commits in the class. |
| `site_key` | integer | 247Sports team-site key. |
| `institution_root_path` | character | 247sports.com root path of the institution's team site. |
| `ranking_date` | character | Timestamp the ranking row was last updated. |
| `city` | character | Institution city. |
| `state` | character | Full name of the U.S. state where the institution is located. |
| `state_abbreviation` | character | Institution state abbreviation. |
| `institution_ranking_url` | character | 247sports.com URL of the institution's class-ranking page. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_institution_rankings(year=2026, sport_key=1)
```

_Last validated n/a._
