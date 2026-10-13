# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: receiving_coverage

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: receiving_coverage — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_receiving_coverage

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/coverage`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/coverage](https://premium.pff.com/api/v1/facet/receiving/coverage)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_receiving_coverage-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_receiving_coverage-example}

```python
pff_facet_receiving_coverage()
```

_Last validated n/a._
