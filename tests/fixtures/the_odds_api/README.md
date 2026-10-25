<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [The Odds API fixtures](#the-odds-api-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# The Odds API fixtures

The API needs a key, so no live capture is committed. As for `pff_api`, the fixture is the provider's own
published example, not a hand-built payload.

| File | Source | Notes |
|---|---|---|
| `event_odds_alternate_spreads_spec_example.json` | `sdv-swagger/the_odds_api.openapi.yaml` (46b7a14), `GET /v4/sports/{sport}/events/{eventId}/odds`, 200 response: the event fields from `components.schemas` examples (`MatchId`, `SportKey`, `SportTitle`, `CommenceTime`, `HomeTeam`, `AwayTeam`) and the `bookmakers[].markets[]` property examples (`draftkings`, `alternate_spreads`, six outcomes) | The outcomes' `point` mixes floats (`-23.0`, `22.5`) and a whole number (`-22`), as the spec shows. Drives `test_outcome_point_dtype_is_float64_on_every_market` (task 5.1, B-L90). |
