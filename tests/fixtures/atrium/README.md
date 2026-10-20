# Sportradar Atrium embed API fixtures (`embed-api.eui.connect.sportradar.com/v1/embed`)

**Per-route captures** (`<websiteId>__<type>[__<sub>].json` in this directory): copied verbatim from
`sdv-internal-refs/atrium-embed/captures/` (refs commit `40142b0`, captured 2026-10-12, keyless GET, one
request per route). They are truncated (every array and every UUID-keyed map cut to 3 entries), so a rate
that is whole in the sample is an integer there; they type nothing. `sites.json` (the registry) is not copied.
The exact requests are in the refs `ENDPOINTS.md`.

**`full/`**: full bodies fetched live on 2026-10-12 (keyless `GET`, 1.5 s apart, the same pinned ids as the
captures; LNB season Betclic ELITE 2025 = `df310a05-51ad-11f0-bd89-c735508e1e09`, LNB game
`5423072b-6715-11f0-bd8b-9ffadebc148f`, LNBP game `0dc631bd-c34c-11f1-8d16-4d5a21afb79d`). They exist because
the dtype of a statistic comes from the whole body, not the 3-row sample. Compacted JSON (values untouched);
three are cut to their first rows to stay small, every kept row complete:

| File | Body | Cut |
|---|---|---|
| `14__fixture_detail__statistics.json` | LNBP box score (field-goal totals, fouls drawn, no plus/minus) | none |
| `12__fixture_detail__statistics.json` | LNB box score (blocks received, plus/minus, no field-goal totals) | none |
| `14__fixture_detail__pbp.json` | LNBP play-by-play, 497 events | none |
| `14__fixture_detail__shot_chart.json` | LNBP shot chart, 129 shots | none |
| `12__standings.json` | LNB standings, 16 teams | none |
| `12__statistics_entities.json` | LNB team season statistics | none |
| `12__statistics_leaders.json` | LNB season leaders | none |
| `12__fixtures.json` | LNB results of one season (`sub=RESULTS`, 240 games) and the site's 19-season list | first 60 fixtures |
| `12__persons.json` | LNB players of one season (294) | first 60 rows |
| `12__statistics_persons.json` | LNB player season statistics, three tables of 276 players | first 60 players per table |

**`refusals/`**: `14__fixture_detail__pbp__other_site.json` is the LNB game `5423072b-6715-11f0-bd8b-9ffadebc148f`
asked under site 14 (LNBP), fetched live on 2026-10-18 (keyless `GET`, compacted): HTTP 200 with an empty `fixture`
(no `fixtureId`, `seasonId` or competition) and no periods, byte-for-byte the body an unknown fixture id gets on its
own site. It sits outside `full/` because it parses to nothing.

`spec_paths.json` is the spec's route list, written by `tools/codegen/gen_atrium.py` so the tests check the
YAML without the private refs checkout.
