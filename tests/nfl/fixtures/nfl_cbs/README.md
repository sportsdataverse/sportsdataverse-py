<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CBS Sports NFL fixtures](#cbs-sports-nfl-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CBS Sports NFL fixtures

| file | source | notes |
|---|---|---|
| `scoreboard_2025_regular_1.html.gz` | `GET https://www.cbssports.com/nfl/scoreboard/2025/regular/1/` (HTTP 200, 1,678,222 bytes), captured 2026-10-11 | Trimmed to the slice the parser reads, from the first game card to 4,000 characters past the last card header (29,335 characters, 16 cards), then gzipped. Each card keeps its `/nfl/teams/<CLUB>/` links and `team-logos/alt/<id>.svg` logos, which `test_the_first_card_logo_is_the_away_team_on_a_real_page` uses to check the away-first logo order `_parse_scoreboard` assumes (B-L61). |

`2026_01_CLE_JAX.json.gz` and `scoreboard_2019_regular_1_cards.html` came with #542 (`32b8586687`) and predate this README.
