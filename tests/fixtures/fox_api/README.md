<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fox Sports API fixtures](#fox-sports-api-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Fox Sports API fixtures

Real responses from the public `api.foxsports.com` (public `apikey` + `api-version=1.1`
query pair shipped in the foxsports.com web bundle; no account). Captured 2026-10-05
with `curl`; `trending_*` and `foxpolls` are sliced to their first 2 / 5 / 5 records
(rows sliced, nothing edited).

| File | Route |
|---|---|
| `nfl_scoreboard.json` | `/bifrost/v1/nfl/scoreboard/main` (week selector shell, no events) |
| `nfl_league_scores_segment.json` | `/bifrost/v1/nfl/league/scores-segment/2026-3-1` (16 games) |
| `nfl_league_header.json` | `/bifrost/v1/nfl/league/header` |
| `nfl_league_standings.json` | `/bifrost/v1/nfl/league/standings` |
| `cfb_league_polls.json` | `/bifrost/v1/cfb/league/polls` |
| `cbk_league_conferences.json` | `/bifrost/v1/cbk/league/conferences` |
| `nfl_event_matchup.json` | `/bifrost/v1/nfl/event/11195/matchup` |
| `nfl_team_header.json` | `/bifrost/v1/nfl/team/25/header` |
| `nfl_team_roster.json` | `/bifrost/v1/nfl/team/25/roster` |
| `explore_browse_sports.json` | `/bifrost/v1/explore/browse/sports/main` |
| `search_content.json` | `/bifrost/v1/search/content?text=mahomes` |
| `search_popular.json` | `/bifrost/v1/search/popular` |
| `trending_articles.json` | `/bifrost/v1/general/trending/articles?duration=4` (feed key) |
| `trending_videos.json` | `/bifrost/v1/general/trending/videos?duration=4&maxItems=12` (feed key) |
| `foxpolls.json` | `/foxpolls/v1/polls?includeAnswers=true` (feed key) |

Live status on 2026-10-05 (known-positive control: the 15 routes above return 200 with
the same key pair in the same run): `/fs/feed`, `/fs/images`, `/fs/layouts`, `/fs/videos`
return 404 (`Unable to identify proxy for host: secure`) with BOTH keys; `scorechip`
(`nfl11195`) returns 400; `topevents/scoreboard/segment/2026-3-1` returns 404;
`nfl/league/polls` and `nfl/league/conferences` 404 (polls/conferences are CFB/CBK
concepts). Those endpoints stay wired from the sdv-js spec but have no fixture.
