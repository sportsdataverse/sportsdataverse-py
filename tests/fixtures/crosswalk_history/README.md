<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Crosswalk history harvest fixtures](#crosswalk-history-harvest-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Crosswalk history harvest fixtures

Real payloads used by `tests/test_crosswalk_history.py`, captured 2026-10-11/12 by
`sportsdataverse._crosswalk_history` (and, for the CBS 404 envelope, by the 2026-10-11 CBS probe).
Files marked *trimmed* keep the real structure with fewer lineup / stat rows.

| File | Source |
|---|---|
| `fox_nba_scores_segment_20241114.json` | `api.foxsports.com/bifrost/v1/nba/league/scores-segment/20241114` |
| `yahoo_nba_game_ids_by_date_20250617_20250623.json` | Yahoo shangrila `leagueGameIdsByDate?leagues=nba&startRange=2025-06-17&endRange=2025-06-23&season=2024` |
| `yahoo_nba_playbook_boxscore_2024102202_trimmed.json` | Yahoo shangrila `playbookBoxscore?gameId=nba.g.2024102202&isProBasketball=true` (trimmed) |
| `fox_nba_event_data_42022_trimmed.json` | `api.foxsports.com/bifrost/v1/nba/event/42022/data` (header + boxscore only) |
| `cbs_nfl_game_boxscore_29375848_trimmed.json` | `api.cbssports.com/napi/resource/game/boxscore/29375848` (trimmed) |
| `cbs_cfb_game_boxscore_2168097_404_envelope.json` | `api.cbssports.com/napi/resource/game/boxscore/2168097` (CBS's 200-status 404 envelope) |
