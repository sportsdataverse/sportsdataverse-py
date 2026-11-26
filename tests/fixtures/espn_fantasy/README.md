<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN Fantasy fixtures](#espn-fantasy-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN Fantasy fixtures

Byte copies of the maintainers' captures of ESPN's fantasy read API, taken **2026-11-26** with no
cookies (public data only). `manifest.json` records each file's full URL, its `X-Fantasy-Filter`
header, the status and the size of the full body.

- `games.json`: `https://lm-api-reads.fantasy.espn.com/apis/v3/games`
- `<game>/` for `ffl`, `fba`, `flb`, `fhl`, `wfba`:
  - `seasons.json`: `/games/<game>/seasons`
  - `pro_team_schedules.json`: `/games/<game>/seasons/<season>?view=proTeamSchedules_wl`
  - `players.json`: `/games/<game>/seasons/<season>/players?view=players_wl` with filter `{"filterActive":{"value":true}}`
  - `league_directory.json`: `/games/<game>/seasons/<season>/leaguedirectory/DRAFT_LOBBY`
  - `default_settings.json`: `/games/<game>/seasons/<season>/segments/0/leaguedefaults/<id>?view=mSettings`
  - `player_news.json`: `https://site.api.espn.com/apis/fantasy/v3/games/<game>/news/players?playerId=<id>`
- `fba/league_<view>.json`: public league `1535462463` (2027 season), from ESPN's own public-league directory:
  `/games/fba/seasons/2027/segments/0/leagues/1535462463?view=<view>` for `mTeam`, `mRoster`,
  `mMatchupScore`, `mBoxscore` (+ `scoringPeriodId=37`), `mSettings`, `mDraftDetail`, `mTransactions2`
  (+ `scoringPeriodId=37`) and `kona_player_info` (filter: the 25 most-owned players). No other game had a
  public league with a finished draft on the capture date.

The captures are redacted before they are written:

- member records are dropped;
- member account GUIDs become `{MEMBER-nn}`;
- team names become `Team <id>`, and logos are blanked;
- licensed prose (`story`, `seasonOutlook`, player-news text) becomes a placeholder;
- sportsbook promotion blocks are dropped.

Every array is then cut to its first 3 elements, with played matchups first.
