<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA / WNBA neutral-site games (`neutral_site_games.csv`)](#nba--wnba-neutral-site-games-neutral_site_gamescsv)
  - [Coverage](#coverage)
  - [Not neutral, on purpose](#not-neutral-on-purpose)
  - [Weak spots](#weak-spots)
  - [Maintaining the list](#maintaining-the-list)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA / WNBA neutral-site games (`neutral_site_games.csv`)

The stats.nba.com schedule release has no neutral-site or arena column. ESPN's `neutral_site` flag
in its schedule release is wrong both ways (checked 2008-2026 on 2026-11-08):

- **Missed:** the 2020 bubble (no game flagged), the 2011 London games (no game is flagged before
  2013), the 2022-12-17 Spurs-Heat game in Mexico City and the 2023-24 Cup semifinals.
- **Wrongly flagged:** 15 All-Star and Rising Stars games, which ESPN files as regular season.

So `sportsdataverse.nba.nba_neutral_site` marks a game neutral by a rule plus this list. The list
was cross-checked against ESPN's venue city. Every 2008-2026 game played away from the home team's
usual city is on the list or explained under "Not neutral, on purpose", except ESPN venue errors:
50 games from October to December 2010, six in late February 2020 and three others show another arena.

The two parts:

- **Rule:** every game dated inside a 2020 bubble.
  - NBA, Walt Disney World: 2020-07-30 to 2020-10-11. That covers the seeding games, the play-in
    and the playoffs ([source](https://en.wikipedia.org/wiki/2020_NBA_Bubble)).
  - WNBA, IMG Academy: 2020-07-25 to 2020-10-06, regular season and playoffs
    ([source](https://en.wikipedia.org/wiki/2020_WNBA_season)).
- **List:** this file, one row per game, keyed by the stats `game_id`. Each `game_id` was found in
  the stats schedule release by date and teams; every row's date and teams agree with the release.

`nba_neutral_site_games()` reads the file. The team ratings and team-season rates
(`nba_possession_rate_rows`, BB-D3 / BB-D19), the season sim (BB-D18), the rated priors and game
flow all drop the home-court term for every neutral game. For the WNBA, pass `league_id="10"`.

Researched 2026-11-06. Nothing comes from stats.nba.com or Sports Reference: the games and
designated home teams are from Wikipedia's [NBA Global Games](https://en.wikipedia.org/wiki/NBA_Global_Games)
list, each checked against the home team's season game log, and from nba.com game pages.

| Column | Meaning |
|---|---|
| `league` | `nba` or `wnba`. |
| `season` | End year of the NBA season; the WNBA season is its calendar year. |
| `game_id` | stats.nba.com game id (Utf8). |
| `game_date` | The release's date (US Eastern). A game in Japan can be a day earlier than its local date. |
| `season_type` | `regular-season`, or `nba-cup` for a Cup final (the final does not count in the standings). |
| `home_team`, `away_team` | stats abbreviations, designated home team first. |
| `arena`, `city`, `country` | Where the game was played. |
| `reason` | `international` (abroad) or `cup_knockout` (an NBA Cup knockout game in Las Vegas). |
| `source_url` | The source for the row. |

## Coverage

There are 50 rows:

- **NBA international (40 games):**
  - Tokyo (1996, 1999), Mexico City (1997, then from 2014), Saitama (2003), London (2011-2019,
    2026), Paris (2020, 2023-2025), Berlin (2026).
  - The 2026-27 games are also listed: Mexico City (scheduled 2026-11-07), Paris and Manchester.
- **NBA Cup knockout games in Las Vegas (9 games):** the two semifinals and the final in each of
  2023-24, 2024-25 and 2025-26.
  - From 2026-27 the semifinals are at the higher seed's arena, so they are home games.
  - The 2026-27 final is in Indianapolis (Hinkle Fieldhouse, 2026-12-11). Add its row once the
    teams are known.
- **WNBA (1 game):** Atlanta vs Seattle in Vancouver, 2025-08-15, the first WNBA regular-season
  game outside the US. The Toronto (2023) and Edmonton (2024) games were preseason.

## Not neutral, on purpose

A team's designated home games at another of its own sites stay home games:

- the 2020-21 Raptors in Tampa;
- the Spurs at the Alamodome and in Austin;
- the 2005-07 Hornets in New Orleans, Baton Rouge and Norman;
- the Toronto Tempo in Montreal and Vancouver;
- the Sun in Boston and Hartford;
- the Mystics in Baltimore.

Also left out:

- **WNBA Commissioner's Cup finals:** they are not in the stats schedule release and do not count
  in the standings. The 2021 final in Phoenix was the only one at a neutral site; since 2022 the
  better team hosts.
- **All-Star games and preseason games.**

## Weak spots

- Pre-2008 NBA and 2012-15 WNBA team season pages carry no arena column, so a neutral game in
  those seasons that is not an international game could be missing.
- The 2026-27 rows were taken from the schedule: check each date and venue once the game is played.

## Maintaining the list

When the NBA announces a game abroad or the Cup sites (usually in summer), add a row. Find its
`game_id` in `load_nba_stats_schedules` by date and teams, and give it a source URL. Then run
`uv run pytest tests/nba/test_nba_neutral_site.py`.
