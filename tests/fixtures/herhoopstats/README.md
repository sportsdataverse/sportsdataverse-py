<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [herhoopstats fixtures](#herhoopstats-fixtures)
  - [Free-tier page (2026-10-20)](#free-tier-page-2026-10-20)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# herhoopstats fixtures

| File | Provenance |
|---|---|
| `team_roster.trim.html` | A team page reached from `GET /stats/ncaa/research/team_single_seasons/` (2024, D-I), captured 2026-09-02 with a live subscription. **Trimmed**: the roster table's `<thead>` plus its first 3 `<tbody>` rows; nothing else modified. |

The `<audio>` name-pronunciation widget inside the player cell is retained
deliberately — it is the whole point of the fixture. `pandas.read_html`
concatenates every descendant text node of a cell, so that element's fallback
copy was being glued onto the player name (`"Te-Hina Paopao  This HTML5
audio..."`), silently breaking name joins.

## Free-tier page (2026-10-20)

| File | Provenance |
|---|---|
| `team_single_seasons_2025_logged_out.html` | `GET https://herhoopstats.com/stats/ncaa/research/team_single_seasons/?min_season=2025&max_season=2025&division=1&games=all&stats_to_show=summary&submit=true` (the request `herhoopstats_teams(min_season=2025)` sends) with no session cookie and the package's `User-Agent`; HTTP 200, `text/html`, 166,126 bytes; captured 2026-10-20 from a residential IP. **Not modified** (it carries no per-request token). |

What an expired session or a lapsed subscription gets (A-M19): a 5-row teaser table with the full
schema and **no login form**, so only the page's analytics `dataLayer` (`'logged_in': 0,
'subscribed': 0`) marks it. The same request through a live subscription the same day answered
`'logged_in': 1, 'subscribed': 1` and 362 rows (not committed: subscriber data). It has no
player column, so it is also the B-GB-24 trigger for `herhoopstats_team_roster`. Used by
`tests/wbb/test_herhoopstats.py`.
