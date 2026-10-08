# kenpom fixtures

| File | Provenance |
|---|---|
| `ratings_2025.trim.html` | `GET https://kenpom.com/index.php?y=2025`, captured 2026-09-02 with a live subscription. **Trimmed**: 3 of the page's 10 `<thead>` blocks and the first 8 `<tbody>` rows; nothing else modified. |

The trim keeps what the tests need and nothing more. KenPom is subscription
content, so only enough rows to pin parsing behaviour are committed.

Why 3 `<thead>` blocks: KenPom re-renders its 2-row header roughly every 40 data
rows as **10 separate `<thead>` elements in one table**, so `pandas.read_html`
reports a **20-level** MultiIndex on the live page. Three blocks reproduce the
same bug class (6 levels, group/name labels repeating non-adjacently) at a
fraction of the size. The rows include a seeded team (`"Duke 1"`) and a blank
separator row.

## Returns-table captures (2026-10-07)

One page per endpoint, captured with a live subscription by `tools/codegen/capture_fixtures.py`
and stored as the wrapper's raw HTML string. Same rule as above: each page is cut to a minimal
excerpt -- `<script>`/`<style>` removed and every table body capped at 3 rows (40 for `conference`, `efficiency`, `foul_trouble`, `four_factors`, `point_distribution`, whose
header re-renders every ~40 rows; `team` keeps its scripts, which carry its depth chart) -- and
the trim was kept only where the endpoint's parser returns exactly the columns it returns on the
full page. No page carries account details. `win_probability` is not vendored: KenPom draws that
page from an embedded script and `parse_kenpom_page` reads HTML tables only, so it parses empty.

| file | URL |
|---|---|
| `archive_ratings.json` | `https://kenpom.com/archive.php?d=2025-02-01` |
| `arenas.json` | `https://kenpom.com/arenas.php?y=2025` |
| `box.json` | `https://kenpom.com/box.php?g=1097&y=2025` |
| `coach_history.json` | `https://kenpom.com/history.php?c=Jon+Scheyer` |
| `conference.json` | `https://kenpom.com/conf.php?c=ACC&y=2025` |
| `conference_history.json` | `https://kenpom.com/confhistory.php?c=ACC` |
| `conference_stats.json` | `https://kenpom.com/confstats.php?y=2025` |
| `efficiency.json` | `https://kenpom.com/summary.php?y=2025` |
| `fan_match.json` | `https://kenpom.com/fanmatch.php?d=2025-02-01` |
| `foul_trouble.json` | `https://kenpom.com/foul_trouble.php?y=2025` |
| `four_factors.json` | `https://kenpom.com/stats.php?y=2025` |
| `game_attributes.json` | `https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore` |
| `game_plan.json` | `https://kenpom.com/gameplan.php?team=Duke&y=2025` |
| `height.json` | `https://kenpom.com/height.php?y=2025` |
| `home_court_advantage.json` | `https://kenpom.com/hca.php` |
| `kpoy.json` | `https://kenpom.com/kpoy.php?y=2025` |
| `officials.json` | `https://kenpom.com/officials.php?y=2025` |
| `opponent_tracker.json` | `https://kenpom.com/opptracker.php?team=Duke&y=2025&t=o` |
| `player_career.json` | `https://kenpom.com/player.php?p=51234` |
| `player_stats.json` | `https://kenpom.com/playerstats.php?y=2025&s=eFG` |
| `point_distribution.json` | `https://kenpom.com/pointdist.php?y=2025` |
| `program_ratings.json` | `https://kenpom.com/programs.php` |
| `ratings.json` | `https://kenpom.com/index.php?y=2025` |
| `referee.json` | `https://kenpom.com/referee.php?r=714294&y=2025` |
| `team.json` | `https://kenpom.com/team.php?team=Duke&y=2025` |
| `team_history.json` | `https://kenpom.com/history.php?t=Duke` |
| `team_players_expanded.json` | `https://kenpom.com/player-expanded.php?team=Duke&y=2025` |
| `team_stats.json` | `https://kenpom.com/teamstats.php?y=2025&od=o` |
| `trends.json` | `https://kenpom.com/trends.php` |
