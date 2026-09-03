# herhoopstats fixtures

| File | Provenance |
|---|---|
| `team_roster.trim.html` | A team page reached from `GET /stats/ncaa/research/team_single_seasons/` (2024, D-I), captured 2026-09-02 with a live subscription. **Trimmed**: the roster table's `<thead>` plus its first 3 `<tbody>` rows; nothing else modified. |

The `<audio>` name-pronunciation widget inside the player cell is retained
deliberately — it is the whole point of the fixture. `pandas.read_html`
concatenates every descendant text node of a cell, so that element's fallback
copy was being glued onto the player name (`"Te-Hina Paopao  This HTML5
audio..."`), silently breaking name joins.
