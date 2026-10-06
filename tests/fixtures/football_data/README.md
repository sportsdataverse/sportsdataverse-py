# Football-Data.co.uk fixtures — provenance

Sample heads (header + the first 3 data rows) of the CSV files served by
`https://www.football-data.co.uk`, captured **2026-10-06** live and keyless,
copied here from `sdv-internal-refs/football-data-co-uk/captures/`.

| Fixture | Source URL | Columns |
|---|---|---|
| `league_season.csv` | `https://www.football-data.co.uk/mmz4281/2526/E0.csv` | 132 |
| `extra_league.csv` | `https://www.football-data.co.uk/new/BRA.csv` | 25 |
| `fixtures.csv` | `https://www.football-data.co.uk/fixtures.csv` | 94 |

The BOM is stripped and line endings are normalised to `\n`; nothing is
redacted (these are public downloads with no personal data beyond referee
names).

The site is a **static CSV archive on shared hosting**, not an API — keep live
use to about one request per second. The column glossary lives at
`/notes.txt`, transcribed in
`sdv-internal-refs/football-data-co-uk/football-data-co-uk-returns.md`; it is a
prose glossary rather than data and is deliberately not wrapped.
