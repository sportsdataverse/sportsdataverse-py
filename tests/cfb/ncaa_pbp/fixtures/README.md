# NCAA CFB adapter fixtures

Real `stats.ncaa.org` contest bundles from the `ncaa-mfb-football-raw` archive
(`mfb/raw/{academic year}/{contest id}.json.gz`, captured by the producer's daily sweep),
**trimmed** to the three pages the adapter reads — `play_by_play`, `box_score`, `drives` — and
re-gzipped. The other three tabs the archive stores (`team_stats`, `individual_stats`,
`officials`) are dropped: nothing in this adapter reads them.

Season key is the **starting** year (2025 = fall 2025); the archive speaks academic years, so
these live under `mfb/raw/2026/` upstream.

| fixture | contest | ESPN event | game | why it is here |
|---|---|---|---|---|
| `final_fcs_6386315.json.gz` | 6386315 | 401767513 | Youngstown St. at North Dakota St., 2025 | **FCS-hosted** (both clubs division 12) — the case no other source carries. Complete page. Carries a try separated from its touchdown by a penalty row, and a kickoff return touchdown scored by the team not in possession. |
| `final_fbs_6386337.json.gz` | 6386337 | 401762505 | Tulsa at Florida Atlantic, 2025 | FBS. Complete page. Carries a two-point try separated from its touchdown by a penalty row, and a `Fumble Recovery (Opponent) Touchdown` — the defensive-scorer end state. |
| `truncated_6386337.json.gz` | 6386337 | 401762505 | same, cut after 9 drives, last drive cut to 2 plays | in-progress shape; the newest row is a 6-yard rush, so its end state cannot be its own start. |
| `truncated_early_6386337.json.gz` | 6386337 | 401762505 | same, cut after 2 drives | the opening-drive case: the whole game is one open drive under `drives.current`. |
| `truncated_6386315.json.gz` | 6386315 | 401767513 | FCS game cut after 6 drives | in-progress FCS; the newest snap is a punt whose end spot the page states in its own text. |

Built by `background-research/2026-09-17-football-sources-program/s2-ncaa-cfb/make_fixtures.py`.
A contest page carrying **no** play-by-play is not committed: the producer's capture gate refuses
a pbp page under 40 KB or without `drives`, so the archive can never hold one. The test builds
that shape from `final_fbs_6386337`'s own real `box_score` page, which is a genuine
stats.ncaa.org contest tab with no drive markup.
