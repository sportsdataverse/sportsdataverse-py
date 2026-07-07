<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MBB player-value oracle fixtures](#mbb-player-value-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MBB player-value oracle fixtures

External-oracle captures used by the gate tests in
`tests/mbb/test_mbb_player_value_oracle.py`. All captured **2026-07-07** by
`dev/mbb_player_value/capture_oracle.py` (gitignored working script).

**Era note:** the sportsdataverse-data `mbb_player_boxscore` / `mbb_shots` /
`mbb_rosters` releases floor at season **2025**, so the whole player-value
spine (train + oracle gates) is scoped to seasons 2025-2026. Widening the
release backfill widens the trainable era with no code change.

| File | Rows | Source / provenance |
|---|---:|---|
| `barttorvik_bpm_2025.parquet` | 5,059 | `barttorvik.com/getadvstats.php?year=2025&csv=1` — headerless CSV, layout auto-detected (68→67 fields between 2024 and 2025; see `_detect_bart_layout`). Columns: player/team/season, `bpm`, `obpm`, `dbpm` (additive identity `bpm == obpm + dbpm` verified at capture), minutes %, role, plus the ESPN `team_id` matched by the ordered team-name matcher. |
| `barttorvik_bpm_2026.parquet` | 4,978 | Same, `year=2026`. |
| `recruits_2025_2026.parquet` | 200 | ESPN Core v2 recruiting rankings ($ref-resolved): top-100 classes of 2024 + 2025 → college seasons 2025/2026. `grade` = composite grade, `rank` = ESPN order. |
| `draft_2025_2026.parquet` | 119 | NBA Core v2 draft rounds + athlete $refs, 2025 + 2026 drafts: `pick`, `round`, athlete name/college. |
| `rosters_2025_2026.parquet` | 25,494 | sportsdataverse-data `mbb_rosters` 2025+2026: display_name, position, class (`experience_display_value`), height parsed from `6' 5"` strings. Used to anchor archetype/recruit/transfer joins. |

**EvanMiya note:** the plan's secondary BPM oracle (evanmiya.com) is
login-walled with no capturable flat endpoint from this environment — not
captured. The box-BPM gate rests on Barttorvik (primary) plus the independent
125-game NCAA RAPM validation correlation documented in the model card /
`mbb_box_bpm.json` artifact provenance.

Re-capture: `uv run python dev/mbb_player_value/capture_oracle.py all`
(script lives in the gitignored `dev/`; PRED_LEAGUE=womens for the WBB pass).
