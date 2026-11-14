---
title: "Men's college basketball — additional Python functions — Analytics: mbb_team–ncaa_mbb"
sidebar_label: "Analytics: mbb_team–ncaa_mbb"
sidebar_position: 17
description: "Men's college basketball — additional Python functions — Analytics: mbb_team–ncaa_mbb — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: mbb_team–ncaa_mbb

### mbb_team_shot_zones {#mbb_team_shot_zones}

`mbb_team_shot_zones(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College season shot profile by zone per team, for and allowed: rim / mid / corner 3 / above-the-break 3.

One row per team and season over every game: the team's own attempts and, with an `opp_`
prefix, its opponents' attempts against it (a non-D1 opponent's attempts included).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)` (the ESPN team id, Int64) with `games` and the columns of `mbb_player_shot_zones` for the team and again with an `opp_` prefix for its opponents, each rate with `<m>_n`, `<m>_rank` and `<m>_pct`, ranked within the season among teams whose `<m>_n` reaches the gate and null under five teams. `<m>_rank` is 1 = highest, except the allowed `opp_{z}_fg_pct`, where 1 is the lowest. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | The team's ESPN team id (Int64). |
| `games` | Int64 | The team's games with a field-goal attempt by or against it. |
| `fga` | Int64 | Field-goal attempts by the team (games). |
| `fgm` | Int64 | Field goals made by the team. |
| `unlocated_fga` | Int64 | Attempts by the team with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the team at the rim (within 4 ft, the shot chart's rim zone). |
| `rim_fgm` | Int64 | Makes by the team at the rim (within 4 ft, the shot chart's rim zone). |
| `rim_ast` | Int64 | Assisted makes by the team at the rim (within 4 ft, the shot chart's rim zone). |
| `mid_fga` | Int64 | Attempts by the team in the mid zone (every other two: the chart's paint and mid-range). |
| `mid_fgm` | Int64 | Makes by the team in the mid zone (every other two: the chart's paint and mid-range). |
| `mid_ast` | Int64 | Assisted makes by the team in the mid zone (every other two: the chart's paint and mid-range). |
| `corner_3_fga` | Int64 | Attempts by the team from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `corner_3_fgm` | Int64 | Makes by the team from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `corner_3_ast` | Int64 | Assisted makes by the team from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `above_break_3_fga` | Int64 | Attempts by the team from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the team from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the team from above the break (every other three). |
| `opp_fga` | Int64 | Allowed: Field-goal attempts by the team's opponents (games). |
| `opp_fgm` | Int64 | Allowed: Field goals made by the team's opponents. |
| `opp_unlocated_fga` | Int64 | Allowed: Attempts by the team's opponents with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in opp_fga, in no zone. |
| `opp_rim_fga` | Int64 | Allowed: Attempts by the team's opponents at the rim (within 4 ft, the shot chart's rim zone). |
| `opp_rim_fgm` | Int64 | Allowed: Makes by the team's opponents at the rim (within 4 ft, the shot chart's rim zone). |
| `opp_rim_ast` | Int64 | Allowed: Assisted makes by the team's opponents at the rim (within 4 ft, the shot chart's rim zone). |
| `opp_mid_fga` | Int64 | Allowed: Attempts by the team's opponents in the mid zone (every other two: the chart's paint and mid-range). |
| `opp_mid_fgm` | Int64 | Allowed: Makes by the team's opponents in the mid zone (every other two: the chart's paint and mid-range). |
| `opp_mid_ast` | Int64 | Allowed: Assisted makes by the team's opponents in the mid zone (every other two: the chart's paint and mid-range). |
| `opp_corner_3_fga` | Int64 | Allowed: Attempts by the team's opponents from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `opp_corner_3_fgm` | Int64 | Allowed: Makes by the team's opponents from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `opp_corner_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `opp_above_break_3_fga` | Int64 | Allowed: Attempts by the team's opponents from above the break (every other three). |
| `opp_above_break_3_fgm` | Int64 | Allowed: Makes by the team's opponents from above the break (every other three). |
| `opp_above_break_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from above the break (every other three). |
| `rim_fg_pct` | Float64 | rim_fgm / rim_fga; null with no attempt. |
| `rim_freq` | Float64 | rim_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `rim_ast_pct` | Float64 | Share of rim_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `mid_fg_pct` | Float64 | mid_fgm / mid_fga; null with no attempt. |
| `mid_freq` | Float64 | mid_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `mid_ast_pct` | Float64 | Share of mid_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `corner_3_fg_pct` | Float64 | corner_3_fgm / corner_3_fga; null with no attempt. |
| `corner_3_freq` | Float64 | corner_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `corner_3_ast_pct` | Float64 | Share of corner_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `above_break_3_fg_pct` | Float64 | above_break_3_fgm / above_break_3_fga; null with no attempt. |
| `above_break_3_freq` | Float64 | above_break_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `above_break_3_ast_pct` | Float64 | Share of above_break_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_rim_fg_pct` | Float64 | Allowed: opp_rim_fgm / opp_rim_fga; null with no attempt. |
| `opp_rim_freq` | Float64 | Allowed: opp_rim_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_rim_ast_pct` | Float64 | Allowed: Share of opp_rim_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_mid_fg_pct` | Float64 | Allowed: opp_mid_fgm / opp_mid_fga; null with no attempt. |
| `opp_mid_freq` | Float64 | Allowed: opp_mid_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_mid_ast_pct` | Float64 | Allowed: Share of opp_mid_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_corner_3_fg_pct` | Float64 | Allowed: opp_corner_3_fgm / opp_corner_3_fga; null with no attempt. |
| `opp_corner_3_freq` | Float64 | Allowed: opp_corner_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_corner_3_ast_pct` | Float64 | Allowed: Share of opp_corner_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_above_break_3_fg_pct` | Float64 | Allowed: opp_above_break_3_fgm / opp_above_break_3_fga; null with no attempt. |
| `opp_above_break_3_freq` | Float64 | Allowed: opp_above_break_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_above_break_3_ast_pct` | Float64 | Allowed: Share of opp_above_break_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `rim_fg_pct_n` | Int64 | Sample size of rim_fg_pct: rim_fga. |
| `rim_freq_n` | Int64 | Sample size of rim_freq: located attempts (fga - unlocated_fga). |
| `rim_ast_pct_n` | Int64 | Sample size of rim_ast_pct: makes linked to a play-by-play row in the zone. |
| `mid_fg_pct_n` | Int64 | Sample size of mid_fg_pct: mid_fga. |
| `mid_freq_n` | Int64 | Sample size of mid_freq: located attempts (fga - unlocated_fga). |
| `mid_ast_pct_n` | Int64 | Sample size of mid_ast_pct: makes linked to a play-by-play row in the zone. |
| `corner_3_fg_pct_n` | Int64 | Sample size of corner_3_fg_pct: corner_3_fga. |
| `corner_3_freq_n` | Int64 | Sample size of corner_3_freq: located attempts (fga - unlocated_fga). |
| `corner_3_ast_pct_n` | Int64 | Sample size of corner_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `above_break_3_fg_pct_n` | Int64 | Sample size of above_break_3_fg_pct: above_break_3_fga. |
| `above_break_3_freq_n` | Int64 | Sample size of above_break_3_freq: located attempts (fga - unlocated_fga). |
| `above_break_3_ast_pct_n` | Int64 | Sample size of above_break_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_rim_fg_pct_n` | Int64 | Sample size of opp_rim_fg_pct: opp_rim_fga. |
| `opp_rim_freq_n` | Int64 | Sample size of opp_rim_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_rim_ast_pct_n` | Int64 | Sample size of opp_rim_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_mid_fg_pct_n` | Int64 | Sample size of opp_mid_fg_pct: opp_mid_fga. |
| `opp_mid_freq_n` | Int64 | Sample size of opp_mid_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_mid_ast_pct_n` | Int64 | Sample size of opp_mid_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_corner_3_fg_pct_n` | Int64 | Sample size of opp_corner_3_fg_pct: opp_corner_3_fga. |
| `opp_corner_3_freq_n` | Int64 | Sample size of opp_corner_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_corner_3_ast_pct_n` | Int64 | Sample size of opp_corner_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_above_break_3_fg_pct_n` | Int64 | Sample size of opp_above_break_3_fg_pct: opp_above_break_3_fga. |
| `opp_above_break_3_freq_n` | Int64 | Sample size of opp_above_break_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_above_break_3_ast_pct_n` | Int64 | Sample size of opp_above_break_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among teams whose rim_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among teams whose rim_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among teams whose rim_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among teams whose mid_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among teams whose mid_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among teams whose mid_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among teams whose corner_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among teams whose corner_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose corner_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among teams whose above_break_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among teams whose above_break_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose above_break_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_fg_pct_rank` | Float64 | Rank of opp_rim_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_rim_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_freq_rank` | Float64 | Rank of opp_rim_freq within the season (the highest ranks 1; ties average), among teams whose opp_rim_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_ast_pct_rank` | Float64 | Rank of opp_rim_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_rim_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_fg_pct_rank` | Float64 | Rank of opp_mid_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_mid_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_freq_rank` | Float64 | Rank of opp_mid_freq within the season (the highest ranks 1; ties average), among teams whose opp_mid_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_ast_pct_rank` | Float64 | Rank of opp_mid_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_mid_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_fg_pct_rank` | Float64 | Rank of opp_corner_3_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_corner_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_freq_rank` | Float64 | Rank of opp_corner_3_freq within the season (the highest ranks 1; ties average), among teams whose opp_corner_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_ast_pct_rank` | Float64 | Rank of opp_corner_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_corner_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_fg_pct_rank` | Float64 | Rank of opp_above_break_3_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_above_break_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_freq_rank` | Float64 | Rank of opp_above_break_3_freq within the season (the highest ranks 1; ties average), among teams whose opp_above_break_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_ast_pct_rank` | Float64 | Rank of opp_above_break_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_above_break_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_fg_pct_pct` | Float64 | Percentile of rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_freq_pct` | Float64 | Percentile of rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_ast_pct_pct` | Float64 | Percentile of rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_fg_pct_pct` | Float64 | Percentile of mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_freq_pct` | Float64 | Percentile of mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_ast_pct_pct` | Float64 | Percentile of mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_fg_pct_pct` | Float64 | Percentile of corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_freq_pct` | Float64 | Percentile of corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_ast_pct_pct` | Float64 | Percentile of corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_fg_pct_pct` | Float64 | Percentile of above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_freq_pct` | Float64 | Percentile of above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_ast_pct_pct` | Float64 | Percentile of above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_fg_pct_pct` | Float64 | Percentile of opp_rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_freq_pct` | Float64 | Percentile of opp_rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_ast_pct_pct` | Float64 | Percentile of opp_rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_fg_pct_pct` | Float64 | Percentile of opp_mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_freq_pct` | Float64 | Percentile of opp_mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_ast_pct_pct` | Float64 | Percentile of opp_mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_fg_pct_pct` | Float64 | Percentile of opp_corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_freq_pct` | Float64 | Percentile of opp_corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_ast_pct_pct` | Float64 | Percentile of opp_corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_fg_pct_pct` | Float64 | Percentile of opp_above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_freq_pct` | Float64 | Percentile of opp_above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_ast_pct_pct` | Float64 | Percentile of opp_above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.mbb.mbb_shot_zone_profiles import mbb_team_shot_zones
teams = mbb_team_shot_zones(2025)
teams.sort("opp_corner_3_freq").select("team_id", "opp_corner_3_freq").head()
```

### mbb_team_strength {#mbb_team_strength}

`mbb_team_strength(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College team strength: adjusted ratings, SOS, SOV, tempo and the four factors.

One row per team and season, with two adjusted-rating methods. `adj_*_rtg` are the
published college ratings (`load_mbb_ratings` / `load_wbb_ratings`, a fixed point on ESPN
box scores). `adj_*_rtg_ridge` are the NBA / WNBA possession ridge on the stats.ncaa.org
possessions (points per 100 against an average Division I opponent on a neutral court; home
court fitted, neutral sites from ESPN, non-Division I opponents left out). SOS is the mean
opponent `adj_net_rtg` over the team's completed ESPN games (unrated opponents left out),
SOV the same over its wins. Bracketology and the season sims keep the fixed point.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_team_strength.wbb_team_strength`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`; `team_id` is the ESPN team id (Int64). `games`, `poss` / `opp_poss`, `pts` / `opp_pts` are the season totals from the NCAA possessions; `wins` / `losses` come from ESPN's completed games. The rated metrics are `adj_off_rtg`, `adj_def_rtg`, `adj_net_rtg`, `sos`, `sov`, `adj_off_rtg_ridge`, `adj_def_rtg_ridge`, `adj_net_rtg_ridge` and the BB-D3 rates (`off_rtg`, `def_rtg`, `net_rtg`, `pace`, the four factors and the four against). Each has `<m>_rank` (1 = best; `adj_def_rtg`, `adj_def_rtg_ridge`, `def_rtg`, `tov_pct` and the opponent shooting, rebounding and free-throw rates rank low first; `sos` ranks the hardest schedule first), `<m>_pct` (the Weibull percentile) and `<m>_n` (possessions for the ratings, rated games for SOS, the clock in regulation games for pace, wins over rated opponents for SOV). Ranks are within the season, among teams with at least half the most games (which leaves out the non-D1 opponents), the BB-D3 rates' also behind their `~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate, and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id. Non-D1 opponents have none and get no row. |
| `games` | Int64 | Games with at least one possession in the season (from the possession release). |
| `wins` | Int64 | Wins in the season's completed ESPN games. |
| `losses` | Int64 | Losses in the season's completed ESPN games. |
| `poss` | Int64 | Offensive possessions in the season, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the season (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the season. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the season. |
| `adj_off_rtg` | Float64 | Adjusted offensive rating: points per 100 possessions against an average defense, from the published college ratings (adj_o, the mbb_team_ratings fixed point). |
| `adj_def_rtg` | Float64 | Adjusted defensive rating: points allowed per 100 possessions against an average offense, from the published college ratings (adj_d); lower is better. |
| `adj_net_rtg` | Float64 | Adjusted net rating: adj_off_rtg minus adj_def_rtg. |
| `sos` | Float64 | Strength of schedule: the mean adj_net_rtg of the opponents of the team's completed games, one weight per game (an opponent met four times counts four times). Opponents with no rating are left out. |
| `sov` | Float64 | Strength of victory: the mean adj_net_rtg of the opponents the team beat, one weight per win; null with no win. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Game-level, from the NCAA team box. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Game-level, from the NCAA team box. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. Game-level, from the NCAA team box. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. Game-level, from the NCAA team box. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Game-level, from the NCAA team box. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. Game-level, from the NCAA team box. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). Game-level, from the NCAA team box. |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. Game-level, from the NCAA team box. |
| `adj_off_rtg_rank` | Float64 | Rank of adj_off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_def_rtg_rank` | Float64 | Rank of adj_def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_net_rtg_rank` | Float64 | Rank of adj_net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sos_rank` | Float64 | Rank of sos within the season (the highest value ranks 1; the hardest schedule ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sov_rank` | Float64 | Rank of sov within the season (the highest value ranks 1; the strongest wins rank 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose off_rtg_n is at least 270 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose def_rtg_n is at least 460 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose net_rtg_n is at least 430 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the season, whose pace_n is at least 4 games (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose efg_pct_n is at least 500 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose tov_pct_n is at least 450 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose ft_rate_n is at least 530 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_efg_pct_n is at least 630 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_tov_pct_n is at least 340 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_orb_pct_n is at least 330 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_ft_rate_n is at least 450 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `adj_off_rtg_pct` | Float64 | Percentile of adj_off_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_def_rtg_pct` | Float64 | Percentile of adj_def_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_net_rtg_pct` | Float64 | Percentile of adj_net_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `sos_pct` | Float64 | Percentile of sos within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `sov_pct` | Float64 | Percentile of sov within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pace_pct` | Float64 | Percentile of pace within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_off_rtg_n` | Int64 | Sample size of adj_off_rtg: the team's offensive possessions in the season. |
| `adj_def_rtg_n` | Int64 | Sample size of adj_def_rtg: the team's defensive possessions in the season. |
| `adj_net_rtg_n` | Int64 | Sample size of adj_net_rtg: the team's offensive plus defensive possessions in the season. |
| `sos_n` | Int64 | Sample size of sos: the completed games against a rated opponent. |
| `sov_n` | Int64 | Sample size of sov: the wins against a rated opponent. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: the team's offensive possessions in the season. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: the team's defensive possessions in the season. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: the team's offensive plus defensive possessions in the season. |
| `pace_n` | Int64 | Sample size of pace: the game clock the season's possessions used, in regulation games (48 minutes, 40 in the WNBA and college), rounded: about the games played, overtime adding a little. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the season, 0 when the rate is null. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the NCAA team box's possessions (o_poss) in the season, 0 when the rate is null. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the season, 0 when the rate is null. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the season, 0 when the rate is null. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the season, 0 when the rate is null. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the opponents' NCAA team-box possessions in the season, 0 when the rate is null. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the season, 0 when the rate is null. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the season, 0 when the rate is null. |
| `adj_off_rtg_ridge` | Float64 | Adjusted offensive rating, possession-ridge method: points per 100 possessions against an average Division I defense on a neutral court. The NBA / WNBA ridge (possession_ridge_ratings) on the ncaa_mbb_possessions release, penalty COLLEGE_RIDGE_LAMBDA, home court fitted and ESPN neutral sites; possessions against a non-Division I opponent left out. Published beside the fixed-point adj_off_rtg, which it does not replace. |
| `adj_def_rtg_ridge` | Float64 | Adjusted defensive rating, possession-ridge method: points allowed per 100 possessions against an average Division I offense on a neutral court; lower is better. |
| `adj_net_rtg_ridge` | Float64 | Adjusted net rating, possession-ridge method: adj_off_rtg_ridge minus adj_def_rtg_ridge. |
| `adj_off_rtg_ridge_rank` | Float64 | Rank of adj_off_rtg_ridge within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_def_rtg_ridge_rank` | Float64 | Rank of adj_def_rtg_ridge within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_net_rtg_ridge_rank` | Float64 | Rank of adj_net_rtg_ridge within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_off_rtg_ridge_pct` | Float64 | Percentile of adj_off_rtg_ridge within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_def_rtg_ridge_pct` | Float64 | Percentile of adj_def_rtg_ridge within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_net_rtg_ridge_pct` | Float64 | Percentile of adj_net_rtg_ridge within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_off_rtg_ridge_n` | Int64 | Sample size of adj_off_rtg_ridge: the team's offensive possessions in the season, counting any against a non-Division I opponent that the fit leaves out. |
| `adj_def_rtg_ridge_n` | Int64 | Sample size of adj_def_rtg_ridge: the team's defensive possessions in the season, counting any against a non-Division I opponent that the fit leaves out. |
| `adj_net_rtg_ridge_n` | Int64 | Sample size of adj_net_rtg_ridge: the team's offensive plus defensive possessions in the season, counting any against a non-Division I opponent that the fit leaves out. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_strength import mbb_team_strength
strength = mbb_team_strength(2025)
strength.sort("adj_net_rtg_rank").select("team_id", "adj_net_rtg", "sos", "sov").head()

# The two methods side by side

strength.select("team_id", "adj_net_rtg_rank", "adj_net_rtg_ridge_rank").head()
```

### mbb_tournament_ineligible {#mbb_tournament_ineligible}

`mbb_tournament_ineligible(seasons: 'list[int] | None' = None, *, league: 'str | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Division I basketball programs that could not play in the NCAA tournament, per season.

A bundled, hand-curated list (2010-2027): programs reclassifying to Division I (four seasons
from Division II until 2025, three since the NCAA's January 2025 rule; St. Thomas came from
Division III), and APR, NCAA-infractions and self-imposed postseason bans. Every row cites
its source. The college season products (bracketology, season sim, strength of schedule) use
it by default to set `eligible`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int] \| None` | `None` | Seasons to keep, as the end year (`2024` is 2023-24). `None` keeps all. |
| `league` | `str \| None` | `None` | `"mens"`, `"womens"`, or `None` for both. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per ineligible `(season, league, team_id)`: `season, league, team_id` (ESPN team id, Utf8), `team`, `reason` (`reclassifying`, `apr_ban`, `infractions_ban`, `self_imposed_ban` or `other`), `detail`, `source_url` and `confidence` (`high` / `medium`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, as the end year (2024 is 2023-24). |
| `league` | String | mens or womens. |
| `team_id` | String | ESPN team id (Utf8). |
| `team` | String | School name of the barred program, as ESPN spells it. |
| `reason` | String | Why the program was barred: reclassifying, apr_ban, infractions_ban, self_imposed_ban or other. |
| `detail` | String | Short note on the row, such as the reclassification year or the case behind the ban. |
| `source_url` | String | The source that documents the row. |
| `confidence` | String | How well the source documents the row: high (the NCAA, the school or the conference says so) or medium (a secondary source). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"`, `"womens"` or `None`. |

**Example**

```python
from sportsdataverse.mbb import mbb_tournament_ineligible
barred = mbb_tournament_ineligible([2023], league="mens")

# Pipeline next step (one line)

barred.filter(pl.col("reason") == "reclassifying").get_column("team").to_list()
```

### ncaa_box_events {#ncaa_box_events}

`ncaa_box_events(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Each player's box counts per stats.ncaa.org event, the input of the stint and split box.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release order (`season`, `contest_id`, `event_team_espn_team_id`, `player_1_id`, `player_2_id`, `event_type`, `event_result`, `shot_value`). |

**Returns**

Rows in `sportsdataverse._common.game_stints.box_event_schema` (String player ids), `order` the event's row in the release (the stint rows' `order`): one per event with a count for its `player_1_id`, and one per assisted field goal for its `player_2_id`. An event with no player id or no ESPN team id (a non-D1 side) has no row.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `player_id` | String | stats.ncaa.org player id. |
| `order` | Int64 | The action's place in the game: the stint rows' order (stats.nba.com order_index; for college the event's row in the pbp release). |
| `points` | Int64 | Points the player scored: 2 or 3 per made field goal and 1 per made free throw. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goals attempted. |
| `fg3m` | Int64 | Three-pointers made. |
| `fg3a` | Int64 | Three-pointers attempted. |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throws attempted. |
| `oreb` | Int64 | Offensive rebounds (a team rebound is no player's). |
| `dreb` | Int64 | Defensive rebounds (a team rebound is no player's). |
| `ast` | Int64 | Assists: made field goals the play-by-play credits him as the passer on. |
| `stl` | Int64 | Steals he is credited with. |
| `blk` | Int64 | Shots he blocked. |
| `tov` | Int64 | Turnovers (a team turnover is no player's). |
| `pf` | Int64 | Personal fouls (technicals are not counted). |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_game_stints import ncaa_box_events
ncaa_box_events(load_ncaa_mbb_pbp(2025)).head()
```

### ncaa_coach_stints {#ncaa_coach_stints}

`ncaa_coach_stints(coaches: 'pl.DataFrame', records: 'pl.DataFrame') -> 'pl.DataFrame'`

The card coach of each team-season as a whole-season stint, unless the card says the season changed hands.

A card whose season record (`coach_season_records`) is short of the team's by more than
a game and more than a loss marks a mid-season change, and its figures are not the card
coach's own games: on Sports-Reference they are the other coach's (Ole Miss 2017-18's card
names Andy Kennedy with 1-4, Tony Madlock's five games; Kennedy went 11-16). Such a team-season
gets no stint, so it stays uncredited (and is counted in a log line). Otherwise -- the record
matches, is a loss off, drops vacated wins (never losses), or cannot be computed -- the card
coach has the whole season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `coaches` | `DataFrame` |  | `ncaa_team_coaches` rows, with the season before each wanted one for the record difference. |
| `records` | `DataFrame` |  | The teams' season records: `season`, `team_id`, `games`, `losses` (as `sportsdataverse._common.team_luck.season_records` gives them). |

**Returns**

Stints in `~sportsdataverse._common.coach_seasons.STINTS_SCHEMA` with no counts and `position="any"`, one per team-season with a card, an ESPN team id (cards with none are counted in a log line) and no mid-season change.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id of the school (from the card's NCAA team-season crosswalk). |
| `coach_id` | Int64 | NCAA person id of the card's head coach (stats.ncaa.org /people/{id}). |
| `coach` | String | Head coach's name on the card. |
| `games` | Int64 | Always null here; the card coach has the whole season (a season the card shows changing hands has no stint). |
| `wins` | Int64 | Always null here, as games. |
| `position` | String | Always any; one whole-season stint per team-season. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If two cards name a coach for the same (season, team_id). |

### ncaa_impact_rows {#ncaa_impact_rows}

`ncaa_impact_rows(pbp: 'pl.DataFrame', possessions: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp and possessions onto the game-impact row frame.

One row per event (the clock since the previous event in the period and the points scored on
it, on that event's lineup; a free throw on the lineup at the last foul) and one per
possession (its lineup and which side had it, `poss_team == home`). The clock left in a
period after its last event goes to that event's lineup, so each period's clock is whole. The
possessions of a contest with no pbp events, and those whose `poss_team` is neither side,
are dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order, with the cumulative `game_seconds` and the `period`. |
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows. |
| `league` | `str` | `'mens'` | `"mens"` (20-minute halves) or `"womens"` (10-minute quarters from 2016, halves before); overtimes are 5 minutes. |

**Returns**

Rows in `sportsdataverse._common.game_impact.impact_row_schema` with String player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the action or possession. |
| `game_id` | String | stats.ncaa.org contest id. |
| `home_team_id` | Int64 | Home ESPN team id (null for a non-D1 team). |
| `away_team_id` | Int64 | Away ESPN team id (null for a non-D1 team). |
| `home_1` | String | Home player 1 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_2` | String | Home player 2 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_3` | String | Home player 3 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_4` | String | Home player 4 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_5` | String | Home player 5 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_1` | String | Away player 1 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_2` | String | Away player 2 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_3` | String | Away player 3 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_4` | String | Away player 4 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_5` | String | Away player 5 on the floor (on a free throw, the player on the floor at the last foul). |
| `seconds` | Float64 | Game clock that ran into this action (0 on a possession row). |
| `home_pts` | Int64 | Points the home team scored on this action (0 on a possession row). |
| `away_pts` | Int64 | Points the away team scored on this action (0 on a possession row). |
| `home_poss` | Int64 | 1 on a possession row where the home team has the ball, else 0. |
| `away_poss` | Int64 | 1 on a possession row where the away team has the ball, else 0. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
rows = ncaa_impact_rows(load_ncaa_mbb_pbp(2025), load_ncaa_mbb_possessions(2025))
```

### ncaa_lineup_count_rows {#ncaa_lineup_count_rows}

`ncaa_lineup_count_rows(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp onto the lineup-seasons count frame.

Each shot, free throw, rebound and turnover counts for the side whose `event_team` it is,
on the lineup at the event; a free throw on the lineup at the most recent foul
(`sportsdataverse._common.game_impact.credit_free_throws_to_foul_lineup`). Events with
no count are dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order. |

**Returns**

One row per counted event in `sportsdataverse._common.lineup_seasons.lineup_count_schema` (with the shot zones) and String player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.ncaa.org contest id. |
| `home_team_id` | Int64 | ESPN team id of the home side. |
| `away_team_id` | Int64 | ESPN team id of the away side. |
| `home_1` | String | stats.ncaa.org id of home player 1 on the floor at the event (a free throw: at the last foul). |
| `home_2` | String | stats.ncaa.org id of home player 2 on the floor at the event (a free throw: at the last foul). |
| `home_3` | String | stats.ncaa.org id of home player 3 on the floor at the event (a free throw: at the last foul). |
| `home_4` | String | stats.ncaa.org id of home player 4 on the floor at the event (a free throw: at the last foul). |
| `home_5` | String | stats.ncaa.org id of home player 5 on the floor at the event (a free throw: at the last foul). |
| `away_1` | String | stats.ncaa.org id of away player 1 on the floor at the event (a free throw: at the last foul). |
| `away_2` | String | stats.ncaa.org id of away player 2 on the floor at the event (a free throw: at the last foul). |
| `away_3` | String | stats.ncaa.org id of away player 3 on the floor at the event (a free throw: at the last foul). |
| `away_4` | String | stats.ncaa.org id of away player 4 on the floor at the event (a free throw: at the last foul). |
| `away_5` | String | stats.ncaa.org id of away player 5 on the floor at the event (a free throw: at the last foul). |
| `home_fga` | Int64 | The home side's field-goal attempts on the event (0 or 1). |
| `home_fgm` | Int64 | The home side's field goals made on the event (0 or 1). |
| `home_fg3a` | Int64 | The home side's three-point attempts on the event (0 or 1). |
| `home_fg3m` | Int64 | The home side's threes made on the event (0 or 1). |
| `home_fta` | Int64 | The home side's free-throw attempts on the event (0 or 1). |
| `home_oreb` | Int64 | The home side's offensive rebounds on the event (0 or 1). |
| `home_dreb` | Int64 | The home side's defensive rebounds on the event (0 or 1). |
| `home_tov` | Int64 | The home side's turnovers on the event (0 or 1). |
| `home_rima` | Int64 | The home side's rim attempts on the event (0 or 1). |
| `home_rimm` | Int64 | The home side's rim makes on the event (0 or 1). |
| `home_mida` | Int64 | The home side's mid-range attempts on the event (0 or 1). |
| `home_midm` | Int64 | The home side's mid-range makes on the event (0 or 1). |
| `away_fga` | Int64 | The away side's field-goal attempts on the event (0 or 1). |
| `away_fgm` | Int64 | The away side's field goals made on the event (0 or 1). |
| `away_fg3a` | Int64 | The away side's three-point attempts on the event (0 or 1). |
| `away_fg3m` | Int64 | The away side's threes made on the event (0 or 1). |
| `away_fta` | Int64 | The away side's free-throw attempts on the event (0 or 1). |
| `away_oreb` | Int64 | The away side's offensive rebounds on the event (0 or 1). |
| `away_dreb` | Int64 | The away side's defensive rebounds on the event (0 or 1). |
| `away_tov` | Int64 | The away side's turnovers on the event (0 or 1). |
| `away_rima` | Int64 | The away side's rim attempts on the event (0 or 1). |
| `away_rimm` | Int64 | The away side's rim makes on the event (0 or 1). |
| `away_mida` | Int64 | The away side's mid-range attempts on the event (0 or 1). |
| `away_midm` | Int64 | The away side's mid-range makes on the event (0 or 1). |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows
rows = ncaa_lineup_count_rows(load_ncaa_mbb_pbp(2025))
```

### ncaa_mbb_lineups {#ncaa_mbb_lineups}

`ncaa_mbb_lineups(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-lineup stats.

Port of bigballR `get_lineups` (`all_functions.R:1945-2521`). Rows
with any missing on-court player and substitution rows are dropped, each
row's home/away five are byte-sorted so a lineup always occupies the same
columns, and the home + away passes are combined per `(p1..p5, team)`.
Ratios are derived from the summed counters, rounded to 3 decimals, and
NA/Inf are zeroed exactly where R does it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`parse_ncaa_bb_game_pbp` output). May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the `_trans`/`_half` split surface plus `o_trans_pct`/`d_trans_pct` (213 columns total). |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2012`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per lineup+team — 75 columns (`LINEUPS_COLUMNS`) or 213 with `include_transition=True` (`LINEUPS_TRANSITION_COLUMNS`), rows sorted by `p1..p5, team`. Empty input yields an empty frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `p1` | String |  |
| `p2` | String |  |
| `p3` | String |  |
| `p4` | String |  |
| `p5` | String |  |
| `team` | String | Team-side label or team identifier. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_lineups
lineups = ncaa_mbb_lineups(pbp)
print(lineups.shape)

# Transition/half-court splits, pandas out

df_pd = ncaa_mbb_lineups(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

lineups.filter(pl.col("mins") > 10).sort("netrtg", descending=True).head()
```

### ncaa_mbb_on_off {#ncaa_mbb_on_off}

`ncaa_mbb_on_off(players: 'Union[str, Sequence[str]]', lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every on/off combination of the given players.

Port of bigballR `on_off_generator` (`all_functions.R:2555-2749`,
`include_transition=F` path). For k players, all `2^k` on/off
assignments are enumerated in R's `expand.grid` order (first player
varies fastest; first row all-On, last all-Off); each combination sums
the lineups whose membership matches exactly, re-derives the full ratio
block (including `e_poss`), rounds to 3 decimals, and zeroes NA/Inf.
Combinations matching zero lineups produce an all-zero row.

Faithful R quirk (kept, flagged): when `included`/`excluded` is
passed, the base lineup set comes from the membership filter ONLY — the
inferred-team filter is skipped (`all_functions.R:2584-2589`), so an
included player on another team would leak that team's lineups in.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[str, Sequence[str]]` |  | Player name(s) to split on (the `Status` axis). |
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with `2^k` rows — `status` (e.g. `"A.PLAYER On | B.PLAYER Off"`) + the 69 stat columns (`ON_OFF_COLUMNS`).

| col_name | type | description |
|---|---|---|
| `status` | String | Status label. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the players' team cannot be uniquely inferred from the lineups (R's `"ERROR- Player team not found"`). |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_on_off
split = ncaa_mbb_on_off("KEATON.WAGLER", lineups)
print(split.shape)

# Two-player interaction

duo = ncaa_mbb_on_off(["A.PLAYER", "B.PLAYER"], lineups)

# Pipeline next step (one line)

split.select("status", "netrtg")
```
