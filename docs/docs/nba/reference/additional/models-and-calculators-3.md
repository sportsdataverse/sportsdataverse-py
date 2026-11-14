---
title: "NBA — additional Python functions — Models and calculators: nba_player–ot_adjusted"
sidebar_label: "Models and calculators: nba_player–ot_adjusted"
sidebar_position: 13
description: "NBA — additional Python functions — Models and calculators: nba_player–ot_adjusted — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: nba_player–ot_adjusted

### nba_player_impact_context {#nba_player_impact_context}

`nba_player_impact_context(impact: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Append `<m>_n`, `<m>_rank` and `<m>_pct` to a `player_impact` table.

Ranks are computed within `(season, season_type)` over the players whose `<m>_n` reaches
the metric's gate (module docstring); a playoff run rarely reaches the regular-season gates,
so most playoff ranks are null. A row's rank and percentile are also null when its metric is
null or non-finite, when its `season` or `season_type` is null, when fewer than
`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` players qualify in the
group, or when every qualifying value in the group is equal. Nothing is dropped, and running
it on its own output gives the same frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `impact` | `DataFrame` |  | A `player_impact` table: `season`, `season_type`, `off_poss`, `def_poss` and the 16 metrics of `IMPACT_RANKED`, one row per player, season and season type. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA, which picks the sample gates. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`impact` with `<m>_n` (Int64), then `<m>_rank` and `<m>_pct` (Float64) appended for every metric of `IMPACT_RANKED`, in that order.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | String | Nested list of member-team membership spans. |
| `season` | Int64 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `o_rapm` | Float64 | Offensive regularized adjusted plus-minus per 100 possessions from the single-season ridge fit over possession-level lineup indicators, at the fixed NBA penalty described under rapm; positive means the player raised his team's scoring rate while on offense. |
| `d_rapm` | Float64 | Defensive regularized adjusted plus-minus per 100 possessions, negated from the raw points-allowed coefficient so a positive value marks a defender who suppresses opponent scoring; fit at the fixed NBA penalty described under rapm. |
| `rapm` | Float64 | Total regularized adjusted plus-minus per 100 possessions, exactly the sum of o_rapm and d_rapm. The ridge penalty is one fixed NBA value (2,683, sportsdataverse.nba.nba_rapm.RAPM_PENALTY) in every season and season type, so the scale compares across seasons (regular-season sd 1.53 to 1.99 per 100 over players with 400 possessions, 1997-2026; a shorter season, such as the 1999 lockout (1.53), shrinks harder, so part of a spread difference is sample size). Seasons published before November 2026 were fit at a per-season RidgeCV pick (1,931 or 5,179 in the regular season, up to 37,276 in the playoffs), and their spread jumps with it (sd 1.15 to 2.34): do not compare those values across seasons or with values fit at the fixed penalty. |
| `off_poss` | Int64 | Number of possessions the player was on the floor on offense, the count of design-matrix rows carrying his offensive indicator and therefore the offensive-side sample size behind o_rapm. |
| `def_poss` | Int64 | Number of possessions the player was on the floor on defense, the sample size behind d_rapm; it tracks off_poss almost exactly because substitutions rarely split an offense-defense pair. |
| `o_adj_rapm` | Float64 | Offensive prior-informed RAPM per 100 possessions: the ridge fit shrunk toward the player's box-score (SPM) prior. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025-26 +0.32). A player with no prior (a rookie, or no previous-season SPM) is shrunk toward 0 instead. Releases fit before #1137, whatever their build date, fit no intercept, so the league's points per possession leaked into every coefficient: o_adj_rapm averaged about +12.03 and d_adj_rapm -10.84 (2025-26), a thinly-used player kept less of that offset, and only their sum, adj_rapm, read on a common scale. |
| `d_adj_rapm` | Float64 | Defensive prior-informed RAPM per 100 possessions, positive for a defender who lowers opponent scoring. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025-26 +0.06). Releases fit before #1137, whatever their build date, averaged about -10.84 here (see o_adj_rapm). |
| `adj_rapm` | Float64 | Total prior-informed RAPM per 100 possessions, exactly the sum of o_adj_rapm and d_adj_rapm. Releases fit with sportsdataverse-py #1137 or later fit the ridge with an unpenalized intercept; releases fit before it (whatever their build date) fit none, which leaned this total toward heavy-minute players. Releases fit with #1150 or later also use one ridge penalty per league (NBA 3,728, WNBA 2,683) and the SPM prior doubled (nba_adj_rapm.ADJ_RAPM_PENALTY / ADJ_RAPM_PRIOR_SCALE); earlier ones took a per-season RidgeCV pick, whose spread collapsed in some seasons (WNBA 2004 sd 0.64 against plain RAPM's 1.47). |
| `ospm` | Float64 | Offensive statistical plus-minus per 100 possessions: the player's per-100 box-score feature vector scored through ridge coefficients trained on that season's o_rapm target. |
| `dspm` | Float64 | Defensive statistical plus-minus per 100 possessions from the same box-score feature vector scored through coefficients trained on the d_rapm target. |
| `spm` | Float64 | Total statistical plus-minus per 100 possessions, exactly the sum of ospm and dspm. |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |
| `obpm` | Float64 | Offensive box plus-minus per 100 possessions for this season and season type only, team-adjusted on its own so a team's minute-weighted obpm is its offensive rating minus the league's, divided by 5 (a traded player counted with his first team; league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 put the whole team adjustment here (regular-season league minute-weighted mean about -2.9). |
| `dbpm` | Float64 | Defensive box plus-minus per 100 possessions for this season and season type only: bpm minus obpm, so a team's minute-weighted dbpm is (the league's offensive rating minus its defensive rating) / 5, positive for a good defense (league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 left it unadjusted (regular-season league minute-weighted mean about +2.9, mostly positive). |
| `bpm` | Float64 | Box plus-minus (BPM 2.0) per 100 possessions for this season and season type only, not a career value: the box-score estimate of total impact. The team adjustment makes a team's minute-weighted bpm its net rating per 100 divided by 5 (the five %Min slots sum to the net rating; a traded player's minutes count with the first team he appears for in the logs). Releases fit before sportsdataverse-py #1148 targeted the full net rating, five times BPM 2.0's team mean. |
| `war` | Float64 | Wins above replacement, computed as (rapm minus a replacement level of -2.0 per 100) times total possessions divided by 100, divided by a points-per-win constant calibrated each season by regressing team wins on full-season point margin. |
| `boban_filtered_skill` | Float64 | BOBAN Kalman-filtered skill estimate at the end of the player's observed multi-season RAPM panel, i.e. his current-form rating after aging drift and possession-weighted observation noise. |
| `boban_projected_rating` | Float64 | One-season-ahead BOBAN forecast, the filtered skill plus the empirical aging-curve drift at the player's last observed age; both season_type rows of a player-season carry the same value because the projection is not playoff-specific. |
| `boban_projected_sd` | Float64 | Standard deviation of the one-season-ahead BOBAN forecast, the square root of the filtered state variance plus the Kalman process variance; it sits at roughly 10.19 for a player with only one season in the panel, whose diffuse prior variance was never updated. |
| `darko_filtered_skill` | Float64 | Deprecated alias of boban_filtered_skill, kept for one release; use that column. |
| `darko_projected_rating` | Float64 | Deprecated alias of boban_projected_rating, kept for one release; use that column. |
| `darko_projected_sd` | Float64 | Deprecated alias of boban_projected_sd, kept for one release; use that column. |
| `o_rapm_n` | Int64 | Possessions o_rapm is measured over: off_poss, his offensive possessions. o_rapm_rank and o_rapm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_rapm_n` | Int64 | Possessions d_rapm is measured over: def_poss, his defensive possessions. d_rapm_rank and d_rapm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `rapm_n` | Int64 | Possessions rapm is measured over: off_poss + def_poss, his possessions at both ends. rapm_rank and rapm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `o_adj_rapm_n` | Int64 | Possessions o_adj_rapm is measured over: off_poss, his offensive possessions. o_adj_rapm_rank and o_adj_rapm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_adj_rapm_n` | Int64 | Possessions d_adj_rapm is measured over: def_poss, his defensive possessions. d_adj_rapm_rank and d_adj_rapm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `adj_rapm_n` | Int64 | Possessions adj_rapm is measured over: off_poss + def_poss, his possessions at both ends. adj_rapm_rank and adj_rapm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `ospm_n` | Int64 | Possessions ospm is measured over: off_poss, his offensive possessions. ospm_rank and ospm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dspm_n` | Int64 | Possessions dspm is measured over: def_poss, his defensive possessions. dspm_rank and dspm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `spm_n` | Int64 | Possessions spm is measured over: off_poss + def_poss, his possessions at both ends. spm_rank and spm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `obpm_n` | Int64 | Possessions obpm is measured over: off_poss, his offensive possessions. obpm_rank and obpm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dbpm_n` | Int64 | Possessions dbpm is measured over: def_poss, his defensive possessions. dbpm_rank and dbpm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `bpm_n` | Int64 | Possessions bpm is measured over: off_poss + def_poss, his possessions at both ends. bpm_rank and bpm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `war_n` | Int64 | Possessions behind war: off_poss + def_poss. war is a count, not a rate, so its rank has no sample gate. |
| `boban_filtered_skill_n` | Int64 | Possessions behind boban_filtered_skill: off_poss + def_poss, this season's possessions at both ends. The boban filtered skill is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_filtered_skill_rank and boban_filtered_skill_pct are set only when this reaches the league's gate (3,200 NBA, 1,600 WNBA). Conservative, since the filter also carries earlier seasons. |
| `boban_projected_rating_n` | Int64 | Possessions behind boban_projected_rating: off_poss + def_poss, this season's possessions at both ends. The boban next-season projection is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_projected_rating_rank and boban_projected_rating_pct are set only when this reaches the league's gate (3,200 NBA, 1,600 WNBA). Conservative, since the filter also carries earlier seasons. |
| `boban_projected_sd_n` | Int64 | Possessions behind boban_projected_sd: off_poss + def_poss, this season's possessions at both ends. The boban projection's standard deviation is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_projected_sd_rank and boban_projected_sd_pct are set only when this reaches the league's gate (3,200 NBA, 1,600 WNBA). Conservative, since the filter also carries earlier seasons. |
| `o_rapm_rank` | Float64 | Rank of o_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_rapm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_rapm_rank` | Float64 | Rank of d_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_rapm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `rapm_rank` | Float64 | Rank of rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when rapm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `o_adj_rapm_rank` | Float64 | Rank of o_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_adj_rapm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_adj_rapm_rank` | Float64 | Rank of d_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_adj_rapm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `adj_rapm_rank` | Float64 | Rank of adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when adj_rapm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `ospm_rank` | Float64 | Rank of ospm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when ospm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dspm_rank` | Float64 | Rank of dspm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dspm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `spm_rank` | Float64 | Rank of spm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when spm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `obpm_rank` | Float64 | Rank of obpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when obpm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dbpm_rank` | Float64 | Rank of dbpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dbpm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `bpm_rank` | Float64 | Rank of bpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when bpm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `war_rank` | Float64 | Rank of war among the season and season type's ranked players, 1 = best (higher is better), ties averaged; ungated (every player with a finite war qualifies), null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. |
| `boban_filtered_skill_rank` | Float64 | Rank of boban_filtered_skill among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when boban_filtered_skill_n is under the league's gate (3,200 NBA, 1,600 WNBA), when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `boban_projected_rating_rank` | Float64 | Rank of boban_projected_rating among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when boban_projected_rating_n is under the league's gate (3,200 NBA, 1,600 WNBA), when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `boban_projected_sd_rank` | Float64 | Rank of boban_projected_sd among the season and season type's ranked players, ASCENDING: 1 = the smallest sd, the most certain projection; ties averaged; null when boban_projected_sd_n is under the league's gate (3,200 NBA, 1,600 WNBA), when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `o_rapm_pct` | Float64 | Percentile of o_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `d_rapm_pct` | Float64 | Percentile of d_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `rapm_pct` | Float64 | Percentile of rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `o_adj_rapm_pct` | Float64 | Percentile of o_adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `d_adj_rapm_pct` | Float64 | Percentile of d_adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `adj_rapm_pct` | Float64 | Percentile of adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `ospm_pct` | Float64 | Percentile of ospm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `dspm_pct` | Float64 | Percentile of dspm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `spm_pct` | Float64 | Percentile of spm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `obpm_pct` | Float64 | Percentile of obpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `dbpm_pct` | Float64 | Percentile of dbpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `bpm_pct` | Float64 | Percentile of bpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `war_pct` | Float64 | Percentile of war_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_filtered_skill_pct` | Float64 | Percentile of boban_filtered_skill_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_projected_rating_pct` | Float64 | Percentile of boban_projected_rating_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_projected_sd_pct` | Float64 | Percentile of boban_projected_sd_rank, 100 * (n + 1 - rank) / (n + 1): high means a more certain projection (a smaller sd), not a larger one; null where the rank is. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |
| `KeyError` | If a metric, `season`, `season_type`, `off_poss` or `def_poss` is missing. |

**Example**

```python
from sportsdataverse.nba import load_nba_player_impact, nba_player_impact_context
ranked = nba_player_impact_context(load_nba_player_impact(2026))
ranked.sort("adj_rapm_rank").select("player_name", "adj_rapm", "adj_rapm_rank", "adj_rapm_pct").head()
```

### nba_player_impact_league_averages {#nba_player_impact_league_averages}

`nba_player_impact_league_averages(impact: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The league-average rows of a `player_impact` table, one block per season type.

The football F1 rows of `~sportsdataverse._common.league_context.league_averages` over
the players who pass each metric's sample gate, so `n` is that metric's qualified cohort:
the players with a finite value and enough possessions. It is reported even where
`nba_player_impact_context` leaves the ranks null (fewer than 10 qualifiers, or all
equal). `category` is `player_impact_` plus the `season_type` lowercased with spaces as
underscores: `player_impact_regular_season` and `player_impact_playoffs` for the releases'
two season types. `qualifier_min` is null (the gates differ by metric).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `impact` | `DataFrame` |  | A `player_impact` table, as `nba_player_impact_context` takes. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA: the `level` label and the sample gates. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per `(season, season type, metric)` in `~sportsdataverse._common.league_context.LEAGUE_AVERAGES_SCHEMA`; an empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the league-average row, as the END year the player_impact table stamps (2026 = 2025-26). |
| `level` | String | Population the row describes: nba or wnba (the league_id passed). |
| `entity` | String | Kind of row summarized: always player for the impact table. |
| `category` | String | Table and season type the row summarizes: player_impact_regular_season or player_impact_playoffs. |
| `metric` | String | Impact metric the row summarizes, one of the 13 ranked columns (o_rapm through war). |
| `mean` | Float64 | Unweighted mean of the metric over the season type's ranked players, those whose <metric>_n reaches its sample gate. |
| `median` | Float64 | Median of the metric over the same ranked players as mean. |
| `sd` | Float64 | Sample standard deviation (ddof 1) of the metric over the same ranked players; null with one player. |
| `n` | Int64 | Number of ranked players with a finite value: the n of the <metric>_pct formula, not the possession count <metric>_n. |
| `qualifier_min` | Float64 | Always null here: each metric has its own possession gate rather than one table-wide threshold. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |
| `KeyError` | If a metric, `season`, `off_poss` or `def_poss` is missing. |

**Example**

```python
from sportsdataverse.nba import load_nba_player_impact, nba_player_impact_league_averages
nba_player_impact_league_averages(load_nba_player_impact(2026))
```

### nba_player_stats_advanced_context {#nba_player_stats_advanced_context}

`nba_player_stats_advanced_context(adv: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Replace `player_stats_advanced`'s API ranks of the rated measures with gated producer ranks.

For each measure of `ADVANCED_RANKED`, `<m>_rank` becomes the rank among the players of
its `(season, season_type, per_mode)` whose `poss` reaches `advanced_min_poss`, and
`<m>_pct` is appended (module docstring). A row's rank and percentile are null when it is
under the gate, when its value is null or non-finite, when a group key is null, when fewer than
`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` players qualify or every
qualifying value is equal. Nothing is dropped, and running it on its own output gives the same
frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adv` | `DataFrame` |  | A `player_stats_advanced` table (`load_nba_stats_leaguedash("player_stats_advanced")` / `load_wnba_stats_leaguedash`): `season`, `season_type`, `per_mode`, `poss` and the measures of `ADVANCED_RANKED`. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA, which picks the gate. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`adv` with each ranked measure's `<m>_rank` replaced in place (Float64; ties share the average rank) and its `<m>_pct` (Float64) appended, in `ADVANCED_RANKED` order.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `age` | Float64 | Player age (in years). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `e_off_rating` | Float64 | Estimated offensive rating: points produced per 100 possessions on the stats API's estimated-possession formula. |
| `off_rating` | Float64 | Offensive rating: points scored per 100 possessions while the player is on the floor. |
| `sp_work_off_rating` | Float64 | The stats API's SP_WORK copy of off_rating (the same measure under its alternate column set). |
| `e_def_rating` | Float64 | Estimated defensive rating: points allowed per 100 possessions on the estimated-possession formula. |
| `def_rating` | Float64 | Defensive rating: points allowed per 100 possessions while the player is on the floor. |
| `sp_work_def_rating` | Float64 | The stats API's SP_WORK copy of def_rating (the same measure under its alternate column set). |
| `e_net_rating` | Float64 | Estimated net rating: e_off_rating minus e_def_rating. |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `sp_work_net_rating` | Float64 | The stats API's SP_WORK copy of net_rating (the same measure under its alternate column set). |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 | Assist-to-turnover ratio: assists divided by turnovers. |
| `ast_ratio` | Float64 | Assist ratio: assists per 100 possessions the player used. |
| `oreb_pct` | Float64 | Offensive rebound percentage: share of available offensive rebounds the player grabbed while on the floor. |
| `dreb_pct` | Float64 | Defensive rebound percentage: share of available defensive rebounds the player grabbed while on the floor. |
| `reb_pct` | Float64 | Rebound percentage: share of all available rebounds the player grabbed while on the floor. |
| `tm_tov_pct` | Float64 | Turnover percentage: turnovers per 100 possessions the player used. |
| `e_tov_pct` | Float64 | Estimated turnover percentage on the stats API's estimated-possession formula. |
| `efg_pct` | Float64 | Effective field-goal percentage: (fgm + 0.5 * fg3m) / fga. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `usg_pct` | Float64 | Usage percentage: share of the team's possessions the player used while on the floor. |
| `e_usg_pct` | Float64 | Estimated usage percentage on the stats API's estimated-possession formula. |
| `e_pace` | Float64 | Estimated pace: possessions per regulation game on the estimated-possession formula. |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `sp_work_pace` | Float64 | The stats API's SP_WORK copy of pace (the same measure under its alternate column set). |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `poss` | Int64 | Poss. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fgm_pg` | Float64 | Field goals made per game. |
| `fga_pg` | Float64 | Field goals attempted per game. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `gp_rank` | Int64 | The stats API's own rank of gp among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `w_rank` | Int64 | The stats API's own rank of w among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `l_rank` | Int64 | The stats API's own rank of l among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `w_pct_rank` | Int64 | The stats API's own rank of w_pct among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `min_rank` | Int64 | The stats API's own rank of min among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `e_off_rating_rank` | Int64 | The stats API's own rank of e_off_rating among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `off_rating_rank` | Float64 | Producer rank of off_rating (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `sp_work_off_rating_rank` | Int64 | The stats API's own rank of sp_work_off_rating among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `e_def_rating_rank` | Int64 | The stats API's own rank of e_def_rating among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `def_rating_rank` | Float64 | Producer rank of def_rating (lowest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `sp_work_def_rating_rank` | Int64 | The stats API's own rank of sp_work_def_rating among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `e_net_rating_rank` | Int64 | The stats API's own rank of e_net_rating among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `net_rating_rank` | Float64 | Producer rank of net_rating (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `sp_work_net_rating_rank` | Int64 | The stats API's own rank of sp_work_net_rating among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `ast_pct_rank` | Float64 | Producer rank of ast_pct (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `ast_to_rank` | Float64 | Producer rank of ast_to (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `ast_ratio_rank` | Int64 | The stats API's own rank of ast_ratio among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `oreb_pct_rank` | Float64 | Producer rank of oreb_pct (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `dreb_pct_rank` | Float64 | Producer rank of dreb_pct (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `reb_pct_rank` | Float64 | Producer rank of reb_pct (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `tm_tov_pct_rank` | Float64 | Producer rank of tm_tov_pct (lowest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `e_tov_pct_rank` | Int64 | The stats API's own rank of e_tov_pct among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `efg_pct_rank` | Float64 | Producer rank of efg_pct (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `ts_pct_rank` | Float64 | Producer rank of ts_pct (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `usg_pct_rank` | Float64 | Producer rank of usg_pct (heaviest usage first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `e_usg_pct_rank` | Int64 | The stats API's own rank of e_usg_pct among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `e_pace_rank` | Int64 | The stats API's own rank of e_pace among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `pace_rank` | Int64 | The stats API's own rank of pace among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `sp_work_pace_rank` | Int64 | The stats API's own rank of sp_work_pace among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `pie_rank` | Float64 | Producer rank of pie (highest first) within season, season_type and per_mode, among players with poss of at least 1,600 (WNBA 800); null under the gate. Replaces the API's rank, which ranked every player. |
| `fgm_rank` | Int64 | The stats API's own rank of fgm among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `fga_rank` | Int64 | The stats API's own rank of fga among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `fgm_pg_rank` | Int64 | The stats API's own rank of fgm_pg among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `fga_pg_rank` | Int64 | The stats API's own rank of fga_pg among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `fg_pct_rank` | Int64 | The stats API's own rank of fg_pct among every player in the table (1 is the leader); not gated, unlike the producer ranks of the rated measures. |
| `team_count` | Int64 | Number of teams the player's row covers (the stats API's TEAM_COUNT; more than 1 after a trade). |
| `season` | Int64 | Season year. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `per_mode` | String | The stats API PerMode the row was requested with; the producer ranks are computed within each mode. |
| `off_rating_pct` | Float64 | Percentile of off_rating among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `def_rating_pct` | Float64 | Percentile of def_rating among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `net_rating_pct` | Float64 | Percentile of net_rating among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `ast_pct_pct` | Float64 | Percentile of ast_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `ast_to_pct` | Float64 | Percentile of ast_to among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `oreb_pct_pct` | Float64 | Percentile of oreb_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `dreb_pct_pct` | Float64 | Percentile of dreb_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `reb_pct_pct` | Float64 | Percentile of reb_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `tm_tov_pct_pct` | Float64 | Percentile of tm_tov_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `ts_pct_pct` | Float64 | Percentile of ts_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `usg_pct_pct` | Float64 | Percentile of usg_pct among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |
| `pie_pct` | Float64 | Percentile of pie among the same ranked players: 100 * (n + 1 - rank) / (n + 1); null where the rank is. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |
| `KeyError` | If a measure, `season`, `season_type`, `per_mode` or `poss` is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import load_nba_stats_leaguedash, nba_player_stats_advanced_context
adv = nba_player_stats_advanced_context(load_nba_stats_leaguedash("player_stats_advanced", seasons=2026))
adv.filter(pl.col("season_type") == "Regular Season").sort("ts_pct_rank").select(
    "player_name", "poss", "ts_pct", "ts_pct_rank", "ts_pct_pct"
).head()
```

### nba_predict_games {#nba_predict_games}

`nba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home/away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` / `predict_total`
math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace` (the `~sportsdataverse.nba.nba_team_ratings.nba_team_ratings` output for one season/as-of date). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

No returns table is published for this function: no capture: it raises a join-key dtype mismatch on a real schedule (the schedule's home_team_id is Int32, nba_team_ratings' team_id is String).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a join-key dtype mismatches `ratings['team_id']`, or if `ratings` has duplicate `team_id` rows (e.g. multiple seasons passed at once). |

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
preds = nba_predict_games(games, nba_team_ratings(2024))
```

### nba_reml_rapm {#nba_reml_rapm}

`nba_reml_rapm(possessions: 'pl.DataFrame', prior: 'dict[int, tuple[float, float]]', *, penalty: 'str' = 'fixed', lambdas: 'tuple[float, float] | None' = None, league_id: 'str | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season impact rating: RAPM shrunk toward a box prior, at a fixed league penalty or one chosen by REML.

The posterior mean of a prior-informed RAPM (the same design and sign convention as
`~sportsdataverse.nba.nba_rapm.nba_rapm` and `~sportsdataverse.nba.nba_adj_rapm.nba_adj_rapm`).
The prior is used as given (no scale multiplier). Two penalty rules:

* `penalty="fixed"` (the default): the league's adjusted-RAPM penalty,
  `~sportsdataverse.nba.nba_adj_rapm.ADJ_RAPM_PENALTY` (NBA 3,728), on offense and defense.
* `penalty="reml"`: one offense and one defense penalty chosen per call by restricted maximum
  likelihood (`fit_reml_prior_ridge`). REML's penalty is the ratio of the per-possession noise to
  how far players sit from their prior; with the v2 box prior it puts 90% of the weight on the prior on
  offense and 81% on defense (possession-weighted), and offense reaches the upper bound in 2018, 2019
  and 2023.

Why fixed is the default: measured by `tools/fit_box_prior_v2.py` on NBA regular seasons 1998-2026
with the v2 box prior, against today's `~sportsdataverse.nba.nba_adj_rapm.nba_adj_rapm` on the
same held-out possessions, the fixed rule lowers the error by 1.64 bp of MSE (SE 0.10, better in all 29
seasons) and by 1.43 bp after a mid-season cut date (2006-2026, SE 0.29, 17 of 21); REML by 0.73
(SE 0.17, 22 of 29) and 1.03 (SE 0.49, 14 of 21). Head to head the fixed rule beats the REML pick by
0.91 bp on held-out games (SE 0.14, 27 of 29 seasons). The 3,728 was cross-validated on these seasons for
the previous-season SPM prior, so refitting it for the v2 prior is open. The cut-date test counts 2006+
only (1998-2005 priors learn from later seasons); height and position come from one later index.

Built for the box prior of `sportsdataverse.nba.nba_box_prior` (`o_prior` / `d_prior` of
`~sportsdataverse.nba.nba_box_prior.apply_box_prior`), but any per-100 `(o, d)` prior works.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | A possession+lineup frame (`points`, `off_player_1..5`, `def_player_1..5`). |
| `prior` | `dict[int, tuple[float, float]]` |  | `{player_id: (o_prior, d_prior)}` per 100 possessions (`d` positive = good defense). A player absent from it gets `(0, 0)`: a league-average prior. |
| `penalty` | `str` | `'fixed'` | `"fixed"` (the league's adjusted-RAPM penalty) or `"reml"` (chosen per call). |
| `lambdas` | `tuple[float, float] \| None` | `None` | `(lambda_o, lambda_d)` to fit at; overrides `penalty`. |
| `league_id` | `str \| None` | `None` | `"00"`, `"10"` or `"20"`: the league whose fixed penalty is used. `None` reads it from the `game_id` prefix (NBA without one). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player, columns `REML_RAPM_SCHEMA`: `o_reml_rapm` / `d_reml_rapm` / `reml_rapm` (per 100, `d` positive = good defense, `reml_rapm = o + d`), the prior used (`o_box_prior` / `d_box_prior`: whatever `prior` held, the box prior or another), the weight of a player's own prior in his posterior mean (`o_prior_weight` / `d_prior_weight`, 0 to 1; see `RemlFit`), possession counts, and the chosen penalties (`lambda_o` / `lambda_d`, the same on every row). Zero rows with that schema when no possession has a full lineup.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | NBA stats person id. |
| `o_reml_rapm` | Float64 | Offensive season impact per 100: the posterior mean of RAPM shrunk toward the prior at lambda_o (the league fixed penalty by default, REML with penalty="reml", or the lambdas= given). |
| `d_reml_rapm` | Float64 | Defensive season impact per 100 (positive = good defense) at lambda_d (the league fixed penalty by default, REML with penalty="reml", or the lambdas= given). |
| `reml_rapm` | Float64 | o_reml_rapm + d_reml_rapm. |
| `o_box_prior` | Float64 | The offensive prior the rating shrank toward (per 100; the box prior v2 when built by tools/fit_box_prior_v2.py). |
| `d_box_prior` | Float64 | The defensive prior the rating shrank toward (per 100, positive = good defense). |
| `o_prior_weight` | Float64 | Weight of the player's own offensive prior in his posterior mean, 0 to 1 (diagonal of M^-1 diag(lambda)). |
| `d_prior_weight` | Float64 | Weight of the player's own defensive prior in his posterior mean, 0 to 1. |
| `off_poss` | Int64 | Offensive possessions in the fit. |
| `def_poss` | Int64 | Defensive possessions in the fit. |
| `lambda_o` | Float64 | Offense penalty the fit used (points-per-possession scale; ADJ_RAPM_PENALTY by default, REML with penalty="reml", or the lambdas= given; the same on every row). |
| `lambda_d` | Float64 | Defense penalty the fit used (ADJ_RAPM_PENALTY by default, REML with penalty="reml", or the lambdas= given; the same on every row). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `penalty` is not `"fixed"` or `"reml"`. |

**Example**

```python
from sportsdataverse.nba import nba_reml_rapm
prior = {r["player_id"]: (r["o_prior"], r["d_prior"]) for r in priors.iter_rows(named=True)}
rating = nba_reml_rapm(possessions, prior)  # fixed penalty
by_reml = nba_reml_rapm(possessions, prior, penalty="reml")
print(rating.sort("reml_rapm", descending=True).head())
```

### nba_rookie_projection {#nba_rookie_projection}

`nba_rookie_projection(draft_year: "'int | list[int]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project rookie/sophomore value by composing draft x aging x availability.

Composition (no re-derived features -- each term is the verbatim public
output of ①②③):

- `base = nba_draft_model(...).proj_career_value * rookie_fraction`
  (`rookie_fraction` from the bundled residual artifact -- the share of
  career value realized in a single rookie season).
- `proj_rookie_value = base * rel_value(rookie_age) / rel_value(peak_age)
  + residual[pro_tier]`; `proj_soph_value` uses `rookie_age + 1`.
- `proj_avail_pct` from `sportsdataverse.nba.nba_availability.nba_availability`
  at rookie age -- reported separately, **never** multiplied into the
  value columns (availability is availability, not skill).
- `proj_rookie_min = games_full_season * proj_avail_pct * expected_mpg(pro_tier)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional college-side prior frame, forwarded verbatim to `sportsdataverse.nba.nba_draft_model.nba_draft_model`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64, proj_soph_value:Float64, proj_rookie_min:Float64, proj_avail_pct:Float64, pro_tier:Utf8`. Empty input -> zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_rookie_projection
board = nba_rookie_projection(2019)
print(board.sort("proj_rookie_value", descending=True).head())
```

### nba_spm {#nba_spm}

`nba_spm(box_features: 'pl.DataFrame', coefficients: 'SpmCoefficients', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Apply fitted SPM coefficients to per-100 box features -> OSPM/DSPM/SPM.

Applies a linear scoring rule:

.. code-block:: text

    ospm = X @ o_coef + o_intercept
    dspm = X @ d_coef + d_intercept
    spm  = ospm + dspm

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id`, every column in `coefficients.feature_names`, `min`, and `gp`. |
| `coefficients` | `SpmCoefficients` |  | A `SpmCoefficients` instance from `train_spm`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Per-player frame with columns `player_id` (Int64), `ospm` (Float64), `dspm` (Float64), `spm` (Float64), `min` (Float64), `gp` (Int64).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `ospm` | Float64 |  |
| `dspm` | Float64 |  |
| `spm` | Float64 |  |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |

**Example**

```python
from sportsdataverse.nba import nba_spm
ratings = nba_spm(box_feats, coef)
print(ratings.sort("spm", descending=True).head())

# Pipeline next step

ratings.filter(pl.col("min") >= 500).sort("spm", descending=True)
```

### nba_team_ratings {#nba_team_ratings}

`nba_team_ratings(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Opponent-adjusted team ratings (AdjOffRtg/AdjDefRtg/AdjNet/AdjPace), as-of-date aware.

Loads schedule + team box score for `seasons`, optionally filters to
games strictly before `as_of_date` (the leakage boundary, via
`~sportsdataverse.nba.nba_prediction_constants.as_of_ratings_split`),
computes per-game efficiency, runs the opponent-adjustment fixed points,
and adds a per-season dense `rank` (on `adj_net_rtg` descending) and
`adj_net_z` (z-score of `adj_net_rtg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | If given, only games with `date < as_of_date` are used (predictive/backtest usage); `None` computes full-season descriptive ratings. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg, raw_pace, games, rank, adj_net_z`, then `<m>_rank`, `<m>_pct` and `<m>_n` for each of the seven ratings (BB-D2, the football F5/F6 grammar of `sportsdataverse._common.league_context`). `_rank` is 1 for the best team in the season: the highest offensive and net ratings, the lowest `*_def_rtg`, the fastest pace. `_pct` is the Weibull percentile. Both are null for a team under half the season's most games, and for every team when fewer than five qualifying teams have the rating or they all share one value. On a full season the half-games rule drops the All-Star teams; with `as_of_date` the most games is taken before the cut, so an early cut can also null regular teams. `_n` counts the sample: possessions (rounded) for the ratings, games for the paces. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `adj_off_rtg` | Float64 |  |
| `adj_def_rtg` | Float64 |  |
| `adj_net_rtg` | Float64 |  |
| `adj_pace` | Float64 |  |
| `raw_off_rtg` | Float64 |  |
| `raw_def_rtg` | Float64 |  |
| `raw_pace` | Float64 |  |
| `games` | Int64 | Games played. |
| `rank` | Int64 | Rank. |
| `adj_net_z` | Float64 |  |
| `adj_off_rtg_rank` | Float64 | Rank of adj_off_rtg, the opponent-adjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the highest value ranks 1, average ties. |
| `adj_def_rtg_rank` | Float64 | Rank of adj_def_rtg, the opponent-adjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the lowest value ranks 1 (fewer points allowed is better), average ties. |
| `adj_net_rtg_rank` | Float64 | Rank of adj_net_rtg, the opponent-adjusted net rating (adj_off_rtg minus adj_def_rtg), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the highest value ranks 1, average ties. |
| `adj_pace_rank` | Float64 | Rank of adj_pace, the opponent-adjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the fastest team ranks 1 (pace has no good direction), average ties. |
| `raw_off_rtg_rank` | Float64 | Rank of raw_off_rtg, the unadjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the highest value ranks 1, average ties. |
| `raw_def_rtg_rank` | Float64 | Rank of raw_def_rtg, the unadjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the lowest value ranks 1 (fewer points allowed is better), average ties. |
| `raw_pace_rank` | Float64 | Rank of raw_pace, the unadjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the fastest team ranks 1 (pace has no good direction), average ties. |
| `adj_off_rtg_pct` | Float64 | Percentile of adj_off_rtg, the opponent-adjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_def_rtg_pct` | Float64 | Percentile of adj_def_rtg, the opponent-adjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_net_rtg_pct` | Float64 | Percentile of adj_net_rtg, the opponent-adjusted net rating (adj_off_rtg minus adj_def_rtg), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_pace_pct` | Float64 | Percentile of adj_pace, the opponent-adjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `raw_off_rtg_pct` | Float64 | Percentile of raw_off_rtg, the unadjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `raw_def_rtg_pct` | Float64 | Percentile of raw_def_rtg, the unadjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `raw_pace_pct` | Float64 | Percentile of raw_pace, the unadjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_off_rtg_n` | Int64 | Sample size of adj_off_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `adj_def_rtg_n` | Int64 | Sample size of adj_def_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `adj_net_rtg_n` | Int64 | Sample size of adj_net_rtg: the box-score possessions the rating is computed over on both ends (offense plus defense, twice the possession estimate), summed over the team's games and rounded. |
| `adj_pace_n` | Int64 | Sample size of adj_pace: the games the pace is computed over (equal to games). |
| `raw_off_rtg_n` | Int64 | Sample size of raw_off_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `raw_def_rtg_n` | Int64 | Sample size of raw_def_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `raw_pace_n` | Int64 | Sample size of raw_pace: the games the pace is computed over (equal to games). |

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
ratings = nba_team_ratings(2024)
ratings.sort("rank").head()

# As-of-date (leakage-safe) ratings for a backtest

import datetime as dt
ratings = nba_team_ratings(2024, as_of_date=dt.date(2024, 1, 15))

# WNBA / G-League via ``league_id``

wnba_ratings = nba_team_ratings(2024, league_id="10")
```

### nba_war {#nba_war}

`nba_war(ratings: 'pl.DataFrame', poss: 'pl.DataFrame', *, replacement_level: 'float', pts_per_win: 'float', rating_col: 'str' = 'rating', poss_col: 'str' = 'poss', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Points-above-replacement -> wins for each player.

`war_i = (rating_i - replacement_level) * poss_i / 100 / pts_per_win`.

`poss_i` counts each possession once, as a team's net rating per 100 does: a game is about
100 possessions a side, not 200. A net rating per 100 (`rapm = o_rapm + d_rapm`) spends
`o_rapm` over the offensive possessions and `d_rapm` over the defensive ones, so its
points are about `rapm * (off_poss + def_poss) / 2 / 100`: pass `(off_poss + def_poss) / 2`,
never the sum. The sum doubles every WAR. The 2025-26 NBA `player_impact` release was built
with the sum: its league WAR totals 1,582 against the 791 the halved possessions give, and a
team's summed WAR rose 1.55 per win (0.78 halved, under 1 as a shrunken rating's should).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Per-player rating frame with `player_id` and `rating_col` (e.g. `nba_rapm`'s `rapm` column renamed, a `nba_ratings_panel` row filtered to one date, or `nba_bpm`'s `bpm` column). |
| `poss` | `DataFrame` |  | Per-player possession-count frame with `player_id` and `poss_col`: the possessions the rating is per 100 of, each counted once (`(off_poss + def_poss) / 2` from `nba_rapm`; see above). |
| `replacement_level` | `float` |  | Per-100-possession rating of a replacement-level player. No built-in default — calibrate via `calibrate_replacement_level`. |
| `pts_per_win` | `float` |  | Points of season point-margin per marginal win. No built-in default — calibrate via `calibrate_pts_per_win`. |
| `rating_col` | `str` | `'rating'` | Column in `ratings` to score. |
| `poss_col` | `str` | `'poss'` | Column in `poss` giving the possessions played, each counted once. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Frame with `WAR_SCHEMA` columns (`player_id`, `war`). Empty (that schema) when either input is empty.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `war` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If both inputs are non-empty but share no `player_id` (a probable id-source or dtype mismatch, not a normal empty case). |

**Example**

```python
from sportsdataverse.nba.nba_war import nba_war
war = nba_war(rapm_df.rename({"rapm": "rating"}), poss_df,
               replacement_level=-2.0, pts_per_win=250.0)
print(war.sort("war", descending=True).head())

# Derive both required kwargs from real data first

from sportsdataverse.nba.nba_war import (
    calibrate_pts_per_win, calibrate_replacement_level, nba_war,
)
pts_per_win = calibrate_pts_per_win(team_standings)
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=pts_per_win, target_total_war=300.0,
)
war = nba_war(ratings, poss, replacement_level=repl, pts_per_win=pts_per_win)
```

### ot_adjusted_seconds {#ot_adjusted_seconds}

`ot_adjusted_seconds(pbp: 'pl.DataFrame', *, regulation_periods: 'int' = 4) -> 'pl.DataFrame'`

Tag overtime rows and give them the overtime period clock as their seconds remaining.

The in-game models clip `start_game_seconds_remaining` at zero, so every overtime play
(negative in the NBA frame) would score as "game over at the current margin". Each overtime is
scored as its own five-minute endgame instead: on rows with `period_number > regulation_periods`
`start_game_seconds_remaining` becomes the period clock, `start_quarter_seconds_remaining` or
(men's college frames) `start_period_seconds_remaining`, whichever is present and not null.
In a halves game (`regulation_periods=2`) period 3 is the first overtime.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the basketball pbp schema. |
| `regulation_periods` | `int` | `4` | The game's regulation periods (4 quarters, 2 halves); see `game_regulation_periods`. |

**Returns**

The frame with `wp_regime` (`"reg"` / `"ot"`) and the adjusted seconds column.

| col_name | type | description |
|---|---|---|
| `wp_regime` | String | "reg" for regulation plays; "ot" for plays after the last regulation period (period 3 on in a halves game, 5 on in a quarters game), scored on the overtime period clock as a five-minute endgame. |
| `start_game_seconds_remaining` | Float64 | Seconds remaining the model sees: the game clock in regulation, the overtime period clock (the quarter clock, or the period clock in men's college frames) on overtime plays. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If `period_number` or `start_game_seconds_remaining` is missing, or neither period clock column is present. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import ot_adjusted_seconds
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
tagged = ot_adjusted_seconds(pbp)
tagged.filter(pl.col("wp_regime") == "ot").select("start_game_seconds_remaining").head()

# A men's college game (two halves)

from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
ot_adjusted_seconds(one, regulation_periods=2)
```
