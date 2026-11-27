- **CFB:** `cfb_resume` adds reference-team schedule strength (`sos_<ref>_past` / `_remaining` / `_season` for the
  5th, 25th and 12th-best rated FBS team and an average team), record difficulty (`record_difficulty_<ref>`, an exact
  Poisson-binomial tail) and `wins_above_bubble`; `as_of_date` splits completed from remaining games for them. (#1375)
- **CFB:** against the selection-day committee ranking (2014-2023) `wins_above_bubble` ties `wab` (Spearman +0.0003,
  95% CI -0.019 to +0.018); `wab` keeps its definition here and is deprecated, if at all, in a later release. (#1375)
