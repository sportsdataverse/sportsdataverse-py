- **CFB:** `cfb_resume` adds reference-team schedule strength (`sos_<ref>_past` / `_remaining` / `_season` for the
  5th, 25th and 12th-best rated FBS team and an average team), record difficulty (`record_difficulty_<ref>`, an exact
  Poisson-binomial tail) and `wins_above_bubble`; `as_of_date` now splits completed from remaining games. (#1375)
- **CFB:** `wins_above_bubble` does not replace `wab`: against the selection-day committee ranking (2014-2023) its
  Spearman is 0.014 lower on average (95% CI -0.040 to 0.011), so both columns stay. (#1375)
