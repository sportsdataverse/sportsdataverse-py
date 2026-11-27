- **CFB:** `cfb_hierarchical_ratings` rates teams as of a slot with the hierarchical team-strength model, an EM ridge
  over completed games before the slot carried over season to season from 2014. (#1370)
- **CFB:** `cfb_predict_games(..., model="hierarchical", hierarchical=...)` serves its margins and win probabilities.
  It is opt-in; the default stays the closed form, totals stay closed-form, and it raises rather than fall back. (#1370)
