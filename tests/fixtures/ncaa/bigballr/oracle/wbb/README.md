<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [wbigballR oracle fixtures (WBB) — provenance](#wbigballr-oracle-fixtures-wbb--provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# wbigballR oracle fixtures (WBB) — provenance

captured: 2026-07-12 22:37:31 EDT
wbigballR: run from source, git SHA 7d097d1a8ada8b60d9bbf7965047aa777f4c9bdf (bigballR namespace co-loaded: teamids fork bug)
R: R version 4.5.3 (2026-03-11 ucrt)
games: 5722355, 5732292, 5728709, 5733807  (5722355 SC 92-60 blowout; 5732292 SC-Michigan 68-62 neutral; 5728709 ND-Texas 1 OT; 5733807 NCSU-ND 2 OT)
team: South Carolina WBB 2024-25 (team_id 592003); scoreboard date 12/05/2024 (W season 18423)
transport: FULLY OFFLINE — use_file=TRUE over browser-captured pages (rcache/, byte-identical to ../html fixtures)
known R-side quirks reproduced: Home/Away self-name = NA (bigballR::teamids men's-table lookup);
  get_box_scores(multi.games=TRUE) broken vs current markup (Pos column dropped);
  shots surface unexported in wbigballR (no oracle).
NA cells are written as the literal string NA (write.csv default) — readers must set null_values.
