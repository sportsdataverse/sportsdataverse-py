- **BBL / DBBL (`scb`):** `tv_channel` on `scb_games_recent` and `scb_game` joins the channel names
  (`"DYN,Sport Bild Stream"`) instead of writing Python dict text; null entries and null names are
  skipped, never written as `"None"`. (#892)
- **P. LEAGUE+ (`pleague`):** `action_en` in the play-by-play is read from each row's own phrase, so free throws say
  which throw of how many (`"free throw 2, made"`) instead of every one reading `"free throw 1 of 3 awarded, ..."`.
  (#892)
- **KBL:** the docs say an earlier (pre-2006-07) game's shot chart is a zero-row frame, as the code
  returns, not `NoDataError`. (#892)
- **Basketligaen (`sportality`):** five returns-table descriptions (`ppg`, `3_p_perc`, `blkpg`, `ec`, `ft_perc`) are no
  longer cut mid-parenthesis, and the `standings_standings` test samples keep their whole `dataColumns` header. (#892)
