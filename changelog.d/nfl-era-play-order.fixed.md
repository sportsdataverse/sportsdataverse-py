- **NFL:** before 2014, a play ESPN files under a later period than the plays on both sides of it takes their period
  (727 rows 2002-13, every one that joins agreeing with nflverse `qtr`), so a half no longer "ends" on a misfiled
  kickoff and its drives, clock and EPA follow; 261022011 now has one first-half end. (#856)
- **NFL:** the EPA scoring overlay reads the corrected touchdown flags: a kicking-team recovery returned for a score
  takes the kicker's touchdown (2010: +6.30 -> -7.54, nflverse -7.67), and a touchdown-typed row whose score did not
  stand takes no touchdown value (2024 short and onside kicks: 5.13 / 5.75 -> 0.67 / 1.02, nflverse 0.93 / 1.42). (#856)
- **NFL:** a touchback kickoff (its text says touchback, or the receiver next snaps at the touchback spot) scores
  `EP_end` at its start clock, so ESPN's end clock no longer moves its EPA (2007 KC kickoff at 0:23 with a 1:08 end
  clock: +0.52 -> 0, nflverse 0), at the touchback spot when ESPN's end clock is 0; WPA is unchanged. (#856)
