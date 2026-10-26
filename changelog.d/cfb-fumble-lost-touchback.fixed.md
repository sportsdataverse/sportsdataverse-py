- **CFB pbp:** `fumble_lost` / `fumble_recovered` read the recovering team from the text: a punt returner recovering
  his own fumble is no loss, and a muffed punt the kicking team recovers is one. (#778)
- **CFB pbp:** kickoff touchbacks come out to the 25 from 2012 (they switched only after 2013, in `yds_kickoff_return`
  and the EP touchback yardline), and `yds_fumble_return` reads the 2004-06 "returned for a N yard touchdown". (#778)
