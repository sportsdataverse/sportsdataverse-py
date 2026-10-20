- **Genius hosted pages:** the HTTP 500 "A website error has occurred" page is now reported as an intermittent
  Genius host-side failure marked `transient` (retry later), not as "the org has no plain hosted site": on
  2026-10-18 the same WBBL, SLB, BAL and NZNBL schedule URLs answered it and then a full schedule, and ESAKE
  answered it too. (#870)
