- **HockeyTech:** the 0.4 s request spacing holds when several threads call `hockeytech_api` at once; they could all
  read one stale timestamp and fire together. (#TBD)
