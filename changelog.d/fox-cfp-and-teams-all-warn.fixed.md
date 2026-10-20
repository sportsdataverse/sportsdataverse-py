- **Fox CFB / college basketball:** a failed Fox CFP group fetch in `fox_cfb_schedule(season)` is
  retried once and then warns (it was a log line); `fox_{mbb,wbb}_teams_all` warn when `max_calls` runs
  out while the last fetch was still finding teams. (#TBD)
