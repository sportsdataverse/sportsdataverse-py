- **Fox CFB / college basketball:** a failed Fox CFP group fetch in `fox_cfb_schedule(season)` is retried once, then
  warns (a 404 means "no CFP group"); `fox_{mbb,wbb}_teams_all` warn when `max_calls` runs out while the last fetch
  was still finding teams. (#922)
